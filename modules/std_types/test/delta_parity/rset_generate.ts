import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";

import { createMockRContext } from "../mock_rcontext.js";
import { RCap, rCapFactory } from "../../src/types/rcap/rcap.js";
import { RSet, rSetFactory } from "../../src/types/rset/rset.js";
import { serializePublicKeyToBase64 } from "../../src/authorship.js";

import { pickConcurrentAt, recordCheckpoint } from "./checkpoints.js";
import { makeIdentity } from "./identities.js";
import type { RSetHistory } from "./parity.js";
import { PRNG } from "./prng.js";

const ELEMENT_POOL_SIZE = 20;

function elementName(index: number): string {
    return `el-${index}`;
}

async function resolveAt(rawDag: dag.Dag, at?: Version): Promise<Version> {
    return at ?? await rawDag.getFrontier();
}

type PlainRSetOp = "add" | "delete";

async function availablePlainOpsAt(
    rset: RSet,
    at: Version,
): Promise<PlainRSetOp[]> {
    const view = await rset.getView(at, at);
    const ops: PlainRSetOp[] = ["add"];

    for (let i = 0; i < ELEMENT_POOL_SIZE; i++) {
        if (await view.has(elementName(i))) {
            ops.push("delete");
            break;
        }
    }

    return ops;
}

async function pickElementInSet(
    rset: RSet,
    at: Version,
    prng: PRNG,
): Promise<string | undefined> {
    const view = await rset.getView(at, at);
    const present: string[] = [];
    for (let i = 0; i < ELEMENT_POOL_SIZE; i++) {
        const element = elementName(i);
        if (await view.has(element)) {
            present.push(element);
        }
    }
    if (present.length === 0) {
        return undefined;
    }
    return present[prng.nextInt(0, present.length - 1)];
}

async function pickElementNotInSet(
    rset: RSet,
    at: Version,
    prng: PRNG,
): Promise<string> {
    const view = await rset.getView(at, at);
    const absent: string[] = [];
    for (let i = 0; i < ELEMENT_POOL_SIZE; i++) {
        const element = elementName(i);
        if (!(await view.has(element))) {
            absent.push(element);
        }
    }
    if (absent.length > 0) {
        return absent[prng.nextInt(0, absent.length - 1)];
    }
    return elementName(prng.nextInt(0, ELEMENT_POOL_SIZE - 1));
}

export async function generatePlainRSetHistory(seed: number, ops: number): Promise<RSetHistory> {
    const prng = new PRNG(seed);
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSet.typeId, rSetFactory);

    const init = await RSet.create({
        seed: `fuzz-rset-plain-${seed}`,
        initialElements: [],
        hashAlgorithm: "sha256",
    });
    const rset = (await ctx.createObject(init)) as RSet;
    const rawDag = (await ctx.getDag(rset.getId()))!;

    const checkpoints: Version[] = [version(rset.getId())];

    for (let opIndex = 0; opIndex < ops; opIndex++) {
        const at = pickConcurrentAt(prng, checkpoints);
        const atVersion = await resolveAt(rawDag, at);
        const choices = await availablePlainOpsAt(rset, atVersion);

        let op: PlainRSetOp;
        if (choices.includes("delete") && prng.next() < 0.45) {
            op = "delete";
        } else {
            op = "add";
        }
        if (!choices.includes(op)) {
            op = choices[prng.nextInt(0, choices.length - 1)];
        }

        switch (op) {
            case "add": {
                const element = await pickElementNotInSet(rset, atVersion, prng);
                await rset.add(element, at);
                break;
            }
            case "delete": {
                const element = await pickElementInSet(rset, atVersion, prng);
                if (element === undefined) continue;
                await rset.delete(element, at);
                break;
            }
        }

        await recordCheckpoint(checkpoints, await rawDag.getFrontier());
    }

    return { rset, rawDag, checkpoints, seed };
}

type CapOp = "addIdentity" | "grant" | "revoke";

async function availableCapOpsAt(
    cap: RCap,
    at: Version,
    registered: OwnIdentity[],
): Promise<CapOp[]> {
    const view = await cap.getView(at, at);
    const ops: CapOp[] = ["addIdentity"];
    let canGrant = false;
    let canRevoke = false;

    for (const identity of registered) {
        if (cap.isCreator(identity.keyId)) continue;
        if (!(await view.isIdentity(identity.keyId))) continue;
        if (await view.hasCapability(identity.keyId, "write")) {
            canRevoke = true;
        } else {
            canGrant = true;
        }
    }

    if (canGrant) ops.push("grant");
    if (canRevoke) ops.push("revoke");

    return ops;
}

async function syncRefAdvance(
    cap: RCap,
    rset: RSet,
    admin: OwnIdentity,
    at?: Version,
): Promise<void> {
    const capFrontier = await (await cap.getScopedDag()).getFrontier();
    await rset.refAdvance(capFrontier, admin, at);
}

async function writersAt(
    cap: RCap,
    at: Version,
    registered: OwnIdentity[],
): Promise<OwnIdentity[]> {
    const view = await cap.getView(at, at);
    const result: OwnIdentity[] = [];
    for (const identity of registered) {
        if (await view.hasCapability(identity.keyId, "write")) {
            result.push(identity);
        }
    }
    return result;
}

export async function generatePermissionedRSetHistory(seed: number, ops: number): Promise<RSetHistory> {
    const prng = new PRNG(seed);
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RCap.typeId, rCapFactory);
    ctx.getRegistry().register(RSet.typeId, rSetFactory);

    const admin = await makeIdentity();
    const extraCount = prng.nextInt(2, 3);
    const extras: OwnIdentity[] = [];
    for (let i = 0; i < extraCount; i++) {
        extras.push(await makeIdentity());
    }

    const capInit = await RCap.create({
        seed: `fuzz-rset-perm-cap-${seed}`,
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        initialCaps: {
            admin: { managedBy: ["creator"] },
            write: { managedBy: ["admin"] },
        },
    });
    const cap = (await ctx.createObject(capInit)) as RCap;

    const setInit = await RSet.create({
        seed: `fuzz-rset-perm-${seed}`,
        initialElements: [],
        hashAlgorithm: "sha256",
        capabilityRef: cap.getId(),
        capRequirements: { add: "write", delete: "write" },
    });
    const rset = (await ctx.createObject(setInit)) as RSet;
    const capDag = (await ctx.getDag(cap.getId()))!;
    const rawDag = (await ctx.getDag(rset.getId()))!;

    const capCheckpoints: Version[] = [version(cap.getId())];
    const checkpoints: Version[] = [version(rset.getId())];
    const registered: OwnIdentity[] = [admin, ...extras];

    for (const identity of extras) {
        await cap.addIdentity(
            identity.keyId,
            serializePublicKeyToBase64(identity.publicKey),
            admin,
        );
        await recordCheckpoint(capCheckpoints, await capDag.getFrontier());
    }

    await syncRefAdvance(cap, rset, admin);
    await recordCheckpoint(checkpoints, await rawDag.getFrontier());

    // Seed at least one writer besides the creator.
    if (extras.length > 0) {
        await cap.grant(extras[0].keyId, "write", admin);
        await syncRefAdvance(cap, rset, admin);
        await recordCheckpoint(capCheckpoints, await capDag.getFrontier());
        await recordCheckpoint(checkpoints, await rawDag.getFrontier());
    }

    for (let opIndex = 0; opIndex < ops; opIndex++) {
        const capFrontier = await capDag.getFrontier();
        const writers = await writersAt(cap, capFrontier, registered);

        const useCapOp = prng.next() < 0.35 || writers.length === 0;

        if (useCapOp) {
            const capAt = pickConcurrentAt(prng, capCheckpoints);
            const capAtVersion = await resolveAt(capDag, capAt);
            const capChoices = await availableCapOpsAt(cap, capAtVersion, registered);
            const capOp = capChoices[prng.nextInt(0, capChoices.length - 1)];

            switch (capOp) {
                case "addIdentity": {
                    const identity = await makeIdentity();
                    registered.push(identity);
                    await cap.addIdentity(
                        identity.keyId,
                        serializePublicKeyToBase64(identity.publicKey),
                        admin,
                        capAt,
                    );
                    break;
                }
                case "grant": {
                    const view = await cap.getView(capAtVersion, capAtVersion);
                    const grantable = [];
                    for (const identity of registered) {
                        if (cap.isCreator(identity.keyId)) continue;
                        if (!(await view.isIdentity(identity.keyId))) continue;
                        if (await view.hasCapability(identity.keyId, "write")) continue;
                        grantable.push(identity);
                    }
                    const grantee = grantable[prng.nextInt(0, grantable.length - 1)];
                    await cap.grant(grantee.keyId, "write", admin, capAt);
                    break;
                }
                case "revoke": {
                    const view = await cap.getView(capAtVersion, capAtVersion);
                    const revokable = [];
                    for (const identity of registered) {
                        if (cap.isCreator(identity.keyId)) continue;
                        if (await view.hasCapability(identity.keyId, "write")) {
                            revokable.push(identity);
                        }
                    }
                    const grantee = revokable[prng.nextInt(0, revokable.length - 1)];
                    await cap.revoke(grantee.keyId, "write", admin, capAt);
                    break;
                }
            }

            await recordCheckpoint(capCheckpoints, await capDag.getFrontier());
            await syncRefAdvance(cap, rset, admin);
            await recordCheckpoint(checkpoints, await rawDag.getFrontier());
            continue;
        }

        const currentWriters = await writersAt(cap, await capDag.getFrontier(), registered);
        if (currentWriters.length === 0) {
            continue;
        }

        const view = await rset.getView();
        const canDelete: string[] = [];
        for (let i = 0; i < ELEMENT_POOL_SIZE; i++) {
            const element = elementName(i);
            if (await view.has(element)) {
                canDelete.push(element);
            }
        }

        const doDelete = canDelete.length > 0 && prng.next() < 0.45;
        const author = currentWriters[prng.nextInt(0, currentWriters.length - 1)];

        if (doDelete) {
            const element = canDelete[prng.nextInt(0, canDelete.length - 1)];
            await rset.deleteSigned(element, author);
        } else {
            const absent: string[] = [];
            for (let i = 0; i < ELEMENT_POOL_SIZE; i++) {
                const element = elementName(i);
                if (!(await view.has(element))) {
                    absent.push(element);
                }
            }
            if (absent.length === 0) {
                continue;
            }
            const element = absent[prng.nextInt(0, absent.length - 1)];
            await rset.addSigned(element, author);
        }

        await recordCheckpoint(checkpoints, await rawDag.getFrontier());
    }

    return { rset, rawDag, checkpoints, seed };
}

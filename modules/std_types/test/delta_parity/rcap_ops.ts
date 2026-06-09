import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";

import { RCap } from "../../src/types/rcap/rcap.js";
import { serializePublicKeyToBase64 } from "../../src/authorship.js";

import { makeIdentity } from "./identities.js";
import { PRNG } from "./prng.js";

export type RCapOp = "addIdentity" | "grant" | "revoke" | "createCap" | "deleteCap";

export async function resolveAt(rawDag: dag.Dag, at?: Version): Promise<Version> {
    return at ?? await rawDag.getFrontier();
}

export async function availableOpsAt(
    cap: RCap,
    at: Version,
    registered: OwnIdentity[],
    dynamicCaps: string[],
): Promise<RCapOp[]> {
    const view = await cap.getView(at, at);
    const ops: RCapOp[] = ["addIdentity", "createCap"];
    let canGrant = false;
    let canRevoke = false;

    for (const identity of registered) {
        if (cap.isCreator(identity.keyId)) {
            continue;
        }
        if (!(await view.isIdentity(identity.keyId))) {
            continue;
        }
        if (await view.hasCapability(identity.keyId, "write")) {
            canRevoke = true;
        } else {
            canGrant = true;
        }
    }

    if (canGrant) ops.push("grant");
    if (canRevoke) ops.push("revoke");

    for (const capName of dynamicCaps) {
        if (await view.capabilityExists(capName)) {
            ops.push("deleteCap");
        }
    }

    return ops;
}

export function pickRCapOp(
    prng: PRNG,
    choices: RCapOp[],
    preferGrantRevoke: boolean,
): RCapOp {
    if (!preferGrantRevoke || choices.length === 1) {
        return choices[prng.nextInt(0, choices.length - 1)];
    }

    const barrierOps = choices.filter((op) => op === "grant" || op === "revoke");
    if (barrierOps.length > 0 && prng.next() < 0.7) {
        return barrierOps[prng.nextInt(0, barrierOps.length - 1)];
    }

    return choices[prng.nextInt(0, choices.length - 1)];
}

export type RCapGeneratorState = {
    registered: OwnIdentity[];
    dynamicCaps: string[];
    nextCapIndex: number;
};

export async function executeRCapOp(
    cap: RCap,
    admin: OwnIdentity,
    rawDag: dag.Dag,
    state: RCapGeneratorState,
    prng: PRNG,
    at: Version | undefined,
    preferGrantRevoke: boolean,
): Promise<void> {
    const atVersion = await resolveAt(rawDag, at);
    const choices = await availableOpsAt(cap, atVersion, state.registered, state.dynamicCaps);
    const op = pickRCapOp(prng, choices, preferGrantRevoke);

    switch (op) {
        case "addIdentity": {
            const identity = await makeIdentity();
            state.registered.push(identity);
            await cap.addIdentity(
                identity.keyId,
                serializePublicKeyToBase64(identity.publicKey),
                admin,
                at,
            );
            break;
        }
        case "grant": {
            const view = await cap.getView(atVersion, atVersion);
            const grantable = [];
            for (const identity of state.registered) {
                if (cap.isCreator(identity.keyId)) continue;
                if (!(await view.isIdentity(identity.keyId))) continue;
                if (await view.hasCapability(identity.keyId, "write")) continue;
                grantable.push(identity);
            }
            const grantee = grantable[prng.nextInt(0, grantable.length - 1)];
            await cap.grant(grantee.keyId, "write", admin, at);
            break;
        }
        case "revoke": {
            const view = await cap.getView(atVersion, atVersion);
            const revokable = [];
            for (const identity of state.registered) {
                if (cap.isCreator(identity.keyId)) continue;
                if (await view.hasCapability(identity.keyId, "write")) {
                    revokable.push(identity);
                }
            }
            const grantee = revokable[prng.nextInt(0, revokable.length - 1)];
            await cap.revoke(grantee.keyId, "write", admin, at);
            break;
        }
        case "createCap": {
            const capName = `dyn-${state.nextCapIndex++}`;
            state.dynamicCaps.push(capName);
            await cap.createCap(capName, ["admin"], admin, at);
            break;
        }
        case "deleteCap": {
            const view = await cap.getView(atVersion, atVersion);
            const deletable = [];
            for (const capName of state.dynamicCaps) {
                if (await view.capabilityExists(capName)) {
                    deletable.push(capName);
                }
            }
            const capName = deletable[prng.nextInt(0, deletable.length - 1)];
            await cap.deleteCap(capName, admin, at);
            break;
        }
    }
}

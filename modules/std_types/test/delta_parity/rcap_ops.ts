import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";

import { RCap } from "../../src/types/rcap/rcap.js";
import type { RCapView } from "../../src/types/rcap/interfaces.js";
import { serializePublicKeyToBase64 } from "../../src/authorship.js";

import { makeIdentity } from "./identities.js";
import { PRNG } from "./prng.js";

export type RCapOp = "addIdentity" | "grant" | "revoke" | "createCap" | "deleteCap" | "recreateCap";

export async function resolveAt(rawDag: dag.Dag, at?: Version): Promise<Version> {
    return at ?? await rawDag.getFrontier();
}

// The capabilities grants and revokes pick from: `write` and every dynamic
// capability that exists at `view`.
async function grantableCaps(view: RCapView, dynamicCaps: string[]): Promise<string[]> {
    const caps = ["write"];
    for (const capName of dynamicCaps) {
        if (await view.capabilityExists(capName)) caps.push(capName);
    }
    return caps;
}

// Every (identity, capability) pair that is not a creator's, whose grantee is an
// identity at `view`, and whose holding at `view` is `held`.
async function pairsWithHolding(
    cap: RCap, view: RCapView, registered: OwnIdentity[], caps: string[], held: boolean,
): Promise<Array<[OwnIdentity, string]>> {
    const pairs: Array<[OwnIdentity, string]> = [];
    for (const identity of registered) {
        if (cap.isCreator(identity.keyId)) continue;
        if (!(await view.isIdentity(identity.keyId))) continue;
        for (const capName of caps) {
            if (await view.hasCapability(identity.keyId, capName) === held) pairs.push([identity, capName]);
        }
    }
    return pairs;
}

export async function availableOpsAt(
    cap: RCap,
    at: Version,
    registered: OwnIdentity[],
    dynamicCaps: string[],
): Promise<RCapOp[]> {
    const view = await cap.getView(at, at);
    const ops: RCapOp[] = ["addIdentity", "createCap"];
    const caps = await grantableCaps(view, dynamicCaps);

    if ((await pairsWithHolding(cap, view, registered, caps, false)).length > 0) ops.push("grant");
    if ((await pairsWithHolding(cap, view, registered, caps, true)).length > 0) ops.push("revoke");

    for (const capName of dynamicCaps) {
        ops.push(await view.capabilityExists(capName) ? "deleteCap" : "recreateCap");
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
        case "grant":
        case "revoke": {
            const view = await cap.getView(atVersion, atVersion);
            const caps = await grantableCaps(view, state.dynamicCaps);
            const pairs = await pairsWithHolding(cap, view, state.registered, caps, op === "revoke");
            const [grantee, capName] = pairs[prng.nextInt(0, pairs.length - 1)];
            if (op === "grant") await cap.grant(grantee.keyId, capName, admin, at);
            else await cap.revoke(grantee.keyId, capName, admin, at);
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
        case "recreateCap": {
            // Turns a deleted slot back on: the grants written before the
            // delete hold again.
            const view = await cap.getView(atVersion, atVersion);
            const deleted = [];
            for (const capName of state.dynamicCaps) {
                if (!(await view.capabilityExists(capName))) deleted.push(capName);
            }
            const capName = deleted[prng.nextInt(0, deleted.length - 1)];
            await cap.createCap(capName, ["admin"], admin, at);
            break;
        }
    }
}

import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";

import { createMockRContext } from "../mock_rcontext.js";
import { RCap, rCapFactory } from "../../src/types/rcap/rcap.js";
import { serializePublicKeyToBase64 } from "../../src/authorship.js";

import { recordCheckpoint } from "./checkpoints.js";
import { makeIdentity } from "./identities.js";
import type { RCapHistory } from "./parity.js";
import { PRNG } from "./prng.js";

// Shaped RCap histories: the skeleton of the bounded-delta counterexample
// (revokeChainHistory in delta_cap_tests.ts), with randomized barriers on
// it. The random generators never build it: all their grants and revokes are
// the creator's, so no verdict ever flips.
//
//     x ── E ── s1 ── N         start = {s1, T}, end = {N, T}
//      \
//       T                       T is concurrent with E and with s1
//
// super is managed by the creator, admin by super, write by admin. S1 and S2
// hold super, and A and A2 hold admin, all granted before x. E is A granting
// write to B, so its verdict reads every revoke of A's admin concurrent with
// it. T is an old barrier on a side branch:
//   - a revoke of A's admin by S1 or S2, which reaches E;
//   - a revoke of A2's admin, which does not; or
//   - a delete-cap of a dynamic capability, whose effect never changes.
// Each variant adds 0-2 noise identities after E and after T. N usually revokes
// the super of S1 or S2 (voiding T when it is T's author), and is otherwise
// noise. Checkpoints are the frontier at x and after every later append, so the
// pairs include (start, end) as well as the prefixes, and the setup below x
// does not crowd them out of the smoke profile's sample.

const VARIANTS_PER_SEED = 5;

async function revokeChainShape(prng: PRNG, seed: number, variant: number): Promise<RCapHistory> {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RCap.typeId, rCapFactory);

    const R = await makeIdentity();
    const cap = (await ctx.createObject(await RCap.create({
        seed: `fuzz-rcap-shaped-${seed}-${variant}`,
        creators: [{ keyId: R.keyId, publicKey: R.publicKey }],
        initialCaps: {
            super: { managedBy: ["creator"] },
            admin: { managedBy: ["super"] },
            write: { managedBy: ["admin"] },
        },
    }))) as RCap;
    const rawDag = (await ctx.getDag(cap.getId()))!;

    const checkpoints: Version[] = [];
    const record = async (appended: Promise<B64Hash>): Promise<B64Hash> => {
        const hash = await appended;
        await recordCheckpoint(checkpoints, await rawDag.getFrontier());
        return hash;
    };
    const addIdentity = async (identity: OwnIdentity, at?: B64Hash): Promise<B64Hash> =>
        record(cap.addIdentity(identity.keyId, serializePublicKeyToBase64(identity.publicKey), R, at !== undefined ? version(at) : undefined));
    const noise = async (tip: B64Hash): Promise<B64Hash> => {
        for (let i = prng.nextInt(0, 2); i > 0; i--) {
            tip = await addIdentity(await makeIdentity(), tip);
        }
        return tip;
    };

    const [S1, S2, A, A2, B] = [await makeIdentity(), await makeIdentity(), await makeIdentity(), await makeIdentity(), await makeIdentity()];
    for (const identity of [S1, S2, A, A2, B]) {
        await cap.addIdentity(identity.keyId, serializePublicKeyToBase64(identity.publicKey), R);
    }
    await cap.grant(S1.keyId, "super", R);
    await cap.grant(S2.keyId, "super", R);
    await cap.grant(A.keyId, "admin", prng.next() < 0.5 ? R : S2);
    await cap.grant(A2.keyId, "admin", R);
    const x = await cap.createCap("dyn", ["admin"], R);
    await recordCheckpoint(checkpoints, await rawDag.getFrontier());

    const E = await record(cap.grant(B.keyId, "write", A, version(x)));
    const s1 = await addIdentity(await makeIdentity(), await noise(E));

    const supers = [S1, S2];
    const tKind = prng.next();
    let tAuthor: OwnIdentity | undefined;
    let T: B64Hash;
    if (tKind < 0.6) {
        tAuthor = supers[prng.nextInt(0, 1)];
        T = await record(cap.revoke(A.keyId, "admin", tAuthor, version(x)));
    } else if (tKind < 0.85) {
        tAuthor = supers[prng.nextInt(0, 1)];
        T = await record(cap.revoke(A2.keyId, "admin", tAuthor, version(x)));
    } else {
        T = await record(cap.deleteCap("dyn", R, version(x)));
    }
    await noise(T);

    if (prng.next() < 0.75) {
        const target = tAuthor !== undefined && prng.next() < 0.8 ? tAuthor : supers[prng.nextInt(0, 1)];
        await record(cap.revoke(target.keyId, "super", R, version(s1)));
    } else {
        await addIdentity(await makeIdentity(), s1);
    }

    return { cap, rawDag, checkpoints, seed };
}

// Several variants of the skeleton for this seed. `ops` is unused: the shape is fixed.
export async function generateShapedRCapHistories(seed: number, _ops: number): Promise<RCapHistory[]> {
    const histories: RCapHistory[] = [];
    for (let variant = 0; variant < VARIANTS_PER_SEED; variant++) {
        histories.push(await revokeChainShape(new PRNG(seed * 31 + variant), seed, variant));
    }
    return histories;
}

import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";

import { createMockRContext } from "../mock_rcontext.js";
import { RCap, rCapFactory } from "../../src/types/rcap/rcap.js";
import { RSet, rSetFactory } from "../../src/types/rset/rset.js";
import { serializePublicKeyToBase64 } from "../../src/authorship.js";

import { recordCheckpoint } from "./checkpoints.js";
import { makeIdentity } from "./identities.js";
import type { RSetHistory } from "./parity.js";
import { PRNG } from "./prng.js";

// Shaped permissioned RSet histories: the skeletons of the bounded-delta
// counterexamples ([SET_BOUND01-03] in delta_set_tests.ts), with randomized
// content. The other RSet generators append at the set's frontier and publish
// the RCap frontier, so they never put a ref-advance on a side branch or
// publish a version that a later revoke is concurrent with.
//
// One RCap per history (root by the creator, super by root, admin by super,
// write by admin):
//
//     gA ── G2 ── G1 ── s1 ── V      G2 grants B2 write, by the creator; G1 grants B1 write, by A
//      \           \      \          Tc revokes A's admin, concurrent with G2 and G1: it kills G1 while live
//       Tc          t1     W         V revokes S's super, concurrent with Tc: it voids Tc while live
//                                    W revokes Rt's root, concurrent with V: it voids V
//                                    r (on G1) revokes B1's write, concurrent with s1 and t1
//
// G2 is below G1 so that R0 publishes one head: RCap's bound is the meet of
// the new entries' attachment points, which leaves out a concurrent head no
// new entry touches, and an R0 above the bound makes every pair walk it all.
//
// The set, with every ref-advance by the creator publishing the fold at its
// position plus one RCap entry drawn from a list for its role:
//
//     R0 ── [e0] ── X ── [RM] ── [P] ── N
//              \                 \
//               T ── [noise]      RS
//
// - R0 publishes G1. e0 is the creator's add of el-0, made when X deletes it.
// - X is an add by B1 or B2 (a barrier add on half of them, in that flavor), or
//   B1's delete of el-0 (a barrier on that flavor).
// - T, concurrent with X, widens X's refAt (gap 1: t1 or s1 with r imported by N).
// - RM, RS and N carry the revoke chain (gap 2: Tc, then V, then W).
// Checkpoints are the frontier after every append, so (frontier before N, end)
// and its prefixes are among the pairs, and the whole history fits the smoke
// profile's sample.

const VARIANTS_PER_SEED = 6;

type Flavor = "plain" | "barrierDelete" | "barrierAdd";
const FLAVORS: Flavor[] = ["plain", "barrierDelete", "barrierAdd"];

async function shapedHistory(prng: PRNG, seed: number, variant: number): Promise<RSetHistory> {
    const flavor = FLAVORS[variant % FLAVORS.length];
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RCap.typeId, rCapFactory);
    ctx.getRegistry().register(RSet.typeId, rSetFactory);

    const [C, Rt, S, A, B1, B2] = [
        await makeIdentity(), await makeIdentity(), await makeIdentity(),
        await makeIdentity(), await makeIdentity(), await makeIdentity(),
    ];
    const cap = (await ctx.createObject(await RCap.create({
        seed: `fuzz-rset-shaped-cap-${seed}-${variant}`,
        creators: [{ keyId: C.keyId, publicKey: C.publicKey }],
        initialCaps: {
            root: { managedBy: ["creator"] },
            super: { managedBy: ["root"] },
            admin: { managedBy: ["super"] },
            write: { managedBy: ["admin"] },
        },
    }))) as RCap;
    const rset = (await ctx.createObject(await RSet.create({
        seed: `fuzz-rset-shaped-${seed}-${variant}`,
        initialElements: [],
        hashAlgorithm: "sha256",
        supportBarrierDelete: flavor === "barrierDelete",
        supportBarrierAdd: flavor === "barrierAdd",
        capabilityRef: cap.getId(),
        capRequirements: { add: "write", delete: "write" },
    }))) as RSet;
    const rawDag = (await ctx.getDag(rset.getId()))!;

    const newIdentity = async (at: B64Hash): Promise<B64Hash> => {
        const identity = await makeIdentity();
        return cap.addIdentity(identity.keyId, serializePublicKeyToBase64(identity.publicKey), C, version(at));
    };
    for (const identity of [Rt, S, A, B1, B2]) {
        await cap.addIdentity(identity.keyId, serializePublicKeyToBase64(identity.publicKey), C);
    }
    await cap.grant(Rt.keyId, "root", C);
    await cap.grant(S.keyId, "super", Rt);
    const gA = await cap.grant(A.keyId, "admin", S);
    const G2 = await cap.grant(B2.keyId, "write", C, version(gA));
    const G1 = await cap.grant(B1.keyId, "write", A, version(G2));
    const Tc = await cap.revoke(A.keyId, "admin", S, version(gA));
    const s1 = await newIdentity(G1);
    const t1 = await newIdentity(G1);
    const r = await cap.revoke(B1.keyId, "write", C, version(G1));
    const V = await cap.revoke(S.keyId, "super", Rt, version(s1));
    const W = await cap.revoke(Rt.keyId, "root", C, version(s1));

    const checkpoints: Version[] = [];
    const record = async (appended: Promise<B64Hash>): Promise<B64Hash> => {
        const hash = await appended;
        await recordCheckpoint(checkpoints, await rawDag.getFrontier());
        return hash;
    };
    const pick = <T>(choices: T[]): T => choices[prng.nextInt(0, choices.length - 1)];
    // A creator ref-advance at `at`, publishing its fold plus one of `choices`.
    const refAdvance = async (at: B64Hash, choices: B64Hash[]): Promise<B64Hash> => {
        const fold = await (await rset.getView(version(at), version(at))).resolveRefVersion(cap.getId());
        return record(rset.refAdvance(version(...fold, pick(choices)), C, version(at)));
    };
    const creatorAdd = (element: string, at: B64Hash): Promise<B64Hash> =>
        record(rset.addSigned(element, C, version(at)));

    const R0 = await record(rset.refAdvance(version(G1), C));

    let beforeX = R0;
    let X: B64Hash;
    const xKind = prng.next();
    if (xKind < 0.25) {
        beforeX = await creatorAdd("el-0", R0);
        X = await record(rset.deleteSigned("el-0", B1, version(beforeX)));
    } else {
        const author: OwnIdentity = xKind < 0.75 ? B1 : B2;
        X = await record(flavor === "barrierAdd" && prng.next() < 0.5
            ? rset.addWithBarrierSigned("el-1", author, version(beforeX))
            : rset.addSigned("el-1", author, version(beforeX)));
    }

    if (prng.next() < 0.8) {
        const T = await refAdvance(beforeX, [t1, s1, Tc]);
        if (prng.next() < 0.5) await creatorAdd("el-2", T);
    }

    let tip = X;
    if (prng.next() < 0.6) {
        const RM = await refAdvance(tip, [Tc, s1, t1]);
        tip = RM;
        if (prng.next() < 0.7) {
            const P = await creatorAdd("el-3", RM);
            await refAdvance(RM, [V, t1, Tc]);
            tip = P;
        }
    } else if (prng.next() < 0.5) {
        tip = await creatorAdd("el-3", X);
    }

    await refAdvance(tip, [r, W, V, s1]);

    return { rset, rawDag, checkpoints, seed };
}

// Several variants of the skeletons for this seed, cycling through the set
// flavors. `ops` is unused: the shapes are fixed.
export async function generateShapedRSetHistories(seed: number, _ops: number): Promise<RSetHistory[]> {
    const histories: RSetHistory[] = [];
    for (let variant = 0; variant < VARIANTS_PER_SEED; variant++) {
        // Nearby LCG seeds start out nearly equal: scramble, then discard a few.
        const prng = new PRNG(Math.imul(seed * 31 + variant, 0x9e3779b1));
        for (let i = 0; i < 4; i++) prng.next();
        histories.push(await shapedHistory(prng, seed, variant));
    }
    return histories;
}

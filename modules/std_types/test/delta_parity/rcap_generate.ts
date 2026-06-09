import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";

import { createMockRContext } from "../mock_rcontext.js";
import { RCap, rCapFactory } from "../../src/types/rcap/rcap.js";
import { serializePublicKeyToBase64 } from "../../src/authorship.js";

import { pickConcurrentAt, recordCheckpoint } from "./checkpoints.js";
import { makeIdentity } from "./identities.js";
import type { RCapHistory } from "./parity.js";
import { PRNG } from "./prng.js";
import { executeRCapOp, type RCapGeneratorState } from "./rcap_ops.js";

export async function generateRCapHistory(seed: number, ops: number): Promise<RCapHistory> {
    const prng = new PRNG(seed);
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RCap.typeId, rCapFactory);

    const admin = await makeIdentity();
    const extraCount = prng.nextInt(2, 4);
    const extras: OwnIdentity[] = [];
    for (let i = 0; i < extraCount; i++) {
        extras.push(await makeIdentity());
    }

    const init = await RCap.create({
        seed: `fuzz-rcap-${seed}`,
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        initialCaps: {
            admin: { managedBy: ["creator"] },
            write: { managedBy: ["admin"] },
        },
    });
    const cap = (await ctx.createObject(init)) as RCap;
    const rawDag = (await ctx.getDag(cap.getId()))!;

    const checkpoints: Version[] = [version(cap.getId())];
    const state: RCapGeneratorState = {
        registered: [admin, ...extras],
        dynamicCaps: [],
        nextCapIndex: 0,
    };

    for (const identity of extras) {
        await cap.addIdentity(
            identity.keyId,
            serializePublicKeyToBase64(identity.publicKey),
            admin,
        );
        await recordCheckpoint(checkpoints, await rawDag.getFrontier());
    }

    for (let opIndex = 0; opIndex < ops; opIndex++) {
        const at = pickConcurrentAt(prng, checkpoints);
        await executeRCapOp(cap, admin, rawDag, state, prng, at, false);
        await recordCheckpoint(checkpoints, await rawDag.getFrontier());
    }

    return { cap, rawDag, checkpoints, seed };
}

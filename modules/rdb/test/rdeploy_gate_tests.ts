import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version, Version, RContext } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import type { CreateRSchemaPayload } from "../src/rschema/payload.js";
import { RDeployGateImpl, rDeployGateFactory, ensureDeployGate } from "../src/rdeploy_gate/rdeploy_gate.js";
import { deployGateId, computeMirrorHashes, mirrorEntryHash, admitPayload } from "../src/rdeploy_gate/mirror.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function newCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RDeployGateImpl.typeId, rDeployGateFactory);
    return ctx;
}

async function frontierOf(obj: { getScopedDag(): Promise<{ getFrontier(): Promise<Version> }> }): Promise<Version> {
    return (await obj.getScopedDag()).getFrontier();
}

// A schema with two concurrent updates on its genesis (a, b) and a merge (m).
async function branchySchema(ctx: RContext, dev: OwnIdentity) {
    const init = await RSchemaImpl.create({
        name: 'gate:schema',
        creators: [{ keyId: dev.keyId, publicKey: dev.publicKey }],
        tables: [{ name: 'items', columns: { name: { type: 'string' } } }],
    });
    const schema = (await ctx.createObject(init)) as RSchemaImpl;
    const genesis = version(schema.getId());
    const a = await schema.updateSchema([{ rule: 'add-column', table: 'items', column: 'a', def: { type: 'string', nullable: true } }], dev, undefined, genesis);
    const b = await schema.updateSchema([{ rule: 'add-column', table: 'items', column: 'b', def: { type: 'string', nullable: true } }], dev, undefined, genesis);
    const m = await schema.updateSchema([{ rule: 'add-column', table: 'items', column: 'c', def: { type: 'string', nullable: true } }], dev);
    return { schema, init, a, b, m };
}

// Replays a schema's entries into another context, so both replicas share
// the exact same schema DAG.
async function copySchema(from: RSchemaImpl, init: CreateRSchemaPayload, ctx: RContext): Promise<RSchemaImpl> {
    const copy = (await ctx.createObject(init)) as RSchemaImpl;
    const target = await copy.getScopedDag();
    for await (const entry of (await from.getScopedDag()).loadAllEntries()) {
        if (entry.hash === from.getId()) continue;
        await target.append(entry.payload, entry.meta, new Set(json.fromSet(entry.header.prevEntryHashes)));
    }
    return copy;
}

export const rdeployGateTests = {
    title: '[GATE] RDeployGate tests',
    tests: [
        {
            name: '[GATE01] golden vectors: gate ids and mirror hashes',
            invoke: async () => {
                const gate = deployGateId('group-1', 'schema-1');
                assertEquals(gate, '+3O9VkWk+zYhjnTnf7TLoQt1h8z/XErQiTo6liuuEsY=', 'gate id vector');

                const headers: { [h: string]: string[] } = { g: [], a: ['g'], b: ['g'], m: ['a', 'b'] };
                const source = {
                    loadEntry: async (h: string) => headers[h] === undefined
                        ? undefined
                        : { header: { prevEntryHashes: json.toSet(headers[h]) } },
                };
                const memo = new Map<string, string>();
                const mirrors = await computeMirrorHashes(source, gate, ['m'], memo);
                assertEquals(mirrors.join(','), '/8/c1Ckp2z6mxtqeolWVM+J6apkC1+LEeJF2mC5HE3M=', 'merge mirror vector');
                assertEquals(memo.get('g'), '/WSb+oLwlmCcg0UzYQbTgx/uyZWfyUoJEELlT1Jys98=', 'genesis mirror vector');
                assertEquals(memo.get('a'), 'KwgDUFG78GttvXVLcxgrqyv1DDSHKKs0K73efV6hVqs=', 'branch a mirror vector');
                assertEquals(memo.get('b'), '4riWvoKZfuZUxFZzAR31T9480JeHUcUoUhaR5Ezs/OY=', 'branch b mirror vector');
                assertEquals(mirrorEntryHash('g', [gate]), memo.get('g'), 'the genesis mirror sits on the gate genesis');
            }
        },
        {
            name: '[GATE02] mirror hashes are identical across replicas and admission orders',
            invoke: async () => {
                const dev = await makeIdentity();
                const ctx1 = newCtx();
                const { schema, init, a, b, m } = await branchySchema(ctx1, dev);
                const ctx2 = newCtx();
                const schema2 = await copySchema(schema, init, ctx2);
                assertEquals([...(await frontierOf(schema2))].join(','), m, 'the copied schema has the same frontier');

                const gate1 = await ensureDeployGate(ctx1, 'group-x', schema.getId());
                const gate2 = await ensureDeployGate(ctx2, 'group-x', schema.getId());
                assertEquals(gate1.getId(), deployGateId('group-x', schema.getId()), 'the gate id is derived');
                assertEquals(gate1.getId(), gate2.getId(), 'both replicas derive the same gate id');

                await gate1.admit(version(a));
                await gate1.admit(version(b));
                await gate1.admit(version(m));
                await gate2.admit(version(m));

                const f1 = [...(await frontierOf(gate1))].sort().join(',');
                const f2 = [...(await frontierOf(gate2))].sort().join(',');
                assertEquals(f1, f2, 'the gate frontiers match');
                const expected = await computeMirrorHashes(await schema.getScopedDag(), gate1.getId(), [m]);
                assertEquals(f1, expected.join(','), 'the frontier is the computed mirror of the merge');
                assertEquals(f1.split(',').length, 1, 'the frontier mirrors the schema frontier: one entry');
            }
        },
        {
            name: '[GATE03] admit is incremental and idempotent; isAdmitted and the admitted frontier track it',
            invoke: async () => {
                const dev = await makeIdentity();
                const ctx = newCtx();
                const { schema, a, b, m } = await branchySchema(ctx, dev);
                const gate = await ensureDeployGate(ctx, 'group-y', schema.getId());

                assertEquals((await gate.getAdmittedFrontier()).size, 0, 'nothing is admitted at first');
                assertFalse(await gate.isAdmitted(version(a)), 'a is not admitted yet');

                const first = await gate.admit(version(a));
                assertEquals(first.length, 2, 'admitting a admits the genesis and a');
                assertTrue(await gate.isAdmitted(version(schema.getId(), a)), 'the genesis and a are admitted');
                assertFalse(await gate.isAdmitted(version(b)), 'b is not admitted');

                const second = await gate.admit(version(m));
                assertEquals(second.length, 2, 'admitting the merge only appends the delta: b and m');
                assertEquals((await gate.admit(version(m))).length, 0, 'admitting again appends nothing');
                assertEquals([...(await gate.getAdmittedFrontier())].join(','), m, 'the admitted frontier is the merge');
                assertEquals(await gate.mirrorOf(m), (await computeMirrorHashes(await schema.getScopedDag(), gate.getId(), [m]))[0],
                    'mirrorOf finds the canonical mirror');
            }
        },
        {
            name: '[GATE04] closure is enforced by append, and malformed admits are rejected',
            invoke: async () => {
                const dev = await makeIdentity();
                const ctx = newCtx();
                const { schema, a, b } = await branchySchema(ctx, dev);
                const gate = await ensureDeployGate(ctx, 'group-z', schema.getId());
                const gateDag = await gate.getScopedDag();

                const orphanPrev = mirrorEntryHash(schema.getId(), [gate.getId()]);
                let threw = false;
                try { await gateDag.append(admitPayload(a), {}, version(orphanPrev)); } catch { threw = true; }
                assertTrue(threw, 'an admit whose predecessor mirror is missing cannot be appended');

                const onGenesis = await gate.validatePayload(admitPayload(a), version(gate.getId()));
                assertFalse(onGenesis.valid, 'a non-genesis schema entry cannot be admitted on the gate genesis');

                await gate.admit(version(a));
                const wrongPrevs = await gate.validatePayload(admitPayload(b), version((await gate.mirrorOf(a))!));
                assertFalse(wrongPrevs.valid, 'an admit whose predecessors do not mirror its own is rejected');

                const unknown = await gate.validatePayload(admitPayload('no-such-entry'), version(gate.getId()));
                assertFalse(unknown.valid, 'an admit of a missing schema entry is rejected');

                const good = await gate.validatePayload(admitPayload(b), version((await gate.mirrorOf(schema.getId()))!));
                assertTrue(good.valid, 'b on the genesis mirror is valid');

                let noView = false;
                try { await gate.getView(); } catch { noView = true; }
                assertTrue(noView, 'the gate has no view');
            }
        },
    ],
};

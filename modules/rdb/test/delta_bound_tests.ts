// The bounded group delta on the histories where a bound taken at the fork
// meet misses a flip (INCREMENTAL_DELTA_BOUND.md), and on the side branches it
// must not reprocess. Each test checks the bounded delta against the full one,
// and computeRevisionBound against the delta's bound.

import { assertTrue, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import type { RTableGroupDelta } from "../src/rtable_group/delta.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { Predicate, TableDef } from "../src/rschema/payload.js";
import { assertGroupDeltaParity, assertRevisionBoundParity } from "./delta_parity/normalize.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

function newCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    return ctx;
}

function open(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

function gatedDelete(name: string, columns: TableDef['columns'], rule: Predicate): TableDef {
    return {
        name, columns,
        restrictions: [{ on: 'insert', rule: { p: 'true' } }, { on: 'update', rule: { p: 'true' } }, { on: 'delete', rule }],
        concurrentDeletes: true,
    };
}

async function createGroup(
    ctx: RContext, admin: OwnIdentity, name: string, tables: TableDef[], bindings?: { [name: string]: B64Hash },
): Promise<{ group: RTableGroupImpl; schema: RSchemaImpl; pinned: Version }> {
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: `dbound:${name.replace(/-/g, '_')}_schema`,
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables,
    }))) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();
    const group = (await ctx.createObject(await RTableGroupImpl.create({
        name, seed: name,
        schemaRef: schema.getId(), schemaVersion: pinned,
        ...(bindings !== undefined ? { bindings } : {}),
    }))) as RTableGroupImpl;
    return { group, schema, pinned };
}

async function makeAdmin(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// The bounded delta, after checking it against the full delta and the
// bound-only computation.
async function boundedDelta(group: RTableGroupImpl, start: Version, end: Version): Promise<RTableGroupDelta> {
    group.setDeltaStrategy('full');
    const full = await group.computeDelta(start, end);
    group.setDeltaStrategy('bounded');
    const bounded = await group.computeDelta(start, end);
    assertGroupDeltaParity(bounded, full, { seed: 0, start, end });
    assertRevisionBoundParity(await group.computeRevisionBound(start, end), bounded, { seed: 0, start, end });
    return bounded;
}

function hasFlip(delta: RTableGroupDelta, entry: B64Hash): boolean {
    return delta.opVerdictChanges.some((c) => c.entry === entry);
}

function isExactly(v: Version, ...hashes: B64Hash[]): boolean {
    return v.size === hashes.length && hashes.every((h) => v.has(h));
}

// x inserts a target, E an item pointing at it, s1 follows E. T deletes x
// (rule: some perm row is 'ok'), concurrent with E and s1. N follows s1 and
// either deletes the 'ok' perm row, which voids T and so revives E, or is a
// note that changes nothing.
async function localFloorHistory(name: string, nDeletesPerm: boolean) {
    const ctx = newCtx();
    const { group } = await createGroup(ctx, await makeAdmin(), name, [
        open('perm', { label: { type: 'string', pub: true } }, { concurrentDeletes: true }),
        gatedDelete('targets', { n: { type: 'string' } }, { p: 'exists', table: 'perm', where: { label: 'ok' } }),
        open('items', { t: { type: 'string' } }, { fks: { t: 'targets' } }),
        open('notes', { n: { type: 'string' } }),
    ]);
    const perm = await group.getTable('perm');
    const targets = await group.getTable('targets');
    const items = await group.getTable('items');
    const notes = await group.getTable('notes');

    const p = await perm.insert('p-1', { label: 'ok' });
    const x = await targets.insert('x-1', { n: 'x' }, undefined, version(p));
    const E = await items.insert('i-1', { t: deriveRowId('x-1') }, undefined, version(x));
    const s1 = await notes.insert('n-1', { n: 's1' }, undefined, version(E));
    const T = await targets.delete(deriveRowId('x-1'), undefined, version(x));
    const N = nDeletesPerm
        ? await perm.delete(deriveRowId('p-1'), undefined, version(s1))
        : await notes.insert('n-2', { n: 'N' }, undefined, version(s1));
    return { group, x, E, s1, T, start: version(s1, T), end: version(N, T) };
}

export const deltaBoundTests = {
    title: '[DELTA_BOUND] bounded group delta: widening bound on side-branch barriers',
    tests: [
        {
            name: '[DBOUND01] a side-branch delete voided by a new entry lowers the bound and the item it revives is reported',
            invoke: async () => {
                const h = await localFloorHistory('dbound-local', true);
                const delta = await boundedDelta(h.group, h.start, h.end);
                assertTrue(hasFlip(delta, h.T), 'the delete flips to void');
                assertTrue(hasFlip(delta, h.E), 'the item insert flips to live');
                assertTrue(isExactly(delta.revisionBound, h.x), 'the bound drops to the delete\'s predecessor');
            },
        },
        {
            name: '[DBOUND02] a side-branch delete that does not change keeps the bound at the meet',
            invoke: async () => {
                const h = await localFloorHistory('dbound-unflipped', false);
                const delta = await boundedDelta(h.group, h.start, h.end);
                assertEquals(delta.opVerdictChanges.length, 0, 'nothing flips');
                assertTrue(isExactly(delta.revisionBound, h.s1), 'the bound stays at the meet');
            },
        },
        {
            name: '[DBOUND03] a live side-branch observe above the foreign bound lowers the bound (cross-group)',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeAdmin();
                const { group: users } = await createGroup(ctx, admin, 'dbound-observe-users', [
                    open('caps', { label: { type: 'string', pub: true } }, { concurrentDeletes: true }),
                    open('fill', { n: { type: 'string' } }),
                ]);
                const caps = await users.getTable('caps');
                const fill = await users.getTable('fill');
                const grant = await caps.insert('c-1', { label: 'grant' });
                const filled = await fill.insert('w', { n: 'w' }, undefined, version(grant));
                const revoke = await caps.delete(deriveRowId('c-1'), undefined, version(grant));

                const { group } = await createGroup(ctx, admin, 'dbound-observe', [
                    {
                        name: 'items', columns: { n: { type: 'string' } },
                        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'users.caps', where: { label: 'grant' } } }],
                    },
                    open('notes', { n: { type: 'string' } }),
                ], { users: users.getId() });
                const items = await group.getTable('items');
                const notes = await group.getTable('notes');
                const A = await group.observe('users', version(grant));
                const E = await items.insert('i-1', { n: 'item' }, undefined, version(A));
                const s1 = await notes.insert('n-1', { n: 's1' }, undefined, version(E));
                const W = await group.observe('users', version(filled), undefined, version(A));
                const Z = await group.observe('users', version(revoke), undefined, version(s1));

                const start = version(s1, W);
                const end = version(Z, W);
                assertTrue(!await group.isEntryVoided(E, start) && await group.isEntryVoided(E, end), 'E flips from live to void');
                const delta = await boundedDelta(group, start, end);
                assertTrue(hasFlip(delta, E), 'the item insert is reported');
                assertTrue(!hasFlip(delta, W), 'the side-branch observe stays live');
            },
        },
        {
            name: '[DBOUND04] the foreign bound is taken from the version observed at start, not at the meet (cross-group)',
            invoke: async () => {
                // F: T_F deletes x (rule: an 'ok' perm row); N1 deletes that perm
                // row (rule: a gate row); N3 deletes the gate row. T_F is live at
                // v1, void at v2 and live again at v3, and so is the item E_F
                // inserted pointing at x, the other way around.
                const ctx = newCtx();
                const admin = await makeAdmin();
                const { group: F } = await createGroup(ctx, admin, 'dbound-intermediate-f', [
                    gatedDelete('perm', { label: { type: 'string', pub: true } }, { p: 'exists', table: 'gate', where: { k: 'g' } }),
                    open('gate', { k: { type: 'string', pub: true } }, { concurrentDeletes: true }),
                    gatedDelete('targets', { n: { type: 'string' } }, { p: 'exists', table: 'perm', where: { label: 'ok' } }),
                    open('items', { t: { type: 'string' } }, { fks: { t: 'targets' } }),
                    open('notes', { n: { type: 'string' } }),
                ]);
                const perm = await F.getTable('perm');
                const gate = await F.getTable('gate');
                const targets = await F.getTable('targets');
                const fItems = await F.getTable('items');
                const fNotes = await F.getTable('notes');
                const p = await perm.insert('p-1', { label: 'ok' });
                const g = await gate.insert('g-1', { k: 'g' }, undefined, version(p));
                const x = await targets.insert('x-1', { n: 'x' }, undefined, version(g));
                const EF = await fItems.insert('i-1', { t: deriveRowId('x-1') }, undefined, version(x));
                const fs1 = await fNotes.insert('n-1', { n: 's1' }, undefined, version(EF));
                const TF = await targets.delete(deriveRowId('x-1'), undefined, version(x));
                const N1 = await perm.delete(deriveRowId('p-1'), undefined, version(fs1));
                const N3 = await gate.delete(deriveRowId('g-1'), undefined, version(fs1));
                const v1 = version(fs1, TF);
                const v2 = version(N1, TF);
                const v3 = version(N1, N3, TF);
                assertTrue(await F.isEntryVoided(EF, v1) && !await F.isEntryVoided(EF, v2) && await F.isEntryVoided(EF, v3),
                    'the foreign item is void, live, then void');

                // G: E_G references the foreign item. O_m (in the meet) observes
                // v1, the side branch O2 observes v2, the new O3 observes v3.
                const { group: G } = await createGroup(ctx, admin, 'dbound-intermediate-g', [
                    open('refs', { item: { type: 'string' } }, { fks: { item: 'f.items' } }),
                    open('notes', { n: { type: 'string' } }),
                ], { f: F.getId() });
                const refs = await G.getTable('refs');
                const gNotes = await G.getTable('notes');
                const O1 = await G.observe('f', version(fs1));
                const EG = await refs.insert('r-1', { item: deriveRowId('i-1') }, undefined, version(O1));
                const Om = await G.observe('f', v1, undefined, version(EG));
                const s1G = await gNotes.insert('n-1', { n: 's1G' }, undefined, version(Om));
                const O2 = await G.observe('f', v2, undefined, version(EG));
                const O3 = await G.observe('f', v3, undefined, version(s1G));

                const start = version(s1G, O2);
                const end = version(O3, O2);
                assertTrue(!await G.isEntryVoided(EG, start) && await G.isEntryVoided(EG, end), 'E_G flips from live to void');
                const delta = await boundedDelta(G, start, end);
                assertTrue(hasFlip(delta, EG), 'the ref insert is reported');
                assertTrue(isExactly(delta.revisionBound, G.getId()), 'the bound drops below the first observe');
            },
        },
        {
            name: '[DBOUND05] a side-branch deploy that deletes rows lowers the bound when a new deploy flips its concurrentDeletes',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeAdmin();
                const { group, schema, pinned } = await createGroup(ctx, admin, 'dbound-deploy-kill', [
                    open('items', { t: { type: 'string' } }, { concurrentDeletes: true }),
                    open('targets', { n: { type: 'string' } }),
                    open('refs', { item: { type: 'string' } }, { fks: { item: 'items' } }),
                    open('notes', { n: { type: 'string' } }),
                ]);
                const items = await group.getTable('items');
                const refs = await group.getTable('refs');
                const notes = await group.getTable('notes');
                const adopt = await schema.updateSchema([{ rule: 'set-fks', table: 'items', fks: { t: 'targets' } }], admin, 'adopt fk', pinned);
                const cdOff = await schema.updateSchema([{ rule: 'set-concurrent-deletes', table: 'items', value: false }], admin, 'cd off', version(adopt));

                // R points at no target, so a deploy adopting the FK deletes it.
                // D2 (in the meet) and D (side branch) both deploy the adoption;
                // N stops honoring concurrent deletes on items.
                const R = await items.insert('r-1', { t: 'missing' });
                const E = await refs.insert('e-1', { item: deriveRowId('r-1') }, undefined, version(R));
                const D2 = await group.deploy(version(adopt), undefined, version(E));
                const s1 = await notes.insert('n-1', { n: 's1' }, undefined, version(D2));
                const D = await group.deploy(version(adopt), undefined, version(R));
                const N = await group.deploy(version(cdOff), undefined, version(s1));

                const start = version(s1, D);
                const end = version(N, D);
                assertTrue(await group.isEntryVoided(E, start) && !await group.isEntryVoided(E, end), 'E flips from void to live');
                const delta = await boundedDelta(group, start, end);
                assertTrue(hasFlip(delta, E), 'the ref insert is reported');
                assertTrue(isExactly(delta.revisionBound, R), 'the bound drops to the deploy\'s predecessor');
            },
        },
        {
            name: '[DBOUND06] a side-branch schema deploy with no new deploy keeps the bound at the meet',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeAdmin();
                const { group, schema, pinned } = await createGroup(ctx, admin, 'dbound-side-deploy', [
                    open('items', { t: { type: 'string' } }),
                    open('notes', { n: { type: 'string' } }),
                ]);
                const items = await group.getTable('items');
                const notes = await group.getTable('notes');
                const addColumn = await schema.updateSchema(
                    [{ rule: 'add-column', table: 'notes', column: 'extra', def: { type: 'string', default: 'd' } }],
                    admin, 'add column', pinned);

                const a = await items.insert('a-1', { t: 'a' });
                const E = await items.insert('e-1', { t: 'e' }, undefined, version(a));
                const s1 = await notes.insert('n-1', { n: 's1' }, undefined, version(E));
                const D = await group.deploy(version(addColumn), undefined, version(a));
                const N = await notes.insert('n-2', { n: 'N' }, undefined, version(s1));

                const delta = await boundedDelta(group, version(s1, D), version(N, D));
                assertTrue(isExactly(delta.revisionBound, s1), 'the bound stays at the meet');
            },
        },
    ],
};

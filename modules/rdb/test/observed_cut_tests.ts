// Revocation across multi-head observed cuts. A barrier is concurrent with a
// cut unless it is after every head (ideal concurrency).

import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";
import { version, signPayload, createRefAdvancePayload, extractRefVersion, formatValidationFailure } from "@hyper-hyper-space/hhs3_mvt";
import { json } from "@hyper-hyper-space/hhs3_json";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { Predicate, TableDef } from "../src/rschema/payload.js";
import {
    usersSchemaTables, identityRow, capRow, revokeCap, registerIdentity,
    IDENTITIES_TABLE, CAPS_TABLE, USERS_MANAGER_LABEL, USERS_SCHEMA_NAME,
    USERS_BINDING, USERS_IDENTITIES_PROVIDER,
} from "../src/users/users.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function newCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    return ctx;
}

async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

function schemaName(seed: string): string {
    return seed.replace(/[^a-zA-Z0-9_]+/g, '_') + '_schema';
}

const MUST_BE_MANAGER: Predicate = {
    p: 'exists', table: CAPS_TABLE, where: { label: USERS_MANAGER_LABEL, grantee: '$author' },
};

// A Users group with every member as a genesis identity and manager.
async function makeUsers(ctx: RContext, seed: string, members: OwnIdentity[]): Promise<RTableGroupImpl> {
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: USERS_SCHEMA_NAME,
        creators: [{ keyId: members[0].keyId, publicKey: members[0].publicKey }],
        tables: usersSchemaTables(),
    }))) as RSchemaImpl;
    return (await ctx.createObject(await RTableGroupImpl.create({
        name: seed, seed: seed + '-group',
        schemaRef: schema.getId(), schemaVersion: await (await schema.getScopedDag()).getFrontier(),
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: members.map((m, i) => identityRow(seed + '-id-' + i, m)),
            [CAPS_TABLE]: members.map((m, i) => capRow(seed + '-cap-' + i, m.keyId, USERS_MANAGER_LABEL)),
        },
    }))) as RTableGroupImpl;
}

// An app group whose observation of `users` requires a live manager cap there.
async function makeGatedApp(ctx: RContext, seed: string, usersId: B64Hash, gated = true): Promise<RTableGroupImpl> {
    const creator = await makeIdentity();
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: schemaName(seed),
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: [{ name: 'notes', columns: { body: { type: 'string' } }, restrictions: [{ on: 'all', rule: { p: 'true' } }] }],
    }))) as RSchemaImpl;
    return (await ctx.createObject(await RTableGroupImpl.create({
        name: seed, seed: seed + '-group',
        schemaRef: schema.getId(), schemaVersion: await (await schema.getScopedDag()).getFrontier(),
        bindings: { [USERS_BINDING]: usersId },
        idProvider: USERS_IDENTITIES_PROVIDER,
        ...(gated ? { canObserve: { [USERS_BINDING]: MUST_BE_MANAGER } } : {}),
    }))) as RTableGroupImpl;
}

// An unauthored identity registration: a filler entry anchored at `at`.
async function filler(group: RTableGroupImpl, at: Version): Promise<B64Hash> {
    return registerIdentity(group, await makeIdentity(), undefined, at);
}

type ConcurrentImportCut = 'v1' | 'mixed' | 'dominated';

// G: v0 -> v1 -> r (admin revokes alice), v0 -> vAdv, v1 -> v2.
// O: alice observes the chosen cut and admin imports {r}, concurrently at base.
async function observeAgainstConcurrentImport(seed: string, cut: ConcurrentImportCut): Promise<{ alice: boolean; admin: boolean }> {
    const ctx = newCtx();
    const admin = await makeIdentity();
    const alice = await makeIdentity();
    const G = await makeUsers(ctx, seed + '-g', [admin, alice]);
    const v0 = await frontier(G);
    const v1 = await filler(G, v0);
    const vAdv = await filler(G, v0);
    const v2 = await filler(G, version(v1));
    const r = (await revokeCap(G, admin, alice.keyId, USERS_MANAGER_LABEL, version(v1)))!;

    const O = await makeGatedApp(ctx, seed + '-o', G.getId());
    const base = await frontier(O);
    const aliceCut = cut === 'v1' ? version(v1) : cut === 'mixed' ? version(v1, vAdv) : version(v1, v2);
    const y = await O.observe(USERS_BINDING, aliceCut, alice, base);
    const z = await O.observe(USERS_BINDING, version(r), admin, base);

    const merged = await frontier(O);
    return { alice: !(await O.isEntryVoided(y, merged)), admin: !(await O.isEntryVoided(z, merged)) };
}

// G: v0 -> f1 -> r (admin revokes alice), v0 -> f2 -> w.
// O: alice observes {f2}; optionally bob concurrently observes {w, f1}; admin
// then imports {r, w} causally after both.
async function concurrentObserveLaterImport(seed: string, withBob: boolean): Promise<boolean> {
    const ctx = newCtx();
    const admin = await makeIdentity();
    const alice = await makeIdentity();
    const bob = await makeIdentity();
    const G = await makeUsers(ctx, seed + '-g', [admin, alice, bob]);
    const v0 = await frontier(G);
    const f1 = await filler(G, v0);
    const f2 = await filler(G, v0);
    const r = (await revokeCap(G, admin, alice.keyId, USERS_MANAGER_LABEL, version(f1)))!;
    const w = await filler(G, version(f2));

    const O = await makeGatedApp(ctx, seed + '-o', G.getId());
    const base = await frontier(O);
    const y = await O.observe(USERS_BINDING, version(f2), alice, base);
    const before = [y];
    if (withBob) before.push(await O.observe(USERS_BINDING, version(w, f1), bob, base));
    await O.observe(USERS_BINDING, version(r, w), admin, version(...before));

    return !(await O.isEntryVoided(y, await frontier(O)));
}

// G: v0 -> v1 -> v2, v1 -> r (admin revokes alice).
// O: alice observes {v1}; optionally bob concurrently observes {v2}; admin then
// imports {r, v2} causally after both.
async function earlierObserveLaterImport(seed: string, withBob: boolean): Promise<boolean> {
    const ctx = newCtx();
    const admin = await makeIdentity();
    const alice = await makeIdentity();
    const bob = await makeIdentity();
    const G = await makeUsers(ctx, seed + '-g', [admin, alice, bob]);
    const v0 = await frontier(G);
    const v1 = await filler(G, v0);
    const v2 = await filler(G, version(v1));
    const r = (await revokeCap(G, admin, alice.keyId, USERS_MANAGER_LABEL, version(v1)))!;

    const O = await makeGatedApp(ctx, seed + '-o', G.getId());
    const base = await frontier(O);
    const y = await O.observe(USERS_BINDING, version(v1), alice, base);
    const before = [y];
    if (withBob) before.push(await O.observe(USERS_BINDING, version(v2), bob, base));
    await O.observe(USERS_BINDING, version(r, v2), admin, version(...before));

    return !(await O.isEntryVoided(y, await frontier(O)));
}

function open(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

async function makeGroup(ctx: RContext, seed: string, tables: TableDef[], bindings?: { [name: string]: B64Hash }): Promise<RTableGroupImpl> {
    const creator = await makeIdentity();
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: schemaName(seed),
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables,
    }))) as RSchemaImpl;
    return (await ctx.createObject(await RTableGroupImpl.create({
        name: seed, seed: seed + '-group',
        schemaRef: schema.getId(), schemaVersion: await (await schema.getScopedDag()).getFrontier(),
        ...(bindings !== undefined ? { bindings } : {}),
    }))) as RTableGroupImpl;
}

type ItemCut = 'f1' | 'f2' | 'padded' | 'bob-widens' | 'dominated';

// G: g (grant witness) -> f1 -> d (deletes the witness), g -> f2 -> w, f1 -> f3.
// O (ungated binding): an observe of the chosen cut, an item that needs the
// witness on top of it, then an import of {d, w, f3} causally after the item.
// 'bob-widens' observes {f2} and adds a concurrent observe of {w, f1};
// 'dominated' observes {f1} and adds a concurrent observe of {f3}.
async function itemLaterImport(seed: string, cut: ItemCut): Promise<boolean> {
    const ctx = newCtx();
    const G = await makeGroup(ctx, seed + '-g', [
        open('caps', { label: { type: 'string', pub: true } }, { concurrentDeletes: true }),
        open('fill', { n: { type: 'string' } }),
    ]);
    const O = await makeGroup(ctx, seed + '-o', [{
        name: 'items', columns: { name: { type: 'string' } },
        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'users.caps', where: { label: 'grant' } } }],
    }], { users: G.getId() });

    const caps = await G.getTable('caps');
    const fill = await G.getTable('fill');
    const g = await caps.insert('c-1', { label: 'grant' });
    const f1 = await fill.insert('f1', { n: '1' }, undefined, version(g));
    const f2 = await fill.insert('f2', { n: '2' }, undefined, version(g));
    const f3 = await fill.insert('f3', { n: '3' }, undefined, version(f1));
    const d = await caps.delete(deriveRowId('c-1'), undefined, version(f1));
    const w = await fill.insert('w', { n: 'w' }, undefined, version(f2));

    const base = await frontier(O);
    const observed = cut === 'f2' || cut === 'bob-widens' ? version(f2)
        : cut === 'f1' || cut === 'dominated' ? version(f1)
        : version(f2, f1);
    const y = await O.observe('users', observed, undefined, base);
    const item = await (await O.getTable('items')).insert('i-1', { name: 'thing' }, undefined, version(y));
    const before = [item];
    if (cut === 'bob-widens') before.push(await O.observe('users', version(w, f1), undefined, base));
    if (cut === 'dominated') before.push(await O.observe('users', version(f3), undefined, base));
    await O.observe('users', version(d, w, f3), undefined, version(...before));

    const merged = await frontier(O);
    const view = await (await O.getView(merged, merged)).getTableView('items');
    return view.hasRow(deriveRowId('i-1'));
}

// G: g (grant) -> f1, g -> f2, g -> c' (a second caps row, concurrent with f2).
// The witness delete d is anchored at {f1, c'}. O observes {f2, c'}, inserts
// the item, then imports {d, w} where w is after f2. c' is the only caps head
// of the cut; f2 is not. d has seen c' and has not seen f2, so it is
// concurrent with the cut.
async function itemAtMixedCapsCut(seed: string): Promise<boolean> {
    const ctx = newCtx();
    const G = await makeGroup(ctx, seed + '-g', [
        open('caps', { label: { type: 'string', pub: true } }, { concurrentDeletes: true }),
        open('fill', { n: { type: 'string' } }),
    ]);
    const O = await makeGroup(ctx, seed + '-o', [{
        name: 'items', columns: { name: { type: 'string' } },
        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'users.caps', where: { label: 'grant' } } }],
    }], { users: G.getId() });

    const caps = await G.getTable('caps');
    const fill = await G.getTable('fill');
    const g = await caps.insert('c-1', { label: 'grant' });
    const cPrime = await caps.insert('c-2', { label: 'other' }, undefined, version(g));
    const f1 = await fill.insert('f1', { n: '1' }, undefined, version(g));
    const f2 = await fill.insert('f2', { n: '2' }, undefined, version(g));
    const d = await caps.delete(deriveRowId('c-1'), undefined, version(f1, cPrime));
    // w is after f2, so {d, w} dominates the observed cut {f2, c'} and the
    // later import stays monotonic. d itself has not seen f2.
    const w = await fill.insert('w', { n: 'w' }, undefined, version(f2));

    const base = await frontier(O);
    const y = await O.observe('users', version(f2, cPrime), undefined, base);
    const item = await (await O.getTable('items')).insert('i-1', { name: 'thing' }, undefined, version(y));
    await O.observe('users', version(d, w), undefined, version(item));

    const merged = await frontier(O);
    const view = await (await O.getView(merged, merged)).getTableView('items');
    return view.hasRow(deriveRowId('i-1'));
}

export const observedCutTests = {
    title: '[OCUT] Revocation across multi-head observed cuts',
    tests: [
        {
            name: '[OCUT01] a back-dated observe of {v1} is voided by a concurrent import of a revoke above v1',
            invoke: async () => {
                const { alice, admin } = await observeAgainstConcurrentImport('ocut01', 'v1');
                assertFalse(alice, 'the concurrent import strictly dominates {v1}, widens it to the revoke and voids it');
                assertTrue(admin, 'the revoke-import stays live');
            },
        },
        {
            name: '[OCUT02] a mixed-cut observe {v1, vAdv} is voided by a concurrent import of a revoke above v1',
            invoke: async () => {
                const { alice, admin } = await observeAgainstConcurrentImport('ocut02', 'mixed');
                assertFalse(alice, 'the revoke is not after vAdv, so it is concurrent with the cut and voids the observe');
                assertTrue(admin, 'the revoke-import stays live');
            },
        },
        {
            name: '[OCUT03] an observe whose refVersion carries a dominated ancestor {v1, v2} is voided by a concurrent revoke-import',
            invoke: async () => {
                const { alice, admin } = await observeAgainstConcurrentImport('ocut03', 'dominated');
                assertFalse(alice, 'concurrency depends on the causal past, so the dominated v1 does not hide r');
                assertTrue(admin, 'the revoke-import stays live');
            },
        },
        {
            name: '[OCUT04] an observe of {f2} concurrent with the revoke is voided by a later revoke-import',
            invoke: async () => {
                assertFalse(await concurrentObserveLaterImport('ocut04', false),
                    'the revoke is concurrent with f2 and visible from the frontier, so Step 2 voids the observe');
            },
        },
        {
            name: '[OCUT05] a benign concurrent observe with a head below the revoke does not rescue the observe of {f2}',
            invoke: async () => {
                assertFalse(await concurrentObserveLaterImport('ocut05', true),
                    "bob's head below the revoke does not make the revoke later than the widened cut");
            },
        },
        {
            name: '[OCUT06] use-before-revoke: an observe of {v1} below the revoke survives a later revoke-import',
            invoke: async () => {
                assertTrue(await earlierObserveLaterImport('ocut06', false),
                    'the revoke is causally after the whole observed cut and imported after the observe');
            },
        },
        {
            name: '[OCUT07] widened to {v1, v2} by a benign observe, an observe of {v1} is voided by a later import of a revoke concurrent with v2',
            invoke: async () => {
                assertFalse(await earlierObserveLaterImport('ocut07', true),
                    'the widened cut is the head v2, and the revoke is concurrent with v2');
            },
        },
        {
            name: '[OCUT08] use-before-revoke: an item at {f1} below the delete survives a later import of the delete',
            invoke: async () => {
                assertTrue(await itemLaterImport('ocut08', 'f1'),
                    'the delete is causally after the whole observed cut and imported after the item');
            },
        },
        {
            name: '[OCUT09] an item at {f2} concurrent with the delete is voided by a later import of the delete',
            invoke: async () => {
                assertFalse(await itemLaterImport('ocut09', 'f2'),
                    'the delete is concurrent with f2 and visible from the frontier, so Step 2 kills the witness');
            },
        },
        {
            name: '[OCUT10] padding the observed cut to {f2, f1} does not save the item from a later import of the delete',
            invoke: async () => {
                assertFalse(await itemLaterImport('ocut10', 'padded'),
                    'the delete is not after f2, so it is concurrent with the padded cut');
            },
        },
        {
            name: '[OCUT11] a benign concurrent observe of {w, f1} does not rescue the item at {f2}',
            invoke: async () => {
                assertFalse(await itemLaterImport('ocut11', 'bob-widens'),
                    'adding f1 below the delete does not make the delete later than the widened cut');
            },
        },
        {
            name: '[OCUT13] a refVersion carrying a dominated ancestor is canonicalized on write and rejected on admission',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const G = await makeUsers(ctx, 'ocut13-g', [admin]);
                const v0 = await frontier(G);
                const v1 = await filler(G, v0);
                const v2 = await filler(G, version(v1));

                const gated = await makeGatedApp(ctx, 'ocut13-gated', G.getId());
                const at = await frontier(gated);
                const raw = await signPayload(
                    createRefAdvancePayload(G.getId(), version(v1, v2)) as unknown as json.LiteralMap, admin, at,
                );
                const rejected = await gated.validatePayload(raw, at);
                assertTrue(!rejected.valid && formatValidationFailure(rejected.why).includes('not an antichain'),
                    'a gated observe carrying a dominated ancestor is rejected');

                const open = await makeGatedApp(ctx, 'ocut13-open', G.getId(), false);
                const openAt = await frontier(open);
                const openRaw = createRefAdvancePayload(G.getId(), version(v1, v2));
                const openRejected = await open.validatePayload(openRaw as unknown as json.LiteralMap, openAt);
                assertTrue(!openRejected.valid && formatValidationFailure(openRejected.why).includes('not an antichain'),
                    'an ungated observe carrying a dominated ancestor is rejected');

                const written = await open.observe(USERS_BINDING, version(v1, v2), undefined, openAt);
                const stored = extractRefVersion((await (await open.getScopedDag()).loadEntry(written))!.payload as never);
                assertTrue(stored.size === 1 && stored.has(v2), 'the writer publishes the minimal cover');
            },
        },
        {
            name: '[OCUT14] an item at {f2, c\'} is voided by a later import of a witness delete that has seen c\' but not f2',
            invoke: async () => {
                assertFalse(await itemAtMixedCapsCut('ocut14'),
                    'the delete is not after f2, so it is concurrent with the cut and voids the item');
            },
        },
        {
            name: '[OCUT12] an item at {f1} widened by a concurrent observe of {f3} above f1 is voided by a later import of a delete concurrent with f3',
            invoke: async () => {
                assertFalse(await itemLaterImport('ocut12', 'dominated'),
                    'the widened cut is the head f3, and the delete is concurrent with f3');
            },
        },
    ],
};

// HIP-29's two steps of bundle and observe, and the second validation of a
// local op through the live observes.
//
// The void scenario: K holds a grant for Bob. G binds K behind a gate that
// lets only G's admins observe it. Mallory, an admin, writes the only observe
// that brings in Bob's grant, and concurrently the other admin, Alice,
// removes her from the admins, so her observe is void. A note in G needs its
// author to hold a grant in K: by a rule, or by a foreign key into the grant.
//
// The gate graph: G binds K and M, and M's gate reads K. Bob granted Alice a
// reader cap in K, and concurrently root revoked Bob's manager cap, which
// voids the grant. Alice observed M after G observed K at the grant.

import { assertTrue, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";
import { ValidationRejectedError } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { PreparedHashMismatchError } from "../src/rtable_group/interfaces.js";
import type { BundleWrite } from "../src/rtable_group/interfaces.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { Predicate, TableDef } from "../src/rschema/payload.js";
import {
    usersSchemaTables, identityRow, capRow, grantCap, revokeCap, IDENTITIES_TABLE, CAPS_TABLE, USERS_MANAGER_LABEL,
} from "../src/users/users.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function newCtx(selfValidate = true): RContext {
    const ctx = createMockRContext({ selfValidate });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    return ctx;
}

async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

async function rejected(fn: () => Promise<unknown>): Promise<boolean> {
    try { await fn(); } catch (e) { return e instanceof ValidationRejectedError; }
    return false;
}

const OPEN: Predicate = { p: 'true' };

function openTable(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: OPEN }], ...extra };
}

async function makeGroup(ctx: RContext, name: string, creator: OwnIdentity, tables: TableDef[], opts: Partial<Parameters<typeof RTableGroupImpl.create>[0]> = {}): Promise<RTableGroupImpl> {
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: `${name.replace(/[^a-zA-Z0-9_]+/g, '_')}_schema`, creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }], tables,
    }))) as RSchemaImpl;
    return (await ctx.createObject(await RTableGroupImpl.create({
        name, seed: `${name}-seed`,
        schemaRef: schema.getId(), schemaVersion: await (await schema.getScopedDag()).getFrontier(),
        ...opts,
    }))) as RTableGroupImpl;
}

const IN_ADMINS: Predicate = { p: 'exists', table: 'admins', where: { key: '$author' } };

type VoidScenario = {
    K: RTableGroupImpl; G: RTableGroupImpl; mallory: OwnIdentity; alice: OwnIdentity; bob: OwnIdentity;
    kGrant: Version; g0: Version; malloryObserve: B64Hash; removal: B64Hash; voidAt: Version;
    note: (uuid: string) => BundleWrite;
};

async function voidScenario(form: 'rule' | 'fk', selfValidate = true): Promise<VoidScenario> {
    const ctx = newCtx(selfValidate);
    const [mallory, alice, bob] = [await makeIdentity(), await makeIdentity(), await makeIdentity()];
    const K = await makeGroup(ctx, `k_${form}`, alice, [openTable('grants', { holder: { type: 'string', pub: true } })]);
    await (await K.getTable('grants')).insert('grant', { holder: bob.keyId });
    const grantRow = deriveRowId('grant');
    const kGrant = await frontier(K);

    const notes: TableDef = form === 'rule'
        ? { name: 'notes', columns: { body: { type: 'string' } },
            restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'k.grants', where: { holder: '$author' } } }] }
        : openTable('notes', { body: { type: 'string' }, grant: { type: 'string' } }, { fks: { grant: 'k.grants' } });
    const admins: TableDef = {
        name: 'admins', columns: { key: { type: 'string', pub: true } },
        concurrentDeletes: true, restrictions: [{ on: 'all', rule: IN_ADMINS }],
    };
    const G = await makeGroup(ctx, `g_${form}`, alice, [usersSchemaTables().find((t) => t.name === IDENTITIES_TABLE)!, admins, notes], {
        bindings: { k: K.getId() }, canObserve: { k: IN_ADMINS }, idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: [identityRow('mallory', mallory), identityRow('alice', alice), identityRow('bob', bob)],
            admins: [mallory, alice].map((who, i) => ({
                action: 'insert' as const, rowId: deriveRowId(`admin-${i}`), uuid: `admin-${i}`, values: { key: who.keyId },
            })),
        },
    });
    const g0 = await frontier(G);
    const malloryObserve = await G.observe('k', kGrant, mallory, g0);
    const removal = await (await G.getTable('admins')).delete(deriveRowId('admin-0'), alice, g0);
    const note = (uuid: string): BundleWrite => ({
        table: 'notes',
        op: { action: 'insert', rowId: deriveRowId(uuid, bob.keyId), uuid, values: form === 'rule' ? { body: 'hi' } : { body: 'hi', grant: grantRow } },
    });
    return { K, G, mallory, alice, bob, kGrant, g0, malloryObserve, removal, voidAt: new Set([malloryObserve, removal]), note };
}

type GateGraph = {
    K: RTableGroupImpl; M: RTableGroupImpl; G: RTableGroupImpl; alice: OwnIdentity;
    kRevoked: Version; observeOfM: B64Hash; at: Version;
};

async function gateGraph(): Promise<GateGraph> {
    const ctx = newCtx();
    const [root, bob, alice] = [await makeIdentity(), await makeIdentity(), await makeIdentity()];
    const kSchema = (await ctx.createObject(await RSchemaImpl.create({
        name: 'gate_k_users', creators: [{ keyId: root.keyId, publicKey: root.publicKey }], tables: usersSchemaTables(),
    }))) as RSchemaImpl;
    const K = (await ctx.createObject(await RTableGroupImpl.create({
        name: 'gate-k', seed: 'gate-k-seed',
        schemaRef: kSchema.getId(), schemaVersion: await (await kSchema.getScopedDag()).getFrontier(),
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: [identityRow('root', root), identityRow('bob', bob), identityRow('alice', alice)],
            [CAPS_TABLE]: [capRow('root-cap', root.keyId, USERS_MANAGER_LABEL), capRow('bob-cap', bob.keyId, USERS_MANAGER_LABEL)],
        },
    }))) as RTableGroupImpl;
    const k0 = await frontier(K);
    const grant = (await grantCap(K, bob, alice.keyId, 'reader', k0))!;
    const revoke = (await revokeCap(K, root, bob.keyId, USERS_MANAGER_LABEL, k0))!;
    const M = await makeGroup(ctx, 'gate-m', root, [openTable('things', { name: { type: 'string', pub: true } })]);
    await (await M.getTable('things')).insert('thing-1', { name: 'one' });
    const G = await makeGroup(ctx, 'gate-g', root, [{
        name: 'notes', columns: { body: { type: 'string' } },
        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'm.things', where: { name: 'one' } } }],
    }], {
        bindings: { k: K.getId(), m: M.getId() },
        canObserve: { m: { p: 'exists', table: `k.${CAPS_TABLE}`, where: { label: 'reader', grantee: '$author' } } },
        idProvider: `k.${IDENTITIES_TABLE}`,
    });
    const observeOfK = await G.observe('k', new Set([grant]), undefined, await frontier(G));
    const observeOfM = await G.observe('m', await frontier(M), alice, new Set([observeOfK]));
    return { K, M, G, alice, kRevoked: new Set([grant, revoke]), observeOfM, at: new Set([observeOfM]) };
}

// Whether the two readings of G's binding of `bound` differ at `at`.
async function readingsDiffer(G: RTableGroupImpl, bound: RTableGroupImpl, at: Version): Promise<boolean> {
    const every = await G.resolveObservedForeignVersion(bound.getId(), at, at);
    const live = await G.resolveLiveObservedForeignVersion(bound.getId(), at);
    if (every.size !== live.size) return true;
    for (const h of every) if (!live.has(h)) return true;
    return false;
}

export const preparedEntryTests = {
    title: "[PREP] HIP-29: row ops, bundle and observe in two steps, and the second validation through the live observes",
    tests: [
        {
            name: '[BUNDLE08] the two steps of bundle give the entry and the hash that one call gives',
            invoke: async () => {
                const s = await voidScenario('rule');
                const at = new Set([s.malloryObserve]);
                const write = s.note('n-1');
                const prepared = await s.G.prepareBundle([write], s.bob, at);
                const hash = await s.G.bundle([write], s.bob, at);
                assertEquals(prepared.hash, hash, 'the prepared hash is the appended one');
                const entry = await (await s.G.getScopedDag()).loadEntry(hash);
                assertEquals(JSON.stringify(entry?.payload), JSON.stringify(prepared.payload), 'and so is the payload');
                assertEquals(await s.G.appendPrepared(prepared), hash, 'appending it again gives the same hash');
            },
        },
        {
            name: '[BUNDLE09] appendPrepared throws PreparedHashMismatchError for a prepared entry whose hash was changed, and appends nothing',
            invoke: async () => {
                const s = await voidScenario('rule');
                const at = new Set([s.malloryObserve]);
                const prepared = await s.G.prepareBundle([s.note('n-1')], s.bob, at);
                const before = await frontier(s.G);
                let error: unknown;
                try { await s.G.appendPrepared({ ...prepared, hash: 'not-the-hash' }); } catch (e) { error = e; }
                assertTrue(error instanceof PreparedHashMismatchError, 'the mismatch is reported');
                const after = await frontier(s.G);
                assertEquals([...after].sort().join(), [...before].sort().join(), 'nothing was appended');
            },
        },
        {
            name: "[OBSGATE11] the two steps of observe give observe's entry; with selfValidate off, prepareObserve refuses what the gate refuses, and observe doesn't",
            invoke: async () => {
                const s = await voidScenario('rule');
                const prepared = await s.G.prepareObserve('k', s.kGrant, s.alice, s.g0);
                assertEquals(await s.G.observe('k', s.kGrant, s.alice, s.g0), prepared.hash, 'one call gives the prepared hash');

                const off = await voidScenario('rule', false);
                assertTrue(await rejected(() => off.G.prepareObserve('k', off.kGrant, off.bob, off.g0)),
                    'prepareObserve refuses an observe by a non-admin');
                const appended = await off.G.observe('k', off.kGrant, off.bob, off.g0);
                assertTrue(appended !== undefined, 'observe appends it without validating, as today');
            },
        },
        {
            name: "[ENF20] the void scenario, by a rule: with liveReads Bob's insert is refused, and without, accepted",
            invoke: async () => {
                const s = await voidScenario('rule');
                assertTrue(await readingsDiffer(s.G, s.K, s.voidAt), "the two readings of K differ at the void state");
                assertTrue(await rejected(() => s.G.prepareBundle([s.note('n-1')], s.bob, s.voidAt, { liveReads: true })),
                    'refused with liveReads');
                const prepared = await s.G.prepareBundle([s.note('n-1')], s.bob, s.voidAt);
                await s.G.appendPrepared(prepared);
                assertTrue(await s.G.isEntryVoided(prepared.hash, new Set([prepared.hash])), 'accepted without it, and void where it stands');
            },
        },
        {
            name: "[ENF21] the void scenario, by a foreign key into the grant: with liveReads Bob's insert is refused, and without, accepted",
            invoke: async () => {
                const s = await voidScenario('fk');
                assertTrue(await rejected(() => s.G.prepareBundle([s.note('n-1')], s.bob, s.voidAt, { liveReads: true })),
                    'refused with liveReads');
                const prepared = await s.G.prepareBundle([s.note('n-1')], s.bob, s.voidAt);
                await s.G.appendPrepared(prepared);
                assertTrue(await s.G.isEntryVoided(prepared.hash, new Set([prepared.hash])), 'accepted without it, and void where it stands');
            },
        },
        {
            name: "[ENF22] after a live observe of the grant, and after a later observe by an admin, Bob's insert is accepted with liveReads",
            invoke: async () => {
                for (const form of ['rule', 'fk'] as const) {
                    const s = await voidScenario(form);
                    const live = await s.G.observe('k', s.kGrant, s.alice, s.voidAt);
                    const at = new Set([live]);
                    const prepared = await s.G.prepareBundle([s.note('n-live')], s.bob, at, { liveReads: true });
                    await s.G.appendPrepared(prepared);
                    assertTrue(!await s.G.isEntryVoided(prepared.hash, new Set([prepared.hash])), `${form}: accepted and live`);
                }
            },
        },
        {
            name: '[ENF23] the two readings of a gated binding are equal in every state but the void observe\'s',
            invoke: async () => {
                const s = await voidScenario('rule');
                assertTrue(!await readingsDiffer(s.G, s.K, s.g0), 'equal before the observes');
                assertTrue(!await readingsDiffer(s.G, s.K, new Set([s.malloryObserve])), "equal with Mallory's observe alone");
                assertTrue(!await readingsDiffer(s.G, s.K, new Set([s.removal])), 'equal with the removal alone');
                assertTrue(await readingsDiffer(s.G, s.K, s.voidAt), 'they differ where the observe is void');
                const live = await s.G.observe('k', s.kGrant, s.alice, s.voidAt);
                assertTrue(!await readingsDiffer(s.G, s.K, new Set([live])), 'equal again after a live observe');
            },
        },
        {
            name: "[ENF24] the gate graph: an observe of K voids G's observe of M, M's readings then differ, and with liveReads a write that reads M is refused",
            invoke: async () => {
                const s = await gateGraph();
                assertTrue(!await readingsDiffer(s.G, s.M, s.at), "M's readings are equal before the observe of K");
                const observeOfK = await s.G.observe('k', s.kRevoked, undefined, s.at);
                const at = new Set([observeOfK]);
                assertTrue(await s.G.isEntryVoided(s.observeOfM, at), "the observe of K voids Alice's observe of M");
                assertTrue(await readingsDiffer(s.G, s.M, at), "M's readings now differ");
                const write: BundleWrite = { table: 'notes', op: { action: 'insert', rowId: deriveRowId('n-1'), uuid: 'n-1', values: { body: 'x' } } };
                assertTrue(await rejected(() => s.G.prepareBundle([write], undefined, at, { liveReads: true })), 'refused with liveReads');
                await s.G.prepareBundle([write], undefined, at);   // accepted without it
            },
        },
        {
            name: '[PREP01] a row op in two steps is the entry a native write makes: a row envelope of its table, appended under its prepared hash, signed and unsigned, for an insert, an update and a delete',
            invoke: async () => {
                const s = await voidScenario('rule');
                const payloadOf = async (group: RTableGroupImpl, hash: string) =>
                    (await (await group.getScopedDag()).loadEntry(hash))?.payload as { action?: string; table?: string; op?: { action?: string; author?: string } } | undefined;

                // signed, in G: Bob's note, where Mallory's observe brings in his grant
                const at = new Set([s.malloryObserve]);
                const { op } = s.note('n-1');
                const prepared = await s.G.prepareRowOp('notes', op, s.bob, at);
                assertEquals(await s.G.appendPrepared(prepared), prepared.hash, 'appended under the prepared hash');
                const inserted = await payloadOf(s.G, prepared.hash);
                assertEquals(`${inserted?.action} ${inserted?.table} ${inserted?.op?.action}`, 'row notes insert', 'a row envelope of its table, not a bundle');
                assertEquals(inserted?.op?.author, s.bob.keyId, 'signed by its author');
                const native = await (await s.G.getTable('notes')).insert('n-1', { body: 'hi' }, s.bob, at);
                assertEquals(native, prepared.hash, 'the same entry a native insert makes');

                const rowId = deriveRowId('n-1', s.bob.keyId);
                const update = await s.G.prepareRowOp('notes', { action: 'update', rowId, values: { body: 'edited' } }, s.bob, new Set([prepared.hash]));
                await s.G.appendPrepared(update);
                assertEquals((await payloadOf(s.G, update.hash))?.op?.action, 'update', 'an update');
                const del = await s.G.prepareRowOp('notes', { action: 'delete', rowId }, s.bob, new Set([update.hash]));
                await s.G.appendPrepared(del);
                assertEquals((await payloadOf(s.G, del.hash))?.op?.action, 'delete', 'and a delete');

                // unsigned, in K, which has no identity provider
                const kAt = await frontier(s.K);
                const grant = await s.K.prepareRowOp('grants', { action: 'insert', rowId: deriveRowId('g-x'), uuid: 'g-x', values: { holder: 'x' } }, undefined, kAt);
                await s.K.appendPrepared(grant);
                const anonymous = await payloadOf(s.K, grant.hash);
                assertEquals(`${anonymous?.action} ${anonymous?.table} ${anonymous?.op?.author}`, 'row grants undefined', 'unsigned');
                assertEquals(await (await s.K.getTable('grants')).insert('g-x', { holder: 'x' }, undefined, kAt), grant.hash, 'the same entry a native insert makes');
            },
        },
        {
            name: '[PREP02] appendPrepared throws PreparedHashMismatchError for a prepared row op whose hash was changed, and appends nothing',
            invoke: async () => {
                const s = await voidScenario('rule');
                const prepared = await s.G.prepareRowOp('notes', s.note('n-1').op, s.bob, new Set([s.malloryObserve]));
                const before = await frontier(s.G);
                let error: unknown;
                try { await s.G.appendPrepared({ ...prepared, hash: 'not-the-hash' }); } catch (e) { error = e; }
                assertTrue(error instanceof PreparedHashMismatchError, 'the mismatch is reported');
                assertEquals([...(await frontier(s.G))].sort().join(), [...before].sort().join(), 'nothing was appended');
            },
        },
        {
            name: "[PREP03] the void scenario with a row op: with liveReads Bob's insert is refused, and without, accepted and void where it stands",
            invoke: async () => {
                for (const form of ['rule', 'fk'] as const) {
                    const s = await voidScenario(form);
                    const { op } = s.note('n-1');
                    assertTrue(await rejected(() => s.G.prepareRowOp('notes', op, s.bob, s.voidAt, { liveReads: true })), `${form}: refused with liveReads`);
                    const prepared = await s.G.prepareRowOp('notes', op, s.bob, s.voidAt);
                    await s.G.appendPrepared(prepared);
                    assertTrue(await s.G.isEntryVoided(prepared.hash, new Set([prepared.hash])), `${form}: accepted without it, and void where it stands`);
                }
            },
        },
    ],
};

// Multi-group fixtures for HIP-29's cases, built on rdb's public API. All the
// groups of one fixture live in one context, so their bindings resolve.
//
// - Caps and items: a signed group with a `caps` table, and an anonymous group
//   bound to it whose inserts need a cap there. A permission and its use.
// - The void scenario: K holds a grant for Bob. G binds K behind a gate that
//   lets only G's admins observe it. Mallory, an admin, writes the only observe
//   that brings in Bob's grant, and concurrently the other admin removes her
//   from the admins, so her observe is void. A note in G needs its author to
//   hold a grant in K: by a rule, or by a foreign key into the grant's row.
// - The gate graph: G binds K and M, and M's gate reads K. K holds a cap that
//   Bob granted to Alice, and concurrently a revocation of Bob's authority.
//   G's observe of M, by Alice, read K before the revocation.

import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    CAPS_TABLE, IDENTITIES_TABLE, identityRow, capRow, deriveRowId, grantCap, revokeCap, usersSchemaTables,
    USERS_MANAGER_LABEL, RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory,
    type Predicate, type TableDef,
} from "@hyper-hyper-space/hhs3_rdb";

import { createMockRContext } from "./mock_rcontext.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

export async function newIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

export function newContext(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    return ctx;
}

export async function frontierOf(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

const OPEN: Predicate = { p: 'true' };

function openTable(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: OPEN }], ...extra };
}

function identitiesTable(): TableDef {
    return usersSchemaTables().find((t) => t.name === IDENTITIES_TABLE)!;
}

type GroupOptions = {
    bindings?: { [alias: string]: B64Hash };
    canObserve?: { [alias: string]: Predicate };
    idProvider?: string;
    initialRows?: Parameters<typeof RTableGroupImpl.create>[0]['initialRows'];
};

async function makeGroup(ctx: RContext, name: string, creator: OwnIdentity, tables: TableDef[], opts: GroupOptions = {}): Promise<RTableGroupImpl> {
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: `${name.replace(/[^a-zA-Z0-9_]+/g, '_')}_schema`, creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }], tables,
    }))) as RSchemaImpl;
    return (await ctx.createObject(await RTableGroupImpl.create({
        name, seed: `${name}-seed`,
        schemaRef: schema.getId(), schemaVersion: await (await schema.getScopedDag()).getFrontier(),
        ...(opts.bindings !== undefined ? { bindings: opts.bindings } : {}),
        ...(opts.canObserve !== undefined ? { canObserve: opts.canObserve } : {}),
        ...(opts.idProvider !== undefined ? { idProvider: opts.idProvider } : {}),
        ...(opts.initialRows !== undefined ? { initialRows: opts.initialRows } : {}),
    }))) as RTableGroupImpl;
}

// ---------------------------------------------------------------------------
// Caps and items
// ---------------------------------------------------------------------------

export type CapsItems = {
    ctx: RContext;
    // `perm`: signed, with `caps (label)`; `admin` is its registered identity.
    perm: RTableGroupImpl;
    // `items`: anonymous, binds `perm`; an insert needs a `grant` cap there.
    items: RTableGroupImpl;
    admin: OwnIdentity;
};

export async function createCapsItems(opts: { gate?: Predicate } = {}): Promise<CapsItems> {
    const ctx = newContext();
    const admin = await newIdentity();
    const perm = await makeGroup(ctx, 'perm', admin, [
        identitiesTable(),
        openTable('caps', { label: { type: 'string', pub: true } }),
    ], { idProvider: IDENTITIES_TABLE, initialRows: { [IDENTITIES_TABLE]: [identityRow('admin', admin)] } });
    const items = await makeGroup(ctx, 'items', admin, [{
        name: 'items',
        columns: { name: { type: 'string' } },
        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'perm.caps', where: { label: 'grant' } } }],
    }], {
        bindings: { perm: perm.getId() },
        ...(opts.gate !== undefined ? { canObserve: { perm: opts.gate } } : {}),
    });
    return { ctx, perm, items, admin };
}

// ---------------------------------------------------------------------------
// The void scenario
// ---------------------------------------------------------------------------

export type VoidForm = 'rule' | 'fk';

export type VoidScenario = {
    ctx: RContext;
    K: RTableGroupImpl;
    G: RTableGroupImpl;
    mallory: OwnIdentity;
    alice: OwnIdentity;
    bob: OwnIdentity;
    // Bob's grant in K, and K's version that holds it.
    grantRow: B64Hash;
    kGrant: Version;
    // Mallory's observe of K, and Alice's concurrent removal of her admin row.
    malloryObserve: B64Hash;
    removal: B64Hash;
    // G at the void state: the observe and the removal.
    voidAt: Version;
    // The values of a note that needs Bob's grant, in this form.
    note: (body: string) => { [column: string]: string };
};

// `$author IN admins`, over G's own table.
const IN_ADMINS: Predicate = { p: 'exists', table: 'admins', where: { key: '$author' } };

export async function createVoidScenario(form: VoidForm): Promise<VoidScenario> {
    const ctx = newContext();
    const [mallory, alice, bob] = [await newIdentity(), await newIdentity(), await newIdentity()];

    const K = await makeGroup(ctx, `k_${form}`, alice, [openTable('grants', { holder: { type: 'string', pub: true } })]);
    const grantUuid = `grant-${form}`;
    await (await K.getTable('grants')).insert(grantUuid, { holder: bob.keyId });
    const grantRow = deriveRowId(grantUuid);
    const kGrant = await frontierOf(K);

    const notes: TableDef = form === 'rule'
        ? {
            name: 'notes', columns: { body: { type: 'string' } },
            restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'k.grants', where: { holder: '$author' } } }],
        }
        : openTable('notes', { body: { type: 'string' }, grant: { type: 'string' } }, { fks: { grant: 'k.grants' } });
    const admins: TableDef = {
        name: 'admins', columns: { key: { type: 'string', pub: true } },
        concurrentDeletes: true, restrictions: [{ on: 'all', rule: IN_ADMINS }],
    };
    const G = await makeGroup(ctx, `g_${form}`, alice, [identitiesTable(), admins, notes], {
        bindings: { k: K.getId() },
        canObserve: { k: IN_ADMINS },
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: [identityRow('mallory', mallory), identityRow('alice', alice), identityRow('bob', bob)],
            admins: [mallory, alice].map((who, i) => ({
                action: 'insert' as const, rowId: deriveRowId(`admin-${form}-${i}`), uuid: `admin-${form}-${i}`,
                values: { key: who.keyId },
            })),
        },
    });

    const g0 = await frontierOf(G);
    const malloryObserve = await G.observe('k', kGrant, mallory, g0);
    const removal = await (await G.getTable('admins')).delete(deriveRowId(`admin-${form}-0`), alice, g0);
    const voidAt = new Set([malloryObserve, removal]);

    const note = (body: string): { [column: string]: string } =>
        form === 'rule' ? { body } : { body, grant: grantRow };
    return { ctx, K, G, mallory, alice, bob, grantRow, kGrant, malloryObserve, removal, voidAt, note };
}

// ---------------------------------------------------------------------------
// The revocation scenario
// ---------------------------------------------------------------------------

// K is a users group where Bob, a manager, grants Alice a `reader` cap. In the
// `concurrent` form, root revokes Bob's manager cap concurrently, which voids
// the grant; in the `after` form, Bob removes Alice's cap after granting it,
// which voids nothing that came before. G binds K (ungated) and observes it at
// the grant; its notes are guarded by the cap: on insert (`guard: 'insert'`),
// where Alice writes a note; on update (`guard: 'update'`), where root writes a
// note and Alice edits its body; or on delete (`guard: 'delete'`), where root
// writes a note and `deleteNote` has Alice delete it. G also has an open
// `logs` table. Neither group's checkpoint has seen K's removal, so a pass that
// observes K brings it in.
export type RevocationScenario = {
    ctx: RContext;
    K: RTableGroupImpl;
    G: RTableGroupImpl;
    root: OwnIdentity;
    bob: OwnIdentity;
    alice: OwnIdentity;
    kGranted: Version;
    observeOfK: B64Hash;
    // The note the cap guards: Alice's, or root's that Alice edits or deletes.
    noteRow: B64Hash;
    // The delete guard's write, left to the case: Alice deletes root's note.
    deleteNote: () => Promise<B64Hash>;
};

const READER_IN_BOUND: Predicate = { p: 'exists', table: `k.${CAPS_TABLE}`, where: { label: 'reader', grantee: '$author' } };

export async function createRevocationScenario(kind: 'concurrent' | 'after', guard: 'insert' | 'update' | 'delete' = 'insert'): Promise<RevocationScenario> {
    const ctx = newContext();
    const [root, bob, alice] = [await newIdentity(), await newIdentity(), await newIdentity()];
    const kSchema = (await ctx.createObject(await RSchemaImpl.create({
        name: `rev_k_${kind}_${guard}`, creators: [{ keyId: root.keyId, publicKey: root.publicKey }], tables: usersSchemaTables(),
    }))) as RSchemaImpl;
    const K = (await ctx.createObject(await RTableGroupImpl.create({
        name: `rev-k-${kind}-${guard}`, seed: `rev-k-${kind}-${guard}`,
        schemaRef: kSchema.getId(), schemaVersion: await (await kSchema.getScopedDag()).getFrontier(),
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: [identityRow('root', root), identityRow('bob', bob), identityRow('alice', alice)],
            [CAPS_TABLE]: [capRow('root-cap', root.keyId, USERS_MANAGER_LABEL), capRow('bob-cap', bob.keyId, USERS_MANAGER_LABEL)],
        },
    }))) as RTableGroupImpl;
    const k0 = await frontierOf(K);
    const grant = await grantCap(K, bob, alice.keyId, 'reader', k0);
    if (grant === undefined) throw new Error('revocation scenario: the grant must be written');
    const kGranted = new Set([grant]);
    const removal = kind === 'concurrent'
        ? await revokeCap(K, root, bob.keyId, USERS_MANAGER_LABEL, k0)
        : await revokeCap(K, bob, alice.keyId, 'reader', kGranted);
    if (removal === undefined) throw new Error('revocation scenario: the removal must be written');

    const OPEN_RULE: Predicate = { p: 'true' };
    const rule = (on: 'insert' | 'update' | 'delete'): Predicate => on === guard ? READER_IN_BOUND : OPEN_RULE;
    const notes: TableDef = {
        name: 'notes', columns: { body: { type: 'string' }, tag: { type: 'string', nullable: true } },
        restrictions: [{ on: 'insert', rule: rule('insert') }, { on: 'update', rule: rule('update') }, { on: 'delete', rule: rule('delete') }],
    };
    const G = await makeGroup(ctx, `rev-g-${kind}-${guard}`, root, [notes, openTable('logs', { entry: { type: 'string' } })], {
        bindings: { k: K.getId() },
        idProvider: `k.${IDENTITIES_TABLE}`,
    });
    const observeOfK = await G.observe('k', kGranted, undefined, await frontierOf(G));
    const notesTable = await G.getTable('notes');
    let noteRow: B64Hash;
    if (guard === 'insert') {
        await notesTable.insert('n1', { body: "alice's" }, alice, new Set([observeOfK]));
        noteRow = deriveRowId('n1', alice.keyId);
    } else {
        await notesTable.insert('n0', { body: "root's" }, root, new Set([observeOfK]));
        noteRow = deriveRowId('n0', root.keyId);
        if (guard === 'update') await notesTable.update(noteRow, { body: "alice's edit" }, alice, await frontierOf(G));
    }
    const deleteNote = async (): Promise<B64Hash> => notesTable.delete(noteRow, alice, await frontierOf(G));
    return { ctx, K, G, root, bob, alice, kGranted, observeOfK, noteRow, deleteNote };
}

// ---------------------------------------------------------------------------
// The reinstatement scenario
// ---------------------------------------------------------------------------

// K is a users group of three managers, root, Bob and Carl. Bob grants Alice a
// `reader` cap; concurrently root revokes Bob's manager cap, which voids the
// grant, and Carl revokes root's, which voids that revocation. G binds K and
// guards its notes' inserts by the cap: Alice's note was written while G
// observed only the grant, and G then observed the first revocation, so the
// note is void at G's frontier. An observe that brings in Carl's revocation
// reinstates it. G also has an open `logs` table.
export type ReinstateScenario = {
    ctx: RContext;
    K: RTableGroupImpl;
    G: RTableGroupImpl;
    root: OwnIdentity;
    alice: OwnIdentity;
    noteRow: B64Hash;
};

export async function createReinstateScenario(): Promise<ReinstateScenario> {
    const ctx = newContext();
    const [root, bob, carl, alice] = [await newIdentity(), await newIdentity(), await newIdentity(), await newIdentity()];
    const kSchema = (await ctx.createObject(await RSchemaImpl.create({
        name: 'reinstate_k', creators: [{ keyId: root.keyId, publicKey: root.publicKey }], tables: usersSchemaTables(),
    }))) as RSchemaImpl;
    const K = (await ctx.createObject(await RTableGroupImpl.create({
        name: 'reinstate-k', seed: 'reinstate-k',
        schemaRef: kSchema.getId(), schemaVersion: await (await kSchema.getScopedDag()).getFrontier(),
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: [identityRow('root', root), identityRow('bob', bob), identityRow('carl', carl), identityRow('alice', alice)],
            [CAPS_TABLE]: [capRow('root-cap', root.keyId, USERS_MANAGER_LABEL), capRow('bob-cap', bob.keyId, USERS_MANAGER_LABEL),
                capRow('carl-cap', carl.keyId, USERS_MANAGER_LABEL)],
        },
    }))) as RTableGroupImpl;
    const k0 = await frontierOf(K);
    const grant = await grantCap(K, bob, alice.keyId, 'reader', k0);
    const first = await revokeCap(K, root, bob.keyId, USERS_MANAGER_LABEL, k0);
    const second = await revokeCap(K, carl, root.keyId, USERS_MANAGER_LABEL, k0);
    if (grant === undefined || first === undefined || second === undefined) throw new Error('reinstatement scenario: every K entry must be written');

    const G = await makeGroup(ctx, 'reinstate-g', root, [
        { name: 'notes', columns: { body: { type: 'string' } }, restrictions: [{ on: 'insert', rule: READER_IN_BOUND }] },
        openTable('logs', { entry: { type: 'string' } }),
    ], { bindings: { k: K.getId() }, idProvider: `k.${IDENTITIES_TABLE}` });
    const seen = await G.observe('k', new Set([grant]), undefined, await frontierOf(G));
    await (await G.getTable('notes')).insert('n1', { body: "alice's" }, alice, new Set([seen]));
    await G.observe('k', new Set([grant, first]), undefined, await frontierOf(G));
    return { ctx, K, G, root, alice, noteRow: deriveRowId('n1', alice.keyId) };
}

// ---------------------------------------------------------------------------
// The gate graph
// ---------------------------------------------------------------------------

export type GateGraph = {
    ctx: RContext;
    K: RTableGroupImpl;
    M: RTableGroupImpl;
    G: RTableGroupImpl;
    root: OwnIdentity;
    bob: OwnIdentity;
    alice: OwnIdentity;
    // K with Bob's grant of Alice's `reader` cap, and K with it and root's
    // concurrent revocation of Bob's manager cap, which voids the grant.
    kGranted: Version;
    kRevoked: Version;
    // G's observe of K at kGranted, then Alice's observe of M, live there.
    observeOfK: B64Hash;
    observeOfM: B64Hash;
    // G after both observes.
    at: Version;
};

// `$author` holds a `reader` cap in the group bound as `k`.
const READER_IN_K: Predicate = { p: 'exists', table: `k.${CAPS_TABLE}`, where: { label: 'reader', grantee: '$author' } };

export async function createGateGraph(): Promise<GateGraph> {
    const ctx = newContext();
    const [root, bob, alice] = [await newIdentity(), await newIdentity(), await newIdentity()];

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
    const k0 = await frontierOf(K);
    const grant = await grantCap(K, bob, alice.keyId, 'reader', k0);
    const revoke = await revokeCap(K, root, bob.keyId, USERS_MANAGER_LABEL, k0);
    if (grant === undefined || revoke === undefined) throw new Error('gate graph: the grant and the revocation must both be written');
    const kGranted = new Set([grant]);
    const kRevoked = new Set([grant, revoke]);

    const M = await makeGroup(ctx, 'gate-m', root, [openTable('things', { name: { type: 'string', pub: true } })]);
    await (await M.getTable('things')).insert('thing-1', { name: 'one' });
    const mAt = await frontierOf(M);

    const G = await makeGroup(ctx, 'gate-g', root, [{
        name: 'notes', columns: { body: { type: 'string' } },
        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'm.things', where: { name: 'one' } } }],
    }], {
        bindings: { k: K.getId(), m: M.getId() },
        canObserve: { m: READER_IN_K },
        idProvider: `k.${IDENTITIES_TABLE}`,
    });
    const observeOfK = await G.observe('k', kGranted, undefined, await frontierOf(G));
    const observeOfM = await G.observe('m', mAt, alice, new Set([observeOfK]));
    return { ctx, K, M, G, root, bob, alice, kGranted, kRevoked, observeOfK, observeOfM, at: new Set([observeOfM]) };
}

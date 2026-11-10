// Observer-frame canObserve gates: a gate on binding `b => H` of a group G is
// read in G's frame. `a.t` reads table t of the group bound to alias a,
// through a's table map, and an unqualified `t` is G's own table.
//
// These tests cover reads of other bindings (a revoke or a grant in a second
// bound group K), gates over G's own tables with nothing pending, admission,
// the creation rules and table-map translation.

import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";
import { formatValidationFailure, ValidationRejectedError, version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import type { TableMaps } from "../src/rtable_group/payload.js";
import { deriveRowId } from "../src/rtable/hash.js";
import { formatOpVoidDetail } from "../src/rtable_group/op_void.js";
import type { MigrationRule, Predicate, TableDef } from "../src/rschema/payload.js";
import {
    usersSchemaTables, identityRow, capRow, grantCap, revokeCap, registerIdentity,
    IDENTITIES_TABLE, CAPS_TABLE, USERS_MANAGER_LABEL, USERS_SCHEMA_NAME,
} from "../src/users/users.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

export async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

export function newCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    return ctx;
}

export async function frontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

async function expectRejected(fn: () => Promise<unknown>, messageIncludes: string, why: string): Promise<void> {
    let error: unknown;
    try { await fn(); } catch (e) { error = e; }
    assertTrue(error !== undefined, why);
    const msg = error instanceof ValidationRejectedError
        ? formatValidationFailure(error.why)
        : error instanceof Error ? error.message : String(error);
    assertTrue(msg.includes(messageIncludes), `${why}: expected '${messageIncludes}', got: ${msg}`);
}

// `$author IN admins`, over the observer's own table.
const IN_ADMINS: Predicate = { p: 'exists', table: 'admins', where: { key: '$author' } };

// A manager cap in the group bound as `auth`.
const MANAGER_IN_AUTH: Predicate = { p: 'exists', table: `auth.${CAPS_TABLE}`, where: { label: USERS_MANAGER_LABEL, grantee: '$author' } };

function adminsTable(rule: Predicate): TableDef {
    return {
        name: 'admins',
        columns: { key: { type: 'string', pub: true } },
        concurrentDeletes: true,
        restrictions: [{ on: 'all', rule }],
    };
}

const NOTES: TableDef = { name: 'notes', columns: { body: { type: 'string' } }, restrictions: [{ on: 'all', rule: { p: 'true' } }] };

// K: a Users group whose `identities` are genesis identity rows and whose
// `managers` hold genesis manager caps.
export async function makeUsers(ctx: RContext, seed: string, opts: { identities: OwnIdentity[]; managers: OwnIdentity[] }): Promise<RTableGroupImpl> {
    const creator = opts.managers[0] ?? opts.identities[0]!;
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: USERS_SCHEMA_NAME, creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }], tables: usersSchemaTables(),
    }))) as RSchemaImpl;
    return (await ctx.createObject(await RTableGroupImpl.create({
        name: seed, seed: seed + '-group',
        schemaRef: schema.getId(), schemaVersion: await (await schema.getScopedDag()).getFrontier(),
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: opts.identities.map((id, i) => identityRow(`${seed}-id-${i}`, id)),
            [CAPS_TABLE]: opts.managers.map((m, i) => capRow(`${seed}-cap-${i}`, m.keyId, USERS_MANAGER_LABEL)),
        },
    }))) as RTableGroupImpl;
}

// H: a data group with one open table, advanced past its genesis.
export async function makeData(ctx: RContext, seed: string, creator: OwnIdentity): Promise<RTableGroupImpl> {
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: `${seed.replace(/[^a-zA-Z0-9_]+/g, '_')}:data`, creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: [{ name: 'items', columns: { body: { type: 'string' } }, restrictions: [{ on: 'all', rule: { p: 'true' } }] }],
    }))) as RSchemaImpl;
    const group = (await ctx.createObject(await RTableGroupImpl.create({
        name: seed, seed: seed + '-group',
        schemaRef: schema.getId(), schemaVersion: await (await schema.getScopedDag()).getFrontier(),
    }))) as RTableGroupImpl;
    await (await group.getTable('items')).insert(`${seed}-item`, { body: 'x' });
    return group;
}

// G: the observer, with its own tables, genesis admins and gates.
export async function makeObserver(ctx: RContext, seed: string, opts: {
    creator: OwnIdentity;
    bindings: { [alias: string]: B64Hash };
    tables?: TableDef[];
    admins?: OwnIdentity[];
    canObserve?: { [alias: string]: Predicate };
    tableMaps?: TableMaps;
    idProvider?: string;
    seeds?: { [table: string]: OwnIdentity[] };
}): Promise<RTableGroupImpl> {
    const seeds: { [table: string]: OwnIdentity[] } = { ...(opts.seeds ?? {}), ...(opts.admins !== undefined ? { admins: opts.admins } : {}) };
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: `${seed.replace(/[^a-zA-Z0-9_]+/g, '_')}:observer`,
        creators: [{ keyId: opts.creator.keyId, publicKey: opts.creator.publicKey }],
        tables: opts.tables ?? [NOTES],
    }))) as RSchemaImpl;
    return (await ctx.createObject(await RTableGroupImpl.create({
        name: seed, seed: seed + '-group',
        schemaRef: schema.getId(), schemaVersion: await (await schema.getScopedDag()).getFrontier(),
        bindings: opts.bindings,
        ...(opts.tableMaps !== undefined ? { tableMaps: opts.tableMaps } : {}),
        ...(opts.idProvider !== undefined ? { idProvider: opts.idProvider } : {}),
        ...(opts.canObserve !== undefined ? { canObserve: opts.canObserve } : {}),
        ...(Object.keys(seeds).length > 0 ? {
            initialRows: Object.fromEntries(Object.entries(seeds).map(([table, who]) => [table, who.map((a, i) => ({
                action: 'insert' as const, rowId: deriveRowId(`${seed}-${table}-${i}`), uuid: `${seed}-${table}-${i}`, values: { key: a.keyId },
            }))])),
        } : {}),
    }))) as RTableGroupImpl;
}

export async function adminRow(group: RTableGroupImpl, who: OwnIdentity, at: Version, table = 'admins'): Promise<B64Hash> {
    const rows = await (await (await group.getView(at, at)).getTableView(table)).findRowIds({ key: who.keyId });
    assertTrue(rows.length === 1, 'one admins row for the identity');
    return rows[0]!;
}

export async function verdict(group: RTableGroupImpl, entry: B64Hash): Promise<string> {
    const detail = await group.explainEntryVoided(entry, await frontier(group));
    return detail === undefined ? 'live' : detail.kind;
}

// `$author IN auth.caps`: any cap the author holds in the group bound as `auth`.
const IN_AUTH_CAPS: Predicate = { p: 'exists', table: `auth.${CAPS_TABLE}`, where: { grantee: '$author' } };

// The loop configuration of the walkthrough: G binds `auth => K` and
// `target => H`, keeps `admins` (genesis: root, alice, carol), which honors
// concurrent deletes and whose rules are `$author IN auth.caps`, and gates
// both bindings with `$author IN admins`. Everyone holds a cap in K, and root
// revokes bob's in K's version v1. G imports K's genesis v0 at `base`.
export type LoopFixture = {
    root: OwnIdentity; alice: OwnIdentity; bob: OwnIdentity; carol: OwnIdentity;
    K: RTableGroupImpl; H: RTableGroupImpl; G: RTableGroupImpl;
    v0: Version; v1: Version; base: Version;
};

export async function loopFixture(ctx: RContext, seed: string): Promise<LoopFixture> {
    const [root, alice, bob, carol] = [await makeIdentity(), await makeIdentity(), await makeIdentity(), await makeIdentity()];
    const K = await makeUsers(ctx, `${seed}-k`, { identities: [root, alice, bob, carol], managers: [root, alice, bob, carol] });
    const v0 = await frontier(K);
    assertTrue(await revokeCap(K, root, bob.keyId, USERS_MANAGER_LABEL) !== undefined, "root revokes bob's cap in K");
    const v1 = await frontier(K);
    const H = await makeData(ctx, `${seed}-h`, root);
    const G = await makeObserver(ctx, `${seed}-g`, {
        creator: root, bindings: { target: H.getId(), auth: K.getId() },
        tables: [adminsTable(IN_AUTH_CAPS)], admins: [root, alice, carol],
        canObserve: { auth: IN_ADMINS, target: IN_ADMINS }, idProvider: 'auth.identities',
    });
    const base = version(await G.observe('auth', v0, root, await frontier(G)));
    return { root, alice, bob, carol, K, H, G, v0, v1, base };
}

export async function verdictAt(group: RTableGroupImpl, entry: B64Hash, from: Version): Promise<string> {
    const detail = await group.explainEntryVoided(entry, from);
    return detail === undefined ? 'live' : detail.kind;
}

const OPEN: Predicate = { p: 'true' };

// A bound group with open rules for the cases that need writes the users
// schema does not allow: caps with a mutable, unsearched `note`, and
// `allowed` (user, active with default no) for the default-reset loops.
function authTables(): TableDef[] {
    return [
        usersSchemaTables().find((t) => t.name === IDENTITIES_TABLE)!,
        {
            name: CAPS_TABLE,
            columns: { grantee: { type: 'string', pub: true }, note: { type: 'string', nullable: true } },
            concurrentDeletes: true, restrictions: [{ on: 'all', rule: OPEN }],
        },
        {
            name: 'allowed',
            columns: { user: { type: 'string', pub: true }, active: { type: 'string', pub: true, default: 'no' } },
            concurrentDeletes: true, restrictions: [{ on: 'all', rule: OPEN }],
        },
    ];
}

async function makeAuth(ctx: RContext, seed: string, opts: {
    creator: OwnIdentity; identities: OwnIdentity[]; caps?: OwnIdentity[];
    allowed?: [OwnIdentity, string][]; tables?: TableDef[];
}): Promise<{ K: RTableGroupImpl; schema: RSchemaImpl; capRow: (who: OwnIdentity) => B64Hash }> {
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: `${seed.replace(/[^a-zA-Z0-9_]+/g, '_')}:auth`,
        creators: [{ keyId: opts.creator.keyId, publicKey: opts.creator.publicKey }],
        tables: opts.tables ?? authTables(),
    }))) as RSchemaImpl;
    const capUuid = (who: OwnIdentity) => `${seed}-cap-${who.keyId}`;
    const K = (await ctx.createObject(await RTableGroupImpl.create({
        name: seed, seed: seed + '-group',
        schemaRef: schema.getId(), schemaVersion: await (await schema.getScopedDag()).getFrontier(),
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            [IDENTITIES_TABLE]: opts.identities.map((id, i) => identityRow(`${seed}-id-${i}`, id)),
            ...(opts.caps !== undefined ? { [CAPS_TABLE]: opts.caps.map((c) => ({
                action: 'insert' as const, rowId: deriveRowId(capUuid(c)), uuid: capUuid(c), values: { grantee: c.keyId },
            })) } : {}),
            ...(opts.allowed !== undefined ? { allowed: opts.allowed.map(([who, active], i) => ({
                action: 'insert' as const, rowId: deriveRowId(`${seed}-allowed-${i}`), uuid: `${seed}-allowed-${i}`,
                values: { user: who.keyId, active },
            })) } : {}),
        },
    }))) as RTableGroupImpl;
    return { K, schema, capRow: (who) => deriveRowId(capUuid(who)) };
}

// Deploys a schema update of K at `at`, returning K's new version.
async function deployOn(K: RTableGroupImpl, schema: RSchemaImpl, admin: OwnIdentity, rules: MigrationRule[], at: Version): Promise<Version> {
    const s = await schema.updateSchema(rules, admin, 'update', await (await schema.getScopedDag()).getFrontier());
    return version(await K.deploy(version(s), undefined, at));
}

const RESET_ACTIVE: MigrationRule[] = [
    { rule: 'drop-column', table: 'allowed', column: 'active' },
    { rule: 'add-column', table: 'allowed', column: 'active', def: { type: 'string', pub: true, default: 'yes' } },
];

// `EXISTS (auth.allowed WHERE user = $author AND active = yes)`
const ALLOWED_ACTIVE: Predicate = { p: 'exists', table: 'auth.allowed', where: { user: '$author', active: 'yes' } };

function namedTable(name: string, rule: Predicate): TableDef {
    return { ...adminsTable(rule), name };
}

function inTable(name: string): Predicate {
    return { p: 'exists', table: name, where: { key: '$author' } };
}

// The loop observer of the walkthrough over a given K: admins (rule
// `adminsRule`, default `$author IN auth.caps`), both bindings gated by
// `$author IN admins`.
async function loopObserver(ctx: RContext, seed: string, opts: {
    creator: OwnIdentity; K: RTableGroupImpl; H: RTableGroupImpl; admins: OwnIdentity[]; adminsRule?: Predicate;
}): Promise<RTableGroupImpl> {
    return makeObserver(ctx, seed, {
        creator: opts.creator, bindings: { target: opts.H.getId(), auth: opts.K.getId() },
        tables: [adminsTable(opts.adminsRule ?? IN_AUTH_CAPS)], admins: opts.admins,
        canObserve: { auth: IN_ADMINS, target: IN_ADMINS }, idProvider: 'auth.identities',
    });
}

export const observerFrameTests = {
    title: '[OBSFRAME] Observer-frame gates: other bindings, the observer\'s own tables, admission, creation and table maps',
    tests: [
        {
            name: '[OBSFRAME11] walkthrough 3: an import that revokes a deleter, raced by the deleter\'s delete of the importer\'s admins row, voids both, in either order and at every horizon',
            invoke: async () => {
                const ctx = newCtx();
                for (const deleteFirst of [true, false]) {
                    const fx = await loopFixture(ctx, `of11-${deleteFirst}`);
                    const { G, alice, bob, root, v1, base } = fx;
                    const admins = await G.getTable('admins');
                    const aliceRow = await adminRow(G, alice, base);
                    let d: B64Hash;
                    let y: B64Hash;
                    if (deleteFirst) {
                        d = await admins.delete(aliceRow, bob, base);
                        y = await G.observe('auth', v1, alice, base);
                    } else {
                        y = await G.observe('auth', v1, alice, base);
                        d = await admins.delete(aliceRow, bob, base);
                    }
                    assertTrue(await verdictAt(G, d, version(d)) === 'live', 'alone, the delete is live');
                    assertTrue(await verdictAt(G, y, version(y)) === 'live', 'alone, the import is live');
                    const both = version(d, y);
                    assertTrue(await verdictAt(G, d, both) === 'undecided-cycle', 'together, the delete collapses');
                    assertTrue(await verdictAt(G, y, both) === 'undecided-cycle', 'together, the import collapses');

                    // a later, unrelated entry and one concurrent with both change nothing
                    const unrelated = await admins.insert(`of11-${deleteFirst}-x`, { key: root.keyId }, root, both);
                    const side = await admins.insert(`of11-${deleteFirst}-z`, { key: root.keyId }, root, base);
                    for (const from of [version(unrelated), version(unrelated, side)]) {
                        assertTrue(await verdictAt(G, d, from) === 'undecided-cycle' && await verdictAt(G, y, from) === 'undecided-cycle',
                            'the core stays collapsed at later horizons');
                    }
                    // alice stays an admin, and G reads K without the revoke: bob's cap stays live
                    const at = await frontier(G);
                    assertTrue((await (await (await G.getView(at, at)).getTableView('admins')).findRowIds({ key: alice.keyId })).length === 1,
                        'alice keeps her admins row');
                    assertTrue([...await G.resolveLiveObservedForeignVersion(fx.K.getId(), at)].join() === [...fx.v0].join(),
                        "G's live view of K stays at v0");
                }
            }
        },
        {
            name: '[OBSFRAME12] after the loop: the importer\'s observe from the merge is live and G reads the revoke; the deleter\'s delete from the merge is refused, and a back-dated one contests again',
            invoke: async () => {
                const ctx = newCtx();
                const fx = await loopFixture(ctx, 'of12');
                const { G, alice, bob, K, v1, base } = fx;
                const admins = await G.getTable('admins');
                const aliceRow = await adminRow(G, alice, base);
                const d = await admins.delete(aliceRow, bob, base);
                const y = await G.observe('auth', v1, alice, base);
                const y2 = await G.observe('auth', v1, alice, version(d, y));
                assertTrue(await verdict(G, y2) === 'live', "alice's import from the merge has the delete in its history, and is live");
                assertTrue(await verdict(G, d) === 'undecided-cycle' && await verdict(G, y) === 'undecided-cycle', 'the core stays collapsed');
                assertTrue([...await G.resolveLiveObservedForeignVersion(K.getId(), await frontier(G))].join() === [...v1].join(),
                    'G now reads K with the revoke');

                await expectRejected(() => admins.delete(aliceRow, bob, version(y2)), 'does not satisfy ALLOW delete IF',
                    "from the merge, admission reads bob's revoked cap");

                // a second delete written below both imports (above an unrelated entry, so it
                // is a new entry, not d again)
                const unrelated = await admins.insert('of12-u', { key: fx.root.keyId }, fx.root, base);
                const d2 = await admins.delete(aliceRow, bob, version(unrelated));
                assertTrue(await verdict(G, y2) === 'undecided-cycle' && await verdict(G, d2) === 'undecided-cycle',
                    "a back-dated delete contests alice's new import again");
            }
        },
        {
            name: '[OBSFRAME13] walkthrough 4: an import of the revoke by an admin no delete targets decides the delete void, and an import of the same version by the targeted admin is live',
            invoke: async () => {
                const ctx = newCtx();
                const fx = await loopFixture(ctx, 'of13');
                const { G, H, alice, bob, carol, v1, base } = fx;
                const admins = await G.getTable('admins');
                const d = await admins.delete(await adminRow(G, alice, base), bob, base);
                const yC = await G.observe('auth', v1, carol, base);
                const y = await G.observe('auth', v1, alice, base);
                assertTrue(await verdict(G, yC) === 'live', "carol's import is live");
                assertTrue(await verdict(G, d) === 'restriction', "with carol's import, bob's cap is revoked in the delete's read");
                assertTrue(await verdict(G, y) === 'live', "alice's import adds nothing to the read, and is live");
                const t = await G.observe('target', await frontier(H), alice, version(d, yC, y));
                assertTrue(await verdict(G, t) === 'live', "target's gate is unaffected");
            }
        },
        {
            name: '[OBSFRAME14] walkthrough 5: an import that misses the delete\'s read leaves it decided live, and the import is void with observe-gate',
            invoke: async () => {
                const ctx = newCtx();
                const fx = await loopFixture(ctx, 'of14');
                const { G, K, alice, bob, v0, base } = fx;
                const stranger = await makeIdentity();
                const vU = version(await registerIdentity(K, stranger, undefined, v0));   // beside the revoke, not above it
                const d = await (await G.getTable('admins')).delete(await adminRow(G, alice, base), bob, base);
                const y = await G.observe('auth', vU, alice, base);
                assertTrue(await verdict(G, d) === 'live', "an identity registration touches nothing bob's cap read uses");
                assertTrue(await verdict(G, y) === 'observe-gate', "alice's row is deleted, so her import is void");
            }
        },
        {
            name: '[OBSFRAME15] a second cap for the deleter, imported by the observe, does not contest a delete that his first cap decides',
            invoke: async () => {
                const ctx = newCtx();
                const fx = await loopFixture(ctx, 'of15');
                const { G, K, root, alice, bob, v0, base } = fx;
                const vX = version((await (await K.getTable(CAPS_TABLE)).insert('of15-extra', { label: 'extra', grantee: bob.keyId }, root, v0))!);
                const d = await (await G.getTable('admins')).delete(await adminRow(G, alice, base), bob, base);
                const y = await G.observe('auth', vX, alice, base);
                assertTrue(await verdict(G, d) === 'live', "bob's first cap, untouched, decides the read");
                assertTrue(await verdict(G, y) === 'observe-gate', "alice's row is deleted, so her import is void");
            }
        },
        {
            name: '[OBSFRAME16] an import that writes a column of the deleter\'s cap row that the read does not search leaves the delete decided',
            invoke: async () => {
                const ctx = newCtx();
                const [root, alice, bob] = [await makeIdentity(), await makeIdentity(), await makeIdentity()];
                const { K, capRow: capOf } = await makeAuth(ctx, 'of16-k', { creator: root, identities: [root, alice, bob], caps: [root, alice, bob] });
                const v0 = await frontier(K);
                const vN = version(await (await K.getTable(CAPS_TABLE)).update(capOf(bob), { note: 'reviewed' }, undefined, v0));
                const G = await loopObserver(ctx, 'of16-g', { creator: root, K, H: await makeData(ctx, 'of16-h', root), admins: [root, alice] });
                const base = version(await G.observe('auth', v0, root, await frontier(G)));
                const d = await (await G.getTable('admins')).delete(await adminRow(G, alice, base), bob, base);
                const y = await G.observe('auth', vN, alice, base);
                assertTrue(await verdict(G, d) === 'live', 'a write to note touches nothing a grantee search reads');
                assertTrue(await verdict(G, y) === 'observe-gate', "alice's row is deleted, so her import is void");
            }
        },
        {
            name: '[OBSFRAME17] a pending deploy that writes the existence slot or a searched column\'s slot makes the read undecided, also with the table absent at D; one that changes only rules leaves it decided',
            invoke: async () => {
                const ctx = newCtx();
                const outcome = async (seed: string, rules: MigrationRule[]) => {
                    const [root, alice, bob] = [await makeIdentity(), await makeIdentity(), await makeIdentity()];
                    const { K, schema } = await makeAuth(ctx, `${seed}-k`, { creator: root, identities: [root, alice, bob], caps: [root, alice, bob] });
                    const v0 = await frontier(K);
                    const vD = await deployOn(K, schema, root, rules, v0);
                    const G = await loopObserver(ctx, `${seed}-g`, { creator: root, K, H: await makeData(ctx, `${seed}-h`, root), admins: [root, alice] });
                    const base = version(await G.observe('auth', v0, root, await frontier(G)));
                    const d = await (await G.getTable('admins')).delete(await adminRow(G, alice, base), bob, base);
                    const y = await G.observe('auth', vD, alice, base);
                    return [await verdict(G, d), await verdict(G, y)].join(' ');
                };
                const grantee: MigrationRule[] = [
                    { rule: 'drop-column', table: CAPS_TABLE, column: 'grantee' },
                    { rule: 'add-column', table: CAPS_TABLE, column: 'grantee', def: { type: 'string', pub: true, nullable: true } },
                ];
                const existence: MigrationRule[] = [{ rule: 'drop-table', table: CAPS_TABLE }];
                const rulesOnly: MigrationRule[] = [{ rule: 'set-restrictions', table: CAPS_TABLE, restrictions: [{ on: 'insert', rule: OPEN }] }];
                assertTrue(await outcome('of17-col', grantee) === 'undecided-cycle undecided-cycle', 'a reset of the searched column touches the read');
                assertTrue(await outcome('of17-table', existence) === 'undecided-cycle undecided-cycle', 'a drop of the table touches the read');
                assertTrue(await outcome('of17-rules', rulesOnly) === 'live observe-gate', 'a deploy of rules only leaves the read decided');

                // caps absent at D: K starts without caps; a deploy adds it and bob's cap.
                // mallory's import w of that version is void (root deletes her admins row
                // concurrently), so D lacks caps while admission's geometric fold has it.
                const [root, alice, bob, mallory] = [await makeIdentity(), await makeIdentity(), await makeIdentity(), await makeIdentity()];
                const { K, schema } = await makeAuth(ctx, 'of17-absent-k', {
                    creator: root, identities: [root, alice, bob, mallory], tables: authTables().filter((t) => t.name !== CAPS_TABLE),
                });
                const v0 = await frontier(K);
                const capsDef = authTables().find((t) => t.name === CAPS_TABLE)!;
                const vCaps = await deployOn(K, schema, root, [{ rule: 'add-table', def: capsDef }], v0);
                const v2 = version(await (await K.getTable(CAPS_TABLE)).insert('of17-bob-cap', { grantee: bob.keyId }, undefined, vCaps));
                const G = await loopObserver(ctx, 'of17-absent-g', {
                    creator: root, K, H: await makeData(ctx, 'of17-absent-h', root), admins: [root, alice, mallory],
                    adminsRule: { p: 'or', args: [IN_AUTH_CAPS, IN_ADMINS] },
                });
                const base = version(await G.observe('auth', v0, root, await frontier(G)));
                const admins = await G.getTable('admins');
                const w = await G.observe('auth', v2, mallory, base);
                const e = await admins.delete(await adminRow(G, mallory, base), root, base);
                const d = await admins.delete(await adminRow(G, alice, base), bob, version(w, e));
                const y = await G.observe('auth', v2, alice, version(w, e));
                assertTrue(await verdict(G, w) === 'observe-gate' && await verdict(G, e) === 'live', "mallory's import is void");
                assertTrue(await verdict(G, d) === 'undecided-cycle' && await verdict(G, y) === 'undecided-cycle',
                    'with caps absent at D, the pending import that adds it touches the read, and the delete and the import collapse');
            }
        },
        {
            name: '[OBSFRAME18] the even loop through a re-added default voids all three entries instead of settling on one side',
            invoke: async () => {
                // carol imports the reset with z; bob, having seen z, deletes mallory's
                // moderators row with d, whose rule needs bob's allowed row active, which
                // holds only with the reset; mallory, concurrently with both, deletes
                // carol's admins row with e, whose rule is $author IN moderators. z live
                // makes d live and e void, and z void makes d void and e live: both are
                // consistent, so all three collapse.
                const ctx = newCtx();
                const [root, carol, bob, mallory] = [await makeIdentity(), await makeIdentity(), await makeIdentity(), await makeIdentity()];
                const { K, schema } = await makeAuth(ctx, 'of18-k', { creator: root, identities: [root, carol, bob, mallory], allowed: [[bob, 'no']] });
                const vD = await deployOn(K, schema, root, RESET_ACTIVE, await frontier(K));
                const H = await makeData(ctx, 'of18-h', root);
                const G = await makeObserver(ctx, 'of18-g', {
                    creator: root, bindings: { target: H.getId(), auth: K.getId() },
                    tables: [namedTable('admins', inTable('moderators')), namedTable('moderators', ALLOWED_ACTIVE)],
                    seeds: { admins: [carol], moderators: [mallory] },
                    canObserve: { auth: IN_ADMINS, target: IN_ADMINS }, idProvider: 'auth.identities',
                });
                const base = await frontier(G);
                const z = await G.observe('auth', vD, carol, base);
                const d = await (await G.getTable('moderators')).delete(await adminRow(G, mallory, base, 'moderators'), bob, version(z));
                const e = await (await G.getTable('admins')).delete(await adminRow(G, carol, base), mallory, base);
                const verdicts = [await verdict(G, z), await verdict(G, d), await verdict(G, e)];
                assertTrue(verdicts.every((v) => v === 'undecided-cycle'), `all three collapse (${verdicts.join(', ')})`);
            }
        },
        {
            name: '[OBSFRAME19] the odd loop through a re-added default voids both entries instead of throwing',
            invoke: async () => {
                // alice deletes carol's admins row with e, whose rule needs alice's allowed
                // row active, which holds only with the reset; e is admitted on mallory's
                // earlier import w of the reset, which is void at use. carol's import z of
                // the reset, concurrent with e: z live makes e live, which voids z, and z
                // void makes e void, which makes z live.
                const ctx = newCtx();
                const [root, carol, alice, mallory] = [await makeIdentity(), await makeIdentity(), await makeIdentity(), await makeIdentity()];
                const { K, schema } = await makeAuth(ctx, 'of19-k', {
                    creator: root, identities: [root, carol, alice, mallory], allowed: [[alice, 'no'], [root, 'yes']],
                });
                const vD = await deployOn(K, schema, root, RESET_ACTIVE, await frontier(K));
                const G = await makeObserver(ctx, 'of19-g', {
                    creator: root, bindings: { target: (await makeData(ctx, 'of19-h', root)).getId(), auth: K.getId() },
                    // root's delete of mallory's row is decided through admins; alice, not an
                    // admin, depends on her allowed row alone
                    tables: [namedTable('admins', { p: 'or', args: [ALLOWED_ACTIVE, IN_ADMINS] })], admins: [root, carol, mallory],
                    canObserve: { auth: IN_ADMINS, target: IN_ADMINS }, idProvider: 'auth.identities',
                });
                const base = await frontier(G);
                const admins = await G.getTable('admins');
                const w = await G.observe('auth', vD, mallory, base);
                const f = await admins.delete(await adminRow(G, mallory, base), root, base);
                const e = await admins.delete(await adminRow(G, carol, base), alice, version(w, f));
                const z = await G.observe('auth', vD, carol, version(w, f));
                assertTrue(await verdict(G, w) === 'observe-gate' && await verdict(G, f) === 'live', "mallory's import is void");
                const verdicts = [await verdict(G, e), await verdict(G, z)];
                assertTrue(verdicts.every((v) => v === 'undecided-cycle'), `the delete and the import collapse (${verdicts.join(', ')})`);
            }
        },
        {
            name: '[OBSFRAME20] the stalemate needs an exposed importer: an importer the deleter cannot remove gets the revoke through, and so does a gate that reads auth.caps itself',
            invoke: async () => {
                const ctx = newCtx();
                for (const gate of ['owners', 'caps'] as const) {
                    const fx = await loopFixture(ctx, `of20-${gate}`);
                    const { K, H, root, alice, bob, v0, v1 } = fx;
                    // alice's authority to import is either in owners, which no rule lets anyone
                    // change, or in auth.caps itself; bob can still delete her admins row
                    const G = await makeObserver(ctx, `of20-${gate}-g`, {
                        creator: root, bindings: { target: H.getId(), auth: K.getId() },
                        tables: [adminsTable(IN_AUTH_CAPS), namedTable('owners', { p: 'false' })],
                        seeds: { admins: [root, alice], owners: [alice] },
                        canObserve: { auth: gate === 'owners' ? inTable('owners') : IN_AUTH_CAPS, target: IN_ADMINS },
                        idProvider: 'auth.identities',
                    });
                    const base = version(await G.observe('auth', v0, alice, await frontier(G)));
                    const d = await (await G.getTable('admins')).delete(await adminRow(G, alice, base), bob, base);
                    const y = await G.observe('auth', v1, alice, base);
                    assertTrue(await verdict(G, y) === 'live', `${gate}: the import of the revoke is live`);
                    assertTrue(await verdict(G, d) === 'restriction', `${gate}: with it, bob's delete is void`);
                }
            }
        },
        {
            name: '[OBSFRAME21] a void observe\'s reason notes that its version still advanced the horizon, and a collapsed one is contested',
            invoke: async () => {
                const gate = { kind: 'observe-gate' as const, binding: 'auth', rule: IN_ADMINS };
                const plain = formatOpVoidDetail(gate);
                assertTrue(plain.startsWith("canObserve predicate rejected observation of 'auth'") && !plain.includes('horizon'),
                    'without a context the reason is unchanged');
                assertTrue(formatOpVoidDetail(gate, { observe: { binding: 'auth' } }) === `${plain}; its version still advanced the horizon of 'auth'`,
                    'a void observe advanced the horizon of its binding');
                assertTrue(formatOpVoidDetail({ kind: 'undecided-cycle' }, { observe: { binding: 'auth' } })
                    === "contested: undecided dependency cycle (denied); observe again from the merge; its version still advanced the horizon of 'auth'",
                    'a collapsed observe is contested');
                assertTrue(formatOpVoidDetail({ kind: 'undecided-cycle' }, { observe: {} }).endsWith('its version still advanced the horizon'),
                    'the binding is named only when known');
                assertTrue(formatOpVoidDetail({ kind: 'undecided-cycle' }) === 'undecided dependency cycle (denied)', 'a row op is unchanged');
            }
        },
        {
            name: '[OBSFRAME22] a pending deploy that writes the slot of a column only one arm of an exists names leaves the other arm decided, as two atoms do; a leaf that names the column is touched',
            invoke: async () => {
                const ctx = newCtx();
                // caps (grantee, role), role defaulting to member. The deploy drops role
                // and adds it again with the same default: it writes role's slot alone.
                const tables = authTables().map((t) => t.name !== CAPS_TABLE ? t : {
                    ...t, columns: { grantee: { type: 'string' as const, pub: true }, role: { type: 'string' as const, pub: true, default: 'member' } },
                });
                const resetRole: MigrationRule[] = [
                    { rule: 'drop-column', table: CAPS_TABLE, column: 'role' },
                    { rule: 'add-column', table: CAPS_TABLE, column: 'role', def: { type: 'string', pub: true, default: 'member' } },
                ];
                // bob deletes alice's admins row while alice imports the deploy
                const outcome = async (seed: string, adminsRule: Predicate) => {
                    const [root, alice, bob] = [await makeIdentity(), await makeIdentity(), await makeIdentity()];
                    const { K, schema } = await makeAuth(ctx, `${seed}-k`, { creator: root, identities: [root, alice, bob], caps: [root, alice, bob], tables });
                    const v0 = await frontier(K);
                    const vD = await deployOn(K, schema, root, resetRole, v0);
                    const G = await loopObserver(ctx, `${seed}-g`, { creator: root, K, H: await makeData(ctx, `${seed}-h`, root), admins: [root, alice], adminsRule });
                    const base = version(await G.observe('auth', v0, root, await frontier(G)));
                    const d = await (await G.getTable('admins')).delete(await adminRow(G, alice, base), bob, base);
                    const y = await G.observe('auth', vD, alice, base);
                    return [await verdict(G, d), await verdict(G, y)].join(' ');
                };
                const anyCap = { grantee: '$author' };
                const adminCap = { grantee: '$author', role: 'admin' };
                const memberCap = { grantee: '$author', role: 'member' };
                const caps = `auth.${CAPS_TABLE}`;

                // EXISTS (auth.caps WHERE grantee = $author OR (grantee = $author AND role = 'admin'))
                assertTrue(await outcome('of22-tree', { p: 'exists', table: caps, where: { $or: [anyCap, adminCap] } }) === 'live observe-gate',
                    'as one EXISTS, the arm that names only grantee decides the delete');
                assertTrue(await outcome('of22-tree-rev', { p: 'exists', table: caps, where: { $or: [adminCap, anyCap] } }) === 'live observe-gate',
                    'in either order of the arms');
                assertTrue(await outcome('of22-atoms', { p: 'or', args: [
                    { p: 'exists', table: caps, where: anyCap }, { p: 'exists', table: caps, where: adminCap },
                ] }) === 'live observe-gate', 'as two EXISTS atoms, the same');
                assertTrue(await outcome('of22-leaf', { p: 'exists', table: caps, where: memberCap }) === 'undecided-cycle undecided-cycle',
                    'a leaf that names role is touched: the delete and the import collapse');
                assertTrue(await outcome('of22-both', { p: 'exists', table: caps, where: { $or: [memberCap, adminCap] } }) === 'undecided-cycle undecided-cycle',
                    'and so is a tree whose every arm names role');
            }
        },
        {
            name: '[OBSFRAME01] a revoke in K imported by an observe of K concurrent with a back-dated observe of H voids it; imported after it, it does not',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const alice = await makeIdentity();
                const K = await makeUsers(ctx, 'of01-k', { identities: [admin, alice], managers: [admin, alice] });
                const H = await makeData(ctx, 'of01-h', admin);
                const v0 = await frontier(K);
                assertTrue(await revokeCap(K, admin, alice.keyId, USERS_MANAGER_LABEL) !== undefined, 'admin revokes alice');
                const v1 = await frontier(K);
                const h = await frontier(H);

                for (const after of [false, true]) {
                    const G = await makeObserver(ctx, `of01-g-${after}`, {
                        creator: admin, bindings: { target: H.getId(), auth: K.getId() },
                        canObserve: { target: MANAGER_IN_AUTH }, idProvider: 'auth.identities',
                    });
                    const base = version(await G.observe('auth', v0, undefined, await frontier(G)));
                    const y = await G.observe('target', h, alice, base);   // admitted: alice is a manager at v0
                    await G.observe('auth', v1, undefined, after ? version(y) : base);
                    if (after) {
                        assertTrue(await verdict(G, y) === 'live', 'imported after the observe, the revoke leaves it live');
                    } else {
                        assertTrue(await verdict(G, y) === 'observe-gate', 'imported concurrently, the revoke voids the back-dated observe');
                    }
                }
            }
        },
        {
            name: '[OBSFRAME02] a grant imported only by a void observe of K passes admission and does not authorize at use',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const mallory = await makeIdentity();
                const bob = await makeIdentity();
                const K = await makeUsers(ctx, 'of02-k', { identities: [admin, mallory, bob], managers: [admin] });
                assertTrue(await grantCap(K, admin, bob.keyId, USERS_MANAGER_LABEL) !== undefined, 'admin grants bob a manager cap');
                const v1 = await frontier(K);
                const H = await makeData(ctx, 'of02-h', admin);

                // auth's gate reads G's own admins; target's reads auth.caps
                const G = await makeObserver(ctx, 'of02-g', {
                    creator: admin, bindings: { target: H.getId(), auth: K.getId() },
                    tables: [adminsTable(IN_ADMINS)], admins: [admin, mallory],
                    canObserve: { auth: IN_ADMINS, target: MANAGER_IN_AUTH }, idProvider: 'auth.identities',
                });
                const base = await frontier(G);
                const w = await G.observe('auth', v1, mallory, base);   // mallory is an admin at base
                const d = await (await G.getTable('admins')).delete(await adminRow(G, mallory, base), admin, base);
                // admission's geometric fold holds w's version, with bob's grant
                const y = await G.observe('target', await frontier(H), bob, version(w, d));

                assertTrue(await verdict(G, d) === 'live', "admin's concurrent delete of mallory's admins row is live");
                assertTrue(await verdict(G, w) === 'observe-gate', "the delete kills mallory's row in w's gate, so w is void");
                assertTrue(await verdict(G, y) === 'observe-gate', 'at use the void import brings no grant, so bob is not a manager');

                // the live fold leaves w out; the geometric fold counts it
                const at = await frontier(G);
                const live = await G.resolveLiveObservedForeignVersion(K.getId(), at);
                const geometric = await G.resolveObservedForeignVersion(K.getId(), at, at);
                assertTrue(live.size === 1 && live.has(K.getId()), 'the live fold is the root of K, since w is its only observe and void');
                assertTrue([...geometric].join() === [...v1].join(), "the geometric fold holds w's version");
            }
        },
        {
            name: '[OBSFRAME03] benign concurrent observes of H by two authorized authors both stay live',
            invoke: async () => {
                const ctx = newCtx();
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const K = await makeUsers(ctx, 'of03-k', { identities: [alice, bob], managers: [alice, bob] });
                const H = await makeData(ctx, 'of03-h', alice);
                const G = await makeObserver(ctx, 'of03-g', {
                    creator: alice, bindings: { target: H.getId(), auth: K.getId() },
                    canObserve: { target: MANAGER_IN_AUTH }, idProvider: 'auth.identities',
                });
                const base = version(await G.observe('auth', await frontier(K), undefined, await frontier(G)));
                const h = await frontier(H);
                const ya = await G.observe('target', h, alice, base);
                const yb = await G.observe('target', h, bob, base);
                assertTrue(await verdict(G, ya) === 'live' && await verdict(G, yb) === 'live', 'both observes stay live');
            }
        },
        {
            name: '[OBSFRAME04] two aliases of one group read their gates through their own table maps, combined with and',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const K = await makeUsers(ctx, 'of04-k', { identities: [admin, alice, bob], managers: [admin] });
                await grantCap(K, admin, alice.keyId, 'source_sync');
                await grantCap(K, admin, bob.keyId, 'source_sync');
                await grantCap(K, admin, bob.keyId, 'target_sync');

                const G = await makeObserver(ctx, 'of04-g', {
                    creator: admin, bindings: { source: K.getId(), target: K.getId() },
                    tableMaps: { source: { grants: CAPS_TABLE } },
                    canObserve: {
                        source: { p: 'exists', table: 'source.grants', where: { label: 'source_sync', grantee: '$author' } },
                        target: { p: 'exists', table: `target.${CAPS_TABLE}`, where: { label: 'target_sync', grantee: '$author' } },
                    },
                    idProvider: 'source.identities',
                });
                const k = await frontier(K);
                await expectRejected(() => G.observe('source', k, alice), "canObserve predicate rejected observation of 'source/target'",
                    'alice holds source_sync only, so the combined gate rejects');
                const y = await G.observe('target', k, bob);
                assertTrue(await verdict(G, y) === 'live', 'bob passes both gates, source.grants read through the map');
            }
        },
        {
            name: '[OBSFRAME05] walkthrough 1: a gate over the observer\'s admins, whose rules read only admins, reads what any reader of admins sees',
            invoke: async () => {
                const ctx = newCtx();
                const root = await makeIdentity();
                const carol = await makeIdentity();
                const K = await makeUsers(ctx, 'of05-k', { identities: [root, carol], managers: [root] });
                const H = await makeData(ctx, 'of05-h', root);
                const G = await makeObserver(ctx, 'of05-g', {
                    creator: root, bindings: { target: H.getId(), auth: K.getId() },
                    tables: [adminsTable(IN_ADMINS)], admins: [root],
                    canObserve: { target: IN_ADMINS }, idProvider: 'auth.identities',
                });
                const h = await frontier(H);
                const yr = await G.observe('target', h, root);
                assertTrue(await verdict(G, yr) === 'live', 'root, an admin, observes');
                await expectRejected(() => G.observe('target', h, carol), "canObserve predicate rejected observation of 'target'",
                    'carol is not an admin');
                await (await G.getTable('admins')).insert('of05-carol', { key: carol.keyId }, root);
                const yc = await G.observe('target', h, carol);
                assertTrue(await verdict(G, yc) === 'live', 'once root adds her to admins, carol observes');
            }
        },
        {
            name: '[OBSFRAME06] walkthrough 2: admins\' rules read an ungated binding, so nothing is pending and the gate reads what ordinary views read',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const K = await makeUsers(ctx, 'of06-k', { identities: [admin, alice, bob], managers: [admin, alice] });
                const H = await makeData(ctx, 'of06-h', admin);
                const G = await makeObserver(ctx, 'of06-g', {
                    creator: admin, bindings: { target: H.getId(), auth: K.getId() },
                    tables: [adminsTable(MANAGER_IN_AUTH)],
                    canObserve: { target: IN_ADMINS }, idProvider: 'auth.identities',
                });
                await G.observe('auth', await frontier(K));
                await (await G.getTable('admins')).insert('of06-alice', { key: alice.keyId }, alice);
                await expectRejected(() => G.getTable('admins').then((t) => t.insert('of06-bob', { key: bob.keyId }, bob)),
                    'does not satisfy ALLOW insert IF', 'bob, not a manager in auth, cannot add himself');
                const y = await G.observe('target', await frontier(H), alice);
                assertTrue(await verdict(G, y) === 'live', 'alice, admitted through auth.caps, observes');
            }
        },
        {
            name: '[OBSFRAME07] a delete in the observer concurrent with an observe voids it, as it voids a row op\'s guard',
            invoke: async () => {
                const ctx = newCtx();
                const root = await makeIdentity();
                const carol = await makeIdentity();
                const dave = await makeIdentity();
                const K = await makeUsers(ctx, 'of07-k', { identities: [root, carol, dave], managers: [root] });
                const H = await makeData(ctx, 'of07-h', root);
                const G = await makeObserver(ctx, 'of07-g', {
                    creator: root, bindings: { target: H.getId(), auth: K.getId() },
                    tables: [adminsTable(IN_ADMINS)], admins: [root, carol],
                    canObserve: { target: IN_ADMINS }, idProvider: 'auth.identities',
                });
                const base = await frontier(G);
                const y = await G.observe('target', await frontier(H), carol, base);
                const op = await (await G.getTable('admins')).insert('of07-dave', { key: dave.keyId }, carol, base);
                const d = await (await G.getTable('admins')).delete(await adminRow(G, carol, base), root, base);
                assertTrue(await verdict(G, d) === 'live', "root's delete of carol's row is live");
                assertTrue(await verdict(G, op) === 'restriction', "carol's concurrent row op is void by its guard");
                assertTrue(await verdict(G, y) === 'observe-gate', "carol's concurrent observe is void by its gate");
            }
        },
        {
            name: '[OBSFRAME08] admission reads the observer\'s tables at the observe\'s own position',
            invoke: async () => {
                const ctx = newCtx();
                const root = await makeIdentity();
                const carol = await makeIdentity();
                const K = await makeUsers(ctx, 'of08-k', { identities: [root, carol], managers: [root] });
                const H = await makeData(ctx, 'of08-h', root);
                const G = await makeObserver(ctx, 'of08-g', {
                    creator: root, bindings: { target: H.getId(), auth: K.getId() },
                    tables: [adminsTable(IN_ADMINS)], admins: [root, carol],
                    canObserve: { target: IN_ADMINS }, idProvider: 'auth.identities',
                });
                const base = await frontier(G);
                const d = await (await G.getTable('admins')).delete(await adminRow(G, carol, base), root, base);
                const h = await frontier(H);
                await expectRejected(() => G.observe('target', h, carol, version(d)), "canObserve predicate rejected observation of 'target'",
                    'after the delete, carol fails admission');
                const y = await G.observe('target', h, carol, base);
                assertTrue(await verdict(G, y) === 'observe-gate', 'written before the delete, it is admitted and void at use');
            }
        },
        {
            name: '[OBSFRAME09] creation rejects a gate-graph cycle, a gate atom on an unbound alias and a gate over a table the schema lacks',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const K1 = await makeUsers(ctx, 'of09-k1', { identities: [admin], managers: [admin] });
                const K2 = await makeUsers(ctx, 'of09-k2', { identities: [admin], managers: [admin] });
                const reads = (alias: string): Predicate => ({ p: 'exists', table: `${alias}.${CAPS_TABLE}`, where: { grantee: '$author' } });

                await expectRejected(() => makeObserver(ctx, 'of09-cycle', {
                    creator: admin, bindings: { a: K1.getId(), b: K2.getId() },
                    canObserve: { a: reads('b'), b: reads('a') }, idProvider: 'a.identities',
                }), 'the ALLOW UPDATE REF gates read each other in a cycle', 'gates that read each other form a cycle');

                await expectRejected(() => makeObserver(ctx, 'of09-unbound', {
                    creator: admin, bindings: { a: K1.getId() },
                    canObserve: { a: reads('x') }, idProvider: 'a.identities',
                }), "canObserve gate on 'a' reads 'x.caps', and 'x' is not bound", 'a gate atom names an unbound alias');

                await expectRejected(() => makeObserver(ctx, 'of09-missing', {
                    creator: admin, bindings: { a: K1.getId() },
                    canObserve: { a: { p: 'exists', table: 'nope', where: { key: '$author' } } }, idProvider: 'a.identities',
                }), "canObserve gate on 'a' reads table 'nope', which is not in the pinned schema", 'an unqualified table must be the group\'s own');

                const chain = await makeObserver(ctx, 'of09-chain', {
                    creator: admin, bindings: { a: K1.getId(), b: K2.getId() },
                    canObserve: { a: reads('b') }, idProvider: 'a.identities',
                });
                assertFalse(chain === undefined, 'a gate that reads another binding without a cycle is accepted');
            }
        },
        {
            name: '[OBSFRAME10] a foreign idProvider and a gate atom read through the table map',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const K = await makeUsers(ctx, 'of10-k', { identities: [admin, alice, bob], managers: [admin, alice] });
                const G = await makeObserver(ctx, 'of10-g', {
                    creator: admin, bindings: { u: K.getId() },
                    tableMaps: { u: { people: IDENTITIES_TABLE, grants: CAPS_TABLE } },
                    canObserve: { u: { p: 'exists', table: 'u.grants', where: { label: USERS_MANAGER_LABEL, grantee: '$author' } } },
                    idProvider: 'u.people',
                });
                const k = await frontier(K);
                const y = await G.observe('u', k, alice);
                assertTrue(await verdict(G, y) === 'live', 'alice is authenticated through u.people and authorized through u.grants');
                await expectRejected(() => G.observe('u', k, bob), "canObserve predicate rejected observation of 'u'",
                    'bob holds no manager cap in u.grants');
            }
        },
    ],
};

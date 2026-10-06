import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import {
    version, Version, RContext, createRefAdvancePayload, signPayload, serializePublicKeyToBase64,
    formatValidationFailure, ValidationRejectedError,
} from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { TableDef, Predicate, SchemaCreator, CreateRSchemaPayload } from "../src/rschema/payload.js";
import type { CreateTableGroupPayload } from "../src/rtable_group/payload.js";
import { killedRowIds } from "../src/rtable_group/scopes.js";
import type { RTableView } from "../src/rtable/interfaces.js";
import { computeMirrorHashes, deployGateId } from "../src/rdeploy_gate/mirror.js";
import { usersSchemaTables, IDENTITIES_TABLE, identityRow } from "../src/users/users.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// A schema deploy is a BARRIER ref-advance: a deploy concurrent to a
// use, visible from the view's `from`, revises the effective schema at the
// merged frontier. These tests pin the revision semantics for each migration
// rule: a restriction / FK / column / concurrent-deletes change activates at
// the merge exactly like a concurrent row barrier (the deploy analogue of
// [ENF08]); a deploy applied causally AFTER a use does not retroact ([ENF07]).

async function createEnv(tables: TableDef[]) {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: 'deploy:test_schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables,
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: 'deploy-test-group',
        seed: 'deploy-test-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;

    return { ctx, schema, group, admin, pinned, schemaInit, groupInit };
}

// A second replica of `group` built by sync-style ingestion: the schema's
// entries are copied as stored, the group's are re-applied through
// applyPayload, which derives each entry's meta locally.
async function replicate(
    schema: RSchemaImpl, schemaInit: CreateRSchemaPayload, group: RTableGroupImpl, groupInit: CreateTableGroupPayload,
): Promise<RTableGroupImpl> {
    const ctx = newCtx();
    const schemaCopy = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const schemaDag = await schemaCopy.getScopedDag();
    for await (const entry of (await schema.getScopedDag()).loadAllEntries()) {
        if (entry.hash === schema.getId()) continue;
        await schemaDag.append(entry.payload, entry.meta, new Set(json.fromSet(entry.header.prevEntryHashes)));
    }
    const copy = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    for await (const entry of (await group.getScopedDag()).loadAllEntries()) {
        if (entry.hash === group.getId()) continue;
        await copy.applyPayload(entry.payload, new Set(json.fromSet(entry.header.prevEntryHashes)));
    }
    return copy;
}

// permissive table: unauthored ops never void (focus on the deploy revision)
function open(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

async function groupFrontier(group: RTableGroupImpl): Promise<Version> {
    return (await group.getScopedDag()).getFrontier();
}

async function schemaFrontier(schema: RSchemaImpl): Promise<Version> {
    return (await schema.getScopedDag()).getFrontier();
}

// A member table view at an explicit (at, from) horizon — distinct from the
// [ENF] helper, which reads at (at, at): barrier revision needs from != at.
async function viewAt(group: RTableGroupImpl, name: string, at: Version, from: Version): Promise<RTableView> {
    return (await group.getView(at, from)).getTableView(name);
}

function keyOf(identity: OwnIdentity): SchemaCreator {
    return { keyId: identity.keyId, publicKey: serializePublicKeyToBase64(identity.publicKey) };
}

function authorIs(...identities: OwnIdentity[]): Predicate {
    const terms: Predicate[] = identities.map((i) => ({ p: 'cmp', cmp: 'eq', left: { lit: '$author' }, right: { lit: i.keyId } }));
    return terms.length === 1 ? terms[0] : { p: 'or', args: terms };
}

// A group over `tables` with an explicit deploy authority configuration.
async function createAuthEnv(tables: TableDef[], group: {
    canDeploy?: Predicate;
    deployKeys?: SchemaCreator[];
    idProvider?: string;
    initialRows?: { [t: string]: json.Literal[] };
}, admin: OwnIdentity, ctx: RContext = newCtx()) {
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: 'deploy:auth_schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables,
    }))) as RSchemaImpl;
    const pinned = await schemaFrontier(schema);
    const init = await RTableGroupImpl.create({
        name: 'deploy-auth-group', seed: 'deploy-auth-group',
        schemaRef: schema.getId(), schemaVersion: pinned,
        ...group,
    });
    return { ctx, schema, init, pinned };
}

function newCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    return ctx;
}

async function failureOf(fn: () => Promise<unknown>): Promise<string | undefined> {
    try {
        await fn();
        return undefined;
    } catch (e) {
        return e instanceof ValidationRejectedError ? formatValidationFailure(e.why) : (e as Error).message;
    }
}

// A deploy payload built by hand (gate hashes included), signed by `signer`.
async function signedDeploy(group: RTableGroupImpl, schema: RSchemaImpl, v: Version, signer: OwnIdentity, at: Version) {
    return signPayload({
        ...(createRefAdvancePayload(schema.getId(), v) as unknown as json.LiteralMap),
        gate: json.toSet(await group.computeGateHashes(v)),
    }, signer, at);
}

export const rtableDeployTests = {
    title: '[DEPLOY] schema deploy barrier / revision tests',
    tests: [
        {
            name: '[DEPLOY01] a concurrent deploy that adds an insert restriction voids the concurrent insert at merge',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('caps', { label: { type: 'string', pub: true } }),
                    open('items', { name: { type: 'string' } }),
                ]);
                const items = await group.getTable('items');

                // tighten items: inserts now require a 'grant' cap (none exists)
                await schema.updateSchema([{
                    rule: 'set-restrictions', table: 'items',
                    restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { label: 'grant' } } }],
                }], admin, 'gate items inserts');
                const v2 = await schemaFrontier(schema);

                const base = await groupFrontier(group);

                // concurrent: branch A deploys (barrier), branch B inserts under
                // the still-permissive v1 schema (valid at its own position)
                await group.deploy(v2, undefined, base);
                const itemId = deriveRowId('i-1');
                await items.insert('i-1', { name: 'thing' }, undefined, base);

                const merged = await groupFrontier(group);
                assertFalse(await (await viewAt(group, 'items', merged, merged)).hasRow(itemId),
                    'a concurrent deploy barrier activates the new restriction, voiding the concurrent insert at merge');
            }
        },
        {
            name: '[DEPLOY02] a deploy that activates a restriction AFTER the insert does not void it (use-before-activate)',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('caps', { label: { type: 'string', pub: true } }),
                    open('items', { name: { type: 'string' } }),
                ]);
                const items = await group.getTable('items');

                // insert first (under the permissive v1 schema)
                const itemId = deriveRowId('i-1');
                await items.insert('i-1', { name: 'thing' });

                // THEN tighten + deploy, causally after the insert
                await schema.updateSchema([{
                    rule: 'set-restrictions', table: 'items',
                    restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { label: 'grant' } } }],
                }], admin, 'gate items inserts');
                const v2 = await schemaFrontier(schema);
                await group.deploy(v2);

                const merged = await groupFrontier(group);
                assertTrue(await (await viewAt(group, 'items', merged, merged)).hasRow(itemId),
                    'a deploy that activates a restriction after the use does not retroactively void it');
            }
        },
        {
            name: '[DEPLOY03] a concurrent deploy that adds an FK voids the now-dangling dependent at merge',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                    open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }),
                ]);
                const lines = await group.getTable('lines');

                const base = await groupFrontier(group);

                // branch B: insert a line referencing a non-existent order. Valid
                // under v1 (no FK declared yet), so it is a live row at its position.
                const ghost = deriveRowId('ghost-order');
                const lineId = deriveRowId('l-1');
                const lineEntry = await lines.insert('l-1', { order: ghost, qty: 1 }, undefined, base);
                const linePos = version(lineEntry);

                // branch A (concurrent): deploy an FK lines.order -> orders
                await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } }], admin, 'add fk');
                const v2 = await schemaFrontier(schema);
                await group.deploy(v2, undefined, base);

                const merged = await groupFrontier(group);

                // at the line's own position the FK is not yet observed -> live
                assertTrue(await (await viewAt(group, 'lines', linePos, linePos)).hasRow(lineId),
                    'before the FK deploy is observed the line is live');
                // observed from the merged frontier the concurrent deploy barrier
                // widens the op's position so the new FK is in scope -> the
                // dangling line's insert is voided at-use (drop-on-void)
                assertFalse(await (await viewAt(group, 'lines', linePos, merged)).hasRow(lineId),
                    'a concurrent FK deploy voids the now-dangling line at the merged frontier');
            }
        },
        {
            name: '[DEPLOY04] a concurrent add-column with a default resolves the default for old rows at merge',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                ]);
                const orders = await group.getTable('orders');

                const base = await groupFrontier(group);

                // branch B: insert an order under v1 (no `status` column)
                const orderId = deriveRowId('o-1');
                const orderEntry = await orders.insert('o-1', { customer: 'ada' }, undefined, base);
                const orderPos = version(orderEntry);

                // branch A (concurrent): deploy add-column status with a default
                await schema.updateSchema([{
                    rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'new' },
                }], admin, 'add status');
                const v2 = await schemaFrontier(schema);
                await group.deploy(v2, undefined, base);

                const merged = await groupFrontier(group);

                const before = await (await viewAt(group, 'orders', orderPos, orderPos)).getRow(orderId);
                assertTrue(before!.values['status'] === undefined,
                    'the column does not exist before the deploy is observed');

                const after = await (await viewAt(group, 'orders', orderPos, merged)).getRow(orderId);
                assertEquals(after!.values['status'], 'new',
                    'a concurrent add-column default activates for the old row at the merged frontier');
            }
        },
        {
            name: '[DEPLOY05] a set-concurrent-deletes deploy is observed and takes behavioral effect',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('items', { name: { type: 'string' } }, { concurrentDeletes: false }),
                ]);
                const items = await group.getTable('items');

                assertFalse((await group.getView()).getSchemaView().getConcurrentDeletes('items'),
                    'items starts with concurrentDeletes = false');

                // deploy: flip the flag to true (sequential, structural revision)
                await schema.updateSchema([{ rule: 'set-concurrent-deletes', table: 'items', value: true }], admin, 'flip cd');
                const v2 = await schemaFrontier(schema);
                await group.deploy(v2);

                assertTrue((await group.getView()).getSchemaView().getConcurrentDeletes('items'),
                    'the deploy revises concurrentDeletes to true at the frontier');

                // behavioral: a delete written under the revised flag is a barrier,
                // so it reaches a concurrent reader at the merged frontier (cf. [ENF08]).
                const itemId = deriveRowId('i-1');
                await items.insert('i-1', { name: 'thing' });
                const base = await groupFrontier(group);

                await items.delete(itemId, undefined, base);                       // branch A: barrier delete
                const otherEntry = await items.insert('i-2', { name: 'other' }, undefined, base);   // branch B
                const otherPos = version(otherEntry);

                const merged = await groupFrontier(group);
                // read on branch B (where the delete is genuinely concurrent, not
                // causal) observed from the merge: the deployed concurrentDeletes
                // barrier reach voids the row.
                assertFalse(await (await viewAt(group, 'items', otherPos, merged)).hasRow(itemId),
                    'under the deployed concurrentDeletes=true, the concurrent barrier delete voids the row at merge');
            }
        },
        {
            name: '[DEPLOY06] a delete authored under concurrentDeletes=false is retroactively honored once a concurrent enable deploy is observed',
            invoke: async () => {
                // The payoff of always-tagging deletes: the barrier tag is baked
                // unconditionally at write, so a delete authored while the flag is
                // OFF is still honored once the (at, from)-resolved flag turns ON.
                const { group, schema, admin } = await createEnv([
                    open('items', { name: { type: 'string' } }, { concurrentDeletes: false }),
                ]);
                const items = await group.getTable('items');

                const itemId = deriveRowId('i-1');
                await items.insert('i-1', { name: 'thing' });   // flag is false here
                const base = await groupFrontier(group);

                // branch A: delete i-1 while concurrentDeletes is STILL false. Under
                // the old write-time gate this delete would carry no barrier tag;
                // now it is tagged unconditionally.
                await items.delete(itemId, undefined, base);
                // branch B: a concurrent reader insert (the delete is genuinely
                // concurrent to this position, not causal).
                const otherEntry = await items.insert('i-2', { name: 'other' }, undefined, base);
                const otherPos = version(otherEntry);

                // frontier with the delete + reader but NOT the deploy yet
                const preDeploy = await groupFrontier(group);

                // branch C (concurrent): deploy that enables concurrentDeletes
                await schema.updateSchema([{ rule: 'set-concurrent-deletes', table: 'items', value: true }], admin, 'enable cd');
                const v2 = await schemaFrontier(schema);
                await group.deploy(v2, undefined, base);
                const merged = await groupFrontier(group);

                // observed from a horizon that has the delete but NOT the enable
                // deploy: the flag resolves false, so the tag is ignored -> live.
                // (proves the tag alone is inert; the view-time flag is authority)
                assertTrue(await (await viewAt(group, 'items', otherPos, preDeploy)).hasRow(itemId),
                    'with concurrentDeletes still false at the horizon, the tagged delete is not honored -> row live');

                // observed from the merged frontier the enable deploy resolves the
                // flag true, retroactively honoring the already-tagged delete -> void.
                assertFalse(await (await viewAt(group, 'items', otherPos, merged)).hasRow(itemId),
                    'once a concurrent enable deploy is observed the pre-tagged delete is honored -> row voids at merge');
            }
        },
        {
            name: '[DEPLOY07] an add-fk deploy is accepted and deletes the existing row it strands',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                    open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }),
                ]);
                const orders = await group.getTable('orders');
                const lines = await group.getTable('lines');

                // a pre-existing line referencing a non-existent order: valid under
                // v1 (no FK declared), so it is a live row
                const lineId = deriveRowId('l-1');
                await lines.insert('l-1', { order: deriveRowId('ghost-order'), qty: 1 });
                await orders.insert('o-1', { customer: 'ada' });
                const pre = await groupFrontier(group);

                // deploy add-fk lines.order -> orders: the existing row dangles
                // under the new FK, so the deploy deletes it
                await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } }], admin, 'add fk');
                const deployHash = await group.deploy(await schemaFrontier(schema));
                const merged = await groupFrontier(group);

                const entry = (await (await group.getScopedDag()).loadEntry(deployHash))!;
                assertEquals(killedRowIds(entry, 'lines').join(','), lineId, 'the deploy entry carries the stranded row');
                assertTrue(await (await viewAt(group, 'lines', pre, pre)).hasRow(lineId),
                    'views before the deploy still show the row');
                assertTrue(await (await viewAt(group, 'lines', pre, merged)).hasRow(lineId),
                    'the deploy is causally after `pre`, so observing it from the merge does not revise `pre`');
                assertFalse(await (await viewAt(group, 'lines', merged, merged)).hasRow(lineId),
                    'from the deploy on, the stranded row is dead');

                const update = await failureOf(() => lines.update(lineId, { qty: 2 }));
                assertTrue(update?.includes('is not live') ?? false, `updating the deleted row is rejected, got: ${update}`);
                const reinsert = await failureOf(() => lines.insert('l-1', { order: deriveRowId('o-1'), qty: 1 }));
                assertTrue(reinsert?.includes('already exists or was deleted') ?? false,
                    `re-inserting the deleted rowId is rejected, got: ${reinsert}`);
            }
        },
        {
            name: '[DEPLOY08] an add-fk deploy deletes nothing when every existing row honors the new FK',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                    open('lines', { order: { type: 'string', nullable: true }, qty: { type: 'integer' } }),
                ]);
                const orders = await group.getTable('orders');
                const lines = await group.getTable('lines');

                const orderId = deriveRowId('o-1');
                const lineId = deriveRowId('l-1');
                const unsetId = deriveRowId('l-2');
                await orders.insert('o-1', { customer: 'ada' });
                await lines.insert('l-1', { order: orderId, qty: 1 });   // honors the future FK
                await lines.insert('l-2', { qty: 1 });                   // no value: unconstrained

                await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } }], admin, 'add fk');
                const deployHash = await group.deploy(await schemaFrontier(schema));

                const entry = (await (await group.getScopedDag()).loadEntry(deployHash))!;
                assertTrue(entry.meta['tables'] === undefined, 'the deploy entry lists no table to delete from');
                const merged = await groupFrontier(group);
                assertTrue(await (await viewAt(group, 'lines', merged, merged)).hasRow(lineId),
                    'a row that honored the new FK at the deploy stays live afterwards');
                assertTrue(await (await viewAt(group, 'lines', merged, merged)).hasRow(unsetId),
                    'a row without a value in the FK column stays live');
            }
        },
        {
            name: '[DEPLOY09] a deploy carries its gate hashes, depends on them, and a wrong gate is rejected',
            invoke: async () => {
                const { ctx, group, schema, admin } = await createEnv([open('items', { name: { type: 'string' } })]);
                await schema.updateSchema([{ rule: 'add-column', table: 'items', column: 'size', def: { type: 'integer', nullable: true } }], admin);
                const v2 = await schemaFrontier(schema);

                assertEquals(group.getDeployGateId(), deployGateId(group.getId(), schema.getId()), 'the gate id derives from the group and schema');
                assertTrue(await ctx.getObject(group.getDeployGateId()) === undefined, 'no gate exists on this replica');

                const expected = await computeMirrorHashes(await schema.getScopedDag(), group.getDeployGateId(), v2);
                const at = await groupFrontier(group);
                const h = await group.deploy(v2, undefined, at);
                const stored = (await (await group.getScopedDag()).loadEntry(h))!.payload as json.LiteralMap;
                assertEquals([...json.fromSet(stored['gate'] as json.Set)].sort().join(','), expected.join(','),
                    'the deploy carries the mirrors of its version');
                assertTrue((await group.getView()).getSchemaView().getTable('items')!.columns['size'] !== undefined,
                    'a local deploy applies without any gate');

                const deps = group.extractForeignDeps(stored, at)!;
                assertEquals(deps.length, 2, 'a deploy depends on its schema and its gate');
                assertEquals(deps[0].objectId, schema.getId(), 'the schema dep comes first');
                assertEquals(deps[1].objectId, group.getDeployGateId(), 'the gate dep names the gate');
                assertEquals(deps[1].requiredHashes.sort().join(','), expected.join(','), 'the gate dep requires the mirrors');

                const wrong = { ...stored, gate: json.toSet([group.getDeployGateId()]) };
                assertFalse((await group.validatePayload(wrong, at)).valid, 'a gate that does not mirror the version is rejected');
                const missing = { ...stored };
                delete missing['gate'];
                assertFalse((await group.validatePayload(missing, at)).valid, 'a deploy without gate hashes is rejected');
                assertTrue((await group.validatePayload(stored, at)).valid, 'the original deploy validates at its position');
            }
        },
        {
            name: '[DEPLOY10] embedded deploy keys verify deploy signatures without a provider',
            invoke: async () => {
                const admin = await makeIdentity();
                const stranger = await makeIdentity();
                const { ctx, schema, init } = await createAuthEnv([open('items', { name: { type: 'string' } })],
                    { canDeploy: authorIs(admin), deployKeys: [keyOf(admin)] }, admin);
                const group = (await ctx.createObject(init)) as RTableGroupImpl;

                await schema.updateSchema([{ rule: 'add-column', table: 'items', column: 'x', def: { type: 'string', nullable: true } }], admin);
                const v2 = await schemaFrontier(schema);
                const at = await groupFrontier(group);

                const byStranger = await failureOf(() => group.deploy(v2, stranger, at));
                assertTrue(byStranger?.includes('could not be verified') ?? false, `an unlisted key is rejected at the signature, got: ${byStranger}`);

                const forged = { ...(await signedDeploy(group, schema, v2, stranger, at)), author: admin.keyId };
                assertFalse((await group.validatePayload(forged, at)).valid, "a stranger's signature claiming the admin is rejected");

                const tampered = { ...(await signedDeploy(group, schema, v2, admin, at)), note: 'added after signing' };
                const tamperedResult = await group.validatePayload(tampered, at);
                assertFalse(tamperedResult.valid, 'a tampered deploy is rejected');
                assertTrue(!tamperedResult.valid && formatValidationFailure(tamperedResult.why).includes('could not be verified'),
                    'the tampered deploy fails its signature');

                await group.deploy(v2, admin, at);
                assertTrue((await group.getView()).getSchemaView().getTable('items')!.columns['x'] !== undefined,
                    'the listed admin deploys');
            }
        },
        {
            name: '[DEPLOY11] deploy keys resolve through the provider, then the embedded keys',
            invoke: async () => {
                const viaProvider = await makeIdentity();
                const viaKeys = await makeIdentity();
                const neither = await makeIdentity();
                const { ctx, schema, init } = await createAuthEnv(
                    [...usersSchemaTables(), open('items', { name: { type: 'string' } })],
                    {
                        idProvider: IDENTITIES_TABLE,
                        canDeploy: authorIs(viaProvider, viaKeys, neither),
                        deployKeys: [keyOf(viaKeys)],
                        initialRows: { [IDENTITIES_TABLE]: [identityRow('provider-key', viaProvider)] },
                    }, viaProvider);
                const group = (await ctx.createObject(init)) as RTableGroupImpl;

                await schema.updateSchema([{ rule: 'add-column', table: 'items', column: 'a', def: { type: 'string', nullable: true } }], viaProvider);
                const v2 = await schemaFrontier(schema);
                await group.deploy(v2, viaProvider);

                await schema.updateSchema([{ rule: 'add-column', table: 'items', column: 'b', def: { type: 'string', nullable: true } }], viaProvider);
                const v3 = await schemaFrontier(schema);
                await group.deploy(v3, viaKeys);

                await schema.updateSchema([{ rule: 'add-column', table: 'items', column: 'c', def: { type: 'string', nullable: true } }], viaProvider);
                const v4 = await schemaFrontier(schema);
                const byNeither = await failureOf(() => group.deploy(v4, neither));
                assertTrue(byNeither?.includes('could not be verified') ?? false,
                    `a key in neither source is rejected even when canDeploy names it, got: ${byNeither}`);

                const columns = (await group.getView()).getSchemaView().getTable('items')!.columns;
                assertTrue(columns['a'] !== undefined && columns['b'] !== undefined && columns['c'] === undefined,
                    'the provider key and the embedded key both deployed');
            }
        },
        {
            name: '[DEPLOY12] a canDeploy over $author needs a key source at create',
            invoke: async () => {
                const admin = await makeIdentity();
                const tables = [open('items', { name: { type: 'string' } })];

                const bare = await createAuthEnv(tables, { canDeploy: authorIs(admin) }, admin);
                const bareFailure = await failureOf(() => bare.ctx.createObject(bare.init));
                assertTrue(bareFailure?.includes('requires an idProvider or deployKeys') ?? false,
                    `a $author predicate without a key source is rejected, got: ${bareFailure}`);

                const frozen = await createAuthEnv(tables, { canDeploy: { p: 'false' } }, admin);
                assertTrue(await failureOf(() => frozen.ctx.createObject(frozen.init)) === undefined,
                    'a predicate that does not read $author needs no key source');

                const badKey = await createAuthEnv(tables, {
                    canDeploy: authorIs(admin),
                    deployKeys: [{ keyId: 'not-the-key-id', publicKey: keyOf(admin).publicKey }],
                }, admin);
                const badKeyFailure = await failureOf(() => badKey.ctx.createObject(badKey.init));
                assertTrue(badKeyFailure?.includes('does not match its public key') ?? false,
                    `deploy keys must be self-certifying, got: ${badKeyFailure}`);
            }
        },
        {
            name: '[DEPLOY13] a branch concurrent to an add-fk deploy sees its deletion at the merge iff concurrentDeletes is on',
            invoke: async () => {
                for (const concurrentDeletes of [true, false]) {
                    const { group, schema, admin } = await createEnv([
                        open('orders', { customer: { type: 'string' } }),
                        open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }, { concurrentDeletes }),
                    ]);
                    const lines = await group.getTable('lines');
                    const lineId = deriveRowId('l-1');
                    await lines.insert('l-1', { order: deriveRowId('ghost-order'), qty: 1 });
                    const base = await groupFrontier(group);

                    // branch A deploys the FK (deleting l-1); branch B updates l-1,
                    // which is live at its own position
                    await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } }], admin, 'add fk');
                    await group.deploy(await schemaFrontier(schema), undefined, base);
                    const updatePos = version(await lines.update(lineId, { qty: 2 }, undefined, base));
                    const merged = await groupFrontier(group);

                    assertTrue(await (await viewAt(group, 'lines', updatePos, updatePos)).hasRow(lineId),
                        `cd=${concurrentDeletes}: the row is live on the branch before the merge`);
                    assertEquals(await (await viewAt(group, 'lines', updatePos, merged)).hasRow(lineId), !concurrentDeletes,
                        `cd=${concurrentDeletes}: observed from the merge, the branch sees the deletion only when concurrentDeletes is on`);
                    assertFalse(await (await viewAt(group, 'lines', merged, merged)).hasRow(lineId),
                        `cd=${concurrentDeletes}: at the merge itself the row is dead`);
                }
            }
        },
        {
            name: '[DEPLOY14] a deploy that re-creates the FK column deletes nothing',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                    open('lines', { order: { type: 'string', nullable: true }, qty: { type: 'integer' } }),
                ]);
                const lines = await group.getTable('lines');
                const lineId = deriveRowId('l-1');
                await lines.insert('l-1', { order: deriveRowId('ghost-order'), qty: 1 });

                await schema.updateSchema([
                    { rule: 'drop-column', table: 'lines', column: 'order' },
                    { rule: 'add-column', table: 'lines', column: 'order', def: { type: 'string', nullable: true } },
                    { rule: 'set-fks', table: 'lines', fks: { order: 'orders' } },
                ], admin, 'recreate order with fk');
                const deployHash = await group.deploy(await schemaFrontier(schema));

                const entry = (await (await group.getScopedDag()).loadEntry(deployHash))!;
                assertTrue(entry.meta['tables'] === undefined, 'the deploy deletes no row');
                const merged = await groupFrontier(group);
                const row = await (await viewAt(group, 'lines', merged, merged)).getRow(lineId);
                assertTrue(row !== undefined && row.values['order'] === undefined,
                    'the old row is live and holds no value in the re-created column');
            }
        },
        {
            name: '[DEPLOY15] a deploy deletion is permanent and scoped to the table incarnation',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                    open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }),
                ]);
                const lines = await group.getTable('lines');
                const lineId = deriveRowId('l-1');
                const ghost = deriveRowId('ghost-order');
                await lines.insert('l-1', { order: ghost, qty: 1 });

                await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } }], admin, 'add fk');
                await group.deploy(await schemaFrontier(schema));
                await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: {} }], admin, 'drop fk');
                await group.deploy(await schemaFrontier(schema));

                const afterDrop = await groupFrontier(group);
                assertFalse(await (await viewAt(group, 'lines', afterDrop, afterDrop)).hasRow(lineId),
                    'dropping the FK again does not revive the deleted row');

                await schema.updateSchema([
                    { rule: 'drop-table', table: 'lines' },
                    { rule: 'add-table', def: open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }) },
                ], admin, 'reset lines');
                await group.deploy(await schemaFrontier(schema));
                await lines.insert('l-1', { order: ghost, qty: 3 });

                const reset = await groupFrontier(group);
                const row = await (await viewAt(group, 'lines', reset, reset)).getRow(lineId);
                assertTrue(row !== undefined && row.values['qty'] === 3,
                    'after a table reset the rowId is free: the re-insert is a live row');
            }
        },
        {
            name: '[DEPLOY16] both delta strategies report a deploy deletion of a row the window does not write',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                    open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }),
                ]);
                const orders = await group.getTable('orders');
                const lines = await group.getTable('lines');
                const strandedId = deriveRowId('l-1');
                await lines.insert('l-1', { order: deriveRowId('ghost-order'), qty: 1 });
                await orders.insert('o-1', { customer: 'ada' });
                await lines.insert('l-2', { order: deriveRowId('o-1'), qty: 1 });
                const start = await groupFrontier(group);

                await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } }], admin, 'add fk');
                await group.deploy(await schemaFrontier(schema));
                const end = await groupFrontier(group);

                for (const strategy of ['bounded', 'full'] as const) {
                    group.setDeltaStrategy(strategy);
                    const delta = await group.computeDelta(start, end);
                    const rows = delta.tableChanges.get(lines.getId())?.rowChanges ?? [];
                    assertEquals(JSON.stringify(rows.map((r) => [r.rowId, r.liveBefore, r.liveAfter])),
                        JSON.stringify([[strandedId, true, false]]),
                        `${strategy}: the stranded row is the only row change, and it goes from live to dead`);
                }
            }
        },
        {
            name: '[DEPLOY17] sync ingestion derives the same deploy meta as the local deploy',
            invoke: async () => {
                const { group, schema, admin, schemaInit, groupInit } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                    open('lines', { order: { type: 'string' }, qty: { type: 'integer' } }),
                ]);
                const orders = await group.getTable('orders');
                const lines = await group.getTable('lines');
                await orders.insert('o-1', { customer: 'ada' });
                await lines.insert('l-1', { order: deriveRowId('ghost-order'), qty: 1 });
                await lines.insert('l-2', { order: deriveRowId('o-1'), qty: 1 });
                const base = await groupFrontier(group);

                // the deploy sits beside a concurrent dangling insert, which it
                // does not delete (the at-use FK check covers it)
                await lines.insert('l-3', { order: deriveRowId('ghost-order'), qty: 1 }, undefined, base);
                await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } }], admin, 'add fk');
                const deployHash = await group.deploy(await schemaFrontier(schema), undefined, base);
                await orders.insert('o-2', { customer: 'bob' });

                const local = (await (await group.getScopedDag()).loadEntry(deployHash))!;
                assertEquals(killedRowIds(local, 'lines').join(','), deriveRowId('l-1'),
                    'the deploy deletes the row stranded at its parents only');

                const copy = await replicate(schema, schemaInit, group, groupInit);
                const copyDag = await copy.getScopedDag();
                for await (const entry of (await group.getScopedDag()).loadAllEntries()) {
                    const ingested = await copyDag.loadEntry(entry.hash);
                    assertTrue(ingested !== undefined, `entry ${entry.hash} replicates`);
                    assertEquals(json.toStringNormalized(ingested!.meta), json.toStringNormalized(entry.meta),
                        `entry ${entry.hash}: applyPayload derives the stored meta`);
                }
            }
        },
        {
            name: '[DEPLOY18] deploy deletions close over the FKs the deploy adopts, not over older ones',
            invoke: async () => {
                // a self-referencing FK: the subtree under a broken link goes, a
                // cycle of otherwise valid rows stays
                {
                    const { group, schema, admin } = await createEnv([
                        open('nodes', { parent: { type: 'string', nullable: true } }),
                    ]);
                    const nodes = await group.getTable('nodes');
                    const id = (name: string) => deriveRowId(name);
                    await nodes.insert('root', {});
                    await nodes.insert('a', { parent: id('root') });
                    await nodes.insert('b', { parent: id('a') });
                    await nodes.insert('x', { parent: id('ghost') });
                    await nodes.insert('y', { parent: id('x') });
                    await nodes.insert('z', { parent: id('y') });
                    await nodes.insert('c1', { parent: id('c2') });
                    await nodes.insert('c2', { parent: id('c1') });

                    await schema.updateSchema([{ rule: 'set-fks', table: 'nodes', fks: { parent: 'nodes' } }], admin, 'tree fk');
                    await group.deploy(await schemaFrontier(schema));

                    const view = await viewAt(group, 'nodes', await groupFrontier(group), await groupFrontier(group));
                    assertEquals((await view.liveRowIds()).join(','), ['root', 'a', 'b', 'c1', 'c2'].map(id).sort().join(','),
                        'the subtree under the broken link is deleted; the tree and the cycle stay');
                }

                // FKs adopted together chain: a row pointing at a deleted row
                // through another adopted FK is deleted too
                {
                    const { group, schema, admin } = await createEnv([
                        open('c', { name: { type: 'string' } }),
                        open('b', { y: { type: 'string' } }),
                        open('a', { x: { type: 'string' } }),
                    ]);
                    await (await group.getTable('c')).insert('c1', { name: 'one' });
                    await (await group.getTable('b')).insert('b1', { y: deriveRowId('ghost') });
                    await (await group.getTable('b')).insert('b2', { y: deriveRowId('c1') });
                    await (await group.getTable('a')).insert('a1', { x: deriveRowId('b1') });
                    await (await group.getTable('a')).insert('a2', { x: deriveRowId('b2') });

                    await schema.updateSchema([
                        { rule: 'set-fks', table: 'a', fks: { x: 'b' } },
                        { rule: 'set-fks', table: 'b', fks: { y: 'c' } },
                    ], admin, 'chain fks');
                    await group.deploy(await schemaFrontier(schema));

                    const merged = await groupFrontier(group);
                    assertEquals((await (await viewAt(group, 'b', merged, merged)).liveRowIds()).join(','), deriveRowId('b2'),
                        'the dangling b row is deleted');
                    assertEquals((await (await viewAt(group, 'a', merged, merged)).liveRowIds()).join(','), deriveRowId('a2'),
                        'the a row pointing at the deleted b row is deleted with it');
                }

                // an FK that held before the deploy keeps delete semantics: its
                // dependent stays live, dangling
                {
                    const { group, schema, admin } = await createEnv([
                        open('c', { name: { type: 'string' } }),
                        open('b', { y: { type: 'string' } }),
                        open('a', { x: { type: 'string' } }, { fks: { x: 'b' } }),
                    ]);
                    await (await group.getTable('b')).insert('b1', { y: deriveRowId('ghost') });
                    await (await group.getTable('a')).insert('a1', { x: deriveRowId('b1') });

                    await schema.updateSchema([{ rule: 'set-fks', table: 'b', fks: { y: 'c' } }], admin, 'adopt b.y');
                    await group.deploy(await schemaFrontier(schema));

                    const merged = await groupFrontier(group);
                    assertFalse(await (await viewAt(group, 'b', merged, merged)).hasRow(deriveRowId('b1')),
                        'the dangling b row is deleted');
                    assertTrue(await (await viewAt(group, 'a', merged, merged)).hasRow(deriveRowId('a1')),
                        'the a row pointing at it through the older FK stays live');
                }
            }
        },
    ],
};

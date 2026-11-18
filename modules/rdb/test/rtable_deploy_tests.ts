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

// An insert into `tasks` that carries `carried` for column `c`, concurrent with
// a deploy that drops `c` and adds it again as a foreign key to `projects` with
// the project 'inbox' as its default. 'p1' stands for a live project. Returns
// why the insert is void after the merge (undefined when live) and the row, on
// the writer and on a second replica built by re-applying the entries.
async function columnReadd(opts: { carried: 'some text' | 'p1' | null; deleteDefaultTarget: boolean }) {
    const inbox = deriveRowId('inbox');
    const { group, schema, admin, schemaInit, groupInit } = await createEnv([
        open('projects', { name: { type: 'string' } }),
        open('tasks', { title: { type: 'string' }, c: { type: 'string', nullable: true } }),
    ]);
    const projects = await group.getTable('projects');
    await projects.insert('inbox', { name: 'inbox' });
    await projects.insert('p1', { name: 'p1' });
    if (opts.deleteDefaultTarget) await projects.delete(inbox);

    await schema.updateSchema([
        { rule: 'drop-column', table: 'tasks', column: 'c' },
        { rule: 'add-column', table: 'tasks', column: 'c', def: { type: 'string', nullable: true, default: inbox } },
        { rule: 'set-fks', table: 'tasks', fks: { c: 'projects' } },
    ], admin, 're-add c');
    const base = await groupFrontier(group);

    const deployHash = await group.deploy(await schemaFrontier(schema), undefined, base);
    const carried = opts.carried === 'p1' ? deriveRowId('p1') : opts.carried;
    const insertHash = await (await group.getTable('tasks')).insert('t', { title: 'a', c: carried }, undefined, base);
    const merged = await groupFrontier(group);

    const replicas = [];
    for (const [replica, label] of [[group, 'the writer'], [await replicate(schema, schemaInit, group, groupInit), 'a second replica']] as const) {
        replicas.push({
            label,
            reason: await replica.explainEntryVoided(insertHash, merged),
            row: await (await viewAt(replica, 'tasks', merged, merged)).getRow(deriveRowId('t')),
        });
    }
    return { inbox, replicas, group, deployHash, insertHash, merged };
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
        {
            name: '[DEPLOY19] schema params on RTableGroup: evaluation, per-param deploy fold, version-move requirement, and barrier voiding',
            invoke: async () => {
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

                const admin = await makeIdentity();
                const schemaInit = await RSchemaImpl.create({
                    name: 'dep19:schema',
                    version: '1.0.0',
                    creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
                    params: [
                        { name: 'max_qty', type: 'integer' },
                        { name: 'prefix', type: 'string', default: 'ORD-' },
                    ],
                    tables: [{
                        name: 'orders',
                        columns: { code: { type: 'string', readonly: true }, qty: { type: 'integer', readonly: true } },
                        restrictions: [{
                            on: 'insert',
                            rule: {
                                p: 'and',
                                args: [
                                    { p: 'cmp', cmp: 'le', left: { col: 'qty' }, right: { param: 'max_qty' } },
                                    { p: 'like', value: { col: 'code' }, pattern: { param: 'prefix' } },
                                ],
                            },
                        }],
                    }],
                });
                const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
                const v100 = await schemaFrontier(schema);

                const groupInit = await RTableGroupImpl.create({
                    name: 'orders_group',
                    seed: 'dep19-group',
                    schemaRef: schema.getId(),
                    schemaVersion: v100,
                    params: { max_qty: 10, prefix: 'ORD-%' },
                });
                const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
                const orders = await group.getTable('orders');

                await orders.insert('o-1', { code: 'ORD-1', qty: 10 });
                let overRejected = false;
                try { await orders.insert('o-over', { code: 'ORD-2', qty: 11 }); } catch { overRejected = true; }
                assertTrue(overRejected, 'qty > :max_qty is rejected');

                // A deploy carrying params that does NOT move the schema version is rejected
                let sameVerRejected = false;
                try { await group.deploy(v100, undefined, undefined, { max_qty: 20 }); } catch { sameVerRejected = true; }
                assertTrue(sameVerRejected, 'a deploy carrying params must advance the schema version');

                // Advance schema to 1.1.0 and deploy with max_qty = 20; then advance to 1.2.0 and deploy without touching max_qty
                const h110 = await schema.updateSchema([
                    { rule: 'add-column', table: 'orders', column: 'note', def: { type: 'string', nullable: true } },
                ], admin, { version: '1.1.0' });
                await group.deploy(version(h110), undefined, undefined, { max_qty: 20 });

                const h120 = await schema.updateSchema([
                    { rule: 'add-column', table: 'orders', column: 'tag', def: { type: 'string', nullable: true } },
                ], admin, { version: '1.2.0' });
                await group.deploy(version(h120));

                // Per-parameter fold keeps max_qty = 20 from the 1.1.0 deploy even though 1.2.0 didn't write max_qty
                assertEquals((await group.resolveGroupParams())['max_qty'], 20, 'max_qty from 1.1.0 deploy persists across 1.2.0 deploy');
                await orders.insert('o-2', { code: 'ORD-2', qty: 18 });

                // Concurrent deploy barrier that lowers max_qty to 5 voids a concurrent insert of qty = 15 at merge
                const forkAt = await groupFrontier(group);
                const concurrentOrderId = deriveRowId('o-conc');
                await orders.insert('o-conc', { code: 'ORD-3', qty: 15 }, undefined, forkAt);

                const h130 = await schema.updateSchema([
                    { rule: 'add-column', table: 'orders', column: 'extra', def: { type: 'string', nullable: true } },
                ], admin, { version: '1.3.0' });
                await group.deploy(version(h130), undefined, forkAt, { max_qty: 5 });

                const merged = await groupFrontier(group);
                assertFalse(await (await viewAt(group, 'orders', merged, merged)).hasRow(concurrentOrderId),
                    'concurrent deploy tightening :max_qty to 5 voids the concurrent insert of qty = 15');
            }
        },
        {
            name: '[DEPLOY20] a deploy after one head revises the schema at the two-head frontier',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('items', { name: { type: 'string' } }),
                ]);
                const items = await group.getTable('items');
                const base = await groupFrontier(group);
                const a = await items.insert('a-1', { name: 'a' }, undefined, base);
                const b = await items.insert('b-1', { name: 'b' }, undefined, base);

                await schema.updateSchema([{
                    rule: 'add-table',
                    def: open('notes', { body: { type: 'string' } }),
                }], admin);
                const d = await group.deploy(await schemaFrontier(schema), undefined, version(a));

                const schemaView = (await group.getView(version(a, b), version(d, b))).getSchemaView();
                assertTrue(schemaView.hasTable('notes'),
                    'the deploy is not after the other head, so it revises the schema at the frontier');
            }
        },
        {
            name: '[DEPLOY21] a deploy carrying a dominated ancestor is rejected, and a written deploy is canonical',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('items', { name: { type: 'string' } }),
                ]);
                const pinned = await schemaFrontier(schema);
                await schema.updateSchema([{
                    rule: 'add-table',
                    def: open('notes', { body: { type: 'string' } }),
                }], admin);
                const head = await schemaFrontier(schema);
                const at = await groupFrontier(group);

                const dominated = version(...pinned, ...head);
                const raw = {
                    ...(createRefAdvancePayload(schema.getId(), dominated) as unknown as json.LiteralMap),
                    gate: json.toSet(await group.computeGateHashes(dominated)),
                };
                const rejected = await group.validatePayload(raw, at);
                const why = rejected.valid ? 'valid' : formatValidationFailure(rejected.why);
                assertTrue(!rejected.valid && why.includes('not an antichain'),
                    `a deploy whose version carries the pinned genesis below its head is rejected, got: ${why}`);

                await group.deploy(version(...pinned, ...head));
                const resolved = await group.resolveSchemaVersion(await groupFrontier(group));
                assertTrue(!resolved.has(schema.getId()), 'the canonical schema version drops the genesis below the deploy');
                const shown = (await group.getView()).getSchemaView().getVersion();
                assertTrue(!shown.has(schema.getId()), 'the group view reports that canonical version');
            }
        },
        {
            name: '[DEPLOY22] an add-fk deploy keeps a row whose column is NULL',
            invoke: async () => {
                const { group, schema, admin } = await createEnv([
                    open('orders', { customer: { type: 'string' } }),
                    open('lines', { order: { type: 'string', nullable: true }, qty: { type: 'integer' } }),
                ]);
                const lines = await group.getTable('lines');

                // one row cleared a value that would have dangled, the other was inserted with a null
                const clearedId = deriveRowId('l-1');
                const nulledId = deriveRowId('l-2');
                await lines.insert('l-1', { order: deriveRowId('ghost-order'), qty: 1 });
                await lines.update(clearedId, { order: null });
                await lines.insert('l-2', { order: null, qty: 1 });

                await schema.updateSchema([{ rule: 'set-fks', table: 'lines', fks: { order: 'orders' } }], admin, 'add fk');
                const deployHash = await group.deploy(await schemaFrontier(schema));

                const entry = (await (await group.getScopedDag()).loadEntry(deployHash))!;
                assertTrue(entry.meta['tables'] === undefined, 'the deploy entry lists no table to delete from');
                const merged = await groupFrontier(group);
                const view = await viewAt(group, 'lines', merged, merged);
                assertTrue(await view.hasRow(clearedId), 'a row that nulled the column before the deploy stays live');
                assertTrue(await view.hasRow(nulledId), 'a row inserted with a null stays live');
            }
        },
        {
            name: '[DEPLOY23] an insert concurrent with a re-add of a column it set is checked on what its row reads, not on what it carried',
            invoke: async () => {
                // carried a text that is no project, while the new column's default points at a live one
                const stray = await columnReadd({ carried: 'some text', deleteDefaultTarget: false });
                for (const r of stray.replicas) {
                    assertEquals(r.reason, undefined, `${r.label}: the insert is live; the text it carried was written to the old column`);
                    assertEquals(r.row?.values['c'], stray.inbox, `${r.label}: the row reads the new column's default`);
                }

                // carried a live project, while the new column's default points at a deleted one
                const dangling = await columnReadd({ carried: 'p1', deleteDefaultTarget: true });
                for (const r of dangling.replicas) {
                    assertEquals(r.reason?.kind, 'fk', `${r.label}: the insert is void; its row would read a default with a dead target`);
                    assertTrue(r.row === undefined, `${r.label}: the row is not live`);
                }
            }
        },
        {
            name: '[DEPLOY24] the same holds for an insert that nulled the column: the null was written to the old column',
            invoke: async () => {
                const live = await columnReadd({ carried: null, deleteDefaultTarget: false });
                for (const r of live.replicas) {
                    assertEquals(r.reason, undefined, `${r.label}: the insert is live`);
                    assertEquals(r.row?.values['c'], live.inbox, `${r.label}: the row reads the new column's default, not NULL`);
                }

                const dangling = await columnReadd({ carried: null, deleteDefaultTarget: true });
                for (const r of dangling.replicas) {
                    assertEquals(r.reason?.kind, 'fk', `${r.label}: the insert is void; the null does not cover the new column`);
                    assertTrue(r.row === undefined, `${r.label}: the row is not live`);
                }
            }
        },
        {
            name: '[DEPLOY25] an operation for a table that was dropped and added again concurrently is void, and so is its bundle',
            invoke: async () => {
                for (const newRule of [{ p: 'true' }, { p: 'false' }] as Predicate[]) {
                    const { group, schema, admin, schemaInit, groupInit } = await createEnv([
                        open('tasks', { title: { type: 'string' } }),
                        open('notes', { text: { type: 'string' } }),
                    ]);
                    await schema.updateSchema([
                        { rule: 'drop-table', table: 'tasks' },
                        { rule: 'add-table', def: { name: 'tasks', columns: { title: { type: 'string' } }, restrictions: [{ on: 'all', rule: newRule }] } },
                    ], admin, 're-add tasks');
                    const base = await groupFrontier(group);

                    await group.deploy(await schemaFrontier(schema), undefined, base);
                    const bundleHash = await group.bundle([
                        { table: 'tasks', op: { action: 'insert', rowId: deriveRowId('t'), uuid: 't', values: { title: 'a' } } },
                        { table: 'notes', op: { action: 'insert', rowId: deriveRowId('n'), uuid: 'n', values: { text: 'hello' } } },
                    ], undefined, base);
                    const aloneHash = await (await group.getTable('tasks')).insert('t-alone', { title: 'a' }, undefined, base);
                    const merged = await groupFrontier(group);

                    const what = `the new table allows ${newRule.p === 'true' ? 'every' : 'no'} write`;
                    for (const [replica, label] of [[group, 'the writer'], [await replicate(schema, schemaInit, group, groupInit), 'a second replica']] as const) {
                        const view = await replica.getView(merged, merged);
                        assertTrue(await replica.isEntryVoided(bundleHash, merged), `${label}, ${what}: the bundle is void`);
                        assertFalse(await (await view.getTableView('tasks')).hasRow(deriveRowId('t')), `${label}, ${what}: its row in the reset table is not live`);
                        assertFalse(await (await view.getTableView('notes')).hasRow(deriveRowId('n')), `${label}, ${what}: nor is its row in the other table`);
                        assertTrue(await replica.isEntryVoided(aloneHash, merged), `${label}, ${what}: a single insert into the old table is void`);
                    }
                }
            }
        },
        {
            name: '[DEPLOY26] an update concurrent with a re-add of a column it set answers for no reference there, and its other columns apply',
            invoke: async () => {
                for (const deleteDefaultTarget of [false, true]) {
                    const inbox = deriveRowId('inbox');
                    const { group, schema, admin, schemaInit, groupInit } = await createEnv([
                        open('projects', { name: { type: 'string' } }),
                        open('tasks', { title: { type: 'string' }, c: { type: 'string', nullable: true } }),
                    ]);
                    const projects = await group.getTable('projects');
                    const tasks = await group.getTable('tasks');
                    await projects.insert('inbox', { name: 'inbox' });
                    await tasks.insert('t', { title: 'a' });
                    if (deleteDefaultTarget) await projects.delete(inbox);
                    await schema.updateSchema([
                        { rule: 'drop-column', table: 'tasks', column: 'c' },
                        { rule: 'add-column', table: 'tasks', column: 'c', def: { type: 'string', nullable: true, default: inbox } },
                        { rule: 'set-fks', table: 'tasks', fks: { c: 'projects' } },
                    ], admin, 're-add c');
                    const base = await groupFrontier(group);

                    await group.deploy(await schemaFrontier(schema), undefined, base);
                    const updateHash = await tasks.update(deriveRowId('t'), { title: 'b', c: 'some text' }, undefined, base);
                    const merged = await groupFrontier(group);

                    const what = `the default target is ${deleteDefaultTarget ? 'deleted' : 'live'}`;
                    for (const [replica, label] of [[group, 'the writer'], [await replicate(schema, schemaInit, group, groupInit), 'a second replica']] as const) {
                        assertFalse(await replica.isEntryVoided(updateHash, merged), `${label}, ${what}: the update is live`);
                        const row = (await (await viewAt(replica, 'tasks', merged, merged)).getRow(deriveRowId('t')))!;
                        assertEquals(row.values['title'], 'b', `${label}, ${what}: the other column it set applies`);
                        assertEquals(row.values['c'], inbox, `${label}, ${what}: the re-added column reads its default, not the text`);
                    }
                }
            }
        },
        {
            name: '[DEPLOY27] an insert rule over a readonly column is evaluated on the re-added column\'s default, not on the value the insert carried',
            invoke: async () => {
                const levelIsHigh: Predicate = { p: 'cmp', cmp: 'eq', left: { col: 'level' }, right: { lit: 'high' } };
                const docs = (level: string): TableDef => ({
                    name: 'docs',
                    columns: { title: { type: 'string' }, level: { type: 'string', readonly: true, default: level } },
                    restrictions: [{ on: 'insert', rule: levelIsHigh }],
                });
                const { group, schema, admin, schemaInit, groupInit } = await createEnv([docs('high')]);
                // a column a rule reads cannot be dropped, so the rule steps aside for the re-add
                await schema.updateSchema([
                    { rule: 'set-restrictions', table: 'docs', restrictions: [{ on: 'all', rule: { p: 'true' } }] },
                    { rule: 'drop-column', table: 'docs', column: 'level' },
                    { rule: 'add-column', table: 'docs', column: 'level', def: { type: 'string', readonly: true, default: 'low' } },
                    { rule: 'set-restrictions', table: 'docs', restrictions: [{ on: 'insert', rule: levelIsHigh }] },
                ], admin, 're-add level');
                const base = await groupFrontier(group);

                await group.deploy(await schemaFrontier(schema), undefined, base);
                const table = await group.getTable('docs');
                const carried = await table.insert('d-carried', { title: 'a', level: 'high' }, undefined, base);
                const defaulted = await table.insert('d-defaulted', { title: 'a' }, undefined, base);
                const merged = await groupFrontier(group);

                for (const [replica, label] of [[group, 'the writer'], [await replicate(schema, schemaInit, group, groupInit), 'a second replica']] as const) {
                    assertTrue(await replica.isEntryVoided(carried, merged),
                        `${label}: the insert that carried 'high' is void, since its row would read the new default 'low'`);
                    assertTrue(await replica.isEntryVoided(defaulted, merged),
                        `${label}: the insert that left the column out is void too`);
                }
            }
        },
        {
            name: '[DEPLOY28] the delta from either branch to the merge reports a re-added column the same under both strategies',
            invoke: async () => {
                const rowId = deriveRowId('t');
                const show = (v: unknown): string => v === undefined ? 'unset' : v === null ? 'NULL' : String(v);
                // the delta's op flips for the insert and its row change, checked equal under both strategies
                const deltaOf = async (c: Awaited<ReturnType<typeof columnReadd>>, start: Version) => {
                    const seen: string[] = [];
                    let last: { flip: string; row: string } = { flip: '', row: '' };
                    for (const strategy of ['bounded', 'full'] as const) {
                        c.group.setDeltaStrategy(strategy);
                        const delta = await c.group.computeDelta(start, c.merged);
                        const flip = delta.opVerdictChanges.find((f) => f.entry === c.insertHash);
                        const row = (delta.tableChanges.get((await c.group.getTable('tasks')).getId())?.rowChanges ?? []).find((r) => r.rowId === rowId);
                        last = {
                            flip: flip === undefined ? 'none' : `${flip.voidBefore ? 'void' : 'live'} -> ${flip.voidAfter ? 'void' : 'live'} (${flip.reason?.kind ?? '-'})`,
                            row: row === undefined ? 'none'
                                : `${row.liveBefore ? 'live' : 'absent'} -> ${row.liveAfter ? 'live' : 'absent'}: `
                                    + row.columnChanges.map((ch) => `${ch.column} ${show(ch.before)} -> ${show(ch.after)}`).join(', '),
                        };
                        seen.push(JSON.stringify(last));
                    }
                    assertEquals(seen[0], seen[1], 'the bounded walk and the full walk report the same');
                    return last;
                };

                // the insert ends void: its row would read a default with a dead target
                const dangling = await columnReadd({ carried: 'p1', deleteDefaultTarget: true });
                const fromInsert = await deltaOf(dangling, version(dangling.insertHash));
                assertEquals(fromInsert.flip, 'live -> void (fk)', "from the insert's branch, the op channel reports the insert going void");
                assertEquals(fromInsert.row.split(':')[0], 'live -> absent', "from the insert's branch, the row goes away");
                const fromDeploy = await deltaOf(dangling, version(dangling.deployHash));
                assertEquals(fromDeploy.row, 'none', "from the deploy's branch, the void insert adds no row");

                // the insert stays live, and its row reads the new column's default
                const stray = await columnReadd({ carried: 'some text', deleteDefaultTarget: false });
                const liveFromInsert = await deltaOf(stray, version(stray.insertHash));
                assertEquals(liveFromInsert.flip, 'none', "from the insert's branch, the insert does not change verdict");
                assertEquals(liveFromInsert.row, 'none',
                    'the row channel is silent: the text was written to the old column, and the re-add is in the schema channel');
                const liveFromDeploy = await deltaOf(stray, version(stray.deployHash));
                assertEquals(liveFromDeploy.row, 'absent -> live: title unset -> a',
                    "from the deploy's branch, the row arrives with no write to the re-added column");
            }
        },
    ],
};

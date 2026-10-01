import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";
import { version, serializePublicKeyToBase64, formatValidationFailure, ValidationRejectedError } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import {
    registerCatalogTypes, makeIdentity, openTable, catalogDatabase, buildCatalog, buildDatabase,
    materializeCatalog, FixtureSpec,
} from "./catalog_fixture.js";
import type { RSchemaImpl } from "../src/rschema/rschema.js";
import type { TableDef } from "../src/rschema/payload.js";
import type { RTableGroupImpl } from "../src/rtable_group/group.js";
import type { RCatalogImpl } from "../src/rcatalog/rcatalog.js";
import type { CatalogGroupDef } from "../src/rcatalog/payload.js";
import { RDbImpl } from "../src/rdb/rdb.js";
import type { ParamValue } from "../src/rdb/payload.js";
import { catalogStatus } from "../src/rdb/conformance.js";
import { deployCatalogRelease, planCatalogUpdate, applyCatalogPlan, CatalogUpdateError } from "../src/rdb/catalog_update.js";
import type { RDeployGateImpl } from "../src/rdeploy_gate/rdeploy_gate.js";
import { deriveRowId } from "../src/rtable/hash.js";
import { deriveGenesisRowUuid, deriveGroupSeed } from "../src/rdb/instantiate.js";
import type { RBlobStoreImpl } from "../src/rblob_store/rblob_store.js";
import type { RFileMapImpl } from "../src/rfile_map/rfile_map.js";

function newCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    registerCatalogTypes(ctx);
    return ctx;
}

function identityParam(identity: OwnIdentity): ParamValue {
    return { identity: { keyId: identity.keyId, publicKey: serializePublicKeyToBase64(identity.publicKey) } };
}

function identitiesTable(): TableDef {
    return {
        name: 'identities',
        columns: {
            keyId: { type: 'string', pub: true, readonly: true },
            publicKey: { type: 'string', pub: true, readonly: true },
            name: { type: 'string', nullable: true },
        },
        idProvider: { keyIdColumn: 'keyId', publicKeyColumn: 'publicKey' },
    };
}

function usersSpec(dev: OwnIdentity): FixtureSpec {
    return {
        dev,
        name: 'editor',
        params: [{ name: 'admin', type: 'identity' }],
        groups: [
            {
                name: 'user',
                tables: [identitiesTable(), openTable('caps', { label: { type: 'string', pub: true }, grantee: { type: 'identity', pub: true } })],
                idProvider: 'identities',
                initialRows: {
                    identities: [{ values: { name: 'Admin' }, params: { keyId: { param: 'admin' }, publicKey: { param: 'admin', fn: 'publicKey' } } }],
                    caps: [{ values: { label: 'manager' }, params: { grantee: { param: 'admin' } } }],
                },
            },
            { name: 'doc', tables: [openTable('docs', { title: { type: 'string' } })], bindings: { user: 'user' }, idProvider: 'user.identities' },
        ],
    };
}

function itemsSpec(dev: OwnIdentity): FixtureSpec {
    return {
        dev,
        name: 'shop',
        groups: [
            { name: 'items', tables: [openTable('items', { name: { type: 'string' } })] },
            { name: 'dep', tables: [openTable('notes', { body: { type: 'string' } })], bindings: { items: 'items' } },
        ],
    };
}

async function frontierOf(obj: { getScopedDag(): Promise<{ getFrontier(): Promise<Version> }> }): Promise<Version> {
    return (await obj.getScopedDag()).getFrontier();
}

async function addColumn(schema: RSchemaImpl, dev: OwnIdentity, column: string, at?: Version): Promise<Version> {
    await schema.updateSchema([{ rule: 'add-column', table: 'items', column, def: { type: 'string', nullable: true } }], dev, undefined, at);
    return frontierOf(schema);
}

function change(schema: RSchemaImpl, v: Version) {
    return { schema: schema.getId(), version: json.toSet([...v]) };
}

async function failureOf(fn: () => Promise<unknown>): Promise<string | undefined> {
    try {
        await fn();
        return undefined;
    } catch (e) {
        return e instanceof ValidationRejectedError ? formatValidationFailure(e.why) : (e as Error).message;
    }
}

async function expectFailure(fn: () => Promise<unknown>, reason: string, message: string): Promise<void> {
    const failure = await failureOf(fn);
    assertTrue(failure !== undefined, message);
    assertTrue(failure!.includes(reason), `${message} (expected '${reason}', got '${failure}')`);
}

async function currentSchemaVersion(group: RTableGroupImpl): Promise<string> {
    const schemaDag = await (await group.getSchemaObject()).getScopedDag();
    return [...await schemaDag.findMinimalCover(await group.resolveSchemaVersion(await frontierOf(group)))].sort().join(',');
}

async function gateOf(ctx: RContext, group: RTableGroupImpl): Promise<RDeployGateImpl> {
    return (await ctx.getObject(group.getDeployGateId())) as RDeployGateImpl;
}

export const rdbCatalogTests = {
    title: '[RDBC] Catalog-based RDb tests',
    tests: [
        {
            name: '[RDBC01] membership is computed from the catalog: groups, bindings, rows and gates',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const admin = await makeIdentity();
                const { rdb, groups, built, builtDb } = await catalogDatabase(ctx, usersSpec(dev), {
                    seed: 'rdbc01', creators: [admin], params: { admin: identityParam(admin) }, author: admin,
                });

                const names = await rdb.getMemberGroupNames();
                assertEquals([...names.keys()].sort().join(','), 'doc,user', 'member names come from the definitions');
                assertEquals(names.get('user'), builtDb.groupIds.get('user'), 'the offline id matches the live one');
                assertEquals((await rdb.getMemberSchemas()).length, 2, 'two member schemas');
                assertEquals((await rdb.getDeployedReleases()).join(','), built.catalogId, 'the genesis release is deployed');

                const user = groups.get('user')!;
                const doc = groups.get('doc')!;
                assertEquals(doc.getBindings()['user'], user.getId(), 'the binding resolves to the user group');
                assertTrue(await gateOf(ctx, user) !== undefined && await gateOf(ctx, doc) !== undefined, 'each member has a gate');

                const uuid = deriveGenesisRowUuid(deriveGroupSeed(rdb.getId(), built.hashes.get('user')!), 'identities', 0);
                const row = await (await (await user.getTable('identities')).getView()).getRow(deriveRowId(uuid));
                assertEquals(row?.values['keyId'], admin.keyId, 'the genesis row carries the admin param');

                const deployKeys = user.getDeployKeys();
                assertEquals(deployKeys.map((k) => k.keyId).join(','), admin.keyId, 'the RDb creator is the default deploy key');
            }
        },
        {
            name: '[RDBC02] deploys never move backwards; a concurrent release merges; the planner catches up and is idempotent',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const admin = await makeIdentity();
                const { rdb, groups, catalog, schemas, built } = await catalogDatabase(ctx, itemsSpec(dev), { seed: 'rdbc02', creators: [admin], author: admin });
                const items = groups.get('items')!;
                const itemsHash = (await rdb.getMembership())!.byId.get(items.getId())!.catalogGroupHash;
                const schema = schemas.get('items')!;
                const genesis = version(catalog.getId());
                const v1 = await frontierOf(schema);

                const v2 = await addColumn(schema, dev, 'a');
                const r11 = await catalog.release({ version: '1.1.0', changes: { [itemsHash]: change(schema, v2) } }, dev, genesis);
                // a maintenance release on the genesis line, concurrent with 1.1.0
                const v101 = version(await schema.updateSchema(
                    [{ rule: 'add-column', table: 'items', column: 'b', def: { type: 'string', nullable: true } }], dev, undefined, v1));
                const r101 = await catalog.release({ version: '1.0.1', changes: { [itemsHash]: change(schema, v101) } }, dev, genesis);

                const result = await deployCatalogRelease(rdb, { release: r11, author: admin });
                assertTrue(result.commit !== undefined, 'the planner commits the update-catalog');
                assertEquals(await currentSchemaVersion(items), [...v2].sort().join(','), 'the group is deployed to the release version');
                assertEquals((await rdb.getDeployedReleases()).join(','), r11, '1.1.0 is deployed');

                const again = await deployCatalogRelease(rdb, { release: r11, author: admin });
                assertTrue(again.commit === undefined && again.deployed.length === 0, 're-running the planner changes nothing');

                await expectFailure(() => rdb.updateCatalog(r11, undefined, admin), 'must not move backwards', 're-deploying a deployed release is rejected');

                // 1.0.1 is concurrent with 1.1.0: deploying it merges the two
                const sideways = await deployCatalogRelease(rdb, { release: r101, author: admin });
                assertTrue(sideways.commit !== undefined, 'a concurrent release is deployed');
                assertEquals((await rdb.getDeployedReleases()).length, 2, 'both releases stay deployed');
                assertEquals(await currentSchemaVersion(items), [...v2, ...v101].sort().join(','),
                    'the group moves to the union of its versions in the deployed releases');
                const merged = await (await items.getSchemaObject()).getView(await items.resolveSchemaVersion(await frontierOf(items)));
                assertTrue(merged.hasTable('items') && merged.getTable('items')!.columns['a'] !== undefined && merged.getTable('items')!.columns['b'] !== undefined,
                    'the merged schema carries both lines');

                const merge = await catalog.release({ version: '1.2.0', changes: { [itemsHash]: change(schema, new Set([...v2, ...v101])) } }, dev);
                await rdb.updateCatalog(merge, undefined, admin);
                assertEquals((await rdb.getDeployedReleases()).join(','), merge, 'a release above the deployed ones is accepted and supersedes them');

                // a fresh database at the merge cannot go back to a release below it
                const fresh = (await ctx.createObject((await buildDatabase(built, { seed: 'rdbc02-fresh' })).rdbPayload)) as RDbImpl;
                await fresh.updateCatalog(merge);
                await expectFailure(() => fresh.updateCatalog(r11), 'must not move backwards', 'a release below a deployed one is rejected');
            }
        },
        {
            name: '[RDBC03] params: declared by the target, typed, set once; concurrent values resolve by op hash',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const admin = await makeIdentity();
                const bob = await makeIdentity();
                const carol = await makeIdentity();
                const { rdb, catalog, schemas, built } = await catalogDatabase(ctx, usersSpec(dev), {
                    seed: 'rdbc03', creators: [admin], params: { admin: identityParam(admin) }, author: admin,
                });

                const desk: CatalogGroupDef = {
                    name: 'desk', seedSource: 'rdb',
                    schemaRef: schemas.get('user')!.getId(), schemaVersion: json.toSet([schemas.get('user')!.getId()]),
                    idProvider: 'identities',
                    initialRows: { identities: [{ values: {}, params: { keyId: { param: 'support' }, publicKey: { param: 'support', fn: 'publicKey' } } }] },
                };
                const r11 = await catalog.release({ version: '1.1.0', params: [{ name: 'support', type: 'identity' }], add: [desk] }, dev);

                await expectFailure(() => rdb.updateCatalog(r11, undefined, admin), 'has no value', 'a declared param must be supplied');
                await expectFailure(() => rdb.updateCatalog(r11, { support: { value: 'nope' } }, admin), 'is not of type', 'a param must have its declared type');
                await expectFailure(() => rdb.updateCatalog(r11, { admin: identityParam(bob), support: identityParam(bob) }, admin),
                    'is already set', 'a param cannot be set twice');
                await expectFailure(() => rdb.updateCatalog(r11, { support: identityParam(bob), other: identityParam(bob) }, admin),
                    'is not declared', 'an undeclared param is rejected');

                const at = await frontierOf(rdb);
                const h1 = await rdb.updateCatalog(r11, { support: identityParam(bob) }, admin, undefined, at);
                const h2 = await rdb.updateCatalog(r11, { support: identityParam(carol) }, admin, undefined, at);
                const winner = h1 > h2 ? bob : carol;
                const params = await rdb.getParams();
                assertTrue('identity' in params['support'] && params['support'].identity.keyId === winner.keyId,
                    'the concurrent op with the larger hash wins');
                assertTrue((await rdb.getMemberGroupNames()).has('desk'), 'the new group is a member');
                assertEquals(built.catalogId, rdb.getCatalogRef(), 'the catalog ref is the fixture catalog');
            }
        },
        {
            name: '[RDBC04] concurrent deploys of incomparable releases join their groups; names tie-break by hash',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const { rdb, catalog } = await catalogDatabase(ctx, itemsSpec(dev), { seed: 'rdbc04' });
                const genesis = version(catalog.getId());

                const extra = await buildCatalog({ dev, name: 'extra', groups: [
                    { name: 'notes', tables: [openTable('a', { x: { type: 'string' } })] },
                    { name: 'notes2', tables: [openTable('b', { y: { type: 'string' } })] },
                ] });
                await materializeCatalog(ctx, extra);
                const notesA = { ...extra.defs.get('notes')! };
                const notesB = { ...extra.defs.get('notes2')!, name: 'notes' };

                const left = (await catalog.publishRelease({ version: '1.1.0', add: [notesA] }, dev, genesis)).release;
                const right = (await catalog.publishRelease({ version: '1.0.1', add: [notesB] }, dev, genesis)).release;

                const at = await frontierOf(rdb);
                await rdb.updateCatalog(left, undefined, undefined, undefined, at);
                await rdb.updateCatalog(right, undefined, undefined, undefined, at);

                assertEquals((await rdb.getDeployedReleases()).length, 2, 'both concurrent releases are deployed');
                const names = [...(await rdb.getMemberGroupNames()).keys()].sort();
                assertTrue(names.includes('notes'), 'one group keeps the name');
                assertTrue(names.some((n) => n.startsWith('notes_')), 'the other gets a hash suffix');
                assertEquals(names.length, 4, 'items, dep and both notes groups');
            }
        },
        {
            name: '[RDBC05] a missing catalog, a missing release or missing params resolve as unresolved',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const admin = await makeIdentity();
                const built = await buildCatalog(usersSpec(dev));

                const withParams = await buildDatabase(built, { seed: 'rdbc05', params: { admin: identityParam(admin) } });
                const rdb = (await ctx.createObject(withParams.rdbPayload)) as RDbImpl;
                const missing = await rdb.resolve();
                assertEquals(missing.unresolved?.kind, 'missing-catalog', 'without the catalog the RDb is unresolved');
                assertEquals((await rdb.getMemberGroups()).length, 0, 'an unresolved RDb has no members');

                await materializeCatalog(ctx, built);
                assertTrue((await rdb.resolve()).unresolved === undefined, 'with the catalog it resolves');

                const noParams = await RDbImpl.create({ seed: 'rdbc05-b', catalog: built.catalogId, release: built.catalogId });
                const rdb2 = (await ctx.createObject(noParams)) as RDbImpl;
                assertEquals((await rdb2.resolve()).unresolved?.kind, 'invalid-params', 'a create without its params is unresolved');

                const badRelease = await RDbImpl.create({ seed: 'rdbc05-c', catalog: built.catalogId, release: 'no-such-release' });
                const rdb3 = (await ctx.createObject(badRelease)) as RDbImpl;
                assertEquals((await rdb3.resolve()).unresolved?.kind, 'missing-release', 'a create naming no release is unresolved');
            }
        },
        {
            name: '[RDBC06] adoption: the default range holds a new major; widening admits it; admissions are permanent',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const { rdb, groups, catalog, schemas } = await catalogDatabase(ctx, itemsSpec(dev), { seed: 'rdbc06' });
                const items = groups.get('items')!;
                const itemsHash = (await rdb.getMembership())!.byId.get(items.getId())!.catalogGroupHash;
                const schema = schemas.get('items')!;

                assertEquals(await rdb.getAdoptionRange(), '^1', 'the default range is the major of the genesis release');

                const v2 = await addColumn(schema, dev, 'a');
                const r11 = await catalog.release({ version: '1.1.0', changes: { [itemsHash]: change(schema, v2) } }, dev);
                await deployCatalogRelease(rdb, { release: r11 });
                const gate = await gateOf(ctx, items);
                assertTrue(await gate.isAdmitted(v2), 'a minor release is adopted');
                assertTrue(await gate.isAdmitted(version(schema.getId())), 'adoption admits the closure');

                const v3 = await addColumn(schema, dev, 'b');
                const r20 = await catalog.release({ version: '2.0.0', changes: { [itemsHash]: change(schema, v3) } }, dev);
                await deployCatalogRelease(rdb, { release: r20 });
                assertFalse(await gate.isAdmitted(v3), 'a new major is held under ^1');
                const held = await catalogStatus(rdb);
                assertEquals(held.held.map((r) => r.version).join(','), '2.0.0', 'the status reports the held release');

                await rdb.setAdoptionRange('^2');
                assertTrue(await gate.isAdmitted(v3), 'widening the range admits the major');
                await rdb.setAdoptionRange('^1');
                assertTrue(await gate.isAdmitted(v3), 'narrowing the range does not un-admit');
                assertEquals((await rdb.getAdoptedReleases()).length, 2, 'the genesis and 1.1.0 are adopted under ^1');
            }
        },
        {
            name: '[RDBC07] creators gate update-catalog; without creators it is unsigned',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const admin = await makeIdentity();
                const outsider = await makeIdentity();
                const { rdb, catalog } = await catalogDatabase(ctx, itemsSpec(dev), { seed: 'rdbc07', creators: [admin], author: admin });
                const r11 = await catalog.release({ version: '1.1.0' }, dev);

                await expectFailure(() => rdb.updateCatalog(r11), 'requires an author', 'an unsigned update is rejected');
                await expectFailure(() => rdb.updateCatalog(r11, undefined, outsider), 'is not an RDb creator', 'an outsider is rejected');
                await rdb.updateCatalog(r11, undefined, admin);
                assertEquals((await rdb.getDeployedReleases()).join(','), r11, 'a creator deploys');

                const open = await catalogDatabase(ctx, { ...itemsSpec(dev), name: 'shop_open' }, { seed: 'rdbc07-open' });
                const r11b = await open.catalog.release({ version: '1.1.0' }, dev);
                await open.rdb.updateCatalog(r11b);
                assertEquals((await open.rdb.getDeployedReleases()).join(','), r11b, 'an open RDb deploys unsigned');
            }
        },
        {
            name: '[RDBC08] catalogStatus reports ok, behind, intermediate and missing members',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const { rdb, groups, catalog, schemas } = await catalogDatabase(ctx, itemsSpec(dev), { seed: 'rdbc08' });
                const items = groups.get('items')!;
                const itemsHash = (await rdb.getMembership())!.byId.get(items.getId())!.catalogGroupHash;
                const schema = schemas.get('items')!;

                const stateOf = async (name: string) => (await catalogStatus(rdb)).members.find((m) => m.name === name)!.state;
                assertEquals(await stateOf('items'), 'ok', 'a freshly created group is ok');

                const v2 = await addColumn(schema, dev, 'a');
                const v3 = await addColumn(schema, dev, 'b');
                const r11 = await catalog.release({ version: '1.1.0', changes: { [itemsHash]: change(schema, v3) } }, dev);
                await rdb.updateCatalog(r11);
                assertEquals(await stateOf('items'), 'behind', 'a committed release whose deploys are missing is behind');

                await items.deploy(v2);
                assertEquals(await stateOf('items'), 'intermediate', 'a version no release pins is intermediate');

                await deployCatalogRelease(rdb, { release: r11 });
                assertEquals(await stateOf('items'), 'ok', 'catching up makes it ok');

                const built = await buildCatalog({ ...itemsSpec(dev), name: 'shop_b' });
                await materializeCatalog(ctx, built);
                const bare = await buildDatabase(built, { seed: 'rdbc08-b' });
                const rdb2 = (await ctx.createObject(bare.rdbPayload)) as RDbImpl;
                const status = await catalogStatus(rdb2);
                assertTrue(status.members.every((m) => m.state === 'missing' && !m.present), 'unmaterialized members are missing');
            }
        },
        {
            name: '[RDBC09] the planner creates new groups, deploys bound groups first, advances refs and commits last',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const admin = await makeIdentity();
                const { rdb, groups, catalog, schemas } = await catalogDatabase(ctx, itemsSpec(dev), { seed: 'rdbc09', creators: [admin], author: admin });
                const items = groups.get('items')!;
                const dep = groups.get('dep')!;
                const membership = (await rdb.getMembership())!;
                const itemsHash = membership.byId.get(items.getId())!.catalogGroupHash;
                const schema = schemas.get('items')!;

                const v2 = await addColumn(schema, dev, 'a');
                const extraDef: CatalogGroupDef = {
                    name: 'extra', seedSource: 'rdb', schemaRef: schema.getId(), schemaVersion: json.toSet([...v2]),
                    bindings: { items: itemsHash },
                };
                const r11 = await catalog.release({ version: '1.1.0', changes: { [itemsHash]: change(schema, v2) }, add: [extraDef] }, dev);

                const blocked = await planCatalogUpdate(rdb, { release: r11 });
                assertTrue(blocked.problems.some((p) => p.includes('requires an author')), 'the dry run reports the missing author');
                let rejected = false;
                try { await applyCatalogPlan(blocked); } catch (e) { rejected = e instanceof CatalogUpdateError; }
                assertTrue(rejected, 'a plan with problems is not applied');
                assertEquals(await currentSchemaVersion(items), schema.getId(), 'nothing was deployed');

                const plan = await planCatalogUpdate(rdb, { release: r11, author: admin });
                assertEquals(plan.creates.map((m) => m.name).join(','), 'extra', 'the new group is created');
                const result = await applyCatalogPlan(plan, admin);
                assertEquals(result.deployed.map((d) => d.name).join(','), 'items', 'the changed group is deployed');
                assertEquals(result.observed.map((o) => o.groupId).sort().join(','), [dep.getId(), (await rdb.getMemberGroupNames()).get('extra')!].sort().join(','),
                    'dependents advance their refs to the deployed group');
                assertTrue(result.commit !== undefined, 'the release is committed');
                assertEquals((await rdb.getDeployedReleases()).join(','), r11, 'the RDb records the release');
                assertTrue(await ctx.getObject((await rdb.getMemberGroupNames()).get('extra')!) !== undefined, 'the new group exists');
            }
        },
        {
            name: '[RDBC10] FILES members: deploys create the blob store and file map, bound to the member group',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const admin = await makeIdentity();
                const canWrite = { p: 'exists', table: 'user.caps', where: { grantee: '$author' } } as const;
                const spec: FixtureSpec = { ...usersSpec(dev), files: [{ name: 'media', group: 'user', idProvider: 'user.identities', canWrite }] };
                const { rdb, groups, builtDb, catalog } = await catalogDatabase(ctx, spec, {
                    seed: 'rdbc10', creators: [admin], params: { admin: identityParam(admin) }, author: admin,
                });

                const members = await rdb.getMemberFiles();
                assertEquals(members.map((m) => m.name).join(','), 'media', 'the FILES is a member');
                const media = members[0];
                assertEquals(media.mapId, builtDb.files.get('media')!.mapId, 'the offline ids match the live ones');
                assertEquals(media.groupId, groups.get('user')!.getId(), 'bound to the member group');
                assertFalse((await rdb.getMemberGroupNames()).has('media'), 'a FILES is not a member group');

                const store = (await ctx.getObject(media.storeId)) as RBlobStoreImpl;
                const map = (await ctx.getObject(media.mapId)) as RFileMapImpl;
                assertTrue(store !== undefined && map !== undefined, 'the deploy created both objects');
                assertEquals(map.getBlobStoreId(), store.getId(), 'the map names the store');
                assertEquals(store.getGroupId(), media.groupId, 'the store binds the member group');

                const status = await catalogStatus(rdb);
                assertEquals((status.files ?? []).map((f) => `${f.name}:${f.present}`).join(','), 'media:true', 'status lists the FILES');

                const bytes = new TextEncoder().encode('hello');
                const stored = await store.putFile({ size: bytes.length, read: async function* () { yield bytes; } }, admin, { lane: 0 });
                await map.add({ section: 'common', path: 'hello.txt', fileHash: stored.fileHash }, admin);
                assertEquals((await map.list()).map((f) => f.path).join(','), 'hello.txt', 'the manager writes a file');
                assertFalse(await map.canWrite((await makeIdentity()).keyId), 'an unknown key cannot write');

                const attachments = { name: 'attachments', bindings: { user: builtDb.membership.byId.get(media.groupId)!.catalogGroupHash }, idProvider: 'user.identities', canWrite: { p: 'true' } } as const;
                const r11 = await catalog.release({ version: '1.1.0', files: [attachments] }, dev);
                const result = await deployCatalogRelease(rdb, { release: r11, author: admin });
                const added = (await rdb.getMemberFiles()).find((m) => m.name === 'attachments')!;
                assertEquals(result.created.slice().sort().join(','), [added.storeId, added.mapId].sort().join(','),
                    'deploying a later release creates its FILES objects');
                assertEquals((await rdb.getMemberFiles()).map((m) => m.name).join(','), 'attachments,media', 'members are sorted by name');
                assertEquals((await catalogStatus(rdb)).files!.length, 2, 'status lists both');
            }
        },
        {
            name: '[RDBC11] a FILES whose group drops what ALLOW WRITE IF reads becomes read-only: reads work, writes are refused, nothing throws',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const admin = await makeIdentity();
                const canWrite = { p: 'exists', table: 'user.caps', where: { label: 'manager', grantee: '$author' } } as const;
                const spec: FixtureSpec = { ...usersSpec(dev), files: [{ name: 'media', group: 'user', idProvider: 'user.identities', canWrite }] };
                const { rdb, groups, schemas, catalog } = await catalogDatabase(ctx, spec, {
                    seed: 'rdbc11', creators: [admin], params: { admin: identityParam(admin) }, author: admin,
                });
                const userSchema = schemas.get('user')!;
                const userHash = (await rdb.getMembership())!.byId.get(groups.get('user')!.getId())!.catalogGroupHash;
                const media = (await rdb.getMemberFiles())[0];
                const store = (await ctx.getObject(media.storeId)) as RBlobStoreImpl;
                const map = (await ctx.getObject(media.mapId)) as RFileMapImpl;

                const bytes = new TextEncoder().encode('hello');
                const stored = await store.putFile({ size: bytes.length, read: async function* () { yield bytes; } }, admin, { lane: 0 });
                await map.add({ section: 'common', path: 'hello.txt', fileHash: stored.fileHash }, admin);
                assertTrue(await map.canWriteNow(admin.keyId), 'the manager can write before the change');

                const readText = async () => {
                    const parts: Uint8Array[] = [];
                    for await (const part of store.readFile(stored.tail)) parts.push(part);
                    return new TextDecoder().decode(Uint8Array.from(parts.flatMap((p) => [...p])));
                };
                const expectReadOnly = async (label: string) => {
                    assertFalse(await map.canWriteNow(admin.keyId), `${label}: the manager can't write at the group's current version`);
                    assertEquals((await map.list()).map((f) => f.path).join(','), 'hello.txt', `${label}: the file is still listed`);
                    assertEquals(await readText(), 'hello', `${label}: the file still reads`);
                    await expectFailure(() => map.add({ section: 'common', path: 'more.txt', fileHash: stored.fileHash }, admin),
                        'is not allowed to write', `${label}: a new write is refused`);
                };

                await userSchema.updateSchema([{ rule: 'drop-column', table: 'caps', column: 'label' }], dev);
                const r11 = await catalog.release({ version: '1.1.0', changes: { [userHash]: change(userSchema, await frontierOf(userSchema)) } }, dev);
                await deployCatalogRelease(rdb, { release: r11, author: admin });
                await expectReadOnly('a dropped where column');

                await userSchema.updateSchema([{ rule: 'drop-table', table: 'caps' }], dev);
                const r12 = await catalog.release({ version: '1.2.0', changes: { [userHash]: change(userSchema, await frontierOf(userSchema)) } }, dev);
                await deployCatalogRelease(rdb, { release: r12, author: admin });
                await expectReadOnly('a dropped table');
            }
        },
    ],
};

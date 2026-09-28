import { assertTrue, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { RContext, RObject } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import {
    registerCatalogTypes, makeIdentity, openTable, buildCatalog, buildDatabase, materializeCatalog,
    catalogDatabase, rootIdOf, BuiltCatalog, FixtureSpec,
} from "./catalog_fixture.js";
import { RSchemaImpl } from "../src/rschema/rschema.js";
import type { CatalogGroupDef } from "../src/rcatalog/payload.js";
import { RDbImpl } from "../src/rdb/rdb.js";
import { deployGateId } from "../src/rdeploy_gate/mirror.js";

// A swarm stub that records its lifecycle (mirrors replica test stubs).
type StubSwarm = {
    topic: B64Hash;
    activated: boolean;
    destroyed: boolean;
    activate(): void;
    deactivate(): void;
    sleep(): void;
    destroy(): void;
    peers(): unknown[];
    onPeerJoin(cb: unknown): void;
    onPeerLeave(cb: unknown): void;
    blockPeer(): void;
    wouldAccept(): Promise<boolean>;
    adopt(): boolean;
    mode: string;
};

function createStubMesh() {
    const swarms: StubSwarm[] = [];
    const createOpts: unknown[] = [];
    const mesh = {
        createOpts,
        createSwarm(topic: B64Hash, opts?: unknown): StubSwarm {
            createOpts.push(opts);
            const swarm: StubSwarm = {
                topic,
                activated: false,
                destroyed: false,
                mode: 'active',
                activate() { this.activated = true; },
                deactivate() {},
                sleep() {},
                destroy() { this.destroyed = true; },
                peers() { return []; },
                onPeerJoin(_cb: unknown) {},
                onPeerLeave(_cb: unknown) {},
                blockPeer() {},
                wouldAccept() { return Promise.resolve(false); },
                adopt() { return false; },
            };
            swarms.push(swarm);
            return swarm;
        },
        swarms,
    };
    return mesh;
}

function newCtx(opts?: {
    mesh?: any;
    fetchObject?: RContext['fetchObject'];
}): RContext {
    const ctx = createMockRContext({ selfValidate: true }, { mesh: opts?.mesh, fetchObject: opts?.fetchObject });
    registerCatalogTypes(ctx);
    return ctx;
}

// One group over one open table.
function oneGroup(dev: OwnIdentity, name: string): FixtureSpec {
    return { dev, name, groups: [{ name: 'main', tables: [openTable('t', { name: { type: 'string' } })] }] };
}

// Group `a` binds group `b`.
function boundGroups(dev: OwnIdentity, name: string): FixtureSpec {
    return {
        dev, name, groups: [
            { name: 'b', tables: [openTable('t', { name: { type: 'string' } })] },
            { name: 'a', tables: [openTable('u', { name: { type: 'string' } })], bindings: { b: 'b' } },
        ],
    };
}

async function expectThrow(fn: () => Promise<unknown>, why: string): Promise<void> {
    let threw = false;
    try { await fn(); } catch { threw = true; }
    assertTrue(threw, why);
}

function liveSwarms(mesh: ReturnType<typeof createStubMesh>): StubSwarm[] {
    return mesh.swarms.filter((s) => s.activated && !s.destroyed);
}

function liveTopics(mesh: ReturnType<typeof createStubMesh>): Set<B64Hash> {
    return new Set(liveSwarms(mesh).map((s) => s.topic));
}

async function waitUntil(pred: () => boolean | Promise<boolean>, why: string, timeoutMs = 2000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await pred()) return;
        await new Promise((r) => setTimeout(r, 10));
    }
    throw new Error(`waitUntil timed out: ${why}`);
}

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>((r) => { resolve = r; });
    return { promise, resolve };
}

// A schema of its own for a group added by a later release.
async function extraSchema(ctx: RContext, dev: OwnIdentity, name: string): Promise<RSchemaImpl> {
    return (await ctx.createObject(await RSchemaImpl.create({
        name, creators: [{ keyId: dev.keyId, publicKey: dev.publicKey }], tables: [openTable('x', { v: { type: 'string' } })],
    }))) as RSchemaImpl;
}

function defFor(name: string, schema: RSchemaImpl): CatalogGroupDef {
    return { name, seedSource: 'rdb', schemaRef: schema.getId(), schemaVersion: json.toSet([schema.getId()]) };
}

// Catalog + RDb whose members are NOT materialized (a joining replica).
async function unmaterialized(ctx: RContext, built: BuiltCatalog, seed: string) {
    const db = await buildDatabase(built, { seed });
    const rdb = (await ctx.createObject(db.rdbPayload)) as RDbImpl;
    return { db, rdb };
}

export const rdbSyncTests = {
    title: '[RDB] RDb sync root + startSync fan-out',
    tests: [
        {
            name: '[RDB01] membership is computed from the deployed releases',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const { rdb, catalog, builtDb, schemas } = await catalogDatabase(ctx, oneGroup(dev, 'rdb01'), { seed: 'rdb01' });

                assertEquals((await rdb.getMemberGroups()).join(','), builtDb.groupIds.get('main')!, 'one computed member group');
                assertEquals((await rdb.getMemberSchemas()).join(','), schemas.get('main')!.getId(), 'its schema is a member schema');

                const extra = await extraSchema(ctx, dev, 'rdb01:extra');
                const { release } = await catalog.publishRelease({ version: '1.1.0', add: [defFor('extra', extra)] }, dev);
                assertEquals((await rdb.getMemberGroups()).length, 1, 'a release does not change membership until deployed');
                await rdb.updateCatalog(release);
                assertEquals((await rdb.getMemberGroups()).length, 2, 'deploying the release adds its group');
            },
        },
        {
            name: '[RDB02] startSync opens one session per synced DAG, none for gates; stopSync tears them down',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const dev = await makeIdentity();
                const { rdb, catalog, groups, schemas } = await catalogDatabase(ctx, oneGroup(dev, 'rdb02'), { seed: 'rdb02' });
                const group = groups.get('main')!;

                await rdb.startSync();

                const topics = new Set(mesh.swarms.map((s) => s.topic));
                assertEquals(topics.size, 4, 'RDb + catalog + schema + group');
                assertTrue(topics.has(rdb.getId()) && topics.has(catalog.getId()), 'RDb and catalog synced');
                assertTrue(topics.has(schemas.get('main')!.getId()) && topics.has(group.getId()), 'schema and group synced');
                assertTrue(!topics.has(deployGateId(group.getId(), group.getSchemaRef())), 'the gate gets no session');
                assertTrue(await ctx.getObject(group.getDeployGateId()) !== undefined, 'the gate exists');
                assertTrue(mesh.swarms.every((s) => s.activated), 'all swarms activated');

                await rdb.startSync();
                assertEquals(mesh.swarms.length, 4, 'startSync is idempotent (no new swarms)');

                await rdb.stopSync();
                assertTrue(mesh.swarms.every((s) => s.destroyed), 'all swarms destroyed on stopSync');
            },
        },
        {
            name: '[RDB03] startSync throws when the catalog is absent and the context cannot fetch',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const dev = await makeIdentity();
                const built = await buildCatalog(oneGroup(dev, 'rdb03'));
                const { rdb } = await unmaterialized(ctx, built, 'rdb03');

                await expectThrow(() => rdb.startSync(), 'startSync must throw for an unfetchable absent catalog');
                assertTrue(ctx.fetchObject === undefined, 'mock context exposes no fetchObject');
                assertEquals(liveSwarms(mesh).length, 0, 'failed startSync leaves no live swarms');
            },
        },
        {
            name: '[RDB04] startSync fans out to bound member groups and their schemas',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const dev = await makeIdentity();
                const { rdb, groups, schemas } = await catalogDatabase(ctx, boundGroups(dev, 'rdb04'), { seed: 'rdb04' });

                await rdb.startSync();

                const topics = new Set(mesh.swarms.map((s) => s.topic));
                for (const name of ['a', 'b']) {
                    assertTrue(topics.has(groups.get(name)!.getId()), `group ${name} synced`);
                    assertTrue(topics.has(schemas.get(name)!.getId()), `schema ${name} synced`);
                }
                assertEquals(topics.size, 6, 'RDb + catalog + two schemas + two groups');
                await rdb.stopSync();
            },
        },
        {
            name: '[RDB05] creators gate update-catalog: unsigned rejected, outsider rejected, creator accepted',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const admin = await makeIdentity();
                const outsider = await makeIdentity();
                const { rdb, catalog } = await catalogDatabase(ctx, oneGroup(dev, 'rdb05'), { seed: 'rdb05', creators: [admin], author: admin });
                const release = await catalog.release({ version: '1.1.0' }, dev);

                await expectThrow(() => rdb.updateCatalog(release), 'unsigned update-catalog must be rejected when creators are declared');
                await expectThrow(() => rdb.updateCatalog(release, undefined, outsider), 'non-creator update-catalog must be rejected');
                await rdb.updateCatalog(release, undefined, admin);
                assertEquals((await rdb.getDeployedReleases()).join(','), release, 'creator-signed update-catalog accepted');
            },
        },
        {
            name: '[RDB06] no creators keeps unsigned update-catalog valid',
            invoke: async () => {
                const ctx = newCtx();
                const dev = await makeIdentity();
                const { rdb, catalog } = await catalogDatabase(ctx, oneGroup(dev, 'rdb06'), { seed: 'rdb06' });
                const release = await catalog.release({ version: '1.1.0' }, dev);
                await rdb.updateCatalog(release);
                assertEquals((await rdb.getDeployedReleases()).join(','), release, 'unsigned update-catalog accepted');
            },
        },
        {
            name: '[RDB07] startSync passes runtime authorizer into createSwarm',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const dev = await makeIdentity();
                const { rdb } = await catalogDatabase(ctx, oneGroup(dev, 'rdb07'), { seed: 'rdb07' });

                const authorizer = { authorize: async () => true };
                rdb.setRuntimeConfig({ authorizer });
                await rdb.startSync();

                assertEquals(mesh.createOpts.length, 4, 'one createSwarm per synced DAG');
                assertTrue(
                    mesh.createOpts.every((opts) => (opts as { authorizer?: unknown }).authorizer === authorizer),
                    'authorizer forwarded to every swarm',
                );
                await rdb.stopSync();
            },
        },
        {
            name: '[RDB08] startSync fetches an absent catalog, then creates the members from local payloads',
            invoke: async () => {
                const mesh = createStubMesh();
                const dev = await makeIdentity();
                const built = await buildCatalog(oneGroup(dev, 'rdb08'));

                const fetched: B64Hash[] = [];
                let ctx!: RContext;
                ctx = newCtx({
                    mesh,
                    fetchObject: async (id) => {
                        fetched.push(id);
                        return ctx.createObject(built.catalogPayload);
                    },
                });
                for (const payload of built.schemaPayloads.values()) await ctx.createObject(payload);
                const { rdb, db } = await unmaterialized(ctx, built, 'rdb08');
                const groupId = db.groupIds.get('main')!;
                assertTrue((await ctx.getObject(groupId)) === undefined, 'the member group is absent before startSync');

                await rdb.startSync();

                assertEquals(fetched.join(','), built.catalogId, 'fetchObject called once, for the catalog');
                assertTrue((await ctx.getObject(groupId)) !== undefined, 'the member group was created locally');
                const topics = liveTopics(mesh);
                assertTrue(topics.has(built.catalogId) && topics.has(groupId), 'catalog and group DAGs synced');
                assertEquals(topics.size, 4, 'RDb + catalog + schema + group');
                await rdb.stopSync();
            },
        },
        {
            name: '[RDB09] deploying a release after startSync opens its new sessions without stop/start',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const dev = await makeIdentity();
                const { rdb, catalog } = await catalogDatabase(ctx, oneGroup(dev, 'rdb09'), { seed: 'rdb09' });
                await rdb.startSync();
                assertEquals(mesh.swarms.length, 4, 'initial closure is RDb + catalog + schema + group');

                const extra = await extraSchema(ctx, dev, 'rdb09:extra');
                const { release } = await catalog.publishRelease({ version: '1.1.0', add: [defFor('extra', extra)] }, dev);
                await rdb.updateCatalog(release);

                const extraId = (await rdb.getMemberGroupNames()).get('extra')!;
                await waitUntil(() => liveTopics(mesh).has(extraId), 'the new group swarm should open after the deploy');
                const topics = liveTopics(mesh);
                assertTrue(topics.has(extra.getId()), 'the new schema DAG synced');
                assertEquals(topics.size, 6, 'two more live sessions');
                await rdb.stopSync();
            },
        },
        {
            name: '[RDB10] concurrent startSync shares one fan-out',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const dev = await makeIdentity();
                const { rdb } = await catalogDatabase(ctx, oneGroup(dev, 'rdb10'), { seed: 'rdb10' });

                await Promise.all([rdb.startSync(), rdb.startSync()]);

                assertEquals(liveTopics(mesh).size, 4, 'four live sessions');
                assertEquals(mesh.swarms.length, 4, 'concurrent startSync must not double-create swarms');
                await rdb.stopSync();
            },
        },
        {
            name: '[RDB11] stopSync during fetchObject does not resurrect sessions',
            invoke: async () => {
                const mesh = createStubMesh();
                const dev = await makeIdentity();
                const built = await buildCatalog(oneGroup(dev, 'rdb11'));

                let releaseFetch!: (obj: RObject) => void;
                const fetchStarted = deferred<void>();
                const fetchGate = new Promise<RObject>((resolve) => { releaseFetch = resolve; });

                let ctx!: RContext;
                ctx = newCtx({
                    mesh,
                    fetchObject: async (_id) => {
                        fetchStarted.resolve();
                        return fetchGate;
                    },
                });
                for (const payload of built.schemaPayloads.values()) await ctx.createObject(payload);
                const { rdb } = await unmaterialized(ctx, built, 'rdb11');

                const started = rdb.startSync();
                await fetchStarted.promise;
                await rdb.stopSync();
                releaseFetch(await ctx.createObject(built.catalogPayload));

                await expectThrow(() => started, 'startSync must abort when stopSync wins');
                assertEquals(liveSwarms(mesh).length, 0, 'no live swarms after stop during fetch');

                await rdb.startSync();
                const topics = liveTopics(mesh);
                assertTrue(topics.has(rdb.getId()) && topics.has(built.catalogId), 'a later startSync opens sessions');
                await rdb.stopSync();
            },
        },
        {
            name: '[RDB12] stopSync during startSync aborts start; a following startSync works',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const dev = await makeIdentity();
                const { rdb } = await catalogDatabase(ctx, oneGroup(dev, 'rdb12'), { seed: 'rdb12' });

                const started = rdb.startSync();
                await rdb.stopSync();
                await expectThrow(() => started, 'in-flight startSync must reject when stopSync wins');
                assertEquals(liveSwarms(mesh).length, 0, 'stopSync leaves no live swarms');

                await rdb.startSync();
                assertEquals(liveTopics(mesh).size, 4, 'startSync after abort opens the full closure');
                await rdb.stopSync();
            },
        },
        {
            name: '[RDB13] a deploy during an in-flight start is included in the start promise',
            invoke: async () => {
                const mesh = createStubMesh();
                const dev = await makeIdentity();
                const absentInit = await RSchemaImpl.create({
                    name: 'rdb13:absent', creators: [{ keyId: dev.keyId, publicKey: dev.publicKey }],
                    tables: [openTable('t', { name: { type: 'string' } })],
                });
                const absentId = rootIdOf(absentInit);

                let releaseFetch!: (obj: RObject) => void;
                const fetchStarted = deferred<void>();
                const fetchGate = new Promise<RObject>((resolve) => { releaseFetch = resolve; });

                let ctx!: RContext;
                ctx = newCtx({
                    mesh,
                    fetchObject: async (_id) => {
                        fetchStarted.resolve();
                        return fetchGate;
                    },
                });
                const { rdb, catalog } = await catalogDatabase(ctx, oneGroup(dev, 'rdb13'), { seed: 'rdb13' });
                await catalog.declare([absentId], dev);

                const started = rdb.startSync();
                await fetchStarted.promise;

                const extra = await extraSchema(ctx, dev, 'rdb13:extra');
                const { release } = await catalog.publishRelease({ version: '1.1.0', add: [defFor('extra', extra)] }, dev);
                await rdb.updateCatalog(release);
                releaseFetch(await ctx.createObject(absentInit));
                await started;

                const extraId = (await rdb.getMemberGroupNames()).get('extra')!;
                await waitUntil(() => liveTopics(mesh).has(extraId), 'the group deployed during start is synced');
                assertTrue(liveTopics(mesh).has(absentId), 'the declared schema was fetched and synced');
                await rdb.stopSync();
            },
        },
        {
            name: '[RDB14] failed startSync then retry actually opens sessions',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const dev = await makeIdentity();
                const built = await buildCatalog(oneGroup(dev, 'rdb14'));
                const { rdb, db } = await unmaterialized(ctx, built, 'rdb14');

                await expectThrow(() => rdb.startSync(), 'first startSync fails while the catalog is absent');
                assertEquals(liveSwarms(mesh).length, 0, 'failed start leaves no live swarms');

                await materializeCatalog(ctx, built);
                await rdb.startSync();

                const topics = liveTopics(mesh);
                assertTrue(topics.has(rdb.getId()) && topics.has(built.catalogId), 'retry startSync opens sessions');
                assertTrue(topics.has(db.groupIds.get('main')!), 'the member group is created and synced');
                assertEquals(topics.size, 4, 'RDb + catalog + schema + group');
                await rdb.stopSync();
            },
        },
    ],
};

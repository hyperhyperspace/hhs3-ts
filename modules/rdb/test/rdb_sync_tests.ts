import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RContext, RObject } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { RDbImpl, rDbFactory } from "../src/rdb/rdb.js";
import type { TableDef } from "../src/rschema/payload.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

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

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function open(name: string, columns: TableDef['columns']): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }] };
}

function newCtx(opts?: {
    mesh?: any;
    fetchObject?: RContext['fetchObject'];
}): RContext {
    const ctx = createMockRContext({ selfValidate: true }, { mesh: opts?.mesh, fetchObject: opts?.fetchObject });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    ctx.getRegistry().register(RDbImpl.typeId, rDbFactory);
    return ctx;
}

// Create a schema + group pair. The group references its own schema and may
// bind foreign groups.
async function makeSchemaGroup(ctx: RContext, seed: string, opts?: {
    bindings?: { [name: string]: B64Hash };
}): Promise<{ schema: RSchemaImpl; group: RTableGroupImpl }> {
    const creator = await makeIdentity();
    const schemaInit = await RSchemaImpl.create({
        name: `${seed.replace(/[^a-zA-Z0-9_]+/g, '_')}:schema`,
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: [open('t', { name: { type: 'string' } })],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        name: seed,
        seed: seed + '-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
        ...(opts?.bindings !== undefined ? { bindings: opts.bindings } : {}),
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    return { schema, group };
}

async function makeRDb(ctx: RContext, seed: string, creators?: OwnIdentity[]): Promise<RDbImpl> {
    const init = await RDbImpl.create({
        seed,
        ...(creators !== undefined && creators.length > 0
            ? { creators: creators.map((c) => ({ keyId: c.keyId, publicKey: c.publicKey })) }
            : {}),
    });
    return (await ctx.createObject(init)) as RDbImpl;
}

async function expectThrow(fn: () => Promise<unknown>, why: string): Promise<void> {
    let threw = false;
    try { await fn(); } catch { threw = true; }
    assertTrue(threw, why);
}

function liveSwarms(mesh: ReturnType<typeof createStubMesh>): StubSwarm[] {
    return mesh.swarms.filter((s) => s.activated && !s.destroyed);
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

export const rdbSyncTests = {
    title: '[RDB] RDb sync root + startSync fan-out',
    tests: [
        {
            name: '[RDB01] create + addSchema/addGroup reflect the add-only membership union',
            invoke: async () => {
                const ctx = newCtx();
                const rdb = await makeRDb(ctx, 'rdb01');
                const { schema, group } = await makeSchemaGroup(ctx, 'rdb01');

                assertTrue((await rdb.getMemberSchemas()).length === 0, 'no schemas before add');
                assertTrue((await rdb.getMemberGroups()).length === 0, 'no groups before add');

                await rdb.addSchema(schema.getId(), 'primary schema');
                await rdb.addGroup(group.getId());
                // idempotent re-add (monotonic union keyed by id)
                await rdb.addSchema(schema.getId());

                const schemas = await rdb.getMemberSchemas();
                const groups = await rdb.getMemberGroups();

                assertTrue(schemas.length === 1 && schemas[0] === schema.getId(), 'one member schema (de-duplicated)');
                assertTrue(groups.length === 1 && groups[0] === group.getId(), 'one member group');
            },
        },
        {
            name: '[RDB02] startSync opens one session per transitive DAG; stopSync tears them down',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const rdb = await makeRDb(ctx, 'rdb02');
                const { schema, group } = await makeSchemaGroup(ctx, 'rdb02');

                await rdb.addSchema(schema.getId());
                await rdb.addGroup(group.getId());

                await rdb.startSync();

                // closure: RDb + schema + group (group's schemaRef === schema)
                const topics = new Set(mesh.swarms.map((s) => s.topic));
                assertTrue(topics.size === 3, `expected 3 sync sessions, got ${topics.size}`);
                assertTrue(topics.has(rdb.getId()), 'RDb DAG synced');
                assertTrue(topics.has(schema.getId()), 'schema DAG synced');
                assertTrue(topics.has(group.getId()), 'group DAG synced');
                assertTrue(mesh.swarms.every((s) => s.activated), 'all swarms activated');

                // idempotent
                await rdb.startSync();
                assertTrue(mesh.swarms.length === 3, 'startSync is idempotent (no new swarms)');

                await rdb.stopSync();
                assertTrue(mesh.swarms.every((s) => s.destroyed), 'all swarms destroyed on stopSync');
            },
        },
        {
            name: '[RDB03] startSync throws when a member is absent and the context cannot fetch',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });   // mock ctx has no fetchObject
                const rdb = await makeRDb(ctx, 'rdb03');

                // a real-looking id that was never created in this replica
                const absentInit = await RSchemaImpl.create({
                    name: 'rdb03:absent',
                    creators: [],
                    tables: [open('t', { name: { type: 'string' } })],
                });
                const absentId = await rSchemaFactory.computeRootObjectId(absentInit, ctx);

                await rdb.addSchema(absentId);

                await expectThrow(() => rdb.startSync(), 'startSync must throw for an unfetchable absent member');
                assertTrue(ctx.fetchObject === undefined, 'mock context exposes no fetchObject');
                assertTrue(liveSwarms(mesh).length === 0, 'failed startSync leaves no live swarms');
            },
        },
        {
            name: '[RDB04] startSync fans out transitively to bound foreign groups and their schemas',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const rdb = await makeRDb(ctx, 'rdb04');

                // GroupB (own SchemaB) must exist before GroupA binds it
                const b = await makeSchemaGroup(ctx, 'rdb04-b');
                const a = await makeSchemaGroup(ctx, 'rdb04-a', { bindings: { b: b.group.getId() } });

                // only GroupA is an explicit member
                await rdb.addGroup(a.group.getId());

                await rdb.startSync();

                const topics = new Set(mesh.swarms.map((s) => s.topic));
                // closure: RDb + GroupA + SchemaA + GroupB + SchemaB
                assertTrue(topics.has(a.group.getId()), 'GroupA synced');
                assertTrue(topics.has(a.schema.getId()), 'SchemaA synced (group schema)');
                assertTrue(topics.has(b.group.getId()), 'GroupB synced (bound foreign group)');
                assertTrue(topics.has(b.schema.getId()), 'SchemaB synced (foreign group schema)');
                assertTrue(topics.size === 5, `expected 5 sessions (RDb + A/SchemaA + B/SchemaB), got ${topics.size}`);
            },
        },
        {
            name: '[RDB05] creators gate membership ops: unsigned rejected, creator signed accepted, outsider rejected',
            invoke: async () => {
                const ctx = newCtx();
                const admin = await makeIdentity();
                const outsider = await makeIdentity();
                const rdb = await makeRDb(ctx, 'rdb05', [admin]);
                const { schema, group } = await makeSchemaGroup(ctx, 'rdb05');

                await expectThrow(
                    () => rdb.addSchema(schema.getId()),
                    'unsigned add-schema must be rejected when creators are declared',
                );
                await expectThrow(
                    () => rdb.addSchema(schema.getId(), undefined, outsider),
                    'non-creator add-schema must be rejected',
                );

                await rdb.addSchema(schema.getId(), undefined, admin);
                assertTrue((await rdb.getMemberSchemas()).includes(schema.getId()), 'creator-signed add-schema accepted');

                await expectThrow(
                    () => rdb.addGroup(group.getId()),
                    'unsigned add-group must be rejected when creators are declared',
                );
                await rdb.addGroup(group.getId(), undefined, admin);
                assertTrue((await rdb.getMemberGroups()).includes(group.getId()), 'creator-signed add-group accepted');
            },
        },
        {
            name: '[RDB06] no creators keeps unsigned membership ops valid',
            invoke: async () => {
                const ctx = newCtx();
                const rdb = await makeRDb(ctx, 'rdb06');
                const { schema, group } = await makeSchemaGroup(ctx, 'rdb06');

                await rdb.addSchema(schema.getId());
                await rdb.addGroup(group.getId());

                assertTrue((await rdb.getMemberSchemas()).includes(schema.getId()), 'unsigned add-schema accepted');
                assertTrue((await rdb.getMemberGroups()).includes(group.getId()), 'unsigned add-group accepted');
            },
        },
        {
            name: '[RDB07] startSync passes runtime authorizer into createSwarm',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const rdb = await makeRDb(ctx, 'rdb07');
                const { schema, group } = await makeSchemaGroup(ctx, 'rdb07');
                await rdb.addSchema(schema.getId());
                await rdb.addGroup(group.getId());

                const authorizer = { authorize: async () => true };
                rdb.setRuntimeConfig({ authorizer });
                await rdb.startSync();

                assertTrue(mesh.createOpts.length === 3, 'one createSwarm per DAG');
                assertTrue(
                    mesh.createOpts.every((opts) => (opts as { authorizer?: unknown }).authorizer === authorizer),
                    'authorizer forwarded to every swarm',
                );
            },
        },
        {
            name: '[RDB08] startSync fetches an absent member via ctx.fetchObject',
            invoke: async () => {
                const mesh = createStubMesh();
                const creator = await makeIdentity();
                const schemaInit = await RSchemaImpl.create({
                    name: 'rdb08:schema',
                    creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
                    tables: [open('t', { name: { type: 'string' } })],
                });

                const fetched: B64Hash[] = [];
                let ctx!: RContext;
                ctx = newCtx({
                    mesh,
                    fetchObject: async (id) => {
                        fetched.push(id);
                        return ctx.createObject(schemaInit);
                    },
                });

                const schemaId = await rSchemaFactory.computeRootObjectId(schemaInit, ctx);
                const rdb = await makeRDb(ctx, 'rdb08');
                await rdb.addSchema(schemaId);

                assertTrue((await ctx.getObject(schemaId)) === undefined, 'schema is absent before startSync');

                await rdb.startSync();

                assertTrue(fetched.length === 1 && fetched[0] === schemaId, 'fetchObject called once for the absent schema');
                assertTrue((await ctx.getObject(schemaId)) !== undefined, 'schema present after fetch');

                const topics = new Set(mesh.swarms.map((s) => s.topic));
                assertTrue(topics.has(rdb.getId()), 'RDb DAG synced');
                assertTrue(topics.has(schemaId), 'fetched schema DAG synced');
                assertTrue(topics.size === 2, `expected 2 sessions (RDb + schema), got ${topics.size}`);
            },
        },
        {
            name: '[RDB09] addGroup after startSync opens a new swarm without stop/start',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const rdb = await makeRDb(ctx, 'rdb09');
                const first = await makeSchemaGroup(ctx, 'rdb09-a');
                const second = await makeSchemaGroup(ctx, 'rdb09-b');

                await rdb.addSchema(first.schema.getId());
                await rdb.addGroup(first.group.getId());
                await rdb.startSync();
                assertTrue(mesh.swarms.length === 3, 'initial closure is RDb + schema + group');

                await rdb.addSchema(second.schema.getId());
                await rdb.addGroup(second.group.getId());

                await waitUntil(
                    () => mesh.swarms.some((s) => s.topic === second.group.getId() && s.activated && !s.destroyed),
                    'second group swarm should open after addGroup',
                );
                const topics = new Set(liveSwarms(mesh).map((s) => s.topic));
                assertTrue(topics.has(second.schema.getId()), 'second schema DAG synced');
                assertTrue(topics.has(second.group.getId()), 'second group DAG synced');
                assertTrue(topics.size === 5, `expected 5 live sessions, got ${topics.size}`);

                await rdb.stopSync();
            },
        },
        {
            name: '[RDB10] concurrent startSync shares one fan-out',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const rdb = await makeRDb(ctx, 'rdb10');
                const { schema, group } = await makeSchemaGroup(ctx, 'rdb10');
                await rdb.addSchema(schema.getId());
                await rdb.addGroup(group.getId());

                await Promise.all([rdb.startSync(), rdb.startSync()]);

                const topics = new Set(liveSwarms(mesh).map((s) => s.topic));
                assertTrue(topics.size === 3, `expected 3 live sessions, got ${topics.size}`);
                assertTrue(mesh.swarms.length === 3, 'concurrent startSync must not double-create swarms');

                await rdb.stopSync();
            },
        },
        {
            name: '[RDB11] stopSync during fetchObject does not resurrect sessions',
            invoke: async () => {
                const mesh = createStubMesh();
                const creator = await makeIdentity();
                const schemaInit = await RSchemaImpl.create({
                    name: 'rdb11:schema',
                    creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
                    tables: [open('t', { name: { type: 'string' } })],
                });

                let releaseFetch!: (obj: RObject) => void;
                const fetchStarted = deferred<void>();
                const fetchGate = new Promise<RObject>((resolve) => {
                    releaseFetch = resolve;
                });

                let ctx!: RContext;
                ctx = newCtx({
                    mesh,
                    fetchObject: async (_id) => {
                        fetchStarted.resolve();
                        return fetchGate;
                    },
                });

                const schemaId = await rSchemaFactory.computeRootObjectId(schemaInit, ctx);
                const rdb = await makeRDb(ctx, 'rdb11');
                await rdb.addSchema(schemaId);

                const started = rdb.startSync();
                await fetchStarted.promise;
                await rdb.stopSync();
                releaseFetch(await ctx.createObject(schemaInit));

                await expectThrow(() => started, 'startSync must abort when stopSync wins');
                assertTrue(liveSwarms(mesh).length === 0, 'no live swarms after stop during fetch');

                await rdb.startSync();
                const topics = new Set(liveSwarms(mesh).map((s) => s.topic));
                assertTrue(topics.has(rdb.getId()) && topics.has(schemaId), 'a later startSync opens sessions');
                await rdb.stopSync();
            },
        },
        {
            name: '[RDB12] stopSync during startSync aborts start; a following startSync works',
            invoke: async () => {
                const mesh = createStubMesh();
                const ctx = newCtx({ mesh });
                const rdb = await makeRDb(ctx, 'rdb12');
                const { schema, group } = await makeSchemaGroup(ctx, 'rdb12');
                await rdb.addSchema(schema.getId());
                await rdb.addGroup(group.getId());

                const started = rdb.startSync();
                await rdb.stopSync();
                await expectThrow(() => started, 'in-flight startSync must reject when stopSync wins');
                assertTrue(liveSwarms(mesh).length === 0, 'stopSync leaves no live swarms');

                await rdb.startSync();
                const topics = new Set(liveSwarms(mesh).map((s) => s.topic));
                assertTrue(topics.size === 3, 'startSync after abort opens the full closure');
                await rdb.stopSync();
            },
        },
        {
            name: '[RDB13] addSchema during in-flight start is included in the start promise',
            invoke: async () => {
                const mesh = createStubMesh();
                const creator = await makeIdentity();
                const schemaAInit = await RSchemaImpl.create({
                    name: 'rdb13_schema_a',
                    creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
                    tables: [open('t', { name: { type: 'string' } })],
                });

                let releaseFetch!: (obj: RObject) => void;
                const fetchStarted = deferred<void>();
                const fetchGate = new Promise<RObject>((resolve) => {
                    releaseFetch = resolve;
                });

                let ctx!: RContext;
                ctx = newCtx({
                    mesh,
                    fetchObject: async (_id) => {
                        fetchStarted.resolve();
                        return fetchGate;
                    },
                });

                const schemaAId = await rSchemaFactory.computeRootObjectId(schemaAInit, ctx);
                const rdb = await makeRDb(ctx, 'rdb13');
                const extra = await makeSchemaGroup(ctx, 'rdb13-extra');
                await rdb.addSchema(schemaAId);

                const started = rdb.startSync();
                await fetchStarted.promise;
                await rdb.addSchema(extra.schema.getId());
                releaseFetch(await ctx.createObject(schemaAInit));
                await started;

                const topics = new Set(liveSwarms(mesh).map((s) => s.topic));
                assertTrue(topics.has(schemaAId), 'fetched schema is synced');
                assertTrue(topics.has(extra.schema.getId()), 'schema added during start is synced');
                await rdb.stopSync();
            },
        },
        {
            name: '[RDB14] failed startSync then retry actually opens sessions',
            invoke: async () => {
                const mesh = createStubMesh();
                const creator = await makeIdentity();
                const schemaInit = await RSchemaImpl.create({
                    name: 'rdb14:schema',
                    creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
                    tables: [open('t', { name: { type: 'string' } })],
                });

                const ctx = newCtx({ mesh });
                const schemaId = await rSchemaFactory.computeRootObjectId(schemaInit, ctx);
                const rdb = await makeRDb(ctx, 'rdb14');
                await rdb.addSchema(schemaId);

                await expectThrow(() => rdb.startSync(), 'first startSync fails while the schema is absent');
                assertTrue(liveSwarms(mesh).length === 0, 'failed start leaves no live swarms');

                await ctx.createObject(schemaInit);
                await rdb.startSync();

                const topics = new Set(liveSwarms(mesh).map((s) => s.topic));
                assertTrue(topics.has(rdb.getId()) && topics.has(schemaId), 'retry startSync opens sessions');
                assertTrue(topics.size === 2, `expected 2 live sessions, got ${topics.size}`);
                await rdb.stopSync();
            },
        },
    ],
};

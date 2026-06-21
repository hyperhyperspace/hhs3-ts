import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";

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
    const mesh = {
        createSwarm(topic: B64Hash): StubSwarm {
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

function newCtx(opts?: { mesh?: any }): RContext {
    const ctx = createMockRContext({ selfValidate: true }, { mesh: opts?.mesh });
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
        seed: seed + '-schema',
        creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
        tables: [open('t', { name: { type: 'string' } })],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();

    const groupInit = await RTableGroupImpl.create({
        seed: seed + '-group',
        schemaRef: schema.getId(),
        schemaVersion: pinned,
        ...(opts?.bindings !== undefined ? { bindings: opts.bindings } : {}),
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    return { schema, group };
}

async function makeRDb(ctx: RContext, seed: string): Promise<RDbImpl> {
    const init = await RDbImpl.create({ seed });
    return (await ctx.createObject(init)) as RDbImpl;
}

async function expectThrow(fn: () => Promise<unknown>, why: string): Promise<void> {
    let threw = false;
    try { await fn(); } catch { threw = true; }
    assertTrue(threw, why);
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
                    seed: 'rdb03-absent',
                    creators: [],
                    tables: [open('t', { name: { type: 'string' } })],
                });
                const absentId = await rSchemaFactory.computeRootObjectId(absentInit, ctx);

                await rdb.addSchema(absentId);

                await expectThrow(() => rdb.startSync(), 'startSync must throw for an unfetchable absent member');
                assertTrue(ctx.fetchObject === undefined, 'mock context exposes no fetchObject');
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
    ],
};

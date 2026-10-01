// File mounts on a MemoryDirectory: a catalog FILES member of a real RDb,
// projected into a MemoryTarget (for rdb_keys), with a second writer (bob)
// standing in for remote changes.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { serializePublicKeyToBase64, type RContext } from "@hyper-hyper-space/hhs3_mvt";
import {
    CHUNK_BYTES, RBLOB_STORE_TYPE_ID, RCatalogImpl, RDbImpl, RDeployGateImpl, RFILE_MAP_TYPE_ID, RSchemaImpl, RTableGroupImpl,
    catalogGroupHash, deployCatalogRelease, elementIdOf, grantCap, hashFileSource, registerIdentity, revokeCap, usersSchemaTables,
    rBlobStoreFactory, rCatalogFactory, rDbFactory, rDeployGateFactory, rFileMapFactory, rSchemaFactory, rTableGroupFactory,
    type CatalogFilesDef, type CatalogGroupDef, type ChunkPayload, type FileElement, type FileSource, type RBlobStoreImpl, type RFileMapImpl,
} from "@hyper-hyper-space/hhs3_rdb";
import { MemoryTarget } from "@hyper-hyper-space/hhs3_rdb_adapter";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";
import { MemoryDirectory, RdbProjection, STATE_PATH, elementTag, type FileMount } from "../src/index.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);
const WRITER = 'writer';

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function newCtx(): RContext {
    const ctx = createMockRContext({ selfValidate: true });
    const registry = ctx.getRegistry();
    registry.register(RSchemaImpl.typeId, rSchemaFactory);
    registry.register(RTableGroupImpl.typeId, rTableGroupFactory);
    registry.register(RDbImpl.typeId, rDbFactory);
    registry.register(RCatalogImpl.typeId, rCatalogFactory);
    registry.register(RDeployGateImpl.typeId, rDeployGateFactory);
    registry.register(RBLOB_STORE_TYPE_ID, rBlobStoreFactory);
    registry.register(RFILE_MAP_TYPE_ID, rFileMapFactory);
    return ctx;
}

const text = (s: string) => new TextEncoder().encode(s);

function source(bytes: Uint8Array): FileSource {
    return { size: bytes.length, async *read() { yield bytes; } };
}

function patterned(size: number, seed: number): Uint8Array {
    const bytes = new Uint8Array(size);
    let x = seed * 2654435761 >>> 0;
    for (let i = 0; i < size; i++) {
        x = (x * 1103515245 + 12345) >>> 0;
        bytes[i] = x >>> 24;
    }
    return bytes;
}

// Fails on the second pass (the upload) after `chunks` chunks.
function interruptedSource(bytes: Uint8Array, chunks: number): FileSource {
    let reads = 0;
    return {
        size: bytes.length,
        async *read() {
            const upload = ++reads === 2;
            for (let i = 0, n = 0; i < bytes.length; i += CHUNK_BYTES, n++) {
                if (upload && n === chunks) throw new Error('interrupted');
                yield bytes.subarray(i, Math.min(i + CHUNK_BYTES, bytes.length));
            }
        },
    };
}

function sameBytes(a: Uint8Array | undefined, b: Uint8Array): boolean {
    return a !== undefined && a.length === b.length && a.every((x, i) => x === b[i]);
}

async function bytesOf(dir: MemoryDirectory, path: string): Promise<Uint8Array | undefined> {
    if (await dir.stat(path) === undefined) return undefined;
    const parts: Uint8Array[] = [];
    for await (const part of dir.read(path)) parts.push(part);
    const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
    let offset = 0;
    for (const p of parts) { out.set(p, offset); offset += p.length; }
    return out;
}

async function poll(fn: () => boolean | Promise<boolean>, why: string, timeoutMs = 3000): Promise<void> {
    const start = Date.now();
    while (!await fn()) {
        if (Date.now() - start > timeoutMs) throw new Error(`poll timed out: ${why}`);
        await new Promise((r) => setTimeout(r, 10));
    }
}

type Env = {
    ctx: RContext;
    dev: OwnIdentity;
    admin: OwnIdentity;
    bob: OwnIdentity;
    catalog: RCatalogImpl;
    rdb: RDbImpl;
    group: RTableGroupImpl;
    store: RBlobStoreImpl;
    map: RFileMapImpl;
    userHash: B64Hash;
    projection: RdbProjection;
    dir: MemoryDirectory;
    mount: FileMount;
    stop(): Promise<void>;
};

// A catalog with a users group and a FILES `media` writable by 'writer' cap
// holders; admin (the projection's writer) and bob hold it.
async function filesEnv(seed: string, opts: { writer?: boolean; extra?: { name: string; path: string }[] } = {}): Promise<Env> {
    const ctx = newCtx();
    const dev = await makeIdentity();
    const admin = await makeIdentity();
    const bob = await makeIdentity();
    const creators = [{ keyId: dev.keyId, publicKey: dev.publicKey }];

    const schema = (await ctx.createObject(await RSchemaImpl.create({ name: 'hhs:users', creators, tables: usersSchemaTables() }))) as RSchemaImpl;
    const userDef: CatalogGroupDef = {
        name: 'user', seedSource: 'rdb', schemaRef: schema.getId(), schemaVersion: json.toSet([...await (await schema.getScopedDag()).getFrontier()]),
        idProvider: 'identities',
        initialRows: {
            identities: [{ values: { name: 'Admin' }, params: { keyId: { param: 'admin' }, publicKey: { param: 'admin', fn: 'publicKey' } } }],
            caps: [{ values: { label: 'manager' }, params: { grantee: { param: 'admin' } } }],
        },
    };
    const userHash = catalogGroupHash(userDef);
    const media: CatalogFilesDef = {
        name: 'media', bindings: { user: userHash }, idProvider: 'user.identities',
        canWrite: { p: 'exists', table: 'user.caps', where: { label: WRITER, grantee: '$author' } },
    };
    const catalog = (await ctx.createObject(await RCatalogImpl.create({
        name: `${seed}_catalog`, creators, author: dev, version: '1.0.0', add: [userDef], files: [media], params: [{ name: 'admin', type: 'identity' }],
    }))) as RCatalogImpl;
    const rdb = (await ctx.createObject(await RDbImpl.create({
        seed, catalog: catalog.getId(), release: catalog.getId(),
        params: { admin: { identity: { keyId: admin.keyId, publicKey: serializePublicKeyToBase64(admin.publicKey) } } },
    }))) as RDbImpl;
    await deployCatalogRelease(rdb, { release: catalog.getId() });

    const group = (await ctx.getObject((await rdb.getMemberGroupNames()).get('user')!)) as RTableGroupImpl;
    await grantCap(group, admin, admin.keyId, WRITER);
    await registerIdentity(group, bob);
    await grantCap(group, admin, bob.keyId, WRITER);

    const [member] = await rdb.getMemberFiles();
    const store = (await ctx.getObject(member.storeId)) as RBlobStoreImpl;
    const map = (await ctx.getObject(member.mapId)) as RFileMapImpl;

    const target = new MemoryTarget({ captureChanges: opts.writer !== false });
    const projection = await RdbProjection.open(rdb, ctx, target, { debounceMs: 5, ...(opts.writer === false ? {} : { writer: admin }) });
    const dir = new MemoryDirectory();
    const specs = [{ name: 'media', path: 'media' }, ...(opts.extra ?? [])];
    await projection.reconcileFiles(specs, () => dir, { scanIntervalMs: 0, debounceMs: 5 });
    const mount = projection.fileMount('media')!;
    return { ctx, dev, admin, bob, catalog, rdb, group, store, map, userHash, projection, dir, mount, stop: () => projection.stop() };
}

async function put(env: Env, author: OwnIdentity, element: Omit<FileElement, 'fileHash'>, bytes: Uint8Array): Promise<FileElement> {
    const stored = await env.store.putFile(source(bytes), author, { lane: 0 });
    const full = { ...element, fileHash: stored.fileHash } as FileElement;
    await env.map.add(full, author);
    return full;
}

async function listed(env: Env): Promise<string[]> {
    return (await env.map.list()).map((f) => `${f.section}${f.owner === env.admin.keyId ? ':admin' : f.owner === env.bob.keyId ? ':bob' : ''}:${f.path}`).sort();
}

async function keyDir(env: Env, who: OwnIdentity): Promise<string> {
    return `keys/${(await env.projection.idForKeyHash(who.keyId))!}`;
}

async function headersFor(store: RBlobStoreImpl, fileHash: B64Hash): Promise<number> {
    let n = 0;
    for await (const entry of (await store.getScopedDag()).loadAllEntries()) {
        const p = entry.payload as { action?: string; fileHash?: string };
        if (p.action === 'file' && p.fileHash === fileHash) n++;
    }
    return n;
}

export const filesTests = {
    title: '[FMOUNT] File mounts',
    tests: [
        {
            name: '[FMOUNT01] remote files materialize under common/ and keys/<id>/; the state is saved',
            invoke: async () => {
                const env = await filesEnv('fmount01');
                try {
                    const hello = text('hello');
                    await put(env, env.bob, { section: 'common', path: 'docs/hello.txt' }, hello);
                    await put(env, env.bob, { section: 'key', owner: env.bob.keyId, path: 'notes.md' }, text('bob notes'));
                    await env.mount.pass();

                    assertTrue(sameBytes(await bytesOf(env.dir, 'common/docs/hello.txt'), hello), 'a common file is written');
                    assertEquals(await env.dir.readText(`${await keyDir(env, env.bob)}/notes.md`), 'bob notes', "a key file goes in its owner's folder");
                    assertTrue(await env.dir.stat(STATE_PATH) !== undefined, 'the state is saved');
                    const status = env.mount.status();
                    assertEquals(`${status.state} ${status.files} ${status.missing} ${status.localOnly} ${status.writable}`, 'mounted 2 0 0 true', 'the status');
                    assertEquals(JSON.stringify(env.projection.filesStatus().map((s) => s.name)), '["media"]', 'the projection lists it');
                } finally { await env.stop(); }
            },
        },
        {
            name: '[FMOUNT02] an incomplete upload is pending and absent on disk, then written once complete',
            invoke: async () => {
                const env = await filesEnv('fmount02');
                try {
                    const big = patterned(2 * CHUNK_BYTES + 7, 3);
                    let failed = false;
                    try { await env.store.putFile(interruptedSource(big, 1), env.bob, { lane: 3 }); } catch { failed = true; }
                    assertTrue(failed, 'the upload stops after one chunk');
                    const fileHash = await hashFileSource(source(big));
                    await env.map.add({ section: 'common', path: 'big.bin', fileHash }, env.bob);
                    await env.mount.pass();
                    assertTrue(await env.dir.stat('common/big.bin') === undefined, 'an incomplete file is absent');
                    assertEquals(env.mount.status().missing, 1, 'and missing');

                    const [tail] = [...await env.store.laneCover(3)];
                    const header = ((await (await env.store.getScopedDag()).loadEntry(tail))!.payload as unknown as ChunkPayload).header;
                    await env.store.putFile(source(big), env.bob, { lane: 3, resume: { header, tail } });
                    await env.mount.pass();
                    assertTrue(sameBytes(await bytesOf(env.dir, 'common/big.bin'), big), 'the completed file is written');
                    assertEquals(env.mount.status().missing, 0, 'nothing is missing');
                } finally { await env.stop(); }
            },
        },
        {
            name: '[FMOUNT03] ingest: new files are added, an edit replaces, a delete removes; a remote remove deletes',
            invoke: async () => {
                const env = await filesEnv('fmount03');
                try {
                    await env.mount.pass();
                    const mine = await keyDir(env, env.admin);
                    await env.dir.writeText('common/a.txt', 'one');
                    await env.dir.writeText(`${mine}/private/b.txt`, 'mine');
                    await env.mount.pass();
                    assertEquals((await listed(env)).join(','), 'common:a.txt,key:admin:private/b.txt', 'both files are added');
                    assertTrue(await env.store.findFile(await hashFileSource(source(text('one')))) !== undefined, 'the bytes are in the store');

                    await env.dir.writeText('common/a.txt', 'two');
                    await env.mount.pass();
                    const [a] = (await env.map.list()).filter((f) => f.path === 'a.txt');
                    assertEquals(a.fileHash, await hashFileSource(source(text('two'))), 'an edit replaces the element');
                    assertEquals((await env.map.list()).filter((f) => f.path === 'a.txt').length, 1, 'with no copy left behind');

                    await env.dir.remove(`${mine}/private/b.txt`);
                    await env.mount.pass();
                    assertEquals((await listed(env)).join(','), 'common:a.txt', 'a delete removes the element');

                    const bobs = await put(env, env.bob, { section: 'common', path: 'c.txt' }, text('from bob'));
                    await env.mount.pass();
                    assertEquals(await env.dir.readText('common/c.txt'), 'from bob', 'a remote file arrives');
                    await env.map.remove(bobs, env.bob);
                    await env.mount.pass();
                    assertTrue(await env.dir.stat('common/c.txt') === undefined, 'a remote remove deletes the unchanged file');

                    const again = await put(env, env.bob, { section: 'common', path: 'd.txt' }, text('d'));
                    await env.mount.pass();
                    await env.dir.writeText('common/d.txt', 'edited here');
                    await env.map.remove(again, env.bob);
                    await env.mount.pass();
                    assertEquals(await env.dir.readText('common/d.txt'), 'edited here', 'a locally edited file is not deleted');
                    assertTrue((await listed(env)).includes('common:d.txt'), 'and is added back as the local version');
                } finally { await env.stop(); }
            },
        },
        {
            name: "[FMOUNT04] ingest resumes this writer's interrupted upload and uses the least loaded lane",
            invoke: async () => {
                const env = await filesEnv('fmount04');
                try {
                    const big = patterned(3 * CHUNK_BYTES, 9);
                    const fileHash = await hashFileSource(source(big));
                    try { await env.store.putFile(interruptedSource(big, 2), env.admin, { lane: 5 }); } catch { /* interrupted */ }
                    await env.store.putFile(source(patterned(CHUNK_BYTES, 1)), env.bob, { lane: 0 });
                    await env.mount.pass();

                    await env.dir.write('common/big.bin', [big]);
                    await env.mount.pass();
                    assertEquals(await headersFor(env.store, fileHash), 1, 'the upload resumed its chain');
                    assertTrue((await env.store.findFile(fileHash)) !== undefined, 'and completed it');
                    assertTrue((await listed(env)).includes('common:big.bin'), 'the file is added');

                    await env.dir.write('common/small.bin', [patterned(10, 4)]);
                    await env.mount.pass();
                    const stored = (await env.store.findFile(await hashFileSource(source(patterned(10, 4)))))!;
                    assertEquals(await env.store.laneOf(stored.header), 1, 'a new upload goes to the lowest empty lane');
                } finally { await env.stop(); }
            },
        },
        {
            name: '[FMOUNT05] several hashes at a path, and case-fold collisions, get ~<8 hex> names; one left goes back',
            invoke: async () => {
                const env = await filesEnv('fmount05');
                try {
                    const one = await put(env, env.bob, { section: 'common', path: 'dup.txt' }, text('one'));
                    const two = await put(env, env.admin, { section: 'common', path: 'dup.txt' }, text('two'));
                    const upper = await put(env, env.bob, { section: 'common', path: 'README.md' }, text('upper'));
                    const lower = await put(env, env.bob, { section: 'common', path: 'Readme.md' }, text('lower'));
                    await env.mount.pass();

                    const tag = (e: FileElement) => elementTag(elementIdOf(e));
                    assertEquals(await env.dir.readText(`common/dup~${tag(one)}.txt`), 'one', 'each hash at a path gets its tag');
                    assertEquals(await env.dir.readText(`common/dup~${tag(two)}.txt`), 'two', 'both of them');
                    assertTrue(await env.dir.stat('common/dup.txt') === undefined, 'and the bare name is free');
                    assertEquals(await env.dir.readText(`common/README~${tag(upper)}.md`), 'upper', 'a case-fold collision is tagged');
                    assertEquals(await env.dir.readText(`common/Readme~${tag(lower)}.md`), 'lower', 'on both sides');

                    await env.map.remove(two, env.admin);
                    await env.mount.pass();
                    assertEquals(await env.dir.readText('common/dup.txt'), 'one', 'the one left takes the bare name back');
                    assertEquals(env.dir.paths('common').filter((p) => p.startsWith('common/dup')).length, 1, 'and nothing else remains');
                    assertEquals((await listed(env)).filter((p) => p.endsWith('dup.txt')).length, 1, 'the rename appended nothing');
                } finally { await env.stop(); }
            },
        },
        {
            name: '[FMOUNT06] local-only files are reported and never synced; blocking and edited files move aside to ~local; deletes are restored',
            invoke: async () => {
                const env = await filesEnv('fmount06');
                try {
                    await put(env, env.bob, { section: 'key', owner: env.bob.keyId, path: 'seed.txt' }, text('seed'));
                    await env.mount.pass();
                    const bobDir = await keyDir(env, env.bob);

                    await env.dir.writeText('loose.txt', 'at the root');
                    await env.dir.writeText(`${bobDir}/mine.txt`, 'not mine to share');
                    await env.dir.writeText('common/bad:name.txt', 'bad name');
                    await env.dir.writeText(`${bobDir}/f.txt`, 'local first');
                    await env.mount.pass();
                    const reasons = env.mount.localOnlyFiles().map((f) => `${f.reason} ${f.path}`).sort().join('; ');
                    assertTrue(reasons.includes('outside loose.txt') && reasons.includes(`foreign ${bobDir}/mine.txt`) && reasons.includes('rejected common/bad:name.txt'),
                        `local-only reasons: ${reasons}`);
                    assertEquals((await listed(env)).join(','), 'key:bob:seed.txt', 'none of them is synced');

                    await put(env, env.bob, { section: 'key', owner: env.bob.keyId, path: 'f.txt' }, text('from bob'));
                    await env.mount.pass();
                    assertEquals(await env.dir.readText(`${bobDir}/f.txt`), 'from bob', 'the synced file takes its name');
                    assertEquals(await env.dir.readText(`${bobDir}/f~local.txt`), 'local first', 'the local file moved aside');

                    await env.dir.remove(`${bobDir}/f.txt`);
                    await env.mount.pass();
                    assertEquals(await env.dir.readText(`${bobDir}/f.txt`), 'from bob', 'a delete in a read-only section is restored');
                    await env.dir.writeText(`${bobDir}/seed.txt`, 'scribbled');
                    await env.mount.pass();
                    assertEquals(await env.dir.readText(`${bobDir}/seed.txt`), 'seed', 'an edit in a read-only section is restored');
                    assertEquals(await env.dir.readText(`${bobDir}/seed~local.txt`), 'scribbled', 'after moving the edit aside');
                    assertEquals((await listed(env)).join(','), 'key:bob:f.txt,key:bob:seed.txt', 'still nothing local is synced');
                } finally { await env.stop(); }
            },
        },
        {
            name: '[FMOUNT07] without a writer the mount is read-only: a new file and a delete in common/ stay, waiting, and nothing is synced',
            invoke: async () => {
                const env = await filesEnv('fmount07', { writer: false });
                try {
                    await put(env, env.bob, { section: 'common', path: 'shared.txt' }, text('shared'));
                    await env.mount.pass();
                    await env.dir.writeText('common/new.txt', 'local');
                    await env.dir.remove('common/shared.txt');
                    await env.mount.pass();
                    assertTrue(await env.dir.stat('common/shared.txt') === undefined, 'the delete stays');
                    assertEquals(await env.dir.readText('common/new.txt'), 'local', 'the new file stays');
                    assertEquals(env.mount.localOnlyFiles().length, 0, 'neither is local-only');
                    const status = env.mount.status();
                    assertEquals(`${status.writable} ${status.waiting} ${status.missing}`, 'false 2 0', 'both wait');
                    assertEquals((await listed(env)).join(','), 'common:shared.txt', 'nothing was added or removed');
                    assertEquals(env.dir.ensuredDirs().join(','), 'common,keys', 'with no key id, only common/ and keys/ are made');
                    assertEquals(env.dir.preservedDirs().length, 0, 'and nothing is preserved');
                } finally { await env.stop(); }
            },
        },
        {
            name: '[FMOUNT08] a lost state is rebuilt: matching files are adopted by hash, nothing is uploaded again',
            invoke: async () => {
                const env = await filesEnv('fmount08');
                try {
                    await put(env, env.bob, { section: 'common', path: 'x.txt' }, text('x'));
                    await env.dir.writeText('common/y.txt', 'y');
                    await env.mount.pass();
                    const before = [...await (await env.map.getScopedDag()).getFrontier()].sort().join(',');
                    const storeBefore = [...await (await env.store.getScopedDag()).getFrontier()].sort().join(',');
                    const mtime = (await env.dir.stat('common/x.txt'))!.mtimeMs;

                    await env.dir.remove(STATE_PATH);
                    await env.mount.pass();
                    assertEquals([...await (await env.map.getScopedDag()).getFrontier()].sort().join(','), before, 'no map ops');
                    assertEquals([...await (await env.store.getScopedDag()).getFrontier()].sort().join(','), storeBefore, 'no uploads');
                    assertEquals((await env.dir.stat('common/x.txt'))!.mtimeMs, mtime, 'the file was adopted, not rewritten');
                    assertEquals(env.mount.status().missing, 0, 'nothing is missing');
                    assertTrue(await env.dir.stat(STATE_PATH) !== undefined, 'the state is saved again');
                } finally { await env.stop(); }
            },
        },
        {
            name: '[FMOUNT09] a mount for a FILES not deployed yet is pending, and attaches when a release adds it',
            invoke: async () => {
                const env = await filesEnv('fmount09', { extra: [{ name: 'attachments', path: 'shared/attachments' }] });
                try {
                    assertEquals(env.projection.filesStatus().map((s) => `${s.name}:${s.state}`).join(','), 'media:mounted,attachments:pending', 'pending first');
                    const attachments: CatalogFilesDef = { name: 'attachments', bindings: { user: env.userHash }, idProvider: 'user.identities', canWrite: { p: 'true' } };
                    const release = await env.catalog.release({ version: '1.1.0', files: [attachments] }, env.dev);
                    await deployCatalogRelease(env.rdb, { release });
                    await poll(() => env.projection.filesStatus().every((s) => s.state === 'mounted'), 'the pending mount attaches');
                    const report = await env.projection.reconcileFiles([{ name: 'media', path: 'media' }], () => env.dir, { scanIntervalMs: 0 });
                    assertEquals(`${report.mounted.join(',')}|${report.pending.join(',')}`, 'media|', 'a mount left out of the specs is detached');
                    assertTrue(env.projection.fileMount('attachments') === undefined, 'and gone');
                } finally { await env.stop(); }
            },
        },
        {
            name: "[FMOUNT10] a new mount makes common/, keys/ and the writer's keys/<id>/, preserved, even before the key can write",
            invoke: async () => {
                const env = await filesEnv('fmount10');
                const carol = await makeIdentity();
                const uncapped = await RdbProjection.open(env.rdb, env.ctx, new MemoryTarget({ captureChanges: true }), { debounceMs: 5, writer: carol });
                try {
                    const mine = await keyDir(env, env.admin);
                    assertEquals(env.dir.ensuredDirs().join(','), `common,keys,${mine}`, 'the three folders are made on an empty mount');
                    assertEquals(env.dir.preservedDirs().join(','), mine, "only the writer's folder is preserved");
                    assertEquals(env.dir.paths().filter((p) => !p.startsWith('.hhs/')).length, 0, 'and no file is written into them');

                    const carolDir = new MemoryDirectory();
                    await uncapped.reconcileFiles([{ name: 'media', path: 'media' }], () => carolDir, { scanIntervalMs: 0, debounceMs: 5 });
                    assertEquals(uncapped.fileMount('media')!.status().writable, false, 'carol holds no writer cap');
                    const hers = `keys/${(await uncapped.idForKeyHash(carol.keyId))!}`;
                    assertEquals(carolDir.ensuredDirs().join(','), `common,keys,${hers}`, 'her folder is made before she can write');
                    assertEquals(carolDir.preservedDirs().join(','), hers, 'and preserved');
                } finally {
                    await uncapped.stop();
                    await env.stop();
                }
            },
        },
        {
            name: "[FMOUNT11] a key that can't write keeps its changes waiting; a grant uploads them and a revoke is seen, through the bound group alone",
            invoke: async () => {
                const env = await filesEnv('fmount11');
                const carol = await makeIdentity();
                await registerIdentity(env.group, carol);
                const uncapped = await RdbProjection.open(env.rdb, env.ctx, new MemoryTarget({ captureChanges: true }), { debounceMs: 5, writer: carol });
                const dir = new MemoryDirectory();
                try {
                    await put(env, env.bob, { section: 'common', path: 'edit.txt' }, text('bob edit'));
                    await put(env, env.bob, { section: 'common', path: 'drop.txt' }, text('bob drop'));
                    const kept = await put(env, env.bob, { section: 'common', path: 'kept.txt' }, text('bob v1'));
                    await uncapped.reconcileFiles([{ name: 'media', path: 'media' }], () => dir, { scanIntervalMs: 0, debounceMs: 5 });
                    const mount = uncapped.fileMount('media')!;
                    assertEquals(dir.paths('common').join(','), 'common/drop.txt,common/edit.txt,common/kept.txt', "bob's files are written");

                    await dir.writeText('common/edit.txt', 'carol edit');
                    await dir.remove('common/drop.txt');
                    await dir.writeText('common/new.txt', 'carol new');
                    await mount.pass();
                    assertEquals(await dir.readText('common/edit.txt'), 'carol edit', 'the edit stays');
                    assertTrue(await dir.stat('common/drop.txt') === undefined, 'the delete stays');
                    assertEquals(await dir.readText('common/new.txt'), 'carol new', 'the new file stays');
                    assertEquals(dir.paths().filter((p) => p.includes('~local')).length, 0, 'nothing is moved aside');
                    const waiting = mount.status();
                    assertEquals(`${waiting.writable} ${waiting.waiting} ${waiting.localOnly}`, 'false 3 0', 'three changes wait');
                    assertEquals((await listed(env)).join(','), 'common:drop.txt,common:edit.txt,common:kept.txt', 'nothing is uploaded');

                    await env.map.remove(kept, env.bob);
                    await put(env, env.bob, { section: 'common', path: 'kept.txt' }, text('bob v2'));
                    await poll(async () => await dir.readText('common/kept.txt') === 'bob v2', 'a remote update to an untouched file still arrives');

                    await mount.pass();
                    await new Promise((r) => setTimeout(r, 50));
                    assertEquals(mount.status().waiting, 3, 'the changes still wait');

                    await grantCap(env.group, env.admin, carol.keyId, WRITER);
                    await poll(async () => (await listed(env)).join(',') === 'common:edit.txt,common:kept.txt,common:new.txt', 'the grant uploads the changes');
                    const [edit] = (await env.map.list()).filter((f) => f.path === 'edit.txt');
                    assertEquals(edit.fileHash, await hashFileSource(source(text('carol edit'))), 'with her edit');
                    await poll(() => mount.status().writable === true && mount.status().waiting === 0, 'nothing waits');

                    await revokeCap(env.group, env.admin, carol.keyId, WRITER);
                    await poll(() => mount.status().writable === false, 'the revoke is seen');
                } finally {
                    await uncapped.stop();
                    await env.stop();
                }
            },
        },
    ],
};

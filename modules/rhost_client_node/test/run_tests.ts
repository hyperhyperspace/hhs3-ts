import { promises as fs, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Database from "better-sqlite3";

import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import type { RCatalogImpl } from "@hyper-hyper-space/hhs3_rdb";
import { MemoryKeyVault, RdbRuntime } from "@hyper-hyper-space/hhs3_rdb_runtime";
import type { Rhost } from "@hyper-hyper-space/hhs3_rhost";
import { isHostStatus, type ClientEvent, type ClientStatus } from "@hyper-hyper-space/hhs3_rhost_client";
import { createNodeSyncMeshFactory, initApp, KeyStore, openApp } from "@hyper-hyper-space/hhs3_rhost_node";
import { exportRelease, releaseFileName, serializeReleaseFile } from "@hyper-hyper-space/hhs3_rpack";
import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { hostKey, openClient, projectionPath } from "../src/index.js";

const EDITOR_SQL = join(process.cwd(), '../rdb/examples/editor.sql');
const hashSuite = createBasicCrypto().hash(HASH_SHA256);

function nameRef(text: string) {
    return { kind: 'name' as const, text, parts: text.split('.'), span: { start: 0, end: text.length, line: 1, column: 1 } };
}

let release: Promise<string> | undefined;

// The editor catalog at 1.0.0, as a release file.
function editorRelease(): Promise<string> {
    release ??= (async () => {
        const dir = await fs.mkdtemp(join(tmpdir(), 'rhost-releases-'));
        process.once('exit', () => { rmSync(dir, { recursive: true, force: true }); });
        const runtime = await RdbRuntime.openMemory({ keyVault: new MemoryKeyVault() });
        try {
            await runtime.session.createKey('admin', 'pw');
            runtime.session.selectAuthor('admin');
            await runtime.execute(await fs.readFile(EDITOR_SQL, 'utf8'));
            const catalogId = (await runtime.workspace.roots.resolveCatalog(nameRef('editor'))).id;
            const catalog = (await runtime.workspace.replica.getObject(catalogId)) as unknown as RCatalogImpl;
            const frontier = await (await catalog.getScopedDag()).getFrontier();
            const [hash] = (await catalog.getIndex()).findReleasesByVersion('1.0.0', frontier);
            const path = join(dir, releaseFileName('editor', '1.0.0', hash!));
            await fs.writeFile(path, serializeReleaseFile(await exportRelease(runtime.workspace.replica, catalogId, hash!)));
            return path;
        } finally {
            await runtime.close();
        }
    })();
    return release;
}

async function waitFor(check: () => Promise<boolean> | boolean, what: string, timeoutMs = 15_000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await check()) return;
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
    if (await check()) return;
    throw new Error(`timed out waiting for ${what}`);
}

type Fixture = { dir: string; keystore: string; hostDir: string; app: Rhost };

// An app with a started 'default' host, key 'me' with passphrase 'pw' in a
// keystore beside the app.
async function withRunningHost(fn: (fixture: Fixture) => Promise<void>): Promise<void> {
    const dir = await fs.mkdtemp(join(tmpdir(), 'rh-cn-'));
    const appDir = join(dir, 'app');
    const keystore = join(dir, 'user-keys.json');
    await (await KeyStore.open(keystore, hashSuite)).create('me', 'pw');
    await initApp(appDir, { releases: [await editorRelease()], params: { admin: '$me' } });
    const app = await openApp(appDir, {
        keystore,
        passphrase: async () => 'pw',
        meshFactory: createNodeSyncMeshFactory({ folderRoot: join(dir, 'mesh') }),
    });
    try {
        await app.create(undefined, { key: 'me', passphrase: 'prompt', sync: { scope: 'localhost' } });
        await app.start('default');
        await fn({ dir, keystore, hostDir: join(appDir, 'hosts', 'default'), app });
    } finally {
        await app.close();
        await fs.rm(dir, { recursive: true, force: true });
    }
}

async function newPublicKey(): Promise<string> {
    return serializePublicKeyToBase64((await createIdentity(SIGNING_ED25519, hashSuite)).publicKey);
}

// An app write that the host's key may not make: it has no writer cap.
function rejectedWrite(hostDir: string): void {
    const db = new Database(projectionPath(hostDir));
    db.pragma('busy_timeout = 5000');
    try {
        db.prepare('INSERT INTO doc_pages (title, deleted) VALUES (?, ?)').run('not a writer', 0);
    } finally {
        db.close();
    }
}

const tests: { name: string; invoke: () => Promise<void> }[] = [
    {
        name: '[RHOST_CLIENT_NODE01] on a running host: me, registerKey and events agree with the projection; status comes from run/rhost.sock',
        invoke: async () => {
            await withRunningHost(async ({ hostDir, app }) => {
                const host = await app.host('default');
                const projection = host.projection!;
                const client = openClient(hostDir);
                try {
                    assertEquals(projectionPath(hostDir), join(hostDir, 'db', 'data.sqlite'), 'the default projection file');
                    const me = await client.me();
                    const key = app.keyVault.resolvePublic('me');
                    assertEquals(me.label, 'me', 'the key label');
                    assertEquals(me.keyId, key.keyId, 'the host key');
                    assertEquals(me.publicKey, serializePublicKeyToBase64(key.publicKey), 'its public key');
                    assertEquals(me.id, await projection.idForKeyHash(me.keyId), "me's id is the projection's");
                    assertEquals((await hostKey(hostDir)).keyId, me.keyId, 'hostKey reads the same key');
                    assertEquals(JSON.stringify(await host.client.me()), JSON.stringify(me), 'and so does the in-process client');

                    const bob = await newPublicKey();
                    const bobId = await client.registerKey(bob);
                    assertEquals(await client.registerKey(bob), bobId, 'registerKey is idempotent');
                    assertEquals(await host.client.registerKey(bob), bobId, 'the projection gives the same id');
                    const carol = await newPublicKey();
                    const carolId = await host.client.registerKey(carol);
                    assertEquals(await client.registerKey(carol), carolId, 'and the other way around');

                    const watched: ClientEvent[] = [];
                    const unwatch = client.events.watch((batch) => { watched.push(...batch); });
                    rejectedWrite(hostDir);
                    await waitFor(() => watched.some((e) => e.origin === 'ingestion' && e.direction === 'failure' && e.table === 'doc_pages'),
                        'the rejected write reaches the watch');
                    unwatch();
                    const fromFile = await client.events.since();
                    const inProcess = await host.client.events.since();
                    assertEquals(JSON.stringify(fromFile), JSON.stringify(inProcess), 'the file and the projection give the same events');
                    assertEquals((await client.events.since(fromFile[0]!.id)).length, fromFile.length - 1, 'since skips up to an id');

                    const status = await client.status();
                    assertTrue(isHostStatus(status) && status.running && typeof status.peers === 'number' && status.host === 'default',
                        `the socket status (${JSON.stringify(status)})`);

                    const statuses: ClientStatus[] = [];
                    const stopWatch = client.watchStatus((s) => { statuses.push(s); });
                    await waitFor(() => statuses.some((s) => s.running), 'the first watch push');
                    stopWatch();
                } finally {
                    await client.close();
                }
            });
        },
    },
    {
        name: '[RHOST_CLIENT_NODE02] on a stopped host: status says so from host.json; keys and events still work, without the keystore; the watch reconnects',
        invoke: async () => {
            await withRunningHost(async ({ keystore, hostDir, app }) => {
                rejectedWrite(hostDir);
                const host = await app.host('default');
                await waitFor(async () => (await host.client.events.since()).length > 0, 'the event is logged');
                const running = openClient(hostDir);
                const me = await running.me();
                await running.close();
                await app.stop('default');
                await fs.rm(keystore);

                const client = openClient(hostDir);
                try {
                    const status = await client.status();
                    assertTrue(!isHostStatus(status) && !status.running, 'a stopped status');
                    assertEquals(status.host, 'default', 'the host folder');
                    assertEquals(status.database, host.record.database, 'the database from host.json');
                    assertTrue(status.created && status.catalog === 'editor', 'created, with its catalog');

                    assertEquals((await client.me()).id, me.id, 'me works offline, with no keystore');
                    const dave = await newPublicKey();
                    const daveId = await client.registerKey(dave);
                    assertEquals(await client.registerKey(dave), daveId, 'registerKey works offline');
                    assertTrue((await client.events.since()).some((e) => e.table === 'doc_pages'), 'events work offline');

                    const statuses: ClientStatus[] = [];
                    const stopWatch = client.watchStatus((s) => { statuses.push(s); });
                    await waitFor(() => statuses.some((s) => !s.running), 'the watch reports the stopped host');
                    await app.start('default');
                    await waitFor(() => statuses.some((s) => s.running), 'and reconnects when it starts');
                    stopWatch();
                    const projection = (await app.host('default')).projection!;
                    assertEquals(await projection.idForKeyHash((await hostKey(hostDir)).keyId), me.id, 'the offline ids stay');
                } finally {
                    await client.close();
                }
            });
        },
    },
    {
        name: '[RHOST_CLIENT_NODE03] projectionPath: app.json\'s projection.path, unless host.json overrides it; hostKey reads host.json only',
        invoke: async () => {
            const dir = await fs.mkdtemp(join(tmpdir(), 'rhost-cn-path-'));
            try {
                const hostDir = join(dir, 'app', 'hosts', 'default');
                await fs.mkdir(hostDir, { recursive: true });
                const record = {
                    database: 'DB', catalog: 'editor', created: true,
                    key: { label: 'me', keyId: 'KID', publicKey: 'PK' }, sync: { scope: 'localhost' },
                };
                const writeRecord = (extra: object) => fs.writeFile(join(hostDir, 'host.json'), JSON.stringify({ ...record, ...extra }));

                await writeRecord({});
                let missing = '';
                try { projectionPath(hostDir); } catch (e) { missing = String(e); }
                assertTrue(missing.includes("the app's app.json"), `a missing app.json is named (${missing})`);

                await fs.writeFile(join(dir, 'app', 'app.json'), JSON.stringify({ projection: { path: 'custom/app.sqlite' } }));
                assertEquals(projectionPath(hostDir), join(hostDir, 'custom', 'app.sqlite'), "app.json's path, in the host folder");

                await writeRecord({ projection: { path: 'mine.sqlite' } });
                assertEquals(projectionPath(hostDir), join(hostDir, 'mine.sqlite'), "host.json's override");

                assertEquals(JSON.stringify(await hostKey(hostDir)), '{"label":"me","keyId":"KID","publicKey":"PK"}', 'the key as host.json has it');
                await writeRecord({ key: 'me' });
                let noKey = '';
                try { await hostKey(hostDir); } catch (e) { noKey = String(e); }
                assertTrue(noKey.includes('has no key with a label, keyId and publicKey'), `a key without its parts is refused (${noKey})`);
            } finally {
                await fs.rm(dir, { recursive: true, force: true });
            }
        },
    },
];

async function main() {
    const filters = process.argv.slice(2);

    console.log('Running tests for Hyper Hyper Space v3 rhost_client_node module'
        + (filters.length > 0 ? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');

    console.log('rhost_client_node');

    for (const test of tests) {
        let match = true;
        for (const filter of filters) {
            match = match && test.name.indexOf(filter) >= 0;
        }

        if (match) {
            testing.exitIfFailed(await testing.run(test.name, test.invoke));
        } else {
            await testing.skip(test.name);
        }
    }

    console.log();
}

main();

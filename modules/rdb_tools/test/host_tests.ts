import { spawn } from "node:child_process";
import { existsSync, promises as fs, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import Database from "better-sqlite3";

import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import type { RCatalogImpl } from "@hyper-hyper-space/hhs3_rdb";
import { MemoryKeyVault, RdbRuntime } from "@hyper-hyper-space/hhs3_rdb_runtime";
import { formatReleases, type HostSetup } from "@hyper-hyper-space/hhs3_rhost";
import { exportRelease, releaseFileName, serializeReleaseFile } from "@hyper-hyper-space/hhs3_rpack";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { createNodeSyncMeshFactory, initApp, KeyStore, openApp } from "@hyper-hyper-space/hhs3_rhost_node";

const EDITOR_SQL = join(process.cwd(), '../rdb/examples/editor.sql');
const SECOND_RELEASE = `
    ALTER SCHEMA hhs:doc VERSION '1.1.0' AS (ADD COLUMN pages.tag string NULL);
    ALTER CATALOG editor VERSION '1.1.0' AS (UPDATE SCHEMA hhs:doc TO LATEST ON doc) NOTE 'tags' BY $admin;
`;
const THIRD_RELEASE = "ALTER CATALOG editor VERSION '1.2.0' PARAMS (:moderator identity) BY $admin;";
const hashSuite = createBasicCrypto().hash(HASH_SHA256);
const PASS_ENV = 'RHOST_TEST_PASS';
const HOST_FLAGS = ['--key', 'me', '--scope', 'localhost', '--passphrase-env', PASS_ENV];

function nameRef(text: string) {
    return { kind: 'name' as const, text, parts: text.split('.'), span: { start: 0, end: text.length, line: 1, column: 1 } };
}

// 1.1.0 adds a column; 1.2.0 declares the identity param :moderator.
type ReleasePaths = { v100: string; v110: string; v120: string };

let releases: Promise<ReleasePaths> | undefined;

// The editor catalog at 1.0.0, 1.1.0 and 1.2.0, written as release files once.
function editorReleases(): Promise<ReleasePaths> {
    releases ??= (async () => {
        const dir = await fs.mkdtemp(join(tmpdir(), 'rhost-releases-'));
        process.once('exit', () => { rmSync(dir, { recursive: true, force: true }); });
        const runtime = await RdbRuntime.openMemory({ keyVault: new MemoryKeyVault() });
        try {
            await runtime.session.createKey('admin', 'pw');
            runtime.session.selectAuthor('admin');
            await runtime.execute(await fs.readFile(EDITOR_SQL, 'utf8'));
            const catalogId = (await runtime.workspace.roots.resolveCatalog(nameRef('editor'))).id;
            const write = async (version: string): Promise<string> => {
                const catalog = (await runtime.workspace.replica.getObject(catalogId)) as unknown as RCatalogImpl;
                const frontier = await (await catalog.getScopedDag()).getFrontier();
                const [release] = (await catalog.getIndex()).findReleasesByVersion(version, frontier);
                const file = await exportRelease(runtime.workspace.replica, catalogId, release!);
                const path = join(dir, releaseFileName('editor', version, release!));
                await fs.writeFile(path, serializeReleaseFile(file));
                return path;
            };
            const v100 = await write('1.0.0');
            await runtime.execute(SECOND_RELEASE);
            const v110 = await write('1.1.0');
            await runtime.execute(THIRD_RELEASE);
            const v120 = await write('1.2.0');
            return { v100, v110, v120 };
        } finally {
            await runtime.close();
        }
    })();
    return releases;
}

async function withDir<T>(prefix: string, fn: (dir: string) => Promise<T>): Promise<T> {
    const dir = await fs.mkdtemp(join(tmpdir(), prefix));
    try {
        return await fn(dir);
    } finally {
        await fs.rm(dir, { recursive: true, force: true });
    }
}

// A keystore beside the app, standing in for the user's, with key 'me'
// (passphrase 'pw').
async function userKeystore(dir: string): Promise<string> {
    const path = join(dir, 'user-keys.json');
    await (await KeyStore.open(path, hashSuite)).create('me', 'pw');
    return path;
}

// An app folder whose app.json points at that keystore.
async function newApp(dir: string, releaseFiles: string[]): Promise<string> {
    const app = join(dir, 'app');
    await userKeystore(dir);
    await initApp(app, { releases: releaseFiles, params: { admin: '$me' } });
    await editConfig(app, (config) => { config['keystore'] = '../user-keys.json'; });
    return app;
}

async function editConfig(app: string, edit: (config: Record<string, unknown>) => void): Promise<void> {
    const path = join(app, 'app.json');
    const config = JSON.parse(await fs.readFile(path, 'utf8')) as Record<string, unknown>;
    edit(config);
    await fs.writeFile(path, JSON.stringify(config, undefined, 2) + '\n');
}

async function readHostJson(app: string, name: string): Promise<Record<string, any>> {
    return JSON.parse(await fs.readFile(join(app, 'hosts', name, 'host.json'), 'utf8')) as Record<string, any>;
}

function appOptions(dir: string) {
    return {
        passphrase: async () => 'pw',
        meshFactory: createNodeSyncMeshFactory({ folderRoot: join(dir, 'mesh') }),
    };
}

const SETUP: HostSetup = { key: 'me', passphrase: 'prompt', sync: { scope: 'localhost' } };

type Run = { code: number; stdout: string; stderr: string };

// `--import ../../register.mjs` resolves `ts-node` from the child's cwd. From
// the module folder that walks up to the repo; from anywhere else it doesn't,
// so the loader is registered with the repo as its parent.
function loaderArgs(cwd: string): string[] {
    const out: string[] = [];
    const elsewhere = resolve(cwd) !== resolve(process.cwd());
    const repoLoader = 'data:text/javascript,' + encodeURIComponent(
        `import { register } from 'node:module'; import { pathToFileURL } from 'node:url';`
        + ` register('ts-node/esm', pathToFileURL(${JSON.stringify(resolve(process.cwd(), '../..') + '/')}));`,
    );
    let replaced = false;
    for (let i = 0; i < process.execArgv.length; i++) {
        const arg = process.execArgv[i]!;
        if (arg === '--import' && process.execArgv[i + 1] !== undefined) {
            out.push(arg, elsewhere ? repoLoader : resolve(process.execArgv[i + 1]!));
            replaced = true;
            i += 1;
        } else {
            out.push(arg);
        }
    }
    if (elsewhere && !replaced) out.push('--import', repoLoader);
    return out;
}

// Runs a bin from source, through the same loader as the tests.
function runBin(bin: 'rdb' | 'rhost', args: string[], env: NodeJS.ProcessEnv = {}, cwd = process.cwd()): Promise<Run> {
    return new Promise((done, fail) => {
        const child = spawn(process.execPath, [...loaderArgs(cwd), join(process.cwd(), 'bin', `${bin}.ts`), ...args], {
            cwd,
            env: { ...process.env, ...env },
            stdio: ['ignore', 'pipe', 'pipe'],
        });
        let stdout = '';
        let stderr = '';
        child.stdout.on('data', (chunk) => { stdout += chunk; });
        child.stderr.on('data', (chunk) => { stderr += chunk; });
        child.on('error', fail);
        child.on('close', (code) => done({ code: code ?? 1, stdout, stderr }));
    });
}

async function waitFor(check: () => Promise<boolean> | boolean, what: string, timeoutMs = 15_000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await check()) return;
        await new Promise((resolve) => setTimeout(resolve, 100));
    }
    if (await check()) return;
    throw new Error(`timed out waiting for ${what}`);
}

function readApp<T>(path: string, query: (db: Database.Database) => T): T {
    const db = new Database(path, { readonly: true, fileMustExist: true });
    db.pragma('busy_timeout = 5000');
    try {
        return query(db);
    } finally {
        db.close();
    }
}

function tableHas(path: string, table: string): boolean {
    return readApp(path, (db) => db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?").get(table) !== undefined);
}

function titles(path: string): string[] {
    if (!existsSync(path) || !tableHas(path, 'doc_pages')) return [];
    return readApp(path, (db) => (db.prepare('SELECT title FROM doc_pages').all() as { title: string }[]).map((r) => r.title));
}

function killQuietly(pid: number): void {
    try { process.kill(pid, 'SIGKILL'); } catch { /* already gone */ }
}

export const hostTests = [
    {
        name: '[RDB_TOOLS55] rhost start runs each host in its own background process; stop and remove end them',
        invoke: async () => {
            const { v100 } = await editorReleases();
            await withDir('rh-cli-', async (dir) => {
                const appDir = await newApp(dir, [v100]);
                const env = { [PASS_ENV]: 'pw' };
                const rhost = (...args: string[]) => runBin('rhost', ['--app', appDir, ...args], env);
                const pids = new Set<number>();
                try {
                    for (const name of ['default', 'gaming']) {
                        const created = await rhost('create', name, ...HOST_FLAGS);
                        assertEquals(created.code, 0, `create ${name}: ${created.stdout}${created.stderr}`);
                        assertTrue(created.stdout.includes(`start it with: rhost start ${name}`), 'create does not start it');
                    }

                    const started = await rhost('start');
                    assertEquals(started.code, 0, `start: ${started.stdout}${started.stderr}`);
                    for (const m of started.stdout.matchAll(/started (\w+) \(pid (\d+)\)/g)) pids.add(Number(m[2]));
                    assertEquals(pids.size, 2, `two background processes (${started.stdout})`);

                    const status = await rhost('status');
                    for (const pid of pids) assertTrue(status.stdout.includes(`running   pid ${pid}`), `status shows pid ${pid} (${status.stdout})`);

                    const update = await rhost('update');
                    assertEquals(update.code, 1, 'a bare update of two hosts without a terminal is refused');
                    assertTrue(update.stderr.includes('this app has several hosts') && update.stderr.includes('--yes'),
                        `it names --yes (${update.stderr})`);

                    const stopOne = await rhost('stop', 'gaming');
                    assertTrue(stopOne.stdout.includes('stopped gaming'), `stop gaming (${stopOne.stdout})`);
                    assertTrue((await rhost('status', 'gaming')).stdout.includes('running   stopped'), 'gaming is stopped');
                    assertTrue((await rhost('status', 'default')).stdout.includes('running   pid'), 'default still runs');

                    const stopAll = await rhost('stop');
                    assertTrue(stopAll.stdout.includes('stopped default') && stopAll.stdout.includes('gaming is not running'),
                        `a bare stop stops the rest (${stopAll.stdout})`);

                    const again = await rhost('start', 'default');
                    for (const m of again.stdout.matchAll(/started (\w+) \(pid (\d+)\)/g)) pids.add(Number(m[2]));
                    const removed = await rhost('remove', 'default');
                    assertEquals(removed.code, 0, `remove: ${removed.stdout}${removed.stderr}`);
                    assertTrue(removed.stdout.includes('stopped default') && removed.stdout.includes('removed default'),
                        `remove stops the running host, then deletes it (${removed.stdout})`);
                    assertEquals((await fs.readdir(join(appDir, 'hosts'))).join(','), 'gaming', 'default is gone');
                    const log = await fs.readFile(join(appDir, 'hosts', 'gaming', 'run', 'rhost.log'), 'utf8');
                    assertTrue(log.includes('serving gaming') && log.includes('stopped gaming'), `the log (${log})`);

                    const wrong = await runBin('rhost', ['--app', appDir, 'start', 'gaming'], { [PASS_ENV]: 'nope' });
                    assertEquals(wrong.code, 1, 'a wrong passphrase fails the start');
                    assertTrue(wrong.stdout.includes("gaming: host 'gaming': Wrong passphrase for key 'me'"),
                        `before anything is spawned, naming the host and key (${wrong.stdout})`);
                } finally {
                    for (const pid of pids) killQuietly(pid);
                }
            });
        },
    },
    {
        name: '[RDB_TOOLS56] two processes on one host store: rows both ways, and a deploy from the other process',
        invoke: async () => {
            const { v100, v110 } = await editorReleases();
            await withDir('rh-two-', async (dir) => {
                const appDir = await newApp(dir, [v100]);
                const creator = await openApp(appDir, appOptions(dir));
                await creator.create('default', { ...SETUP, name: 'app' });
                await creator.close();

                await fs.copyFile(v110, join(appDir, 'catalogs', v110.split('/').pop()!));
                await editConfig(appDir, (config) => { config['autoDeploy'] = 'none'; });
                const app = await openApp(appDir, appOptions(dir));
                const folder = join(appDir, 'hosts', 'default');
                const store = join(folder, 'rdb', 'replica.rdb');
                const projection = join(folder, 'db', 'data.sqlite');
                const rdb = (script: string) => fs.writeFile(join(dir, 'script.sql'), script)
                    .then(() => runBin('rdb', [store, '-f', join(dir, 'script.sql')], { RDB_KEYSTORE: join(dir, 'user-keys.json') }));
                try {
                    await app.start('default');
                    const host = await app.host('default');
                    const running = await host.status();
                    assertEquals(formatReleases(running.deployed), '1.0.0', 'autoDeploy none keeps 1.0.0');
                    assertTrue(running.hostBehind, 'while 1.1.0 ships');

                    const wrote = await rdb([
                        '\\key unlock me pw',
                        '\\author me',
                        '\\ref-auto-update auto',
                        "INSERT INTO user.caps (label, grantee) VALUES ('writer', $me);",
                        "INSERT INTO doc.pages (title, deleted) VALUES ('from rdb', false);",
                    ].join('\n') + '\n');
                    assertEquals(wrote.code, 0, `the other process writes, with the user's keystore (${wrote.stdout}${wrote.stderr})`);
                    await waitFor(() => titles(projection).includes('from rdb'), "the other process's row in db/data.sqlite");

                    const appDb = new Database(projection);
                    appDb.pragma('busy_timeout = 5000');
                    try {
                        appDb.prepare('INSERT INTO doc_pages (title, deleted) VALUES (?, ?)').run('from app', 0);
                    } finally {
                        appDb.close();
                    }
                    await waitFor(async () => {
                        const read = await runBin('rdb', [store, '-c', 'SELECT title FROM doc.pages;'], { RDB_KEYSTORE: join(dir, 'user-keys.json') });
                        return read.code === 0 && read.stdout.includes('from app');
                    }, "the app's row in the other process's SELECT");

                    const deploy = await rdb([
                        '\\key unlock me pw',
                        '\\author me',
                        "UPDATE CATALOG editor TO '1.1.0' ON app;",
                    ].join('\n') + '\n');
                    assertEquals(deploy.code, 0, `the other process deploys 1.1.0 (${deploy.stdout}${deploy.stderr})`);
                    await waitFor(async () => formatReleases((await host.status()).deployed) === '1.1.0',
                        'the running host sees 1.1.0 deployed');
                    await waitFor(() => readApp(projection, (db) => (db.prepare('PRAGMA table_info(doc_pages)').all() as { name: string }[])
                        .some((c) => c.name === 'tag')), 'doc_pages gains tag');
                } finally {
                    await app.close();
                }
            });
        },
    },
    {
        name: '[RDB_TOOLS57] rhost whoami, events and deploy, and status from the running process over run/rhost.sock',
        invoke: async () => {
            const { v100, v110 } = await editorReleases();
            await withDir('rh-cmd-', async (dir) => {
                const appDir = await newApp(dir, [v100]);
                await editConfig(appDir, (config) => { config['autoDeploy'] = 'none'; });
                const env = { [PASS_ENV]: 'pw' };
                const rhost = (...args: string[]) => runBin('rhost', ['--app', appDir, ...args], env);
                const pids = new Set<number>();
                try {
                    assertEquals((await rhost('create', ...HOST_FLAGS)).code, 0, 'create');
                    await fs.copyFile(v110, join(appDir, 'catalogs', v110.split('/').pop()!));

                    await fs.rename(join(dir, 'user-keys.json'), join(dir, 'away.json'));
                    const whoami = await rhost('whoami');
                    await fs.rename(join(dir, 'away.json'), join(dir, 'user-keys.json'));
                    const keyId = (await KeyStore.open(join(dir, 'user-keys.json'), hashSuite)).resolveRecord('me').keyId;
                    assertTrue(whoami.stdout.includes('host        default') && whoami.stdout.includes('key         me')
                        && whoami.stdout.includes(`key id      ${keyId}`),
                        `whoami prints the key from host.json, with no keystore (${whoami.stdout}${whoami.stderr})`);
                    const none = await rhost('events');
                    assertTrue(none.stdout.includes('(no events)'), `no events before the first start (${none.stdout}${none.stderr})`);

                    const started = await rhost('start', 'default');
                    for (const m of started.stdout.matchAll(/started (\w+) \(pid (\d+)\)/g)) pids.add(Number(m[2]));
                    assertEquals(pids.size, 1, `started (${started.stdout}${started.stderr})`);

                    const projection = join(appDir, 'hosts', 'default', 'db', 'data.sqlite');
                    await waitFor(() => tableHas(projection, 'doc_pages'), 'the projection');
                    const appDb = new Database(projection);
                    appDb.pragma('busy_timeout = 5000');
                    try {
                        appDb.prepare('INSERT INTO doc_pages (title, deleted) VALUES (?, ?)').run('not a writer', 0);
                    } finally {
                        appDb.close();
                    }
                    await waitFor(async () => {
                        const events = await rhost('events');
                        return events.stdout.includes('ingestion/failure') && events.stdout.includes('doc_pages');
                    }, 'the rejected write in rhost events');

                    const before = await rhost('status');
                    assertTrue(before.stdout.includes('deployed  1.0.0') && before.stdout.includes('flags     host behind')
                        && before.stdout.includes('behind    autoDeploy is none'),
                        `autoDeploy none holds 1.1.0, and status says why (${before.stdout})`);
                    assertTrue(before.stdout.includes('peers     0'), `the socket status has peers (${before.stdout})`);

                    const deployed = await rhost('deploy', '--version', '1.1.0');
                    assertEquals(deployed.code, 0, `deploy (${deployed.stdout}${deployed.stderr})`);
                    assertTrue(deployed.stdout.includes('deployed editor 1.1.0'), `deploy output (${deployed.stdout})`);
                    await waitFor(async () => (await rhost('status')).stdout.includes('deployed  1.1.0'),
                        'the running host shows the deploy');
                    const again = await rhost('deploy');
                    assertEquals(again.code, 1, 'deploying the shipped release again fails');
                    assertTrue(again.stderr.includes('1.1.0 is already deployed'), `and says why (${again.stderr})`);

                    assertTrue((await rhost('stop')).stdout.includes('stopped default'), 'stop');
                } finally {
                    for (const pid of pids) killQuietly(pid);
                }
            });
        },
    },
    {
        name: '[RDB_TOOLS63] from a host folder, rhost uses that app and that host',
        invoke: async () => {
            const { v100 } = await editorReleases();
            await withDir('rh-cwd-', async (dir) => {
                const appDir = await newApp(dir, [v100]);
                const env = { [PASS_ENV]: 'pw' };
                const rhost = (...args: string[]) => runBin('rhost', ['--app', appDir, ...args], env);
                assertEquals((await rhost('create', ...HOST_FLAGS)).code, 0, 'create default');
                assertEquals((await rhost('create', 'gaming', ...HOST_FLAGS)).code, 0, 'create gaming');

                const both = await rhost('status');
                assertTrue(both.stdout.includes('host      default') && both.stdout.includes('host      gaming'),
                    `from the app, status lists both (${both.stdout})`);

                const folder = join(appDir, 'hosts', 'default');
                const here = (...args: string[]) => runBin('rhost', args, env, folder);
                const status = await here('status');
                assertEquals(status.code, 0, `status from the host folder (${status.stderr})`);
                assertTrue(status.stdout.includes('host      default') && !status.stdout.includes('gaming'),
                    `it reports only that host (${status.stdout})`);
                const other = await here('status', 'gaming');
                assertTrue(other.stdout.includes('host      gaming') && !other.stdout.includes('host      default'),
                    `a named host still wins (${other.stdout})`);
                const whoami = await here('whoami');
                assertEquals(whoami.code, 0, `whoami (${whoami.stderr})`);
                assertTrue(whoami.stdout.includes('key         me'), `whoami prints the key (${whoami.stdout})`);

                const removed = await here('remove');
                assertEquals(removed.code, 1, 'remove with no name fails');
                assertTrue(removed.stderr.includes('remove needs a host name'), `and says so (${removed.stderr})`);
                assertTrue(existsSync(join(folder, 'host.json')), 'the host is still there');
            });
        },
    },
    {
        name: '[RDB_TOOLS65] rhost init writes app.json only; create and join take the key, its passphrase source and the network settings',
        invoke: async () => {
            const { v100 } = await editorReleases();
            await withDir('rh-setup-', async (dir) => {
                const appDir = join(dir, 'app');
                const keystore = await userKeystore(dir);
                const env = { [PASS_ENV]: 'pw', RDB_KEYSTORE: keystore };
                const rhost = (...args: string[]) => runBin('rhost', ['--app', appDir, ...args], env);

                const noParam = await rhost('init', '--release', v100);
                assertEquals(noParam.code, 1, 'init without a terminal needs each param');
                assertTrue(noParam.stderr.includes('rhost init needs --param admin=<value>'), `and names it (${noParam.stderr})`);
                const init = await rhost('init', '--release', v100, '--param', 'admin=$me');
                assertEquals(init.code, 0, `init (${init.stdout}${init.stderr})`);
                assertTrue(init.stdout.includes('params    admin=$me') && init.stdout.includes('add a host with: rhost create'),
                    `init reports the params and the next step (${init.stdout})`);
                const config = JSON.parse(await fs.readFile(join(appDir, 'app.json'), 'utf8')) as Record<string, unknown>;
                assertEquals(Object.keys(config).sort().join(','), 'autoDeploy,params,projection,releases', `app.json (${JSON.stringify(config)})`);
                assertTrue(!existsSync(join(appDir, 'keys.json')), 'init writes no keys.json');
                const oldFlag = await rhost('init', '--release', v100, '--key', 'me');
                assertTrue(oldFlag.code === 1 && oldFlag.stderr.includes('unknown flag --key'), `init takes no key (${oldFlag.stderr})`);

                const noKey = await rhost('create');
                assertTrue(noKey.code === 1 && noKey.stderr.includes('rhost create needs --key <label>'), `no terminal, no key (${noKey.stderr})`);
                const noScope = await rhost('create', '--key', 'me');
                assertTrue(noScope.code === 1 && noScope.stderr.includes('rhost create needs --scope internet|localhost'),
                    `no terminal, no scope (${noScope.stderr})`);
                const noPass = await rhost('create', '--key', 'me', '--scope', 'localhost');
                assertTrue(noPass.code === 1 && noPass.stderr.includes("rhost create needs --passphrase-env <VAR> to unlock 'me'"),
                    `no terminal, no passphrase source (${noPass.stderr})`);
                const unknown = await rhost('create', '--key', 'nobody', '--scope', 'localhost');
                assertTrue(unknown.code === 1 && unknown.stderr.includes(`no key 'nobody' in ${keystore}`),
                    `an unknown key names the keystore (${unknown.stderr})`);
                const badScope = await rhost('create', '--key', 'me', '--scope', 'lan');
                assertTrue(badScope.code === 1 && badScope.stderr.includes("--scope is internet or localhost, got 'lan'"), `a bad scope (${badScope.stderr})`);
                assertTrue(!existsSync(join(appDir, 'hosts')), 'nothing was created');

                const created = await rhost('create', '--key', 'me', '--scope', 'internet', '--passphrase-env', PASS_ENV,
                    '--tracker', 'wss://tracker.example', '--tracker-key', 'TK', '--listen', 'ws://127.0.0.1:7499');
                assertEquals(created.code, 0, `create (${created.stdout}${created.stderr})`);
                assertTrue(created.stdout.includes(`key       me (`) && created.stdout.includes(`in ${keystore})`),
                    `create names the key and the keystore (${created.stdout})`);
                assertTrue(created.stdout.includes('sync      internet, tracker wss://tracker.example, tracker key TK, listening on ws://127.0.0.1:7499'),
                    `and the network settings (${created.stdout})`);
                const record = await readHostJson(appDir, 'default');
                const key = (await KeyStore.open(keystore, hashSuite)).resolvePublic('me');
                assertEquals(JSON.stringify(record['key']), JSON.stringify({
                    label: 'me', keyId: key.keyId, publicKey: serializePublicKeyToBase64(key.publicKey), passphrase: `env:${PASS_ENV}`,
                }), 'host.json records the key, its public key and its passphrase source');
                assertEquals(JSON.stringify(record['sync']),
                    '{"scope":"internet","tracker":"wss://tracker.example","trackerKey":"TK","listen":"ws://127.0.0.1:7499"}',
                    'and the network settings');

                const clash = await rhost('create', 'second', '--key', 'me', '--scope', 'localhost', '--passphrase-env', PASS_ENV,
                    '--listen', 'ws://127.0.0.1:7499');
                assertTrue(clash.code === 1 && clash.stderr.includes("port 7499 (sync.listen) is already used by host 'default'"),
                    `a listen port is one host's (${clash.stderr})`);
                const taken = await rhost('create', '--key', 'me', '--scope', 'localhost', '--passphrase-env', PASS_ENV);
                assertTrue(taken.code === 1 && taken.stderr.includes("host 'default' already exists"), `a taken name (${taken.stderr})`);

                const noId = await rhost('join');
                assertTrue(noId.code === 1 && noId.stderr.includes('join needs a database id'), `join needs an id (${noId.stderr})`);
                const joinScope = await rhost('join', '#XYZ', 'joined', '--key', 'me');
                assertTrue(joinScope.code === 1 && joinScope.stderr.includes('rhost join needs --scope internet|localhost'),
                    `join asks for the same settings (${joinScope.stderr})`);
                const joinKey = await rhost('join', '#XYZ', 'joined', '--scope', 'localhost');
                assertTrue(joinKey.code === 1 && joinKey.stderr.includes('rhost join needs --key <label>'), `and the key (${joinKey.stderr})`);
            });
        },
    },
    {
        name: '[RDB_TOOLS66] a release whose new param app.json lacks: update and status hold with a hint; deploy --param supplies it',
        invoke: async () => {
            const { v100, v120 } = await editorReleases();
            await withDir('rh-param-', async (dir) => {
                const appDir = await newApp(dir, [v100]);
                const env = { [PASS_ENV]: 'pw' };
                const rhost = (...args: string[]) => runBin('rhost', ['--app', appDir, ...args], env);
                assertEquals((await rhost('create', ...HOST_FLAGS)).code, 0, 'create on 1.0.0');
                await fs.copyFile(v120, join(appDir, 'catalogs', v120.split('/').pop()!));

                const reason = "1.2.0 needs a value for :moderator (identity), which params don't set";
                const hint = "set it in app.json's params, or run: rhost deploy default --param moderator=<value>";
                const update = await rhost('update');
                assertEquals(update.code, 0, `the hold is not a failure (${update.stdout}${update.stderr})`);
                assertTrue(update.stdout.includes(`not deployed: ${reason}`) && update.stdout.includes(`  ${hint}`),
                    `update says why, and what to do (${update.stdout})`);
                const status = await rhost('status');
                assertTrue(status.stdout.includes('flags     host behind') && status.stdout.includes(`behind    ${reason}`)
                    && status.stdout.includes(`          ${hint}`), `so does status (${status.stdout})`);

                const bare = await rhost('deploy');
                assertEquals(bare.code, 1, 'deploy without the param and without a terminal fails');
                assertTrue(bare.stderr.includes('deploying 1.2.0 needs :moderator (identity); pass --param moderator=<value> ($me, $<key label>, or a JSON value)'),
                    `naming the flag (${bare.stderr})`);
                const notIdentity = await rhost('deploy', '--param', 'moderator=me');
                assertTrue(notIdentity.code === 1 && notIdentity.stderr.includes("param 'moderator' is an identity: use $me or $<key label>, got 'me'"),
                    `an identity takes $ (${notIdentity.stderr})`);
                const undeclared = await rhost('deploy', '--param', 'moderator=$me', '--param', 'color=1');
                assertTrue(undeclared.code === 1 && undeclared.stderr.includes('1.2.0 declares no param :color'),
                    `a param the release doesn't declare (${undeclared.stderr})`);
                const already = await rhost('deploy', '--param', 'moderator=$me', '--param', 'admin=$me');
                assertTrue(already.code === 1 && already.stderr.includes('this database already has :admin'),
                    `a param the database has (${already.stderr})`);

                const deployed = await rhost('deploy', '--param', 'moderator=$me');
                assertEquals(deployed.code, 0, `deploy --param (${deployed.stdout}${deployed.stderr})`);
                assertTrue(deployed.stdout.includes('deployed editor 1.2.0'), `deploy output (${deployed.stdout})`);
                const after = await rhost('status');
                assertTrue(after.stdout.includes('deployed  1.2.0') && !after.stdout.includes('behind'), `nothing waits (${after.stdout})`);
            });
        },
    },
];

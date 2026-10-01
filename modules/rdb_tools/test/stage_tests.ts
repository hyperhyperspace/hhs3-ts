import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Database from "better-sqlite3";

import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { KeyStore } from "@hyper-hyper-space/hhs3_rhost_node";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { runBin, type Run } from "./run_bin.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

const SOURCE = `CREATE SCHEMA hhs:user CREATORS ($dev) AS (
  TABLE identities (
    keyId string PUB READONLY,
    publicKey string PUB READONLY,
    name string NULL PUB
  ) IDENTITY PROVIDER,
  TABLE caps (
    label string PUB READONLY,
    grantee identity PUB READONLY
  ) CONCURRENT DELETES ALLOW all IF true
);

CREATE SCHEMA hhs:doc CREATORS ($dev) AS (
  TABLE pages (
    title string,
    deleted boolean
  ) ALLOW all IF true,

  -- blocks go away
  TABLE blocks (
    pageId string,
    content string
  ) ALLOW all IF true
);

CREATE CATALOG editor CREATORS ($dev) PARAMS (:admin identity) AS (
  TABLEGROUP user USING SCHEMA hhs:user
    USING IDENTITIES identities
    WITH ROWS (
      identities (keyId = :admin, publicKey = publicKey(:admin), name = 'Admin'),
      caps (label = 'manager', grantee = :admin)
    ),
  TABLEGROUP doc USING SCHEMA hhs:doc
    BIND user => user
    USING IDENTITIES user.identities
);
`;

const SAMPLES_100 = `INSERT INTO user.identities (keyId, publicKey, name) VALUES ($alice, publicKey($alice), 'Alice') BY $admin;
INSERT INTO user.caps (label, grantee) VALUES ('writer', $alice) BY $admin;
INSERT INTO doc.pages (title, deleted) VALUES ('Welcome', false) BY $alice;
INSERT INTO doc.blocks (pageId, content) VALUES ('p', 'hello') BY $alice;
`;

const V110 = SOURCE
    .replace('deleted boolean', 'deleted boolean,\n    summary string NULL')
    .replace('PARAMS (:admin identity)', 'PARAMS (:admin identity, :moderator identity)');

const SAMPLES_110 = `INSERT INTO doc.pages (title, deleted, summary) VALUES ('Notes', false, 'hi') BY $alice;\n`;

const V101 = SOURCE.replace('deleted boolean', 'deleted boolean,\n    tag string NULL');

const SAMPLES_101 = `INSERT INTO doc.pages (title, deleted, tag) VALUES ('Patch', false, 'p') BY $alice;\n`;

function titles(path: string): string[] {
    const db = new Database(path, { readonly: true, fileMustExist: true });
    try {
        return (db.prepare('SELECT title FROM doc_pages').all() as { title: string }[]).map((r) => r.title).sort();
    } finally {
        db.close();
    }
}

function columns(path: string, table: string): string[] {
    const db = new Database(path, { readonly: true, fileMustExist: true });
    try {
        return (db.prepare(`PRAGMA table_info(${table})`).all() as { name: string }[]).map((r) => r.name);
    } finally {
        db.close();
    }
}

function hasTable(path: string, table: string): boolean {
    const db = new Database(path, { readonly: true, fileMustExist: true });
    try {
        return db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(table) !== undefined;
    } finally {
        db.close();
    }
}

const exists = (path: string) => fs.access(path).then(() => true, () => false);

export const stageTests = [
    {
        name: '[RDB_TOOLS59] rpack stage replays a released folder with no key, stages a version through a merge, and refuses a bad sample row',
        invoke: async () => {
            const dir = await fs.mkdtemp(join(tmpdir(), 'rpack-stage-'));
            try {
                const keystore = join(dir, 'user-keys.json');
                const keys = await KeyStore.open(keystore, hashSuite);
                await keys.create('dev', 'pw');
                const repo = join(dir, 'editor');
                await fs.mkdir(repo);
                const folder = (version: string, at = repo) => join(at, 'work', version);
                const rpack = (args: string[], input?: string, at = repo) => runBin('rpack', ['-C', at, '--keystore', keystore, ...args], input);
                const read = (path: string) => fs.readFile(join(repo, path), 'utf8');
                const ok = (run: Run, what: string) => assertEquals(run.code, 0, `${what} (${run.stdout}${run.stderr})`);
                const release = async (version: string, source: string, samples: string) => {
                    ok(await rpack(['new', version]), `new ${version}`);
                    await fs.writeFile(join(folder(version), 'target-catalog.sql'), source);
                    await fs.writeFile(join(folder(version), 'test-data.sql'), samples);
                    ok(await rpack(['release', '--passphrase-stdin'], 'pw\n', folder(version)), `release ${version}`);
                };

                ok(await rpack(['init', 'editor', '--key', 'dev']), 'init');
                await release('1.0.0', SOURCE, SAMPLES_100);
                await release('1.1.0', V110, SAMPLES_110);
                await release('1.0.1', V101, SAMPLES_101);

                const staged = await rpack(['stage'], undefined, folder('1.1.0'));
                ok(staged, 'stage 1.1.0 with no passphrase');
                assertTrue(staged.stdout.startsWith('staged editor-1.1.0-') && staged.stdout.includes('1.0.0, 1.1.0'),
                    `replays both releases (${staged.stdout})`);
                const app = join(folder('1.1.0'), 'stage');
                const data = join(app, 'hosts', 'default', 'db', 'data.sqlite');
                const status = await runBin('rhost', ['--app', app, 'status']);
                ok(status, 'status');
                assertTrue(status.stdout.includes('host      default') && status.stdout.includes('deployed  1.1.0'), `deployed 1.1.0 (${status.stdout})`);
                assertEquals(titles(data).join(','), 'Notes,Welcome', "the rows from each release's folder");
                assertTrue(columns(data, 'doc_pages').includes('summary'), 'in 1.1.0\'s shape');
                assertTrue(hasTable(data, 'doc_blocks'), 'blocks is still there');
                const standIns = await KeyStore.open(join(app, 'keys.json'), hashSuite);
                assertEquals(standIns.list().map((k) => k.label).sort().join(','), 'admin,alice', 'stand-in keys');
                const appConfig = JSON.parse(await fs.readFile(join(app, 'app.json'), 'utf8')) as Record<string, any>;
                assertEquals(appConfig['keystore'], 'keys.json', "the app signs with the stand-ins' keystore");
                assertEquals(appConfig['params'].moderator, '$me', 'the new param is set');
                assertTrue(appConfig['sync'] === undefined && appConfig['identity'] === undefined, `app.json has no host settings (${JSON.stringify(appConfig)})`);
                const record = JSON.parse(await fs.readFile(join(app, 'hosts', 'default', 'host.json'), 'utf8')) as Record<string, any>;
                assertTrue(record['key'].label === 'admin' && record['key'].keyId === standIns.resolveRecord('admin').keyId
                    && record['key'].passphrase === undefined, `the host signs as admin, with no passphrase (${JSON.stringify(record['key'])})`);
                assertEquals(JSON.stringify(record['sync']), '{"scope":"localhost"}', 'and syncs on localhost by default');

                await fs.writeFile(join(app, 'marker'), 'keep');
                ok(await rpack(['stage'], undefined, folder('1.1.0')), 'stage again');
                assertEquals(await exists(join(app, 'marker')), false, 'stage always rebuilds');

                const before = (await fs.readdir(join(repo, 'releases'))).sort();
                ok(await rpack(['new', '2.0.0']), 'new 2.0.0');
                const merged = await read('work/2.0.0/target-catalog.sql');
                assertTrue(merged.includes('summary string NULL') && merged.includes('tag string NULL'), 'the merge keeps both lines\' columns');
                await fs.writeFile(join(folder('2.0.0'), 'target-catalog.sql'), merged.replace(/\n\n  -- blocks go away\n  TABLE blocks \([\s\S]*?IF true/, ''));

                const stagingJson = join(folder('2.0.0'), 'staging.json');
                await fs.writeFile(stagingJson, '{ "sync": { "scope": "localhost" }, "identity": { "key": "admin" } }\n');
                const unknownField = await rpack(['stage', '--passphrase-stdin'], 'pw\n', folder('2.0.0'));
                assertTrue(unknownField.code === 1 && unknownField.stderr.includes("staging.json: 'identity' is not a known field"),
                    `staging.json is strict (${unknownField.stderr})`);
                await fs.writeFile(stagingJson, '{ "sync": { "scope": "localhost", "allow": ["user.identities.keyId"] } }\n');
                const nestedAllow = await rpack(['stage', '--passphrase-stdin'], 'pw\n', folder('2.0.0'));
                assertTrue(nestedAllow.code === 1 && nestedAllow.stderr.includes('sync.allow is not a known field'),
                    `allow is not a sync setting (${nestedAllow.stderr})`);
                await fs.writeFile(stagingJson, '{ "params": { "nope": 1 } }\n');
                const undeclared = await rpack(['stage', '--passphrase-stdin'], 'pw\n', folder('2.0.0'));
                assertTrue(undeclared.code === 1 && undeclared.stderr.includes("staging.json sets :nope, which 2.0.0 doesn't declare"),
                    `a param the release doesn't declare (${undeclared.stderr})`);
                await fs.writeFile(stagingJson, JSON.stringify({
                    sync: { scope: 'localhost', listen: 'ws://127.0.0.1:7600' },
                    allow: ['user.identities.keyId'],
                }) + '\n');

                const next = await rpack(['stage', '--passphrase-stdin'], 'pw\n', folder('2.0.0'));
                ok(next, 'stage 2.0.0');
                assertTrue(next.stdout.startsWith('staged 2.0.0 as it would be released in work/2.0.0/stage/'), `names the folder (${next.stdout})`);
                assertTrue(next.stdout.includes('blocks:') && next.stdout.includes('gone'), `the dropped table's rows are gone (${next.stdout})`);
                assertEquals((await fs.readdir(join(repo, 'releases'))).sort().join(','), before.join(','), 'nothing was released');
                const nextApp = join(folder('2.0.0'), 'stage');
                const shipped = await fs.readdir(join(nextApp, 'catalogs'));
                assertTrue(shipped.some((f) => f.startsWith('editor-2.0.0-')), `the planned file is in the staging app (${shipped.join(', ')})`);
                const nextConfig = JSON.parse(await fs.readFile(join(nextApp, 'app.json'), 'utf8')) as Record<string, unknown>;
                assertEquals(JSON.stringify(nextConfig['allow']), '["user.identities.keyId"]', "staging.json's allow goes to app.json");
                const nextRecord = JSON.parse(await fs.readFile(join(nextApp, 'hosts', 'default', 'host.json'), 'utf8')) as Record<string, unknown>;
                assertEquals(JSON.stringify(nextRecord['sync']), '{"scope":"localhost","listen":"ws://127.0.0.1:7600"}', 'and its sync to host.json');
                const nextData = join(nextApp, 'hosts', 'default', 'db', 'data.sqlite');
                assertEquals(hasTable(nextData, 'doc_blocks'), false, 'blocks is dropped');
                assertEquals(titles(nextData).join(','), 'Notes,Patch,Welcome', 'the merge kept every line\'s rows');

                const fresh = join(dir, 'fresh');
                await fs.mkdir(fresh);
                const freshPack = (args: string[], input?: string, at = fresh) => runBin('rpack', ['-C', at, '--keystore', keystore, ...args], input);
                ok(await freshPack(['init', 'editor', '--key', 'dev']), 'fresh init');
                ok(await freshPack(['new', '1.0.0']), 'fresh new');
                await fs.writeFile(join(folder('1.0.0', fresh), 'target-catalog.sql'), SOURCE);
                const first = await freshPack(['stage'], undefined, folder('1.0.0', fresh));
                ok(first, 'stage before any release');
                assertTrue(first.stdout.startsWith('staged 1.0.0 as it would be released'), `no passphrase was needed (${first.stdout}${first.stderr})`);

                await fs.writeFile(join(folder('1.0.0', fresh), 'test-data.sql'), '-- not a row\nCREATE SCHEMA nope CREATORS ($dev) AS (TABLE t (v string));\n');
                ok(await freshPack(['release', '--passphrase-stdin'], 'pw\n', folder('1.0.0', fresh)), 'release with a bad sample');
                const refused = await freshPack(['stage'], undefined, folder('1.0.0', fresh));
                assertEquals(refused.code, 1, `a bad sample fails (${refused.stdout}${refused.stderr})`);
                assertTrue(refused.stderr.includes('test-data.sql:2:'), `with its file and line (${refused.stderr})`);
                const left = await fs.readdir(folder('1.0.0', fresh));
                assertTrue(left.includes('stage') && !left.some((n) => n.startsWith('.stage.')),
                    `the failed stage kept the previous one and left nothing of its own (${left.join(', ')})`);
            } finally {
                await fs.rm(dir, { recursive: true, force: true });
            }
        },
    },
];

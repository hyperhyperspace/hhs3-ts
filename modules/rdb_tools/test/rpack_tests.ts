import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { initApp, KeyStore } from "@hyper-hyper-space/hhs3_rhost_node";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { runBin, type Run } from "./run_bin.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

const SOURCE = `-- The editor app.

CREATE SCHEMA hhs:user CREATORS ($dev) AS (
  TABLE identities (
    keyId string PUB READONLY,
    publicKey string PUB READONLY,
    name string NULL PUB
  ) IDENTITY PROVIDER,

  TABLE caps (
    label string PUB READONLY,
    grantee identity PUB READONLY
  ) CONCURRENT DELETES
    ALLOW insert IF EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
);

CREATE SCHEMA hhs:doc CREATORS ($dev) AS (
  -- the pages of a document
  TABLE pages (
    title string,
    deleted boolean
  ) ALLOW all IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author
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

const ok = (run: Run, what: string) => assertEquals(run.code, 0, `${what} (${run.stdout}${run.stderr})`);

export const rpackTests = [
    {
        name: '[RDB_TOOLS58] the rpack bin: init, new, build, release, a lower release and its merge, log, a re-release with --yes; rhost deploys the merge',
        invoke: async () => {
            const dir = await fs.mkdtemp(join(tmpdir(), 'rpack-cli-'));
            try {
                const keystore = join(dir, 'user-keys.json');
                const keys = await KeyStore.open(keystore, hashSuite);
                await keys.create('dev', 'pw');
                await keys.create('me', 'pw');
                const repo = join(dir, 'editor');
                await fs.mkdir(repo);
                const folder = (version: string) => join(repo, 'work', version);
                const rpack = (args: string[], input?: string, at = repo) => runBin('rpack', ['-C', at, '--keystore', keystore, ...args], input);
                const read = (path: string) => fs.readFile(join(repo, path), 'utf8');
                const released = async () => JSON.parse(await read('rpack.json')).released as { [folder: string]: string };

                ok(await rpack(['init', 'editor', '--key', 'dev']), 'init');
                assertEquals(JSON.stringify(await released()), '{}', 'init writes rpack.json with nothing released');
                const unknown = await rpack(['nosuch']);
                assertEquals(unknown.code, 1, 'an unknown command fails');
                assertTrue(unknown.stderr.startsWith("Unknown command 'nosuch'\n"), `and names the error (${unknown.stderr})`);
                assertTrue(unknown.stderr.includes('Usage:'), 'then the command list');
                const outside = await rpack(['build']);
                assertEquals(outside.code, 1, 'build outside a version folder fails');
                assertTrue(outside.stderr.includes('runs inside a version folder'), `and says where it runs (${outside.stderr})`);
                assertTrue(!outside.stderr.includes('Usage:'), 'which is not a usage error');

                const created = await rpack(['new', '1.0.0']);
                ok(created, 'new 1.0.0');
                assertEquals(created.stdout, 'Created work/1.0.0/ for editor 1.0.0, the first release.\n', `new prints one line (${created.stdout})`);
                assertEquals(await read('work/1.0.0/version.json'), '{\n  "base": []\n}\n', 'and writes version.json');
                await fs.writeFile(join(folder('1.0.0'), 'target-catalog.sql'), SOURCE);
                const built = await rpack(['build'], undefined, folder('1.0.0'));
                ok(built, 'build');
                assertEquals(built.stdout, 'Generated build/update.sql\n', `build prints that it wrote the file (${built.stdout})`);
                assertTrue((await read('work/1.0.0/build/update.sql')).includes("CREATE CATALOG editor CREATORS ($dev) VERSION '1.0.0'"), 'update.sql has the genesis');
                const unsigned = await rpack(['release'], undefined, folder('1.0.0'));
                assertEquals(unsigned.code, 1, 'release without a terminal or --passphrase-stdin fails');
                assertTrue(unsigned.stderr.includes('--passphrase-stdin'), `and says how (${unsigned.stderr})`);
                const wrong = await rpack(['release', '--passphrase-stdin'], 'nope\n', folder('1.0.0'));
                assertEquals(wrong.code, 1, 'a wrong passphrase fails the release');
                const flagged = await rpack(['release', '--passphrase-stdin', '--note', 'initial'], 'pw\n', folder('1.0.0'));
                assertEquals(flagged.code, 1, 'release takes no --note');
                assertTrue(flagged.stderr.includes(`the "note" in its folder's version.json`), `and says where the note goes (${flagged.stderr})`);
                await fs.writeFile(join(folder('1.0.0'), 'version.json'), '{\n  "base": [],\n  "note": "initial"\n}\n');
                ok(await rpack(['release', '--passphrase-stdin'], 'pw\n', folder('1.0.0')), 'release 1.0.0');
                const r100 = (await released())['1.0.0']!;
                assertTrue(/^editor-1\.0\.0-[0-9a-f]{8}$/.test(r100), `rpack.json maps the folder to ${r100}`);
                assertEquals(JSON.parse(await read(`releases/${r100}.rpack`)).manifest.note, 'initial', "the release has version.json's note");
                assertEquals(await read('work/1.0.0/.released/target-catalog.sql'), SOURCE, '.released/ keeps the source');

                const v110 = SOURCE.replace('deleted boolean', 'deleted boolean,\n    summary string NULL');
                ok(await rpack(['new', '1.1.0']), 'new 1.1.0');
                const refused = SOURCE.replace('    title string,', '    title integer DEFAULT 0,');
                await fs.writeFile(join(folder('1.1.0'), 'target-catalog.sql'), refused);
                const refusal = await rpack(['build'], undefined, folder('1.1.0'));
                assertEquals(refusal.code, 1, 'a refused build exits 1');
                assertTrue(refusal.stdout.includes("to reset the column, add to upgrade-manual.sql:  ALTER SCHEMA hhs:doc AS (DROP COLUMN pages.title);"),
                    `and says how to reset (${refusal.stdout})`);
                await fs.writeFile(join(folder('1.1.0'), 'target-catalog.sql'), v110);
                await fs.writeFile(join(folder('1.1.0'), 'staging.json'), '{ "sync": { "scope": "localhost" } }\n');
                ok(await rpack(['release', '--passphrase-stdin'], 'pw\n', folder('1.1.0')), 'release 1.1.0');
                assertEquals(await read('work/1.1.0/target-catalog.sql'), v110, 'release never writes target-catalog.sql');
                assertEquals(await read('work/1.1.0/staging.json'), '{ "sync": { "scope": "localhost" } }\n', 'or staging.json');

                ok(await rpack(['new', '1.0.1']), 'new 1.0.1');
                assertEquals(await read('work/1.0.1/target-catalog.sql'), SOURCE, "1.0.1 starts from 1.0.0's source");
                assertEquals(await read('work/1.0.1/staging.json'), '{}\n', "and 1.0.0's staging config");
                await fs.writeFile(join(folder('1.0.1'), 'target-catalog.sql'), SOURCE.replace('deleted boolean', 'deleted boolean,\n    tag string NULL'));
                ok(await rpack(['release', '--passphrase-stdin'], 'pw\n', folder('1.0.1')), 'release 1.0.1');

                ok(await rpack(['new', '1.2.0', '--base', '1.1.0', '+', '1.0.1']), 'new 1.2.0 on both');
                const merged = await read('work/1.2.0/target-catalog.sql');
                assertTrue(merged.includes('-- the pages of a document') && merged.includes('summary string NULL,\n    tag string NULL'),
                    `the merge keeps 1.1.0's text and gains 1.0.1's column:\n${merged}`);
                const mergePlan = await rpack(['build'], undefined, folder('1.2.0'));
                ok(mergePlan, 'build 1.2.0');
                assertTrue(mergePlan.stdout.startsWith('Generated build/update.sql\n'), `build names the file (${mergePlan.stdout})`);
                const update = await read('work/1.2.0/build/update.sql');
                assertTrue(update.includes('UPDATE SCHEMA hhs:doc TO LATEST ON doc'), `update.sql sets the doc group (${update})`);
                ok(await rpack(['release', '--passphrase-stdin'], 'pw\n', folder('1.2.0')), 'release 1.2.0');

                const log = await rpack(['log']);
                ok(log, 'log');
                assertTrue(/1\.2\.0 +[0-9a-f]{8}  after 1\.(0\.1|1\.0) \+ 1\.(0\.1|1\.0)/.test(log.stdout), `log shows the merge (${log.stdout})`);
                for (const name of await fs.readdir(join(repo, 'releases'))) {
                    ok(await runBin('rpack', ['verify', join(repo, 'releases', name)]), `verify ${name}`);
                }

                const files = await fs.readdir(join(repo, 'releases'));
                const file = (version: string) => join(repo, 'releases', files.find((f) => f.startsWith(`editor-${version}-`))!);
                const app = join(dir, 'app');
                await initApp(app, { releases: [file('1.0.1')], params: { admin: '$me' } });
                const config = JSON.parse(await fs.readFile(join(app, 'app.json'), 'utf8'));
                await fs.writeFile(join(app, 'app.json'), JSON.stringify({ ...config, keystore: '../user-keys.json' }, undefined, 2) + '\n');
                const rhost = (...args: string[]) => runBin('rhost', ['--app', app, ...args]);
                process.env['RPACK_TEST_PASS'] = 'pw';
                try {
                    ok(await rhost('create', '--key', 'me', '--scope', 'localhost', '--passphrase-env', 'RPACK_TEST_PASS'), 'rhost create at 1.0.1');
                    assertTrue((await rhost('status')).stdout.includes('deployed  1.0.1'), 'the host runs 1.0.1');
                    for (const version of ['1.1.0', '1.2.0']) await fs.copyFile(file(version), join(app, 'catalogs', basename(file(version))));
                    const deploy = await rhost('deploy');
                    ok(deploy, 'rhost deploys the merge');
                    const status = await rhost('status');
                    assertTrue(status.stdout.includes('deployed  1.2.0'), `the host moved to 1.2.0 (${status.stdout})`);
                } finally {
                    delete process.env['RPACK_TEST_PASS'];
                }

                const before = { ...await released() };
                await fs.writeFile(join(folder('1.1.0'), 'target-catalog.sql'), v110.replace('summary string NULL', 'summary string NULL,\n    cover string NULL'));
                const unconfirmed = await rpack(['release', '--force', '--passphrase-stdin'], 'pw\n', folder('1.1.0'));
                assertEquals(unconfirmed.code, 1, 'a re-release with releases built on it, and no terminal, fails');
                assertTrue(unconfirmed.stderr.includes('--yes'), `and says how to go ahead (${unconfirmed.stderr})`);
                const redone = await rpack(['release', '--force', '--yes', '--passphrase-stdin'], 'pw\n', folder('1.1.0'));
                ok(redone, 're-release 1.1.0 with --yes');
                const after = await released();
                assertTrue(after['1.1.0'] !== before['1.1.0'] && after['1.2.0'] !== before['1.2.0'], `1.1.0 and 1.2.0 are re-released (${redone.stdout})`);
                assertEquals(after['1.0.0'] + after['1.0.1'], before['1.0.0']! + before['1.0.1']!, 'the others are kept');
                const shipped = (await fs.readdir(join(repo, 'releases'))).sort();
                assertEquals(shipped.join(','), Object.values(after).map((n) => `${n}.rpack`).sort().join(','), 'releases/ holds one file per version');
            } finally {
                await fs.rm(dir, { recursive: true, force: true });
            }
        },
    },
    {
        name: '[RDB_TOOLS62] rpack finds the repository and the version folder from the folder it runs in',
        invoke: async () => {
            // Inside the module, where the loader resolves.
            await fs.mkdir(join(process.cwd(), 'test-build'), { recursive: true });
            const dir = await fs.mkdtemp(join(process.cwd(), 'test-build', 'rpack-cwd-'));
            try {
                const keystore = join(dir, 'user-keys.json');
                await (await KeyStore.open(keystore, hashSuite)).create('dev', 'pw');
                const repo = join(dir, 'editor');
                await fs.mkdir(repo);
                const rpack = (cwd: string, args: string[], input?: string) => runBin('rpack', ['--keystore', keystore, ...args], input, cwd);
                const folder = join(repo, 'work', '1.0.0');

                ok(await rpack(repo, ['init', 'editor', '--key', 'dev']), 'init makes the current folder a repository');
                ok(await rpack(join(repo, 'releases'), ['new', '1.0.0']), 'new runs anywhere in the repository');
                await fs.writeFile(join(folder, 'target-catalog.sql'), SOURCE);
                const status = await rpack(folder, ['status']);
                ok(status, 'status in the version folder');
                assertTrue(status.stdout.includes('Version: 1.0.0'), `names the folder's version (${status.stdout})`);
                const outside = await rpack(repo, ['status']);
                assertEquals(outside.code, 1, 'status at the top of the repository fails');
                assertTrue(outside.stderr.includes('cd work/<version>'), `and says where to run it (${outside.stderr})`);
                ok(await rpack(folder, ['release', '--passphrase-stdin'], 'pw\n'), 'release in the version folder');
                const deeper = await rpack(join(folder, '.released'), ['status']);
                ok(deeper, 'status below the version folder');
                assertTrue(deeper.stdout.includes('Released as editor-1.0.0-'), `names the release (${deeper.stdout})`);

                ok(await rpack(folder, ['new', '1.1.0']), 'new from inside another folder');
                const same = await rpack(join(repo, 'work', '1.1.0'), ['set', 'base', '1.0.0']);
                ok(same, 'set base');
                assertTrue(same.stdout.includes('is already after 1.0.0'), `works on the folder it runs in (${same.stdout})`);
                const nowhere = await rpack(dir, ['log']);
                assertEquals(nowhere.code, 1, 'outside a repository fails');
                assertTrue(nowhere.stderr.includes('rpack init'), `and says how to start one (${nowhere.stderr})`);
            } finally {
                await fs.rm(dir, { recursive: true, force: true });
            }
        },
    },
];

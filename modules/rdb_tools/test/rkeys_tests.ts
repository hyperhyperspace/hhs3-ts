import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { KeyStore } from "@hyper-hyper-space/hhs3_rhost_node";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { runBin, type Run } from "./run_bin.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

const ok = (run: Run, what: string) => assertEquals(run.code, 0, `${what} (${run.stdout}${run.stderr})`);

const labels = async (path: string) => (await KeyStore.open(path, hashSuite)).list().map((key) => key.label).join(',');

async function unlocks(path: string, label: string, passphrase: string): Promise<boolean> {
    try {
        await (await KeyStore.open(path, hashSuite)).unlock(label, passphrase);
        return true;
    } catch {
        return false;
    }
}

export const rkeysTests = [
    {
        name: '[RDB_TOOLS64] the rkeys bin: list, create, export and import keystore records, without a workspace',
        invoke: async () => {
            const dir = await fs.mkdtemp(join(tmpdir(), 'rkeys-cli-'));
            try {
                const store = (name: string) => join(dir, `${name}.json`);
                const rkeys = (keystore: string, args: string[], input?: string) => runBin('rkeys', ['--keystore', keystore, ...args], input);
                const a = store('a');

                const empty = await rkeys(store('empty'), ['list']);
                ok(empty, 'list an empty store');
                assertEquals(empty.stdout, '', 'which prints nothing');
                const unknown = await rkeys(a, ['nosuch']);
                assertEquals(unknown.code, 1, 'an unknown command fails');
                assertTrue(unknown.stderr.includes('Usage:'), `with the command list (${unknown.stderr})`);

                const noPassphrase = await rkeys(a, ['create', 'alice']);
                assertEquals(noPassphrase.code, 1, 'create without a terminal or --passphrase-stdin fails');
                assertTrue(noPassphrase.stderr.includes('--passphrase-stdin'), `and names the flag (${noPassphrase.stderr})`);

                const created = await rkeys(a, ['create', 'alice', '--passphrase-stdin'], 'pw\n');
                ok(created, 'create alice');
                const alice = (await KeyStore.open(a, hashSuite)).resolveRecord('alice');
                assertEquals(created.stdout, `created alice ${alice.keyId}\n`, 'create prints the key id');
                const listed = await runBin('rkeys', ['list', '--keystore', a]);
                ok(listed, '--keystore after the command');
                assertEquals(listed.stdout, `alice  ${alice.keyId}\n`, 'list shows the label and the full key id');
                assertTrue(await unlocks(a, 'alice', 'pw'), 'the passphrase from stdin unlocks alice');
                assertTrue(!await unlocks(a, 'alice', 'nope'), 'a wrong one does not');

                const again = await rkeys(a, ['create', 'alice', '--passphrase-stdin'], 'pw2\n');
                assertEquals(again.code, 1, 'a second alice fails');
                assertTrue(again.stderr.includes("Key label 'alice' already exists"), `and says so (${again.stderr})`);
                const againAsking = await rkeys(a, ['create', 'alice']);
                assertTrue(againAsking.stderr.includes('already exists'), `before asking for a passphrase (${againAsking.stderr})`);

                ok(await rkeys(a, ['create', 'bob', '--passphrase-stdin'], 'pw\n'), 'create bob');
                const bob = (await KeyStore.open(a, hashSuite)).resolveRecord('bob');

                const exported = join(dir, 'alice.keys');
                const wrote = await rkeys(a, ['export', 'alice', '--out', exported]);
                ok(wrote, 'export alice');
                assertEquals(wrote.stdout, `wrote ${exported}\n`, 'export names the file');
                assertEquals((await fs.stat(exported)).mode & 0o777, 0o600, 'which only the user can read');
                assertEquals(await labels(exported), 'alice', 'the export opens as a keystore holding alice');
                const overwrite = await rkeys(a, ['export', 'bob', '--out', exported]);
                assertEquals(overwrite.code, 1, 'export refuses an existing file');
                assertEquals(await labels(exported), 'alice', 'and leaves it as it was');

                const b = store('b');
                const imported = await rkeys(b, ['import', exported]);
                ok(imported, 'import alice into b');
                assertEquals(imported.stdout, 'imported alice\n', 'import names the key');
                assertTrue(await unlocks(b, 'alice', 'pw'), 'alice unlocks in b with her passphrase');
                const twice = await rkeys(b, ['import', exported]);
                ok(twice, 'importing alice again');
                assertEquals(twice.stdout, 'alice already present\n', 'is a no-op');
                assertEquals(await labels(b), 'alice', 'that leaves one record');

                const c = store('c');
                ok(await rkeys(c, ['import', a, 'bob']), 'import bob from a');
                assertEquals(await labels(c), 'bob', 'brings bob alone');

                const x = store('x');
                ok(await rkeys(x, ['create', 'bob', '--passphrase-stdin'], 'other\n'), 'another bob');
                ok(await rkeys(x, ['create', 'carol', '--passphrase-stdin'], 'pw\n'), 'and carol');
                const cBefore = await fs.readFile(c, 'utf8');
                const sameLabel = await rkeys(c, ['import', x]);
                assertEquals(sameLabel.code, 1, 'a label held by another key fails');
                assertTrue(sameLabel.stderr.includes("Key label 'bob' already exists"), `and names it (${sameLabel.stderr})`);
                assertEquals(await fs.readFile(c, 'utf8'), cBefore, 'and imports nothing, not even carol');

                const relabeled = join(dir, 'relabeled.keys');
                await fs.writeFile(relabeled, JSON.stringify({ version: 1, keys: [{ ...alice, label: 'alias' }] }));
                const bBefore = await fs.readFile(b, 'utf8');
                const sameKey = await rkeys(b, ['import', relabeled]);
                assertEquals(sameKey.code, 1, 'a key held under another label fails');
                assertTrue(sameKey.stderr.includes("Key 'alias' is already in") && sameKey.stderr.includes("as 'alice'"),
                    `and names the label it has (${sameKey.stderr})`);
                assertEquals(await fs.readFile(b, 'utf8'), bBefore, 'and imports nothing');

                const edited = join(dir, 'edited.keys');
                const otherId = (alice.keyId.startsWith('A') ? 'B' : 'A') + alice.keyId.slice(1);
                await fs.writeFile(edited, JSON.stringify({ version: 1, keys: [bob, { ...alice, keyId: otherId }] }));
                const d = store('d');
                const forged = await rkeys(d, ['import', edited]);
                assertEquals(forged.code, 1, 'a record whose key id does not match its public key fails');
                assertTrue(forged.stderr.includes("Key 'alice' in") && forged.stderr.includes('does not match its public key'),
                    `and says which (${forged.stderr})`);
                assertEquals(await labels(d), '', 'and imports neither record');

                const piped = await rkeys(a, ['export', 'alice', 'bob', 'alice']);
                ok(piped, 'export to stdout');
                const e = store('e');
                const fromStdin = await rkeys(e, ['import', '-'], piped.stdout);
                ok(fromStdin, 'import from stdin');
                assertEquals(fromStdin.stdout, 'imported alice\nimported bob\n', 'brings both keys, alice once');
                assertTrue(await unlocks(e, 'bob', 'pw'), 'bob unlocks in e');
            } finally {
                await fs.rm(dir, { recursive: true, force: true });
            }
        },
    },
];

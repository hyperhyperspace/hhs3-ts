import { existsSync, promises as fs } from "node:fs";
import { connect } from "node:net";
import { join } from "node:path";

import { chacha20Poly1305, createIdentity, random, SIGNING_ED25519, type PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { formatReleases } from "@hyper-hyper-space/hhs3_rhost";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import {
    LineDecoder, decodeServerMessage, encodeMessage, type HostStatus, type ServerMessage,
} from "@hyper-hyper-space/hhs3_rhost_client";
import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { scrypt } from "@noble/hashes/scrypt.js";

import {
    base64ToBytes, bytesToBase64, chooseKey, chooseScope, encodePublicKey, initApp, KeyStore, NON_INTERACTIVE, openApp,
    readAppConfig, scriptedPrompter, socketPath,
    type StoredKeyRecord,
} from "../src/index.js";
import {
    appOptions, editConfig, editorReleases, hashSuite, hostSetup, newApp, readApp, tableHas, userKeystore, waitFor, withDir,
} from "./fixtures.js";

// A raw client on run/rhost.sock.
async function rawClient(path: string) {
    const socket = connect(path);
    await new Promise<void>((resolve, reject) => {
        socket.once('connect', () => resolve());
        socket.once('error', reject);
    });
    const decoder = new LineDecoder();
    const received: ServerMessage[] = [];
    let closed = false;
    socket.setEncoding('utf8');
    socket.on('data', (chunk: string) => {
        for (const line of decoder.push(chunk)) received.push(decodeServerMessage(line));
    });
    socket.on('close', () => { closed = true; });
    socket.on('error', () => { /* the close handler follows */ });
    return {
        received,
        closed: () => closed,
        send: (message: unknown) => socket.write(typeof message === 'string' ? message + '\n' : encodeMessage(message as never)),
        find: (id: number, kind: 'result' | 'error' | 'event') => received.filter((m) => m.id === id && kind in m),
        close: () => socket.destroy(),
    };
}

// A record sealed the way KeyStore seals one, but with any key pair under any
// keyId, as someone who can write the keystore file could.
function sealRecord(
    label: string,
    keyId: string,
    publicKey: PublicKey,
    sealed: { publicKey: PublicKey; secretKey: Uint8Array },
    passphrase: string,
): StoredKeyRecord {
    const kdf = { name: 'scrypt' as const, salt: bytesToBase64(random.getBytes(16)), N: 2 ** 10, r: 8, p: 1, dkLen: chacha20Poly1305.keySize };
    const key = scrypt(new TextEncoder().encode(passphrase), base64ToBytes(kdf.salt), { N: kdf.N, r: kdf.r, p: kdf.p, dkLen: kdf.dkLen });
    const nonce = random.getBytes(chacha20Poly1305.nonceSize);
    const plaintext = new TextEncoder().encode(JSON.stringify({
        publicKey: encodePublicKey(sealed.publicKey),
        secretKey: bytesToBase64(sealed.secretKey),
    }));
    const ciphertext = chacha20Poly1305.encrypt(plaintext, key, nonce, new TextEncoder().encode(keyId));
    return {
        label,
        keyId,
        publicKey: encodePublicKey(publicKey),
        kdf,
        aead: { name: 'chacha20-poly1305', nonce: bytesToBase64(nonce), ciphertext: bytesToBase64(ciphertext) },
    };
}

async function refusal(fn: () => Promise<unknown>): Promise<string> {
    try {
        await fn();
    } catch (e) {
        return (e as Error).message;
    }
    return '';
}

const tests: { name: string; invoke: () => Promise<void> }[] = [
    {
        name: '[RHOST_NODE01] rhost init writes app.json from the releases and params only; the key and scope prompts serve create and join',
        invoke: async () => {
            const { v100 } = await editorReleases();
            await withDir('rhost-init-', async (dir) => {
                const appDir = join(dir, 'app');
                const asked = scriptedPrompter(['']);
                const result = await initApp(appDir, { releases: [v100] }, asked);
                assertTrue(asked.asked.includes('param admin (identity) [$me]: '), 'the identity param is asked with $me as default');
                const config = await readAppConfig(appDir);
                assertEquals(JSON.stringify(config.params), '{"admin":"$me"}', 'the param defaults to $me');
                assertEquals(config.autoDeploy, 'minor', 'autoDeploy minor');
                assertEquals(config.projection.path, 'db/data.sqlite', 'the projection file');
                assertEquals(config.keystore, undefined, "the user's keystore");
                assertTrue(existsSync(join(appDir, 'catalogs', result.releases[0]!)), 'the release file is copied');
                assertTrue(!existsSync(join(appDir, 'keys.json')), 'no keystore in the app folder');
                assertTrue(!existsSync(join(appDir, 'hosts')), 'and no host yet');

                const refusedAgain = await refusal(() => initApp(appDir, { releases: [v100] }, asked));
                assertTrue(refusedAgain.includes('already exists'), `an existing app.json is refused (${refusedAgain})`);
                const flagged = await initApp(join(dir, 'flags'), { releases: [v100], params: { admin: '$k1' } }, NON_INTERACTIVE);
                assertEquals(JSON.stringify(flagged.config.params), '{"admin":"$k1"}', 'a --param needs no terminal');
                const noParam = await refusal(() => initApp(join(dir, 'missing'), { releases: [v100] }, NON_INTERACTIVE));
                assertTrue(noParam.includes('--param admin='), `no terminal: --param is named (${noParam})`);
                const undeclared = await refusal(() => initApp(join(dir, 'extra'), { releases: [v100], params: { admin: '$me', color: 'red' } }));
                assertTrue(undeclared.includes("declare no param 'color'"), `an undeclared param is refused (${undeclared})`);

                const keystore = await userKeystore(dir, ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']);
                const keys = await KeyStore.open(keystore, hashSuite);
                const other = scriptedPrompter(['9', 'k9']);
                const picked = await chooseKey(undefined, keys, keystore, other);
                assertTrue(other.said.includes('  8. k8 (' + keys.resolveRecord('k8').keyId.slice(0, 8) + ')'),
                    `eight keys are listed (${other.said.join(' | ')})`);
                assertTrue(other.said.includes('  9. Other') && other.said.includes('  10. Create new key pair'),
                    'then Other and Create new key pair');
                assertTrue(picked.kind === 'existing' && picked.record.label === 'k9', 'the key chosen through Other');
                const created = await chooseKey(undefined, keys, keystore, scriptedPrompter(['10', 'hostkey', 'secret', 'secret']));
                assertTrue(created.kind === 'create' && created.label === 'hostkey' && created.passphrase === 'secret', 'a new key pair');
                const byId = await chooseKey(`#${keys.resolveRecord('k1').keyId.slice(0, 8)}`, keys, keystore, NON_INTERACTIVE);
                assertTrue(byId.kind === 'existing' && byId.record.label === 'k1', 'a #prefix names a key');
                const noKey = await refusal(() => chooseKey(undefined, keys, keystore, NON_INTERACTIVE));
                assertTrue(noKey.includes('rhost create needs --key'), `no terminal: --key is named (${noKey})`);

                assertEquals(await chooseScope(undefined, scriptedPrompter(['1'])), 'internet', 'scope 1 is internet');
                assertEquals(await chooseScope(undefined, scriptedPrompter(['localhost'])), 'localhost', 'or by name');
                const noScope = await refusal(() => chooseScope(undefined, NON_INTERACTIVE, 'rhost join'));
                assertTrue(noScope.includes('rhost join needs --scope'), `no terminal: --scope is named (${noScope})`);
            });
        },
    },
    {
        name: '[RHOST_NODE02] openApp creates and serves a host behind its lock file',
        invoke: async () => {
            const { v100 } = await editorReleases();
            await withDir('rh-open-', async (dir) => {
                const appDir = await newApp(dir, [v100]);
                const app = await openApp(appDir, appOptions(dir));
                const second = await openApp(appDir, appOptions(dir));
                try {
                    await app.create(undefined, hostSetup());
                    const folder = join(appDir, 'hosts', 'default');
                    assertTrue(existsSync(join(folder, 'host.json')) && existsSync(join(folder, 'rdb', 'replica.rdb')), 'the host folder');
                    assertEquals((await fs.readdir(join(appDir, 'hosts'))).join(','), 'default', 'no pending folder is left');
                    const record = JSON.parse(await fs.readFile(join(folder, 'host.json'), 'utf8')) as {
                        key: { label: string; keyId: string; publicKey: string; passphrase?: string }; sync: { scope: string };
                    };
                    const me = (await KeyStore.open(join(dir, 'user-keys.json'), hashSuite)).resolvePublic('me');
                    assertEquals(record.key.label, 'me', 'host.json names the key');
                    assertEquals(record.key.keyId, me.keyId, 'with its id');
                    assertEquals(record.key.publicKey, serializePublicKeyToBase64(me.publicKey), 'and its public key');
                    assertEquals(record.key.passphrase, 'prompt', 'and its passphrase source');
                    assertEquals(record.sync.scope, 'localhost', 'host.json has the network settings');

                    await app.start('default');
                    const data = join(folder, 'db', 'data.sqlite');
                    await waitFor(() => existsSync(data) && tableHas(data, 'user_identities')
                        && readApp(data, (db) => db.prepare("SELECT 1 FROM user_identities WHERE name = 'Admin'").get() !== undefined),
                        'the admin identity row in db/data.sqlite');
                    assertEquals((await fs.readdir(folder)).sort().join(','), 'db,host.json,rdb,run', 'the host folder while it serves');
                    const lock = join(folder, 'run', 'rhost.lock');
                    assertEquals((await fs.readFile(lock, 'utf8')).trim(), String(process.pid), 'the lock holds our pid');

                    let refused = '';
                    try { await second.start('default'); } catch (e) { refused = String(e); }
                    assertTrue(refused.includes(`host 'default' is already running (pid ${process.pid})`), `a second process is refused (${refused})`);

                    await app.stop('default');
                    assertTrue(!existsSync(lock), 'stop releases the lock');
                    await fs.writeFile(lock, '2147483646\n');
                    await second.start('default');
                    assertTrue((await second.host('default')).running, 'a stale lock is taken over');
                } finally {
                    await second.close();
                    await app.close();
                }
            });
        },
    },
    {
        name: '[RHOST_NODE03] a running host answers status and watches on run/rhost.sock, readable only by the user',
        invoke: async () => {
            const { v100, v110 } = await editorReleases();
            await withDir('rh-sock-', async (dir) => {
                const appDir = await newApp(dir, [v100]);
                const creator = await openApp(appDir, appOptions(dir));
                await creator.create(undefined, hostSetup());
                await creator.close();
                await fs.copyFile(v110, join(appDir, 'catalogs', v110.split('/').pop()!));
                await editConfig(appDir, (config) => { config['autoDeploy'] = 'none'; });

                const app = await openApp(appDir, appOptions(dir));
                const path = socketPath(appDir, 'default');
                try {
                    assertTrue(!existsSync(path), 'no socket while stopped');
                    await app.start('default');
                    const stat = await fs.stat(path);
                    assertTrue(stat.isSocket(), 'run/rhost.sock is a socket');
                    assertEquals(path, join(appDir, 'hosts', 'default', 'run', 'rhost.sock'), 'the socket path');
                    assertEquals(stat.mode & 0o777, 0o600, 'readable and writable by the user only');

                    const client = await rawClient(path);
                    client.send({ id: 1, method: 'status' });
                    await waitFor(() => client.find(1, 'result').length === 1, 'the status reply');
                    const status = (client.find(1, 'result')[0] as { result: HostStatus }).result;
                    assertTrue(status.running && typeof status.peers === 'number', `running, with a peer count (${JSON.stringify(status)})`);
                    assertEquals(status.host, 'default', 'the status names the host');
                    assertEquals(formatReleases(status.deployed), '1.0.0', 'the deployed release');
                    assertEquals(status.notDeployed, 'autoDeploy is none', 'and why 1.1.0 waits');

                    client.send({ id: 2, method: 'watchStatus' });
                    await waitFor(() => client.find(2, 'result').length === 1, 'the watch reply');
                    const host = await app.host('default');
                    await host.deploy();
                    await waitFor(() => client.find(2, 'event').some((m) => formatReleases((m as { event: HostStatus }).event.deployed) === '1.1.0'),
                        'the watch pushes the deploy');

                    (host as unknown as { fail(err: unknown): void }).fail(new Error('boom'));
                    await waitFor(() => client.find(2, 'event').some((m) => (m as { event: HostStatus }).event.lastError === 'boom'),
                        'the watch pushes the last error');
                    client.send({ id: 3, method: 'status' });
                    await waitFor(() => client.find(3, 'result').length === 1, 'a second status');
                    assertEquals((client.find(3, 'result')[0] as { result: HostStatus }).result.lastError, 'boom', 'status carries the last error');

                    client.send({ id: 4, method: 'deploy' });
                    await waitFor(() => client.find(4, 'error').length === 1, 'the error reply');
                    assertTrue((client.find(4, 'error')[0] as { error: string }).error.includes('unknown method'), 'no signing methods');

                    await app.stop('default');
                    await waitFor(() => client.closed(), 'stop closes the connections');
                    assertTrue(!existsSync(path), 'stop removes the socket');

                    await fs.writeFile(path, 'stale');
                    await app.start('default');
                    assertTrue((await fs.stat(path)).isSocket(), 'a stale file is replaced');
                    const again = await rawClient(path);
                    again.send({ id: 1, method: 'status' });
                    await waitFor(() => again.find(1, 'result').length === 1, 'the restarted host answers');
                    again.close();
                } finally {
                    await app.close();
                }
            });
        },
    },
    {
        name: '[RHOST_NODE04] a projection path creates its parent folders in the host',
        invoke: async () => {
            const { v100 } = await editorReleases();
            await withDir('rh-proj-', async (dir) => {
                const appDir = await newApp(dir, [v100]);
                await editConfig(appDir, (config) => { config['projection'] = { path: 'custom/nested/app.sqlite' }; });
                const app = await openApp(appDir, appOptions(dir));
                try {
                    await app.create(undefined, hostSetup());
                    await app.start('default');
                    const data = join(appDir, 'hosts', 'default', 'custom', 'nested', 'app.sqlite');
                    await waitFor(() => tableHas(data, 'user_identities'), 'the projection at custom/nested/app.sqlite');
                } finally {
                    await app.close();
                }
            });
        },
    },
    {
        name: "[RHOST_NODE05] keys come from app.json's keystore, else the user's; a missing key names the keystore file",
        invoke: async () => {
            const { v100 } = await editorReleases();
            await withDir('rh-keystore-', async (dir) => {
                const appDir = await newApp(dir, [v100]);
                const mesh = appOptions(dir).meshFactory;
                const app = await openApp(appDir, { meshFactory: mesh, env: { HOST_PASS: 'pw' } });
                try {
                    assertEquals(app.platform.keystoreLocation, join(dir, 'user-keys.json'), "app.json's keystore, relative to the app folder");
                    await app.create(undefined, hostSetup({ passphrase: 'env:HOST_PASS' }));
                    await app.start('default');
                    assertTrue((await app.host('default')).running, 'the passphrase came from HOST_PASS');
                } finally {
                    await app.close();
                }

                const noEnv = await openApp(appDir, { meshFactory: mesh, env: {} });
                try {
                    const unset = await refusal(() => noEnv.start('default'));
                    assertTrue(unset.includes("the key 'me' takes its passphrase from HOST_PASS, which is not set"), `an unset variable is named (${unset})`);
                } finally {
                    await noEnv.close();
                }

                await editConfig(appDir, (config) => { delete config['keystore']; });
                const saved = process.env['RDB_KEYSTORE'];
                const empty = join(dir, 'empty-keys.json');
                process.env['RDB_KEYSTORE'] = empty;
                try {
                    const bare = await openApp(appDir, appOptions(dir));
                    try {
                        assertEquals(bare.platform.keystoreLocation, empty, "without one, the user's keystore");
                        const missing = await refusal(() => bare.start('default'));
                        assertTrue(missing.includes("host 'default' signs with the key 'me'") && missing.includes(`which isn't in ${empty}`),
                            `a missing key names the host, the key and the keystore (${missing})`);
                    } finally {
                        await bare.close();
                    }
                } finally {
                    if (saved === undefined) delete process.env['RDB_KEYSTORE'];
                    else process.env['RDB_KEYSTORE'] = saved;
                }
            });
        },
    },
    {
        name: '[RHOST_NODE06] unlock refuses a sealed key pair that does not match the record key id',
        invoke: async () => {
            await withDir('rh-keys-', async (dir) => {
                const keys = await KeyStore.open(join(dir, 'keys.json'), hashSuite);
                const a = await createIdentity(SIGNING_ED25519, hashSuite);
                const b = await createIdentity(SIGNING_ED25519, hashSuite);
                await keys.importRecord(sealRecord('a', a.keyId, a.publicKey, a, 'pw'));
                await keys.importRecord(sealRecord('swapped', a.keyId, a.publicKey, b, 'pw'));
                await keys.importRecord(sealRecord('mixed', a.keyId, a.publicKey, { publicKey: a.publicKey, secretKey: b.secretKey }, 'pw'));

                assertEquals((await keys.unlock('a', 'pw')).keyId, a.keyId, 'an honest record unlocks');

                const swapped = await refusal(() => keys.unlock('swapped', 'pw'));
                assertTrue(swapped.includes('does not match its key id'), `another key pair under a's key id is refused (${swapped})`);

                const mixed = await refusal(() => keys.unlock('mixed', 'pw'));
                assertTrue(mixed.includes('does not match its public key'), `a's public key with b's secret key is refused (${mixed})`);

                const created = await keys.create('c', 'pw');
                assertEquals((await keys.unlock('c', 'pw')).keyId, created.keyId, 'a created key unlocks');
            });
        },
    },
];

async function main() {
    const filters = process.argv.slice(2);

    console.log('Running tests for Hyper Hyper Space v3 rhost_node module'
        + (filters.length > 0 ? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');

    console.log('rhost_node');

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

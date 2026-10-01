import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertFalse, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import { MemoryKeyVault } from "@hyper-hyper-space/hhs3_rdb_runtime";
import type { ClientEvent, HostStatus, ServerMessage } from "@hyper-hyper-space/hhs3_rhost_client";
import { decodeServerMessage } from "@hyper-hyper-space/hhs3_rhost_client";

import {
    Rhost, adoptionRangeFor, effectiveConfig, formatReleases, parseAppConfig, parseHostRecord, serveClient,
    shippedReleases,
    type Host,
} from "../src/index.js";
import {
    MemPlatform, RelabeledVault, SharedMesh, appConfig, editor, hostSetup, rowsOf, waitFor, withFiles, withParam,
} from "./fixtures.js";

// Both ends of an in-memory connection to serveClient.
function clientPair(host: Host) {
    const received: ServerMessage[] = [];
    let onLine: ((line: string) => void) | undefined;
    let onClose: (() => void) | undefined;
    serveClient(host, {
        send: (line) => { received.push(decodeServerMessage(line)); },
        onLine: (listener) => { onLine = listener; },
        onClose: (listener) => { onClose = listener; },
    });
    return {
        received,
        request: (message: unknown) => onLine!(typeof message === 'string' ? message : JSON.stringify(message)),
        close: () => onClose!(),
        find: (id: number, kind: 'result' | 'error' | 'event') => received.filter((m) => m.id === id && kind in m),
    };
}
import { runAllowSourceParseTests, runSyncAuthorizerTests } from "./sync_tests.js";

function throwsWith(fn: () => unknown, fragment: string, what: string): void {
    try {
        fn();
    } catch (e) {
        assertTrue(String(e).includes(fragment), `${what}: expected '${fragment}' in '${String(e)}'`);
        return;
    }
    throw new Error(`${what}: expected an error`);
}

async function rejectsWith(fn: () => Promise<unknown>, fragment: string, what: string): Promise<void> {
    try {
        await fn();
    } catch (e) {
        assertTrue(String(e).includes(fragment), `${what}: expected '${fragment}' in '${String(e)}'`);
        return;
    }
    throw new Error(`${what}: expected an error`);
}

async function deployedOf(host: Host): Promise<string> {
    return formatReleases((await host.status()).deployed);
}

type Json = { [key: string]: unknown };

function copy(value: unknown): Json {
    return JSON.parse(JSON.stringify(value)) as Json;
}

function without(value: unknown, field: string): Json {
    const out = copy(value);
    delete out[field];
    return out;
}

// A host.json as create or join writes it.
const RECORD = {
    database: 'DB', catalog: 'editor', created: false,
    key: { label: 'mine', keyId: 'KID', publicKey: 'PK', passphrase: 'prompt' },
    sync: { scope: 'internet', tracker: 'wss://tracker.example' },
};

const tests: { name: string; invoke: () => Promise<void> }[] = [
    {
        name: '[RHOST01] both files refuse unknown fields and name them; the key and network are the host\'s, the allow list the app\'s',
        invoke: async () => {
            const good = appConfig({ allow: ['user.identities.keyId'], keystore: 'keys.json' });
            const app = parseAppConfig(copy(good));
            assertEquals(app.allow?.[0], 'user.identities.keyId', 'a good app.json parses');
            assertEquals(app.keystore, 'keys.json', 'with its keystore');

            throwsWith(() => parseAppConfig(without(good, 'releases')), 'app.json: releases is missing', 'missing releases');
            throwsWith(() => parseAppConfig(without(good, 'projection')), 'projection is missing', 'missing projection');
            throwsWith(() => parseAppConfig({ ...good, extra: 1 }), "'extra' is not a known field", 'unknown top-level field');
            throwsWith(() => parseAppConfig({ ...good, sync: { scope: 'internet' } }), "'sync' is not a known field", 'sync is the host\'s');
            throwsWith(() => parseAppConfig({ ...good, identity: { key: 'me' } }), "'identity' is not a known field", 'so is the key');
            throwsWith(() => parseAppConfig({ ...good, autoDeploy: 'always' }), 'autoDeploy must be', 'bad autoDeploy');
            throwsWith(() => parseAppConfig({ ...good, allow: ['user.identities'] }), 'allow[0]', 'bad allow entry');
            throwsWith(() => parseAppConfig({ ...good, allow: 'user.identities.keyId' }), 'allow must be an array', 'allow is a list');
            throwsWith(() => parseAppConfig({ ...good, params: { admin: null } }), 'params.admin must be a JSON value', 'a param is a JSON value');
            throwsWith(() => parseAppConfig({ ...good, projection: { path: 'data.sqlite', extra: 1 } }),
                'projection.extra is not a known field', 'unknown projection field');
            throwsWith(() => parseAppConfig({ ...good, projection: { path: 'data.sqlite', indexes: 'indexes.json' } }),
                'projection.indexes', 'an index path is not a spec');
            throwsWith(() => parseAppConfig({
                ...good,
                projection: { path: 'data.sqlite', indexes: [{ name: 'x', group: 'editor', table: 'notes', columns: ['label'], unique: true }] },
            }), 'projection.indexes[0].unique is not a known field', 'unknown index field');
            const indexed = parseAppConfig({
                ...good,
                projection: {
                    path: 'data.sqlite', indexPub: true,
                    indexes: [{ name: 'by_label', group: 'editor', table: 'notes', columns: ['label'] }],
                },
            });
            assertEquals(indexed.projection.indexes?.[0]?.name, 'by_label', 'inline indexes parse');
            assertEquals(indexed.projection.indexPub, true, 'indexPub parses');
            throwsWith(() => parseAppConfig({
                ...good,
                projection: {
                    path: 'data.sqlite',
                    indexes: [{ name: 'pub__x', group: 'editor', table: 'notes', columns: ['label'] }],
                },
            }), 'reserved', 'a reserved index name is refused');

            const record = parseHostRecord(copy(RECORD));
            assertEquals(record.key.keyId, 'KID', 'a good host.json parses');
            throwsWith(() => parseHostRecord({ ...RECORD, extra: 1 }), "host.json: 'extra' is not a known field", 'unknown top-level field');
            throwsWith(() => parseHostRecord({ ...RECORD, releases: 'other/' }), "'releases' is not a known field", 'releases are the app\'s');
            throwsWith(() => parseHostRecord({ ...RECORD, allow: [] }), "'allow' is not a known field", 'so is allow');
            throwsWith(() => parseHostRecord({ ...RECORD, keystore: 'keys.json' }), "'keystore' is not a known field", 'and the keystore');
            throwsWith(() => parseHostRecord({ ...RECORD, created: 'yes' }), 'created must be true or false', 'bad created');
            throwsWith(() => parseHostRecord(without(RECORD, 'created')), 'created is missing', 'missing created');
            throwsWith(() => parseHostRecord({ ...RECORD, key: 'mine' }), 'key must be an object', 'a key is more than a label');
            throwsWith(() => parseHostRecord({ ...RECORD, key: { ...RECORD.key, extra: 1 } }), 'key.extra is not a known field', 'unknown key field');
            throwsWith(() => parseHostRecord({ ...RECORD, key: without(RECORD.key, 'keyId') }), 'key.keyId is missing', 'missing key id');
            throwsWith(() => parseHostRecord({ ...RECORD, key: { ...RECORD.key, passphrase: 'env:' } }), 'key.passphrase', 'bad passphrase source');
            throwsWith(() => parseHostRecord(without(RECORD, 'sync')), 'sync is missing', 'missing sync');
            throwsWith(() => parseHostRecord({ ...RECORD, sync: {} }), 'sync.scope is missing', 'missing scope');
            throwsWith(() => parseHostRecord({ ...RECORD, sync: { scope: 'lan' } }), 'sync.scope must be', 'bad scope');
            throwsWith(() => parseHostRecord({ ...RECORD, sync: { scope: 'localhost', port: 1 } }), 'sync.port is not a known field', 'unknown sync field');
            throwsWith(() => parseHostRecord({ ...RECORD, sync: { scope: 'localhost', allow: [] } }), 'sync.allow is not a known field', 'allow is not a network setting');
            throwsWith(() => parseHostRecord({ ...RECORD, sync: { scope: 'localhost', listen: 'port 7400' } }), 'sync.listen must be an address', 'bad listen');
            throwsWith(() => parseHostRecord({ ...RECORD, projection: { path: 'data.sqlite', extra: 1 } }),
                'projection.extra is not a known field', 'unknown projection override field');

            const plain = effectiveConfig(app, record);
            assertEquals(plain.key.label, 'mine', 'the key comes from host.json');
            assertEquals(plain.key.passphrase, 'prompt', 'and its passphrase source');
            assertEquals(plain.sync.tracker, 'wss://tracker.example', 'the host has its own tracker');
            assertEquals(plain.allow.length, 1, "and keeps the app's allow list");
            assertEquals(plain.autoDeploy, 'minor', 'autoDeploy defaults to minor');
            assertEquals(plain.projection.path, 'data.sqlite', 'the projection is the app\'s');
            const overridden = effectiveConfig(app, parseHostRecord({
                ...RECORD, autoDeploy: 'none', params: {}, projection: { path: 'other.sqlite' },
            }));
            assertEquals(overridden.autoDeploy, 'none', 'autoDeploy is replaced');
            assertEquals(Object.keys(overridden.params).length, 0, 'params are replaced');
            assertEquals(overridden.projection.path, 'other.sqlite', 'the projection is replaced');
            assertEquals(overridden.allow.length, 1, 'allow never is');

            const { v100, v110 } = await editor();
            assertEquals(shippedReleases([v100, v110]).get('editor')?.manifest.version, '1.1.0', 'the newest file ships');
            assertEquals(shippedReleases([v110, v100]).get('editor')?.manifest.version, '1.1.0', 'whatever the order');
            throwsWith(() => shippedReleases([v100, JSON.parse(JSON.stringify(v100))]), 'two release files ship editor 1.0.0', 'same version twice');
            assertEquals(adoptionRangeFor('1.1.0'), '<2.0.0', 'the range is everything below the next major');

            await runSyncAuthorizerTests();
            runAllowSourceParseTests();
        },
    },
    {
        name: '[RHOST02] create deploys the shipped release with the host key as creator, and records the key and network',
        invoke: async () => {
            const { v100 } = await editor();
            const mesh = new SharedMesh();
            const platform = await MemPlatform.withKey([v100], mesh);
            const app = await Rhost.open(appConfig(), platform);
            try {
                const host = await app.create(undefined, hostSetup());
                const me = platform.vault.resolvePublic('me');
                assertEquals(host.name, 'default', 'the host name defaults to default');
                assertEquals(host.record.catalog, 'editor', 'the record names the catalog');
                assertEquals(host.record.key.label, 'me', 'the record names the key');
                assertEquals(host.record.key.keyId, me.keyId, 'with its id');
                assertEquals(host.record.key.publicKey, serializePublicKeyToBase64(me.publicKey), 'and its public key');
                assertEquals(host.record.key.passphrase, 'prompt', 'and where its passphrase comes from');
                assertEquals(host.record.sync.scope, 'localhost', 'the record has the network settings');
                assertTrue(host.record.created, 'the record says it was created here');
                assertEquals(host.db.getName(), 'default', 'the database name defaults to the host name');
                assertTrue(host.db.isCreator(me.keyId), 'the host key is the creator');

                const status = await host.status();
                assertEquals(formatReleases(status.deployed), '1.0.0', 'deployed 1.0.0');
                assertEquals(status.adoptionRange, '<2.0.0', 'adopting below 2.0.0');
                assertEquals(status.members.map((m) => `${m.name}:${m.state}`).sort().join(','), 'doc:ok,user:ok', 'members ok');
                assertFalse(status.hostBehind, 'not behind');
                assertEquals(status.notDeployed, undefined, 'nothing held');
                assertFalse(status.upgradeRequired, 'no upgrade required');
                assertFalse(status.running, 'create does not start the host');
                assertEquals((await app.hosts()).join(','), 'default', 'one host');

                await rejectsWith(() => app.create(undefined, hostSetup()), "host 'default' already exists", 'create twice');

                const noParams = await Rhost.open(appConfig({ params: {} }), platform);
                await rejectsWith(() => noParams.create('other', hostSetup()), "the release needs 'admin'", 'a missing param is named');
                const extra = await Rhost.open(appConfig({ params: { admin: '$me', color: 'red' } }), platform);
                await rejectsWith(() => extra.create('other', hostSetup()), "declares no param 'color'", 'an undeclared param is named');
                await rejectsWith(() => app.create('other', hostSetup({ key: 'nobody' })), "host 'other': no key 'nobody' in the test vault", 'an unknown key');
                await rejectsWith(() => app.create('other', hostSetup({ passphrase: 'ask' })), "host 'other': passphrase must be", 'a bad passphrase source');
                await rejectsWith(() => app.create('other', hostSetup({ sync: { scope: 'lan' } as never })), "host 'other': sync.scope must be", 'a bad scope');
                await rejectsWith(() => app.create('other', hostSetup({ sync: { scope: 'localhost', allow: [] } as never })),
                    'sync.allow is not a known field', 'network settings are checked as host.json would be');
                assertEquals((await app.hosts()).join(','), 'default', 'a failed create leaves no host');
            } finally {
                await app.close();
                mesh.close();
            }
        },
    },
    {
        name: '[RHOST03] update deploys when due: minor within a major, major across, none never',
        invoke: async () => {
            const { v100, v110, v200 } = await editor();
            const mesh = new SharedMesh();
            const platform = await MemPlatform.withKey([v100], mesh);
            try {
                const first = await Rhost.open(appConfig(), platform);
                await first.create(undefined, hostSetup());
                await first.close();

                platform.releases = [v100, v110];
                const none = await Rhost.open(appConfig({ autoDeploy: 'none' }), platform);
                const [held] = await none.update();
                assertEquals(held!.notDeployed, 'autoDeploy is none', 'none never deploys');
                assertTrue(held!.appliedEntries > 0, 'the release is installed anyway');
                const behind = (await none.status())[0]!;
                assertTrue(behind.hostBehind, 'the host is behind the shipped release');
                assertEquals(behind.notDeployed, 'autoDeploy is none', 'and its status says why');
                assertEquals(behind.shipped?.version, '1.1.0', 'shipping 1.1.0');
                await none.close();

                const minor = await Rhost.open(appConfig(), platform);
                const [deployed] = await minor.update();
                assertEquals(deployed!.deployed?.version, '1.1.0', 'minor deploys a minor bump');
                const host = await minor.host('default');
                assertEquals(await deployedOf(host), '1.1.0', 'deployed 1.1.0');
                assertEquals((await minor.update())[0]!.notDeployed, '1.1.0 is already deployed', 'a second update is a no-op');
                await minor.close();

                platform.releases = [v200];
                const blocked = await Rhost.open(appConfig(), platform);
                const [major] = await blocked.update();
                assertTrue(major!.notDeployed?.includes('major bump from 1.1.0') === true, `minor holds a major bump (${major!.notDeployed})`);
                assertEquals(major!.adoptionRange, '<3.0.0', 'the range follows the shipped major');
                await blocked.close();

                const bumped = await Rhost.open(appConfig({ autoDeploy: 'major' }), platform);
                assertEquals((await bumped.update())[0]!.deployed?.version, '2.0.0', 'major deploys it');
                assertEquals(await deployedOf(await bumped.host('default')), '2.0.0', 'deployed 2.0.0');
                await bumped.close();
            } finally {
                mesh.close();
            }
        },
    },
    {
        name: '[RHOST04] a joined host syncs the database and its rows, and deploys nothing',
        invoke: async () => {
            const { v100, v110 } = await editor();
            const mesh = new SharedMesh();
            const platformA = await MemPlatform.withKey([v100], mesh);
            const platformB = await MemPlatform.withKey([v110], mesh);
            const appA = await Rhost.open(appConfig(), platformA);
            const appB = await Rhost.open(appConfig(), platformB);
            try {
                const a = await appA.create(undefined, hostSetup());
                await appA.start('default');

                const b = await appB.join(a.db.getId(), undefined, hostSetup());
                assertFalse(b.record.created, 'joined, not created');
                assertEquals(b.record.catalog, 'editor', 'the catalog is matched by id');
                assertEquals(b.record.key.keyId, platformB.vault.resolvePublic('me').keyId, "the record has B's own key");
                await appB.start('default');

                const adminKey = platformA.vault.resolvePublic('me').keyId;
                await waitFor(async () => formatReleases((await b.status()).deployed) === '1.0.0', "B sees A's deploy");
                await waitFor(async () => {
                    const target = platformB.targets.get('default');
                    for (const row of rowsOf(target, 'user_identities')) {
                        if (row['name'] === 'Admin' && await target!.keyHashForId('', row['key_id'] as number) === adminKey) return true;
                    }
                    return false;
                }, "B's projection has A's admin row");

                const report = await b.update();
                assertTrue(report.notDeployed?.includes('not a creator') === true, `B can't deploy (${report.notDeployed})`);
                await rejectsWith(() => b.deploy(), 'not a creator', 'an explicit deploy keeps the authority check');
                const status = await b.status();
                assertTrue(status.hostBehind, 'B ships 1.1.0 and is behind');
                assertTrue(status.notDeployed?.includes('not a creator') === true, 'and its status says why');
                assertEquals(await deployedOf(a), '1.0.0', "A's database still runs 1.0.0");

                await a.runtime.session.unlockKey('me', 'pw');
                a.runtime.session.selectAuthor('me');
                await a.runtime.execute(`INSERT INTO user.caps (label, grantee) VALUES ('writer', $me) BY $me;`);
                await waitFor(() => rowsOf(platformB.targets.get('default'), 'user_caps')
                    .some((row) => row['label'] === 'writer'), "a row A writes later reaches B's projection");
            } finally {
                await appB.close();
                await appA.close();
                mesh.close();
            }
        },
    },
    {
        name: '[RHOST05] a host holds a newer major deployed elsewhere and reports upgradeRequired',
        invoke: async () => {
            const { v110, v200 } = await editor();
            const mesh = new SharedMesh();
            const platformA = await MemPlatform.withKey([v200], mesh);
            const platformB = await MemPlatform.withKey([v110], mesh);
            const appA = await Rhost.open(appConfig(), platformA);
            const appB = await Rhost.open(appConfig(), platformB);
            try {
                const a = await appA.create(undefined, hostSetup());
                await appA.start('default');
                const b = await appB.join(a.db.getId(), undefined, hostSetup());
                await appB.start('default');

                await waitFor(async () => (await b.status()).held.some((r) => r.version === '2.0.0'), 'B holds 2.0.0');
                const status = await b.status();
                assertEquals(status.adoptionRange, '<2.0.0', 'B adopts below 2.0.0');
                assertTrue(status.upgradeRequired, 'B reports upgradeRequired');
                assertFalse(status.hostBehind, 'B is not behind: it ships an older release');
            } finally {
                await appB.close();
                await appA.close();
                mesh.close();
            }
        },
    },
    {
        name: '[RHOST06] hosts start and stop one by one, behind a per-host lock',
        invoke: async () => {
            const { v100 } = await editor();
            const mesh = new SharedMesh();
            const platformA = await MemPlatform.withKey([v100], mesh);
            const platformB = await MemPlatform.withKey([v100], mesh);
            const appA = await Rhost.open(appConfig(), platformA);
            const appB = await Rhost.open(appConfig(), platformB);
            try {
                const a = await appA.create(undefined, hostSetup());
                const gaming = await appA.create('gaming', hostSetup());
                const results = await appA.start();
                assertEquals(results.map((r) => `${r.host}:${r.error ?? 'ok'}`).join(','), 'default:ok,gaming:ok', 'a bare start starts both');
                assertEquals((await appA.status()).map((s) => s.host).join(','), 'default,gaming', 'a bare status lists every host');

                const b = await appB.join(a.db.getId(), undefined, hostSetup());
                await appB.start('default');
                await appA.stop('gaming');
                assertFalse(gaming.running, 'gaming stopped');
                assertTrue(a.running, 'default still runs');

                await a.runtime.session.unlockKey('me', 'pw');
                a.runtime.session.selectAuthor('me');
                await a.runtime.execute(`INSERT INTO user.caps (label, grantee) VALUES ('writer', $me) BY $me;`);
                await waitFor(() => rowsOf(platformB.targets.get('default'), 'user_caps')
                    .some((row) => row['label'] === 'writer'), 'default keeps syncing after gaming stops');

                const third = await appA.create('third', hostSetup());
                assertFalse(third.running, 'create on a started app does not start the new host');
                await appA.start('third');
                assertTrue(third.running, 'start(name) starts it');

                const second = await Rhost.open(appConfig(), platformA);
                await rejectsWith(() => second.start('default'), "host 'default' is already running", 'the lock refuses a second process');
                await second.close();

                await appA.remove('third');
                assertFalse(platformA.locks.has('third'), 'remove stopped the host and released its lock');
                assertEquals((await appA.hosts()).join(','), 'default,gaming', 'third is gone');
                assertTrue(b.running, 'B is unaffected');
            } finally {
                await appB.close();
                await appA.close();
                mesh.close();
            }
        },
    },
    {
        name: '[RHOST07] the in-process client, the protocol dispatcher, and an explicit deploy',
        invoke: async () => {
            const { v100, v110 } = await editor();
            const mesh = new SharedMesh();
            const platform = await MemPlatform.withKey([v100], mesh);
            const first = await Rhost.open(appConfig(), platform);
            await first.create(undefined, hostSetup());
            await first.close();

            platform.releases = [v110];
            const app = await Rhost.open(appConfig({ autoDeploy: 'none' }), platform);
            try {
                const host = await app.host('default');
                await rejectsWith(() => host.client.me(), "host 'default' is not running", 'key calls need the host running');
                await app.start('default');
                const client = host.client;

                const me = await client.me();
                const key = platform.vault.resolvePublic('me');
                assertEquals(me.label, 'me', 'the key label');
                assertEquals(me.keyId, key.keyId, 'the host key');
                assertEquals(me.publicKey, serializePublicKeyToBase64(key.publicKey), 'its public key');
                assertEquals((await client.me()).id, me.id, 'me is stable');
                const hashSuite = createBasicCrypto().hash(HASH_SHA256);
                const bob = serializePublicKeyToBase64((await createIdentity(SIGNING_ED25519, hashSuite)).publicKey);
                const bobId = await client.registerKey(bob);
                assertEquals(await client.registerKey(bob), bobId, 'registerKey is idempotent');
                assertTrue(bobId !== me.id, 'another key gets another id');

                const watched: ClientEvent[] = [];
                const unwatch = client.events.watch((events) => { watched.push(...events); });
                await new Promise((resolve) => setTimeout(resolve, 100));
                platform.targets.get('default')!.localInsert('doc_pages', { title: 'not a writer', deleted: false });
                await waitFor(() => watched.some((e) => e.origin === 'ingestion' && e.direction === 'failure'),
                    'the rejected write reaches the event watch');
                const logged = await client.events.since();
                assertTrue(logged.some((e) => e.id === watched[0]!.id && e.table === 'doc_pages'), 'and the log, with the same id');
                unwatch();

                const status = await client.status();
                assertTrue(status.running && 'hostBehind' in status && status.hostBehind, 'running and behind 1.1.0');

                const pair = clientPair(host);
                pair.request({ id: 1, method: 'status' });
                await waitFor(() => pair.find(1, 'result').length === 1, 'the status reply');
                assertTrue((pair.find(1, 'result')[0] as { result: HostStatus }).result.running, 'the reply says running');

                pair.request({ id: 2, method: 'watchStatus' });
                await waitFor(() => pair.find(2, 'result').length === 1, 'the watch reply');
                const deployed = await host.deploy();
                assertEquals(deployed.deployed.version, '1.1.0', 'deploy ignores autoDeploy none');
                assertEquals(deployed.host, 'default', 'the report names the host');
                await waitFor(() => pair.find(2, 'event').some((m) => formatReleases((m as { event: HostStatus }).event.deployed) === '1.1.0'),
                    'the watch pushes the deploy');
                await rejectsWith(() => host.deploy(), '1.1.0 is already deployed', 'deploying twice');
                await rejectsWith(() => host.deploy({ release: '9.9.9' }), "no release of 'editor' matches '9.9.9'", 'an unknown version');
                await rejectsWith(() => host.deploy({ release: 'v1' }), 'is not a version', 'a bad selector');
                await rejectsWith(() => host.deploy({ release: '1.0.0' }), '1.0.0 is below the deployed 1.1.0', 'a release below the deployed one');

                pair.request({ id: 3, method: 'unwatch', watch: 2 });
                await waitFor(() => pair.find(3, 'result').length === 1, 'the unwatch reply');
                pair.request({ id: 4, method: 'deploy' });
                pair.request('nope');
                await waitFor(() => pair.find(4, 'error').length === 1 && pair.find(0, 'error').length === 1, 'the error replies');
                assertTrue((pair.find(4, 'error')[0] as { error: string }).error.includes('unknown method "deploy"'), 'no signing over the protocol');
                const pushes = pair.find(2, 'event').length;
                await app.stop('default');
                await new Promise((resolve) => setTimeout(resolve, 100));
                assertEquals(pair.find(2, 'event').length, pushes, 'no pushes after unwatch');
                pair.close();
                await rejectsWith(() => host.client.registerKey(bob), 'is not running', 'a stopped host');
            } finally {
                await app.close();
                mesh.close();
            }
        },
    },
    {
        name: '[RHOST08] a missing param holds the deploy; deploy({ params }) supplies it; project() fills the target without starting',
        invoke: async () => {
            const { v100, v110 } = await withParam();
            const mesh = new SharedMesh();
            const platform = await MemPlatform.withKey([v100], mesh);
            const first = await Rhost.open(appConfig(), platform);
            await first.create(undefined, hostSetup());
            await first.close();

            platform.releases = [v110];
            const app = await Rhost.open(appConfig(), platform);
            try {
                const host = await app.host('default');
                const reason = "1.1.0 needs a value for :moderator (identity), which params don't set";
                const [report] = await app.update();
                assertEquals(report!.notDeployed, reason, 'update holds, and says why');
                assertEquals(JSON.stringify(report!.missingParams), '[{"name":"moderator","type":"identity"}]', 'naming the param and its type');
                const status = await host.status();
                assertTrue(status.hostBehind, 'the host is behind');
                assertEquals(status.notDeployed, reason, 'its status says why');
                assertEquals(status.missingParams?.[0]?.name, 'moderator', 'and which param');

                await app.start('default');
                assertTrue(host.running, 'start does not fail on the hold');
                assertEquals(await deployedOf(host), '1.0.0', 'the host runs the old release');
                await app.stop('default');

                await rejectsWith(() => host.deploy(), reason, 'deploy without the param');
                assertEquals((await host.paramNeeds()).missing.map((p) => p.name).join(','), 'moderator', 'paramNeeds lists it');
                assertEquals((await host.paramNeeds({ params: { moderator: '$me' } })).missing.length, 0, 'and nothing once given');
                const deployed = await host.deploy({ params: { moderator: '$me' } });
                assertEquals(deployed.deployed.version, '1.1.0', 'the release deploys with the param given');
                const params = await host.db.getParams();
                const moderator = params['moderator'];
                assertTrue(moderator !== undefined && 'identity' in moderator
                    && moderator.identity.keyId === platform.vault.resolvePublic('me').keyId,
                'the new param took $me');
                assertFalse((await host.status()).hostBehind, 'the host caught up');

                assertFalse(host.running, 'the host is stopped');
                assertFalse(platform.locks.has('default'), 'no lock is held');
                await host.project();
                assertFalse(host.running, 'project() does not start the host');
                assertFalse(platform.locks.has('default'), 'and releases the lock');
                assertEquals(host.sync, undefined, 'no sync was started');
                const identities = rowsOf(platform.targets.get('default'), 'user_identities');
                assertEquals(identities.length, 1, 'the genesis row is projected');
                assertTrue(platform.targets.get('default')!.hasTable('doc_pages'), 'the doc tables are there');
            } finally {
                await app.close();
                mesh.close();
            }
        },
    },
    {
        name: '[RHOST09] projection paths: relative, clear of rhost\'s own files; mounts with identifier names that nest in nothing',
        invoke: async () => {
            const withMounts = (files: unknown, path = 'data.sqlite') => parseAppConfig({ ...appConfig(), projection: { path, files } });
            const parsed = withMounts([{ name: 'media', path: 'media' }, { name: 'attachments', path: 'shared/attachments' }]);
            assertEquals(parsed.projection.files?.map((f) => `${f.name}@${f.path}`).join(','), 'media@media,attachments@shared/attachments', 'mounts parse');
            assertEquals(effectiveConfig(parsed, parseHostRecord(copy(RECORD))).projection.files?.length, 2, 'and carry into the effective config');

            throwsWith(() => withMounts('media'), 'projection.files must be an array', 'not an array');
            throwsWith(() => withMounts([{ name: 'media' }]), 'projection.files[0].path is missing', 'a missing path');
            throwsWith(() => withMounts([{ name: 'media', path: 'm', extra: 1 }]), 'projection.files[0].extra is not a known field', 'an unknown field');
            throwsWith(() => withMounts([{ name: 'my-media', path: 'm' }]), 'projection.files[0].name must be an identifier', 'a non-identifier name');
            throwsWith(() => withMounts([{ name: 'a', path: 'x' }, { name: 'a', path: 'y' }]), "'a' is already mounted by projection.files[0]", 'a duplicate name');
            throwsWith(() => withMounts([{ name: 'a', path: '/abs' }]), 'must be a relative path', 'an absolute path');
            throwsWith(() => withMounts([{ name: 'a', path: 'C:/files' }]), 'must be a relative path', 'a drive path');
            throwsWith(() => withMounts([{ name: 'a', path: '../out' }]), "must not have empty, '.' or '..' segments", 'a parent segment');
            throwsWith(() => withMounts([{ name: 'a', path: 'x' }, { name: 'b', path: 'x' }]), "'x' overlaps projection.files[0].path 'x'", 'the same path twice');
            throwsWith(() => withMounts([{ name: 'a', path: 'x' }, { name: 'b', path: 'x/y' }]), "'x/y' overlaps projection.files[0].path 'x'", 'a nested path');
            throwsWith(() => withMounts([{ name: 'a', path: 'data.sqlite' }]), "overlaps the projection path 'data.sqlite'", 'the projection path');
            throwsWith(() => withMounts([{ name: 'a', path: 'rdb' }]), "projection.files[0].path 'rdb' overlaps 'rdb'", 'the replica folder');
            throwsWith(() => withMounts([{ name: 'a', path: 'run/x' }]), "projection.files[0].path 'run/x' overlaps 'run'", 'inside the run folder');
            throwsWith(() => withMounts([{ name: 'a', path: 'host.json' }]), "'host.json' overlaps 'host.json'", 'the host record');
            throwsWith(() => withMounts([], '/tmp/data.sqlite'), 'projection.path must be a relative path', 'an absolute projection path');
            throwsWith(() => withMounts([], '../data.sqlite'), "projection.path must not have empty, '.' or '..' segments", 'a projection path outside the host');
            throwsWith(() => withMounts([], 'run/data.sqlite'), "projection.path 'run/data.sqlite' overlaps 'run'", 'a projection path in the run folder');
            assertEquals(withMounts([{ name: 'media', path: 'files/media' }], 'db/data.sqlite').projection.path, 'db/data.sqlite', 'the default layout parses');
            throwsWith(() => parseHostRecord({
                ...RECORD,
                projection: { path: 'data.sqlite', files: [{ name: 'a', path: '..' }] },
            }), 'projection.files[0].path', 'host.json overrides are checked the same way');
        },
    },
    {
        name: '[RHOST10] two hosts sync a FILES mount: a file dropped in keys/<id>/ on one appears on the other',
        invoke: async () => {
            const { v110 } = await withFiles();
            const mesh = new SharedMesh();
            const platformA = await MemPlatform.withKey([v110], mesh);
            const platformB = await MemPlatform.withKey([v110], mesh);
            const projection = { path: 'data.sqlite', files: [{ name: 'media', path: 'media' }, { name: 'later', path: 'later' }] };
            const appA = await Rhost.open(appConfig({ projection }), platformA);
            const appB = await Rhost.open(appConfig({ projection }), platformB);
            try {
                const a = await appA.create(undefined, hostSetup());
                assertEquals(formatReleases((await a.status()).deployed), '1.1.0', 'A deploys the release with FILES media');
                await appA.start('default');
                const mountsA = (await a.status()).files ?? [];
                assertEquals(mountsA.map((m) => `${m.name}:${m.state}`).join(','), 'media:mounted,later:pending', 'media mounts; later waits for its FILES');
                assertTrue(mountsA[0]!.writable === true, "A's key can write");

                const b = await appB.join(a.db.getId(), undefined, hostSetup());
                await appB.start('default');

                const adminKey = platformA.vault.resolvePublic('me').keyId;
                const idA = await platformA.targets.get('default')!.idForKeyHash('', adminKey);
                const folderA = platformA.folders.get('default/media')!;
                await folderA.writeText(`keys/${idA}/hello.txt`, 'hello from A');
                await folderA.writeText('common/shared.txt', 'for everyone');

                const onB = async (at: (idB: number) => string, expected: string) => {
                    const idB = await platformB.targets.get('default')?.idForKeyHash('', adminKey);
                    const folderB = platformB.folders.get('default/media');
                    return idB !== undefined && folderB !== undefined && await folderB.readText(at(idB)) === expected;
                };
                await waitFor(() => onB((idB) => `keys/${idB}/hello.txt`, 'hello from A'), "A's key file reaches B's folder, under A's id there");
                await waitFor(() => onB(() => 'common/shared.txt', 'for everyone'), "A's common file reaches B");

                const mountsB = (await b.status()).files ?? [];
                const media = mountsB.find((m) => m.name === 'media')!;
                assertEquals(`${media.state} ${media.files} ${media.missing}`, 'mounted 2 0', 'B mounts both files');
                assertTrue(media.writable === false, "B's key is not a writer");
            } finally {
                await appB.close();
                await appA.close();
                mesh.close();
            }
        },
    },
    {
        name: '[RHOST11] each host signs with its own key, found by id: relabeling it changes nothing, and a missing one is named',
        invoke: async () => {
            const { v100 } = await editor();
            const mesh = new SharedMesh();
            const platform = await MemPlatform.withKey([v100], mesh);
            const other = await platform.vault.create('other', 'pw');
            const app = await Rhost.open(appConfig(), platform);
            try {
                const mine = await app.create(undefined, hostSetup());
                const second = await app.create('second', hostSetup({ key: `#${other.keyId.slice(0, 8)}`, passphrase: 'env:OTHER_PASS' }));
                assertEquals(second.record.key.label, 'other', 'a #prefix finds the key, and host.json records its label');
                assertEquals(second.record.key.passphrase, 'env:OTHER_PASS', 'and the passphrase source given');
                assertTrue(second.db.isCreator(other.keyId) && !second.db.isCreator(mine.record.key.keyId), 'its database has its own creator');
            } finally {
                await app.close();
            }

            platform.vaultView = new RelabeledVault(platform.vault, 'me', 'renamed');
            const relabeled = await Rhost.open(appConfig(), platform);
            try {
                await relabeled.start('default');
                const host = await relabeled.host('default');
                assertTrue(host.running, 'the host unlocks its key by id after a relabel');
                assertEquals((await host.client.me()).label, 'me', 'me() reports the label host.json recorded');
            } finally {
                await relabeled.close();
            }

            platform.vaultView = new MemoryKeyVault();
            const empty = await Rhost.open(appConfig(), platform);
            try {
                const host = await empty.host('default');
                const short = host.record.key.keyId.slice(0, 8);
                await rejectsWith(() => empty.start('default'), `host 'default' signs with the key 'me' (#${short}), which isn't in the test vault`,
                    'a missing key names the host, the label, the id and the keystore');
                assertFalse(host.running, 'the host did not start');
                assertEquals(formatReleases((await host.status()).deployed), '1.0.0', 'its status needs no key');
            } finally {
                await empty.close();
                mesh.close();
            }
        },
    },
    {
        name: '[RHOST12] every name in params must be a param of the release: create, update and deploy refuse others',
        invoke: async () => {
            const { v100, v110 } = await editor();
            const mesh = new SharedMesh();
            const platform = await MemPlatform.withKey([v100], mesh);
            const first = await Rhost.open(appConfig(), platform);
            await first.create(undefined, hostSetup());
            await first.close();

            const extra = await Rhost.open(appConfig({ params: { admin: '$me', color: 'red' } }), platform);
            try {
                await rejectsWith(() => extra.create('other', hostSetup()), "declares no param 'color'", 'create');
                await rejectsWith(() => extra.update('default'), '1.0.0 declares no param :color', 'update, with nothing to deploy');
            } finally {
                await extra.close();
            }

            platform.releases = [v110];
            const behind = await Rhost.open(appConfig({ params: { admin: '$me', color: 'red' } }), platform);
            try {
                const host = await behind.host('default');
                await rejectsWith(() => host.deploy(), '1.1.0 declares no param :color', 'deploy, with no new param needed');
                assertEquals((await host.status()).notDeployed, '1.1.0 declares no param :color', 'status says why the host is behind');
                await rejectsWith(() => behind.start('default'), '1.1.0 declares no param :color', 'start runs update');
            } finally {
                await behind.close();
            }

            const app = await Rhost.open(appConfig({ autoDeploy: 'none' }), platform);
            try {
                const host = await app.host('default');
                await rejectsWith(() => host.deploy({ params: { color: 'red' } }), '1.1.0 declares no param :color', 'deploy params are checked too');
                await rejectsWith(() => host.deploy({ params: { admin: '$me' } }), 'this database already has :admin', 'a param is set once');
                assertEquals((await host.deploy()).deployed.version, '1.1.0', 'with the params right, it deploys');
            } finally {
                await app.close();
                mesh.close();
            }
        },
    },
    {
        name: '[RHOST13] each host has its own network settings; two hosts of an app cannot listen on one port',
        invoke: async () => {
            const { v100 } = await editor();
            const mesh = new SharedMesh();
            const platform = await MemPlatform.withKey([v100], mesh);
            const app = await Rhost.open(appConfig({ allow: ['user.identities.keyId'] }), platform);
            try {
                const internet = (listen?: string) => hostSetup({
                    sync: { scope: 'internet', tracker: 'wss://tracker.example', ...(listen !== undefined ? { listen } : {}) },
                });
                const a = await app.create('a', internet('ws://0.0.0.0:7400'));
                assertEquals(a.config.sync.tracker, 'wss://tracker.example', 'the host has its own tracker');
                assertEquals(a.config.allow.length, 1, "and keeps the app's allow list");
                await rejectsWith(() => app.create('b', internet('ws://127.0.0.1:7400')),
                    "host 'b': port 7400 (sync.listen) is already used by host 'a'", 'a second host on the same port');
                await app.create('b', internet('ws://0.0.0.0:7401'));
                await app.create('c', internet());
                await app.create('d', internet('ws://0.0.0.0'));
                await rejectsWith(() => app.join(a.db.getId(), 'e', internet('ws://0.0.0.0:7401')),
                    "host 'e': port 7401 (sync.listen) is already used by host 'b'", 'join checks it too');
                assertEquals((await app.hosts()).join(','), 'a,b,c,d', 'hosts without a port never clash');
            } finally {
                await app.close();
                mesh.close();
            }
        },
    },
];

async function main() {
    const filters = process.argv.slice(2);

    console.log('Running tests for Hyper Hyper Space v3 rhost module'
        + (filters.length > 0 ? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');

    console.log('rhost');

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

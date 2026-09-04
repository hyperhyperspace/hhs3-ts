import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    MemoryKeyVault,
    openMemWorkspace,
} from "@hyper-hyper-space/hhs3_rdb_runtime";
import {
    MemoryTarget,
    type BidirectionalTarget,
    type CapturedBatch,
    type IngestSettle,
    type OpEvent,
    type RowAction,
    type SchemaAction,
    type StoredOpEvent,
    type SyncMapping,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import {
    KEM_X25519_HKDF,
    SIGNING_ED25519,
} from "@hyper-hyper-space/hhs3_crypto";
import {
    Mesh,
    MemTransportProvider,
    StaticDiscovery,
    createAuthenticator,
    type NetworkAddress,
    type PeerDiscovery,
    type PeerInfo,
    type TopicId,
    type Transport,
    type TransportProvider,
} from "@hyper-hyper-space/hhs3_mesh";
import {
    ReplSession,
    formatRows,
    promptForSession,
    renderStatementOutput,
    runCommand,
    runLanguageText,
    stopAllProjections,
    stopAllSyncs,
    type SyncMeshFactory,
} from "../src/index.js";
import { runProjectParseTests } from "./project_parse_tests.js";
import { runSyncAuthorizerTests, runSyncParseTests } from "./sync_parse_tests.js";

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

function startedProjectId(output: string): number {
    const match = /started projection (\d+)/.exec(output);
    assert(match !== null, `expected started projection id (got: ${JSON.stringify(output)})`);
    return Number(match[1]);
}

function assertEqual(actual: unknown, expected: unknown, message: string): void {
    assert(actual === expected, `${message}: expected ${String(expected)}, got ${String(actual)}`);
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<boolean> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (predicate()) return true;
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
    return predicate();
}

// A BidirectionalTarget wrapping a MemoryTarget whose apply() throws from a
// chosen call onward. Delegates everything else, so the projection's ingest /
// checkpoint paths behave normally; only materialization fails. Used to prove a
// thrown failure during initial vs reactive sync reaches session.onProjectionError.
class FlakyTarget implements BidirectionalTarget {
    private applyCalls = 0;
    constructor(private readonly inner: MemoryTarget, private readonly failFromCall: number) {}

    async apply(
        groupId: B64Hash, schemaActions: SchemaAction[], rowActions: RowAction[],
        checkpoint: Version, events?: OpEvent[],
    ): Promise<void> {
        this.applyCalls += 1;
        if (this.applyCalls >= this.failFromCall) {
            throw new Error(`simulated apply failure (call ${this.applyCalls})`);
        }
        await this.inner.apply(groupId, schemaActions, rowActions, checkpoint, events);
    }

    getCheckpoint(groupId: B64Hash): Promise<Version | undefined> {
        return this.inner.getCheckpoint(groupId);
    }

    drainChanges(): Promise<CapturedBatch> {
        return this.inner.drainChanges();
    }

    resolveRow(table: string, localId: number): Promise<SyncMapping | undefined> {
        return this.inner.resolveRow(table, localId);
    }

    reserveMint(reservations: SyncMapping[]): Promise<void> {
        return this.inner.reserveMint(reservations);
    }

    commitIngest(settle: IngestSettle): Promise<void> {
        return this.inner.commitIngest(settle);
    }

    drainOpEvents(sinceId?: number): Promise<StoredOpEvent[]> {
        return this.inner.drainOpEvents(sinceId);
    }
}

// Exercises the new projection-notice wiring end to end through the portable
// command surface: reactive throws -> onProjectionError, a failed initial
// materialization -> failed \project start, and ingest rejections ->
// "projection warning:" (hook + command output). Self-contained workspace.
async function runProjectionNoticeTests(): Promise<void> {
    const workspace = await openMemWorkspace();
    const keyVault = new MemoryKeyVault(workspace.replica.getHashSuite());
    const session = new ReplSession({ workspace, keyVault });
    session.enableReplDefaults();

    const notices: string[] = [];
    session.onProjectionError = (message) => { notices.push(message); };

    try {
        await runCommand(session, '\\key create alice', undefined, { requestPassphrase: async () => 'correct horse' });
        await runCommand(session, '\\author alice');

        // --- Scenario A: a reactive sync throw reaches onProjectionError ---
        const setupA = await runCommand(session, `
CREATE SCHEMA flaky_a CREATORS ($me) AS ( TABLE items (label string) ALLOW all IF true );
CREATE TABLEGROUP flaky_a_g USING SCHEMA flaky_a;
CREATE DATABASE flaky_a_db;
ADD TABLEGROUP flaky_a_g TO flaky_a_db;
`);
        assertEqual(setupA.exitCode, 0, 'scenario A setup');

        // Succeed the initial materialization (call 1), fail every reactive apply.
        session.projectionTargetFactory = async () => new FlakyTarget(new MemoryTarget({ captureChanges: true }), 2);
        const startA = await runCommand(session, '\\project start flaky_a_db as alice to :memory:');
        assert(startA.exitCode === 0 && startA.output.includes('started projection'), 'scenario A start succeeds');
        const idA = startedProjectId(startA.output);

        const beforeA = notices.length;
        const insertA = await runCommand(session, "INSERT INTO flaky_a_g.items (label) VALUES ('x');");
        assertEqual(insertA.exitCode, 0, 'scenario A insert accepted by rdb');
        const gotErrorA = await waitFor(() => notices.slice(beforeA).some((m) => m.startsWith('projection error:')));
        assert(gotErrorA, `scenario A reactive throw surfaced (got: ${JSON.stringify(notices)})`);

        const statusA = await runCommand(session, '\\project status');
        assert(statusA.output.includes('simulated apply failure'), 'scenario A error shows on \\project status');
        await runCommand(session, `\\project stop ${idA}`);

        // --- Scenario B: a throw during the initial materialization fails start ---
        const setupB = await runCommand(session, `
CREATE SCHEMA flaky_b CREATORS ($me) AS ( TABLE items (label string) ALLOW all IF true );
CREATE TABLEGROUP flaky_b_g USING SCHEMA flaky_b;
CREATE DATABASE flaky_b_db;
ADD TABLEGROUP flaky_b_g TO flaky_b_db;
`);
        assertEqual(setupB.exitCode, 0, 'scenario B setup');

        // Fail the very first apply (the awaited initial materialization).
        session.projectionTargetFactory = async () => new FlakyTarget(new MemoryTarget({ captureChanges: true }), 1);
        const startB = await runCommand(session, '\\project start flaky_b_db as alice to :memory:');
        assert(startB.exitCode !== 0, 'scenario B start fails when initial materialization throws');
        assert(startB.output.includes('simulated apply failure'), 'scenario B start reports the real error');
        const statusB = await runCommand(session, '\\project status');
        assertEqual(statusB.output, '(no active projections)', 'scenario B leaves no active projection');

        // --- Scenario C: an ingest rejection surfaces as a projection warning ---
        const setupC = await runCommand(session, `
CREATE SCHEMA blog CREATORS ($me) AS (
  TABLE posts (title string) ALLOW all IF true,
  TABLE comments (body string, post string REFERENCES posts) ALLOW all IF true
);
CREATE TABLEGROUP blog_g USING SCHEMA blog;
CREATE DATABASE blog_db;
ADD TABLEGROUP blog_g TO blog_db;
`);
        assertEqual(setupC.exitCode, 0, 'scenario C setup');

        let captured: MemoryTarget | undefined;
        session.projectionTargetFactory = async () => { captured = new MemoryTarget({ captureChanges: true }); return captured; };
        const startC = await runCommand(session, '\\project start blog_db as alice to :memory:');
        assert(startC.exitCode === 0 && captured !== undefined, `scenario C start succeeds (${startC.output})`);
        const idC = startedProjectId(startC.output);
        // Tables are group-qualified in the shared projection (`<group>_<table>`).
        const commentsTable = captured!.listTables().find((t) => t.endsWith('_comments'));
        assert(commentsTable !== undefined, `scenario C materialized comments (tables: ${captured!.listTables().join(', ')})`);

        // A local edit that inserts a comment pointing at a non-existent post
        // (dangling FK): ingestion rejects it (never throws). The MemoryTarget
        // fires its change signal inline, driving one reactive cycle.
        const beforeC = notices.length;
        captured!.localInsert(commentsTable, { body: 'orphan', post_id: 99 });
        const gotWarningC = await waitFor(() => notices.slice(beforeC).some((m) => m.startsWith('projection warning:')));
        assert(gotWarningC, `scenario C ingest rejection surfaced as a warning (got: ${JSON.stringify(notices)})`);

        // The command-output path: a fresh dangling insert drained by an
        // immediate \project update (which wins the db lock before the 50ms
        // debounced reactive cycle can consume the outbox).
        captured!.localInsert(commentsTable, { body: 'orphan2', post_id: 98 });
        const syncC = await runCommand(session, `\\project update ${idC}`);
        assert(syncC.exitCode === 0, 'scenario C explicit update succeeds despite rejects');
        assert(syncC.output.includes('projection warning:'), `scenario C update output includes the reject warning (got: ${JSON.stringify(syncC.output)})`);
        await runCommand(session, `\\project stop ${idC}`);
    } finally {
        await stopAllProjections(session);
        await stopAllSyncs(session);
        await workspace.close();
    }
}

async function main(): Promise<void> {
    const workspace = await openMemWorkspace();
    const keyVault = new MemoryKeyVault(workspace.replica.getHashSuite());
    const session = new ReplSession({
        workspace,
        keyVault,
        projectionTargetFactory: async () => new MemoryTarget({ captureChanges: true }),
    });

    try {
        assertEqual(session.hashWidth, 'auto', 'portable default hash width');
        assertEqual(session.hashLabels, false, 'portable default hash labels');
        assertEqual(session.refAutoUpdate, 'off', 'portable default ref auto update');
        assertEqual(promptForSession(session), 'rdb:-:-> ', 'portable prompt');
        assert(formatRows([{ a: 1 }]).includes('a'), 'portable row formatter');

        session.enableReplDefaults();
        assertEqual(session.hashLabels, true, 'portable REPL labels default');
        assertEqual(session.refAutoUpdate, 'auto', 'portable REPL ref update default');
        session.enableScriptDefaults();
        assertEqual(session.hashWidth, 'full', 'portable script hash width default');
        assertEqual(session.refAutoUpdate, 'off', 'portable script ref update default');

        const pending = await runCommand(session, '\\key create alice');
        assertEqual(pending.exitCode, 1, 'missing passphrase should be structured failure');
        assertEqual(pending.output, 'passphrase required', 'missing passphrase message excludes secret');
        assertEqual(pending.needsPassphrase?.kind, 'create', 'create passphrase need kind');
        assertEqual(pending.needsPassphrase?.label, 'alice', 'create passphrase need label');

        const created = await runCommand(session, '\\key create alice', undefined, {
            requestPassphrase: async () => 'correct horse',
        });
        assert(created.exitCode === 0 && created.output.includes('created alice'), 'interactive key creation');
        assert(!created.output.includes('correct horse'), 'passphrase excluded from output');

        const author = await runCommand(session, '\\author alice');
        assertEqual(author.output, 'author alice', 'author selection');

        const setup = await runCommand(session, `
CREATE SCHEMA shop CREATORS ($me) AS (
  TABLE products (
    sku string PUB READONLY,
    name string
  )
);
CREATE TABLEGROUP shop_prod USING SCHEMA shop;
INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');
SELECT sku, name FROM shop_prod.products;
`);
        assert(setup.exitCode === 0 && setup.output.includes('Widget'), 'portable table rendering');
        assertEqual(promptForSession(session), 'rdb:shop_prod:alice> ', 'canonical prompt labels');

        const progress: string[] = [];
        const streamed = await runCommand(session, 'SELECT sku, name FROM shop_prod.products;', undefined, {
            auth: { onProgress: (line) => progress.push(line) },
        });
        assert(progress.some((line) => line.includes('Widget')), 'streamed main output');
        assertEqual(streamed.output, '', 'streamed main output is not rendered twice');

        const streamedRun = await runLanguageText(session, 'SELECT sku, name FROM shop_prod.products;', {
            onProgress: () => undefined,
        });
        const streamedItem = streamedRun.results[0]!;
        streamedItem.mainStreamed = false;
        streamedItem.notices = ['already streamed notice'];
        streamedItem.noticesStreamed = true;
        const unstreamedOnly = renderStatementOutput(session, streamedItem);
        assert(unstreamedOnly.includes('Widget'), 'unstreamed main output remains visible');
        assert(!unstreamedOnly.includes('already streamed notice'), 'streamed notices are not rendered twice');

        const observerSetup = await runCommand(session, `
CREATE SCHEMA users_schema CREATORS ($me) AS (
  TABLE identities (name string) ALLOW all IF true
);
CREATE TABLEGROUP users USING SCHEMA users_schema;
CREATE SCHEMA observer_schema CREATORS ($me) AS (
  TABLE orders (
    customer string REFERENCES users.identities,
    label string
  ) ALLOW all IF true
);
CREATE TABLEGROUP observer USING SCHEMA observer_schema BIND users => users;
`);
        assertEqual(observerSetup.exitCode, 0, 'observer setup');
        session.setRefAutoUpdate('auto');
        const orderedProgress: string[] = [];
        const observedInsert = await runCommand(
            session,
            "INSERT INTO users.identities (name) VALUES ('Ada');",
            undefined,
            { auth: { onProgress: (line) => orderedProgress.push(line) } },
        );
        assertEqual(observedInsert.exitCode, 0, 'observed insert');
        const insertIndex = orderedProgress.findIndex((line) => line.startsWith('inserted '));
        const refIndex = orderedProgress.findIndex((line) => line.startsWith('updated ref on observer'));
        assert(insertIndex >= 0, 'insert result was streamed');
        assert(refIndex > insertIndex, 'ref update was streamed after its triggering insert');

        await runCommand(session, '\\output json');
        const selected = await runCommand(session, 'SELECT sku, name FROM shop_prod.products;');
        assert(selected.exitCode === 0 && selected.output.includes('"Widget"'), 'portable JSON rendering');

        await runCommand(session, '\\hash-width 12');
        assertEqual(session.hashWidth, 12, 'hash-width meta command');
        const invalid = await runCommand(session, 'SELECT FROM;');
        assert(invalid.exitCode === 2 && invalid.output.length > 0, 'portable diagnostics');

        await runCommand(session, '\\output table');

        // --- \project meta command ---
        const noProj = await runCommand(session, '\\project status');
        assertEqual(noProj.output, '(no active projections)', 'projection status with none active');

        const dbSetup = await runCommand(session, `
CREATE DATABASE shopdb;
ADD TABLEGROUP shop_prod TO shopdb;
`);
        assertEqual(dbSetup.exitCode, 0, 'database + membership setup');

        const missingTo = await runCommand(session, '\\project start shopdb as alice');
        assert(missingTo.exitCode === 1 && missingTo.output.includes("Expected 'to'"), 'start without to is refused');

        const started = await runCommand(session, '\\project start shopdb as alice to :memory:');
        assert(started.exitCode === 0 && started.output.includes('started projection 1'), 'projection start');
        assert(started.output.includes('as alice'), 'projection start names the local id');
        assert(started.output.includes('to :memory:'), 'projection start names the destination');

        const projStatus = await runCommand(session, '\\project status');
        assert(projStatus.exitCode === 0 && projStatus.output.includes('shopdb'), 'projection status lists the projection');
        assert(projStatus.output.includes('alice') && projStatus.output.includes(':memory:'), 'projection status shows as and to');

        const synced = await runCommand(session, '\\project update 1');
        assert(synced.exitCode === 0 && synced.output.includes('updated projection 1'), 'projection update');

        const dup = await runCommand(session, '\\project start shopdb as alice to :memory:');
        assert(dup.exitCode === 1 && dup.output.includes('already projecting'), 'starting a second projection is refused');

        const stopByName = await runCommand(session, '\\project stop shopdb');
        assert(stopByName.exitCode === 1 && stopByName.output.includes('numeric session id'), 'stop by database name is refused');

        const stopped = await runCommand(session, '\\project stop 1');
        assert(stopped.exitCode === 0 && stopped.output.includes('stopped projection 1'), 'projection stop');
        const afterStop = await runCommand(session, '\\project status');
        assertEqual(afterStop.output, '(no active projections)', 'projection removed after stop');

        const reused = await runCommand(session, '\\project start shopdb as alice to :memory:');
        assert(reused.exitCode === 0 && reused.output.includes('started projection 2'), 'ids are never reused after stop');
        await runCommand(session, '\\project stop 2');

        const quit = await runCommand(session, '\\quit');
        assert(quit.quit === true, 'portable quit outcome');

        await runSyncCommandTests(session);
    } finally {
        await stopAllProjections(session);
        await stopAllSyncs(session);
        await workspace.close();
    }

    await runProjectionNoticeTests();
    await runSyncFetchNonDefaultBackendTest();
    await runSyncParseTests();
    await runProjectParseTests();
    await runSyncAuthorizerTests();
}

function memSyncFactory(): SyncMeshFactory {
    return async (req) => {
        const provider = new MemTransportProvider();
        const addr = `mem://sync-${req.identity.keyId.slice(0, 12)}`;
        const discovery = new StaticDiscovery([], []);
        const mesh = new Mesh({
            transports: [provider],
            discovery,
            authenticator: createAuthenticator({
                localKey: req.identity,
                signingName: SIGNING_ED25519,
                kemPrefs: [KEM_X25519_HKDF],
            }),
            localKeyId: req.identity.keyId,
            listenAddresses: [addr],
        });
        return {
            mesh,
            discovery,
            listenAddresses: [addr],
            discoveryNotes: ['mem'],
            closeables: [],
        };
    };
}

async function runSyncCommandTests(session: ReplSession): Promise<void> {
    const missingFactory = await runCommand(session, '\\sync start shopdb as alice on localhost');
    assert(missingFactory.exitCode === 1 && missingFactory.output.includes('No sync mesh factory'), 'sync start without factory');

    session.syncMeshFactory = memSyncFactory();

    const started = await runCommand(session, '\\sync start shopdb as alice on localhost');
    assert(started.exitCode === 0 && started.output.includes('started sync 1'), `sync start (${started.output})`);
    assert(started.output.includes('as alice'), 'sync start names the local id');

    const dup = await runCommand(session, '\\sync start shopdb as alice on localhost');
    assert(dup.exitCode === 1 && dup.output.includes('already syncing'), 'second start on the same db is refused');

    const st = await runCommand(session, '\\sync status');
    assert(st.exitCode === 0 && st.output.includes('shopdb') && st.output.includes('everyone'), 'sync status lists everyone');

    const filtered = await runCommand(session, '\\sync status shopdb');
    assert(filtered.exitCode === 0 && filtered.output.includes('shopdb'), 'sync status can filter by db');

    const listed = await runCommand(session, '\\sync peers 1');
    assert(listed.exitCode === 0 && listed.output.includes('no peers'), 'sync peers with none connected');

    const stopped = await runCommand(session, '\\sync stop 1');
    assert(stopped.exitCode === 0 && stopped.output.includes('stopped sync 1'), 'sync stop');
    const after = await runCommand(session, '\\sync status');
    assertEqual(after.output, '(no active syncs)', 'sync removed after stop');

    const reused = await runCommand(session, '\\sync start shopdb as alice on localhost');
    assert(reused.exitCode === 0 && reused.output.includes('started sync 2'), 'ids are never reused after stop');
    await runCommand(session, '\\sync stop 2');

    const shop = session.workspace.roots.list('database').find((root) => root.name === 'shopdb');
    assert(shop !== undefined, 'shopdb is a local root for fetch tests');
    const fetched = await runCommand(session, `\\sync fetch #${shop.id} as alice on localhost`);
    assert(fetched.exitCode === 0, `already-local fetch (${fetched.output})`);
    assert(fetched.output.includes('fetched'), 'fetch output mentions fetched');
    assert(fetched.output.includes('already local') || fetched.output.includes('genesis only'), 'fetch notes genesis / already local');
    assert(fetched.output.includes('\\sync start'), 'fetch points at start to share');

    const afterFetch = await runCommand(session, '\\sync status');
    assertEqual(afterFetch.output, '(no active syncs)', 'fetch does not create a sync session');

    const named = await runCommand(session, '\\sync fetch shopdb as alice on localhost');
    assert(named.exitCode === 1 && named.output.includes('full #hash'), 'fetch rejects a database name');
}

// Shared mem mesh so two REPL sessions can actually find each other. Mesh.close()
// shuts its TransportProviders, so each mesh gets a wrapper that does not close
// the shared inner provider.
class SharedPeerRegistry {
    private readonly topics = new Map<TopicId, Map<string, PeerInfo>>();

    announce(topic: TopicId, self: PeerInfo): void {
        let peers = this.topics.get(topic);
        if (peers === undefined) {
            peers = new Map();
            this.topics.set(topic, peers);
        }
        peers.set(self.keyId, self);
    }

    leave(topic: TopicId, keyId: string): void {
        this.topics.get(topic)?.delete(keyId);
    }

    list(topic: TopicId): PeerInfo[] {
        return [...(this.topics.get(topic)?.values() ?? [])];
    }
}

class NonClosingProvider implements TransportProvider {
    readonly scheme: string;
    constructor(private readonly inner: TransportProvider) {
        this.scheme = inner.scheme;
    }
    listen(address: NetworkAddress, onConnection: (transport: Transport) => void): Promise<void> {
        return this.inner.listen(address, onConnection);
    }
    connect(remote: NetworkAddress, local?: NetworkAddress): Promise<Transport> {
        return this.inner.connect(remote, local);
    }
    close(): void {}
}

function registryDiscovery(registry: SharedPeerRegistry, selfKeyId: string): PeerDiscovery {
    return {
        async *discover(topic: TopicId, schemes?: string[]): AsyncIterable<PeerInfo> {
            for (const peer of registry.list(topic)) {
                if (peer.keyId === selfKeyId) continue;
                const addresses = schemes === undefined || schemes.length === 0
                    ? peer.addresses
                    : peer.addresses.filter((addr) => schemes.some((s) => addr.startsWith(`${s}://`)));
                if (addresses.length === 0) continue;
                yield { keyId: peer.keyId, addresses };
            }
        },
        announce(topic, self) {
            registry.announce(topic, self);
            return Promise.resolve();
        },
        leave(topic, self) {
            registry.leave(topic, self);
            return Promise.resolve();
        },
    };
}

function sharedMemSyncFactory(provider: MemTransportProvider, registry: SharedPeerRegistry): SyncMeshFactory {
    return async (req) => {
        const addr = `mem://sync-${req.identity.keyId.slice(0, 12)}`;
        const discovery = registryDiscovery(registry, req.identity.keyId);
        const mesh = new Mesh({
            transports: [new NonClosingProvider(provider)],
            discovery,
            authenticator: createAuthenticator({
                localKey: req.identity,
                signingName: SIGNING_ED25519,
                kemPrefs: [KEM_X25519_HKDF],
            }),
            localKeyId: req.identity.keyId,
            listenAddresses: [addr],
        });
        return {
            mesh,
            discovery,
            listenAddresses: [addr],
            discoveryNotes: ['mem-shared'],
            closeables: [],
        };
    };
}

async function runSyncFetchNonDefaultBackendTest(): Promise<void> {
    const provider = new MemTransportProvider();
    const registry = new SharedPeerRegistry();
    const factory = sharedMemSyncFactory(provider, registry);

    const aliceWs = await openMemWorkspace();
    const alice = new ReplSession({
        workspace: aliceWs,
        keyVault: new MemoryKeyVault(aliceWs.replica.getHashSuite()),
        syncMeshFactory: factory,
    });
    alice.enableReplDefaults();

    const bobWs = await openMemWorkspace({ backendLabel: 'rdb-web' });
    const bob = new ReplSession({
        workspace: bobWs,
        keyVault: new MemoryKeyVault(bobWs.replica.getHashSuite()),
        syncMeshFactory: factory,
    });
    bob.enableReplDefaults();

    const passphrase = async () => 'correct horse';
    try {
        const createdAlice = await runCommand(alice, '\\key create alice', undefined, { requestPassphrase: passphrase });
        assertEqual(createdAlice.exitCode, 0, 'alice key');
        await runCommand(alice, '\\author alice');

        const setup = await runCommand(alice, `
CREATE SCHEMA shop CREATORS ($me) AS (
  TABLE products (sku string PUB READONLY, name string)
);
CREATE TABLEGROUP shop_prod USING SCHEMA shop;
CREATE DATABASE shopdb;
ADD TABLEGROUP shop_prod TO shopdb;
`);
        assertEqual(setup.exitCode, 0, `alice shopdb (${setup.output})`);

        const started = await runCommand(alice, '\\sync start shopdb as alice on localhost');
        assert(started.exitCode === 0, `alice sync start (${started.output})`);

        const shop = alice.workspace.roots.list('database').find((root) => root.name === 'shopdb');
        assert(shop !== undefined, 'shopdb is a local root on alice');

        const createdBob = await runCommand(bob, '\\key create bob', undefined, { requestPassphrase: passphrase });
        assertEqual(createdBob.exitCode, 0, 'bob key');

        const fetched = await runCommand(bob, `\\sync fetch #${shop.id} as bob on localhost`);
        assert(fetched.exitCode === 0, `remote fetch (${fetched.output})`);
        assert(!fetched.output.includes('No backend attached'), `must not fail on backend label (${fetched.output})`);
        assert(fetched.output.includes('fetched'), 'fetch output mentions fetched');
        assert(fetched.output.includes('genesis only'), 'fetch notes genesis only');

        const obj = await bob.workspace.replica.getObject(shop.id);
        assert(obj !== undefined, 'fetched object is present on bob');
        assertEqual(obj.getBackendLabel(), 'rdb-web', 'stored on the web-like backend');
    } finally {
        await stopAllSyncs(alice);
        await stopAllSyncs(bob);
        await aliceWs.close();
        await bobWs.close();
        provider.close();
    }
}

void main();

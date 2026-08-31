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
    ReplSession,
    formatRows,
    promptForSession,
    renderStatementOutput,
    runCommand,
    runLanguageText,
} from "../src/index.js";

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
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
// materialization -> failed \projection start, and ingest rejections ->
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
        const startA = await runCommand(session, '\\projection start flaky_a_db');
        assert(startA.exitCode === 0 && startA.output.includes('projection started'), 'scenario A start succeeds');

        const beforeA = notices.length;
        const insertA = await runCommand(session, "INSERT INTO flaky_a_g.items (label) VALUES ('x');");
        assertEqual(insertA.exitCode, 0, 'scenario A insert accepted by rdb');
        const gotErrorA = await waitFor(() => notices.slice(beforeA).some((m) => m.startsWith('projection error:')));
        assert(gotErrorA, `scenario A reactive throw surfaced (got: ${JSON.stringify(notices)})`);

        const statusA = await runCommand(session, '\\projection status');
        assert(statusA.output.includes('simulated apply failure'), 'scenario A error shows on \\projection status');
        await runCommand(session, '\\projection stop flaky_a_db');

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
        const startB = await runCommand(session, '\\projection start flaky_b_db');
        assert(startB.exitCode !== 0, 'scenario B start fails when initial materialization throws');
        assert(startB.output.includes('simulated apply failure'), 'scenario B start reports the real error');
        const statusB = await runCommand(session, '\\projection status');
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
        const startC = await runCommand(session, '\\projection start blog_db');
        assert(startC.exitCode === 0 && captured !== undefined, `scenario C start succeeds (${startC.output})`);
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
        // immediate \projection sync (which wins the db lock before the 50ms
        // debounced reactive cycle can consume the outbox).
        captured!.localInsert(commentsTable, { body: 'orphan2', post_id: 98 });
        const syncC = await runCommand(session, '\\projection sync blog_db');
        assert(syncC.exitCode === 0, 'scenario C explicit sync succeeds despite rejects');
        assert(syncC.output.includes('projection warning:'), `scenario C sync output includes the reject warning (got: ${JSON.stringify(syncC.output)})`);
        await runCommand(session, '\\projection stop blog_db');
    } finally {
        for (const projection of session.projections.values()) await projection.stop();
        session.projections.clear();
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

        // --- \projection meta command ---
        const noProj = await runCommand(session, '\\projection status');
        assertEqual(noProj.output, '(no active projections)', 'projection status with none active');

        const dbSetup = await runCommand(session, `
CREATE DATABASE shopdb;
ADD TABLEGROUP shop_prod TO shopdb;
`);
        assertEqual(dbSetup.exitCode, 0, 'database + membership setup');

        const started = await runCommand(session, '\\projection start shopdb');
        assert(started.exitCode === 0 && started.output.includes('projection started'), 'projection start');
        assert(started.output.includes('bidirectional'), 'projection is bidirectional (author selected)');

        const projStatus = await runCommand(session, '\\projection status');
        assert(projStatus.exitCode === 0 && projStatus.output.includes('database'), 'projection status lists the projection');

        const synced = await runCommand(session, '\\projection sync shopdb');
        assert(synced.exitCode === 0 && synced.output.includes('synced'), 'projection sync');

        const dup = await runCommand(session, '\\projection start shopdb');
        assert(dup.exitCode === 1 && dup.output.includes('already running'), 'starting a second projection is refused');

        const stopped = await runCommand(session, '\\projection stop shopdb');
        assert(stopped.exitCode === 0 && stopped.output.includes('stopped'), 'projection stop');
        const afterStop = await runCommand(session, '\\projection status');
        assertEqual(afterStop.output, '(no active projections)', 'projection removed after stop');

        const quit = await runCommand(session, '\\quit');
        assert(quit.quit === true, 'portable quit outcome');
    } finally {
        await workspace.close();
    }

    await runProjectionNoticeTests();
}

void main();

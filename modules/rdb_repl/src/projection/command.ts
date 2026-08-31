// The `\projection` meta command: create and manage a replica-wide relational
// projection of the current (or named) RDb. It is a thin driver over
// rdb_projection's RdbProjection - the host injects the concrete backend via
// session.projectionTargetFactory, so this stays browser-safe and engine-
// agnostic. Subcommands:
//
//   \projection start [<database>] [label]   open + materialize + keep synced
//   \projection sync  [<database>]           force one sync cycle now
//   \projection status                       list active projections + results
//   \projection stop  [<database>]           stop + drop a projection
//   \projection register-key <keyHash> <publicKey> [<database>]
//                                            intern a key into rdb_keys (returns id)
//   \projection resolve-key <id|keyHash> [<database>]
//                                            look up key_hash / public_key / id

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { formatOpVoidDetail } from "@hyper-hyper-space/hhs3_rdb";
import type { RDb } from "@hyper-hyper-space/hhs3_rdb";
import type { IngestResult, OpEvent } from "@hyper-hyper-space/hhs3_rdb_adapter";
import { RdbProjection } from "@hyper-hyper-space/hhs3_rdb_projection";

import { formatDisplayString } from "../format/display.js";
import { formatRows } from "../format/rows.js";
import type { ReplSession } from "../session.js";

function ref(text = '') {
    const span = { start: 0, end: text.length, line: 1, column: 1 };
    return text.startsWith('#')
        ? { kind: 'hash' as const, prefix: text.slice(1), span }
        : { kind: 'name' as const, text, parts: text.split('.'), span };
}

// Resolve the target database (named arg, else the session's current database).
async function resolveDatabase(session: ReplSession, name?: string): Promise<{ id: B64Hash; db: RDb }> {
    const which = name ?? session.currentDatabase;
    if (which === undefined) throw new Error('No current database; use \\projection start <database> or \\use database <name>');
    const isHash = typeof which === 'string' && which.length > 0 && name === undefined;
    const root = await session.workspace.roots.resolveDatabase(
        isHash ? ref('#' + which) : ref(which), { aliases: session.aliases });
    if (root.db === undefined) throw new Error('Database is not loaded');
    return { id: root.id, db: root.db as RDb };
}

function summarizeResults(results: Map<B64Hash, IngestResult>): string {
    let accepted = 0;
    let rejected = 0;
    for (const r of results.values()) { accepted += r.accepted; rejected += r.rejected.length; }
    return `${accepted} accepted, ${rejected} rejected`;
}

// Ingest rejections do not throw; they land in IngestResult.rejected and are
// reconciled by the next projection. Distill them into one user-facing warning
// line (deduped reasons) so a silent divergence is at least visible.
function formatRejectWarning(results: Map<B64Hash, IngestResult>): string | undefined {
    const rejected = [...results.values()].flatMap((r) => r.rejected);
    if (rejected.length === 0) return undefined;
    const reasons = [...new Set(rejected.map((r) => r.reason))];
    return `projection warning: ${rejected.length} rejected: ${reasons.join('; ')}`;
}

// The human reason behind an op-event: the structured void detail for a
// concurrency flip, or the bundle() validation message for an ingestion failure.
function formatEventReason(event: OpEvent): string {
    if (event.reason === undefined) return '';
    return event.reason.source === 'void'
        ? formatOpVoidDetail(event.reason.detail)
        : event.reason.failure.reason;
}

// One op-event as a single line: origin/direction, op kind, the affected row,
// and the reason. Shared by the reactive push (routed through the error hook)
// and the `\projection events` backlog listing.
function formatOpEvent(session: ReplSession, event: OpEvent): string {
    const where = event.table === undefined ? ''
        : ` ${event.table}` + (event.rowId === undefined ? ''
            : `#${formatDisplayString(session, event.rowId, { role: 'hash' })}`);
    const reason = formatEventReason(event);
    return `${event.origin}/${event.direction} ${event.kind}${where}${reason === '' ? '' : ` - ${reason}`}`;
}

export async function runProjectionCommand(session: ReplSession, args: string[]): Promise<string> {
    const [sub, ...rest] = args;

    switch (sub) {
        case 'start': return await start(session, rest[0], rest[1]);
        case 'sync': return await sync(session, rest[0]);
        case 'stop': return await stop(session, rest[0]);
        case 'events': return await events(session, rest[0]);
        case 'register-key': return await registerKey(session, rest);
        case 'resolve-key': return await resolveKey(session, rest);
        case 'status':
        case undefined: return status(session);
        default: throw new Error(
            'Usage: \\projection start|sync|status|stop|events|register-key|resolve-key [<database>] ...');
    }
}

async function start(session: ReplSession, name?: string, label?: string): Promise<string> {
    if (session.projectionTargetFactory === undefined) {
        throw new Error('No projection backend configured for this host');
    }
    const { id, db } = await resolveDatabase(session, name);
    if (session.projections.has(id)) {
        throw new Error(`A projection is already running for database ${formatDisplayString(session, id, { role: 'hash' })}`);
    }

    const target = await session.projectionTargetFactory({ databaseId: id, label });
    const writer = await session.currentAuthor();
    // Reactive cycles run from timers / DAG listeners: thrown failures and ingest
    // rejections would otherwise be swallowed. Route both through the host hook.
    // These callbacks must never throw (they run off the command path).
    const projection = await RdbProjection.open(db, session.workspace.replica, target, {
        writer,
        createUuid: () => session.createUuid(),
        onError: (err) => {
            const message = err instanceof Error ? err.message : String(err);
            session.onProjectionError?.(`projection error: ${message}`);
        },
        onResult: (results) => {
            const warning = formatRejectWarning(results);
            if (warning !== undefined) session.onProjectionError?.(warning);
        },
        // Ingestion failures + p2p concurrency void/reinstate flips. Route each
        // through the same display hook as errors/warnings so the CLI stderr /
        // web transcript shows them; the durable backlog is listable via
        // \projection events.
        onOpEvents: (opEvents) => {
            for (const event of opEvents) {
                session.onProjectionError?.(`op-event: ${formatOpEvent(session, event)}`);
            }
        },
    });
    session.projections.set(id, projection);

    const groups = projection.memberGroupIds();
    const lines = [
        `projection started for database ${formatDisplayString(session, id, { role: 'hash' })}`
        + ` (${writer === undefined ? 'read-only' : 'bidirectional'}, ${groups.length} group(s))`,
    ];
    if (groups.length > 0) {
        lines.push(formatRows(groups.map((g) => ({ group: formatDisplayString(session, g, { role: 'hash' }) }))));
    }
    // The initial materialization runs through sync(), which does not fire
    // onResult; surface any rejections here (command output + hook, so the CLI
    // stderr / web transcript match the reactive style).
    const warning = formatRejectWarning(projection.lastResult());
    if (warning !== undefined) {
        session.onProjectionError?.(warning);
        lines.push(warning);
    }
    return lines.join('\n');
}

async function sync(session: ReplSession, name?: string): Promise<string> {
    const { id } = await resolveDatabase(session, name);
    const projection = session.projections.get(id);
    if (projection === undefined) throw new Error('No projection running for this database; use \\projection start');
    const results = await projection.sync();
    const line = `synced ${formatDisplayString(session, id, { role: 'hash' })}: ${summarizeResults(results)}`;
    // Rejections are not command failures; append them to stdout rather than
    // firing the hook, so the user does not get a duplicate stderr line.
    const warning = formatRejectWarning(results);
    return warning === undefined ? line : `${line}\n${warning}`;
}

async function stop(session: ReplSession, name?: string): Promise<string> {
    const { id } = await resolveDatabase(session, name);
    const projection = session.projections.get(id);
    if (projection === undefined) throw new Error('No projection running for this database');
    await projection.stop();
    session.projections.delete(id);
    return `projection stopped for database ${formatDisplayString(session, id, { role: 'hash' })}`;
}

// List the durable op-event backlog (ingestion failures + concurrency flips),
// oldest first, for the current (or named) database's projection.
async function events(session: ReplSession, name?: string): Promise<string> {
    const projection = await activeProjection(session, name);
    const backlog = await projection.opEvents();
    if (backlog.length === 0) return '(no op-events)';
    return backlog.map((s) => `#${s.id} ${formatOpEvent(session, s.event)}`).join('\n');
}

function status(session: ReplSession): string {
    if (session.projections.size === 0) return '(no active projections)';
    const rows = [...session.projections.entries()].map(([id, projection]) => ({
        database: formatDisplayString(session, id, { role: 'hash' }),
        groups: projection.memberGroupIds().length,
        last: summarizeResults(projection.lastResult()),
        error: projection.lastError() ?? '',
    }));
    return formatRows(rows);
}

async function activeProjection(session: ReplSession, name?: string): Promise<RdbProjection> {
    const { id } = await resolveDatabase(session, name);
    const projection = session.projections.get(id);
    if (projection === undefined) throw new Error('No projection running for this database; use \\projection start');
    return projection;
}

async function registerKey(session: ReplSession, args: string[]): Promise<string> {
    const [keyHash, publicKey, dbName] = args;
    if (keyHash === undefined || publicKey === undefined) {
        throw new Error('Usage: \\projection register-key <keyHash> <publicKey> [<database>]');
    }
    const projection = await activeProjection(session, dbName);
    const id = await projection.registerKey(keyHash, publicKey);
    return `registered key id=${id} key_hash=${formatDisplayString(session, keyHash, { role: 'hash', identity: true })}`;
}

async function resolveKey(session: ReplSession, args: string[]): Promise<string> {
    const [token, dbName] = args;
    if (token === undefined) {
        throw new Error('Usage: \\projection resolve-key <id|keyHash> [<database>]');
    }
    const projection = await activeProjection(session, dbName);
    const asId = Number(token);
    if (Number.isSafeInteger(asId) && String(asId) === token) {
        const keyHash = await projection.keyHashForId(asId);
        if (keyHash === undefined) return `(no key with id ${asId})`;
        const publicKey = await projection.publicKeyForId(asId);
        return formatRows([{
            id: asId,
            key_hash: formatDisplayString(session, keyHash, { role: 'hash', identity: true }),
            public_key: publicKey ?? '(null)',
        }]);
    }
    const id = await projection.idForKeyHash(token);
    if (id === undefined) return `(no key with hash ${formatDisplayString(session, token, { role: 'hash' })})`;
    const publicKey = await projection.publicKeyForId(id);
    return formatRows([{
        id,
        key_hash: formatDisplayString(session, token, { role: 'hash', identity: true }),
        public_key: publicKey ?? '(null)',
    }]);
}

// The `\project` meta command: create and manage a replica-wide relational
// projection of a named RDb. It is a thin driver over rdb_projection's
// RdbProjection - the host injects the concrete backend via
// session.projectionTargetFactory, so this stays browser-safe and engine-
// agnostic. Subcommands:
//
//   \project start <db> as <local-id> to <path>   open + materialize + keep synced
//   \project update <id>                          force one cycle now
//   \project status [<db>]                        list active projections
//   \project stop <id>                            stop a projection session
//   \project events <id>                          durable op-event backlog
//   \project register-key <id> <keyHash> <publicKey>
//   \project resolve-key <id> <token>

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { formatOpVoidDetail } from "@hyper-hyper-space/hhs3_rdb";
import type { RDb } from "@hyper-hyper-space/hhs3_rdb";
import type { IngestResult, OpEvent } from "@hyper-hyper-space/hhs3_rdb_adapter";
import { RdbProjection } from "@hyper-hyper-space/hhs3_rdb_projection";

import { formatDisplayString, formatSessionRows } from "../format/display.js";
import { formatRows } from "../format/rows.js";
import type { ReplSession } from "../session.js";
import {
    parseProjectCommand,
    type ProjectStartCommand,
} from "./parse.js";
import type { ProjectSessionEntry } from "./types.js";

export type ProjectCommandResult = {
    output?: string;
    needsUnlock?: { label: string };
};

function ref(text = '') {
    const span = { start: 0, end: text.length, line: 1, column: 1 };
    return text.startsWith('#')
        ? { kind: 'hash' as const, prefix: text.slice(1), span }
        : { kind: 'name' as const, text, parts: text.split('.'), span };
}

async function resolveDatabase(session: ReplSession, name: string): Promise<{ id: B64Hash; db: RDb; name: string }> {
    const root = await session.workspace.roots.resolveDatabase(ref(name), { aliases: session.aliases });
    if (root.db === undefined) throw new Error('Database is not loaded');
    return { id: root.id, db: root.db as RDb, name };
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

function formatEventReason(event: OpEvent): string {
    if (event.reason === undefined) return '';
    return event.reason.source === 'void'
        ? formatOpVoidDetail(event.reason.detail)
        : event.reason.failure.reason;
}

function formatOpEvent(session: ReplSession, event: OpEvent): string {
    const where = event.table === undefined ? ''
        : ` ${event.table}` + (event.rowId === undefined ? ''
            : `#${formatDisplayString(session, event.rowId, { role: 'hash' })}`);
    const reason = formatEventReason(event);
    return `${event.origin}/${event.direction} ${event.kind}${where}${reason === '' ? '' : ` - ${reason}`}`;
}

function isMemoryPath(path: string): boolean {
    return path === ':memory:';
}

function destinationsCollide(a: string, b: string): boolean {
    if (isMemoryPath(a) || isMemoryPath(b)) return false;
    return a === b;
}

function requireEntry(session: ReplSession, id: number): ProjectSessionEntry {
    const entry = session.projections.get(id);
    if (entry === undefined) throw new Error(`No projection session ${id}`);
    return entry;
}

export async function runProjectCommand(session: ReplSession, remainder: string): Promise<ProjectCommandResult> {
    const cmd = parseProjectCommand(remainder);
    switch (cmd.kind) {
        case 'start': return await start(session, cmd);
        case 'status': return { output: await status(session, cmd.database) };
        case 'stop': return { output: await stop(session, cmd.id) };
        case 'update': return { output: await update(session, cmd.id) };
        case 'events': return { output: await events(session, cmd.id) };
        case 'register-key': return { output: await registerKey(session, cmd.id, cmd.keyHash, cmd.publicKey) };
        case 'resolve-key': return { output: await resolveKey(session, cmd.id, cmd.token) };
    }
}

export async function stopAllProjections(session: ReplSession): Promise<void> {
    const ids = [...session.projections.keys()];
    for (const id of ids) {
        try {
            await teardown(session, id);
        } catch {
            session.projections.delete(id);
        }
    }
}

async function start(session: ReplSession, cmd: ProjectStartCommand): Promise<ProjectCommandResult> {
    if (session.projectionTargetFactory === undefined) {
        throw new Error('No projection backend configured for this host');
    }
    if (session.keyVault === undefined) throw new Error('No keystore configured');

    const { id, db, name } = await resolveDatabase(session, cmd.database);
    for (const existing of session.projections.values()) {
        if (existing.dbId === id) {
            throw new Error(`Database is already projecting as session ${existing.id}; use \\project stop ${existing.id}`);
        }
        if (destinationsCollide(existing.path, cmd.path)) {
            throw new Error(`Path '${cmd.path}' is already in use by session ${existing.id}; use \\project stop ${existing.id}`);
        }
    }

    const record = session.keyVault.resolveRecord(session.resolveKeyRef(cmd.localId));
    const identity = session.resolveIdentity(record.label);
    if (identity === undefined) {
        return { needsUnlock: { label: record.label } };
    }

    const projectId = session.nextProjectId;
    session.nextProjectId += 1;
    try {
        const target = await session.projectionTargetFactory({ databaseId: id, path: cmd.path });
        // Reactive cycles run from timers / DAG listeners: thrown failures and ingest
        // rejections would otherwise be swallowed. Route both through the host hook.
        // These callbacks must never throw (they run off the command path).
        const projection = await RdbProjection.open(db, session.workspace.replica, target, {
            writer: identity,
            createUuid: () => session.createUuid(),
            onError: (err) => {
                const message = err instanceof Error ? err.message : String(err);
                session.onProjectionError?.(`projection error: ${message}`);
            },
            onResult: (results) => {
                const warning = formatRejectWarning(results);
                if (warning !== undefined) session.onProjectionError?.(warning);
            },
            onOpEvents: (opEvents) => {
                for (const event of opEvents) {
                    session.onProjectionError?.(`op-event: ${formatOpEvent(session, event)}`);
                }
            },
        });
        session.projections.set(projectId, {
            id: projectId,
            dbId: id,
            dbName: name,
            path: cmd.path,
            identityLabel: record.label,
            identityKeyId: identity.keyId,
            projection,
        });

        const groups = projection.memberGroupIds();
        const lines = [
            `started projection ${projectId} for ${name} as ${record.label} to ${cmd.path}`,
        ];
        if (groups.length > 0) {
            lines.push(formatRows(groups.map((g) => ({ group: formatDisplayString(session, g, { role: 'hash' }) }))));
        }
        const warning = formatRejectWarning(projection.lastResult());
        if (warning !== undefined) {
            session.onProjectionError?.(warning);
            lines.push(warning);
        }
        return { output: lines.join('\n') };
    } catch (err) {
        const entry = session.projections.get(projectId);
        if (entry !== undefined) {
            await teardown(session, projectId);
        }
        throw err;
    }
}

async function update(session: ReplSession, id: number): Promise<string> {
    const entry = requireEntry(session, id);
    const results = await entry.projection.sync();
    const line = `updated projection ${id}: ${summarizeResults(results)}`;
    const warning = formatRejectWarning(results);
    return warning === undefined ? line : `${line}\n${warning}`;
}

async function stop(session: ReplSession, id: number): Promise<string> {
    requireEntry(session, id);
    await teardown(session, id);
    return `stopped projection ${id}`;
}

async function events(session: ReplSession, id: number): Promise<string> {
    const entry = requireEntry(session, id);
    const backlog = await entry.projection.opEvents();
    if (backlog.length === 0) return '(no op-events)';
    return backlog.map((s) => `#${s.id} ${formatOpEvent(session, s.event)}`).join('\n');
}

async function status(session: ReplSession, database?: string): Promise<string> {
    let rows = [...session.projections.values()].map((entry) => ({
        id: entry.id,
        db: entry.dbName,
        as: entry.identityLabel,
        to: entry.path,
        groups: entry.projection.memberGroupIds().length,
        last: summarizeResults(entry.projection.lastResult()),
        error: entry.projection.lastError() ?? '',
    }));
    if (database !== undefined) {
        const { id } = await resolveDatabase(session, database);
        rows = rows.filter((row) => {
            const entry = session.projections.get(row.id as number);
            return entry?.dbId === id;
        });
    }
    if (rows.length === 0) return '(no active projections)';
    return formatSessionRows(session, rows, ['id', 'db', 'as', 'to', 'groups', 'last', 'error']);
}

async function teardown(session: ReplSession, id: number): Promise<void> {
    const entry = session.projections.get(id);
    if (entry === undefined) return;
    session.projections.delete(id);
    await entry.projection.stop();
}

async function registerKey(session: ReplSession, id: number, keyHash: string, publicKey: string): Promise<string> {
    const allocated = await requireEntry(session, id).projection.registerKey(keyHash, publicKey);
    return `registered key id=${allocated} key_hash=${formatDisplayString(session, keyHash, { role: 'hash', identity: true })}`;
}

async function resolveKey(session: ReplSession, id: number, token: string): Promise<string> {
    const projection = requireEntry(session, id).projection;
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
    const keyId = await projection.idForKeyHash(token);
    if (keyId === undefined) return `(no key with hash ${formatDisplayString(session, token, { role: 'hash' })})`;
    const publicKey = await projection.publicKeyForId(keyId);
    return formatRows([{
        id: keyId,
        key_hash: formatDisplayString(session, token, { role: 'hash', identity: true }),
        public_key: publicKey ?? '(null)',
    }]);
}

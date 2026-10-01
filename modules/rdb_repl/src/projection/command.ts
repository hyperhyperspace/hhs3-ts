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
//   \project events <id> [after <n>] [before <n>] [limit <m>] [order asc|desc]
//   \project register-key <id> <keyHash> <publicKey>
//   \project resolve-key <id> <token>
//   \project indexes <id> <spec.json | {inline json}> [dry-run]
//                                                 install a projection index spec
//   \project files <id> <name> to <path>          mount FILES <name> as a folder,
//                                                 until the projection stops

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { formatOpVoidDetail } from "@hyper-hyper-space/hhs3_rdb";
import type { RDb } from "@hyper-hyper-space/hhs3_rdb";
import type {
    IndexReconcileReport, IndexSpec, IngestResult, OpEvent, OpEventOrder,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import type { FilesMountSpec } from "@hyper-hyper-space/hhs3_rdb_projection";
import { openProjection } from "@hyper-hyper-space/hhs3_rhost";

import { formatDisplayString, formatSessionRows } from "../format/display.js";
import { formatFilesMount } from "../format/files.js";
import { formatRows } from "../format/rows.js";
import type { ReplSession } from "../session.js";
import {
    parseProjectCommand,
    type ProjectEventsCommand,
    type ProjectFilesCommand,
    type ProjectIndexesCommand,
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

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

// Folder paths compare lexically: the host resolves them, not the repl.
function normalizePath(path: string): string {
    let p = path;
    while (p.startsWith('./')) p = p.slice(2);
    while (p.length > 1 && p.endsWith('/')) p = p.slice(0, -1);
    return p === '' ? '.' : p;
}

function inside(path: string, folder: string): boolean {
    if (folder === '.') return !path.startsWith('/');
    return path.startsWith(folder === '/' ? '/' : `${folder}/`);
}

// The same path, or one inside the other.
function pathsOverlap(a: string, b: string): boolean {
    const x = normalizePath(a);
    const y = normalizePath(b);
    return x === y || inside(x, y) || inside(y, x);
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
        case 'events': return { output: await events(session, cmd) };
        case 'register-key': return { output: await registerKey(session, cmd.id, cmd.keyHash, cmd.publicKey) };
        case 'resolve-key': return { output: await resolveKey(session, cmd.id, cmd.token) };
        case 'indexes': return { output: await indexes(session, cmd) };
        case 'files': return { output: await files(session, cmd) };
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
        const { projection } = await openProjection({
            db,
            ctx: session.workspace.replica,
            target,
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
            files: [],
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
    const folders = requireEntry(session, id).files.map((f) => f.spec.path);
    await teardown(session, id);
    return folders.length === 0
        ? `stopped projection ${id}`
        : `stopped projection ${id}; its folders (${folders.join(', ')}) stay as they are`;
}

const EVENTS_DEFAULT_LIMIT = 50;
const EVENTS_DEFAULT_ORDER: OpEventOrder = 'desc';

async function events(session: ReplSession, cmd: ProjectEventsCommand): Promise<string> {
    const entry = requireEntry(session, cmd.id);
    const order = cmd.order ?? EVENTS_DEFAULT_ORDER;
    const backlog = await entry.projection.opEvents({
        afterId: cmd.afterId,
        beforeId: cmd.beforeId,
        limit: cmd.limit ?? EVENTS_DEFAULT_LIMIT,
        order,
    });
    if (backlog.length === 0) return '(no op-events)';
    return backlog.map((s) => `#${s.id} ${formatOpEvent(session, s.event)}`).join('\n');
}

async function status(session: ReplSession, database?: string): Promise<string> {
    let entries = [...session.projections.values()];
    if (database !== undefined) {
        const { id } = await resolveDatabase(session, database);
        entries = entries.filter((entry) => entry.dbId === id);
    }
    if (entries.length === 0) return '(no active projections)';
    const rows = entries.map((entry) => ({
        id: entry.id,
        db: entry.dbName,
        as: entry.identityLabel,
        to: entry.path,
        groups: entry.projection.memberGroupIds().length,
        last: summarizeResults(entry.projection.lastResult()),
        error: entry.projection.lastError() ?? '',
    }));
    const out = [formatSessionRows(session, rows, ['id', 'db', 'as', 'to', 'groups', 'last', 'error'])];
    const mounts = entries.flatMap((entry) => entry.projection.filesStatus().map((m) => ({
        id: entry.id,
        name: m.name,
        path: m.path,
        state: m.state,
        files: m.files ?? '',
        missing: m.missing ?? '',
        local_only: m.localOnly ?? '',
        waiting: m.waiting ?? '',
        writable: m.writable === undefined ? '' : m.writable ? 'yes' : 'no',
        error: m.lastError ?? '',
    })));
    if (mounts.length > 0) {
        out.push(formatSessionRows(session, mounts, ['id', 'name', 'path', 'state', 'files', 'missing', 'local_only', 'waiting', 'writable', 'error']));
    }
    return out.join('\n\n');
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

async function indexes(session: ReplSession, cmd: ProjectIndexesCommand): Promise<string> {
    const projection = requireEntry(session, cmd.id).projection;
    let text: string;
    if ('inline' in cmd.spec) {
        text = cmd.spec.inline;
    } else {
        if (session.readTextFile === undefined) {
            throw new Error('This host cannot read files; pass the index spec inline as {...}');
        }
        text = await session.readTextFile(cmd.spec.path);
    }
    let spec: IndexSpec;
    try {
        spec = JSON.parse(text) as IndexSpec;
    } catch (e) {
        throw new Error(`Invalid index spec JSON: ${e instanceof Error ? e.message : String(e)}`);
    }
    const report = await projection.reconcileIndexes(spec, { dryRun: cmd.dryRun });
    return formatIndexReport(cmd.id, report);
}

function formatIndexReport(id: number, report: IndexReconcileReport): string {
    const lines = [`indexes ${report.status} for projection ${id}`];
    if (report.actions.length > 0) {
        lines.push(formatRows(report.actions.map((a) => a.kind === 'ensure-index'
            ? {
                action: 'ensure', table: a.index.table, index: a.index.name,
                columns: a.index.columns.map((c) => c.target).join(', '),
            }
            : a.kind === 'drop-index'
                ? { action: 'drop', table: a.table, index: a.name, columns: '' }
                : { action: a.kind, table: '', index: '', columns: '' })));
    } else if (report.status === 'installed' || report.status === 'dry-run') {
        lines.push('(no index changes)');
    }
    if (report.pending.length > 0) {
        lines.push('pending (not buildable yet):');
        lines.push(formatRows(report.pending.map((p) => ({
            group: p.group, index: p.name, table: p.table, missing: p.missing.join(', '),
        }))));
    }
    return lines.join('\n');
}

// Mounts stay until the projection stops. The folder is opened here, so a bad
// path fails the command instead of leaving a mount that keeps failing to
// attach.
async function files(session: ReplSession, cmd: ProjectFilesCommand): Promise<string> {
    if (session.filesDirectoryFactory === undefined) throw new Error("This host can't mount folders");
    const entry = requireEntry(session, cmd.id);
    if (!IDENTIFIER.test(cmd.name)) throw new Error(`A FILES name is an identifier, got '${cmd.name}'`);

    const mounted = entry.files.find((f) => f.spec.name === cmd.name);
    if (mounted !== undefined) {
        if (normalizePath(mounted.spec.path) !== normalizePath(cmd.path)) {
            throw new Error(`FILES ${cmd.name} is already mounted at '${mounted.spec.path}' by projection ${entry.id}; stop the projection to change it`);
        }
        return mountLine(entry, cmd.name);
    }
    for (const other of session.projections.values()) {
        if (!isMemoryPath(other.path) && pathsOverlap(cmd.path, other.path)) {
            throw new Error(`Path '${cmd.path}' overlaps '${other.path}', the target of projection ${other.id}`);
        }
        for (const f of other.files) {
            if (pathsOverlap(cmd.path, f.spec.path)) {
                throw new Error(`Path '${cmd.path}' overlaps '${f.spec.path}', where projection ${other.id} mounts FILES ${f.spec.name}`);
            }
        }
    }

    const spec: FilesMountSpec = { name: cmd.name, path: cmd.path };
    const dir = await session.filesDirectoryFactory({ databaseId: entry.dbId, name: cmd.name, path: cmd.path });
    const before = entry.files;
    const next = [...before, { spec, dir }];
    const opener = (list: typeof next) => (s: FilesMountSpec) => {
        const found = list.find((f) => f.spec.name === s.name);
        if (found === undefined) throw new Error(`No folder for FILES ${s.name}`);
        return found.dir;
    };
    try {
        await entry.projection.reconcileFiles(next.map((f) => f.spec), opener(next));
    } catch (err) {
        await entry.projection.reconcileFiles(before.map((f) => f.spec), opener(before)).catch(() => undefined);
        throw err;
    }
    entry.files = next;
    return mountLine(entry, cmd.name);
}

function mountLine(entry: ProjectSessionEntry, name: string): string {
    const status = entry.projection.filesStatus().find((m) => m.name === name);
    return status === undefined ? `${name}: not mounted` : formatFilesMount(status, entry.identityLabel);
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

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Payload, RObject } from "@hyper-hyper-space/hhs3_mvt";
import { RDB_TYPE_ID, type RDb } from "@hyper-hyper-space/hhs3_rdb";
import { payloadName } from "@hyper-hyper-space/hhs3_rdb_runtime";

import { runLanguageText } from "../adapter.js";
import { formatSessionRows } from "../format/display.js";
import type { ReplSession } from "../session.js";
import { createAllowAuthorizer } from "./authorizer.js";
import {
    allowIsEveryone,
    formatAllow,
    parseSyncCommand,
    type AllowSource,
    type SyncFetchCommand,
    type SyncStartCommand,
} from "./parse.js";
import type { BuiltSyncMesh, SyncSessionEntry } from "./types.js";

export type SyncCommandResult = {
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

export async function runSyncCommand(session: ReplSession, remainder: string): Promise<SyncCommandResult> {
    const cmd = parseSyncCommand(remainder);
    switch (cmd.kind) {
        case 'start': return await start(session, cmd);
        case 'fetch': return await fetch(session, cmd);
        case 'status': return { output: await status(session, cmd.database) };
        case 'stop': return { output: await stop(session, cmd.id) };
        case 'peers': return { output: await peers(session, cmd.id) };
    }
}

export async function stopAllSyncs(session: ReplSession): Promise<void> {
    const ids = [...session.syncs.keys()];
    for (const id of ids) {
        try {
            await teardown(session, id);
        } catch {
            session.syncs.delete(id);
        }
    }
}

async function start(session: ReplSession, cmd: SyncStartCommand): Promise<SyncCommandResult> {
    if (session.syncMeshFactory === undefined) {
        throw new Error('No sync mesh factory configured for this host');
    }
    if (session.keyVault === undefined) throw new Error('No keystore configured');

    const { id, db, name } = await resolveDatabase(session, cmd.database);
    for (const existing of session.syncs.values()) {
        if (existing.dbId === id) {
            throw new Error(`Database is already syncing as session ${existing.id}; use \\sync stop ${existing.id}`);
        }
    }

    const record = session.keyVault.resolveRecord(session.resolveKeyRef(cmd.localId));
    const identity = session.resolveIdentity(record.label);
    if (identity === undefined) {
        return { needsUnlock: { label: record.label } };
    }

    const lookup = makeLookup(session);
    if (!allowIsEveryone(cmd.sources)) {
        await validateAllowQueries(cmd.sources, lookup);
    }
    const authorizer = createAllowAuthorizer(cmd.sources, lookup);

    const built = await session.syncMeshFactory({
        scope: cmd.scope,
        identity,
        trackerAddress: cmd.tracker,
        trackerKeyId: cmd.trackerKey,
        listenAddress: cmd.listen,
    });

    const syncId = session.nextSyncId;
    session.nextSyncId += 1;
    const meshLabel = `sync-${syncId}`;
    const entry: SyncSessionEntry = {
        id: syncId,
        dbId: id,
        dbName: name,
        db,
        meshLabel,
        mesh: built.mesh,
        identityLabel: record.label,
        identityKeyId: identity.keyId,
        scope: cmd.scope,
        sources: cmd.sources,
        announcedAddresses: built.announcedAddresses,
        discoveryNotes: built.discoveryNotes,
        closeables: built.closeables,
    };

    session.workspace.replica.attachMesh(meshLabel, built.mesh);
    db.setRuntimeConfig({ meshLabel, authorizer });

    try {
        await db.startSync();
    } catch (err) {
        session.syncs.set(syncId, entry);
        await teardown(session, syncId);
        throw err;
    }

    session.syncs.set(syncId, entry);
    const notes = built.discoveryNotes.length === 0 ? '' : `\n${built.discoveryNotes.join('\n')}`;
    return {
        output: `started sync ${syncId} for ${name} as ${record.label} on ${cmd.scope}${notes}`,
    };
}

async function fetch(session: ReplSession, cmd: SyncFetchCommand): Promise<SyncCommandResult> {
    const replica = session.workspace.replica;
    const existing = await replica.getObject(cmd.rdbId);
    if (existing !== undefined) {
        if (existing.getType() !== RDB_TYPE_ID) {
            throw new Error(`Object '${cmd.rdbId}' is type '${existing.getType()}', not an RDb`);
        }
        registerFetched(session, existing);
        return { output: formatFetchOutput(session, existing, true) };
    }

    if (session.syncMeshFactory === undefined) {
        throw new Error('No sync mesh factory configured for this host');
    }
    if (session.keyVault === undefined) throw new Error('No keystore configured');

    const record = session.keyVault.resolveRecord(session.resolveKeyRef(cmd.localId));
    const identity = session.resolveIdentity(record.label);
    if (identity === undefined) {
        return { needsUnlock: { label: record.label } };
    }

    const fetchId = session.nextFetchId;
    session.nextFetchId += 1;
    const meshLabel = `fetch-${fetchId}`;
    let built: BuiltSyncMesh | undefined;
    try {
        built = await session.syncMeshFactory({
            scope: cmd.scope,
            identity,
            trackerAddress: cmd.tracker,
            trackerKeyId: cmd.trackerKey,
            listenAddress: cmd.listen,
        });
        replica.attachMesh(meshLabel, built.mesh);
        const obj = await replica.fetchObject(cmd.rdbId, {
            meshLabel,
            backendLabel: session.workspace.backendLabel,
        });
        if (obj.getType() !== RDB_TYPE_ID) {
            throw new Error(
                `Fetched object is type '${obj.getType()}', not an RDb. The RDb may already be local.`,
            );
        }
        registerFetched(session, obj);
        return { output: formatFetchOutput(session, obj, false) };
    } catch (err) {
        const leftover = await replica.getObject(cmd.rdbId);
        const msg = err instanceof Error ? err.message : String(err);
        if (leftover !== undefined && !msg.includes('may already be local')) {
            throw new Error(`${msg} The RDb may already be local.`);
        }
        throw err;
    } finally {
        await closeTempMesh(session, meshLabel, built);
    }
}

async function status(session: ReplSession, database?: string): Promise<string> {
    let rows = [...session.syncs.values()].map((entry) => ({
        id: entry.id,
        db: entry.dbName,
        scope: entry.scope,
        as: entry.identityLabel,
        allow: formatAllow(entry.sources),
        listen: entry.announcedAddresses.join(', '),
        discovery: entry.discoveryNotes.join('; '),
        peers: countPeers(entry),
    }));
    if (database !== undefined) {
        const { id } = await resolveDatabase(session, database);
        rows = rows.filter((row) => {
            const entry = session.syncs.get(row.id as number);
            return entry?.dbId === id;
        });
    }
    if (rows.length === 0) return '(no active syncs)';
    return formatSessionRows(session, rows, ['id', 'db', 'scope', 'as', 'allow', 'listen', 'discovery', 'peers']);
}

async function stop(session: ReplSession, id: number): Promise<string> {
    const entry = session.syncs.get(id);
    if (entry === undefined) throw new Error(`No sync session ${id}`);
    await teardown(session, id);
    return `stopped sync ${id}`;
}

async function peers(session: ReplSession, id: number): Promise<string> {
    const entry = session.syncs.get(id);
    if (entry === undefined) throw new Error(`No sync session ${id}`);
    const seen = new Set<string>();
    const rows: Record<string, unknown>[] = [];
    for (const swarm of entry.mesh.swarms()) {
        for (const peer of swarm.peers()) {
            const key = `${peer.keyId}@${peer.endpoint}`;
            if (seen.has(key)) continue;
            seen.add(key);
            rows.push({
                keyId: peer.keyId,
                endpoint: peer.endpoint,
                topic: swarm.topic,
            });
        }
    }
    if (rows.length === 0) return '(no peers)';
    return formatSessionRows(session, rows, ['keyId', 'endpoint', 'topic'], {
        structuralColumns: new Set(['keyId', 'topic']),
        identityColumns: new Set(['keyId']),
    });
}

async function teardown(session: ReplSession, id: number): Promise<void> {
    const entry = session.syncs.get(id);
    if (entry === undefined) return;
    session.syncs.delete(id);
    try {
        await entry.db.stopSync();
    } catch {
        // already stopped
    }
    for (const closeable of entry.closeables) {
        try {
            await closeable.close();
        } catch {
            // best-effort
        }
    }
    try {
        entry.mesh.close();
    } catch {
        // best-effort
    }
    session.workspace.replica.detachMesh(entry.meshLabel);
}

function countPeers(entry: SyncSessionEntry): number {
    const ids = new Set<string>();
    for (const swarm of entry.mesh.swarms()) {
        for (const peer of swarm.peers()) ids.add(peer.keyId);
    }
    return ids.size;
}

function registerFetched(session: ReplSession, obj: RObject): void {
    const name = objectName(obj) ?? session.workspace.roots.get(obj.getId())?.name;
    session.workspace.roots.registerObject(obj.getId(), obj, name);
}

function objectName(obj: RObject): string | undefined {
    const createOp = (obj as { createOp?: object }).createOp;
    if (createOp === undefined) return undefined;
    return payloadName(createOp as Payload);
}

function formatFetchOutput(session: ReplSession, obj: RObject, alreadyLocal: boolean): string {
    const name = session.workspace.roots.get(obj.getId())?.name ?? `#${obj.getId()}`;
    const already = alreadyLocal ? 'already local; ' : '';
    return `fetched ${name} (${already}genesis only; use \\sync start to share)`;
}

async function closeTempMesh(
    session: ReplSession,
    meshLabel: string,
    built: BuiltSyncMesh | undefined,
): Promise<void> {
    if (built !== undefined) {
        for (const closeable of built.closeables) {
            try {
                await closeable.close();
            } catch {
                // best-effort
            }
        }
        try {
            built.mesh.close();
        } catch {
            // best-effort
        }
    }
    session.workspace.replica.detachMesh(meshLabel);
}

function makeLookup(session: ReplSession) {
    return async (source: Extract<AllowSource, { type: 'column' }>): Promise<Iterable<unknown>> => {
        const sql = `SELECT ${source.column} FROM ${source.group}.${source.table}`
            + (source.where === undefined ? '' : ` WHERE ${source.where}`);
        const run = await runLanguageText(session, sql);
        const result = run.results[0]?.result;
        if (result === undefined || result.kind !== 'select') {
            throw new Error(`allow query did not return a SELECT result`);
        }
        return result.rows.map((row) => row.values[source.column]);
    };
}

async function validateAllowQueries(
    sources: AllowSource[],
    lookup: ReturnType<typeof makeLookup>,
): Promise<void> {
    for (const source of sources) {
        if (source.type !== 'column') continue;
        await lookup(source);
    }
}

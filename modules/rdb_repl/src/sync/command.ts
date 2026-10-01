import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Payload, RObject } from "@hyper-hyper-space/hhs3_mvt";
import { RDB_TYPE_ID, type RDb } from "@hyper-hyper-space/hhs3_rdb";
import { payloadName } from "@hyper-hyper-space/hhs3_rdb_runtime";
import {
    allowIsEveryone,
    columnLookup,
    createAllowAuthorizer,
    fetchDatabase,
    formatAllow,
    startDatabaseSync,
    validateAllowSources,
} from "@hyper-hyper-space/hhs3_rhost";

import { formatSessionRows } from "../format/display.js";
import type { ReplSession } from "../session.js";
import {
    parseSyncCommand,
    type SyncFetchCommand,
    type SyncStartCommand,
} from "./parse.js";

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

    const lookup = columnLookup(session);
    if (!allowIsEveryone(cmd.sources)) {
        await validateAllowSources(cmd.sources, lookup);
    }
    const authorizer = createAllowAuthorizer(cmd.sources, lookup);

    const built = await session.syncMeshFactory({
        scope: cmd.scope,
        identity,
        trackerAddress: cmd.tracker,
        trackerKeyId: cmd.trackerKey,
        listenAddress: cmd.listen,
        report: session.report,
    });

    const syncId = session.nextSyncId;
    session.nextSyncId += 1;
    const sync = await startDatabaseSync({
        replica: session.workspace.replica,
        db,
        built,
        meshLabel: `sync-${syncId}`,
        authorizer,
        report: session.report,
    });

    session.syncs.set(syncId, {
        id: syncId,
        dbId: id,
        dbName: name,
        identityLabel: record.label,
        identityKeyId: identity.keyId,
        scope: cmd.scope,
        sources: cmd.sources,
        sync,
    });
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
    const obj = await fetchDatabase({
        replica,
        id: cmd.rdbId,
        meshFactory: session.syncMeshFactory,
        request: {
            scope: cmd.scope,
            identity,
            trackerAddress: cmd.tracker,
            trackerKeyId: cmd.trackerKey,
            listenAddress: cmd.listen,
            report: session.report,
        },
        meshLabel: `fetch-${fetchId}`,
        backendLabel: session.workspace.backendLabel,
    });
    registerFetched(session, obj);
    return { output: formatFetchOutput(session, obj, false) };
}

async function status(session: ReplSession, database?: string): Promise<string> {
    let rows = [...session.syncs.values()].map((entry) => ({
        id: entry.id,
        db: entry.dbName,
        scope: entry.scope,
        as: entry.identityLabel,
        allow: formatAllow(entry.sources),
        listen: entry.sync.listenAddresses.join(', '),
        discovery: entry.sync.discoveryNotes.join('; '),
        peers: entry.sync.peerCount(),
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
    const rows: Record<string, unknown>[] = entry.sync.peers().map((peer) => ({
        keyId: peer.keyId,
        endpoint: peer.endpoint,
        topic: peer.topic,
    }));
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
    await entry.sync.stop();
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

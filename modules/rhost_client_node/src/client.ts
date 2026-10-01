// A Node app's client for one rhost host, given its folder:
//
//   <app>/app.json                   projection.path, unless host.json overrides it
//   <app>/hosts/<name>/
//     host.json                      the database, catalog and key
//     db/data.sqlite                 the projection: rdb_keys and rdb_op_events
//     run/rhost.sock                 the running process's live state
//
// Keys and events work from the files, so they work while the host is
// stopped; status and its watch need the running process.

import { existsSync, promises as fs, readFileSync } from "node:fs";
import { connect, type Socket } from "node:net";
import { basename, join, resolve } from "node:path";
import Database from "better-sqlite3";

import { createBasicCrypto, HASH_SHA256, keyIdFromPublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { deserializePublicKeyFromBase64 } from "@hyper-hyper-space/hhs3_mvt";
import {
    LineDecoder, decodeServerMessage, encodeMessage, socketPathFor,
    type ClientEvent, type ClientEvents, type ClientKey, type ClientStatus, type HostStatus,
    type RhostClient, type StoppedStatus,
} from "@hyper-hyper-space/hhs3_rhost_client";

const KEY_TABLE = 'rdb_keys';
const EVENT_TABLE = 'rdb_op_events';
const EVENT_POLL_MS = 500;
const RECONNECT_MS = 1_000;
const STATUS_TIMEOUT_MS = 5_000;

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

type ProjectionSection = { path?: unknown };
type HostFile = {
    database: string; catalog: string; created: boolean;
    key?: { label?: unknown; keyId?: unknown; publicKey?: unknown };
    projection?: ProjectionSection;
};
type AppFile = { projection?: ProjectionSection };

export type OpenClientOptions = {
    // The app's own handle on the projection; otherwise the client opens one.
    db?: Database.Database;
};

export type HostKey = { label: string; keyId: string; publicKey: string };

function missing(e: unknown, path: string, what: string): unknown {
    return (e as NodeJS.ErrnoException).code === 'ENOENT' ? new Error(`no ${what} at ${path}`) : e;
}

async function readJson<T>(path: string, what: string): Promise<T> {
    let text: string;
    try {
        text = await fs.readFile(path, 'utf8');
    } catch (e) {
        throw missing(e, path, what);
    }
    return JSON.parse(text) as T;
}

function readJsonSync<T>(path: string, what: string): T {
    let text: string;
    try {
        text = readFileSync(path, 'utf8');
    } catch (e) {
        throw missing(e, path, what);
    }
    return JSON.parse(text) as T;
}

// The host's projection file: projection.path from its host.json when that
// section is set, otherwise from the app's app.json, resolved in the host
// folder. Synchronous, like every read the client makes of the projection.
export function projectionPath(hostDir: string): string {
    const dir = resolve(hostDir);
    const record = readJsonSync<HostFile>(join(dir, 'host.json'), 'host.json');
    const source = record.projection !== undefined ? 'host.json' : "the app's app.json";
    const projection = record.projection ?? readJsonSync<AppFile>(join(dir, '..', '..', 'app.json'), source).projection;
    if (typeof projection?.path !== 'string' || projection.path.length === 0) throw new Error(`${source} has no projection.path`);
    return resolve(dir, projection.path);
}

// The host's key, as its host.json records it: public parts only, so neither
// the keystore nor a passphrase is needed.
export async function hostKey(hostDir: string): Promise<HostKey> {
    const dir = resolve(hostDir);
    const key = (await readJson<HostFile>(join(dir, 'host.json'), 'host.json')).key;
    const { label, keyId, publicKey } = key ?? {};
    if (typeof label !== 'string' || typeof keyId !== 'string' || typeof publicKey !== 'string') {
        throw new Error(`${join(dir, 'host.json')} has no key with a label, keyId and publicKey`);
    }
    return { label, keyId, publicKey };
}

type EventRow = {
    id: number; origin: string; direction: string; group_id: string; op_hash: string; op_json: string | null;
    kind: string; tbl: string | null; row_hash: string | null; local_id: number | null; author: string | null; reason: string | null;
};

function toEvent(r: EventRow): ClientEvent {
    const event: ClientEvent = {
        id: r.id,
        origin: r.origin as ClientEvent['origin'],
        direction: r.direction as ClientEvent['direction'],
        groupId: r.group_id,
        opHash: r.op_hash,
        kind: r.kind,
    };
    if (r.tbl !== null) event.table = r.tbl;
    if (r.row_hash !== null) event.rowId = r.row_hash;
    if (r.local_id !== null) event.localId = r.local_id;
    if (r.author !== null) event.author = r.author;
    if (r.op_json !== null) event.op = JSON.parse(r.op_json);
    if (r.reason !== null) event.reason = JSON.parse(r.reason);
    return event;
}

// One request on a fresh connection; undefined when nothing listens.
function askSocket(path: string): Promise<HostStatus | undefined> {
    return new Promise((done, fail) => {
        const socket = connect(path);
        const decoder = new LineDecoder();
        let settled = false;
        const finish = (settle: () => void) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            socket.destroy();
            settle();
        };
        const timer = setTimeout(() => finish(() => fail(new Error(`${path} did not answer`))), STATUS_TIMEOUT_MS);
        socket.setEncoding('utf8');
        socket.on('connect', () => socket.write(encodeMessage({ id: 1, method: 'status' })));
        socket.on('data', (chunk: string) => {
            for (const line of decoder.push(chunk)) {
                const message = decodeServerMessage(line);
                if (message.id !== 1) continue;
                if ('error' in message) finish(() => fail(new Error(message.error)));
                else if ('result' in message && message.result !== null) finish(() => done(message.result!));
            }
        });
        socket.on('error', (err: NodeJS.ErrnoException) => {
            if (err.code === 'ENOENT' || err.code === 'ECONNREFUSED') finish(() => done(undefined));
            else finish(() => fail(err));
        });
        socket.on('close', () => finish(() => done(undefined)));
    });
}

export function openClient(hostDir: string, options: OpenClientOptions = {}): RhostClient {
    const dir = resolve(hostDir);
    const socketPath = socketPathFor(dir, process.platform);
    const ownDb = options.db === undefined;
    let db = options.db;
    let dbPath: string | undefined;
    const stops = new Set<() => void>();

    // On first use: status() and its watch don't need the config files.
    const projectionFile = (): string => (dbPath ??= projectionPath(dir));

    const database = (): Database.Database => {
        if (db === undefined) {
            const path = projectionFile();
            try {
                db = new Database(path, { fileMustExist: true });
            } catch {
                throw new Error(`host '${basename(dir)}' has no projection yet; start it once`);
            }
            db.pragma('busy_timeout = 5000');
        }
        return db;
    };

    const hasTable = (name: string): boolean =>
        database().prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name) !== undefined;

    // Before the first start there is no projection, and so no events.
    const hasEvents = (): boolean =>
        (db !== undefined || existsSync(projectionFile())) && hasTable(EVENT_TABLE);

    // The same interning as the projection's target: the existing id, or the
    // next one, in one immediate transaction so another writer can't race it.
    const intern = (keyId: string, publicKey: string): number => {
        if (!hasTable(KEY_TABLE)) throw new Error(`host '${basename(dir)}' has no key table yet; start it once`);
        const run = database().transaction(() => {
            const existing = database().prepare(`SELECT id, public_key AS pk FROM ${KEY_TABLE} WHERE key_hash = ?`)
                .get(keyId) as { id: number; pk: string | null } | undefined;
            if (existing !== undefined) {
                if (existing.pk === null) {
                    database().prepare(`UPDATE ${KEY_TABLE} SET public_key = ? WHERE id = ?`).run(publicKey, existing.id);
                }
                return existing.id;
            }
            const next = (database().prepare(`SELECT COALESCE(MAX(id), 0) + 1 AS n FROM ${KEY_TABLE}`).get() as { n: number }).n;
            database().prepare(`INSERT INTO ${KEY_TABLE} (id, key_hash, public_key) VALUES (?, ?, ?)`).run(next, keyId, publicKey);
            return next;
        });
        return run.immediate();
    };

    const eventsAfter = (afterId: number, limit?: number): ClientEvent[] => {
        if (!hasEvents()) return [];
        const rows = database().prepare(
            'SELECT id, origin, direction, group_id, op_hash, op_json, kind, "table" AS tbl, row_hash, local_id, author, reason '
            + `FROM ${EVENT_TABLE} WHERE id > ? ORDER BY id ASC` + (limit !== undefined ? ' LIMIT ?' : ''),
        ).all(...(limit !== undefined ? [afterId, limit] : [afterId])) as EventRow[];
        return rows.map(toEvent);
    };

    const highestEvent = (): number => {
        if (!hasEvents()) return 0;
        return (database().prepare(`SELECT COALESCE(MAX(id), 0) AS n FROM ${EVENT_TABLE}`).get() as { n: number }).n;
    };

    const stopped = async (): Promise<StoppedStatus> => {
        const record = await readJson<HostFile>(join(dir, 'host.json'), 'host.json');
        return { host: basename(dir), database: record.database, catalog: record.catalog, created: record.created, running: false };
    };

    const track = (stop: () => void): (() => void) => {
        const once = () => {
            if (!stops.delete(once)) return;
            stop();
        };
        stops.add(once);
        return once;
    };

    const events: ClientEvents = {
        async since(afterId = 0, limit) {
            return eventsAfter(afterId, limit);
        },
        watch(listener) {
            let cursor = highestEvent();
            const timer = setInterval(() => {
                try {
                    const batch = eventsAfter(cursor);
                    if (batch.length === 0) return;
                    cursor = batch[batch.length - 1]!.id;
                    listener(batch);
                } catch {
                    // the file is busy or gone; the next tick retries
                }
            }, EVENT_POLL_MS);
            return track(() => clearInterval(timer));
        },
    };

    return {
        async me(): Promise<ClientKey> {
            const key = await hostKey(dir);
            return { ...key, id: intern(key.keyId, key.publicKey) };
        },
        async registerKey(publicKey: string): Promise<number> {
            return intern(keyIdFromPublicKey(deserializePublicKeyFromBase64(publicKey), hashSuite), publicKey);
        },
        events,
        async status(): Promise<ClientStatus> {
            return (await askSocket(socketPath)) ?? await stopped();
        },
        watchStatus(listener) {
            let socket: Socket | undefined;
            let retry: ReturnType<typeof setTimeout> | undefined;
            let done = false;
            let reportedStopped = false;
            const reportStopped = () => {
                if (reportedStopped) return;
                reportedStopped = true;
                void stopped().then((s) => { if (!done) listener(s); }, () => { /* no host.json */ });
            };
            const attach = () => {
                if (done) return;
                const decoder = new LineDecoder();
                const current = connect(socketPath);
                socket = current;
                current.setEncoding('utf8');
                current.on('connect', () => current.write(encodeMessage({ id: 1, method: 'watchStatus' })));
                current.on('data', (chunk: string) => {
                    for (const line of decoder.push(chunk)) {
                        const message = decodeServerMessage(line);
                        if (message.id !== 1) continue;
                        const status = 'event' in message ? message.event : 'result' in message ? message.result : undefined;
                        if (status === undefined || status === null || done) continue;
                        reportedStopped = false;
                        listener(status);
                    }
                });
                current.on('error', () => { /* the close handler follows */ });
                current.on('close', () => {
                    if (done) return;
                    reportStopped();
                    retry = setTimeout(attach, RECONNECT_MS);
                });
            };
            attach();
            return track(() => {
                done = true;
                if (retry !== undefined) clearTimeout(retry);
                socket?.destroy();
            });
        },
        async close(): Promise<void> {
            for (const stop of [...stops]) stop();
            if (ownDb && db !== undefined) {
                db.close();
                db = undefined;
            }
        },
    };
}

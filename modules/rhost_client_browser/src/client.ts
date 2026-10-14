// A browser tab's client for one rhost host. It does not run the host.
//
//   rhost-<app>-hosts                 the host record: database, catalog, key
//   rhost-<app>-<host>                BroadcastChannel: status and its watch
//   rhost-<app>-<host>-data           the projection: keys and op_events
//
// Keys and events work from the projection database, so they work while the
// host is stopped. Status comes from whichever tab is serving. With no answer
// within a second, status is a stopped status from the host record.

import { createBasicCrypto, HASH_SHA256, keyIdFromPublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { deserializePublicKeyFromBase64 } from "@hyper-hyper-space/hhs3_mvt";
import {
    BROWSER_HOSTS_STORE, BROWSER_HOSTS_VERSION, browserClientChannel, browserHostsDatabase,
    browserProjectionDatabase, decodeServerMessage, encodeMessage, isBrowserChannelMessage,
    type ClientEvent, type ClientEvents, type ClientKey, type ClientStatus, type HostStatus,
    type RhostClient, type ServerMessage, type StoppedStatus,
} from "@hyper-hyper-space/hhs3_rhost_client";

const KEYS = 'keys';
const COUNTERS = 'counters';
const NEXT_KEY_ID = 'nextKeyId';
const OP_EVENTS = 'op_events';
const STATUS_TIMEOUT_MS = 1_000;
const RECONNECT_MS = 1_000;
const EVENT_POLL_MS = 500;

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

type Channel = {
    postMessage(message: unknown): void;
    close(): void;
    onmessage: ((ev: { data: unknown }) => void) | null;
};

export type OpenClientOptions = {
    indexedDB?: IDBFactory;
    BroadcastChannel?: new (name: string) => Channel;
};

type HostKey = { label: string; keyId: string; publicKey: string };
type HostRow = {
    state?: string;
    record?: { database?: unknown; catalog?: unknown; created?: unknown; key?: { label?: unknown; keyId?: unknown; publicKey?: unknown } };
};
type KeyRec = { id: number; keyHash: string; publicKey?: string };
type EventRec = {
    id?: number;
    origin: ClientEvent['origin'];
    direction: ClientEvent['direction'];
    groupId: string;
    opHash: string;
    kind: string;
    table?: string;
    rowId?: string;
    localId?: number;
    author?: string;
    op?: ClientEvent['op'];
    reason?: ClientEvent['reason'];
};

function request<T>(req: IDBRequest<T>): Promise<T> {
    return new Promise((resolve, reject) => {
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error ?? new Error('IndexedDB request failed'));
    });
}

function txDone(tx: IDBTransaction): Promise<void> {
    return new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve();
        tx.onabort = () => reject(tx.error ?? new Error('transaction aborted'));
        tx.onerror = () => reject(tx.error ?? new Error('transaction failed'));
    });
}

function openDatabase(factory: IDBFactory, name: string, version: number, upgrade: (db: IDBDatabase) => void): Promise<IDBDatabase> {
    return new Promise((resolve, reject) => {
        const req = factory.open(name, version);
        req.onupgradeneeded = () => upgrade(req.result);
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error ?? new Error(`could not open '${name}'`));
    });
}

// Undefined when the database was never created. Opening must not leave an
// empty one behind: before the first project there is no projection.
function openIfPresent(factory: IDBFactory, name: string): Promise<IDBDatabase | undefined> {
    return new Promise((resolve, reject) => {
        let created = false;
        let settled = false;
        const finish = (db: IDBDatabase | undefined, err?: unknown) => {
            if (settled) return;
            settled = true;
            if (err !== undefined) reject(err);
            else resolve(db);
        };
        const req = factory.open(name);
        req.onupgradeneeded = () => {
            created = true;
            try { req.transaction?.abort(); } catch { /* the open fails instead */ }
        };
        req.onsuccess = () => {
            const db = req.result;
            if (created || !db.objectStoreNames.contains(KEYS)) {
                db.close();
                if (created) factory.deleteDatabase(name);
                finish(undefined);
                return;
            }
            finish(db);
        };
        req.onerror = () => {
            if (created) finish(undefined);
            else finish(undefined, req.error ?? new Error(`could not open '${name}'`));
        };
    });
}

function toEvent(rec: EventRec): ClientEvent {
    const event: ClientEvent = {
        id: rec.id ?? 0,
        origin: rec.origin,
        direction: rec.direction,
        groupId: rec.groupId,
        opHash: rec.opHash,
        kind: rec.kind,
    };
    if (rec.table !== undefined) event.table = rec.table;
    if (rec.rowId !== undefined) event.rowId = rec.rowId;
    if (rec.localId !== undefined) event.localId = rec.localId;
    if (rec.author !== undefined) event.author = rec.author;
    if (rec.op !== undefined) event.op = rec.op;
    if (rec.reason !== undefined) event.reason = rec.reason;
    return event;
}

// The same allocation as IdbTarget.internKey, in one readwrite transaction.
async function intern(db: IDBDatabase, keyHash: string, publicKey?: string): Promise<number> {
    const tx = db.transaction([KEYS, COUNTERS], 'readwrite');
    const keys = tx.objectStore(KEYS);
    const existing = await request<KeyRec | undefined>(keys.index('by_hash').get(keyHash));
    if (existing !== undefined) {
        if (publicKey !== undefined && existing.publicKey === undefined) {
            existing.publicKey = publicKey;
            await request(keys.put(existing));
        }
        await txDone(tx);
        return existing.id;
    }
    const counter = await request<{ value: number } | undefined>(tx.objectStore(COUNTERS).get(NEXT_KEY_ID));
    const id = counter?.value ?? 1;
    await request(tx.objectStore(COUNTERS).put({ key: NEXT_KEY_ID, value: id + 1 }));
    const rec: KeyRec = { id, keyHash };
    if (publicKey !== undefined) rec.publicKey = publicKey;
    await request(keys.put(rec));
    await txDone(tx);
    return id;
}

export function openClient(appName: string, host: string, options: OpenClientOptions = {}): RhostClient {
    const factory = options.indexedDB ?? globalThis.indexedDB;
    if (factory === undefined) throw new Error('no IndexedDB; pass indexedDB');
    const Channel = options.BroadcastChannel
        ?? (globalThis as { BroadcastChannel?: new (name: string) => Channel }).BroadcastChannel;
    if (Channel === undefined) throw new Error('no BroadcastChannel; pass BroadcastChannel');

    const clientId = [...crypto.getRandomValues(new Uint8Array(16))].map((b) => b.toString(16).padStart(2, '0')).join('');
    const channel = new Channel(browserClientChannel(appName, host));
    const pending = new Map<number, (message: ServerMessage | undefined) => void>();
    const stops = new Set<() => void>();
    let nextId = 0;
    let closed = false;
    let projectionDb: IDBDatabase | undefined;
    let hostGone: () => void = () => {};

    const track = (stop: () => void): (() => void) => {
        const once = () => {
            if (!stops.delete(once)) return;
            stop();
        };
        stops.add(once);
        return once;
    };

    const noKeyTable = () => new Error(`host '${host}' has no key table yet; start it once`);

    const readRow = async (): Promise<HostRow | undefined> => {
        const db = await openDatabase(factory, browserHostsDatabase(appName), BROWSER_HOSTS_VERSION, (opened) => {
            if (!opened.objectStoreNames.contains(BROWSER_HOSTS_STORE)) {
                opened.createObjectStore(BROWSER_HOSTS_STORE, { keyPath: 'name' });
            }
        });
        try {
            const tx = db.transaction(BROWSER_HOSTS_STORE, 'readonly');
            const row = await request<HostRow | undefined>(tx.objectStore(BROWSER_HOSTS_STORE).get(host));
            await txDone(tx);
            return row;
        } finally {
            db.close();
        }
    };

    const published = async (): Promise<{ database: string; catalog: string; created: boolean; key: HostKey }> => {
        const row = await readRow();
        const record = row?.state === 'published' ? row.record : undefined;
        const key = record?.key;
        if (typeof record?.database !== 'string' || typeof record.catalog !== 'string' || typeof record.created !== 'boolean'
            || typeof key?.label !== 'string' || typeof key.keyId !== 'string' || typeof key.publicKey !== 'string') {
            throw new Error(`no host '${host}'`);
        }
        return {
            database: record.database, catalog: record.catalog, created: record.created,
            key: { label: key.label, keyId: key.keyId, publicKey: key.publicKey },
        };
    };

    const stopped = async (): Promise<StoppedStatus> => {
        const record = await published();
        return { host, database: record.database, catalog: record.catalog, created: record.created, running: false };
    };

    const projection = async (): Promise<IDBDatabase | undefined> => {
        if (projectionDb !== undefined) return projectionDb;
        const db = await openIfPresent(factory, browserProjectionDatabase(appName, host));
        if (db !== undefined) projectionDb = db;
        return db;
    };

    const post = (line: string) => {
        channel.postMessage({ client: clientId, line });
    };

    const ask = (method: 'status' | 'watchStatus', timeoutMs: number): Promise<HostStatus | undefined> => {
        const id = ++nextId;
        return new Promise((resolve) => {
            const timer = setTimeout(() => {
                pending.delete(id);
                resolve(undefined);
            }, timeoutMs);
            pending.set(id, (message) => {
                clearTimeout(timer);
                pending.delete(id);
                if (message === undefined) resolve(undefined);
                else if ('error' in message) resolve(undefined);
                else if ('result' in message) resolve(message.result ?? undefined);
                else resolve(message.event);
            });
            try { post(encodeMessage({ id, method })); }
            catch { clearTimeout(timer); pending.delete(id); resolve(undefined); }
        });
    };

    channel.onmessage = (ev) => {
        const data = ev.data;
        if (!isBrowserChannelMessage(data) || 'stop' in data || data.client !== clientId) return;
        if ('close' in data) {
            const waiters = [...pending.values()];
            pending.clear();
            for (const waiter of waiters) waiter(undefined);
            hostGone();
            return;
        }
        try {
            const message = decodeServerMessage(data.line);
            pending.get(message.id)?.(message);
        } catch { /* not one of our replies */ }
    };

    const onPageHide = () => {
        try { channel.postMessage({ client: clientId, close: true }); } catch { /* the document is going */ }
    };
    if (typeof addEventListener === 'function') addEventListener('pagehide', onPageHide);

    const eventsAfter = async (afterId: number, limit?: number): Promise<ClientEvent[]> => {
        const db = await projection();
        if (db === undefined || !db.objectStoreNames.contains(OP_EVENTS)) return [];
        const tx = db.transaction(OP_EVENTS, 'readonly');
        const rows = await request<EventRec[]>(tx.objectStore(OP_EVENTS).getAll());
        await txDone(tx);
        const events = rows.filter((row) => (row.id ?? 0) > afterId).sort((a, b) => (a.id ?? 0) - (b.id ?? 0)).map(toEvent);
        return limit === undefined ? events : events.slice(0, limit);
    };

    const highestEvent = async (): Promise<number> => {
        const events = await eventsAfter(0);
        return events.length === 0 ? 0 : events[events.length - 1]!.id;
    };

    const events: ClientEvents = {
        async since(afterId = 0, limit) {
            return eventsAfter(afterId, limit);
        },
        watch(listener) {
            let cursor = 0;
            let ready = false;
            let stop = false;
            void highestEvent().then((id) => { cursor = id; ready = true; });
            const timer = setInterval(() => {
                if (stop || !ready) return;
                void eventsAfter(cursor).then((batch) => {
                    if (stop || batch.length === 0) return;
                    cursor = batch[batch.length - 1]!.id;
                    listener(batch);
                }).catch(() => { /* the next tick retries */ });
            }, EVENT_POLL_MS);
            return track(() => { stop = true; clearInterval(timer); });
        },
    };

    return {
        async me(): Promise<ClientKey> {
            const { key } = await published();
            const db = await projection();
            if (db === undefined) throw noKeyTable();
            return { ...key, id: await intern(db, key.keyId, key.publicKey) };
        },
        async registerKey(publicKey: string): Promise<number> {
            const db = await projection();
            if (db === undefined) throw noKeyTable();
            const keyId = keyIdFromPublicKey(deserializePublicKeyFromBase64(publicKey), hashSuite);
            return intern(db, keyId, publicKey);
        },
        events,
        async status(): Promise<ClientStatus> {
            return (await ask('status', STATUS_TIMEOUT_MS)) ?? await stopped();
        },
        watchStatus(listener) {
            let done = false;
            let reportedStopped = false;
            let retry: ReturnType<typeof setTimeout> | undefined;
            let attempt = 0;
            const reportStopped = () => {
                if (reportedStopped || done) return;
                reportedStopped = true;
                void stopped().then((status) => { if (!done) listener(status); }, () => { /* no host record */ });
            };
            const attach = () => {
                if (done) return;
                const mine = ++attempt;
                const id = ++nextId;
                const timer = setTimeout(() => {
                    if (done || mine !== attempt) return;
                    pending.delete(id);
                    reportStopped();
                }, STATUS_TIMEOUT_MS);
                pending.set(id, (message) => {
                    if (done || message === undefined) return;
                    const status = 'event' in message ? message.event : 'result' in message ? message.result : undefined;
                    if (status == null) return;
                    clearTimeout(timer);
                    reportedStopped = false;
                    listener(status);
                });
                try { post(encodeMessage({ id, method: 'watchStatus' })); }
                catch { /* closed */ }
            };
            hostGone = () => {
                if (done) return;
                reportStopped();
                if (retry !== undefined) clearTimeout(retry);
                retry = setTimeout(attach, RECONNECT_MS);
            };
            attach();
            return track(() => {
                done = true;
                if (retry !== undefined) clearTimeout(retry);
                hostGone = () => {};
            });
        },
        async close(): Promise<void> {
            if (closed) return;
            closed = true;
            for (const stop of [...stops]) stop();
            try { channel.postMessage({ client: clientId, close: true }); } catch { /* already closed */ }
            channel.onmessage = null;
            channel.close();
            if (typeof removeEventListener === 'function') removeEventListener('pagehide', onPageHide);
            projectionDb?.close();
            projectionDb = undefined;
        },
    };
}

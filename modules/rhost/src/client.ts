// The client interface in process: straight calls on a Host. Key and event
// calls go through the running projection, so they need the host started.

import { keyIdFromPublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { deserializePublicKeyFromBase64 } from "@hyper-hyper-space/hhs3_mvt";
import type { StoredOpEvent } from "@hyper-hyper-space/hhs3_rdb_adapter";
import type { RdbProjection } from "@hyper-hyper-space/hhs3_rdb_projection";
import type { ClientEvent, ClientEvents, ClientKey, ClientStatus, RhostClient } from "@hyper-hyper-space/hhs3_rhost_client";

import type { Host } from "./host.js";

export function toClientEvent(stored: StoredOpEvent): ClientEvent {
    const { event } = stored;
    const out: ClientEvent = {
        id: stored.id,
        origin: event.origin,
        direction: event.direction,
        groupId: event.groupId,
        opHash: event.opHash,
        kind: event.kind,
    };
    if (event.table !== undefined) out.table = event.table;
    if (event.rowId !== undefined) out.rowId = event.rowId;
    if (event.localId !== undefined) out.localId = event.localId;
    if (event.author !== undefined) out.author = event.author;
    if (event.op !== undefined) out.op = event.op;
    if (event.reason !== undefined) out.reason = event.reason;
    return out;
}

export function inProcessClient(host: Host): RhostClient {
    const unsubscribes = new Set<() => void>();

    const requireProjection = (): RdbProjection => {
        const projection = host.projection;
        if (projection === undefined) throw new Error(`host '${host.name}' is not running`);
        return projection;
    };

    const track = (unsubscribe: () => void): (() => void) => {
        const once = () => {
            if (!unsubscribes.delete(once)) return;
            unsubscribe();
        };
        unsubscribes.add(once);
        return once;
    };

    const events: ClientEvents = {
        async since(afterId = 0, limit) {
            const stored = await requireProjection().opEvents({ afterId, order: 'asc', ...(limit !== undefined ? { limit } : {}) });
            return stored.map(toClientEvent);
        },
        watch(listener) {
            const projection = requireProjection();
            let cursor: number | undefined;
            let stopped = false;
            let chain = Promise.resolve();
            const drain = async () => {
                if (cursor === undefined || stopped) return;
                const batch = await projection.opEvents({ afterId: cursor, order: 'asc' });
                if (batch.length === 0 || stopped) return;
                cursor = batch[batch.length - 1]!.id;
                listener(batch.map(toClientEvent));
            };
            // The projection's push only says that events arrived; the batch
            // is read back by id so this watch keeps its own cursor.
            const trigger = () => { chain = chain.then(drain).catch(() => { /* retried on the next push */ }); };
            void (async () => {
                const tail = await projection.opEvents({ order: 'desc', limit: 1 });
                cursor = tail[0]?.id ?? 0;
                if (!stopped) await projection.subscribeOpEvents(trigger);
            })().catch(() => { /* the projection stopped */ });
            return track(() => {
                stopped = true;
                projection.unsubscribeOpEvents(trigger);
            });
        },
    };

    return {
        async me(): Promise<ClientKey> {
            const { label, keyId, publicKey } = host.config.key;
            const id = await requireProjection().registerKey(keyId, publicKey);
            return { label, keyId, publicKey, id };
        },
        async registerKey(publicKey: string): Promise<number> {
            const keyId = keyIdFromPublicKey(deserializePublicKeyFromBase64(publicKey), host.runtime.workspace.replica.getHashSuite());
            return requireProjection().registerKey(keyId, publicKey);
        },
        events,
        async status(): Promise<ClientStatus> {
            return host.status();
        },
        watchStatus(listener) {
            return track(host.onStatus(listener));
        },
        async close(): Promise<void> {
            for (const unsubscribe of [...unsubscribes]) unsubscribe();
        },
    };
}

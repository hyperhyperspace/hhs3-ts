// Opt-in discovery layer that probes existing authenticated connections for
// additional topics. When a swarm runs discover(), this layer iterates pool
// connections, sends a topic_interest control message on each, and yields
// peers that respond with topic_accept. Applications include this in their
// DiscoveryStack at high priority to prefer reusing existing connections over
// opening new ones. Applications concerned about topic cross-correlation
// privacy should omit it.

import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { PeerInfo, PeerDiscovery, TopicId } from './discovery.js';
import type { NetworkAddress } from './transport.js';
import { ConnectionPool } from './connection_pool.js';
import {
    encodeControlTopicInterest, decodeControlPayload,
    CTRL_TOPIC_ACCEPT, CTRL_TOPIC_REJECT,
} from './mux.js';

const DEFAULT_TIMEOUT_MS = 5_000;

export class PoolReuseDiscovery implements PeerDiscovery {

    private pool: ConnectionPool;
    private timeoutMs: number;
    private pending = new Map<string, { resolve: (v: boolean) => void }>();
    private rejected = new Set<string>();

    constructor(pool: ConnectionPool, timeoutMs = DEFAULT_TIMEOUT_MS) {
        this.pool = pool;
        this.timeoutMs = timeoutMs;

        pool.onControlMessage((_connKey, peerId, endpoint, payload) => {
            this.handleControl(peerId, endpoint, payload);
        });

        pool.onDisconnect((connKey) => {
            for (const key of this.rejected) {
                if (key.startsWith(connKey + '#')) this.rejected.delete(key);
            }
        });
    }

    private handleControl(peerId: KeyId, endpoint: NetworkAddress, payload: Uint8Array): void {
        try {
            const ctrl = decodeControlPayload(payload);
            if (ctrl.ctrl !== CTRL_TOPIC_ACCEPT && ctrl.ctrl !== CTRL_TOPIC_REJECT) return;

            const key = `${peerId}@${endpoint}#${ctrl.topic}`;
            const entry = this.pending.get(key);
            if (entry === undefined) return;

            this.pending.delete(key);
            if (ctrl.ctrl === CTRL_TOPIC_REJECT) this.rejected.add(key);
            entry.resolve(ctrl.ctrl === CTRL_TOPIC_ACCEPT);
        } catch {
            // Ignore malformed control messages
        }
    }

    async *discover(topic: TopicId, _schemes?: string[], targetPeers?: number): AsyncIterable<PeerInfo> {
        const limit = targetPeers ?? 10;
        let count = 0;

        for (const conn of this.pool.all()) {
            if (count >= limit) break;
            if (this.pool.hasTopicChannel(conn.peerId, conn.endpoint, topic)) continue;
            if (this.rejected.has(`${conn.peerId}@${conn.endpoint}#${topic}`)) continue;

            try {
                conn.channel.send(encodeControlTopicInterest(topic));
                const accepted = await this.waitForResponse(conn.peerId, conn.endpoint, topic);
                if (accepted) {
                    count++;
                    yield { keyId: conn.peerId, addresses: [conn.endpoint] };
                }
            } catch {
                continue;
            }
        }
    }

    private waitForResponse(peerId: KeyId, endpoint: NetworkAddress, topic: TopicId): Promise<boolean> {
        const key = `${peerId}@${endpoint}#${topic}`;
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pending.delete(key);
                reject(new Error('pool reuse negotiation timeout'));
            }, this.timeoutMs);

            this.pending.set(key, {
                resolve: (v) => { clearTimeout(timer); resolve(v); },
            });
        });
    }

    async announce(_topic: TopicId, _self: PeerInfo): Promise<void> {}
    async leave(_topic: TopicId, _self: KeyId): Promise<void> {}
}

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
import { ConnectionPool, connectionKey } from './connection_pool.js';
import { DiscoveryStack } from './discovery_stack.js';
import {
    encodeControlTopicInterest, decodeControlPayload,
    CTRL_TOPIC_ACCEPT, CTRL_TOPIC_REJECT,
} from './mux.js';
import { TRACE_MESH, trace } from './trace.js';

const DEFAULT_TIMEOUT_MS = 5_000;

export class PoolReuseDiscovery implements PeerDiscovery {

    private pool: ConnectionPool;
    private timeoutMs: number;
    private pending = new Map<string, { resolve: (v: boolean) => void; timer: ReturnType<typeof setTimeout> }>();
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
            for (const key of [...this.pending.keys()]) {
                if (!key.startsWith(connKey + '#')) continue;
                const entry = this.pending.get(key);
                if (entry === undefined) continue;
                clearTimeout(entry.timer);
                this.pending.delete(key);
                entry.resolve(false);
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

            clearTimeout(entry.timer);
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

            const started = Date.now();
            try {
                conn.channel.send(encodeControlTopicInterest(topic));
                const accepted = await this.waitForResponse(conn.peerId, conn.endpoint, topic);
                if (TRACE_MESH) {
                    trace('mesh.reuse probe', {
                        topic,
                        peer: connectionKey(conn.peerId, conn.endpoint),
                        result: accepted ? 'accept' : 'reject',
                        ms: Date.now() - started,
                    });
                }
                if (accepted) {
                    count++;
                    yield { keyId: conn.peerId, addresses: [conn.endpoint] };
                }
            } catch {
                if (TRACE_MESH) {
                    trace('mesh.reuse probe', {
                        topic,
                        peer: connectionKey(conn.peerId, conn.endpoint),
                        result: 'timeout',
                        ms: Date.now() - started,
                    });
                }
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
                timer,
                resolve: (v) => { clearTimeout(timer); resolve(v); },
            });
        });
    }

    async announce(_topic: TopicId, _self: PeerInfo): Promise<void> {}
    async leave(_topic: TopicId, _self: KeyId): Promise<void> {}
}

// Pool reuse has to sit ahead of the other sources, but it needs the mesh
// pool, which does not exist until Mesh is constructed. The returned
// discovery forwards to `fallback` until `attach`, then probes that pool
// before the fallback. Its identity stays stable so Mesh can hold it.
const POOL_REUSE_PRIORITY = -1;
const FALLBACK_PRIORITY = 0;

export interface PoolReuseBinding {
    readonly discovery: PeerDiscovery;
    attach(pool: ConnectionPool): void;
}

export function attachPoolReuse(fallback: PeerDiscovery): PoolReuseBinding {
    let current: PeerDiscovery = fallback;
    let attached = false;
    return {
        discovery: {
            discover: (topic, schemes, targetPeers) => current.discover(topic, schemes, targetPeers),
            announce: (topic, self) => current.announce(topic, self),
            leave: (topic, self) => current.leave(topic, self),
        },
        attach(pool: ConnectionPool): void {
            if (attached) return;
            attached = true;
            current = new DiscoveryStack([
                { source: new PoolReuseDiscovery(pool), priority: POOL_REUSE_PRIORITY },
                { source: fallback, priority: FALLBACK_PRIORITY },
            ]);
        },
    };
}

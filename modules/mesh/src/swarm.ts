// Swarm: manages a peer group for a single topic. Uses the shared
// ConnectionPool for transport reuse and the PeerDiscovery / PeerResolver /
// PeerAuthenticator trio to find, resolve, and authenticate new peers.
// Supports three lifecycle modes: dormant, passive, and active.

import type { KeyId, PublicKey } from '@hyper-hyper-space/hhs3_crypto';
import type { TopicId } from './discovery.js';
import type { PeerDiscovery } from './discovery.js';
import type { PeerResolver } from './resolver.js';
import type { PeerAuthenticator } from './authenticator.js';
import type { TransportProvider } from './transport.js';
import { ConnectionPool, PooledConnection } from './connection_pool.js';

export type SwarmMode = 'dormant' | 'passive' | 'active';

export interface SwarmConfig {
    topic:        TopicId;
    targetPeers?: number;
    mode?:        SwarmMode;
}

type PeerCallback = (peerId: KeyId) => void;

export interface Swarm {
    readonly topic: TopicId;
    readonly mode:  SwarmMode;

    activate():   void;
    deactivate(): void;
    sleep():      void;
    destroy():    void;

    peers(): KeyId[];
    onPeerJoin(callback: PeerCallback):  void;
    onPeerLeave(callback: PeerCallback): void;
}

export interface SwarmDeps {
    pool:          ConnectionPool;
    discovery:     PeerDiscovery;
    resolver:      PeerResolver;
    authenticator: PeerAuthenticator;
    transports:    TransportProvider[];
    localKey:      { publicKey: PublicKey; secretKey: Uint8Array };
}

const DEFAULT_TARGET_PEERS = 6;

export function createSwarm(config: SwarmConfig, deps: SwarmDeps): Swarm {

    const {
        pool, discovery, resolver, authenticator, transports, localKey,
    } = deps;

    const topic       = config.topic;
    const targetPeers = config.targetPeers ?? DEFAULT_TARGET_PEERS;

    let mode: SwarmMode     = config.mode ?? 'dormant';
    let destroyed           = false;

    const swarmPeers   = new Set<KeyId>();
    const joinCallbacks:  PeerCallback[] = [];
    const leaveCallbacks: PeerCallback[] = [];

    let discoveryAbort: AbortController | undefined;

    const schemes = transports.map(t => t.scheme);

    // --- pool listeners ---

    function onPoolConnect(conn: PooledConnection) {
        if (mode === 'dormant' || destroyed) return;
        // In passive or active mode, adopt peers the pool already knows about.
        // Actual interest probing happens in the activation cycle.
        adoptPeer(conn.peerId);
    }

    function onPoolDisconnect(peerId: KeyId) {
        removePeer(peerId);
    }

    pool.onConnect(onPoolConnect);
    pool.onDisconnect(onPoolDisconnect);

    // --- peer tracking ---

    function adoptPeer(peerId: KeyId): boolean {
        if (swarmPeers.has(peerId) || destroyed) return false;
        swarmPeers.add(peerId);
        for (const cb of joinCallbacks) cb(peerId);
        return true;
    }

    function removePeer(peerId: KeyId): boolean {
        if (!swarmPeers.has(peerId)) return false;
        swarmPeers.delete(peerId);
        for (const cb of leaveCallbacks) cb(peerId);
        return true;
    }

    // --- discovery + connect loop ---

    async function runDiscovery() {
        if (mode !== 'active' || destroyed) return;

        // 1. Probe pool for already-connected peers interested in this topic
        const interested = await pool.queryInterest(topic);
        for (const peerId of interested) {
            if (swarmPeers.size >= targetPeers) return;
            adoptPeer(peerId);
        }

        if (swarmPeers.size >= targetPeers) return;

        // 2. Discover new peer identities
        discoveryAbort = new AbortController();
        const signal = discoveryAbort.signal;

        try {
            const candidates: KeyId[] = [];
            for await (const peerId of discovery.discover(topic)) {
                if (signal.aborted) break;
                if (!swarmPeers.has(peerId) && pool.get(peerId) === undefined) {
                    candidates.push(peerId);
                }
                if (candidates.length >= targetPeers * 2) break;
            }

            if (signal.aborted) return;

            // 3. Resolve and connect
            for await (const { peer, addresses } of resolver.resolveAny(candidates, schemes)) {
                if (signal.aborted || swarmPeers.size >= targetPeers) break;

                const connected = await tryConnect(peer, addresses);
                if (connected) {
                    adoptPeer(peer);
                }
            }
        } catch (_e) {
            // Discovery can fail transiently; swarm stays in active mode
            // and the next activation cycle will retry.
        }
    }

    async function tryConnect(peerId: KeyId, addresses: string[]): Promise<boolean> {
        if (pool.get(peerId) !== undefined) {
            adoptPeer(peerId);
            return true;
        }

        for (const addr of addresses) {
            const provider = transports.find(
                t => addr.startsWith(t.scheme + '://')
            );
            if (provider === undefined) continue;

            try {
                const transport = await provider.connect(addr);
                const channel   = await authenticator.authenticate(
                    transport, localKey, peerId
                );
                pool.add(channel);
                return true;
            } catch {
                continue;
            }
        }

        return false;
    }

    // --- mode transitions ---

    function activate() {
        if (destroyed) return;
        mode = 'active';
        runDiscovery();
    }

    function deactivate() {
        if (destroyed) return;
        mode = 'passive';
        discoveryAbort?.abort();
        discoveryAbort = undefined;
    }

    function sleep() {
        if (destroyed) return;
        mode = 'dormant';
        discoveryAbort?.abort();
        discoveryAbort = undefined;
        for (const peerId of Array.from(swarmPeers)) {
            removePeer(peerId);
        }
    }

    function doDestroy() {
        if (destroyed) return;
        destroyed = true;
        discoveryAbort?.abort();
        discoveryAbort = undefined;
        for (const peerId of Array.from(swarmPeers)) {
            removePeer(peerId);
        }
    }

    return {
        get topic() { return topic; },
        get mode()  { return mode; },
        activate,
        deactivate,
        sleep,
        destroy: doDestroy,
        peers:       () => Array.from(swarmPeers),
        onPeerJoin:  (cb: PeerCallback) => { joinCallbacks.push(cb); },
        onPeerLeave: (cb: PeerCallback) => { leaveCallbacks.push(cb); },
    };
}

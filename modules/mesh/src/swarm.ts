// Swarm: manages a peer group for a single topic. Uses the shared
// ConnectionPool for transport reuse and PeerDiscovery / PeerAuthenticator
// to find and authenticate new peers. Supports three lifecycle modes:
// dormant, passive, and active. Peers are tracked by (keyId, endpoint)
// so the same identity on multiple devices gets separate connections.

import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { TopicId, PeerInfo } from './discovery.js';
import type { PeerDiscovery } from './discovery.js';
import type { PeerAuthenticator } from './authenticator.js';
import type { TransportProvider, NetworkAddress } from './transport.js';
import type { TopicChannel } from './mux.js';
import { ConnectionPool, PooledConnection, connectionKey } from './connection_pool.js';

export type SwarmMode = 'dormant' | 'passive' | 'active';

export interface PeerAuthorizer {
    authorize(keyId: KeyId): Promise<boolean>;
}

export interface SwarmConfig {
    topic:        TopicId;
    targetPeers?: number;
    mode?:        SwarmMode;
    authorizer?:  PeerAuthorizer;
}

export interface SwarmPeer {
    keyId:    KeyId;
    endpoint: NetworkAddress;
    channel:  TopicChannel;
}

type PeerCallback = (peer: SwarmPeer) => void;

export interface Swarm {
    readonly topic: TopicId;
    readonly mode:  SwarmMode;

    activate():   void;
    deactivate(): void;
    sleep():      void;
    destroy():    void;

    peers(): SwarmPeer[];
    onPeerJoin(callback: PeerCallback):  void;
    onPeerLeave(callback: PeerCallback): void;
    blockPeer(keyId: KeyId, endpoint: NetworkAddress): void;
    wouldAccept(keyId: KeyId): Promise<boolean>;
    adopt(keyId: KeyId, endpoint: NetworkAddress): boolean;
}

export interface SwarmDeps {
    pool:          ConnectionPool;
    discovery:     PeerDiscovery;
    authenticator: PeerAuthenticator;
    transports:    TransportProvider[];
    localPeer?:    PeerInfo;
}

const DEFAULT_TARGET_PEERS = 6;

export function createSwarm(config: SwarmConfig, deps: SwarmDeps): Swarm {

    const { pool, discovery, authenticator, transports, localPeer } = deps;

    const topic       = config.topic;
    const targetPeers = config.targetPeers ?? DEFAULT_TARGET_PEERS;
    const authorizer  = config.authorizer;

    let mode: SwarmMode     = config.mode ?? 'dormant';
    let destroyed           = false;

    const swarmPeers    = new Map<string, SwarmPeer>();
    const blockedPeers  = new Set<string>();
    const joinCallbacks:  PeerCallback[] = [];
    const leaveCallbacks: PeerCallback[] = [];

    let discoveryAbort: AbortController | undefined;

    const schemes = transports.map(t => t.scheme);

    // --- pool listeners ---

    function onPoolConnect(conn: PooledConnection) {
        if (mode === 'dormant' || destroyed) return;
        if (authorizer !== undefined) {
            authorizer.authorize(conn.peerId).then((ok) => {
                if (ok && !destroyed && mode !== 'dormant') {
                    adoptPeer(conn.peerId, conn.endpoint);
                }
            }).catch(() => {});
            return;
        }
        adoptPeer(conn.peerId, conn.endpoint);
    }

    function onPoolDisconnect(connKey: string) {
        const peer = swarmPeers.get(connKey);
        if (peer !== undefined) {
            removePeer(connKey);
        }
    }

    pool.onConnect(onPoolConnect);
    pool.onDisconnect(onPoolDisconnect);

    // --- peer tracking ---

    function isKeyIdBlocked(keyId: KeyId): boolean {
        for (const blocked of blockedPeers) {
            if (blocked.startsWith(keyId + '@')) return true;
        }
        return false;
    }

    function blockPeer(keyId: KeyId, endpoint: NetworkAddress): void {
        const key = connectionKey(keyId, endpoint);
        blockedPeers.add(key);
        removePeer(key);
    }

    function adoptPeer(keyId: KeyId, endpoint: NetworkAddress): boolean {
        const key = connectionKey(keyId, endpoint);
        if (swarmPeers.has(key) || destroyed || blockedPeers.has(key)) return false;
        if (swarmPeers.size >= targetPeers) return false;

        const conn = pool.get(keyId, endpoint);
        if (conn === undefined) return false;

        const channel = pool.openTopic(keyId, endpoint, topic);
        const swarmPeer: SwarmPeer = { keyId, endpoint, channel };
        swarmPeers.set(key, swarmPeer);
        for (const cb of joinCallbacks) cb(swarmPeer);
        return true;
    }

    function removePeer(key: string): boolean {
        const peer = swarmPeers.get(key);
        if (peer === undefined) return false;
        swarmPeers.delete(key);
        peer.channel.close();
        for (const cb of leaveCallbacks) cb(peer);
        return true;
    }

    async function wouldAccept(keyId: KeyId): Promise<boolean> {
        if (mode === 'dormant' || destroyed) return false;
        if (isKeyIdBlocked(keyId)) return false;
        if (swarmPeers.size >= targetPeers) return false;
        if (authorizer !== undefined) {
            return authorizer.authorize(keyId);
        }
        return true;
    }

    // --- discovery + connect loop ---

    async function runDiscovery() {
        if (mode !== 'active' || destroyed) return;

        discoveryAbort = new AbortController();
        const signal = discoveryAbort.signal;

        try {
            const candidates: PeerInfo[] = [];
            for await (const peerInfo of discovery.discover(topic, schemes)) {
                if (signal.aborted) break;

                if (authorizer !== undefined) {
                    const ok = await authorizer.authorize(peerInfo.keyId);
                    if (!ok) continue;
                }

                for (const addr of peerInfo.addresses) {
                    const key = connectionKey(peerInfo.keyId, addr);
                    if (!swarmPeers.has(key) && !blockedPeers.has(key)) {
                        if (pool.get(peerInfo.keyId, addr) !== undefined) {
                            adoptPeer(peerInfo.keyId, addr);
                        } else {
                            candidates.push({ keyId: peerInfo.keyId, addresses: [addr] });
                        }
                    }
                }

                if (swarmPeers.size >= targetPeers) break;
                if (candidates.length >= targetPeers * 2) break;
            }

            if (signal.aborted) return;

            for (const candidate of candidates) {
                if (signal.aborted || swarmPeers.size >= targetPeers) break;
                await tryConnect(candidate);
            }
        } catch (_e) {
            // Discovery can fail transiently; swarm stays in active mode
            // and the next activation cycle will retry.
        }
    }

    async function tryConnect(peerInfo: PeerInfo): Promise<boolean> {
        for (const addr of peerInfo.addresses) {
            if (pool.get(peerInfo.keyId, addr) !== undefined) {
                return adoptPeer(peerInfo.keyId, addr);
            }

            const provider = transports.find(
                t => addr.startsWith(t.scheme + '://')
            );
            if (provider === undefined) continue;

            try {
                const transport = await provider.connect(addr);
                const channel   = await authenticator.authenticate(
                    transport, 'initiator', peerInfo.keyId
                );
                pool.add(channel, addr);
                adoptPeer(peerInfo.keyId, addr);
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
        if (localPeer !== undefined) {
            discovery.announce(topic, localPeer).catch(() => {});
        }
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
        if (localPeer !== undefined) {
            discovery.leave(topic, localPeer.keyId).catch(() => {});
        }
        for (const key of Array.from(swarmPeers.keys())) {
            removePeer(key);
        }
    }

    function doDestroy() {
        if (destroyed) return;
        destroyed = true;
        discoveryAbort?.abort();
        discoveryAbort = undefined;
        if (localPeer !== undefined) {
            discovery.leave(topic, localPeer.keyId).catch(() => {});
        }
        for (const key of Array.from(swarmPeers.keys())) {
            removePeer(key);
        }
    }

    return {
        get topic() { return topic; },
        get mode()  { return mode; },
        activate,
        deactivate,
        sleep,
        destroy: doDestroy,
        peers:       () => Array.from(swarmPeers.values()),
        onPeerJoin:  (cb: PeerCallback) => { joinCallbacks.push(cb); },
        onPeerLeave: (cb: PeerCallback) => { leaveCallbacks.push(cb); },
        blockPeer,
        wouldAccept,
        adopt: adoptPeer,
    };
}

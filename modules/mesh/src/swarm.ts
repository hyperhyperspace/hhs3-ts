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
import { TRACE_MESH, TRACE_SWARM, trace } from './trace.js';

export type SwarmMode = 'dormant' | 'passive' | 'active';

export interface PeerAuthorizer {
    authorize(keyId: KeyId): Promise<boolean>;
}

export interface SwarmConfig {
    topic:        TopicId;
    // Outbound seek goal (low water): the swarm stops initiating new dials once
    // it holds this many peers. Defaults to DEFAULT_TARGET_PEERS.
    targetPeers?: number;
    // Hard cap (high water): no peer is adopted once the swarm holds this many.
    // The band (targetPeers, maxPeers] is reserved for inbound / pool adoption,
    // since outbound seeking stops at targetPeers. Defaults to targetPeers + 2.
    maxPeers?:    number;
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
const DEFAULT_MAX_PEERS_MARGIN = 2;
const REFILL_DEBOUNCE_MS = 150;

export function createSwarm(config: SwarmConfig, deps: SwarmDeps): Swarm {

    const { pool, discovery, authenticator, transports, localPeer } = deps;

    const topic       = config.topic;
    const targetPeers = config.targetPeers ?? DEFAULT_TARGET_PEERS;
    const maxPeers    = config.maxPeers ?? targetPeers + DEFAULT_MAX_PEERS_MARGIN;
    if (maxPeers <= targetPeers) {
        throw new Error(`maxPeers (${maxPeers}) must be greater than targetPeers (${targetPeers})`);
    }
    const authorizer  = config.authorizer;

    let mode: SwarmMode     = config.mode ?? 'dormant';
    let destroyed           = false;
    let discovering         = false;
    let inFlightDials       = 0;
    let refillTimer: ReturnType<typeof setTimeout> | undefined;

    const swarmPeers    = new Map<string, SwarmPeer>();
    const blockedPeers  = new Set<string>();
    const joinCallbacks:  PeerCallback[] = [];
    const leaveCallbacks: PeerCallback[] = [];

    let discoveryAbort: AbortController | undefined;

    const schemes = transports.map(t => t.scheme);

    // A candidate is "self" when it carries our own keyId at an address we
    // listen on / announce. Same keyId at a different address is a valid peer
    // (multi-device), so we key on (keyId, addr), not keyId alone.
    function isSelfEndpoint(keyId: KeyId, addr: NetworkAddress): boolean {
        return localPeer !== undefined
            && keyId === localPeer.keyId
            && localPeer.addresses.includes(addr);
    }

    // --- pool listeners ---

    function onPoolConnect(conn: PooledConnection) {
        if (mode === 'dormant' || destroyed) return;
        if (authorizer !== undefined) {
            authorizer.authorize(conn.peerId).then((ok) => {
                if (ok && !destroyed && mode !== 'dormant') {
                    adoptPeer(conn.peerId, conn.endpoint, 'pool');
                } else if (!ok && TRACE_SWARM) {
                    trace('swarm.peer skip', {
                        topic,
                        peer: connectionKey(conn.peerId, conn.endpoint),
                        why: 'authorizer',
                    });
                }
            }).catch(() => {});
            return;
        }
        adoptPeer(conn.peerId, conn.endpoint, 'pool');
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

    function adoptPeer(keyId: KeyId, endpoint: NetworkAddress, via: 'pool' | 'dial' = 'pool'): boolean {
        const key = connectionKey(keyId, endpoint);
        if (swarmPeers.has(key) || destroyed) return false;
        if (blockedPeers.has(key)) {
            if (TRACE_SWARM) trace('swarm.peer skip', { topic, peer: key, why: 'blocked' });
            return false;
        }
        if (swarmPeers.size >= maxPeers) {
            if (TRACE_SWARM) trace('swarm.peer skip', { topic, peer: key, why: 'maxPeers' });
            return false;
        }

        const conn = pool.get(keyId, endpoint);
        if (conn === undefined) return false;

        const channel = pool.openTopic(keyId, endpoint, topic);
        const swarmPeer: SwarmPeer = { keyId, endpoint, channel };
        swarmPeers.set(key, swarmPeer);
        if (TRACE_SWARM) trace('swarm.peer join', { topic, peer: key, via });
        for (const cb of joinCallbacks) cb(swarmPeer);
        return true;
    }

    function removePeer(key: string): boolean {
        const peer = swarmPeers.get(key);
        if (peer === undefined) return false;
        swarmPeers.delete(key);
        peer.channel.close();
        if (TRACE_SWARM) trace('swarm.peer leave', { topic, peer: key });
        for (const cb of leaveCallbacks) cb(peer);
        // A leave may drop us below the seek goal; refill (debounced) so a flap
        // does not hammer discovery. Guards ensure sleep/destroy do not refill.
        maybeSeekPeers();
        return true;
    }

    // Low-water refill: when active and below the seek goal, run another
    // discovery pass. Debounced, and skipped while a pass is already running.
    function maybeSeekPeers(): void {
        if (mode !== 'active' || destroyed || discovering) return;
        if (swarmPeers.size >= targetPeers) return;
        if (refillTimer !== undefined) return;
        refillTimer = setTimeout(() => {
            refillTimer = undefined;
            runDiscovery();
        }, REFILL_DEBOUNCE_MS);
    }

    async function wouldAccept(keyId: KeyId): Promise<boolean> {
        if (mode === 'dormant' || destroyed) return false;
        if (isKeyIdBlocked(keyId)) return false;
        if (swarmPeers.size >= maxPeers) return false;
        if (authorizer !== undefined) {
            return authorizer.authorize(keyId);
        }
        return true;
    }

    // --- discovery + connect loop ---

    async function runDiscovery() {
        if (mode !== 'active' || destroyed || discovering) return;

        discovering = true;
        discoveryAbort = new AbortController();
        const signal = discoveryAbort.signal;

        try {
            const started = Date.now();
            if (TRACE_SWARM) trace('swarm.discover start', { topic });

            // Hint discovery sources how many more peers we want to reach the
            // seek goal. Sources like DiscoveryStack use this to stop early.
            const deficit = Math.max(0, targetPeers - swarmPeers.size);

            const candidates: PeerInfo[] = [];
            let found = 0;
            for await (const peerInfo of discovery.discover(topic, schemes, deficit)) {
                if (signal.aborted) break;
                found++;

                if (authorizer !== undefined) {
                    const ok = await authorizer.authorize(peerInfo.keyId);
                    if (!ok) {
                        if (TRACE_SWARM) {
                            trace('swarm.peer skip', { topic, peer: peerInfo.keyId, why: 'authorizer' });
                        }
                        continue;
                    }
                }

                for (const addr of peerInfo.addresses) {
                    if (isSelfEndpoint(peerInfo.keyId, addr)) {
                        if (TRACE_SWARM) {
                            trace('swarm.peer skip', { topic, to: addr, peer: peerInfo.keyId, why: 'self' });
                        }
                        continue;
                    }
                    const key = connectionKey(peerInfo.keyId, addr);
                    if (!swarmPeers.has(key) && !blockedPeers.has(key)) {
                        if (pool.get(peerInfo.keyId, addr) !== undefined) {
                            if (TRACE_SWARM) {
                                trace('swarm.connect skip', { topic, to: addr, peer: key });
                            }
                            adoptPeer(peerInfo.keyId, addr, 'pool');
                        } else {
                            candidates.push({ keyId: peerInfo.keyId, addresses: [addr] });
                        }
                    }
                }

                // Stop collecting once the seek goal is met or we have a
                // healthy budget of dial candidates queued.
                if (swarmPeers.size >= targetPeers) break;
                if (candidates.length >= targetPeers * 2) break;
            }

            if (signal.aborted) return;

            if (TRACE_SWARM) {
                trace('swarm.discover ok', {
                    topic,
                    found,
                    candidates: candidates.length,
                    ms: Date.now() - started,
                });
            }

            await dialCandidates(candidates, signal);
        } catch (_e) {
            // Discovery can fail transiently; swarm stays in active mode
            // and the next activation cycle will retry.
        } finally {
            discovering = false;
        }
    }

    // Dial candidates in parallel, keeping several connects in flight at once so
    // a slow/dead address does not block a live one. The targetPeers seek goal
    // is enforced here (not in adoptPeer): a new dial only starts while
    // swarmPeers.size + inFlightDials < targetPeers, so pure-outbound growth
    // cannot exceed targetPeers and the (targetPeers, maxPeers] band stays free
    // for inbound. Concurrent inbound joins reduce how many more we initiate.
    async function dialCandidates(candidates: PeerInfo[], signal: AbortSignal): Promise<void> {
        const queue = [...candidates];

        async function worker(): Promise<void> {
            while (!signal.aborted && !destroyed) {
                if (swarmPeers.size + inFlightDials >= targetPeers) return;
                const candidate = queue.shift();
                if (candidate === undefined) return;

                inFlightDials++;
                try {
                    await tryConnect(candidate);
                } finally {
                    inFlightDials--;
                }
            }
        }

        const slots = Math.max(0, Math.min(queue.length, targetPeers - swarmPeers.size));
        if (slots === 0) return;
        await Promise.all(Array.from({ length: slots }, () => worker()));
    }

    async function tryConnect(peerInfo: PeerInfo): Promise<boolean> {
        for (const addr of peerInfo.addresses) {
            if (isSelfEndpoint(peerInfo.keyId, addr)) {
                if (TRACE_SWARM) {
                    trace('swarm.connect skip', { topic, to: addr, peer: connectionKey(peerInfo.keyId, addr), why: 'self' });
                }
                continue;
            }
            const key = connectionKey(peerInfo.keyId, addr);
            if (pool.get(peerInfo.keyId, addr) !== undefined) {
                if (TRACE_SWARM) trace('swarm.connect skip', { topic, to: addr, peer: key });
                return adoptPeer(peerInfo.keyId, addr, 'pool');
            }

            const provider = transports.find(
                t => addr.startsWith(t.scheme + '://')
            );
            if (provider === undefined) continue;

            const started = Date.now();
            let connected = false;
            if (TRACE_MESH) {
                trace('mesh.connect start', { to: addr, reusedPool: false });
            }
            try {
                const localAddr = localPeer?.addresses.find(
                    a => a.startsWith(provider.scheme + '://')
                );
                const transport = await provider.connect(addr, localAddr);
                connected = true;
                if (TRACE_MESH) {
                    trace('mesh.connect ok', {
                        to: addr,
                        reusedPool: false,
                        ms: Date.now() - started,
                    });
                }
                const channel   = await authenticator.authenticate(
                    transport, 'initiator', peerInfo.keyId
                );
                pool.add(channel, addr);
                adoptPeer(peerInfo.keyId, addr, 'dial');
                return true;
            } catch (err) {
                if (!connected && TRACE_MESH) {
                    trace('mesh.connect fail', {
                        to: addr,
                        reusedPool: false,
                        ms: Date.now() - started,
                        err: err instanceof Error ? err.message : String(err),
                    });
                }
                continue;
            }
        }

        return false;
    }

    // --- mode transitions ---

    function activate() {
        if (destroyed) return;
        mode = 'active';
        if (TRACE_SWARM) trace('swarm.activate', { topic });
        // Only announce when we have a dialable address. A dial-out-only mesh
        // (empty listen/announce set) must not register useless presence on a
        // tracker: peers could not reach it anyway.
        if (localPeer !== undefined && localPeer.addresses.length > 0) {
            discovery.announce(topic, localPeer).then(() => {
                if (TRACE_SWARM) trace('swarm.announce', { topic });
            }).catch(() => {});
        }
        runDiscovery();
    }

    function clearRefillTimer() {
        if (refillTimer !== undefined) {
            clearTimeout(refillTimer);
            refillTimer = undefined;
        }
    }

    function deactivate() {
        if (destroyed) return;
        mode = 'passive';
        clearRefillTimer();
        discoveryAbort?.abort();
        discoveryAbort = undefined;
    }

    function sleep() {
        if (destroyed) return;
        mode = 'dormant';
        clearRefillTimer();
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
        clearRefillTimer();
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

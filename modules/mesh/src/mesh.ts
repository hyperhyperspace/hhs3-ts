// Top-level facade for a single network environment. Each Mesh instance owns
// its own ConnectionPool and uses a fixed set of transports, a single
// discovery service, and a single authenticator. Create one Mesh per network
// environment (e.g. local LAN, public internet, private device sync).
//
// When listenAddresses are provided, the Mesh accepts incoming connections,
// runs topic negotiation via the mux control channel, and delegates
// authorization to per-swarm PeerAuthorizers.

import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { PeerDiscovery, PeerInfo } from './discovery.js';
import type { TopicId } from './discovery.js';
import type { PeerAuthenticator } from './authenticator.js';
import type { Transport, TransportProvider, NetworkAddress } from './transport.js';
import { ConnectionPool } from './connection_pool.js';
import { createSwarm, Swarm, SwarmMode, PeerAuthorizer } from './swarm.js';
import {
    decodeMessage, decodeControlPayload, MSG_TYPE_CONTROL,
    CTRL_TOPIC_INTEREST, CTRL_TOPIC_ACCEPT,
    encodeControlTopicInterest, encodeControlTopicAccept, encodeControlTopicReject,
    awaitMessage,
} from './mux.js';

const DEFAULT_NEGOTIATION_TIMEOUT_MS = 10_000;

export interface MeshConfig {
    transports:             TransportProvider[];
    discovery:              PeerDiscovery;
    authenticator:          PeerAuthenticator;
    localKeyId?:            KeyId;
    listenAddresses?:       NetworkAddress[];
    negotiationTimeoutMs?:  number;
}

export class Mesh {

    readonly pool: ConnectionPool;

    private config: MeshConfig;
    private activeSwarms: Swarm[] = [];
    private closed = false;
    private localPeer: PeerInfo | undefined;
    private negotiationTimeoutMs: number;

    constructor(config: MeshConfig) {
        this.config = config;
        this.negotiationTimeoutMs = config.negotiationTimeoutMs ?? DEFAULT_NEGOTIATION_TIMEOUT_MS;
        this.pool = new ConnectionPool();

        if (config.localKeyId !== undefined) {
            this.localPeer = {
                keyId: config.localKeyId,
                addresses: config.listenAddresses ?? [],
            };
        }

        this.startListening();
        this.registerPoolControlHandler();
    }

    createSwarm(
        topic: TopicId,
        opts?: { targetPeers?: number; mode?: SwarmMode; authorizer?: PeerAuthorizer },
    ): Swarm {
        if (this.closed) throw new Error('mesh is closed');

        const innerAuth = this.config.authenticator;
        const timeoutMs = this.negotiationTimeoutMs;

        const negotiatingAuth: PeerAuthenticator = {
            async authenticate(transport: Transport, role: 'initiator' | 'responder', expectedRemote?: KeyId) {
                const channel = await innerAuth.authenticate(transport, role, expectedRemote);
                if (role === 'initiator') {
                    // Defer to the macro-task queue so the responder's
                    // handshake microtask chain finishes and its
                    // EncryptedChannel is ready to receive.
                    await new Promise(resolve => setTimeout(resolve, 0));
                    channel.send(encodeControlTopicInterest(topic));
                    const response = await awaitMessage(channel, timeoutMs);
                    const decoded = decodeMessage(response);
                    if (decoded.type !== MSG_TYPE_CONTROL) {
                        channel.close();
                        throw new Error('expected control message during topic negotiation');
                    }
                    const ctrl = decodeControlPayload(decoded.payload);
                    if (ctrl.ctrl !== CTRL_TOPIC_ACCEPT || ctrl.topic !== topic) {
                        channel.close();
                        throw new Error('topic rejected by remote');
                    }
                }
                return channel;
            },
        };

        const swarm = createSwarm(
            {
                topic,
                targetPeers: opts?.targetPeers,
                mode: opts?.mode,
                authorizer: opts?.authorizer,
            },
            {
                pool:          this.pool,
                discovery:     this.config.discovery,
                authenticator: negotiatingAuth,
                transports:    this.config.transports,
                localPeer:     this.localPeer,
            },
        );

        this.activeSwarms.push(swarm);
        return swarm;
    }

    swarms(): Swarm[] {
        return [...this.activeSwarms];
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;

        for (const swarm of this.activeSwarms) {
            swarm.destroy();
        }
        this.activeSwarms = [];

        this.pool.close();

        for (const t of this.config.transports) {
            t.close();
        }
    }

    // --- topic interest evaluation (shared by inbound + pool-reuse responders) ---

    private async evaluateTopicInterest(topic: TopicId, peerId: KeyId): Promise<Swarm | undefined> {
        const swarm = this.activeSwarms.find(s => s.topic === topic);
        if (swarm === undefined) return undefined;
        if (!(await swarm.wouldAccept(peerId))) return undefined;
        return swarm;
    }

    // --- incoming connection handling ---

    private startListening(): void {
        const addrs = this.config.listenAddresses;
        if (addrs === undefined) return;

        for (const addr of addrs) {
            const provider = this.config.transports.find(
                t => addr.startsWith(t.scheme + '://')
            );
            if (provider === undefined) continue;
            provider.listen(addr, (transport) => {
                this.handleIncoming(transport, addr).catch(() => {});
            }).catch(() => {});
        }
    }

    private async handleIncoming(transport: Transport, endpoint: NetworkAddress): Promise<void> {
        if (this.closed) { transport.close(); return; }

        const channel = await this.config.authenticator.authenticate(transport, 'responder');

        const msg = await awaitMessage(channel, this.negotiationTimeoutMs);
        const decoded = decodeMessage(msg);
        if (decoded.type !== MSG_TYPE_CONTROL) {
            channel.close();
            return;
        }

        const ctrl = decodeControlPayload(decoded.payload);
        if (ctrl.ctrl !== CTRL_TOPIC_INTEREST) {
            channel.close();
            return;
        }

        const swarm = await this.evaluateTopicInterest(ctrl.topic, channel.remoteKeyId);
        if (swarm === undefined) {
            channel.send(encodeControlTopicReject(ctrl.topic));
            channel.close();
            return;
        }

        channel.send(encodeControlTopicAccept(ctrl.topic));
        this.pool.add(channel, endpoint);
    }

    // --- control messages on established connections (for pool reuse) ---

    private registerPoolControlHandler(): void {
        this.pool.onControlMessage((_connKey, peerId, endpoint, payload) => {
            this.handlePoolControl(peerId, endpoint, payload).catch(() => {});
        });
    }

    private async handlePoolControl(peerId: KeyId, endpoint: NetworkAddress, payload: Uint8Array): Promise<void> {
        const ctrl = decodeControlPayload(payload);
        if (ctrl.ctrl !== CTRL_TOPIC_INTEREST) return;

        const conn = this.pool.get(peerId, endpoint);
        if (conn === undefined) return;

        const swarm = await this.evaluateTopicInterest(ctrl.topic, peerId);
        if (swarm !== undefined) {
            conn.channel.send(encodeControlTopicAccept(ctrl.topic));
            swarm.adopt(peerId, endpoint);
        } else {
            conn.channel.send(encodeControlTopicReject(ctrl.topic));
        }
    }
}

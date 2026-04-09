// Tracker-based PeerDiscovery implementation. Connects on demand to a remote
// tracker server over Transport + Noise, sends list-based protocol messages,
// and disconnects after each exchange. A periodic heartbeat re-announces all
// active topics to keep registrations alive.

import type {
    TopicId, PeerInfo, PeerDiscovery,
    NetworkAddress, TransportProvider, PeerAuthenticator,
} from '@hyper-hyper-space/hhs3_mesh';
import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import {
    encodeMessage, decodeResponse,
    type TrackerRequest, type TrackerResponse,
    type AnnounceRequest, type QueryRequest, type LeaveRequest,
} from './protocol.js';

export interface TrackerClientConfig {
    trackerAddress: NetworkAddress;
    trackerKeyId?: KeyId;
    transportProvider: TransportProvider;
    authenticator: PeerAuthenticator;
    localPeer: PeerInfo;
    announceTtl?: number;
    heartbeatInterval?: number;
}

const DEFAULT_TTL = 180;
const DEFAULT_HEARTBEAT_MS = 60_000;

export class TrackerClient implements PeerDiscovery {
    private readonly address: NetworkAddress;
    private readonly trackerKeyId?: KeyId;
    private readonly transport: TransportProvider;
    private readonly authenticator: PeerAuthenticator;
    private readonly localPeer: PeerInfo;
    private readonly requestedTtl: number;
    private readonly heartbeatMs: number;

    private activeTopics = new Map<TopicId, number>();
    private heartbeatTimer: ReturnType<typeof setInterval> | null = null;

    constructor(config: TrackerClientConfig) {
        this.address = config.trackerAddress;
        this.trackerKeyId = config.trackerKeyId;
        this.transport = config.transportProvider;
        this.authenticator = config.authenticator;
        this.localPeer = config.localPeer;
        this.requestedTtl = config.announceTtl ?? DEFAULT_TTL;
        this.heartbeatMs = config.heartbeatInterval ?? DEFAULT_HEARTBEAT_MS;
    }

    // -- PeerDiscovery ---------------------------------------------------------

    async *discover(
        topic: TopicId,
        schemes?: string[],
        targetPeers?: number,
    ): AsyncIterable<PeerInfo> {
        const req: QueryRequest = { type: 'query', topics: [topic], schemes };
        const res = await this.exchange(req);
        if (res.type === 'error') throw new Error(res.message);
        if (res.type !== 'query_response') throw new Error('unexpected response');
        const peers = res.results[topic] ?? [];
        let count = 0;
        for (const peer of peers) {
            if (targetPeers !== undefined && count >= targetPeers) break;
            yield peer;
            count++;
        }
    }

    async announce(topic: TopicId, _self: PeerInfo): Promise<void> {
        const req: AnnounceRequest = {
            type: 'announce',
            entries: [{ topic, ttl: this.requestedTtl }],
            peer: this.localPeer,
        };
        const res = await this.exchange(req);
        if (res.type === 'error') throw new Error(res.message);
        if (res.type !== 'announce_ack') throw new Error('unexpected response');
        this.activeTopics.set(topic, res.ttls[0] ?? this.requestedTtl);
        this.ensureHeartbeat();
    }

    async leave(topic: TopicId, _self: KeyId): Promise<void> {
        const req: LeaveRequest = { type: 'leave', topics: [topic] };
        const res = await this.exchange(req);
        if (res.type === 'error') throw new Error(res.message);
        this.activeTopics.delete(topic);
        if (this.activeTopics.size === 0) this.stopHeartbeat();
    }

    // -- lifecycle -------------------------------------------------------------

    async close(): Promise<void> {
        this.stopHeartbeat();
        if (this.activeTopics.size === 0) return;
        const topics = [...this.activeTopics.keys()];
        this.activeTopics.clear();
        const req: LeaveRequest = { type: 'leave', topics };
        try {
            await this.exchange(req);
        } catch {
            // best-effort; the TTL will expire on the server
        }
    }

    // -- internal -------------------------------------------------------------

    /** Connect, register a response listener, send the request, await the
     *  response, then close. A macrotask yield after authentication gives
     *  the responder time to finish its handshake and register its message
     *  handler (needed with synchronous transports where the Noise initiator
     *  completes before the responder). */
    private async exchange(req: TrackerRequest): Promise<TrackerResponse> {
        const raw = await this.transport.connect(this.address);
        const channel = await this.authenticator.authenticate(
            raw, 'initiator', this.trackerKeyId,
        );
        try {
            await new Promise(resolve => setTimeout(resolve, 0));
            const responsePromise = new Promise<TrackerResponse>((resolve, reject) => {
                channel.onMessage((data) => resolve(decodeResponse(data)));
                channel.onClose(() => reject(new Error('channel closed before response')));
            });
            channel.send(encodeMessage(req));
            return await responsePromise;
        } finally {
            channel.close();
        }
    }

    private ensureHeartbeat(): void {
        if (this.heartbeatTimer !== null) return;
        this.heartbeatTimer = setInterval(() => this.heartbeat(), this.heartbeatMs);
        if (typeof this.heartbeatTimer === 'object' && 'unref' in this.heartbeatTimer) {
            (this.heartbeatTimer as NodeJS.Timeout).unref();
        }
    }

    private stopHeartbeat(): void {
        if (this.heartbeatTimer !== null) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
    }

    private async heartbeat(): Promise<void> {
        if (this.activeTopics.size === 0) return;
        const entries = [...this.activeTopics.entries()].map(
            ([topic, ttl]) => ({ topic, ttl }),
        );
        const req: AnnounceRequest = {
            type: 'announce',
            entries,
            peer: this.localPeer,
        };
        try {
            const res = await this.exchange(req);
            if (res.type === 'announce_ack') {
                for (let i = 0; i < entries.length; i++) {
                    if (res.ttls[i] !== undefined) {
                        this.activeTopics.set(entries[i].topic, res.ttls[i]);
                    }
                }
            }
        } catch {
            // transient failure; will retry on next tick
        }
    }
}

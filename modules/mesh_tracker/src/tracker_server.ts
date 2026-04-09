// Tracker server: accepts authenticated connections, maintains an in-memory
// registry of (topic, peer) entries with TTL-based expiry, and responds to
// ANNOUNCE / QUERY / LEAVE requests from tracker clients.

import type {
    TransportProvider, PeerAuthenticator, AuthenticatedChannel,
    NetworkAddress, TopicId, PeerInfo,
} from '@hyper-hyper-space/hhs3_mesh';
import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import {
    decodeRequest, encodeMessage,
    type TrackerRequest, type AnnounceAck, type QueryResponse,
    type LeaveAck, type ErrorResponse,
} from '@hyper-hyper-space/hhs3_mesh_tracker_client';

export interface TrackerServerConfig {
    transportProvider: TransportProvider;
    authenticator: PeerAuthenticator;
    listenAddress: NetworkAddress;
    ttlMin?: number;
    ttlMax?: number;
    sweepInterval?: number;
}

interface RegistryEntry {
    info: PeerInfo;
    expiresAt: number;
}

const DEFAULT_TTL_MIN = 60;
const DEFAULT_TTL_MAX = 600;
const DEFAULT_SWEEP_MS = 30_000;

export class TrackerServer {
    private readonly transportProvider: TransportProvider;
    private readonly authenticator: PeerAuthenticator;
    private readonly listenAddress: NetworkAddress;
    private readonly ttlMin: number;
    private readonly ttlMax: number;
    private readonly sweepMs: number;

    private registry = new Map<string, Map<string, RegistryEntry>>();
    private sweepTimer: ReturnType<typeof setInterval> | null = null;

    constructor(config: TrackerServerConfig) {
        this.transportProvider = config.transportProvider;
        this.authenticator = config.authenticator;
        this.listenAddress = config.listenAddress;
        this.ttlMin = config.ttlMin ?? DEFAULT_TTL_MIN;
        this.ttlMax = config.ttlMax ?? DEFAULT_TTL_MAX;
        this.sweepMs = config.sweepInterval ?? DEFAULT_SWEEP_MS;
    }

    async start(): Promise<void> {
        await this.transportProvider.listen(this.listenAddress, (transport) => {
            this.handleConnection(transport);
        });
        this.sweepTimer = setInterval(() => this.sweep(), this.sweepMs);
        if (typeof this.sweepTimer === 'object' && 'unref' in this.sweepTimer) {
            (this.sweepTimer as NodeJS.Timeout).unref();
        }
    }

    stop(): void {
        if (this.sweepTimer !== null) {
            clearInterval(this.sweepTimer);
            this.sweepTimer = null;
        }
        this.transportProvider.close();
        this.registry.clear();
    }

    registrySize(): number {
        let count = 0;
        for (const topicMap of this.registry.values()) count += topicMap.size;
        return count;
    }

    // -- connection handler ----------------------------------------------------

    private async handleConnection(transport: import('@hyper-hyper-space/hhs3_mesh').Transport): Promise<void> {
        let channel: AuthenticatedChannel;
        try {
            channel = await this.authenticator.authenticate(transport, 'responder');
        } catch {
            try { transport.close(); } catch {}
            return;
        }

        channel.onMessage((data) => {
            try {
                const req = decodeRequest(data);
                const res = this.processRequest(req, channel.remoteKeyId);
                channel.send(encodeMessage(res));
            } catch {
                const err: ErrorResponse = { type: 'error', message: 'invalid request' };
                try { channel.send(encodeMessage(err)); } catch {}
            }
        });
    }

    private processRequest(
        req: TrackerRequest,
        clientKeyId: KeyId,
    ): AnnounceAck | QueryResponse | LeaveAck | ErrorResponse {
        switch (req.type) {
            case 'announce':
                return this.handleAnnounce(req, clientKeyId);
            case 'query':
                return this.handleQuery(req);
            case 'leave':
                return this.handleLeave(req, clientKeyId);
        }
    }

    private handleAnnounce(
        req: import('@hyper-hyper-space/hhs3_mesh_tracker_client').AnnounceRequest,
        clientKeyId: KeyId,
    ): AnnounceAck | ErrorResponse {
        if (req.peer.keyId !== clientKeyId) {
            return { type: 'error', message: 'peer keyId does not match authenticated identity' };
        }

        const ttls: number[] = [];
        const now = Date.now();
        for (const entry of req.entries) {
            const clamped = Math.max(this.ttlMin, Math.min(this.ttlMax, entry.ttl));
            ttls.push(clamped);

            let topicMap = this.registry.get(entry.topic);
            if (!topicMap) {
                topicMap = new Map();
                this.registry.set(entry.topic, topicMap);
            }
            topicMap.set(clientKeyId, {
                info: req.peer,
                expiresAt: now + clamped * 1000,
            });
        }

        return { type: 'announce_ack', ttls };
    }

    private handleQuery(
        req: import('@hyper-hyper-space/hhs3_mesh_tracker_client').QueryRequest,
    ): QueryResponse {
        const now = Date.now();
        const results: Record<string, PeerInfo[]> = {};

        for (const topic of req.topics) {
            const topicMap = this.registry.get(topic);
            if (!topicMap) {
                results[topic] = [];
                continue;
            }
            let peers: PeerInfo[] = [];
            for (const [keyId, entry] of topicMap) {
                if (entry.expiresAt <= now) {
                    topicMap.delete(keyId);
                    continue;
                }
                peers.push(entry.info);
            }
            if (req.schemes && req.schemes.length > 0) {
                peers = peers.filter(p =>
                    p.addresses.some(a => req.schemes!.some(s => a.startsWith(s + '://'))),
                );
            }
            results[topic] = peers;
        }

        return { type: 'query_response', results };
    }

    private handleLeave(
        req: import('@hyper-hyper-space/hhs3_mesh_tracker_client').LeaveRequest,
        clientKeyId: KeyId,
    ): LeaveAck {
        for (const topic of req.topics) {
            const topicMap = this.registry.get(topic);
            if (topicMap) {
                topicMap.delete(clientKeyId);
                if (topicMap.size === 0) this.registry.delete(topic);
            }
        }
        return { type: 'leave_ack' };
    }

    // -- sweep ----------------------------------------------------------------

    private sweep(): void {
        const now = Date.now();
        for (const [topic, topicMap] of this.registry) {
            for (const [keyId, entry] of topicMap) {
                if (entry.expiresAt <= now) topicMap.delete(keyId);
            }
            if (topicMap.size === 0) this.registry.delete(topic);
        }
    }
}

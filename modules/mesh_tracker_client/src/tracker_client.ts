// Tracker-based PeerDiscovery implementation. Connects on demand to a remote
// tracker server over Transport + Noise, sends list-based protocol messages,
// and disconnects after each exchange. A periodic heartbeat re-announces all
// active topics to keep registrations alive.

import type {
    TopicId, PeerInfo, PeerDiscovery,
    NetworkAddress, TransportProvider, PeerAuthenticator,
    Transport, AuthenticatedChannel,
} from '@hyper-hyper-space/hhs3_mesh';
import { random, type KeyId } from '@hyper-hyper-space/hhs3_crypto';
import {
    encodeMessage, decodeResponse,
    type TrackerRequest, type TrackerResponse,
    type AnnounceRequest, type QueryRequest, type LeaveRequest,
} from './protocol.js';
import { TRACE_TRACKER, trace } from './trace.js';

export const DEFAULT_EXCHANGE_TIMEOUT_MS = 5_000;

export interface TrackerClientConfig {
    trackerAddress: NetworkAddress;
    trackerKeyId?: KeyId;
    transportProvider: TransportProvider;
    authenticator: PeerAuthenticator;
    localPeer: PeerInfo;
    announceTtl?: number;
    heartbeatInterval?: number;
    timeoutMs?: number;
    // Process id for this mesh. Omitted means a fresh one, so two processes
    // that share an identity do not overwrite each other on the tracker.
    instanceId?: string;
}

const DEFAULT_TTL = 180;
const DEFAULT_HEARTBEAT_MS = 60_000;

export class TrackerClient implements PeerDiscovery {
    readonly instanceId: string;
    private readonly address: NetworkAddress;
    private readonly trackerKeyId?: KeyId;
    private readonly transport: TransportProvider;
    private readonly authenticator: PeerAuthenticator;
    private readonly localPeer: PeerInfo;
    private readonly requestedTtl: number;
    private readonly heartbeatMs: number;
    private readonly timeoutMs: number;

    private activeTopics = new Map<TopicId, number>();
    // Topics already handed to a leave. Announce and heartbeat acks must not
    // put them back, or close() would open a second leave for them.
    private leftTopics = new Set<TopicId>();
    // leave() only records the topic. One send runs on the next turn so a
    // destroy loop's back-to-back leaves share a single request.
    private pendingLeaves = new Set<TopicId>();
    private leaveSend: Promise<void> | undefined;
    private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
    // One in-flight connect-on-demand exchange at a time. Overlapping
    // handshakes can drop the first post-handshake frame (browser \sync fetch
    // raced announce vs query). Failures must not stall the chain.
    private chain: Promise<void> = Promise.resolve();
    // After close(), leave/announce/discover must not open a new socket: mesh
    // teardown destroys swarms (which fire leave) after discovery closeables
    // have already run, and a late connect would outlive provider.close().
    private closed = false;

    constructor(config: TrackerClientConfig) {
        this.instanceId = config.instanceId ?? randomInstanceId();
        this.address = config.trackerAddress;
        this.trackerKeyId = config.trackerKeyId;
        this.transport = config.transportProvider;
        this.authenticator = config.authenticator;
        this.localPeer = config.localPeer;
        this.requestedTtl = config.announceTtl ?? DEFAULT_TTL;
        this.heartbeatMs = config.heartbeatInterval ?? DEFAULT_HEARTBEAT_MS;
        this.timeoutMs = config.timeoutMs ?? DEFAULT_EXCHANGE_TIMEOUT_MS;
    }

    // -- PeerDiscovery ---------------------------------------------------------

    async *discover(
        topic: TopicId,
        schemes?: string[],
        targetPeers?: number,
    ): AsyncIterable<PeerInfo> {
        if (this.closed) return;
        const req: QueryRequest = { type: 'query', topics: [topic], schemes };
        const res = await this.exchange(req);
        if (res.type === 'error') throw new Error(res.message);
        if (res.type !== 'query_response') throw new Error('unexpected response');
        const peers = res.results[topic] ?? [];
        let count = 0;
        for (const peer of peers) {
            if (targetPeers !== undefined && count >= targetPeers) break;
            const filtered = this.stripSelf(peer);
            if (filtered === undefined) continue;
            yield filtered;
            count++;
        }
    }

    // Defense-in-depth against self-dial: a tracker returns everyone registered
    // for a topic, including us. Drop our own advertised addresses for our key;
    // if nothing remains, drop the peer entirely. Same key at a different
    // address (multi-device) still passes through.
    private stripSelf(peer: PeerInfo): PeerInfo | undefined {
        if (peer.keyId !== this.localPeer.keyId) return peer;
        const addresses = peer.addresses.filter(
            (addr) => !this.localPeer.addresses.includes(addr),
        );
        if (addresses.length === 0) return undefined;
        return { keyId: peer.keyId, addresses };
    }

    async announce(topic: TopicId, _self: PeerInfo): Promise<void> {
        if (this.closed) return;
        this.leftTopics.delete(topic);
        const req: AnnounceRequest = {
            type: 'announce',
            entries: [{ topic, ttl: this.requestedTtl }],
            peer: this.localPeer,
            instance: this.instanceId,
        };
        const res = await this.exchange(req);
        if (res.type === 'error') throw new Error(res.message);
        if (res.type !== 'announce_ack') throw new Error('unexpected response');
        if (this.leftTopics.has(topic)) return;
        this.activeTopics.set(topic, res.ttls[0] ?? this.requestedTtl);
        this.ensureHeartbeat();
    }

    async leave(topic: TopicId, _self: KeyId): Promise<void> {
        if (this.closed) return;
        this.pendingLeaves.add(topic);
        const send = this.leaveSend ?? this.scheduleLeaveSend();
        this.leaveSend = send;
        return send;
    }

    // -- lifecycle -------------------------------------------------------------

    async close(): Promise<void> {
        if (this.closed) return;
        // Block leave/announce before the final leave exchange so mesh.close()'s
        // swarm.destroy() → discovery.leave cannot open another socket afterward.
        this.closed = true;
        this.stopHeartbeat();
        // leave() only remembers topics until the next turn. Yield so a destroy
        // loop's leaves enqueue their one exchange before we snapshot the chain.
        await Promise.resolve();
        // Finish announce/query already on the chain, and that coalesced leave
        // if the destroy loop queued one, before reading activeTopics. An
        // in-flight announce has not registered yet, and an abandoned discover
        // must not reject after the mesh tears down its transports.
        await this.chain;
        const topics = [...this.activeTopics.keys()];
        this.activeTopics.clear();
        if (topics.length === 0) return;
        for (const topic of topics) this.leftTopics.add(topic);
        const req: LeaveRequest = { type: 'leave', topics, instance: this.instanceId };
        try {
            await this.exchange(req);
        } catch {
            // best-effort; the TTL will expire on the server
        }
    }

    // -- internal -------------------------------------------------------------

    /** Remembered leaves become one request on the next turn. Every leave()
     *  from the same turn waits on this one reply. */
    private scheduleLeaveSend(): Promise<void> {
        const run = Promise.resolve().then(() => this.sendPendingLeaves());
        void run.catch(() => {});
        return run;
    }

    private async sendPendingLeaves(): Promise<void> {
        const topics = [...this.pendingLeaves];
        this.pendingLeaves.clear();
        this.leaveSend = undefined;
        if (topics.length === 0) return;
        for (const topic of topics) {
            this.leftTopics.add(topic);
            this.activeTopics.delete(topic);
        }
        if (this.activeTopics.size === 0) this.stopHeartbeat();
        const res = await this.exchange({
            type: 'leave',
            topics,
            instance: this.instanceId,
        });
        if (res.type === 'error') throw new Error(res.message);
    }

    /** Enqueue an exchange so announce/query/leave/heartbeat never overlap. */
    private exchange(req: TrackerRequest): Promise<TrackerResponse> {
        const run = this.chain.then(() => this.exchangeOnce(req));
        this.chain = run.then(() => {}, () => {});
        // Callers may abandon (async generator closed on swarm destroy). Keep
        // rejections observed so Node does not treat them as unhandled.
        void run.catch(() => {});
        return run;
    }

    /** Connect, register a response listener, send the request, await the
     *  response, then close. Timeout is measured here, not while queued.
     *  A macrotask yield after authentication gives the responder time to
     *  finish its handshake and register its message handler (needed with
     *  synchronous transports where the Noise initiator completes before
     *  the responder). */
    private async exchangeOnce(req: TrackerRequest): Promise<TrackerResponse> {
        let raw: Transport | undefined;
        let channel: AuthenticatedChannel | undefined;
        let timer: ReturnType<typeof setTimeout> | undefined;
        let settled = false;
        let finish!: (err?: Error) => void;
        const failOrTimeout = new Promise<never>((_, reject) => {
            finish = (err) => {
                if (settled) return;
                settled = true;
                reject(err ?? new Error(`tracker exchange timed out after ${this.timeoutMs}ms`));
            };
        });
        // Observed so a timeout that loses the race cannot unhandled-reject.
        void failOrTimeout.catch(() => {});

        const started = Date.now();
        const fields = exchangeFields(req, this.address);
        if (TRACE_TRACKER) trace('tracker.exchange start', fields);

        timer = setTimeout(() => {
            try { channel?.close(); } catch { /* ignore */ }
            try { raw?.close(); } catch { /* ignore */ }
            finish();
        }, this.timeoutMs);

        const work = (async () => {
            raw = await this.transport.connect(this.address);
            channel = await this.authenticator.authenticate(
                raw, 'initiator', this.trackerKeyId,
            );
            await new Promise(resolve => setTimeout(resolve, 0));

            const responsePromise = new Promise<TrackerResponse>((resolve, reject) => {
                channel!.onMessage((data) => {
                    if (settled) return;
                    settled = true;
                    try {
                        resolve(decodeResponse(data));
                    } catch (err) {
                        reject(err);
                    }
                });
                channel!.onClose(() => {
                    if (settled) return;
                    settled = true;
                    reject(new Error('channel closed before response'));
                });
            });

            channel.send(encodeMessage(req));
            return await responsePromise;
        })();
        // Same: work may reject after the timeout already won the race.
        void work.catch(() => {});

        try {
            const res = await Promise.race([work, failOrTimeout]);
            if (TRACE_TRACKER) {
                const extra: Record<string, unknown> = { ...fields, ms: Date.now() - started };
                if (res.type === 'query_response') {
                    extra.peers = Object.values(res.results).reduce((n, list) => n + list.length, 0);
                }
                trace('tracker.exchange ok', extra);
            }
            return res;
        } catch (err) {
            if (TRACE_TRACKER) {
                trace('tracker.exchange fail', {
                    ...fields,
                    ms: Date.now() - started,
                    err: err instanceof Error ? err.message : String(err),
                });
            }
            throw err;
        } finally {
            if (timer !== undefined) clearTimeout(timer);
            settled = true;
            try { channel?.close(); } catch { /* ignore */ }
            try { raw?.close(); } catch { /* ignore */ }
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
            instance: this.instanceId,
        };
        try {
            const res = await this.exchange(req);
            if (res.type === 'announce_ack') {
                for (let i = 0; i < entries.length; i++) {
                    const topic = entries[i].topic;
                    if (res.ttls[i] !== undefined && !this.leftTopics.has(topic)) {
                        this.activeTopics.set(topic, res.ttls[i]);
                    }
                }
            }
        } catch {
            // transient failure; will retry on next tick
        }
    }
}

function randomInstanceId(): string {
    let binary = '';
    const bytes = random.getBytes(8);
    for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]!);
    return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function exchangeFields(req: TrackerRequest, addr: NetworkAddress): Record<string, unknown> {
    if (req.type === 'announce') {
        return { op: req.type, topic: req.entries.map((e) => e.topic), addr };
    }
    return { op: req.type, topic: req.topics, addr };
}

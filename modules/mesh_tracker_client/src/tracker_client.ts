// Tracker-based PeerDiscovery implementation. Connects on demand to a remote
// tracker server over Transport + Noise, sends list-based protocol messages,
// and disconnects after each exchange. A periodic heartbeat re-announces all
// active topics to keep registrations alive.
//
// announce() and discover() from the same turn share one request each, so a
// database that opens many swarms at once does not open one socket per topic.
// close() drops discovery that has not reached the wire, lets an in-flight
// announce finish, and sends one leave for every topic the tracker accepted.

import type {
    TopicId, PeerInfo, PeerDiscovery,
    NetworkAddress, TransportProvider, PeerAuthenticator,
    Transport, AuthenticatedChannel,
} from '@hyper-hyper-space/hhs3_mesh';
import { random, type KeyId } from '@hyper-hyper-space/hhs3_crypto';
import {
    encodeMessage, decodeResponse,
    type TrackerRequest, type TrackerResponse,
    type AnnounceEntry, type AnnounceRequest, type QueryRequest, type LeaveRequest,
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
const CLOSED_ERROR = 'tracker client closed';

interface QueryWaiter {
    resolve: (peers: PeerInfo[]) => void;
    reject: (err: Error) => void;
}

interface QueryBatch {
    schemes?: string[];
    topics: TopicId[];
    waiters: Map<TopicId, QueryWaiter[]>;
}

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
    // Same-turn announces share one request. A leave that arrives before that
    // request is on the wire removes the topic here instead.
    private pendingAnnounces = new Map<TopicId, number>();
    private announceSend: Promise<void> | undefined;
    // Same-turn discovers with the same scheme filter share one query.
    private pendingQueries = new Map<string, QueryBatch>();
    private querySend: Promise<void> | undefined;
    // leave() only records the topic. One send runs on the next turn so a
    // destroy loop's back-to-back leaves share a single request.
    private pendingLeaves = new Set<TopicId>();
    private leaveSend: Promise<void> | undefined;
    // Topics in an announce that has reached exchangeOnce. A leave during that
    // exchange still deregisters them after the ack.
    private inflightAnnounceTopics = new Set<TopicId>();
    private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
    // One in-flight connect-on-demand exchange at a time. Overlapping
    // handshakes can drop the first post-handshake frame (browser \sync fetch
    // raced announce vs query). Failures must not stall the chain.
    private chain: Promise<void> = Promise.resolve();
    private inflightKind: TrackerRequest['type'] | undefined;
    private inflightAbort: (() => void) | undefined;
    // After close(), leave/announce/discover must not open a new socket: mesh
    // teardown destroys swarms (which fire leave) after discovery closeables
    // have already run, and a late connect would outlive provider.close().
    // A leave for topics already on the tracker still runs.
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
        const peers = await this.enqueueQuery(topic, schemes);
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
        this.pendingLeaves.delete(topic);
        this.pendingAnnounces.set(topic, this.requestedTtl);
        const send = this.announceSend ?? this.scheduleAnnounceSend();
        this.announceSend = send;
        return send;
    }

    async leave(topic: TopicId, _self: KeyId): Promise<void> {
        if (this.closed) return;
        this.pendingAnnounces.delete(topic);
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
        // Announces that have not been snapshotted never reached the tracker.
        this.pendingAnnounces.clear();
        this.rejectPendingQueries(new Error(CLOSED_ERROR));
        // A discover sitting on the wire is useless during shutdown. An announce
        // or leave already on the wire must finish so the tracker entry is real
        // before we deregister it.
        if (this.inflightKind === 'query') this.inflightAbort?.();
        // leave() only remembers topics until the next turn. Yield so a destroy
        // loop's leaves enqueue their one exchange before we snapshot the chain.
        await Promise.resolve();
        await this.chain;
        const topics = [...this.activeTopics.keys()];
        this.activeTopics.clear();
        if (topics.length === 0) return;
        for (const topic of topics) this.leftTopics.add(topic);
        try {
            await this.enqueue(() => this.exchangeOnce({
                type: 'leave',
                topics,
                instance: this.instanceId,
            }));
        } catch {
            // best-effort; the TTL will expire on the server
        }
    }

    // -- internal -------------------------------------------------------------

    private enqueueQuery(topic: TopicId, schemes?: string[]): Promise<PeerInfo[]> {
        const key = schemesKey(schemes);
        let batch = this.pendingQueries.get(key);
        if (batch === undefined) {
            batch = { schemes, topics: [], waiters: new Map() };
            this.pendingQueries.set(key, batch);
        }
        if (!batch.topics.includes(topic)) batch.topics.push(topic);
        const result = new Promise<PeerInfo[]>((resolve, reject) => {
            const waiters = batch!.waiters.get(topic) ?? [];
            waiters.push({ resolve, reject });
            batch!.waiters.set(topic, waiters);
        });
        // The generator awaits this too. An abandoned discover must not surface
        // as an unhandled rejection when shutdown rejects the waiter.
        void result.catch(() => {});
        if (this.querySend === undefined) this.querySend = this.scheduleQuerySend();
        return result;
    }

    private scheduleQuerySend(): Promise<void> {
        const run = Promise.resolve().then(() => this.sendPendingQueries());
        void run.catch(() => {});
        return run;
    }

    private async sendPendingQueries(): Promise<void> {
        const batches = [...this.pendingQueries.values()];
        this.pendingQueries.clear();
        this.querySend = undefined;
        for (const batch of batches) {
            try {
                const res = await this.enqueue(async () => {
                    if (this.closed) throw new Error(CLOSED_ERROR);
                    const req: QueryRequest = {
                        type: 'query',
                        topics: batch.topics,
                        ...(batch.schemes !== undefined ? { schemes: batch.schemes } : {}),
                    };
                    return this.exchangeOnce(req);
                });
                if (res.type === 'error') throw new Error(res.message);
                if (res.type !== 'query_response') throw new Error('unexpected response');
                for (const [topic, waiters] of batch.waiters) {
                    const peers = res.results[topic] ?? [];
                    for (const waiter of waiters) waiter.resolve(peers);
                }
            } catch (err) {
                const error = err instanceof Error ? err : new Error(String(err));
                for (const waiters of batch.waiters.values()) {
                    for (const waiter of waiters) waiter.reject(error);
                }
            }
        }
    }

    private rejectPendingQueries(err: Error): void {
        const batches = [...this.pendingQueries.values()];
        this.pendingQueries.clear();
        for (const batch of batches) {
            for (const waiters of batch.waiters.values()) {
                for (const waiter of waiters) waiter.reject(err);
            }
        }
    }

    private scheduleAnnounceSend(): Promise<void> {
        const run = Promise.resolve().then(() => this.sendPendingAnnounces());
        void run.catch(() => {});
        return run;
    }

    private async sendPendingAnnounces(): Promise<void> {
        const entries: AnnounceEntry[] = [...this.pendingAnnounces.entries()].map(
            ([topic, ttl]) => ({ topic, ttl }),
        );
        this.pendingAnnounces.clear();
        this.announceSend = undefined;
        if (entries.length === 0) return;

        // Ack handling stays inside the chain slot so close(), waiting on
        // `this.chain`, observes activeTopics before it sends leave.
        await this.enqueue(async () => {
            if (this.closed) return;
            const live = entries.filter((entry) =>
                !this.leftTopics.has(entry.topic) && !this.pendingLeaves.has(entry.topic),
            );
            if (live.length === 0) return;
            this.inflightAnnounceTopics = new Set(live.map((entry) => entry.topic));
            try {
                const res = await this.exchangeOnce(this.announceRequest(live));
                if (res.type === 'error') throw new Error(res.message);
                if (res.type !== 'announce_ack') throw new Error('unexpected response');
                for (let i = 0; i < live.length; i++) {
                    const topic = live[i]!.topic;
                    if (this.leftTopics.has(topic)) continue;
                    this.activeTopics.set(topic, res.ttls[i] ?? live[i]!.ttl);
                }
                if (!this.closed && this.activeTopics.size > 0) this.ensureHeartbeat();
            } catch (err) {
                // The request may have reached the tracker even though we never
                // saw the ack. Remember it so close() still deregisters.
                if (this.closed && !isClosedError(err)) {
                    for (const entry of live) {
                        if (!this.leftTopics.has(entry.topic)) {
                            this.activeTopics.set(entry.topic, entry.ttl);
                        }
                    }
                    return;
                }
                if (isClosedError(err)) return;
                throw err;
            } finally {
                this.inflightAnnounceTopics.clear();
            }
        });
    }

    private announceRequest(entries: AnnounceEntry[]): AnnounceRequest {
        return {
            type: 'announce',
            entries,
            peer: this.localPeer,
            instance: this.instanceId,
        };
    }

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
            this.pendingAnnounces.delete(topic);
        }
        // Only topics the tracker knows about. A leave that cancelled a pending
        // announce, or that raced a discover, must not open a socket.
        const toLeave = topics.filter((topic) =>
            this.activeTopics.has(topic) || this.inflightAnnounceTopics.has(topic),
        );
        for (const topic of toLeave) this.activeTopics.delete(topic);
        if (this.activeTopics.size === 0) this.stopHeartbeat();
        if (toLeave.length === 0) return;
        const res = await this.enqueue(() => this.exchangeOnce({
            type: 'leave',
            topics: toLeave,
            instance: this.instanceId,
        }));
        if (res.type === 'error') throw new Error(res.message);
    }

    /** Run `op` only when the previous exchange has finished. */
    private enqueue<T>(op: () => Promise<T>): Promise<T> {
        const run = this.chain.then(op);
        this.chain = run.then(() => {}, () => {});
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

        this.inflightKind = req.type;
        this.inflightAbort = () => {
            try { channel?.close(); } catch { /* ignore */ }
            try { raw?.close(); } catch { /* ignore */ }
            finish(new Error(CLOSED_ERROR));
        };

        timer = setTimeout(() => {
            try { channel?.close(); } catch { /* ignore */ }
            try { raw?.close(); } catch { /* ignore */ }
            finish();
        }, this.timeoutMs);

        const work = (async () => {
            raw = await this.transport.connect(this.address);
            if (settled) {
                try { raw.close(); } catch { /* ignore */ }
                throw new Error(CLOSED_ERROR);
            }
            channel = await this.authenticator.authenticate(
                raw, 'initiator', this.trackerKeyId,
            );
            if (settled) throw new Error(CLOSED_ERROR);
            await new Promise(resolve => setTimeout(resolve, 0));
            if (settled) throw new Error(CLOSED_ERROR);

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
            this.inflightKind = undefined;
            this.inflightAbort = undefined;
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
        if (this.closed || this.activeTopics.size === 0) return;
        const entries = [...this.activeTopics.entries()].map(
            ([topic, ttl]) => ({ topic, ttl }),
        );
        try {
            await this.enqueue(async () => {
                if (this.closed) return;
                const live = entries.filter((entry) =>
                    this.activeTopics.has(entry.topic) && !this.leftTopics.has(entry.topic),
                );
                if (live.length === 0) return;
                const res = await this.exchangeOnce(this.announceRequest(live));
                if (res.type !== 'announce_ack') return;
                for (let i = 0; i < live.length; i++) {
                    const topic = live[i]!.topic;
                    if (res.ttls[i] !== undefined && !this.leftTopics.has(topic) && this.activeTopics.has(topic)) {
                        this.activeTopics.set(topic, res.ttls[i]!);
                    }
                }
            });
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

function schemesKey(schemes: string[] | undefined): string {
    if (schemes === undefined || schemes.length === 0) return '';
    return [...schemes].sort().join('\0');
}

function isClosedError(err: unknown): boolean {
    return err instanceof Error && err.message === CLOSED_ERROR;
}

function exchangeFields(req: TrackerRequest, addr: NetworkAddress): Record<string, unknown> {
    if (req.type === 'announce') {
        return { op: req.type, topic: req.entries.map((e) => e.topic), addr };
    }
    return { op: req.type, topic: req.topics, addr };
}

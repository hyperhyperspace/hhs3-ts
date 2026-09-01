// Query/response PeerDiscovery over BroadcastChannel. announce() registers a
// listener that replies to query; leave() stops answering. discover() posts
// query, collects presence for a short window, and returns a finite snapshot.
// No heartbeat or TTL: being able to answer is liveness.

import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { PeerDiscovery, PeerInfo, TopicId } from '@hyper-hyper-space/hhs3_mesh';
import {
    BroadcastChannelCtor, BroadcastChannelLike,
    defaultBroadcastChannelCtor, DEFAULT_BC_BASE,
} from './bc_types.js';

const DEFAULT_COLLECT_WINDOW_MS = 100;

export interface BroadcastChannelDiscoveryOptions {
    self: PeerInfo;
    base?: string;
    BroadcastChannelCtor?: BroadcastChannelCtor;
    collectWindowMs?: number;
}

interface QueryMessage {
    kind: 'query';
    topic: string;
}

interface PresenceMessage {
    kind: 'presence';
    topic: string;
    peer: PeerInfo;
}

type DiscMessage = QueryMessage | PresenceMessage;

function filterAddresses(addresses: string[], schemes?: string[]): string[] {
    if (schemes === undefined || schemes.length === 0) return addresses;
    return addresses.filter(addr => schemes.some(s => addr.startsWith(s + '://')));
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export class BroadcastChannelDiscovery implements PeerDiscovery {

    readonly self: PeerInfo;
    private readonly channelName: string;
    private readonly Ctor: BroadcastChannelCtor;
    private readonly collectWindowMs: number;
    private readonly announced = new Map<TopicId, PeerInfo>();
    private readonly collectors = new Map<TopicId, PeerInfo[]>();
    private channel: BroadcastChannelLike | undefined;
    private closed = false;

    constructor(opts: BroadcastChannelDiscoveryOptions) {
        this.self = opts.self;
        this.channelName = `${opts.base ?? DEFAULT_BC_BASE}:disc`;
        this.Ctor = opts.BroadcastChannelCtor ?? defaultBroadcastChannelCtor();
        this.collectWindowMs = opts.collectWindowMs ?? DEFAULT_COLLECT_WINDOW_MS;
    }

    async *discover(
        topic: TopicId,
        schemes?: string[],
        targetPeers?: number,
    ): AsyncIterable<PeerInfo> {
        if (this.closed) return;
        this.ensureChannel();
        const collected: PeerInfo[] = [];
        this.collectors.set(topic, collected);
        this.channel!.postMessage({ kind: 'query', topic } satisfies QueryMessage);
        await delay(this.collectWindowMs);
        this.collectors.delete(topic);
        this.dropChannelIfIdle();

        let count = 0;
        for (const peer of collected) {
            if (this.isSelf(peer)) continue;
            const addresses = filterAddresses(peer.addresses, schemes);
            if (addresses.length === 0) continue;
            yield { keyId: peer.keyId, addresses };
            count++;
            if (targetPeers !== undefined && count >= targetPeers) return;
        }
    }

    async announce(topic: TopicId, self: PeerInfo): Promise<void> {
        if (this.closed) return;
        this.announced.set(topic, self);
        this.ensureChannel();
    }

    async leave(topic: TopicId, _self: KeyId): Promise<void> {
        this.announced.delete(topic);
        this.dropChannelIfIdle();
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;
        this.announced.clear();
        this.collectors.clear();
        this.closeChannel();
    }

    private ensureChannel(): void {
        if (this.channel !== undefined) return;
        const channel = new this.Ctor(this.channelName);
        channel.onmessage = (ev) => this.onMessage(ev.data);
        this.channel = channel;
    }

    private onMessage(raw: unknown): void {
        const msg = raw as DiscMessage;
        if (msg === null || typeof msg !== 'object' || typeof msg.kind !== 'string') return;

        if (msg.kind === 'query') {
            const self = this.announced.get(msg.topic as TopicId);
            if (self === undefined || this.channel === undefined) return;
            this.channel.postMessage({
                kind: 'presence',
                topic: msg.topic,
                peer: self,
            } satisfies PresenceMessage);
            return;
        }

        if (msg.kind === 'presence') {
            const bucket = this.collectors.get(msg.topic as TopicId);
            if (bucket === undefined) return;
            if (msg.peer === undefined || typeof msg.peer.keyId !== 'string') return;
            if (!Array.isArray(msg.peer.addresses)) return;
            bucket.push(msg.peer);
        }
    }

    private isSelf(peer: PeerInfo): boolean {
        if (peer.keyId !== this.self.keyId) return false;
        const mine = new Set(this.self.addresses);
        return peer.addresses.length > 0 && peer.addresses.every(a => mine.has(a));
    }

    private dropChannelIfIdle(): void {
        if (this.announced.size === 0 && this.collectors.size === 0) {
            this.closeChannel();
        }
    }

    private closeChannel(): void {
        if (this.channel === undefined) return;
        try { this.channel.close(); } catch { /* ignore */ }
        this.channel = undefined;
    }
}

// Folder-based PeerDiscovery for Node. Presence is a lease: announce() writes
// a JSON file under <root>/<encTopic>/, a heartbeat keeps mtime fresh, and
// discover() returns a finite snapshot of currently-live files. Swarm calls
// announce() once per activate(), so the heartbeat lives in this source.

import { mkdir, readdir, readFile, rename, stat, unlink, utimes, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { random } from '@hyper-hyper-space/hhs3_crypto';
import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { PeerDiscovery, PeerInfo, TopicId } from '@hyper-hyper-space/hhs3_mesh';

const DEFAULT_TTL_MS = 180_000;
const DEFAULT_HEARTBEAT_MS = 60_000;

export function defaultMeshFolderRoot(): string {
    const explicit = process.env.RDB_MESH;
    if (explicit !== undefined && explicit !== '') return explicit;
    const home = process.env.RDB_HOME ?? join(homedir(), '.rdb');
    return join(home, 'mesh');
}

export interface FolderDiscoveryOptions {
    root?: string;
    self: PeerInfo;
    instanceId?: string;
    ttlMs?: number;
    heartbeatMs?: number;
}

interface PresenceFile {
    keyId: string;
    addresses: string[];
    topic: string;
    updatedAt: number;
}

function encodeFs(value: string): string {
    return Buffer.from(value, 'utf8').toString('base64url');
}

function randomInstanceId(): string {
    return Buffer.from(random.getBytes(8)).toString('base64url');
}

function filterAddresses(addresses: string[], schemes?: string[]): string[] {
    if (schemes === undefined || schemes.length === 0) return addresses;
    return addresses.filter(addr => schemes.some(s => addr.startsWith(s + '://')));
}

export class FolderDiscovery implements PeerDiscovery {

    readonly self: PeerInfo;
    readonly instanceId: string;
    readonly root: string;

    private readonly ttlMs: number;
    private readonly heartbeatMs: number;
    private readonly activeTopics = new Map<TopicId, { path: string; peer: PeerInfo }>();
    private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
    private closed = false;

    constructor(opts: FolderDiscoveryOptions) {
        this.self = opts.self;
        this.instanceId = opts.instanceId ?? randomInstanceId();
        this.root = opts.root ?? defaultMeshFolderRoot();
        this.ttlMs = opts.ttlMs ?? DEFAULT_TTL_MS;
        this.heartbeatMs = opts.heartbeatMs ?? DEFAULT_HEARTBEAT_MS;
    }

    async *discover(
        topic: TopicId,
        schemes?: string[],
        targetPeers?: number,
    ): AsyncIterable<PeerInfo> {
        const dir = this.topicDir(topic);
        let names: string[];
        try {
            names = await readdir(dir);
        } catch {
            return;
        }

        const ownName = this.fileName(this.self.keyId);
        const now = Date.now();
        let count = 0;

        for (const name of names) {
            if (!name.endsWith('.json') || name.endsWith('.tmp')) continue;
            const path = join(dir, name);
            if (name === ownName) continue;

            let mtimeMs: number;
            try {
                mtimeMs = (await stat(path)).mtimeMs;
            } catch {
                continue;
            }

            const age = now - mtimeMs;
            if (age > this.ttlMs * 2) {
                try { await unlink(path); } catch { /* ignore */ }
                continue;
            }
            if (age > this.ttlMs) continue;

            const peer = await this.readPresence(path);
            if (peer === undefined) continue;

            const addresses = filterAddresses(peer.addresses, schemes);
            if (addresses.length === 0) continue;

            yield { keyId: peer.keyId as KeyId, addresses };
            count++;
            if (targetPeers !== undefined && count >= targetPeers) return;
        }
    }

    async announce(topic: TopicId, self: PeerInfo): Promise<void> {
        if (this.closed) return;
        const path = await this.writePresence(topic, self);
        this.activeTopics.set(topic, { path, peer: self });
        this.ensureHeartbeat();
    }

    async leave(topic: TopicId, _self: KeyId): Promise<void> {
        const entry = this.activeTopics.get(topic);
        this.activeTopics.delete(topic);
        if (entry !== undefined) {
            try { await unlink(entry.path); } catch { /* ignore */ }
        }
        if (this.activeTopics.size === 0) this.stopHeartbeat();
    }

    async close(): Promise<void> {
        if (this.closed) return;
        this.closed = true;
        this.stopHeartbeat();
        const entries = [...this.activeTopics.values()];
        this.activeTopics.clear();
        for (const entry of entries) {
            try { await unlink(entry.path); } catch { /* ignore */ }
        }
    }

    private topicDir(topic: TopicId): string {
        return join(this.root, encodeFs(topic));
    }

    private fileName(keyId: KeyId): string {
        return `${encodeFs(keyId)}.${this.instanceId}.json`;
    }

    private presencePath(topic: TopicId, keyId: KeyId): string {
        return join(this.topicDir(topic), this.fileName(keyId));
    }

    private async writePresence(topic: TopicId, self: PeerInfo): Promise<string> {
        const dir = this.topicDir(topic);
        await mkdir(dir, { recursive: true });
        const path = this.presencePath(topic, self.keyId);
        const body: PresenceFile = {
            keyId: self.keyId,
            addresses: self.addresses,
            topic,
            updatedAt: Date.now(),
        };
        const tmp = `${path}.${process.pid}.tmp`;
        await writeFile(tmp, JSON.stringify(body), 'utf8');
        await rename(tmp, path);
        return path;
    }

    private async readPresence(path: string): Promise<PeerInfo | undefined> {
        try {
            const raw = await readFile(path, 'utf8');
            const parsed = JSON.parse(raw) as PresenceFile;
            if (typeof parsed.keyId !== 'string' || !Array.isArray(parsed.addresses)) {
                return undefined;
            }
            const addresses = parsed.addresses.filter((a): a is string => typeof a === 'string');
            if (addresses.length === 0) return undefined;
            return { keyId: parsed.keyId as KeyId, addresses };
        } catch {
            return undefined;
        }
    }

    private ensureHeartbeat(): void {
        if (this.heartbeatTimer !== null) return;
        this.heartbeatTimer = setInterval(() => { void this.heartbeat(); }, this.heartbeatMs);
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
        const now = new Date();
        for (const [topic, entry] of this.activeTopics) {
            try {
                await utimes(entry.path, now, now);
            } catch {
                try {
                    const rewritten = await this.writePresence(topic, entry.peer);
                    this.activeTopics.set(topic, { path: rewritten, peer: entry.peer });
                } catch {
                    // transient; retry next tick
                }
            }
        }
    }
}

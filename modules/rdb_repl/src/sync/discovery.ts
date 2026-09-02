import type { KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type {
    NetworkAddress,
    PeerDiscovery,
    PeerInfo,
    TopicId,
    Transport,
    TransportProvider,
} from "@hyper-hyper-space/hhs3_mesh";

import type { SyncScope } from "./parse.js";

export const DEFAULT_LOCAL_TRACKER = 'ws://127.0.0.1:4610';
export const DEFAULT_INTERNET_TRACKER = 'wss://mypeer.net:4610';
export const DEFAULT_INTERNET_TRACKER_KEY = 'VjrVDsjsuEOzhsREAA0jl9Wj/TnQu8IF+XMLLbnXBeQ=';

export const TRACKER_PROBE_TIMEOUT_MS = 1500;

export type TrackerEnv = {
    tracker?: string;
    trackerKey?: string;
    listen?: string;
};

export type ResolvedTracker = {
    address: string;
    keyId?: KeyId;
};

export function resolveTrackerConfig(
    scope: SyncScope,
    flags: { tracker?: string; trackerKey?: string } = {},
    env: TrackerEnv = {},
): ResolvedTracker {
    const address = flags.tracker ?? env.tracker ?? (scope === 'internet' ? DEFAULT_INTERNET_TRACKER : DEFAULT_LOCAL_TRACKER);
    const keyId = (flags.trackerKey ?? env.trackerKey ?? (address === DEFAULT_INTERNET_TRACKER ? DEFAULT_INTERNET_TRACKER_KEY : undefined)) as KeyId | undefined;
    return keyId === undefined ? { address } : { address, keyId };
}

export function trackerScheme(address: string): string {
    const colon = address.indexOf('://');
    if (colon <= 0) throw new Error(`unsupported tracker URL '${address}'`);
    return address.slice(0, colon);
}

/** Swallow discover/announce/leave failures so a down tracker cannot abort DiscoveryStack. */
export class QuietDiscovery implements PeerDiscovery {
    constructor(private readonly inner: PeerDiscovery) {}

    async *discover(topic: TopicId, schemes?: string[], targetPeers?: number): AsyncIterable<PeerInfo> {
        try {
            for await (const peer of this.inner.discover(topic, schemes, targetPeers)) {
                yield peer;
            }
        } catch {
            return;
        }
    }

    async announce(topic: TopicId, self: PeerInfo): Promise<void> {
        try {
            await this.inner.announce(topic, self);
        } catch {
            // tracker outage must not fail the backup layer
        }
    }

    async leave(topic: TopicId, self: KeyId): Promise<void> {
        try {
            await this.inner.leave(topic, self);
        } catch {
            // best-effort
        }
    }
}

/** Rewrite announced addresses (bind 0.0.0.0, announce a dialable host). */
export class RewriteAnnounceDiscovery implements PeerDiscovery {
    constructor(
        private readonly inner: PeerDiscovery,
        private readonly addresses: NetworkAddress[],
    ) {}

    discover(topic: TopicId, schemes?: string[], targetPeers?: number): AsyncIterable<PeerInfo> {
        return this.inner.discover(topic, schemes, targetPeers);
    }

    announce(topic: TopicId, self: PeerInfo): Promise<void> {
        return this.inner.announce(topic, { ...self, addresses: this.addresses });
    }

    leave(topic: TopicId, self: KeyId): Promise<void> {
        return this.inner.leave(topic, self);
    }
}

export async function probeTracker(
    provider: TransportProvider,
    address: NetworkAddress,
    timeoutMs = TRACKER_PROBE_TIMEOUT_MS,
): Promise<boolean> {
    let transport: Transport | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let timedOut = false;
    try {
        const connected = provider.connect(address).then((t) => {
            if (timedOut) {
                try { t.close(); } catch { /* ignore */ }
                throw new Error('probe timed out');
            }
            transport = t;
            return t;
        });
        await Promise.race([
            connected,
            new Promise<never>((_, reject) => {
                timer = setTimeout(() => {
                    timedOut = true;
                    reject(new Error('timeout'));
                }, timeoutMs);
            }),
        ]);
        return true;
    } catch {
        return false;
    } finally {
        if (timer !== undefined) clearTimeout(timer);
        if (transport !== undefined) {
            try { transport.close(); } catch { /* ignore */ }
        }
    }
}

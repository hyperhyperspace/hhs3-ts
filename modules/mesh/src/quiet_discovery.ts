// A PeerDiscovery wrapper that swallows discover/announce/leave failures so a
// down or flaky source (typically a tracker) cannot abort a DiscoveryStack or
// take down a backup layer running alongside it.

import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { PeerDiscovery, PeerInfo, TopicId } from './discovery.js';

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
            // a source outage must not fail the backup layer
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

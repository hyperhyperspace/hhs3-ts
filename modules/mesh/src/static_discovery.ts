// Static bootstrap discovery: yields a fixed set of peers for specific topics.
// Results are shuffled on each discover() call for effective epidemic gossip.
// announce() and leave() are no-ops since the peer list is immutable.

import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { PeerInfo, PeerDiscovery, TopicId } from './discovery.js';

// Fisher-Yates shuffle using crypto-grade randomness.
function fisherYatesShuffle<T>(arr: T[]): T[] {
    const buf = new Uint32Array(arr.length);
    globalThis.crypto.getRandomValues(buf);
    for (let i = arr.length - 1; i > 0; i--) {
        const j = buf[i] % (i + 1);
        const tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
    return arr;
}

export class StaticDiscovery implements PeerDiscovery {

    private peers: PeerInfo[];
    private topicSet: Set<TopicId>;

    constructor(peers: PeerInfo[], topics: TopicId[]) {
        this.peers = peers;
        this.topicSet = new Set(topics);
    }

    async *discover(topic: TopicId, schemes?: string[], _targetPeers?: number): AsyncIterable<PeerInfo> {
        if (!this.topicSet.has(topic)) return;

        const shuffled = fisherYatesShuffle([...this.peers]);

        for (const peer of shuffled) {
            if (schemes === undefined || schemes.length === 0) {
                yield peer;
                continue;
            }

            const filtered = peer.addresses.filter(
                addr => schemes.some(s => addr.startsWith(s + '://'))
            );
            if (filtered.length > 0) {
                yield { keyId: peer.keyId, addresses: filtered };
            }
        }
    }

    async announce(_topic: TopicId, _self: PeerInfo): Promise<void> {}
    async leave(_topic: TopicId, _self: KeyId): Promise<void> {}
}

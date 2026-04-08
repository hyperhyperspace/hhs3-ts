// Peer discovery: find peers interested in syncing a given topic. Returns
// actionable (keyId, addresses) pairs so callers can connect immediately
// without a separate resolution step. Implementations may use DHT, signalling
// servers, static bootstrap files, or gossip.

import type { Hash, KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { NetworkAddress } from './transport.js';

export type TopicId = Hash;

export interface PeerInfo {
    keyId: KeyId;
    addresses: NetworkAddress[];
}

export interface PeerDiscovery {
    discover(topic: TopicId, schemes?: string[]): AsyncIterable<PeerInfo>;
    announce(topic: TopicId, self: PeerInfo): Promise<void>;
    leave(topic: TopicId, self: KeyId): Promise<void>;
}

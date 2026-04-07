// Peer discovery: find peers interested in syncing a given topic. Returns only
// cryptographic identities (KeyId); address resolution is a separate concern
// handled by PeerResolver. Implementations may use DHT, signalling servers,
// static bootstrap files, or gossip -- the interface is transport-agnostic.

import type { Hash, KeyId } from '@hyper-hyper-space/hhs3_crypto';

export type TopicId = Hash;

export interface PeerDiscovery {
    discover(topic: TopicId): AsyncIterable<KeyId>;
    announce(topic: TopicId, self: KeyId): Promise<void>;
    leave(topic: TopicId, self: KeyId): Promise<void>;
}

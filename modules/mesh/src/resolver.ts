// Peer resolver: maps a cryptographic KeyId to one or more network addresses.
// Supports optional transport-scheme filtering so callers can restrict results
// to reachable transports without iterating through unresolvable peers.

import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { NetworkAddress } from './transport.js';

export interface PeerResolver {
    resolve(peer: KeyId, schemes?: string[]): Promise<NetworkAddress[]>;
    resolveAny(peers: KeyId[], schemes?: string[]): AsyncIterable<{ peer: KeyId; addresses: NetworkAddress[] }>;
    publish(self: KeyId, addresses: NetworkAddress[]): Promise<void>;
    unpublish(self: KeyId): Promise<void>;
}

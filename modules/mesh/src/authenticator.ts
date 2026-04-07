// Peer authentication: verifies that the entity on the other end of a raw
// Transport holds the private key corresponding to a claimed public identity.
// Wraps the raw channel into an AuthenticatedChannel that carries the verified
// identity. Concrete implementations (challenge-response, KEM-based encrypted
// sessions) are provided separately; only the interface is defined here.

import type { PublicKey, KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { Transport } from './transport.js';

export interface AuthenticatedChannel {
    readonly remotePeer: PublicKey;
    readonly remoteKeyId: KeyId;
    readonly open: boolean;
    send(message: Uint8Array): void;
    close(): void;
    onMessage(callback: (message: Uint8Array) => void): void;
    onClose(callback: () => void): void;
}

export interface PeerAuthenticator {
    authenticate(
        transport: Transport,
        localKey: { publicKey: PublicKey; secretKey: Uint8Array },
        expectedRemote?: KeyId
    ): Promise<AuthenticatedChannel>;
}

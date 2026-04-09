// Peer authentication: verifies that the entity on the other end of a raw
// Transport holds the private key corresponding to a claimed public identity.
// Wraps the raw channel into an AuthenticatedChannel that carries the verified
// identity. The authenticator is constructed with the local signing key already
// bound; concrete implementations are provided separately.

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
        role: 'initiator' | 'responder',
        expectedRemote?: KeyId
    ): Promise<AuthenticatedChannel>;
}

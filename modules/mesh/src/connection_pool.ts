// Shared pool of authenticated peer connections. Swarms draw from and
// contribute to this pool so that a single transport connection can serve
// multiple topics without duplicate handshakes.

import type { KeyId, PublicKey } from '@hyper-hyper-space/hhs3_crypto';
import type { AuthenticatedChannel } from './authenticator.js';
import type { TopicId } from './discovery.js';

export interface PooledConnection {
    readonly peer: PublicKey;
    readonly peerId: KeyId;
    readonly channel: AuthenticatedChannel;
}

type ConnectCallback    = (conn: PooledConnection) => void;
type DisconnectCallback = (peerId: KeyId) => void;
type InterestQuery      = (peerId: KeyId, topic: TopicId) => Promise<boolean>;

export class ConnectionPool {

    private connections = new Map<KeyId, PooledConnection>();
    private connectCallbacks:    ConnectCallback[]    = [];
    private disconnectCallbacks: DisconnectCallback[] = [];
    private interestQuery?: InterestQuery;

    setInterestQuery(query: InterestQuery): void {
        this.interestQuery = query;
    }

    get(peer: KeyId): PooledConnection | undefined {
        return this.connections.get(peer);
    }

    all(): PooledConnection[] {
        return Array.from(this.connections.values());
    }

    add(channel: AuthenticatedChannel): PooledConnection {
        const existing = this.connections.get(channel.remoteKeyId);
        if (existing !== undefined) {
            channel.close();
            return existing;
        }

        const conn: PooledConnection = {
            peer:    channel.remotePeer,
            peerId:  channel.remoteKeyId,
            channel,
        };

        this.connections.set(conn.peerId, conn);

        channel.onClose(() => {
            this.connections.delete(conn.peerId);
            for (const cb of this.disconnectCallbacks) cb(conn.peerId);
        });

        for (const cb of this.connectCallbacks) cb(conn);

        return conn;
    }

    remove(peer: KeyId): void {
        const conn = this.connections.get(peer);
        if (conn !== undefined) {
            conn.channel.close();
        }
    }

    async queryInterest(topic: TopicId): Promise<KeyId[]> {
        if (this.interestQuery === undefined) return [];

        const results: KeyId[] = [];
        const query = this.interestQuery;

        await Promise.all(
            Array.from(this.connections.keys()).map(async (peerId) => {
                if (await query(peerId, topic)) {
                    results.push(peerId);
                }
            })
        );

        return results;
    }

    onConnect(callback: ConnectCallback): void {
        this.connectCallbacks.push(callback);
    }

    onDisconnect(callback: DisconnectCallback): void {
        this.disconnectCallbacks.push(callback);
    }

    size(): number {
        return this.connections.size;
    }

    close(): void {
        for (const conn of this.connections.values()) {
            conn.channel.close();
        }
    }
}

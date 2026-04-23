// Shared pool of authenticated peer connections. Keyed on (keyId, endpoint)
// pairs so multiple devices for the same identity get separate connections.
// Provides topic-multiplexed channels via openTopic(): callers get a
// TopicChannel shim that frames/unframes messages on the shared wire.

import type { KeyId, PublicKey } from '@hyper-hyper-space/hhs3_crypto';
import type { NetworkAddress } from './transport.js';
import type { AuthenticatedChannel } from './authenticator.js';
import type { TopicId } from './discovery.js';
import {
    TopicChannel, encodeTopicMessage, decodeMessage, MSG_TYPE_TOPIC, MSG_TYPE_CONTROL,
} from './mux.js';

export function connectionKey(keyId: KeyId, endpoint: NetworkAddress): string {
    return `${keyId}@${endpoint}`;
}

function topicChannelKey(connKey: string, topic: TopicId): string {
    return `${connKey}#${topic}`;
}

export interface PooledConnection {
    readonly peerId: KeyId;
    readonly peer: PublicKey;
    readonly endpoint: NetworkAddress;
    readonly channel: AuthenticatedChannel;
}

type ConnectCallback    = (conn: PooledConnection) => void;
type DisconnectCallback = (connKey: string) => void;
type ControlCallback    = (connKey: string, peerId: KeyId, endpoint: NetworkAddress, payload: Uint8Array) => void;

export class ConnectionPool {

    private connections = new Map<string, PooledConnection>();
    private topicChannels = new Map<string, TopicChannelImpl>();
    private connectCallbacks:    ConnectCallback[]    = [];
    private disconnectCallbacks: DisconnectCallback[] = [];
    private controlCallbacks:    ControlCallback[]    = [];

    get(keyId: KeyId, endpoint: NetworkAddress): PooledConnection | undefined {
        return this.connections.get(connectionKey(keyId, endpoint));
    }

    getByKeyId(keyId: KeyId): PooledConnection[] {
        const result: PooledConnection[] = [];
        for (const conn of this.connections.values()) {
            if (conn.peerId === keyId) result.push(conn);
        }
        return result;
    }

    all(): PooledConnection[] {
        return Array.from(this.connections.values());
    }

    add(channel: AuthenticatedChannel, endpoint: NetworkAddress): PooledConnection {
        const key = connectionKey(channel.remoteKeyId, endpoint);
        const existing = this.connections.get(key);
        if (existing !== undefined) {
            channel.close();
            return existing;
        }

        const conn: PooledConnection = {
            peer:     channel.remotePeer,
            peerId:   channel.remoteKeyId,
            endpoint,
            channel,
        };

        this.connections.set(key, conn);
        this.installDispatch(key, conn);

        channel.onClose(() => {
            this.connections.delete(key);
            this.closeTopicsForConnection(key);
            for (const cb of this.disconnectCallbacks) cb(key);
        });

        for (const cb of this.connectCallbacks) cb(conn);

        return conn;
    }

    remove(keyId: KeyId, endpoint: NetworkAddress): void {
        const key = connectionKey(keyId, endpoint);
        const conn = this.connections.get(key);
        if (conn !== undefined) {
            conn.channel.close();
        }
    }

    openTopic(keyId: KeyId, endpoint: NetworkAddress, topic: TopicId): TopicChannel {
        const connKey = connectionKey(keyId, endpoint);
        const topicKey = topicChannelKey(connKey, topic);

        const existing = this.topicChannels.get(topicKey);
        if (existing !== undefined && existing.open) return existing;

        const conn = this.connections.get(connKey);
        if (conn === undefined) {
            throw new Error(`no connection for ${connKey}`);
        }

        const tc = new TopicChannelImpl(topic, keyId, endpoint, conn.channel, () => {
            this.topicChannels.delete(topicKey);
        });

        this.topicChannels.set(topicKey, tc);
        return tc;
    }

    hasTopicChannel(keyId: KeyId, endpoint: NetworkAddress, topic: TopicId): boolean {
        const tc = this.topicChannels.get(topicChannelKey(connectionKey(keyId, endpoint), topic));
        return tc !== undefined && tc.open;
    }

    onConnect(callback: ConnectCallback): void {
        this.connectCallbacks.push(callback);
    }

    onDisconnect(callback: DisconnectCallback): void {
        this.disconnectCallbacks.push(callback);
    }

    onControlMessage(callback: ControlCallback): void {
        this.controlCallbacks.push(callback);
    }

    size(): number {
        return this.connections.size;
    }

    close(): void {
        for (const conn of this.connections.values()) {
            conn.channel.close();
        }
    }

    // --- internal dispatch ---

    private installDispatch(connKey: string, conn: PooledConnection): void {
        conn.channel.onMessage((frame: Uint8Array) => {
            try {
                const msg = decodeMessage(frame);
                if (msg.type === MSG_TYPE_TOPIC && msg.topic !== undefined) {
                    const tc = this.topicChannels.get(topicChannelKey(connKey, msg.topic));
                    if (tc !== undefined && tc.open) {
                        tc.deliver(msg.payload);
                    }
                } else if (msg.type === MSG_TYPE_CONTROL) {
                    for (const cb of this.controlCallbacks) {
                        cb(connKey, conn.peerId, conn.endpoint, msg.payload);
                    }
                }
            } catch {
                // Malformed frames are silently dropped
            }
        });
    }

    private closeTopicsForConnection(connKey: string): void {
        for (const [key, tc] of this.topicChannels) {
            if (key.startsWith(connKey + '#')) {
                tc.forceClose();
                this.topicChannels.delete(key);
            }
        }
    }
}

class TopicChannelImpl implements TopicChannel {
    readonly topic: TopicId;
    readonly peerId: KeyId;
    readonly endpoint: NetworkAddress;

    private channel: AuthenticatedChannel;
    private onUnregister: () => void;
    private messageCallbacks: ((msg: Uint8Array) => void)[] = [];
    private closeCallbacks: (() => void)[] = [];
    private _open = true;

    constructor(
        topic: TopicId,
        peerId: KeyId,
        endpoint: NetworkAddress,
        channel: AuthenticatedChannel,
        onUnregister: () => void,
    ) {
        this.topic = topic;
        this.peerId = peerId;
        this.endpoint = endpoint;
        this.channel = channel;
        this.onUnregister = onUnregister;
    }

    get open(): boolean {
        return this._open && this.channel.open;
    }

    send(message: Uint8Array): void {
        if (!this.open) throw new Error('topic channel closed');
        this.channel.send(encodeTopicMessage(this.topic, message));
    }

    onMessage(callback: (message: Uint8Array) => void): void {
        this.messageCallbacks.push(callback);
    }

    onClose(callback: () => void): void {
        this.closeCallbacks.push(callback);
    }

    close(): void {
        if (!this._open) return;
        this._open = false;
        this.onUnregister();
        for (const cb of this.closeCallbacks) cb();
    }

    deliver(payload: Uint8Array): void {
        if (!this._open) return;
        for (const cb of this.messageCallbacks) cb(payload);
    }

    forceClose(): void {
        if (!this._open) return;
        this._open = false;
        for (const cb of this.closeCallbacks) cb();
    }
}

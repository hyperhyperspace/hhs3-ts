// Topic-based message multiplexing over a shared connection. Encodes a
// lightweight header so the pool can route messages to the right TopicChannel.
//
// Wire format:
//   [1 byte type][2 bytes topic-length BE][topic UTF-8 bytes][payload]
//   Type 0x01 = topic data, Type 0x02 = control (reserved, no topic field)

import type { KeyId } from '@hyper-hyper-space/hhs3_crypto';
import type { NetworkAddress } from './transport.js';
import type { TopicId } from './discovery.js';

export const MSG_TYPE_TOPIC   = 0x01;
export const MSG_TYPE_CONTROL = 0x02;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encodeTopicMessage(topic: TopicId, payload: Uint8Array): Uint8Array {
    const topicBytes = encoder.encode(topic);
    const frame = new Uint8Array(1 + 2 + topicBytes.length + payload.length);
    frame[0] = MSG_TYPE_TOPIC;
    frame[1] = (topicBytes.length >> 8) & 0xff;
    frame[2] = topicBytes.length & 0xff;
    frame.set(topicBytes, 3);
    frame.set(payload, 3 + topicBytes.length);
    return frame;
}

export function encodeControlMessage(payload: Uint8Array): Uint8Array {
    const frame = new Uint8Array(1 + payload.length);
    frame[0] = MSG_TYPE_CONTROL;
    frame.set(payload, 1);
    return frame;
}

export interface DecodedMessage {
    type: number;
    topic?: TopicId;
    payload: Uint8Array;
}

export function decodeMessage(frame: Uint8Array): DecodedMessage {
    if (frame.length < 1) throw new Error('empty frame');

    const type = frame[0];

    if (type === MSG_TYPE_CONTROL) {
        return { type, payload: frame.subarray(1) };
    }

    if (type === MSG_TYPE_TOPIC) {
        if (frame.length < 3) throw new Error('truncated topic header');
        const topicLen = (frame[1] << 8) | frame[2];
        if (frame.length < 3 + topicLen) throw new Error('truncated topic');
        const topic = decoder.decode(frame.subarray(3, 3 + topicLen));
        const payload = frame.subarray(3 + topicLen);
        return { type, topic, payload };
    }

    throw new Error(`unknown message type: ${type}`);
}

export interface TopicChannel {
    readonly topic: TopicId;
    readonly peerId: KeyId;
    readonly endpoint: NetworkAddress;
    readonly open: boolean;
    send(message: Uint8Array): void;
    onMessage(callback: (message: Uint8Array) => void): void;
    close(): void;
    onClose(callback: () => void): void;
}

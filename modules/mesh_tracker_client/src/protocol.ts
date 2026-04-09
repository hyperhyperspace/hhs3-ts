// Tracker wire protocol. All messages are JSON-encoded over an authenticated
// channel. Each message type operates on lists of topics so that heartbeats,
// bulk queries, and bulk leaves collapse into a single round-trip.

import type { TopicId, PeerInfo } from '@hyper-hyper-space/hhs3_mesh';

// ---- client → server -------------------------------------------------------

export interface AnnounceEntry {
    topic: TopicId;
    ttl: number;
}

export interface AnnounceRequest {
    type: 'announce';
    entries: AnnounceEntry[];
    peer: PeerInfo;
}

export interface QueryRequest {
    type: 'query';
    topics: TopicId[];
    schemes?: string[];
}

export interface LeaveRequest {
    type: 'leave';
    topics: TopicId[];
}

export type TrackerRequest = AnnounceRequest | QueryRequest | LeaveRequest;

// ---- server → client -------------------------------------------------------

export interface AnnounceAck {
    type: 'announce_ack';
    ttls: number[];
}

export interface QueryResponse {
    type: 'query_response';
    results: Record<string, PeerInfo[]>;
}

export interface LeaveAck {
    type: 'leave_ack';
}

export interface ErrorResponse {
    type: 'error';
    message: string;
}

export type TrackerResponse = AnnounceAck | QueryResponse | LeaveAck | ErrorResponse;

// ---- encode / decode --------------------------------------------------------

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encodeMessage(msg: TrackerRequest | TrackerResponse): Uint8Array {
    return encoder.encode(JSON.stringify(msg));
}

export function decodeRequest(data: Uint8Array): TrackerRequest {
    const obj = JSON.parse(decoder.decode(data));
    if (!obj || typeof obj.type !== 'string') throw new Error('invalid tracker request');
    return obj as TrackerRequest;
}

export function decodeResponse(data: Uint8Array): TrackerResponse {
    const obj = JSON.parse(decoder.decode(data));
    if (!obj || typeof obj.type !== 'string') throw new Error('invalid tracker response');
    return obj as TrackerResponse;
}

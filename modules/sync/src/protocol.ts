import type { B64Hash } from '@hyper-hyper-space/hhs3_crypto';
import type { dag } from '@hyper-hyper-space/hhs3_dag';
import type { json } from '@hyper-hyper-space/hhs3_json';

// --- Gossip ---

export type NewFrontierMsg = {
    type: 'new-frontier';
    dagId: B64Hash;
    frontier: B64Hash[];
};

// --- Header fetch ---

export type HeaderRequest = {
    type: 'header-request';
    requestId: string;
    dagId: B64Hash;
    start: B64Hash[];
    limits: B64Hash[];
    maxHeaders: number;
    autoPayload: boolean;
};

export type HeaderResponseMeta = {
    type: 'header-response-meta';
    requestId: string;
    headerCount: number;
    complete: boolean;
    payloadCount: number;
};

export type HeaderBatch = {
    type: 'header-batch';
    requestId: string;
    sequence: number;
    headers: Array<{ hash: B64Hash; header: dag.Header }>;
};

// --- Payload fetch ---

export type PayloadRequest = {
    type: 'payload-request';
    requestId: string;
    dagId: B64Hash;
    hashes: B64Hash[];
};

export type PayloadResponseMeta = {
    type: 'payload-response-meta';
    requestId: string;
    payloadCount: number;
};

export type PayloadMsg = {
    type: 'payload-msg';
    requestId: string;
    sequence: number;
    hash: B64Hash;
    payload: json.Literal;
};

// --- Control ---

export type CancelRequest = {
    type: 'cancel-request';
    requestId: string;
};

// --- Union ---

export type SyncMsg =
    | NewFrontierMsg
    | HeaderRequest
    | HeaderResponseMeta
    | HeaderBatch
    | PayloadRequest
    | PayloadResponseMeta
    | PayloadMsg
    | CancelRequest;

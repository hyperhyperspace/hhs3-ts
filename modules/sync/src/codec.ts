import { json } from '@hyper-hyper-space/hhs3_json';

import type { SyncMsg } from './protocol.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encode(msg: SyncMsg): Uint8Array {
    return encoder.encode(JSON.stringify(msg));
}

// Throws unless the bytes are JSON text for a Literal. JSON text can carry
// what a Literal cannot: null, and a lone surrogate written as an escape
// ("\ud800"). Rejecting the whole message here keeps both out of every header
// and payload the receiver goes on to hash.
export function decode(data: Uint8Array): SyncMsg {
    const msg: unknown = JSON.parse(decoder.decode(data));
    const problem = json.literalProblem(msg);
    if (problem !== undefined) {
        throw new Error('A sync message cannot contain ' + problem + '.');
    }
    return msg as SyncMsg;
}

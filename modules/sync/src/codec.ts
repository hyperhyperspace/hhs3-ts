import { json } from '@hyper-hyper-space/hhs3_json';

import type { SyncMsg } from './protocol.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encode(msg: SyncMsg): Uint8Array {
    return encoder.encode(JSON.stringify(msg));
}

// A message holds a payload one level in (payload-msg.payload,
// init-response.createPayload), so it may nest one level deeper than a
// Literal: a payload at the limit must still decode.
const MAX_MESSAGE_DEPTH = json.MAX_LITERAL_DEPTH + 1;

// Throws unless the bytes are JSON text for a Literal. JSON text can carry
// what a Literal cannot: null, a lone surrogate written as an escape
// ("\ud800"), and nesting of any depth. Rejecting the whole message here keeps
// all three out of every header and payload the receiver goes on to hash, and
// bounds the recursion of that hashing.
export function decode(data: Uint8Array): SyncMsg {
    const msg: unknown = JSON.parse(decoder.decode(data));
    const problem = json.literalProblem(msg, MAX_MESSAGE_DEPTH);
    if (problem !== undefined) {
        throw new Error('A sync message cannot contain ' + problem + '.');
    }
    return msg as SyncMsg;
}

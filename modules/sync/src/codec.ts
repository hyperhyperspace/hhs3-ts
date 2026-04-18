import type { SyncMsg } from './protocol.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encode(msg: SyncMsg): Uint8Array {
    return encoder.encode(JSON.stringify(msg));
}

export function decode(data: Uint8Array): SyncMsg {
    return JSON.parse(decoder.decode(data)) as SyncMsg;
}

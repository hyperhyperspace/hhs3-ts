// The wire protocol between a client and a running host: one JSON message per
// line. The channel is read-only; it carries the host's live state and nothing
// that signs.
//
//   client -> host   { id, method: 'status' }
//                    { id, method: 'watchStatus' }
//                    { id, method: 'unwatch', watch: <id of the watchStatus request> }
//   host -> client   { id, result }         the reply to a request
//                    { id, error }          a failed request
//                    { id, event }          a push on a watch, tagged with its request id

import type { HostStatus } from "./types.js";

export type ClientMethod = 'status' | 'watchStatus' | 'unwatch';

export type ClientRequest =
    | { id: number; method: 'status' }
    | { id: number; method: 'watchStatus' }
    | { id: number; method: 'unwatch'; watch: number };

export type ClientResult = { id: number; result: HostStatus | null };
export type ClientError = { id: number; error: string };
export type ClientPush = { id: number; event: HostStatus };

export type ServerMessage = ClientResult | ClientError | ClientPush;

export class ProtocolError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ProtocolError';
    }
}

export function encodeMessage(message: ClientRequest | ServerMessage): string {
    return JSON.stringify(message) + '\n';
}

function parseObject(line: string): { [key: string]: unknown } {
    let value: unknown;
    try {
        value = JSON.parse(line);
    } catch {
        throw new ProtocolError(`not JSON: ${line.slice(0, 80)}`);
    }
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
        throw new ProtocolError('a message must be a JSON object');
    }
    const o = value as { [key: string]: unknown };
    if (!Number.isSafeInteger(o['id'])) throw new ProtocolError('a message needs an integer id');
    return o;
}

export function decodeRequest(line: string): ClientRequest {
    const o = parseObject(line);
    const id = o['id'] as number;
    switch (o['method']) {
        case 'status': return { id, method: 'status' };
        case 'watchStatus': return { id, method: 'watchStatus' };
        case 'unwatch':
            if (!Number.isSafeInteger(o['watch'])) throw new ProtocolError('unwatch needs the id of a watchStatus request');
            return { id, method: 'unwatch', watch: o['watch'] as number };
        default:
            throw new ProtocolError(`unknown method ${JSON.stringify(o['method'])}`);
    }
}

export function decodeServerMessage(line: string): ServerMessage {
    const o = parseObject(line);
    const id = o['id'] as number;
    if ('error' in o) return { id, error: String(o['error']) };
    if ('event' in o) return { id, event: o['event'] as HostStatus };
    if ('result' in o) return { id, result: o['result'] as HostStatus | null };
    throw new ProtocolError('a server message has a result, an error or an event');
}

// Splits a stream of text chunks into lines.
export class LineDecoder {
    private buffer = '';

    push(chunk: string): string[] {
        this.buffer += chunk;
        const lines = this.buffer.split('\n');
        this.buffer = lines.pop()!;
        return lines.filter((line) => line.trim().length > 0);
    }
}

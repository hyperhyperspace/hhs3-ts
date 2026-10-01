// Answers the client protocol for one host over any line-based connection: a
// Unix socket on Node, a BroadcastChannel in a browser.

import {
    decodeRequest, encodeMessage,
    type ClientRequest, type HostStatus, type ServerMessage,
} from "@hyper-hyper-space/hhs3_rhost_client";

import type { Host } from "./host.js";

export interface Connection {
    send(line: string): void;
    onLine(listener: (line: string) => void): void;
    onClose(listener: () => void): void;
}

function errorMessage(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
}

// The id of a line that failed to decode, when it has one.
function lineId(line: string): number {
    try {
        const id = (JSON.parse(line) as { id?: unknown }).id;
        return Number.isSafeInteger(id) ? id as number : 0;
    } catch {
        return 0;
    }
}

export function serveClient(host: Host, connection: Connection): void {
    const watches = new Map<number, () => void>();
    let closed = false;

    const send = (message: ServerMessage) => {
        if (closed) return;
        try {
            connection.send(encodeMessage(message));
        } catch {
            // the other end is gone
        }
    };

    const reply = (id: number, work: Promise<HostStatus>) => {
        work.then(
            (result) => send({ id, result }),
            (err) => send({ id, error: errorMessage(err) }),
        );
    };

    const handle = (request: ClientRequest) => {
        switch (request.method) {
            case 'status':
                reply(request.id, host.status());
                return;
            case 'watchStatus': {
                if (watches.has(request.id)) {
                    send({ id: request.id, error: `request id ${request.id} is already watching` });
                    return;
                }
                watches.set(request.id, host.onStatus((status) => send({ id: request.id, event: status })));
                reply(request.id, host.status());
                return;
            }
            case 'unwatch': {
                const unsubscribe = watches.get(request.watch);
                if (unsubscribe === undefined) {
                    send({ id: request.id, error: `no watch ${request.watch}` });
                    return;
                }
                unsubscribe();
                watches.delete(request.watch);
                send({ id: request.id, result: null });
                return;
            }
        }
    };

    connection.onLine((line) => {
        let request: ClientRequest;
        try {
            request = decodeRequest(line);
        } catch (err) {
            send({ id: lineId(line), error: errorMessage(err) });
            return;
        }
        handle(request);
    });

    connection.onClose(() => {
        closed = true;
        for (const unsubscribe of watches.values()) unsubscribe();
        watches.clear();
    });
}

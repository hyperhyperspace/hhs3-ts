// The client BroadcastChannel, adapted to rhost's line connection. One
// serveClient per client id. The channel does not deliver a message to the
// object that sent it.

import {
    isBrowserChannelMessage,
    type BrowserChannelMessage,
} from "@hyper-hyper-space/hhs3_rhost_client";
import type { Connection } from "@hyper-hyper-space/hhs3_rhost";

export type BrowserChannel = {
    postMessage(message: unknown): void;
    close(): void;
    onmessage: ((ev: { data: unknown }) => void) | null;
};

export type BrowserChannelCtor = new (name: string) => BrowserChannel;

type ClientSlot = {
    onLine?: (line: string) => void;
    onClose?: () => void;
};

export function listenOnChannel(
    name: string,
    Ctor: BrowserChannelCtor,
    onConnection: (connection: Connection) => void,
): () => Promise<void> {
    const channel = new Ctor(name);
    const clients = new Map<string, ClientSlot>();

    const deliver = (message: BrowserChannelMessage): void => {
        if ('stop' in message) return;
        const slot = clients.get(message.client);
        if ('close' in message) {
            if (slot === undefined) return;
            clients.delete(message.client);
            slot.onClose?.();
            return;
        }
        let current = slot;
        if (current === undefined) {
            current = {};
            clients.set(message.client, current);
            const client = message.client;
            onConnection({
                send(line) { channel.postMessage({ client, line }); },
                onLine(listener) { current!.onLine = listener; },
                onClose(listener) { current!.onClose = listener; },
            });
        }
        current.onLine?.(message.line);
    };

    channel.onmessage = (ev) => {
        if (isBrowserChannelMessage(ev.data)) deliver(ev.data);
    };

    return async () => {
        channel.onmessage = null;
        for (const [client, slot] of clients) {
            try { channel.postMessage({ client, close: true }); } catch { /* already closed */ }
            slot.onClose?.();
        }
        clients.clear();
        channel.close();
    };
}

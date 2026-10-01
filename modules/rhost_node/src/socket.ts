// The rhost.sock server: a running host answers the client protocol on a Unix
// socket at run/rhost.sock in its folder (a named pipe on Windows), readable
// only by the user.

import { promises as fs } from "node:fs";
import { createServer, type Socket } from "node:net";
import { dirname } from "node:path";

import type { Connection } from "@hyper-hyper-space/hhs3_rhost";
import { LineDecoder } from "@hyper-hyper-space/hhs3_rhost_client";

// sun_path holds 104 bytes on macOS and the BSDs, 108 on Linux, NUL included.
const MAX_SOCKET_PATH = process.platform === 'linux' ? 107 : 103;

export function checkSocketPath(path: string): void {
    if (process.platform === 'win32') return;
    const length = Buffer.byteLength(path);
    if (length > MAX_SOCKET_PATH) {
        throw new Error(`the socket path ${path} is ${length} bytes, over the ${MAX_SOCKET_PATH}-byte limit `
            + 'for Unix sockets on this system; move the app to a shorter folder');
    }
}

function connectionFor(socket: Socket): Connection {
    const decoder = new LineDecoder();
    const lineListeners: ((line: string) => void)[] = [];
    const closeListeners: (() => void)[] = [];
    socket.setEncoding('utf8');
    socket.on('data', (chunk: string) => {
        for (const line of decoder.push(chunk)) {
            for (const listener of lineListeners) listener(line);
        }
    });
    socket.on('error', () => { /* the close handler follows */ });
    socket.on('close', () => {
        for (const listener of closeListeners) listener();
    });
    return {
        send: (line) => { if (!socket.destroyed) socket.write(line); },
        onLine: (listener) => { lineListeners.push(listener); },
        onClose: (listener) => { closeListeners.push(listener); },
    };
}

// Listens on `path`, replacing a stale socket file (the caller holds the
// host's lock, so nothing else serves it). Returns the function that closes
// every connection, stops listening and removes the file.
export async function listenOnSocket(path: string, onConnection: (connection: Connection) => void): Promise<() => Promise<void>> {
    checkSocketPath(path);
    const posix = process.platform !== 'win32';
    if (posix) {
        await fs.mkdir(dirname(path), { recursive: true });
        await fs.rm(path, { force: true });
    }

    const sockets = new Set<Socket>();
    const server = createServer((socket) => {
        sockets.add(socket);
        socket.on('close', () => { sockets.delete(socket); });
        onConnection(connectionFor(socket));
    });
    const umask = posix ? process.umask(0o177) : undefined;
    try {
        await new Promise<void>((resolveListen, reject) => {
            server.once('error', reject);
            server.listen(path, () => {
                server.off('error', reject);
                resolveListen();
            });
        });
    } finally {
        if (umask !== undefined) process.umask(umask);
    }
    if (posix) await fs.chmod(path, 0o600);

    let closed = false;
    return async () => {
        if (closed) return;
        closed = true;
        for (const socket of sockets) socket.destroy();
        await new Promise<void>((resolveClose) => { server.close(() => resolveClose()); });
        if (posix) await fs.rm(path, { force: true });
    };
}

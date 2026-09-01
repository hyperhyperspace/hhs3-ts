import { testing } from '@hyper-hyper-space/hhs3_util';
import { WebSocket as NodeWs } from 'ws';
import type { Transport } from '@hyper-hyper-space/hhs3_mesh';
import { WsTransportProvider } from '@hyper-hyper-space/hhs3_mesh_ws';
import { BrowserWsTransportProvider, type WebSocketCtor } from '../src/index.js';

const NodeWsCtor = NodeWs as unknown as WebSocketCtor;

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) return false;
    }
    return true;
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function browserClient(): BrowserWsTransportProvider {
    return new BrowserWsTransportProvider({ WebSocketCtor: NodeWsCtor });
}

async function withServer<T>(
    fn: (server: WsTransportProvider, port: number, inbound: () => Transport | undefined) => Promise<T>,
): Promise<T> {
    const server = new WsTransportProvider();
    let inbound: Transport | undefined;
    await server.listen('ws://127.0.0.1:0', (t) => { inbound = t; });
    const port = server.serverPort()!;
    try {
        return await fn(server, port, () => inbound);
    } finally {
        server.close();
    }
}

async function testListenRejected() {
    const client = browserClient();
    let threw = false;
    try {
        await client.listen('ws://127.0.0.1:1', () => {});
    } catch (e) {
        threw = true;
        testing.assertTrue(
            (e as Error).message.includes('dial-only'),
            'listen() should mention dial-only',
        );
    }
    testing.assertTrue(threw, 'listen() should reject');
    client.close();
}

async function testSchemeDefaultAndWss() {
    const ws = new BrowserWsTransportProvider({ WebSocketCtor: NodeWsCtor });
    testing.assertEquals(ws.scheme, 'ws', 'default scheme should be ws');
    ws.close();

    const wss = new BrowserWsTransportProvider({ scheme: 'wss', WebSocketCtor: NodeWsCtor });
    testing.assertEquals(wss.scheme, 'wss', 'scheme option should set wss');
    wss.close();
}

async function testSendReceiveRoundTrip() {
    await withServer(async (server, port, getInbound) => {
        const clientProvider = browserClient();
        const client = await clientProvider.connect(`ws://127.0.0.1:${port}`);
        await delay(20);

        const serverTransport = getInbound();
        testing.assertTrue(serverTransport !== undefined, 'server should receive connection');

        const serverReceived: Uint8Array[] = [];
        const clientReceived: Uint8Array[] = [];
        serverTransport!.onMessage((msg) => {
            serverReceived.push(msg);
            serverTransport!.send(msg);
        });
        client.onMessage((msg) => clientReceived.push(msg));

        const message = new TextEncoder().encode('hello browser-ws');
        client.send(message);
        await delay(50);

        testing.assertEquals(serverReceived.length, 1, 'server should receive one message');
        testing.assertTrue(bytesEqual(serverReceived[0], message), 'server message content should match');
        testing.assertEquals(clientReceived.length, 1, 'client should receive echo');
        testing.assertTrue(bytesEqual(clientReceived[0], message), 'echo content should match');
        testing.assertTrue(serverReceived[0] instanceof Uint8Array, 'server message should be Uint8Array');
        testing.assertTrue(clientReceived[0] instanceof Uint8Array, 'client message should be Uint8Array');

        clientProvider.close();
    });
}

async function testBidirectional() {
    await withServer(async (_server, port, getInbound) => {
        const clientProvider = browserClient();
        const client = await clientProvider.connect(`ws://127.0.0.1:${port}`);
        await delay(20);

        const fromClient: Uint8Array[] = [];
        const fromServer: Uint8Array[] = [];
        getInbound()!.onMessage((msg) => fromClient.push(msg));
        client.onMessage((msg) => fromServer.push(msg));

        client.send(new TextEncoder().encode('to-server'));
        getInbound()!.send(new TextEncoder().encode('to-client'));
        await delay(50);

        testing.assertEquals(fromClient.length, 1, 'server should receive from client');
        testing.assertEquals(fromServer.length, 1, 'client should receive from server');
        testing.assertTrue(
            bytesEqual(fromClient[0], new TextEncoder().encode('to-server')),
            'server received correct content',
        );
        testing.assertTrue(
            bytesEqual(fromServer[0], new TextEncoder().encode('to-client')),
            'client received correct content',
        );

        clientProvider.close();
    });
}

async function testClosePropagation() {
    await withServer(async (_server, port, getInbound) => {
        const clientProvider = browserClient();
        const client = await clientProvider.connect(`ws://127.0.0.1:${port}`);
        await delay(20);

        let serverClosed = false;
        let clientClosed = false;
        getInbound()!.onClose(() => { serverClosed = true; });
        client.onClose(() => { clientClosed = true; });

        client.close();
        await delay(50);

        testing.assertTrue(clientClosed, 'client onClose should fire');
        testing.assertTrue(serverClosed, 'server onClose should fire when client closes');

        clientProvider.close();
    });
}

async function testBinaryIntegrity() {
    await withServer(async (_server, port, getInbound) => {
        const clientProvider = browserClient();
        const client = await clientProvider.connect(`ws://127.0.0.1:${port}`);
        await delay(20);

        const received: Uint8Array[] = [];
        getInbound()!.onMessage((msg) => received.push(msg));

        const binary = new Uint8Array(256);
        for (let i = 0; i < 256; i++) binary[i] = i;
        client.send(binary);
        await delay(50);

        testing.assertEquals(received.length, 1, 'should receive binary message');
        testing.assertEquals(received[0].length, 256, 'binary length should match');
        testing.assertTrue(bytesEqual(received[0], binary), 'all 256 byte values should survive round-trip');

        clientProvider.close();
    });
}

async function testConnectFailure() {
    const clientProvider = browserClient();
    let threw = false;
    try {
        await clientProvider.connect('ws://127.0.0.1:1');
    } catch {
        threw = true;
    }
    testing.assertTrue(threw, 'connect to a closed port should reject');
    clientProvider.close();
}

async function testProviderClose() {
    await withServer(async (_server, port, getInbound) => {
        const clientProvider = browserClient();
        const client = await clientProvider.connect(`ws://127.0.0.1:${port}`);
        await delay(20);

        let clientClosed = false;
        let serverClosed = false;
        client.onClose(() => { clientClosed = true; });
        getInbound()!.onClose(() => { serverClosed = true; });

        clientProvider.close();
        await delay(100);

        testing.assertTrue(clientClosed, 'client should close when provider closes');
        testing.assertTrue(serverClosed, 'server transport should close when client provider closes');
    });
}

export const browserWsTests = {
    title: '[BROWSER_WS] Browser WebSocket transport',
    tests: [
        { name: '[BWS_00] listen() is rejected', invoke: testListenRejected },
        { name: '[BWS_01] scheme default and wss', invoke: testSchemeDefaultAndWss },
        { name: '[BWS_02] Send/receive round-trip', invoke: testSendReceiveRoundTrip },
        { name: '[BWS_03] Bidirectional messaging', invoke: testBidirectional },
        { name: '[BWS_04] Close propagation', invoke: testClosePropagation },
        { name: '[BWS_05] Binary integrity', invoke: testBinaryIntegrity },
        { name: '[BWS_06] Connect failure', invoke: testConnectFailure },
        { name: '[BWS_07] Provider close', invoke: testProviderClose },
    ],
};

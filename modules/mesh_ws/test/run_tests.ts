import { testing } from '@hyper-hyper-space/hhs3_util';
import { WsTransportProvider, type WsClientCtor } from '../src/ws_transport_provider.js';
import type { Transport } from '@hyper-hyper-space/hhs3_mesh';

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

// --- tests ---

async function testSendReceiveRoundTrip() {
    const provider = new WsTransportProvider();

    let serverTransport: Transport | undefined;
    await provider.listen('ws://127.0.0.1:0', (t) => { serverTransport = t; });

    const port = provider.serverPort()!;
    const client = await provider.connect(`ws://127.0.0.1:${port}`);

    await delay(20);
    testing.assertTrue(serverTransport !== undefined, 'server should receive connection');

    const serverReceived: Uint8Array[] = [];
    const clientReceived: Uint8Array[] = [];

    serverTransport!.onMessage((msg) => {
        serverReceived.push(msg);
        serverTransport!.send(msg);
    });
    client.onMessage((msg) => clientReceived.push(msg));

    const message = new TextEncoder().encode('hello ws');
    client.send(message);

    await delay(50);

    testing.assertEquals(serverReceived.length, 1, 'server should receive one message');
    testing.assertTrue(bytesEqual(serverReceived[0], message), 'server message content should match');
    testing.assertEquals(clientReceived.length, 1, 'client should receive echo');
    testing.assertTrue(bytesEqual(clientReceived[0], message), 'echo content should match');

    testing.assertTrue(serverReceived[0] instanceof Uint8Array, 'server message should be Uint8Array');
    testing.assertTrue(clientReceived[0] instanceof Uint8Array, 'client message should be Uint8Array');

    provider.close();
}

async function testBidirectional() {
    const provider = new WsTransportProvider();

    let serverTransport: Transport | undefined;
    await provider.listen('ws://127.0.0.1:0', (t) => { serverTransport = t; });

    const port = provider.serverPort()!;
    const client = await provider.connect(`ws://127.0.0.1:${port}`);

    await delay(20);

    const fromClient: Uint8Array[] = [];
    const fromServer: Uint8Array[] = [];

    serverTransport!.onMessage((msg) => fromClient.push(msg));
    client.onMessage((msg) => fromServer.push(msg));

    client.send(new TextEncoder().encode('to-server'));
    serverTransport!.send(new TextEncoder().encode('to-client'));

    await delay(50);

    testing.assertEquals(fromClient.length, 1, 'server should receive from client');
    testing.assertEquals(fromServer.length, 1, 'client should receive from server');
    testing.assertTrue(
        bytesEqual(fromClient[0], new TextEncoder().encode('to-server')),
        'server received correct content'
    );
    testing.assertTrue(
        bytesEqual(fromServer[0], new TextEncoder().encode('to-client')),
        'client received correct content'
    );

    provider.close();
}

async function testClosePropagation() {
    const provider = new WsTransportProvider();

    let serverTransport: Transport | undefined;
    await provider.listen('ws://127.0.0.1:0', (t) => { serverTransport = t; });

    const port = provider.serverPort()!;
    const client = await provider.connect(`ws://127.0.0.1:${port}`);

    await delay(20);

    let serverClosed = false;
    let clientClosed = false;

    serverTransport!.onClose(() => { serverClosed = true; });
    client.onClose(() => { clientClosed = true; });

    client.close();

    await delay(50);

    testing.assertTrue(clientClosed, 'client onClose should fire');
    testing.assertTrue(serverClosed, 'server onClose should fire when client closes');

    provider.close();
}

async function testMultipleClients() {
    const provider = new WsTransportProvider();

    const serverTransports: Transport[] = [];
    await provider.listen('ws://127.0.0.1:0', (t) => { serverTransports.push(t); });

    const port = provider.serverPort()!;
    const client1 = await provider.connect(`ws://127.0.0.1:${port}`);
    const client2 = await provider.connect(`ws://127.0.0.1:${port}`);

    await delay(30);

    testing.assertEquals(serverTransports.length, 2, 'server should accept two connections');

    const received1: Uint8Array[] = [];
    const received2: Uint8Array[] = [];

    client1.onMessage((msg) => received1.push(msg));
    client2.onMessage((msg) => received2.push(msg));

    serverTransports[0].send(new TextEncoder().encode('for-client-1'));
    serverTransports[1].send(new TextEncoder().encode('for-client-2'));

    await delay(50);

    testing.assertEquals(received1.length, 1, 'client1 should receive one message');
    testing.assertEquals(received2.length, 1, 'client2 should receive one message');
    testing.assertTrue(
        bytesEqual(received1[0], new TextEncoder().encode('for-client-1')),
        'client1 received correct message'
    );
    testing.assertTrue(
        bytesEqual(received2[0], new TextEncoder().encode('for-client-2')),
        'client2 received correct message'
    );

    provider.close();
}

async function testProviderClose() {
    const provider = new WsTransportProvider();

    let serverTransport: Transport | undefined;
    await provider.listen('ws://127.0.0.1:0', (t) => { serverTransport = t; });

    const port = provider.serverPort()!;
    const client = await provider.connect(`ws://127.0.0.1:${port}`);

    await delay(20);

    let clientClosed = false;
    let serverClosed = false;

    client.onClose(() => { clientClosed = true; });
    serverTransport!.onClose(() => { serverClosed = true; });

    provider.close();

    await delay(100);

    testing.assertTrue(clientClosed, 'client should close when provider closes');
    testing.assertTrue(serverClosed, 'server transport should close when provider closes');
}

async function testSchemeDefaultAndWss() {
    const ws = new WsTransportProvider();
    testing.assertEquals(ws.scheme, 'ws', 'default scheme should be ws');
    ws.close();

    const wss = new WsTransportProvider('wss');
    testing.assertEquals(wss.scheme, 'wss', 'constructor should set wss');

    let threw = false;
    try {
        await wss.listen('wss://127.0.0.1:0', () => {});
    } catch (e) {
        threw = true;
        testing.assertTrue(
            (e as Error).message.includes('TLS termination'),
            'wss listen() should mention TLS termination',
        );
    }
    testing.assertTrue(threw, 'wss listen() should throw');
    wss.close();
}

async function testBinaryIntegrity() {
    const provider = new WsTransportProvider();

    let serverTransport: Transport | undefined;
    await provider.listen('ws://127.0.0.1:0', (t) => { serverTransport = t; });

    const port = provider.serverPort()!;
    const client = await provider.connect(`ws://127.0.0.1:${port}`);

    await delay(20);

    const received: Uint8Array[] = [];
    serverTransport!.onMessage((msg) => received.push(msg));

    const binary = new Uint8Array(256);
    for (let i = 0; i < 256; i++) binary[i] = i;
    client.send(binary);

    await delay(50);

    testing.assertEquals(received.length, 1, 'should receive binary message');
    testing.assertEquals(received[0].length, 256, 'binary length should match');
    testing.assertTrue(bytesEqual(received[0], binary), 'all 256 byte values should survive round-trip');

    provider.close();
}

async function testConnectTimeout() {
    const hung: HungSocket[] = [];

    class HungSocket {
        binaryType = 'nodebuffer';
        closed = false;
        constructor(_url: string) {
            hung.push(this);
        }
        on(_event: string, _cb: (...args: unknown[]) => void): void {}
        close(): void {
            this.closed = true;
        }
    }

    const provider = new WsTransportProvider('ws', {
        connectTimeoutMs: 50,
        WebSocketCtor: HungSocket as unknown as WsClientCtor,
    });
    const started = Date.now();
    let threw = false;
    try {
        await provider.connect('ws://127.0.0.1:9');
    } catch (e) {
        threw = true;
        testing.assertTrue(
            (e as Error).message.includes('timeout'),
            `connect should mention timeout (got: ${(e as Error).message})`,
        );
    }
    testing.assertTrue(threw, 'hung connect should reject');
    testing.assertTrue(Date.now() - started < 1000, 'hung connect should fail well under a second');
    testing.assertTrue(hung.length === 1 && hung[0]!.closed, 'timed-out socket should be closed');
    provider.close();
}

// --- main ---

const allSuites = [
    {
        title: '[WS] WebSocket transport',
        tests: [
            { name: '[WS_00] Send/receive round-trip', invoke: testSendReceiveRoundTrip },
            { name: '[WS_01] Bidirectional messaging', invoke: testBidirectional },
            { name: '[WS_02] Close propagation', invoke: testClosePropagation },
            { name: '[WS_03] Multiple clients', invoke: testMultipleClients },
            { name: '[WS_04] Provider close', invoke: testProviderClose },
            { name: '[WS_05] Binary integrity', invoke: testBinaryIntegrity },
            { name: '[WS_06] Scheme default and wss listen guard', invoke: testSchemeDefaultAndWss },
            { name: '[WS_07] Connect timeout closes hung socket', invoke: testConnectTimeout },
        ],
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for HHSv3 mesh_ws module' + (filters.length > 0 ? ` (filter: ${filters})` : '') + '\n');

    for (const suite of allSuites) {
        console.log(suite.title);
        for (const test of suite.tests) {
            let match = true;
            for (const filter of filters) {
                match = match && test.name.indexOf(filter) >= 0;
            }
            if (match) {
                testing.exitIfFailed(await testing.run(test.name, test.invoke));
            } else {
                await testing.skip(test.name);
            }
        }
        console.log();
    }
}

main();

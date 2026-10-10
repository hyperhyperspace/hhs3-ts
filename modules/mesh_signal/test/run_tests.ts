import { createServer } from 'node:net';
import WebSocket from 'ws';
import { testing } from '@hyper-hyper-space/hhs3_util';
import {
    createIdentity,
    sha256,
    SIGNING_ED25519,
    type OwnIdentity,
} from '@hyper-hyper-space/hhs3_crypto';
import {
    delegationToWire,
    encodeSignalMessage,
    parseSignalMessage,
    RtcTransportProvider,
    signDelegation,
    type SignalSocket,
} from '@hyper-hyper-space/hhs3_mesh_rtc';
import type { Transport } from '@hyper-hyper-space/hhs3_mesh';
import { publicOriginFor, SignalServer } from '../src/index.js';
import { FakePeer, hungPeer, resetFakePeers } from './fake_peer.js';

async function freePort(): Promise<number> {
    return new Promise((resolve, reject) => {
        const server = createServer();
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => {
            const addr = server.address();
            const port = typeof addr === 'object' && addr !== null ? addr.port : 0;
            server.close(err => err ? reject(err) : resolve(port));
        });
    });
}

function openTestSignaling(url: string): Promise<SignalSocket> {
    const wsUrl = url.replace(/^wss:/, 'ws:');
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(wsUrl);
        const timer = setTimeout(() => {
            try { ws.close(); } catch { /* ignore */ }
            reject(new Error('signaling socket timeout'));
        }, 2_000);
        const pending: string[] = [];
        const messages: ((text: string) => void)[] = [];
        const closes: (() => void)[] = [];
        let closed = false;
        ws.on('message', data => {
            const text = typeof data === 'string' ? data : data.toString('utf8');
            if (messages.length === 0) pending.push(text);
            else for (const cb of messages) cb(text);
        });
        ws.on('close', () => {
            closed = true;
            for (const cb of closes) cb();
        });
        ws.once('open', () => {
            clearTimeout(timer);
            resolve({
                send(text: string) { ws.send(text); },
                close() { try { ws.close(); } catch { /* ignore */ } },
                onMessage(cb) {
                    messages.push(cb);
                    if (pending.length === 0) return;
                    const batch = pending.splice(0, pending.length);
                    for (const text of batch) cb(text);
                },
                onClose(cb) {
                    closes.push(cb);
                    if (closed) cb();
                },
            });
        });
        ws.once('error', () => {
            clearTimeout(timer);
            reject(new Error('signaling socket failed'));
        });
    });
}

async function boot(opts: { publicMode?: boolean; allow?: OwnIdentity[]; now?: () => number; renewLeadMs?: number }) {
    const port = await freePort();
    const origin = publicOriginFor('127.0.0.1', port);
    const signalBase = origin;
    const server = new SignalServer({
        host: '127.0.0.1',
        port,
        publicOrigin: origin,
        publicMode: opts.publicMode ?? false,
        allow: opts.allow?.map(id => id.keyId),
        now: opts.now,
        renewLeadMs: opts.renewLeadMs,
        renewCheckMs: 60_000,
    });
    await server.start();
    return { server, signalBase, origin };
}

function provider(identity: OwnIdentity, signalBase: string, extra?: {
    createPeer?: () => FakePeer;
    connectTimeoutMs?: number;
    now?: () => number;
    delegationTtlSec?: number;
    endpointId?: string;
}): RtcTransportProvider {
    return new RtcTransportProvider({
        identity,
        signalBase,
        endpointId: extra?.endpointId,
        iceServers: [],
        connectTimeoutMs: extra?.connectTimeoutMs,
        now: extra?.now,
        delegationTtlSec: extra?.delegationTtlSec,
        createPeer: () => extra?.createPeer?.() ?? new FakePeer(),
        openSignaling: openTestSignaling,
    });
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function testModes() {
    resetFakePeers();
    const alice = await createIdentity(SIGNING_ED25519, sha256);
    const bob = await createIdentity(SIGNING_ED25519, sha256);
    const { server, signalBase } = await boot({ publicMode: false, allow: [alice] });
    const aliceRtc = provider(alice, signalBase);
    const bobRtc = provider(bob, signalBase);
    try {
        await aliceRtc.listen(aliceRtc.localAddress!, () => {});
        testing.assertEquals(server.registrationCount(), 1, 'allowlisted key registers');
        let rejected = '';
        try {
            await bobRtc.listen(bobRtc.localAddress!, () => {});
        } catch (err) {
            rejected = err instanceof Error ? err.message : '';
        }
        testing.assertTrue(rejected.includes('not allowed'), `private server rejects other keys (${rejected})`);
    } finally {
        aliceRtc.close();
        bobRtc.close();
        server.stop();
    }

    const pub = await boot({ publicMode: true });
    const a2 = provider(alice, pub.signalBase);
    const b2 = provider(bob, pub.signalBase);
    try {
        await a2.listen(a2.localAddress!, () => {});
        await b2.listen(b2.localAddress!, () => {});
        testing.assertEquals(pub.server.registrationCount(), 2, 'public server accepts both keys');
    } finally {
        a2.close();
        b2.close();
        pub.server.stop();
    }
}

async function testBadPossession() {
    const alice = await createIdentity(SIGNING_ED25519, sha256);
    const { server, origin } = await boot({ publicMode: true });
    const endpointId = 'copied-endpoint';
    const url = `ws://127.0.0.1:${server.port}/${encodeURIComponent(endpointId)}`;
    const ws = new WebSocket(url);
    try {
        const text = await new Promise<string>((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error('no hello')), 2_000);
            ws.once('message', data => {
                clearTimeout(timer);
                resolve(typeof data === 'string' ? data : data.toString('utf8'));
            });
            ws.once('error', reject);
        });
        const hello = parseSignalMessage(text);
        testing.assertEquals(hello.type, 'hello', 'server greets');
        const expiry = Math.floor(Date.now() / 1000) + 60;
        const delegation = delegationToWire(await signDelegation(alice, origin, endpointId, expiry));
        ws.send(encodeSignalMessage({ type: 'register', delegation, possessionSig: 'AAAA' }));
        const reply = await new Promise<string>((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error('no reject')), 2_000);
            ws.once('message', data => {
                clearTimeout(timer);
                resolve(typeof data === 'string' ? data : data.toString('utf8'));
            });
        });
        const msg = parseSignalMessage(reply);
        testing.assertTrue(msg.type === 'reject' && msg.reason === 'bad possession', `copied delegation cannot register (${reply})`);
        testing.assertEquals(server.registrationCount(), 0, 'slot stays empty');
    } finally {
        ws.close();
        server.stop();
    }
}

async function testRenewalKeepsEndpoint() {
    let now = 1_700_000_000_000;
    const alice = await createIdentity(SIGNING_ED25519, sha256);
    const { server, signalBase, origin } = await boot({ publicMode: true, now: () => now, renewLeadMs: 0 });
    const rtc = provider(alice, signalBase, { now: () => now, delegationTtlSec: 100, endpointId: 'stable-ep' });
    try {
        await rtc.listen(rtc.localAddress!, () => {});
        const first = await readWarrant(server.port, rtc.endpointId);
        testing.assertEquals(first.endpointId, 'stable-ep', 'endpoint id');
        testing.assertEquals(first.origin, origin, 'origin');
        now += 5_000;
        server.requestRenewals(true);
        await delay(100);
        const second = await readWarrant(server.port, rtc.endpointId);
        testing.assertEquals(second.endpointId, first.endpointId, 'renewal keeps the endpoint id');
        testing.assertTrue(second.expiry > first.expiry, `expiry moves forward (${first.expiry} -> ${second.expiry})`);
        testing.assertEquals(rtc.localAddress, `rtc://127.0.0.1:${server.port}/stable-ep`, 'announced address is unchanged');
    } finally {
        rtc.close();
        server.stop();
    }
}

async function readWarrant(port: number, endpointId: string) {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/${encodeURIComponent(endpointId)}`);
    const messages: string[] = [];
    await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('warrant timeout')), 2_000);
        ws.on('message', data => {
            messages.push(typeof data === 'string' ? data : data.toString('utf8'));
            const last = parseSignalMessage(messages[messages.length - 1]!);
            if (last.type === 'hello') {
                ws.send(encodeSignalMessage({ type: 'dial', session: 'probe' }));
            }
            if (last.type === 'warrant' || last.type === 'reject') {
                clearTimeout(timer);
                resolve();
            }
        });
        ws.once('error', reject);
    });
    ws.close();
    const warrant = messages.map(text => parseSignalMessage(text)).find(msg => msg.type === 'warrant');
    if (warrant === undefined || warrant.type !== 'warrant') throw new Error('no warrant');
    return warrant.delegation;
}

async function testDataAndGlare() {
    resetFakePeers();
    const alice = await createIdentity(SIGNING_ED25519, sha256);
    const bob = await createIdentity(SIGNING_ED25519, sha256);
    const { server, signalBase } = await boot({ publicMode: true });
    const a = provider(alice, signalBase, { endpointId: 'aaaa-endpoint' });
    const b = provider(bob, signalBase, { endpointId: 'zzzz-endpoint' });
    const inboundA: Transport[] = [];
    const inboundB: Transport[] = [];
    try {
        await a.listen(a.localAddress!, t => inboundA.push(t));
        await b.listen(b.localAddress!, t => inboundB.push(t));

        const dial = await a.connect(b.localAddress!, a.localAddress, bob.keyId);
        const received = await new Promise<Uint8Array>((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error('no mesh message')), 2_000);
            if (inboundB.length === 0) {
                reject(new Error('listener did not accept'));
                return;
            }
            inboundB[0]!.onMessage(msg => {
                clearTimeout(timer);
                resolve(msg);
            });
            dial.send(Uint8Array.from([1, 2, 3, 9]));
        });
        testing.assertEquals(received.length, 4, 'one mesh message');
        testing.assertEquals(received[3]!, 9, 'payload byte');
        testing.assertEquals(dial.remoteAddress, b.localAddress, 'dialer remote address');
        testing.assertEquals(inboundB[0]!.remoteAddress, a.localAddress, 'listener remote address');

        const big = new Uint8Array(40_000);
        big[0] = 7;
        big[big.length - 1] = 8;
        const bigGot = new Promise<Uint8Array>(resolve => inboundB[0]!.onMessage(resolve));
        dial.send(big);
        const bigMsg = await bigGot;
        testing.assertEquals(bigMsg.length, big.length, 'fragmented payload length');
        testing.assertEquals(bigMsg[0]!, 7, 'fragmented head');
        testing.assertEquals(bigMsg[bigMsg.length - 1]!, 8, 'fragmented tail');
        dial.close();
        await delay(20);

        const againA: Transport[] = [];
        const againB: Transport[] = [];
        a.close();
        b.close();
        const a2 = provider(alice, signalBase, { endpointId: 'aaaa-endpoint' });
        const b2 = provider(bob, signalBase, { endpointId: 'zzzz-endpoint' });
        await a2.listen(a2.localAddress!, t => againA.push(t));
        await b2.listen(b2.localAddress!, t => againB.push(t));
        const results = await Promise.allSettled([
            a2.connect(b2.localAddress!, a2.localAddress, bob.keyId),
            b2.connect(a2.localAddress!, b2.localAddress, alice.keyId),
        ]);
        await delay(50);
        const aPolite = a2.endpointId < b2.endpointId;
        const politeResult = aPolite ? results[0]! : results[1]!;
        const impoliteResult = aPolite ? results[1]! : results[0]!;
        testing.assertEquals(politeResult.status, 'rejected', 'polite side drops its outbound dial');
        testing.assertEquals(impoliteResult.status, 'fulfilled', 'impolite side keeps its dial');
        const politeInbound = aPolite ? againA : againB;
        const impoliteInbound = aPolite ? againB : againA;
        testing.assertEquals(politeInbound.length, 1, 'one inbound on the polite side');
        testing.assertEquals(impoliteInbound.length, 0, 'impolite side ignores the glare dial');
        if (impoliteResult.status === 'fulfilled') {
            const politeAddress = aPolite ? a2.localAddress : b2.localAddress;
            const impoliteAddress = aPolite ? b2.localAddress : a2.localAddress;
            testing.assertEquals(impoliteResult.value.remoteAddress, politeAddress, 'outbound remote address');
            testing.assertEquals(politeInbound[0]!.remoteAddress, impoliteAddress, 'inbound remote address');
            impoliteResult.value.close();
        }
        a2.close();
        b2.close();
    } finally {
        a.close();
        b.close();
        server.stop();
    }
}

async function testTimeoutsAndKey() {
    resetFakePeers();
    const alice = await createIdentity(SIGNING_ED25519, sha256);
    const bob = await createIdentity(SIGNING_ED25519, sha256);
    const { server, signalBase } = await boot({ publicMode: true });
    let created = 0;
    const listener = provider(alice, signalBase);
    const dialer = new RtcTransportProvider({
        identity: bob,
        iceServers: [],
        connectTimeoutMs: 200,
        createPeer: () => {
            created++;
            return hungPeer();
        },
        openSignaling: openTestSignaling,
    });
    const silent = new RtcTransportProvider({
        identity: bob,
        iceServers: [],
        connectTimeoutMs: 80,
        createPeer: () => {
            created++;
            return new FakePeer();
        },
        openSignaling: () => new Promise(() => {}),
    });
    try {
        await listener.listen(listener.localAddress!, () => {});

        let keyError = '';
        try {
            await dialer.connect(listener.localAddress!, undefined, bob.keyId);
        } catch (err) {
            keyError = err instanceof Error ? err.message : '';
        }
        testing.assertTrue(keyError.includes('key mismatch'), `wrong key fails before ICE (${keyError})`);
        testing.assertEquals(created, 0, 'no peer connection after a bad delegation');

        created = 0;
        let iceError = '';
        try {
            await dialer.connect(listener.localAddress!, undefined, alice.keyId);
        } catch (err) {
            iceError = err instanceof Error ? err.message : '';
        }
        testing.assertTrue(iceError.includes('rtc connect timeout: ice'), `ice stall is one connect timeout (${iceError})`);
        testing.assertTrue(created >= 1, 'peer connection starts only after the delegation checks');

        let signalError = '';
        try {
            await silent.connect(listener.localAddress!, undefined, alice.keyId);
        } catch (err) {
            signalError = err instanceof Error ? err.message : '';
        }
        testing.assertTrue(signalError.includes('rtc connect timeout: signaling'), `signaling stall (${signalError})`);
    } finally {
        listener.close();
        dialer.close();
        silent.close();
        server.stop();
    }
}

const allSuites = [
    {
        title: '[MESH_SIGNAL] Signaling server',
        tests: [
            { name: '[MESH_SIGNAL_00] private allowlist and public mode', invoke: testModes },
            { name: '[MESH_SIGNAL_01] copied delegation cannot take the slot', invoke: testBadPossession },
            { name: '[MESH_SIGNAL_02] renewal keeps the endpoint id', invoke: testRenewalKeepsEndpoint },
            { name: '[MESH_SIGNAL_03] data channel message, fragments, and glare', invoke: testDataAndGlare },
            { name: '[MESH_SIGNAL_04] key check precedes ICE; timeouts name the stage', invoke: testTimeoutsAndKey },
        ],
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for HHSv3 mesh_signal module' + (filters.length > 0 ? ` (filter: ${filters})` : '') + '\n');
    for (const suite of allSuites) {
        console.log(suite.title);
        for (const test of suite.tests) {
            let match = true;
            for (const filter of filters) match = match && test.name.indexOf(filter) >= 0;
            if (match) testing.exitIfFailed(await testing.run(test.name, test.invoke));
            else await testing.skip(test.name);
        }
        console.log();
    }
}

main();

import { createServer } from 'node:net';
import { testing } from '@hyper-hyper-space/hhs3_util';
import { createIdentity, sha256, SIGNING_ED25519 } from '@hyper-hyper-space/hhs3_crypto';
import { publicOriginFor, SignalServer } from '@hyper-hyper-space/hhs3_mesh_signal';
import type { Transport } from '@hyper-hyper-space/hhs3_mesh';
import { NodeRtcTransportProvider, openNodeSignaling } from '../src/index.js';

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

async function testWeriftPair() {
    const port = await freePort();
    const origin = publicOriginFor('127.0.0.1', port);
    const server = new SignalServer({
        host: '127.0.0.1',
        port,
        publicOrigin: origin,
        publicMode: true,
        renewCheckMs: 60_000,
    });
    await server.start();
    const alice = await createIdentity(SIGNING_ED25519, sha256);
    const bob = await createIdentity(SIGNING_ED25519, sha256);
    const openSignaling = (url: string) => openNodeSignaling(url.replace(/^wss:/, 'ws:'));
    const werift = { iceAdditionalHostAddresses: ['127.0.0.1'], iceServers: [] as { urls: string }[] };
    const a = new NodeRtcTransportProvider({
        identity: alice,
        signalBase: origin,
        iceServers: [],
        openSignaling,
        werift,
        connectTimeoutMs: 15_000,
    });
    const b = new NodeRtcTransportProvider({
        identity: bob,
        signalBase: origin,
        iceServers: [],
        openSignaling,
        werift,
        connectTimeoutMs: 15_000,
    });
    let inbound: Transport | undefined;
    try {
        await b.listen(b.localAddress!, t => { inbound = t; });
        const dial = await a.connect(b.localAddress!, a.localAddress, bob.keyId);
        if (inbound === undefined) throw new Error('no inbound transport');
        const got = new Promise<Uint8Array>((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error('no werift payload')), 5_000);
            inbound!.onMessage(msg => {
                clearTimeout(timer);
                resolve(msg);
            });
        });
        dial.send(Uint8Array.from([4, 5, 6]));
        const msg = await got;
        testing.assertEquals(msg.length, 3, 'werift carried one mesh message');
        testing.assertEquals(msg[0]!, 4, 'payload');
        dial.close();
    } finally {
        a.close();
        b.close();
        server.stop();
    }
}

const allSuites = [
    {
        title: '[MESH_RTC_NODE] werift transport',
        tests: [
            { name: '[MESH_RTC_NODE_00] two werift peers exchange a mesh message', invoke: testWeriftPair },
        ],
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for HHSv3 mesh_rtc_node module' + (filters.length > 0 ? ` (filter: ${filters})` : '') + '\n');
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

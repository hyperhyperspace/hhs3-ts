import { testing } from '@hyper-hyper-space/hhs3_util';
import { createIdentity, sha256, SIGNING_ED25519 } from '@hyper-hyper-space/hhs3_crypto';
import type { Transport } from '@hyper-hyper-space/hhs3_mesh';
import { NodeRtcTransportProvider } from '../src/index.js';

const SIGNAL_BASE = 'wss://signal.mypeer.net:4612';

async function testPublicSignaler() {
    const alice = await createIdentity(SIGNING_ED25519, sha256);
    const bob = await createIdentity(SIGNING_ED25519, sha256);
    const werift = { iceAdditionalHostAddresses: ['127.0.0.1'], iceServers: [] as { urls: string }[] };
    const a = new NodeRtcTransportProvider({
        identity: alice,
        signalBase: SIGNAL_BASE,
        iceServers: [],
        werift,
        connectTimeoutMs: 15_000,
    });
    const b = new NodeRtcTransportProvider({
        identity: bob,
        signalBase: SIGNAL_BASE,
        iceServers: [],
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
    }
}

async function main() {
    console.log('Running live test for HHSv3 mesh_rtc_node against ' + SIGNAL_BASE + '\n');
    testing.exitIfFailed(await testing.run(
        '[MESH_RTC_NODE_LIVE] two werift peers exchange a mesh message via signal.mypeer.net',
        testPublicSignaler,
    ));
}

main();

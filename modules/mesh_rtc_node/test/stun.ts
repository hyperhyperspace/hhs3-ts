import { testing } from '@hyper-hyper-space/hhs3_util';
import { createIdentity, sha256, SIGNING_ED25519 } from '@hyper-hyper-space/hhs3_crypto';
import type { Transport } from '@hyper-hyper-space/hhs3_mesh';
import type { PeerConfig } from 'werift';
import { NodeRtcTransportProvider } from '../src/index.js';

const SIGNAL_BASE = 'wss://signal.mypeer.net:4612';
const GOOGLE_STUN = 'stun:stun.l.google.com:19302';
const MYPEER_STUN = 'stun:stun.mypeer.net:3478';

async function testStun(stunUrl: string) {
    const alice = await createIdentity(SIGNING_ED25519, sha256);
    const bob = await createIdentity(SIGNING_ED25519, sha256);
    const seen = new Set<string>();
    const werift: Partial<PeerConfig> = {
        iceFilterCandidatePair: (pair) => {
            const line = `${pair.localCandidate.type} ${pair.localCandidate.host}:${pair.localCandidate.port}`
                + ` -> ${pair.remoteCandidate.type} ${pair.remoteCandidate.host}:${pair.remoteCandidate.port}`;
            if (!seen.has(line)) {
                seen.add(line);
                console.log('  ice ' + line);
            }
            return true;
        },
    };
    const iceServers = [{ urls: stunUrl }];
    const a = new NodeRtcTransportProvider({
        identity: alice,
        signalBase: SIGNAL_BASE,
        iceServers,
        werift,
        connectTimeoutMs: 20_000,
    });
    const b = new NodeRtcTransportProvider({
        identity: bob,
        signalBase: SIGNAL_BASE,
        iceServers,
        werift,
        connectTimeoutMs: 20_000,
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
    console.log('Running STUN tests for HHSv3 mesh_rtc_node\n');
    testing.exitIfFailed(await testing.run(
        '[MESH_RTC_NODE_STUN] two werift peers exchange a mesh message via Google STUN',
        () => testStun(GOOGLE_STUN),
    ));
    testing.exitIfFailed(await testing.run(
        '[MESH_RTC_NODE_STUN] two werift peers exchange a mesh message via stun.mypeer.net',
        () => testStun(MYPEER_STUN),
    ));
}

main();

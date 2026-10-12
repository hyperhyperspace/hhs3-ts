import { testing } from '@hyper-hyper-space/hhs3_util';
import {
    createIdentity,
    random,
    sha256,
    KEM_X25519_HKDF,
    SIGNING_ED25519,
} from '@hyper-hyper-space/hhs3_crypto';
import {
    createAuthenticator,
    type PeerInfo,
    type TopicId,
} from '@hyper-hyper-space/hhs3_mesh';
import { WsTransportProvider } from '@hyper-hyper-space/hhs3_mesh_ws';
import { TrackerClient } from '../src/tracker_client.js';
import {
    DEFAULT_INTERNET_TRACKER,
    DEFAULT_INTERNET_TRACKER_KEY,
} from '../src/tracker_config.js';

const TIMEOUT_MS = 15_000;

async function collect(client: TrackerClient, topic: TopicId): Promise<PeerInfo[]> {
    const peers: PeerInfo[] = [];
    for await (const peer of client.discover(topic)) peers.push(peer);
    return peers;
}

async function testPublicTracker() {
    const aliceId = await createIdentity(SIGNING_ED25519, sha256);
    const bobId = await createIdentity(SIGNING_ED25519, sha256);
    const topic = sha256.hashToB64(random.getBytes(32)) as TopicId;
    const aliceAddress = 'rtc://live-test/alice';
    const transport = new WsTransportProvider('wss', { connectTimeoutMs: TIMEOUT_MS });

    const aliceAuth = createAuthenticator({
        localKey: aliceId,
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });
    const bobAuth = createAuthenticator({
        localKey: bobId,
        signingName: SIGNING_ED25519,
        kemPrefs: [KEM_X25519_HKDF],
    });

    const alicePeer: PeerInfo = { keyId: aliceId.keyId, addresses: [aliceAddress] };
    const alice = new TrackerClient({
        trackerAddress: DEFAULT_INTERNET_TRACKER,
        trackerKeyId: DEFAULT_INTERNET_TRACKER_KEY,
        transportProvider: transport,
        authenticator: aliceAuth,
        localPeer: alicePeer,
        heartbeatInterval: 3_600_000,
        timeoutMs: TIMEOUT_MS,
    });
    const bob = new TrackerClient({
        trackerAddress: DEFAULT_INTERNET_TRACKER,
        trackerKeyId: DEFAULT_INTERNET_TRACKER_KEY,
        transportProvider: transport,
        authenticator: bobAuth,
        localPeer: { keyId: bobId.keyId, addresses: [] },
        heartbeatInterval: 3_600_000,
        timeoutMs: TIMEOUT_MS,
    });

    try {
        await alice.announce(topic, alicePeer);
        const found = await collect(bob, topic);
        const hit = found.find(peer => peer.keyId === aliceId.keyId);
        testing.assertTrue(hit !== undefined, 'bob sees alice on the public tracker');
        testing.assertTrue(
            hit?.addresses.includes(aliceAddress) === true,
            `alice address is published (got ${hit?.addresses.join(', ')})`,
        );

        await alice.leave(topic, aliceId.keyId);
        const after = await collect(bob, topic);
        testing.assertTrue(
            !after.some(peer => peer.keyId === aliceId.keyId),
            'alice is gone after leave',
        );
    } finally {
        await alice.close();
        await bob.close();
        transport.close();
    }
}

async function main() {
    console.log('Running live test for HHSv3 mesh_tracker_client against ' + DEFAULT_INTERNET_TRACKER + '\n');
    testing.exitIfFailed(await testing.run(
        '[TRACKER_CLIENT_LIVE] announce, query, and leave on tracker.mypeer.net',
        testPublicTracker,
    ));
}

main();

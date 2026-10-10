import { testing } from '@hyper-hyper-space/hhs3_util';
import {
    createIdentity,
    sha256,
    SIGNING_ED25519,
} from '@hyper-hyper-space/hhs3_crypto';
import {
    delegationToWire,
    encodeFrames,
    FrameDecoder,
    parseRtcAddress,
    rtcAddressFor,
    signDelegation,
    verifyDelegation,
    DEFAULT_MAX_FRAME,
} from '../src/index.js';

async function testAddressRoundTrip() {
    const id = 'abc-DEF_012';
    const address = rtcAddressFor('wss://signal.example.com:8443/hhs3/signal', id);
    testing.assertEquals(address, 'rtc://signal.example.com:8443/hhs3/signal/abc-DEF_012', 'formatted address');
    const parsed = parseRtcAddress(address);
    testing.assertEquals(parsed.endpointId, id, 'endpoint id');
    testing.assertEquals(parsed.mount, 'hhs3/signal', 'mount');
    testing.assertEquals(parsed.port, 8443, 'port');
    testing.assertEquals(parsed.signalingOrigin, 'wss://signal.example.com:8443/hhs3/signal', 'origin');
    testing.assertEquals(
        parsed.signalingUrl,
        'wss://signal.example.com:8443/hhs3/signal/abc-DEF_012',
        'signaling url',
    );
    testing.assertEquals(parseRtcAddress(parsed.address).address, parsed.address, 'stable format');
}

async function testDelegationChecks() {
    const alice = await createIdentity(SIGNING_ED25519, sha256);
    const bob = await createIdentity(SIGNING_ED25519, sha256);
    const origin = 'wss://signal.example.com:443';
    const now = 1_700_000_000_000;
    const expiry = Math.floor(now / 1000) + 60;
    const wire = delegationToWire(await signDelegation(alice, origin, 'ep-1', expiry));

    const keyId = await verifyDelegation(wire, { origin, nowMs: now, expectedKeyId: alice.keyId });
    testing.assertEquals(keyId, alice.keyId, 'delegation names alice');

    let originError = '';
    try {
        await verifyDelegation(wire, { origin: 'wss://evil.example', nowMs: now, expectedKeyId: alice.keyId });
    } catch (err) {
        originError = err instanceof Error ? err.message : '';
    }
    testing.assertTrue(originError.includes('origin mismatch'), `origin rejected (${originError})`);

    let keyError = '';
    try {
        await verifyDelegation(wire, { origin, nowMs: now, expectedKeyId: bob.keyId });
    } catch (err) {
        keyError = err instanceof Error ? err.message : '';
    }
    testing.assertTrue(keyError.includes('key mismatch'), `wrong key rejected (${keyError})`);

    let expired = '';
    try {
        await verifyDelegation(wire, { origin, nowMs: (expiry + 120) * 1000, expectedKeyId: alice.keyId });
    } catch (err) {
        expired = err instanceof Error ? err.message : '';
    }
    testing.assertTrue(expired.includes('expired'), `expired delegation rejected (${expired})`);
}

async function testFrames() {
    const small = new Uint8Array([1, 2, 3, 4]);
    const one = encodeFrames(small);
    testing.assertEquals(one.length, 1, 'small payload is one frame');
    const decoder = new FrameDecoder();
    const out = decoder.push(one[0]!);
    testing.assertTrue(out !== undefined && out.length === 4 && out[3] === 4, 'small payload round-trips');

    const big = new Uint8Array(DEFAULT_MAX_FRAME * 2 + 10);
    for (let i = 0; i < big.length; i++) big[i] = i % 251;
    const frames = encodeFrames(big);
    testing.assertTrue(frames.length > 1, 'large payload is fragmented');
    const again = new FrameDecoder();
    let joined: Uint8Array | undefined;
    for (const frame of frames) joined = again.push(frame) ?? joined;
    testing.assertTrue(joined !== undefined && joined.length === big.length, 'fragment length');
    testing.assertEquals(joined![0], big[0]!, 'fragment head');
    testing.assertEquals(joined![joined!.length - 1], big[big.length - 1]!, 'fragment tail');
}

const allSuites = [
    {
        title: '[MESH_RTC] Address and delegation',
        tests: [
            { name: '[MESH_RTC_00] rtc address embeds the signaling server', invoke: testAddressRoundTrip },
            { name: '[MESH_RTC_01] delegation checks origin, key, and expiry', invoke: testDelegationChecks },
            { name: '[MESH_RTC_02] frames reassemble one mesh message', invoke: testFrames },
        ],
    },
];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for HHSv3 mesh_rtc module' + (filters.length > 0 ? ` (filter: ${filters})` : '') + '\n');
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

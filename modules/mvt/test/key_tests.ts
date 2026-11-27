// validateKey (hips/0020-unified-write-api.md, Key columns): whether a string
// is the canonical base64 of a public key of a registered signing suite, and
// if not, why. The writer, every registerKey and lookupKey run it.

import { testing } from '@hyper-hyper-space/hhs3_util';
import {
    createIdentity, sha256, SIGNING_ED25519, SIGNING_ED25519_ML_DSA_65, SIGNING_ML_DSA_65, type PublicKey,
} from '@hyper-hyper-space/hhs3_crypto';

import { serializePublicKeyToBase64, validateKey } from '../src/authorship.js';

function fromHex(hex: string): Uint8Array {
    return Uint8Array.from(hex.match(/../g)!.map((byte) => parseInt(byte, 16)));
}

// Two of the ed25519 curve's small-order points, and bytes that aren't a point.
const NEUTRAL = fromHex('0100000000000000000000000000000000000000000000000000000000000000');
const ORDER_TWO = fromHex('ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f');
const NOT_A_POINT = fromHex('0200000000000000000000000000000000000000000000000000000000000000');

const encode = (pk: PublicKey): string => serializePublicKeyToBase64(pk);

function refused(text: string, what: string): string {
    const result = validateKey(text);
    if (result.valid) throw new Error(`${what} should be refused`);
    testing.assertTrue(result.why.reason.length > 0, `${what} is refused with a reason`);
    return result.why.reason;
}

async function testKeysOfEachSuite() {
    for (const suite of [SIGNING_ED25519, SIGNING_ML_DSA_65, SIGNING_ED25519_ML_DSA_65]) {
        const { publicKey } = await createIdentity(suite, sha256);
        testing.assertTrue(validateKey(encode(publicKey)).valid, `a ${suite} key pair's public key is valid`);
    }
    const pattern = Uint8Array.from({ length: 1952 }, (_, i) => (i * 7) % 256);
    testing.assertTrue(validateKey(encode({ suite: SIGNING_ML_DSA_65, key: pattern })).valid, 'any 1952 bytes are an ml-dsa-65 key');
}

async function testRefusals() {
    const { publicKey, keyId } = await createIdentity(SIGNING_ED25519, sha256);
    const pk = encode(publicKey);
    refused('', 'the empty string');
    refused(keyId, 'a key hash');
    refused('not a key', 'a bare word');
    refused(encode({ suite: 'no-such-suite', key: new Uint8Array(32).fill(7) }), 'a suite that is not registered');
    refused(encode({ suite: SIGNING_ED25519, key: publicKey.key.subarray(0, 31) }), 'an ed25519 key a byte short');
    refused(encode({ suite: SIGNING_ML_DSA_65, key: new Uint8Array(1951) }), 'an ml-dsa-65 key a byte short');
    refused(encode({ suite: SIGNING_ED25519, key: NOT_A_POINT }), 'an ed25519 key that is not a point of the curve');
    refused(encode({ suite: SIGNING_ED25519, key: NEUTRAL }), 'the neutral point');
    refused(encode({ suite: SIGNING_ED25519, key: ORDER_TWO }), 'the point of order two');
    const { publicKey: pq } = await createIdentity(SIGNING_ML_DSA_65, sha256);
    const hybrid = new Uint8Array(32 + 1952);
    hybrid.set(NEUTRAL, 0);
    hybrid.set(pq.key, 32);
    refused(encode({ suite: SIGNING_ED25519_ML_DSA_65, key: hybrid }), 'a hybrid key with a small-order classical half');
    const reason = refused(pk.replace(/=+$/, ''), 'base64 without its padding');
    testing.assertTrue(reason.includes('canonical'), `which isn't canonical: ${reason}`);
}

export const keySuite = {
    title: '[KEY] validateKey',
    tests: [
        { name: '[KEY01] a public key of each registered suite is valid, and any 1952 bytes are an ml-dsa-65 key', invoke: testKeysOfEachSuite },
        { name: "[KEY02] text that isn't a well-formed public key is refused with its reason: the empty string, a key hash, an unregistered suite, a wrong length, an ed25519 key off the curve or of small order, a hybrid with a bad half, and base64 that isn't canonical", invoke: testRefusals },
    ],
};

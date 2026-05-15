import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { HASH_SHA256, createBasicCrypto, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";

import {
    signPayload, verifyPayloadSignature, extractAuthor, isAuthoredPayload,
} from "../src/authorship.js";
import type { KeyLookup } from "../src/authorship.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function makeKeyLookup(identities: { keyId: string; publicKey: { suite: string; key: Uint8Array } }[]): KeyLookup {
    return async (keyId: string) => {
        const found = identities.find(i => i.keyId === keyId);
        return found?.publicKey;
    };
}

export const authorshipTests = {
    title: '[AUTH] Authorship helper tests',
    tests: [
        {
            name: '[AUTH01] Sign and verify a payload',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;

                const signed = await signPayload(payload, alice);

                assertTrue(typeof signed.signature === 'string' && signed.signature.length > 0, 'signature should be present');
                assertTrue(signed.author === alice.keyId, 'author should match');

                const lookup = makeKeyLookup([alice]);
                const valid = await verifyPayloadSignature(signed, lookup);
                assertTrue(valid, 'signature should verify');
            }
        },
        {
            name: '[AUTH02] Verify fails with wrong key',
            invoke: async () => {
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;

                const signed = await signPayload(payload, alice);

                const lookup = makeKeyLookup([{ keyId: alice.keyId, publicKey: bob.publicKey }]);
                const valid = await verifyPayloadSignature(signed, lookup);
                assertFalse(valid, 'signature should fail with wrong key');
            }
        },
        {
            name: '[AUTH03] Verify fails with tampered payload',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;

                const signed = await signPayload(payload, alice);
                const tampered = { ...signed, data: 'modified' };

                const lookup = makeKeyLookup([alice]);
                const valid = await verifyPayloadSignature(tampered, lookup);
                assertFalse(valid, 'tampered payload should fail verification');
            }
        },
        {
            name: '[AUTH04] Verify fails when key lookup returns undefined',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;

                const signed = await signPayload(payload, alice);

                const emptyLookup: KeyLookup = async () => undefined;
                const valid = await verifyPayloadSignature(signed, emptyLookup);
                assertFalse(valid, 'should fail when key not found');
            }
        },
        {
            name: '[AUTH05] extractAuthor returns correct KeyId',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;

                const signed = await signPayload(payload, alice);

                const author = extractAuthor(signed);
                assertTrue(author === alice.keyId, 'extracted author should match alice keyId');

                const noAuthor = extractAuthor('not an object');
                assertTrue(noAuthor === undefined, 'should return undefined for non-object');
            }
        },
        {
            name: '[AUTH06] isAuthoredPayload type guard',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;

                const signed = await signPayload(payload, alice);

                assertTrue(isAuthoredPayload(signed), 'signed payload should pass type guard');
                assertFalse(isAuthoredPayload(payload), 'unsigned payload should not pass type guard');
                assertFalse(isAuthoredPayload('string'), 'string should not pass type guard');
                assertFalse(isAuthoredPayload([1, 2, 3]), 'array should not pass type guard');
            }
        },
    ]
};

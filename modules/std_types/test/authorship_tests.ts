import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { HASH_SHA256, createBasicCrypto, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version } from "@hyper-hyper-space/hhs3_mvt";

import {
    signPayload, verifyPayloadSignature, extractAuthor, isAuthoredPayload,
} from "../src/authorship.js";
import type { KeyLookup } from "../src/authorship.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

const AT = version('entry-a', 'entry-b');

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

                const signed = await signPayload(payload, alice, AT);

                assertTrue(typeof signed.signature === 'string' && signed.signature.length > 0, 'signature should be present');
                assertTrue(signed.author === alice.keyId, 'author should match');

                const lookup = makeKeyLookup([alice]);
                const valid = await verifyPayloadSignature(signed, AT, lookup);
                assertTrue(valid, 'signature should verify');
            }
        },
        {
            name: '[AUTH02] Verify fails with wrong key',
            invoke: async () => {
                const alice = await makeIdentity();
                const bob = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;

                const signed = await signPayload(payload, alice, AT);

                const lookup = makeKeyLookup([{ keyId: alice.keyId, publicKey: bob.publicKey }]);
                const valid = await verifyPayloadSignature(signed, AT, lookup);
                assertFalse(valid, 'signature should fail with wrong key');
            }
        },
        {
            name: '[AUTH03] Verify fails with tampered payload',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;

                const signed = await signPayload(payload, alice, AT);
                const tampered = { ...signed, data: 'modified' };

                const lookup = makeKeyLookup([alice]);
                const valid = await verifyPayloadSignature(tampered, AT, lookup);
                assertFalse(valid, 'tampered payload should fail verification');
            }
        },
        {
            name: '[AUTH04] Verify fails when key lookup returns undefined',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;

                const signed = await signPayload(payload, alice, AT);

                const emptyLookup: KeyLookup = async () => undefined;
                const valid = await verifyPayloadSignature(signed, AT, emptyLookup);
                assertFalse(valid, 'should fail when key not found');
            }
        },
        {
            name: '[AUTH05] extractAuthor returns correct KeyId',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;

                const signed = await signPayload(payload, alice, AT);

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

                const signed = await signPayload(payload, alice, AT);

                assertTrue(isAuthoredPayload(signed), 'signed payload should pass type guard');
                assertFalse(isAuthoredPayload(payload), 'unsigned payload should not pass type guard');
                assertFalse(isAuthoredPayload('string'), 'string should not pass type guard');
                assertFalse(isAuthoredPayload([1, 2, 3]), 'array should not pass type guard');
            }
        },
        {
            name: '[AUTH07] Verify fails at a different insertion point',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;
                const lookup = makeKeyLookup([alice]);

                const signed = await signPayload(payload, alice, AT);

                assertFalse(await verifyPayloadSignature(signed, version('entry-c'), lookup),
                    'a disjoint position should not verify');
                assertFalse(await verifyPayloadSignature(signed, version('entry-a'), lookup),
                    'a subset of the signed position should not verify');
                assertFalse(await verifyPayloadSignature(signed, version('entry-a', 'entry-b', 'entry-c'), lookup),
                    'a superset of the signed position should not verify');
            }
        },
        {
            name: '[AUTH08] Insertion point is order-insensitive',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;
                const lookup = makeKeyLookup([alice]);

                const signed = await signPayload(payload, alice, version('entry-b', 'entry-a'));

                assertTrue(await verifyPayloadSignature(signed, version('entry-a', 'entry-b'), lookup),
                    'the same position built in another order should verify');
            }
        },
        {
            name: '[AUTH09] Empty insertion point is rejected',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;
                const lookup = makeKeyLookup([alice]);

                let threw = false;
                try {
                    await signPayload(payload, alice, version());
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'signing at an empty position should throw');

                const signed = await signPayload(payload, alice, AT);
                assertFalse(await verifyPayloadSignature(signed, version(), lookup),
                    'verifying at an empty position should fail');
            }
        },
        {
            name: '[AUTH10] Empty signing scope matches an omitted scope',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;
                const lookup = makeKeyLookup([alice]);

                const unscoped = await signPayload(payload, alice, AT);
                const emptyScope = await signPayload(payload, alice, AT, []);

                assertTrue(unscoped.signature === emptyScope.signature,
                    'an empty scope should produce the exact same signature as no scope');
                assertTrue(await verifyPayloadSignature(unscoped, AT, lookup, []),
                    'an unscoped signature should verify with an empty scope');
                assertTrue(await verifyPayloadSignature(emptyScope, AT, lookup),
                    'an empty-scope signature should verify with no scope');
            }
        },
        {
            name: '[AUTH11] Verify fails under a different signing scope',
            invoke: async () => {
                const alice = await makeIdentity();
                const payload = { action: 'test', data: 'hello' } as json.LiteralMap;
                const lookup = makeKeyLookup([alice]);

                const signed = await signPayload(payload, alice, AT, [{ table: 'a' }]);

                assertTrue(await verifyPayloadSignature(signed, AT, lookup, [{ table: 'a' }]),
                    'the signing scope should verify');
                assertFalse(await verifyPayloadSignature(signed, AT, lookup, []),
                    'the root scope should not verify');
                assertFalse(await verifyPayloadSignature(signed, AT, lookup, [{ table: 'b' }]),
                    'a sibling scope should not verify');
                assertFalse(await verifyPayloadSignature(signed, AT, lookup, [{ table: 'a' }, { elmt: 'x' }]),
                    'a deeper scope should not verify');
            }
        },
    ]
};

import { testing } from '@hyper-hyper-space/hhs3_util';

import { ed25519, mlDsa65, ed25519_mlDsa65, SigningSuite } from '../signing.js';
import { x25519Hkdf, mlKem768, x25519Hkdf_mlKem768, KemSuite } from '../kem.js';
import { chacha20Poly1305 } from '../aead.js';
import { hkdfSha256 } from '../hkdf.js';
import { serializePublicKey, deserializePublicKey, keyIdFromPublicKey, PublicKey } from '../identity.js';
import { sha256, blake3 } from '../hashing.js';
import { random } from '../index.js';
import { getSigningSuite, getKemSuite, getAeadSuite, getKdfSuite, getHashSuite, createBasicCrypto } from '../registry.js';
import { ed25519 as nobleEd25519 } from '@noble/curves/ed25519.js';

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) return false;
    }
    return true;
}

function tamperByte(buf: Uint8Array, pos: number): Uint8Array {
    const copy = new Uint8Array(buf);
    copy[pos] ^= 0xff;
    return copy;
}

// ---- Signing round-trip tests ----

async function testSigningRoundTrip(suite: SigningSuite) {
    const { publicKey, secretKey } = await suite.generateKeyPair();

    testing.assertEquals(publicKey.length, suite.publicKeySize, `${suite.name} publicKey size`);
    testing.assertEquals(secretKey.length, suite.secretKeySize, `${suite.name} secretKey size`);

    const message = new TextEncoder().encode('hello hhs3');
    const signature = await suite.sign(message, secretKey);
    testing.assertEquals(signature.length, suite.signatureSize, `${suite.name} signature size`);

    const valid = await suite.verify(message, signature, publicKey);
    testing.assertTrue(valid, `${suite.name} valid signature should verify`);

    const badMsg = new TextEncoder().encode('tampered');
    const invalid = await suite.verify(badMsg, signature, publicKey);
    testing.assertFalse(invalid, `${suite.name} wrong message should not verify`);

    const badSig = tamperByte(signature, 0);
    const invalid2 = await suite.verify(message, badSig, publicKey);
    testing.assertFalse(invalid2, `${suite.name} tampered signature should not verify`);
}

// ---- KEM round-trip tests ----

async function testKemRoundTrip(suite: KemSuite) {
    const { publicKey, secretKey } = await suite.generateKeyPair();

    testing.assertEquals(publicKey.length, suite.publicKeySize, `${suite.name} publicKey size`);
    testing.assertEquals(secretKey.length, suite.secretKeySize, `${suite.name} secretKey size`);

    const { ciphertext, sharedSecret } = await suite.encapsulate(publicKey);
    testing.assertEquals(ciphertext.length, suite.ciphertextSize, `${suite.name} ciphertext size`);
    testing.assertEquals(sharedSecret.length, suite.sharedSecretSize, `${suite.name} sharedSecret size`);

    const decapsulated = await suite.decapsulate(ciphertext, secretKey);
    testing.assertTrue(bytesEqual(sharedSecret, decapsulated), `${suite.name} decapsulated secret should match`);
}

// ---- AEAD round-trip test ----

async function testAeadRoundTrip() {
    const key = random.getBytes(chacha20Poly1305.keySize);
    const nonce = random.getBytes(chacha20Poly1305.nonceSize);
    const plaintext = new TextEncoder().encode('confidential payload for hhs3');
    const aad = new TextEncoder().encode('associated data');

    const ciphertext = chacha20Poly1305.encrypt(plaintext, key, nonce, aad);
    testing.assertTrue(ciphertext.length === plaintext.length + chacha20Poly1305.tagSize, 'ciphertext length = plaintext + tag');

    const decrypted = chacha20Poly1305.decrypt(ciphertext, key, nonce, aad);
    testing.assertTrue(bytesEqual(plaintext, decrypted), 'decrypted should match plaintext');

    let caught = false;
    try {
        const badCt = tamperByte(ciphertext, 0);
        chacha20Poly1305.decrypt(badCt, key, nonce, aad);
    } catch {
        caught = true;
    }
    testing.assertTrue(caught, 'tampered ciphertext should fail decryption');

    let caught2 = false;
    try {
        const wrongAad = new TextEncoder().encode('wrong aad');
        chacha20Poly1305.decrypt(ciphertext, key, nonce, wrongAad);
    } catch {
        caught2 = true;
    }
    testing.assertTrue(caught2, 'wrong AAD should fail decryption');
}

// ---- HKDF determinism test ----

async function testHkdfDeterminism() {
    const ikm = new TextEncoder().encode('input keying material');
    const salt = new TextEncoder().encode('salt value');
    const info = new TextEncoder().encode('context info');

    const key1 = hkdfSha256.deriveKey(ikm, salt, info, 32);
    const key2 = hkdfSha256.deriveKey(ikm, salt, info, 32);
    testing.assertTrue(bytesEqual(key1, key2), 'HKDF should be deterministic');

    const key3 = hkdfSha256.deriveKey(ikm, salt, info, 64);
    testing.assertEquals(key3.length, 64, 'HKDF should produce requested length');

    const prk = hkdfSha256.extract(ikm, salt);
    const expanded = hkdfSha256.expand(prk, info, 32);
    testing.assertTrue(bytesEqual(key1, expanded), 'extract+expand should equal deriveKey');
}

// ---- Hybrid signing component tampering ----

async function testHybridSigningTampering() {
    const { publicKey, secretKey } = await ed25519_mlDsa65.generateKeyPair();
    const message = new TextEncoder().encode('hybrid test');
    const signature = await ed25519_mlDsa65.sign(message, secretKey);

    const valid = await ed25519_mlDsa65.verify(message, signature, publicKey);
    testing.assertTrue(valid, 'hybrid signature should verify');

    const badClassicalSig = tamperByte(signature, 0);
    const r1 = await ed25519_mlDsa65.verify(message, badClassicalSig, publicKey);
    testing.assertFalse(r1, 'tampered classical component should fail');

    const pqOffset = ed25519.signatureSize;
    const badPqSig = tamperByte(signature, pqOffset);
    const r2 = await ed25519_mlDsa65.verify(message, badPqSig, publicKey);
    testing.assertFalse(r2, 'tampered PQ component should fail');
}

// ---- Hybrid KEM component tampering ----

async function testHybridKemTampering() {
    const { publicKey, secretKey } = await x25519Hkdf_mlKem768.generateKeyPair();
    const { ciphertext, sharedSecret } = await x25519Hkdf_mlKem768.encapsulate(publicKey);

    const decapsulated = await x25519Hkdf_mlKem768.decapsulate(ciphertext, secretKey);
    testing.assertTrue(bytesEqual(sharedSecret, decapsulated), 'hybrid KEM round-trip');

    const badClassicalCt = tamperByte(ciphertext, 0);
    const ss1 = await x25519Hkdf_mlKem768.decapsulate(badClassicalCt, secretKey);
    testing.assertFalse(bytesEqual(sharedSecret, ss1), 'tampered classical ciphertext should produce different secret');

    const pqOffset = x25519Hkdf.ciphertextSize;
    const badPqCt = tamperByte(ciphertext, pqOffset);
    let pqFailed = false;
    try {
        const ss2 = await x25519Hkdf_mlKem768.decapsulate(badPqCt, secretKey);
        pqFailed = !bytesEqual(sharedSecret, ss2);
    } catch {
        pqFailed = true;
    }
    testing.assertTrue(pqFailed, 'tampered PQ ciphertext should fail or produce different secret');
}

// ---- Identity round-trip test ----

async function testIdentityRoundTrip() {
    const { publicKey: rawKey } = await ed25519.generateKeyPair();
    const pk: PublicKey = { suite: 'ed25519', key: rawKey };

    const serialized = serializePublicKey(pk);
    const deserialized = deserializePublicKey(serialized);

    testing.assertEquals(deserialized.suite, pk.suite, 'suite should round-trip');
    testing.assertTrue(bytesEqual(deserialized.key, pk.key), 'key bytes should round-trip');

    const keyId1 = keyIdFromPublicKey(pk, sha256);
    const keyId2 = keyIdFromPublicKey(pk, sha256);
    testing.assertEquals(keyId1, keyId2, 'same PublicKey should produce same KeyId');

    const { publicKey: rawKey2 } = await ed25519.generateKeyPair();
    const pk2: PublicKey = { suite: 'ed25519', key: rawKey2 };
    const keyId3 = keyIdFromPublicKey(pk2, sha256);
    testing.assertTrue(keyId1 !== keyId3, 'different keys should produce different KeyIds');
}

async function testIdentitySuiteDistinction() {
    const keyBytes = random.getBytes(32);
    const pk1: PublicKey = { suite: 'ed25519', key: keyBytes };
    const pk2: PublicKey = { suite: 'ml-dsa-65', key: keyBytes };

    const id1 = keyIdFromPublicKey(pk1, sha256);
    const id2 = keyIdFromPublicKey(pk2, sha256);
    testing.assertTrue(id1 !== id2, 'same key bytes with different suites should produce different KeyIds');
}

// ---- Fixed test vectors for cross-language interop ----

async function testEd25519FixedVector() {
    // RFC 8032 Test Vector 1 (empty message)
    const secretKey = new Uint8Array([
        0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60,
        0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c, 0xc4,
        0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19,
        0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae, 0x7f, 0x60,
    ]);
    const expectedPk = new Uint8Array([
        0xd7, 0x5a, 0x98, 0x01, 0x82, 0xb1, 0x0a, 0xb7,
        0xd5, 0x4b, 0xfe, 0xd3, 0xc9, 0x64, 0x07, 0x3a,
        0x0e, 0xe1, 0x72, 0xf3, 0xda, 0xa6, 0x23, 0x25,
        0xaf, 0x02, 0x1a, 0x68, 0xf7, 0x07, 0x51, 0x1a,
    ]);
    const expectedSig = new Uint8Array([
        0xe5, 0x56, 0x43, 0x00, 0xc3, 0x60, 0xac, 0x72,
        0x90, 0x86, 0xe2, 0xcc, 0x80, 0x6e, 0x82, 0x8a,
        0x84, 0x87, 0x7f, 0x1e, 0xb8, 0xe5, 0xd9, 0x74,
        0xd8, 0x73, 0xe0, 0x65, 0x22, 0x49, 0x01, 0x55,
        0x5f, 0xb8, 0x82, 0x15, 0x90, 0xa3, 0x3b, 0xac,
        0xc6, 0x1e, 0x39, 0x70, 0x1c, 0xf9, 0xb4, 0x6b,
        0xd2, 0x5b, 0xf5, 0xf0, 0x59, 0x5b, 0xbe, 0x24,
        0x65, 0x51, 0x41, 0x43, 0x8e, 0x7a, 0x10, 0x0b,
    ]);

    const publicKey = nobleEd25519.getPublicKey(secretKey);

    testing.assertTrue(bytesEqual(publicKey, expectedPk), 'Ed25519 public key matches RFC 8032 vector');

    const message = new Uint8Array(0);
    const sig = await ed25519.sign(message, secretKey);
    testing.assertTrue(bytesEqual(sig, expectedSig), 'Ed25519 signature matches RFC 8032 vector');

    const valid = await ed25519.verify(message, sig, publicKey);
    testing.assertTrue(valid, 'Ed25519 RFC 8032 vector verifies');
}

// ---- HashSuite tests ----

async function testHashSuiteRoundTrip() {
    const input = new TextEncoder().encode('hello hhs3');
    const h1 = sha256.hash(input);
    const h2 = sha256.hash(input);
    testing.assertEquals(h1, h2, 'SHA-256 should be deterministic');
    testing.assertTrue(h1.length > 0, 'SHA-256 hash should be non-empty');

    const different = sha256.hash(new TextEncoder().encode('different'));
    testing.assertTrue(h1 !== different, 'different inputs should produce different hashes');
}

async function testBlake3RoundTrip() {
    const input = new TextEncoder().encode('hello hhs3');
    const h1 = blake3.hash(input);
    const h2 = blake3.hash(input);
    testing.assertEquals(h1, h2, 'BLAKE3 should be deterministic');
    testing.assertEquals(blake3.digestSize, 32, 'BLAKE3 digest size should be 32');

    const different = blake3.hash(new TextEncoder().encode('different'));
    testing.assertTrue(h1 !== different, 'different inputs should produce different hashes');

    testing.assertTrue(h1 !== sha256.hash(input), 'BLAKE3 and SHA-256 should produce different hashes');
}

// ---- Registry tests ----

async function testRegistryLookups() {
    const s = getSigningSuite('ed25519');
    testing.assertTrue(s !== undefined, 'ed25519 signing suite should be in registry');
    testing.assertEquals(s!.name, 'ed25519', 'registry should return correct suite');

    const s2 = getSigningSuite('ml-dsa-65');
    testing.assertTrue(s2 !== undefined, 'ml-dsa-65 should be in registry');

    const s3 = getSigningSuite('ed25519+ml-dsa-65');
    testing.assertTrue(s3 !== undefined, 'hybrid signing should be in registry');

    const k = getKemSuite('x25519-hkdf');
    testing.assertTrue(k !== undefined, 'x25519-hkdf should be in registry');

    const k2 = getKemSuite('ml-kem-768');
    testing.assertTrue(k2 !== undefined, 'ml-kem-768 should be in registry');

    const a = getAeadSuite('chacha20-poly1305');
    testing.assertTrue(a !== undefined, 'chacha20-poly1305 should be in registry');

    const kdf = getKdfSuite('hkdf-sha256');
    testing.assertTrue(kdf !== undefined, 'hkdf-sha256 should be in registry');

    const h = getHashSuite('sha-256');
    testing.assertTrue(h !== undefined, 'sha-256 should be in registry');
    testing.assertEquals(h!.digestSize, 32, 'sha-256 digest size should be 32');

    const h2 = getHashSuite('blake3');
    testing.assertTrue(h2 !== undefined, 'blake3 should be in registry');
    testing.assertEquals(h2!.digestSize, 32, 'blake3 digest size should be 32');

    const missing = getSigningSuite('nonexistent');
    testing.assertTrue(missing === undefined, 'unknown suite should return undefined');
}

// ---- BasicCrypto typed facade tests ----

async function testBasicCrypto() {
    const c = createBasicCrypto();

    const h = c.hash('sha-256');
    testing.assertEquals(h.name, 'sha-256', 'BasicCrypto hash should return sha-256');
    testing.assertEquals(h.digestSize, 32, 'sha-256 digest size via BasicCrypto');

    const s = c.signing('ed25519');
    testing.assertEquals(s.name, 'ed25519', 'BasicCrypto signing should return ed25519');

    const s2 = c.signing('ed25519+ml-dsa-65');
    testing.assertEquals(s2.name, 'ed25519+ml-dsa-65', 'BasicCrypto hybrid signing');

    const k = c.kem('x25519-hkdf');
    testing.assertEquals(k.name, 'x25519-hkdf', 'BasicCrypto kem should return x25519-hkdf');

    const a = c.aead('chacha20-poly1305');
    testing.assertEquals(a.name, 'chacha20-poly1305', 'BasicCrypto aead');

    const kdf = c.kdf('hkdf-sha256');
    testing.assertEquals(kdf.name, 'hkdf-sha256', 'BasicCrypto kdf');
}

// ---- Main ----

const signingTests = {
    title: 'Signing suites',
    tests: [
        { name: 'Ed25519 round-trip', invoke: () => testSigningRoundTrip(ed25519) },
        { name: 'ML-DSA-65 round-trip', invoke: () => testSigningRoundTrip(mlDsa65) },
        { name: 'Hybrid Ed25519+ML-DSA-65 round-trip', invoke: () => testSigningRoundTrip(ed25519_mlDsa65) },
        { name: 'Hybrid signing component tampering', invoke: testHybridSigningTampering },
    ]
};

const kemTests = {
    title: 'KEM suites',
    tests: [
        { name: 'X25519-HKDF round-trip', invoke: () => testKemRoundTrip(x25519Hkdf) },
        { name: 'ML-KEM-768 round-trip', invoke: () => testKemRoundTrip(mlKem768) },
        { name: 'Hybrid X25519+ML-KEM-768 round-trip', invoke: () => testKemRoundTrip(x25519Hkdf_mlKem768) },
        { name: 'Hybrid KEM component tampering', invoke: testHybridKemTampering },
    ]
};

const aeadTests = {
    title: 'AEAD',
    tests: [
        { name: 'ChaCha20-Poly1305 round-trip', invoke: testAeadRoundTrip },
    ]
};

const hkdfTests = {
    title: 'HKDF',
    tests: [
        { name: 'HKDF-SHA256 determinism', invoke: testHkdfDeterminism },
    ]
};

const identityTests = {
    title: 'Identity',
    tests: [
        { name: 'PublicKey serialize/deserialize round-trip', invoke: testIdentityRoundTrip },
        { name: 'Suite name affects KeyId', invoke: testIdentitySuiteDistinction },
    ]
};

const hashTests = {
    title: 'Hashing',
    tests: [
        { name: 'SHA-256 round-trip', invoke: testHashSuiteRoundTrip },
        { name: 'BLAKE3 round-trip', invoke: testBlake3RoundTrip },
    ]
};

const registryTests = {
    title: 'Registry',
    tests: [
        { name: 'Registry lookups', invoke: testRegistryLookups },
        { name: 'BasicCrypto typed facade', invoke: testBasicCrypto },
    ]
};

const vectorTests = {
    title: 'Fixed test vectors',
    tests: [
        { name: 'Ed25519 RFC 8032 vector', invoke: testEd25519FixedVector },
    ]
};

const allSuites = [signingTests, kemTests, aeadTests, hkdfTests, hashTests, identityTests, registryTests, vectorTests];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for HHSv3 crypto module' + (filters.length > 0 ? ` (filter: ${filters})` : '') + '\n');

    for (const suite of allSuites) {
        console.log(suite.title);
        for (const test of suite.tests) {
            let match = true;
            for (const filter of filters) {
                match = match && test.name.indexOf(filter) >= 0;
            }
            if (match) {
                const result = await testing.run(test.name, test.invoke);
                if (!result) return;
            } else {
                await testing.skip(test.name);
            }
        }
        console.log();
    }
}

main();

// Suite registry. All supported cryptographic algorithms are registered here at
// module load. Provides both untyped lookups (for deserialization / wire-protocol
// negotiation where names arrive as strings) and BasicCrypto, a typed facade
// with compile-time-checked algorithm names for use by application code.

import { SigningSuite, ed25519, mlDsa65, ed25519_mlDsa65 } from './signing.js';
import { KemSuite, x25519Hkdf, mlKem768, x25519Hkdf_mlKem768 } from './kem.js';
import { AeadSuite, chacha20Poly1305 } from './aead.js';
import { KdfSuite, hkdfSha256 } from './hkdf.js';
import { HashSuite, sha256, blake3 } from './hashing.js';

// Supported algorithm name types (compile-time checked)

export type HashName    = 'sha-256' | 'blake3';
export type SigningName = 'ed25519' | 'ml-dsa-65' | 'ed25519+ml-dsa-65';
export type KemName     = 'x25519-hkdf' | 'ml-kem-768' | 'x25519-hkdf+ml-kem-768';
export type AeadName    = 'chacha20-poly1305';
export type KdfName     = 'hkdf-sha256';

// Registry maps

const signingSuites = new Map<string, SigningSuite>();
const kemSuites     = new Map<string, KemSuite>();
const aeadSuites    = new Map<string, AeadSuite>();
const kdfSuites     = new Map<string, KdfSuite>();
const hashSuites    = new Map<string, HashSuite>();

function registerSigning(s: SigningSuite)  { signingSuites.set(s.name, s); }
function registerKem(s: KemSuite)          { kemSuites.set(s.name, s); }
function registerAead(s: AeadSuite)        { aeadSuites.set(s.name, s); }
function registerKdf(s: KdfSuite)          { kdfSuites.set(s.name, s); }
function registerHash(s: HashSuite)        { hashSuites.set(s.name, s); }

registerSigning(ed25519);
registerSigning(mlDsa65);
registerSigning(ed25519_mlDsa65);

registerKem(x25519Hkdf);
registerKem(mlKem768);
registerKem(x25519Hkdf_mlKem768);

registerAead(chacha20Poly1305);

registerKdf(hkdfSha256);

registerHash(sha256);
registerHash(blake3);

// Untyped lookups for deserialization / wire-protocol negotiation

export function getSigningSuite(name: string): SigningSuite | undefined { return signingSuites.get(name); }
export function getKemSuite(name: string):     KemSuite     | undefined { return kemSuites.get(name); }
export function getAeadSuite(name: string):    AeadSuite    | undefined { return aeadSuites.get(name); }
export function getKdfSuite(name: string):     KdfSuite     | undefined { return kdfSuites.get(name); }
export function getHashSuite(name: string):    HashSuite    | undefined { return hashSuites.get(name); }

// Typed facade: compile-time-safe access to registered suites

export type BasicCrypto = {
    hash:    (name: HashName)    => HashSuite;
    signing: (name: SigningName) => SigningSuite;
    kem:     (name: KemName)     => KemSuite;
    aead:    (name: AeadName)    => AeadSuite;
    kdf:     (name: KdfName)     => KdfSuite;
};

function requireSuite<T>(map: Map<string, T>, category: string, name: string): T {
    const suite = map.get(name);
    if (suite === undefined) throw new Error(`${category} suite '${name}' not registered`);
    return suite;
}

export function createBasicCrypto(): BasicCrypto {
    return {
        hash:    (name) => requireSuite(hashSuites,    'hash',    name),
        signing: (name) => requireSuite(signingSuites, 'signing', name),
        kem:     (name) => requireSuite(kemSuites,     'kem',     name),
        aead:    (name) => requireSuite(aeadSuites,    'aead',    name),
        kdf:     (name) => requireSuite(kdfSuites,     'kdf',     name),
    };
}

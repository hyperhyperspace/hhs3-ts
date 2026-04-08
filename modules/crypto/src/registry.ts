// Suite registry. All supported cryptographic algorithms are registered here at
// module load. Provides both untyped lookups (for deserialization / wire-protocol
// negotiation where names arrive as strings) and BasicCrypto, a typed facade
// with compile-time-checked algorithm names for use by application code.

import { SigningSuite, ed25519, mlDsa65, ed25519_mlDsa65 } from './signing.js';
import { KemSuite, x25519Hkdf, mlKem768, x25519Hkdf_mlKem768 } from './kem.js';
import { AeadSuite, chacha20Poly1305 } from './aead.js';
import { KdfSuite, hkdfSha256 } from './hkdf.js';
import { HashSuite, sha256, blake3 } from './hashing.js';

// Algorithm name constants (single source of truth for wire labels)

export const HASH_SHA256                = 'sha-256'                  as const;
export const HASH_BLAKE3                = 'blake3'                   as const;

export const SIGNING_ED25519            = 'ed25519'                  as const;
export const SIGNING_ML_DSA_65          = 'ml-dsa-65'                as const;
export const SIGNING_ED25519_ML_DSA_65  = 'ed25519+ml-dsa-65'        as const;

export const KEM_X25519_HKDF            = 'x25519-hkdf'              as const;
export const KEM_ML_KEM_768             = 'ml-kem-768'               as const;
export const KEM_X25519_HKDF_ML_KEM_768 = 'x25519-hkdf+ml-kem-768'  as const;

export const AEAD_CHACHA20_POLY1305     = 'chacha20-poly1305'        as const;

export const KDF_HKDF_SHA256            = 'hkdf-sha256'              as const;

// Union types derived from the constants

export type HashName    = typeof HASH_SHA256 | typeof HASH_BLAKE3;
export type SigningName = typeof SIGNING_ED25519 | typeof SIGNING_ML_DSA_65 | typeof SIGNING_ED25519_ML_DSA_65;
export type KemName     = typeof KEM_X25519_HKDF | typeof KEM_ML_KEM_768 | typeof KEM_X25519_HKDF_ML_KEM_768;
export type AeadName    = typeof AEAD_CHACHA20_POLY1305;
export type KdfName     = typeof KDF_HKDF_SHA256;

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

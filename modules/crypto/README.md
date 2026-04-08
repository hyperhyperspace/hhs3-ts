# Crypto

Cryptographic primitives for Hyper Hyper Space v3. This module consolidates all cryptographic operations into a single place — hashing, signing, key encapsulation, authenticated encryption, key derivation, and identity management — with classical, hybrid and post-quantum algorithm options. All implementations are backed by the audited [`@noble`](https://paulmillr.com/noble/) library family and follow FIPS and RFC standards for cross-language interoperability.

## Design

Every category of cryptographic operation is modeled as a **suite interface** with a `name` property and uniform method signatures. Concrete implementations are plain objects that satisfy the interface. A **registry** maps algorithm names to suite instances, enabling both typed compile-time access and untyped runtime lookups (for deserialization and wire-protocol negotiation).

```
┌───────────────┐
│  BasicCrypto  │  ← typed facade, compile-time checked algorithm names
└───────┬───────┘
        │
  ┌─────┴──────┐
  │  Registry   │  ← maps name strings → suite instances
  └─────┬──────┘
        │
 ┌──────┼──────────┬──────────┬────────┬───────┐
 │      │          │          │        │       │
Hash  Signing    KEM       AEAD     KDF   Identity
```

## Algorithm suites

### Hashing (`HashSuite`)

| Constant | Name | Digest | Source |
|----------|------|--------|--------|
| `HASH_SHA256` | `sha-256` | 32 bytes | FIPS 180-4 |
| `HASH_BLAKE3` | `blake3` | 32 bytes | [BLAKE3 spec](https://github.com/BLAKE3-team/BLAKE3-specs) |

```typescript
interface HashSuite {
    readonly name: string;
    readonly digestSize: number;
    hash(input: Uint8Array): Uint8Array;          // raw bytes
    hashToB64(input: Uint8Array): B64Hash;         // base64-encoded string
}
```

Used for DAG entry hashing, element identity in replicated data types, and `KeyId` derivation.

### Signing (`SigningSuite`)

| Constant | Name | PK | SK | Sig | Standard |
|----------|------|----|----|-----|----------|
| `SIGNING_ED25519` | `ed25519` | 32 B | 32 B | 64 B | RFC 8032 |
| `SIGNING_ML_DSA_65` | `ml-dsa-65` | 1952 B | 4032 B | 3309 B | FIPS 204 |
| `SIGNING_ED25519_ML_DSA_65` | `ed25519+ml-dsa-65` | 1984 B | 4064 B | 3373 B | Hybrid (both must verify) |

```typescript
interface SigningSuite {
    readonly name: string;
    readonly publicKeySize: number;
    readonly secretKeySize: number;
    readonly signatureSize: number;
    generateKeyPair(): Promise<{ publicKey: Uint8Array; secretKey: Uint8Array }>;
    sign(message: Uint8Array, secretKey: Uint8Array): Promise<Uint8Array>;
    verify(message: Uint8Array, signature: Uint8Array, publicKey: Uint8Array): Promise<boolean>;
}
```

The hybrid suite concatenates classical and post-quantum keys/signatures and requires **both** to verify.

### Key Encapsulation (`KemSuite`)

| Constant | Name | PK | SK | CT | SS | Source |
|----------|------|----|----|----|----|--------|
| `KEM_X25519_HKDF` | `x25519-hkdf` | 32 B | 32 B | 32 B | 32 B | RFC 9180 DHKEM |
| `KEM_ML_KEM_768` | `ml-kem-768` | 1184 B | 2400 B | 1088 B | 32 B | FIPS 203 |
| `KEM_X25519_HKDF_ML_KEM_768` | `x25519-hkdf+ml-kem-768` | 1216 B | 2432 B | 1120 B | 32 B | Hybrid (HKDF combiner) |

```typescript
interface KemSuite {
    readonly name: string;
    readonly publicKeySize: number;
    readonly secretKeySize: number;
    readonly ciphertextSize: number;
    readonly sharedSecretSize: number;
    generateKeyPair(): Promise<{ publicKey: Uint8Array; secretKey: Uint8Array }>;
    encapsulate(publicKey: Uint8Array): Promise<{ ciphertext: Uint8Array; sharedSecret: Uint8Array }>;
    decapsulate(ciphertext: Uint8Array, secretKey: Uint8Array): Promise<Uint8Array>;
}
```

Used for session key negotiation in the mesh layer's Noise-like handshake.

### Authenticated Encryption (`AeadSuite`)

| Constant | Name | Key | Nonce | Tag |
|----------|------|-----|-------|-----|
| `AEAD_CHACHA20_POLY1305` | `chacha20-poly1305` | 32 B | 12 B | 16 B |

```typescript
interface AeadSuite {
    readonly name: string;
    readonly keySize: number;
    readonly nonceSize: number;
    readonly tagSize: number;
    encrypt(plaintext: Uint8Array, key: Uint8Array, nonce: Uint8Array, aad?: Uint8Array): Uint8Array;
    decrypt(ciphertext: Uint8Array, key: Uint8Array, nonce: Uint8Array, aad?: Uint8Array): Uint8Array;
}
```

### Key Derivation (`KdfSuite`)

| Constant | Name | Standard |
|----------|------|----------|
| `KDF_HKDF_SHA256` | `hkdf-sha256` | RFC 5869 |

```typescript
interface KdfSuite {
    readonly name: string;
    extract(ikm: Uint8Array, salt?: Uint8Array): Uint8Array;
    expand(prk: Uint8Array, info: Uint8Array, length: number): Uint8Array;
    deriveKey(ikm: Uint8Array, salt: Uint8Array, info: Uint8Array, length: number): Uint8Array;
}
```

## Identity

A `PublicKey` is self-describing key material — a suite name paired with raw bytes. A `KeyId` is a compact, suite-agnostic hash of a serialized `PublicKey`, used as a stable peer identifier.

```typescript
type PublicKey = { suite: string; key: Uint8Array };
type KeyId = B64Hash;

serializePublicKey(pk: PublicKey): Uint8Array;
deserializePublicKey(bytes: Uint8Array): PublicKey;
keyIdFromPublicKey(pk: PublicKey, hash: HashSuite): KeyId;
```

The serialization format is `[4-byte big-endian suite-name length][suite-name UTF-8][key bytes]`, designed for unambiguous cross-language parsing.

## Registry and `BasicCrypto`

Suites are registered by name at module load. Two access patterns are supported:

**Untyped lookups** — for deserialization or wire-protocol negotiation where algorithm names arrive as runtime strings:

```typescript
const suite = getSigningSuite('ed25519');       // SigningSuite | undefined
const hash  = getHashSuite('sha-256');          // HashSuite | undefined
```

**`BasicCrypto` typed facade** — for application code where algorithm choices are known at compile time:

```typescript
const crypto = createBasicCrypto();

const h = crypto.hash(HASH_SHA256);             // HashSuite (never undefined)
const s = crypto.signing(SIGNING_ED25519);      // SigningSuite
const k = crypto.kem(KEM_X25519_HKDF);          // KemSuite
```

The name constants (`HASH_SHA256`, `SIGNING_ED25519`, etc.) are the single source of truth for algorithm wire labels. The union types (`HashName`, `SigningName`, etc.) are derived from them.

## Utilities

- `random.getBytes(n)` — cryptographically secure random bytes
- `random.getInt(min, max)` — cryptographically secure random integer in range
- `base64.fromArrayBuffer(buf)` / `base64.toArrayBuffer(b64)` — base64 encoding
- `stringToUint8Array(str)` / `uint8ArrayToString(bytes)` — UTF-8 conversion

## Dependencies

All cryptographic implementations use the [`@noble`](https://paulmillr.com/noble/) library family:

- [`@noble/hashes`](https://github.com/paulmillr/noble-hashes) — SHA-256, BLAKE3, HKDF
- [`@noble/curves`](https://github.com/paulmillr/noble-curves) — Ed25519, X25519
- [`@noble/post-quantum`](https://github.com/paulmillr/noble-post-quantum) — ML-DSA-65, ML-KEM-768
- [`@noble/ciphers`](https://github.com/paulmillr/noble-ciphers) — ChaCha20-Poly1305

These are pure TypeScript, audited, and follow FIPS/RFC specifications, ensuring interoperability with Rust and other implementations.

## Building

To build, run the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```

## Testing

The test suite covers all suite interfaces, hybrid constructions (component tampering), identity serialization round-trips, registry lookups, and an RFC 8032 Ed25519 test vector. To run it, first build the workspace and then within `modules/crypto`:

```
npm run test
```

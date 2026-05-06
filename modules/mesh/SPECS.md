# Mesh Protocol Specification

Status: Initial Version v0.1

## 1. Transport Requirements

The mesh protocol runs over any bidirectional, reliable, message-oriented byte
channel. WebSocket is the reference transport. The protocol assumes:

- Messages are delivered in order and without corruption (the transport handles
  framing and integrity).
- Either side can close the channel at any time.

The mesh protocol does not define transport-level framing. A WebSocket message
boundary = one protocol message.

## 2. Authentication Handshake

A 3-message (1.5 round-trip) authenticated key exchange that establishes an
encrypted channel between two peers. The initiator's identity is protected:
it is never revealed unless the responder first proves possession of the
expected key.

### 2.1 Binary Field Encoding

Handshake messages use a length-prefixed field array encoding:

```
[4 bytes] field count (big-endian uint32)
for each field:
    [4 bytes] field length (big-endian uint32)
    [N bytes] field data
```

All multi-byte integers in the handshake are big-endian.

### 2.2 Public Key Serialization

A public key is serialized as:

```
[4 bytes] suite name length (big-endian uint32)
[N bytes] suite name (UTF-8)
[M bytes] raw key material
```

Suite names are the wire labels defined in Section 5 (e.g. `ed25519`,
`ed25519+ml-dsa-65`). A `KeyId` is `Base64(SHA-256(serialized_public_key))`.

### 2.3 Transcript

The handshake maintains a running transcript hash (SHA-256, raw bytes):

- `T1 = SHA-256(msg1_bytes)`
- `T2 = SHA-256(T1 || msg2_pre_sig_bytes)`
- `T3 = SHA-256(T2 || kem_ciphertext)`

Where `||` is byte concatenation. `msg2_pre_sig_bytes` is the fields encoding
of the first three fields of Msg2 (public key, chosen KEM, ephemeral KEM
public key) — the signature is computed over `T2` and is not part of the
transcript input.

### 2.4 Message Sequence

**Msg1** (initiator -> responder): anonymous, no identity revealed.

Fields:

| # | Contents | Format |
|---|----------|--------|
| 0 | Session nonce | 32 random bytes |
| 1 | KEM preferences | UTF-8 JSON array of KEM suite names, ordered by preference |

The initiator sends Msg1 as a single transport message encoded via the fields
format (Section 2.1).

**Msg2** (responder -> initiator): responder proves identity.

The responder parses Msg1, extracts the KEM preference list, and selects
the first entry that it also supports. If no common KEM exists, the
responder MUST close the transport.

The responder generates an ephemeral KEM keypair using the chosen suite.

Fields:

| # | Contents | Format |
|---|----------|--------|
| 0 | Responder public key | Serialized per Section 2.2 |
| 1 | Chosen KEM name | UTF-8 string |
| 2 | Ephemeral KEM public key | Raw bytes (suite-specific) |
| 3 | Transcript signature | `Sign(T2, responder_secret_key)` |

The signature is computed over `T2` using the responder's signing suite
(determined by the `suite` field in its public key).

**Msg3** (initiator -> responder): initiator reveals identity (encrypted).

The initiator verifies the responder's signature over `T2`. If
`expectedRemote` is set and the responder's `KeyId` does not match, the
initiator MUST close the transport without sending Msg3 (identity
protection).

The initiator encapsulates a shared secret using the ephemeral KEM public
key from Msg2, producing `(kem_ciphertext, shared_secret)`.

Session keys are derived via HKDF-SHA256:

```
i2r_key = HKDF(ikm=shared_secret, salt=T3, info="hhs3-noise-i2r", len=32)
r2i_key = HKDF(ikm=shared_secret, salt=T3, info="hhs3-noise-r2i", len=32)
```

The initiator constructs an identity payload (fields encoding of
`[serialized_public_key, Sign(T3, initiator_secret_key)]`), encrypts it
with ChaCha20-Poly1305 using `i2r_key` and an all-zero 12-byte nonce.

Fields:

| # | Contents | Format |
|---|----------|--------|
| 0 | KEM ciphertext | Raw bytes (suite-specific) |
| 1 | Encrypted identity | AEAD ciphertext of the identity payload |

The responder decapsulates the shared secret from field 0, derives the
same session keys, decrypts field 1 with `i2r_key` and the all-zero nonce,
deserializes the initiator's public key, and verifies its signature over
`T3`.

### 2.5 Handshake Completion

After Msg3, both sides hold:

- The remote peer's verified `PublicKey` and `KeyId`.
- Session keys `i2r_key` and `r2i_key`.

The channel transitions to encrypted mode (Section 3). The initiator uses
`i2r_key` to send and `r2i_key` to receive. The responder uses the
reverse.

### 2.6 Handshake Errors

If any of the following occurs, the peer MUST close the transport immediately:

- Malformed fields (wrong count, truncated data).
- Unknown or unsupported signing suite in the remote's public key.
- No common KEM suite.
- Signature verification failure.
- AEAD decryption failure (Msg3 identity).
- `expectedRemote` mismatch (initiator side, before sending Msg3).

Implementations SHOULD apply a timeout to the overall handshake. A
recommended value is 10 seconds.

## 3. Encrypted Channel

After the handshake, all traffic on the transport is encrypted with
ChaCha20-Poly1305. Each direction maintains an independent message counter
starting at zero.

### 3.1 Nonce Construction

The nonce is 12 bytes. Given a counter value `c`:

```
bytes  0..3:  0x00 0x00 0x00 0x00
bytes  4..7:  uint32 little-endian (c & 0xFFFFFFFF)
bytes  8..11: uint32 little-endian (floor(c / 2^32) & 0xFFFFFFFF)
```

The first four bytes are always zero. The counter occupies the remaining
eight bytes in little-endian order.

### 3.2 Send/Receive

To send: encrypt the plaintext with the send key and the nonce for the
current send counter, then increment the counter. Transmit the ciphertext
(which includes the 16-byte Poly1305 tag) as a single transport message.

To receive: decrypt the ciphertext with the receive key and the nonce for
the current receive counter, then increment the counter.

No associated data (AAD) is used.

### 3.3 Errors

If decryption fails (bad tag, wrong length, etc.), the peer MUST close the
channel. There is no recovery mechanism — a decryption failure indicates
either corruption or a desynchronized counter, both unrecoverable.

## 4. Multiplexing (Mux)

An encrypted channel carries messages for multiple topics. Each message is
wrapped in a lightweight frame before encryption.

### 4.1 Frame Format

**Topic data frame** (type `0x01`):

```
[1 byte]  0x01
[2 bytes] topic name length (big-endian uint16)
[N bytes] topic name (UTF-8)
[M bytes] payload
```

**Control frame** (type `0x02`):

```
[1 byte]  0x02
[N bytes] payload
```

### 4.2 Decoding Rules

- If the frame is empty (zero bytes), the receiver MUST drop it silently.
- If the type byte is `0x01`, parse the topic length and topic name. If
  the frame is too short to contain them, drop it silently.
- If the type byte is `0x02`, the rest of the frame is the control payload.
- If the type byte is anything else, drop the frame silently. This allows
  future protocol extensions without breaking existing peers.

## 5. Algorithm Suites

### 5.1 Fixed Algorithms

The following are not negotiated and MUST be supported by all implementations:

| Function | Algorithm | Parameters |
|----------|-----------|------------|
| Transcript hash | SHA-256 | 32-byte digest |
| Key derivation | HKDF-SHA256 | RFC 5869 |
| Session encryption | ChaCha20-Poly1305 | 32-byte key, 12-byte nonce, 16-byte tag |
| Key ID derivation | SHA-256 + Base64 | Of serialized public key |

### 5.2 Signing Suites

Each peer has a long-term signing key. The suite name is embedded in the
serialized public key and determines how signatures are computed and
verified.

| Wire name | Algorithm | Public key | Signature |
|-----------|-----------|-----------|-----------|
| `ed25519` | Ed25519 (RFC 8032) | 32 bytes | 64 bytes |
| `ml-dsa-65` | ML-DSA-65 (FIPS 204) | 1952 bytes | 3309 bytes |
| `ed25519+ml-dsa-65` | Hybrid: both must verify | 32 + 1952 bytes | 64 + 3309 bytes |

**Mandatory baseline.** All implementations MUST support `ed25519`.
The post-quantum suites are optional.

Hybrid keys and signatures are the concatenation of the classical and
post-quantum components, in that order.

### 5.3 KEM Suites

Negotiated during the handshake via the initiator's preference list.

| Wire name | Algorithm | Public key | Ciphertext | Shared secret |
|-----------|-----------|-----------|------------|---------------|
| `x25519-hkdf` | X25519 DHKEM (RFC 9180 style) | 32 bytes | 32 bytes | 32 bytes |
| `ml-kem-768` | ML-KEM-768 (FIPS 203) | 1184 bytes | 1088 bytes | 32 bytes |
| `x25519-hkdf+ml-kem-768` | Hybrid: both KEMs, HKDF-combined | 32+1184 bytes | 32+1088 bytes | 32 bytes |

**Mandatory baseline.** All implementations MUST support `x25519-hkdf`.
The post-quantum suites are optional.

The X25519-HKDF KEM derives the shared secret as
`HKDF(ikm=DH(eph_sk, pk), salt=eph_pk||pk, info="hhs3-dhkem-x25519", len=32)`.
The ciphertext is the ephemeral public key.

The hybrid KEM runs both KEMs independently. The ciphertext is the
concatenation of both ciphertexts. The shared secret is
`HKDF(ikm=ss_classical||ss_pq, salt="", info="hhs3-hybrid-kem-x25519-mlkem768", len=32)`.

### 5.4 KEM Negotiation

The initiator sends its KEM preferences as a JSON array ordered from most
preferred to least. The responder iterates the initiator's list and selects
the first entry it supports. If none match, the responder MUST close the
transport.

## 6. Topic Negotiation

After the handshake, the initiator and responder agree on a topic before
exchanging application data.

### 6.1 Control Message Format

Control messages are carried in control frames (type `0x02`). The control
payload is:

```
[1 byte]  control sub-type
[N bytes] topic name (UTF-8)
```

| Sub-type | Name | Meaning |
|----------|------|---------|
| `0x01` | `topic_interest` | "I want to exchange data on this topic" |
| `0x02` | `topic_accept` | "Agreed" |
| `0x03` | `topic_reject` | "No" |

### 6.2 Initial Negotiation

Immediately after the handshake completes, the initiator sends a
`topic_interest` message naming one topic. The responder replies with
`topic_accept` or `topic_reject`.

If the responder rejects, the initiator SHOULD close the connection (it
was opened for this topic and has no further use).

If the responder does not reply within a reasonable time, the initiator
SHOULD close the connection. Recommended timeout: 10 seconds.

### 6.3 Connection Reuse

On an already-established connection, either side MAY send additional
`topic_interest` messages to request data exchange on further topics. The
remote replies with `topic_accept` or `topic_reject` for each.

This enables multiple topics to share a single authenticated connection
without repeating the handshake.

### 6.4 Privacy Consideration

Sending `topic_interest` on an existing connection reveals to the remote
peer that the sender is interested in an additional topic. Since topic
IDs are derived from object IDs, this creates a correlation between the
objects a peer is synchronizing. If topic correlation is sensitive (e.g.
because topics correspond to private documents or social groups),
implementations SHOULD open separate connections per topic instead of
reusing connections. The decision to reuse or isolate is left to the
application.

## 7. Error Summary

| Condition | Action |
|-----------|--------|
| Malformed handshake message | Close transport |
| Signature verification failure | Close transport |
| AEAD decryption failure (handshake) | Close transport |
| AEAD decryption failure (session) | Close channel |
| No common KEM suite | Close transport |
| `expectedRemote` mismatch | Close transport (before Msg3) |
| Unknown mux frame type | Drop silently |
| Empty mux frame | Drop silently |
| Truncated topic data frame | Drop silently |
| Negotiation timeout | Close connection |
| Transport closed during handshake | Abort handshake |

## 8. Relationship to Noise Framework

The handshake is inspired by the [Noise Protocol Framework](https://noiseprotocol.org/)
but is not a standard Noise pattern. Key differences:

- Uses KEM-based key agreement instead of DH, allowing post-quantum suites.
- KEM suite is negotiated in-band (Msg1 preference list) rather than fixed
  at protocol compile time.
- The signing key doubles as the static identity (Noise patterns typically
  use a separate static DH key).
- Transcript binding uses a hash chain rather than Noise's symmetric state
  abstraction.

The initiator identity protection property is analogous to Noise's IX
pattern with responder-first identity, but achieved through KEM encryption
of the initiator's identity payload.

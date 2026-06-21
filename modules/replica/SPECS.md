# HHS v3 Protocol Specification Overview

Status: Initial Version v0.1

## 1. Purpose and Scope

Protocol specification overview for the HHS v3 replication protocols. Describes
the protocol layers, how they compose, and what each one requires for
interoperability. Low-level protocol details live in module-level
specifications, referenced from Section 4.

The key words MUST, MUST NOT, SHOULD, and MAY follow RFC 2119 semantics.

## 2. Architecture

HHS v3 replicates application state across peers without central coordination.
State is represented as operations on a content-addressed DAG. Peers exchange
DAG entries over authenticated, topic-multiplexed channels. Agreement on what
the data means is provided by a shared type system (Monotone View Types).

Five protocol layers are involved, bottom-up:

### 2.1 Mesh — Authenticated Channels

The mesh layer establishes secure peer-to-peer connections and multiplexes
them into topic-scoped channels.

A connection begins with a raw bidirectional byte channel (a WebSocket, for
example). A Noise-like 1.5-RTT handshake authenticates both peers using
signing keys and a negotiated KEM, with initiator identity protection: the
initiator's public key is only revealed after the responder proves possession
of the expected key. The handshake produces an encrypted channel
(ChaCha20-Poly1305 with counter-based nonces and independent send/receive
session keys derived via HKDF).

Authenticated channels are multiplexed by topic. Each frame on the wire
carries a one-byte type tag, a two-byte big-endian topic-name length, the
topic name in UTF-8, and the payload. A separate control frame type
(`0x02`) carries topic negotiation messages (`topic_interest`,
`topic_accept`, `topic_reject`) so peers can agree on which topics to
exchange before any application data flows.

Multiple topics can share a single authenticated connection. Peer identity
is a `KeyId`: the SHA-256 hash of a self-describing serialized public key
(`[4-byte BE suite-name length][suite UTF-8][raw key bytes]`), encoded as
standard Base64.

The mesh protocol is defined in the `mesh` module ([spec](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/mesh/SPECS.md)).

`mesh_ws` provides a reference WebSocket transport implementation.

### 2.2 Discovery — Peer Rendezvous (Optional)

Peers need to find each other. The tracker protocol is one way to do this:
a lightweight JSON-over-authenticated-channel request/response protocol where
clients announce presence on topics with a TTL, query for peers by topic, and
leave. The server clamps TTLs, expires stale entries, and enforces that the
announcing `keyId` matches the authenticated connection identity.

Discovery is optional. Peers that rendezvous by other means (static
configuration, QR code exchange, local network broadcast) skip this layer
entirely and only need to conform to the layers above and below.

The tracker wire protocol is defined in the `mesh_tracker_client` module
([spec](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/mesh_tracker_client/SPECS.md)). Server-specific behavior (anti-spoofing, TTL clamping, expiry) is defined in
the `mesh_tracker` module ([spec](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/mesh_tracker/SPECS.md)).

### 2.3 DAG — Content-Addressed Causal History

Application state lives in append-only DAGs. Each DAG entry consists of:

- A **payload**: an arbitrary JSON value (the operation).
- A **header**: `{ payloadHash, prevEntryHashes }` — the hash of the
payload, and the set of predecessor entry hashes that define where
in causal history this operation was applied.
- An **entry hash**: the hash of the canonicalized header.

Hashing is deterministic: JSON values are canonicalized (sorted keys, no
whitespace, minimal escaping, no trailing `.0` on integers), encoded as
UTF-8, hashed with SHA-256, and the digest is encoded as standard Base64
with padding. The resulting `B64Hash` type is the universal identifier
throughout the protocol — entry hashes, object IDs, topic IDs, and key IDs
all share this format.

A **position** (or version) is a set of entry hashes representing a point
in causal time. The **frontier** of a DAG is the minimal cover of its
current position — its "tips."

Two peers that append the same payload at the same position will always
compute the same entry hash. This is the foundation of convergence.

The entry format, canonical JSON rules, and hash derivation are defined in the
`dag` module ([spec](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/dag/SPECS.md)).

The canonical serialization implementation lives in the `json` module.

### 2.4 Sync — DAG Exchange Protocol

The sync protocol moves DAG entries between peers. It runs over topic
channels (one topic per DAG) and uses JSON-encoded messages.

The protocol has three phases:

1. **Frontier gossip.** Peers broadcast `new-frontier` messages carrying
  their current DAG frontier. When a peer receives a frontier containing
   unknown hashes, it knows it is behind and initiates a fetch.
2. **Header fetch.** The behind peer sends a `header-request` with the
  unknown hashes as `start` and its own frontier as `limits`. The
   serving peer walks backwards from `start`, stopping at `limits`, and
   streams back `header-batch` messages containing `{ hash, header }`
   pairs. A `header-response-meta` message announces how many headers
   to expect and whether the walk is complete. An optional `autoPayload`
   flag asks the server to immediately follow headers with payloads.
3. **Payload fetch.** Once headers are known, the requesting peer sends
  `payload-request` messages for specific hashes. The server responds
   with `payload-response-meta` (count) followed by sequenced
   `payload-msg` messages, each carrying a hash and its payload.

Both header and payload streams use sequence numbers starting at zero.
Requests are correlated by a random `requestId`. A `cancel-request`
message can abort in-flight streams.

On the receiving side, each entry goes through a validation pipeline:

1. Verify that the header hashes to the claimed entry hash.
2. Verify that the payload hashes to `header.payloadHash`.
3. Call type-specific validation on the payload.
4. Apply the payload and verify that the resulting entry hash matches.
5. If any step fails, discard the entry and all entries that depend on it.

The sync wire protocol is defined in the `sync` module
([spec](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/sync/SPECS.md)).

### 2.5 Data Types — Monotone View Types

The type layer defines what operations mean and how concurrent operations
are reconciled. It sits above the DAG: types read and write entries through
the DAG interface, and the sync layer moves those entries without needing
to understand their semantics beyond validation.

Monotone View Types (MVTs) are operation-based replicated data types that
allow non-commutative **barrier operations**. State is inspected through
**scoped views** — `getView(at, from)` — where `at` is the version to
observe and `from` is the later version that may contain concurrent
barrier ops affecting the view. This mechanism makes non-monotonic types
safe to replicate without coordination, at the cost of views that may be
revised as new operations arrive.

Each data type requires its own specification defining payload formats,
validation rules, and conflict resolution semantics. These are out of
scope for this document. The `mvt` module defines the general framework;
`std_types` contains the standard type implementations.

## 3. Layering Summary

```
┌─────────────────────────────────┐
│  Data Types (MVT)               │  application semantics, validation
├─────────────────────────────────┤
│  DAG                            │  content-addressed causal history
├─────────────────────────────────┤
│  Sync                           │  frontier gossip, entry exchange
├─────────────────────────────────┤
│  Mesh                           │  auth, encryption, topic mux
├ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┤
│  Discovery (optional)           │  tracker rendezvous
└─────────────────────────────────┘
         │
    Raw Transport (WebSocket, WebRTC, TCP, …)
```

Sync depends on DAG (to read/write entries) and on Mesh (for topic
channels). Data Types depend on DAG (to append and query) and are invoked
by Sync (for validation). Discovery feeds Mesh with peer addresses.

## 4. Module Specifications

- `[dag](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/dag/SPECS.md)` —
canonical JSON, entry format, hash derivation, positions and frontiers.
- `[sync](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/sync/SPECS.md)` —
frontier gossip, header/payload fetch, serving algorithm, validation pipeline.
- `[mesh](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/mesh/SPECS.md)` —
authenticated handshake, encrypted channel, mux wire format, topic negotiation.
- `[mesh_tracker_client](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/mesh_tracker_client/SPECS.md)` —
tracker wire protocol (announce, query, leave).
- `[mesh_tracker](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/mesh_tracker/SPECS.md)` —
tracker server behavior (anti-spoofing, TTL clamping, expiry).

## 5. Conformance

To interoperate, an implementation MUST:

- Implement the `dag` spec: produce identical `B64Hash` values for
identical inputs using the same hash algorithm, and construct/verify
entries per the hash derivation rules.
- Implement the `sync` spec: speak the sync wire protocol, validate
received entries per the pipeline, and support `autoPayload`.
- Implement the `mesh` spec: complete the authentication handshake,
encrypt the channel, and frame topic messages per the mux format.
- Derive `KeyId` from public keys per the identity format in the `mesh`
spec.
- Use the object ID (derived from the creation payload) as the sync
topic and DAG ID. Root-object genesis create payloads MUST include a
`type` field equal to the object's MVT type id (same string as
`getType()` and the registry key). `createObject` accepts this payload
directly; sync init-response sends it as `createPayload`, making persisted roots
self-describing for cold reopen.

Discovery (`mesh_tracker_client`, `mesh_tracker`) is optional. Peers
that rendezvous by other means need not implement the tracker protocol.
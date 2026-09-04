# Sync Protocol Specification

Status: Initial Version v0.1

## 1. Overview

The sync protocol moves DAG entries between peers. It runs over topic
channels provided by the mesh layer (one topic per DAG). All messages
are JSON-encoded as UTF-8.

The protocol has three concerns: frontier gossip (discovering
divergence), header fetch (learning causal structure), and payload fetch
(obtaining entry contents). A peer acts as both requester and server
simultaneously — every peer serves headers and payloads from its local
DAG while also fetching entries it does not yet have.

## 2. Message Encoding

Each message is a JSON object with a `type` field that identifies the
message kind. Messages are serialized with standard JSON encoding (not
canonical — these are not hashed) and transmitted as UTF-8 bytes over the
topic channel. Framing is handled by the mux layer below; the sync
protocol does not add its own length prefix.

## 3. Messages

### 3.1 Frontier Gossip

#### `new-frontier`

Direction: any peer → all peers on the topic.

```json
{
  "type": "new-frontier",
  "dagId": "<B64Hash>",
  "frontier": ["<B64Hash>", ...]
}
```

- `dagId`: the DAG being synced (also the topic ID).
- `frontier`: the sender's current DAG frontier as an array of entry
  hashes.

A peer MUST broadcast a `new-frontier` when its DAG grows (new entries
appended locally or received from sync). A peer MUST send its frontier
to a newly connected peer.

**Push-back.** When a peer receives a `new-frontier` that differs from
the local frontier, it SHOULD send its own `new-frontier` back so the
sender can discover divergence. If the received frontier is identical to
the local frontier, the peer MUST NOT push back. If the local frontier
has not changed since the last `new-frontier` sent to that peer (whether
by push-back or broadcast), the peer SHOULD NOT push back more than once
per 15 seconds.

### 3.2 Header Fetch

#### `header-request`

Direction: requester → server.

```json
{
  "type": "header-request",
  "requestId": "<opaque string>",
  "dagId": "<B64Hash>",
  "start": ["<B64Hash>", ...],
  "limits": ["<B64Hash>", ...],
  "maxHeaders": <integer>,
  "autoPayload": <boolean>
}
```

- `requestId`: opaque correlation ID chosen by the requester.
- `start`: entry hashes to start the backward walk from (typically
  unknown hashes from the peer's frontier).
- `limits`: entry hashes where the walk stops (typically the
  requester's own frontier). The server MUST NOT return entries whose
  hash appears in `limits`.
- `maxHeaders`: upper bound on the number of headers the server may
  return in this response.
- `autoPayload`: if `true` and the walk completes within `maxHeaders`,
  the server MUST follow the header stream with the payloads for the
  returned entries (see Section 4). Servers MUST support `autoPayload`.

#### `header-response-meta`

Direction: server → requester.

```json
{
  "type": "header-response-meta",
  "requestId": "<string>",
  "headerCount": <integer>,
  "complete": <boolean>,
  "payloadCount": <integer>
}
```

- `headerCount`: the number of headers that will follow in
  `header-batch` messages. MUST be ≤ the request's `maxHeaders`.
- `complete`: `true` if the backward walk reached `limits` (or
  exhausted the DAG) without hitting `maxHeaders`. `false` if
  truncated.
- `payloadCount`: the number of payloads that will follow after the
  headers. Non-zero only when `autoPayload` was `true` and `complete`
  is `true`.

This message MUST be sent before any `header-batch` for the same
`requestId`.

#### `header-batch`

Direction: server → requester.

```json
{
  "type": "header-batch",
  "requestId": "<string>",
  "sequence": <integer>,
  "headers": [
    { "hash": "<B64Hash>", "header": { "payloadHash": "...", "prevEntryHashes": {...} } },
    ...
  ]
}
```

- `sequence`: zero-based, strictly incrementing per `requestId`.
- `headers`: array of `{ hash, header }` pairs. The `header` is the
  raw JSON header object (see DAG spec §4.1).

The total number of headers across all batches for a `requestId` MUST
equal the `headerCount` announced in `header-response-meta`.

### 3.3 Payload Fetch

#### `payload-request`

Direction: requester → server.

```json
{
  "type": "payload-request",
  "requestId": "<opaque string>",
  "dagId": "<B64Hash>",
  "hashes": ["<B64Hash>", ...]
}
```

- `hashes`: entry hashes whose payloads are requested.

#### `payload-response-meta`

Direction: server → requester.

```json
{
  "type": "payload-response-meta",
  "requestId": "<string>",
  "payloadCount": <integer>
}
```

- `payloadCount`: MUST equal the number of hashes in the
  corresponding request (or the `payloadCount` from the preceding
  `header-response-meta` if this is an auto-payload stream).

This message MUST be sent before any `payload-msg` for the same
`requestId`.

#### `payload-msg`

Direction: server → requester.

```json
{
  "type": "payload-msg",
  "requestId": "<string>",
  "sequence": <integer>,
  "hash": "<B64Hash>",
  "payload": <Literal>
}
```

- `sequence`: zero-based, strictly incrementing per `requestId`.
- `hash`: the entry hash this payload belongs to.
- `payload`: the entry's payload as a JSON Literal.

The total number of `payload-msg` messages for a `requestId` MUST
equal the announced `payloadCount`.

### 3.4 Control

#### `cancel-request`

Direction: requester → server.

```json
{
  "type": "cancel-request",
  "requestId": "<string>"
}
```

The server SHOULD stop sending messages for the cancelled `requestId`
as soon as possible. Messages already in flight may still arrive; the
requester MUST tolerate them.

## 4. Header Serving Algorithm

The server walks the DAG backward from `start`, using a breadth-first
traversal. For each entry reached:

1. If the entry's hash is in `limits`, skip it (do not include or
   traverse its predecessors).
2. If the entry has already been visited, skip it.
3. Otherwise, include the entry's `{ hash, header }` in the response.
4. Enqueue the entry's predecessors (`prevEntryHashes`) for traversal.

The walk stops when the queue is empty or `maxHeaders` entries have been
collected. If it stopped due to `maxHeaders`, set `complete` to `false`;
otherwise `true`.

When `autoPayload` is `true` and `complete` is `true`, the server MUST
send a `payload-response-meta` followed by `payload-msg` messages for
every header returned, reusing the same `requestId`. The payloads are
sent in reverse collection order (deepest predecessors first).

## 5. Receiving and Validation

A conforming receiver MUST validate every entry before incorporating it
into the local DAG. The pipeline is:

1. **Header hash verification.** Compute the entry hash from the
   received header (DAG spec §4.2). If it does not match the claimed
   `hash`, discard.
2. **Payload hash verification.** Compute the payload hash from the
   received payload. If it does not match `header.payloadHash`,
   discard.
3. **Predecessor availability.** All entries in `prevEntryHashes` MUST
   be present in the local DAG before the entry can be applied. Entries
   MAY be held until predecessors arrive.
4. **Type-level validation.** The data type's validation logic MUST
   accept the payload at the given version (predecessor set).
5. **Apply and verify.** The data type's apply logic produces an entry
   hash. If this hash does not match the claimed `hash`, discard.
6. **Dependent discard.** If an entry is discarded at any step, all
   entries that transitively depend on it (via `prevEntryHashes`) MUST
   also be discarded from the receive pipeline.

**Channel-bound provenance.** Every header and payload is verified on
the topic channel it arrived on, and misbehavior is attributed to that
channel's `(keyId, endpoint)`. The `requestId` alone is NOT provenance:
a frame carrying a `requestId` that belongs to a request the receiver
sent to a *different* peer MUST be dropped without mutating that
request (the delivering channel is reported for protocol abuse). When an
entry is type-rejected, the peer blamed is the one whose channel
delivered the hash-verified payload that was applied — not a request
target, and not a peer that merely gossiped the same hash.

**Header/payload pairing.** A hash moves through: unknown, header-only,
header+verified-payload (ready to apply), applied, or rejected. A payload
that arrives before its header (so its payload hash cannot yet be
checked) MAY be buffered as *unpaired*, scoped to the delivering
request; it is promoted (and its payload hash verified) when the header
arrives, and dropped when that request completes, fails, or times out. A
receiver MUST NOT retain a payload with no in-pipeline header outside
this short unpaired buffer. Every in-order payload frame for a live
request counts toward completion, including duplicate, already-applied,
rejected, and unpaired frames, so a stream always terminates without
waiting out the request timeout.

**Type reject vs hash mismatch.** A payload-hash or header-hash mismatch
is misbehavior of the delivering channel: the entry is dropped and never
retained, and the hash is NOT remembered (there were never trustworthy
bytes). A type-level reject (step 4 fails, or step 5's applied hash
differs) happens only *after* both hashes verified; the receiver
remembers the offending entry hash in a bounded, insertion-ordered
per-synchronizer set (evicting the oldest hash when full) so it is not
re-queued. Only the offending hash is remembered; transitively dependent
entries are dropped from the pipeline but NOT added to that set. A header
whose `prevEntryHashes` includes a remembered rejected hash is not
admitted. Evicted hashes may be fetched again later; that is the bound.

## 6. Sequencing Constraints

- For a given `requestId`, the `*-response-meta` message MUST arrive
  before any corresponding batch or payload message.
- `sequence` numbers are zero-based and strictly incrementing within a
  `requestId`. A receiver that observes an out-of-order sequence SHOULD
  fail the request.
- A requester SHOULD NOT have more than one `header-request` in flight
  to the same peer at a time.
- A requester MAY have multiple `payload-request`s in flight to the
  same peer.

## 7. Timeouts and Errors

- Implementations SHOULD use a 30-second timeout for each in-flight
  request, reset on each received message for that `requestId`. Because
  every in-order frame counts toward completion (Section 5), a stream
  that has some of its entries rejected still completes normally rather
  than lingering until this timeout.
- If `headerCount` or `payloadCount` is exceeded, the receiver SHOULD
  fail the request and MAY treat the peer as misbehaving.
- Hash mismatches (header or payload) SHOULD be treated as peer
  misbehavior of the delivering channel.
- A single type-level validation failure MUST NOT by itself be treated
  as grounds for disconnecting a peer: a benign transient false reject
  must not cause a receiver to ban the only replica holding a branch. A
  peer that *repeatedly* triggers validation failures or timeouts MAY be
  disconnected.
- Failures MAY be surfaced through a structured issue report carrying an
  optional severity (`high` = terminate the peer, `moderate` = consider
  termination if it recurs, `low` = informational). Hash mismatches and
  framing/protocol violations are high severity; type rejects, timeouts,
  and `requestId` collisions are moderate; local send failures and
  unauthorized/connect failures are low.

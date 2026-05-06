# Tracker Wire Protocol Specification

Status: Initial Version v0.1

## 1. Overview

The tracker protocol provides peer rendezvous for topic-based synchronization.
Clients announce their presence on topics, query for other peers, and leave.
The server maintains a registry of `(topic, peer)` entries with TTL-based
expiry.

All messages are JSON-encoded and exchanged over an authenticated channel
(see the [mesh spec](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/mesh/SPECS.md)
for authentication). The protocol is stateless at the connection level: each
operation opens a fresh authenticated connection, sends one request, receives
one response, and disconnects.

## 2. Connection Model

1. Client connects to the tracker's transport address.
2. Client and server perform a mesh authentication handshake (Section 2 of
   the mesh spec). The client is the initiator.
3. Client sends one request message.
4. Server sends one response message.
5. Client closes the connection.

If the tracker's `KeyId` is known in advance, the client SHOULD verify
the server's identity during the handshake to prevent impersonation.

## 3. Message Encoding

Messages are encoded as `UTF-8(JSON.stringify(msg))` and sent as a single
authenticated-channel message (not wrapped in mux frames — the tracker
protocol does not use topic multiplexing).

Every message is a JSON object with a `type` field that determines its
schema.

## 4. Request Messages (Client -> Server)

### 4.1 `announce`

Register or refresh presence on one or more topics.

```json
{
  "type": "announce",
  "entries": [
    { "topic": "<B64Hash>", "ttl": <number> },
    ...
  ],
  "peer": {
    "keyId": "<B64Hash>",
    "addresses": ["<scheme>://<address>", ...]
  }
}
```

- `entries[].ttl`: requested time-to-live in seconds.
- `peer.keyId`: MUST match the authenticated identity of the connection.
  The server MUST reject requests where it does not.
- `peer.addresses`: transport addresses where this peer can be reached.

Multiple entries allow a single request to announce on several topics and
to refresh all active registrations in a single heartbeat.

### 4.2 `query`

Fetch peers for one or more topics.

```json
{
  "type": "query",
  "topics": ["<B64Hash>", ...],
  "schemes": ["ws", ...]          // optional
}
```

- `schemes`: if present, the server MUST filter results to peers that have
  at least one address matching one of the listed schemes. An address
  matches a scheme if it starts with `<scheme>://`.

### 4.3 `leave`

Deregister from one or more topics.

```json
{
  "type": "leave",
  "topics": ["<B64Hash>", ...]
}
```

The server removes entries for the authenticated client on the listed
topics.

## 5. Response Messages (Server -> Client)

### 5.1 `announce_ack`

```json
{
  "type": "announce_ack",
  "ttls": [<number>, ...]
}
```

- `ttls`: confirmed TTL for each entry, in the same order as the request.
  The server MAY clamp values to its configured bounds.

### 5.2 `query_response`

```json
{
  "type": "query_response",
  "results": {
    "<topic>": [
      { "keyId": "<B64Hash>", "addresses": [...] },
      ...
    ],
    ...
  }
}
```

One entry per requested topic. Topics with no known peers return an empty
array.

### 5.3 `leave_ack`

```json
{
  "type": "leave_ack"
}
```

### 5.4 `error`

```json
{
  "type": "error",
  "message": "<human-readable string>"
}
```

Returned for malformed requests, identity mismatches, or any server-side
failure.

## 6. TTL and Heartbeat

Registrations expire after their confirmed TTL. Clients that wish to remain
discoverable MUST re-announce before expiry. A recommended pattern is a
periodic heartbeat that sends a single `announce` request covering all
active topics.

Recommended defaults:

| Parameter | Value |
|-----------|-------|
| Requested TTL | 180 seconds |
| Heartbeat interval | 60 seconds |

The heartbeat interval SHOULD be well below the TTL to tolerate transient
failures.

## 7. Errors

| Condition | Behavior |
|-----------|----------|
| `peer.keyId` does not match authenticated identity | Server returns `error` |
| Malformed JSON or missing `type` | Server returns `error` |
| Unknown `type` value | Server returns `error` |
| Connection lost before response | Client retries or gives up |

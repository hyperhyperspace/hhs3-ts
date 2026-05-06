# Tracker Server Specification

Status: Initial Version v0.1

## 1. Overview

The tracker server implements the server side of the
[tracker wire protocol](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/mesh_tracker_client/SPECS.md).
This document specifies server-specific behavior: identity verification,
TTL clamping, and entry expiry.

## 2. Authentication

The server listens on a transport address and authenticates incoming
connections as the responder (Section 2 of the
[mesh spec](https://github.com/hyperhyperspace/hhs3-ts/blob/main/modules/mesh/SPECS.md)).

The server has its own long-term signing keypair. Clients MAY verify the
server's `KeyId` during the handshake to prevent impersonation (TOFU model:
learn the `KeyId` on first contact, pin it for future connections).

## 3. Anti-Spoofing

On every `announce` request, the server MUST verify that `peer.keyId`
equals the `remoteKeyId` of the authenticated channel. If they differ, the
server MUST return an `error` response and MUST NOT store the registration.

This prevents a peer from announcing on behalf of another.

## 4. TTL Clamping

The server applies a configured `[ttlMin, ttlMax]` range to every
requested TTL:

```
confirmed_ttl = clamp(requested_ttl, ttlMin, ttlMax)
```

The confirmed values are returned in the `announce_ack`. Clients MUST
use the confirmed TTL to schedule their heartbeats.

Recommended defaults:

| Parameter | Value |
|-----------|-------|
| `ttlMin` | 60 seconds |
| `ttlMax` | 600 seconds |

## 5. Entry Expiry

Each registration is stored with an expiration timestamp:

```
expiresAt = now + confirmed_ttl * 1000
```

The server MUST NOT return expired entries in `query_response` results.
Expired entries MAY be removed lazily (on query) or eagerly (via periodic
sweep). A recommended sweep interval is 30 seconds.

## 6. Query Filtering

When a `query` request includes a `schemes` array, the server MUST filter
results to peers that have at least one address starting with
`<scheme>://` for one of the listed schemes. If a peer has multiple
addresses, only the matching ones need to be included (though returning
all addresses of a matching peer is also acceptable).

## 7. Leave Semantics

A `leave` request removes entries for the authenticated client on the
listed topics. If a topic has no remaining entries after removal, the
server MAY clean up the empty topic map.

Leave is best-effort from the client's perspective. If the connection
fails before the server processes it, the entries will expire naturally
via TTL.

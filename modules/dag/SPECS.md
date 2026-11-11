# DAG Data Format Specification

Status: Initial Version v0.1

## 1. Overview

The DAG is the fundamental data structure for replication in HHS v3. It is
an append-only directed acyclic graph of content-addressed entries. Each
entry records an operation (payload) at a specific point in causal history
(position). The entry's hash is deterministic: two peers that apply the
same payload at the same position will always compute the same hash.

This document defines the entry format, the hashing rules, and the
causal ordering primitives. The sync protocol moves these structures
between peers; the type system interprets them.

## 2. Canonical JSON

All hashing in the protocol depends on a canonical serialization of JSON
values. Two implementations that serialize the same logical value
differently will produce different hashes and fail to interoperate.

### 2.1 Literal Types

A **Literal** is one of:

- `boolean` (`true` or `false`)
- `number` (IEEE 754 float64, finite only — see §2.2)
- `string` (well-formed Unicode, see below)
- `array` of Literals
- `object` mapping string keys to Literals

`null` and `undefined` are not valid Literals.

A string, whether a value or an object key, MUST be well-formed Unicode: a
sequence of Unicode scalar values. A string with an unpaired surrogate
(a code unit in `U+D800`–`U+DFFF` that is not half of a surrogate pair) is
not a valid Literal and MUST be rejected. Such a string has no UTF-8
encoding (§3.2). Encoders that substitute `U+FFFD` for it would give two
different values the same hash. Languages whose strings are sequences of
UTF-16 code units, and JSON text through escapes such as `"\ud800"`, can
both carry an unpaired surrogate, so an implementation MUST check for one
wherever a value enters from outside: when it creates an entry and when it
decodes a message from a peer. `U+FFFD` itself is an ordinary character
and is valid.

A Literal nests at most **128 levels**. A boolean, a number or a string has
depth 0. An array or an object has a depth one more than its deepest
member, and depth 1 when it is empty. A value nested deeper is not a valid
Literal and MUST be rejected at the same two points: when an entry is
created and when a message from a peer is decoded. The limit applies to the
whole payload of an entry (§4.3). JSON text can nest to any depth, while
the code that walks a parsed value usually recurses and fails at a depth
that differs between runtimes. The fixed limit makes every implementation
accept the same values, and an implementation MUST enforce it by counting
levels, not by running out of stack.

### 2.2 Serialization Rules

Canonical serialization is the JSON Canonicalization Scheme of
[RFC 8785](https://www.rfc-editor.org/rfc/rfc8785), applied to a Literal.
The RFC is normative. This table summarizes it:

| Type | Rule |
|------|------|
| `boolean` | `true` or `false` (no quotes) |
| `number` | Per ECMA-262 `Number::toString` (RFC 8785 §3.2.2.3). See §2.2.1 for details. |
| `string` | Enclosed in double quotes. A double quote is written `\"` and a backslash `\\`. The control characters `U+0008`, `U+0009`, `U+000A`, `U+000C` and `U+000D` are written `\b`, `\t`, `\n`, `\f` and `\r`. Any other character below `U+0020` is written as `\u00` followed by its two lowercase hex digits. Every other character is written as is (RFC 8785 §3.2.2.2). |
| `array` | `[` + comma-separated serialized elements, in order + `]` |
| `object` | `{` + comma-separated `key:value` pairs + `}`, with no space after the colon. Keys are serialized as strings and **sorted by UTF-16 code unit**, comparing the keys before they are escaped (RFC 8785 §3.2.3). |

No whitespace between tokens. No trailing commas. The result is encoded as
UTF-8, and it is valid JSON text that parses back to the same Literal.

Sorting by UTF-16 code unit differs from sorting by code point, or by
UTF-8 bytes, in one case only: a character at or above `U+10000` compared
with one in `U+E000`–`U+FFFF`. In UTF-16 the first is a surrogate pair that
starts with a code unit in `U+D800`–`U+DBFF`, so it sorts first. An
implementation in a language whose strings are not UTF-16 needs a
comparator that reproduces this order.

A Literal is narrower than the input the RFC accepts in two ways: it has no
`null`, and it nests at most 128 levels (§2.1). The RFC's own input rules
already hold for a Literal: its strings are well-formed Unicode and its
numbers are finite float64 values. Any implementation of RFC 8785
therefore produces the canonical form of a Literal.

#### 2.2.1 Number Format

Numbers MUST be finite. `NaN`, `+Infinity`, and `-Infinity` are invalid
Literals and MUST be rejected.

Negative zero (`-0`) MUST serialize as `"0"` (indistinguishable from
positive zero).

All other numbers are serialized using the ECMA-262 `Number::toString`
algorithm: produce the shortest decimal representation such that parsing
it back as an IEEE 754 float64 yields the original value. Among
equal-length representations, the one closer to the mathematical value is
chosen. Concretely:

- Integers have no decimal point: `42`, not `42.0`.
- Fractions use the minimum digits needed: `0.1`, `3.14`.
- Very large or small magnitudes use exponential notation: `1e+21`,
  `5e-7`. The exponent sign is always explicit (`e+`, `e-`).

This is the same rule specified by RFC 8785 (JSON Canonicalization
Scheme, §3.2.2.3). Conforming implementations include any ECMAScript
engine's `Number.toString()`, as well as libraries such as Ryu, Grisu3,
and Dragon4 in other languages.

### 2.3 Examples

| Input | Canonical form |
|-------|---------------|
| `{ b: 1, a: 2 }` | `{"a":2,"b":1}` |
| `"hello"` | `"hello"` |
| `42` | `42` |
| `42.0` | `42` |
| `0.1` | `0.1` |
| `-0` | `0` |
| `1e+21` | `1e+21` |
| `[3, 1, 2]` | `[3,1,2]` |
| `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]` | `[0,1,2,3,4,5,6,7,8,9,10,11]` |
| `"say \"hi\""` | `"say \"hi\""` |
| the three characters `a`, `U+000A` (line feed), `b` | `"a\nb"` |
| the single character `U+000F` | `"\u000f"` |
| the single character `U+00E9` (`é`) | `"é"` |

Key order. An object with the keys `U+20AC`, `U+000D`, `U+FB33`, `1`,
`U+1F600`, `U+0080` and `U+00F6` serializes them in the order `U+000D`,
`1`, `U+0080`, `U+00F6`, `U+20AC`, `U+1F600`, `U+FB33`. `U+1F600` comes
before `U+FB33` because its first UTF-16 code unit is `U+D83D`. This is the
example of RFC 8785 §3.2.3.

Nesting. `[[…[1]…]]` with 128 pairs of brackets is a valid Literal. With 129
pairs it is not.

The examples of RFC 8785 (§3.2.2, §3.2.3 and Appendix B) are test vectors
for this section. The `null` in the §3.2.2 example is left out, since a
Literal has none.

### 2.4 Sets

Sets of strings are encoded as JSON objects whose keys are the set elements
and whose values are all empty strings. For example, the set `{A, B, C}`
is encoded as:

```json
{"A":"","B":"","C":""}
```

Since canonical serialization sorts object keys, this encoding produces
a deterministic byte sequence regardless of insertion order. This is used
for `prevEntryHashes` in headers and for metadata values.

## 3. Hashing

### 3.1 Hash Algorithms

| Name | Digest size | Reference | Required |
|------|-------------|-----------|----------|
| `sha-256` | 32 bytes | FIPS 180-4 | Yes |
| `blake3` | 32 bytes | BLAKE3 spec | No |

The hash algorithm is fixed per DAG. All entries in a given DAG use the
same algorithm, chosen at creation time and recorded in the root entry's
payload (e.g., `"hashAlgorithm": "sha256"`). An implementation MUST
support `sha-256` and MAY support additional algorithms.

### 3.2 B64Hash Encoding

The digest is encoded as standard Base64 with padding (the `+/=`
alphabet). The resulting string type is called `B64Hash`. This encoding
is the same regardless of the hash algorithm.

To hash a Literal:

```
B64Hash = Base64(HashAlgorithm(UTF-8(canonicalize(literal))))
```

## 4. Entry Format

An entry has four components:

| Component | Hashed? | Description |
|-----------|---------|-------------|
| `header` | Yes (produces the entry hash) | Payload hash + predecessor set |
| `payload` | Yes (produces the payload hash) | Application-defined operation |
| `meta` | No | Indexing metadata, not part of the hash |
| `hash` | Derived | The entry's content address |

### 4.1 Header

A header is a JSON object with exactly two fields:

```json
{
  "payloadHash": "<B64Hash>",
  "prevEntryHashes": { "<B64Hash>": "", ... }
}
```

- `payloadHash`: the hash of the canonical serialization of the payload.
- `prevEntryHashes`: the predecessor position, encoded as a set (Section 2.4).
  MUST be a minimal cover (Section 5.1) of the intended position.
  For the first entry in a DAG, this is an empty object `{}`.

### 4.2 Entry Hash Derivation

Given a payload and a position (set of predecessor hashes):

1. `payloadHash = hash(canonicalize(payload))`
2. `header = { "payloadHash": payloadHash, "prevEntryHashes": setEncode(position) }`
3. `entryHash = hash(canonicalize(header))`

The canonical serialization of the header sorts its keys, so the header
always serializes as `{"payloadHash":"...","prevEntryHashes":{...}}`
(`payloadHash` before `prevEntryHashes` lexicographically).

### 4.3 Payload

Any valid Literal (Section 2.1). The payload is application-defined;
the DAG layer does not interpret it.

### 4.4 Metadata

Metadata is a map of string keys to sets (Section 2.4). It is not part
of the data format: metadata is excluded from the hash and is not
transmitted by the sync protocol. Only the header and payload cross
the wire.

Each peer regenerates metadata locally when applying a payload. The
type layer derives metadata deterministically from the payload content
(e.g., extracting element hashes for indexing), so all peers produce
identical metadata for the same entry. Metadata exists to support
local indexing and query — for instance, finding all entries that
touch a specific set element — without requiring a full DAG traversal.

## 5. Positions and Frontiers

With the exception of minimal covers (Section 5.1), which constrain the
entry format directly, the definitions in this section are not part of
the entry format itself. They describe the causal semantics that emerge
from the DAG structure defined above. The sync protocol uses frontiers
to detect divergence (Section 5.2), and data type specifications
reference positions and fork positions to define how concurrent
operations are resolved.

Formal definitions for these operations are included here because data
type interoperability depends on all implementations agreeing on them.
Two peers that compute different minimal covers or fork positions for
the same DAG will produce divergent views and fail to converge.

A **position** is a set of entry hashes. It represents a point in
causal history: the set of all entries reachable by following
predecessor links from the position's hashes.

We write `history(P)` for the closure of position `P` under the
predecessor relation — `P` itself plus everything before it.

### 5.1 Minimal Cover

Given a position `P`, its **minimal cover** is the smallest subset
`P'` of `P` such that `history(P') = history(P)`. In other words,
removing any element from `P'` would lose some history.

The `prevEntryHashes` of an entry MUST be a minimal cover of the
intended position (see Section 4.1).

### 5.2 Frontier

The **frontier** of a DAG is the minimal cover of the set of all
entry hashes in the DAG. It represents the "tips" — entries that
are not predecessors of any other entry. The frontier is the
starting point for sync: peers compare frontiers to discover
divergence.

### 5.3 Fork Position

Given two positions `A` and `B`, the **fork position** is a composite
structure that describes where their histories diverge. It has four
fields:

- `commonFrontier`: the minimal cover of `history(A) ∩ history(B)`.
- `common`: entries in the intersection that have a successor only
  in `A` or only in `B` (the "boundary" of the shared history).
- `forkA`: entries only in `history(A)` whose predecessors are in
  the intersection (or that have no predecessors).
- `forkB`: same for `history(B)`.

This is used by the type system for conflict detection and resolution.

## 6. Worked Example

Payload: `{"op":"hello"}`, position: empty (first entry).

1. Canonical payload: `{"op":"hello"}`
2. Payload hash: `Base64(SHA-256(UTF-8({"op":"hello"})))` = some `B64Hash` value, call it `PH`.
3. Header: `{"payloadHash":"PH","prevEntryHashes":{}}`
4. Entry hash: `Base64(SHA-256(UTF-8({"payloadHash":"PH","prevEntryHashes":{}})))`.

A second entry with payload `{"op":"world"}` at position `{entryHash1}`:

1. Payload hash: hash of `{"op":"world"}` = `PH2`.
2. `prevEntryHashes`: `{"<entryHash1>":""}`.
3. Header: `{"payloadHash":"PH2","prevEntryHashes":{"<entryHash1>":""}}`.
4. Entry hash: hash of that header.

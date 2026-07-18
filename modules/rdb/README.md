# Rdb — A Causal/Relational Database Engine

Rdb is a relational database for decentralized applications. Each replica is a complete database; the application reads and writes locally. Replicas synchronize over the open Internet and reconcile deterministically, even when peers are slow, offline, or adversarial. The schema is the authority, enforced by every replica, and it travels with the data. Rdb is built on [Monotone View Types](../mvt) (MVTs).

## Co-transactions

A row operation — insert, update, or delete — is validated optimistically against the local replica: foreign keys resolve, the author satisfies the table's restrictions, the row fits the schema. Valid operations apply immediately, with no coordination.

When a peer's updates arrive, each operation is re-checked through MVT's view-revision mechanism. A view is read *at* one version (here, the operation's own application version) *from* a later one (the current frontier): the rules are evaluated at the version where the operation was authored, now seeing everything concurrent that is visible from the frontier. Whatever breaks the schema in that view is discarded — a write whose permission was concurrently revoked, an insert whose foreign-key target was concurrently deleted, a row that violates a concurrently deployed restriction. Every honest replica reaches the same verdict.

Authority is the schema, not the message: a writer cannot evade a constraint by omitting it. Reconciliation is coordination-free.

## Content addressing

Schemas, table groups, tables and databases are identified by the hash of their creation operations. As they mutate, they are versioned by hash-linked causal history, so integrity of operations can be fully verified, and versioning reduces to the frontier-set of hashes.

## Identities

Operations are signed. An identity is a public key; its id is the key's hash. The signing suite is selectable — Ed25519, ML-DSA, or a hybrid requiring both — so post-quantum identities are opt-in. A group that declares an identity provider verifies signatures at validation and rejects forgeries.

Permissions are data. A capability is a row; restrictions gate operations on positive existence predicates ("allowed if a live row grants it to the author"). Granting is an insert, revoking a delete, and delegation chains follow from re-evaluating each grant at use. There is no privileged table and no access-control server.

## Table groups

A table group is the unit of atomicity, snapshot, observation, and composition. Its member tables share one causal history, so a single position is a consistent snapshot of every table at once, and a multi-table write is one atomic operation.

A group pins one schema version. The schema is a separate object; the group observes it at a fixed version, advanced forward only (a deploy). Pinning at the group is the only path from group to schema: every table is interpreted under the same version, and tables cannot drift onto different schema versions through different references.

## Foreign references

A group depends on another — a cross-group foreign key, a shared capability table — by observing it at a chosen version and advancing that observation forward. The dependency is recorded as data, never implicit. This is MVT's [State-Observation-as-Data](../mvt#composability-soad-architecture) pattern: the observed group is unaware of its observers, the dependency graph stays acyclic, and data is the integration surface between applications.

The same mechanism covers data and schemas. A schema is referenced by hash and reused as a module; a group's pinned schema is an observation, like its foreign-data references. Reusing a schema, sharing a capability system, and composing applications are one operation: a forward-only observation of a content-addressed object.

## Deltas & Projections

A delta reports how the database differs between two versions, on three channels:

- **Schema** — how the schema evolved: added columns and defaults, dropped tables, changed foreign keys, restrictions, flags.
- **Row (data)** — rows whose live values changed (materialized projection diff).
- **Op** — group DAG entries whose at-use void verdict flipped (reconciliation mind-changed), including gated observes when they void. Each flip carries a structured void reason at the voided horizon (`start` when un-voided, `end` when became voided): restriction failure, dangling FK, observe-gate failure, or authorization cycle.

A row-channel liveness transition pinpoints operations discarded by reconciliation: an insert that never went live, or a row revoked when a concurrent revoke or deploy came into view, appears as a row going from live to dead. The op channel names the underlying entry-level void flip and explains *why* at the voided horizon; the row channel does not.

Deltas project the database into ordinary SQL. The delta from the version an application last saw to the latest one projects current state into a plain local relational database, queried with normal SQL. [rdb_adapter](../rdb_adapter) projects deltas into a conventional store to keep it in sync.

## C-SQL

Rdb is driven through **C-SQL** (causal SQL), a SQL-like language with causal extensions: versions and views (`AT` / `FROM`), allow-rules, foreign-group bindings, and identity-aware authorship. It is implemented in [rdb_lang](../rdb_lang); [rdb_tools](../rdb_tools) provides a REPL and CLI.

```sql
CREATE SCHEMA shop AS (
  TABLE products (
    sku string PUB READONLY,
    name string
  ) ALLOW insert IF EXISTS users.caps WHERE label = 'writer' AND grantee = $author
);

CREATE TABLEGROUP shop_prod USING SCHEMA shop AT {#schemaVersion}
  BIND users => users
  USING IDENTITIES users.identities
  ALLOW UPDATE SCHEMA IF EXISTS users.caps WHERE label = 'deployer' AND grantee = $author;

INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget') BY $alice;
SELECT sku, name FROM shop_prod.products WHERE name LIKE 'Wid%' ORDER BY sku LIMIT 10;
```

The two namesake dimensions of an object are read separately. `SELECT` reads the relational dimension — the live rows at a view — and `LOG` reads the causal dimension — the operations that produced it. `SET VIEW` fixes the `AT` / `FROM` horizon both share, so the rows and their history are read at the same point in causal history.

```sql
SET VIEW AT {#at} FROM {#from};
SELECT sku, name FROM shop_prod.products WHERE name LIKE 'Wid%' ORDER BY sku;
LOG shop_prod LIMIT 20;
```

## Building blocks

Rdb is four content-addressed MVT types. C-SQL and the adapter are the intended interfaces; the types are the vocabulary the rest of the docs use.

- **RSchema** — the specification for one table group: tables, columns, foreign keys, restrictions, and migration rules. A standalone object with its own history; it evolves independently and is reusable by many groups. Spec authority belongs to its signed creators.
- **RTableGroup** — the unit of atomicity, snapshot, observation, and composition. Pins a schema version, binds and observes foreign groups, and is where deploys and cross-group references happen.
- **RTable** — a member table on a scoped projection of its group's history. Rows are write-once identities with permanent deletes; column updates are pinned to the schema birth write active at write time and per-field last-writer-wins within that incarnation.
- **RDb** — the deployment sync root: records member schemas and groups and ensures they and their transitive references are present and syncing in the replica.

## Column types

A column has a base type and, optionally, a set of type-scoped `constraints`. Values are carried in the row as `json.Literal`s; the string-carried numeric and byte types use a **canonical string** so they hash stably and round-trip losslessly across target databases (SQLite / Postgres / IndexedDB).

| Type | Carrier | Canonical form | Constraints |
|------|---------|----------------|-------------|
| `string` | JS string | — | `maxLength` |
| `integer` | JS number | `Number.isSafeInteger` | `min`, `max` |
| `float` | JS number | `Number.isFinite` | *(none)* |
| `boolean` | JS boolean | — | *(none)* |
| `json` | any non-null literal | — | *(none)* |
| `bigint` | string | signed decimal integer, no leading zeros, no `-0` (`/^(0\|-?[1-9][0-9]*)$/`) | `min`, `max` |
| `decimal` | string | fixed-scale decimal, exactly `scale` fractional digits, single canonical zero, `-0` normalized to `0` | `scale` (**required**), `precision`, `min`, `max` |
| `bytes` | string | canonical base64 (RFC 4648 standard alphabet, fixed padding) | `maxLength` (decoded byte length) |

`bigint` is an arbitrary-precision signed integer for finance-grade counters and ids; `decimal` is exact fixed-point (never a float); `bytes` is opaque binary. `integer` is now bounded to the JS safe-integer range — use `bigint` beyond it.

### Constraints and the per-type allowlist

`min` / `max` are **canonical strings** (so bigint / decimal bounds are exact) and are inclusive. `constraints` is validated with a strict per-type allowlist: **any constraint key that does not apply to the column's type is a hard reject** (there is no silent, ignored option — this prevents schema fungibility). `decimal` requires `scale >= 0`; if `precision` is present it must be `>= scale`; `min` must be `<= max`; and a column `default` must itself satisfy the type and constraints.

### Reject, never round

Value validation is a synchronous Layer-1 write-time gate (`columnValueValid`): a value that is non-canonical, out of range, or (for `decimal`) carries more fractional digits than the column scale is **hard-rejected at write time — never rounded or coerced**. Comparisons and ordering on `integer` / `float` / `bigint` / `decimal` are numeric (bigint via `BigInt`, decimal via scaled-integer), not lexical; `bytes` supports equality only. `add` / `sub` / `mul` are exact on `integer`, `bigint`, and `decimal` (operands must share a type family).

Deeper notes: [CAPABILITIES.md](./CAPABILITIES.md) (capabilities from rows and at-use predicates), [VOID_SEMANTICS.md](./VOID_SEMANTICS.md) (discarding rule-breaking operations under concurrency), [mvt](../mvt) (the underlying type system and SOaD), [rdb_lang](../rdb_lang) (the C-SQL reference).

## Tests

```
npm test
```

## Example: revoking an insert gate

[`examples/editor.sql`](./examples/editor.sql) defines a `user` group containing capabilities and a `doc` group that observes it. Page inserts require a live `writer` capability:

```sql
TABLE pages (
  title string,
  deleted boolean
) ALLOW insert IF EXISTS user.caps
    WHERE user.caps.label = 'writer'
      AND user.caps.grantee = $author
```

Register `$santi` with the identity provider:

```text
rdb:-:-> insert into user.identities (keyId, publicKey, name) values ($santi, publicKey($santi), 'Santi');
inserted osPHT/Qq (niR/TD+S)
updated ref on doc to #0XQOqMlp
```

The identity is now available to the signing rules:

```text
rdb:-:-> select * from user.identities;
rowId    | keyId  | publicKey       | name
---------+--------+-----------------+------
LzJMa+ww | $admin | AAAAB2VkMjU1MTm | Admin
osPHT/Qq | $santi | AAAAB2VkMjU1MTn | Santi
```

Without a `writer` capability, the insert gate rejects `$santi`:

```text
rdb:-:-> insert into doc.pages (title, deleted) values ('No dice', false) by $santi;
<input>:1:1: error VALIDATION_REJECTED: row envelope rejected
(object A0bzGQCM55iFHvguG0VHNRB73xtHcJ8o0Xl98n3lTJc=):
pages insert on row 'oITlMHy/egymP9j7nhQhCVRNR+u20bvMIvQ9K8T5I/o=' does not satisfy
ALLOW insert IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author
```

Grant `$santi` the `writer` capability:

```text
rdb:-:-> insert into user.caps (grantee, label) values ($santi, 'writer') by $admin;
inserted PNOfPL/+ (atbLbavD)
updated ref on doc to #FN7PWhKt
```

The capability table now contains the grant:

```text
rdb:-:-> select * from user.caps;
rowId    | rowAuthor | label   | grantee
---------+-----------+---------+--------
PNOfPL/+ | $admin    | writer  | $santi
lbz7VOYL |           | manager | $admin
```

The gate now permits `$santi` to insert two pages:

```text
rdb:-:-> insert into doc.pages (title, deleted) values ('hi', false) by $santi;
inserted NNXMJ00Z (0XQ3qXkC)
rdb:-:-> insert into doc.pages (title, deleted) values ('bye', false) by $santi;
inserted WqSuhMmR (HhtcgvFn)
```

Both rows are live:

```text
rdb:-:-> select * from doc.pages;
rowId    | rowAuthor | title | deleted
---------+-----------+-------+--------
NNXMJ00Z | $santi    | hi    | false
WqSuhMmR | $santi    | bye   | false
```

The logs provide the versions used below. The user history contains the identity and capability inserts:

```text
rdb:-:-> log user;
hash      | prev      | op                                | status
----------+-----------+-----------------------------------+-------
#p0N+nsa8 | -         | CREATE TABLEGROUP user SEED 'c... |
#niR/TD+S | #p0N+nsa8 | INSERT INTO identities (uuid, ... | OK
#atbLbavD | #niR/TD+S | INSERT INTO caps (uuid, grante... | OK
```

The document history identifies `#0XQ3qXkC` as the version after the first page insert and before the second:

```text
rdb:-:-> log doc;
hash      | prev      | op                                | status
----------+-----------+-----------------------------------+-------
#A0bzGQCM | -         | CREATE TABLEGROUP doc SEED 'Fc... |
#0XQOqMlp | #A0bzGQCM | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#FN7PWhKt | #0XQOqMlp | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#0XQ3qXkC | #FN7PWhKt | INSERT INTO pages (uuid, title... | OK
#HhtcgvFn | #0XQ3qXkC | INSERT INTO pages (uuid, title... | OK
```

Delete the capability:

```text
rdb:-:-> delete from user.caps where rowId = #PNO;
Delete needs $admin. Sign and retry? [Y/n] y
deleted PNOfPL/+ (AkpW2DhH)
updated ref on doc to #OBXGt1O5
```

The `writer` row is gone:

```text
rdb:-:-> select * from user.caps;
rowId    | rowAuthor | label   | grantee
---------+-----------+---------+--------
lbz7VOYL |           | manager | $admin
```

The document log now ends at `#OBXGt1O5`, whose reference observes the revocation:

```text
rdb:-:-> log doc;
hash      | prev      | op                                | status
----------+-----------+-----------------------------------+-------
#A0bzGQCM | -         | CREATE TABLEGROUP doc SEED 'Fc... |
#0XQOqMlp | #A0bzGQCM | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#FN7PWhKt | #0XQOqMlp | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#0XQ3qXkC | #FN7PWhKt | INSERT INTO pages (uuid, title... | OK
#HhtcgvFn | #0XQ3qXkC | INSERT INTO pages (uuid, title... | OK
#OBXGt1O5 | #HhtcgvFn | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
```

Advancing the reference at the current tip does not rewrite the earlier application views:

```text
rdb:-:-> select * from doc.pages;
rowId    | rowAuthor | title | deleted
---------+-----------+-------+--------
NNXMJ00Z | $santi    | hi    | false
WqSuhMmR | $santi    | bye   | false
```

Use the logged `#0XQ3` prefix to place the latest `user` reference immediately after the first insert, concurrent with the second:

```text
rdb:-:-> update ref user to latest on doc at #0XQ3 by $admin;
updated ref user on A0bzGQCM (R1tDyESK)
```

The second insert now sees the revoked capability from its frontier. Its insert gate is false, so reconciliation cancels the operation and removes its row:

```text
rdb:-:-> select * from doc.pages;
rowId    | rowAuthor | title | deleted
---------+-----------+-------+--------
NNXMJ00Z | $santi    | hi    | false
```

The log records the cancelled operation. The first insert precedes the concurrent reference update and remains live:

```text
rdb:-:-> log doc;
hash      | prev      | op                                | status
----------+-----------+-----------------------------------+----------
#A0bzGQCM | -         | CREATE TABLEGROUP doc SEED 'Fc... |
#0XQOqMlp | #A0bzGQCM | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#FN7PWhKt | #0XQOqMlp | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#0XQ3qXkC | #FN7PWhKt | INSERT INTO pages (uuid, title... | OK
#HhtcgvFn | #0XQ3qXkC | INSERT INTO pages (uuid, title... | Cancelled
#OBXGt1O5 | #HhtcgvFn | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#R1tDyESK | #0XQ3qXkC | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
```


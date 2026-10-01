# Rdb — A Causal/Relational Database Engine

Rdb is a relational database for decentralized applications. Each replica is a complete database; the application reads and writes locally. Replicas synchronize over the open Internet and reconcile deterministically, even when peers are slow, offline, or adversarial. The schema is the authority, enforced by every replica, and it travels with the data. Rdb is built on [Monotone View Types](../mvt) (MVTs).

## Co-transactions

A row operation — insert, update, or delete — is validated optimistically against the local replica: foreign keys resolve, the author satisfies the table's restrictions, the row fits the schema. Valid operations apply immediately, with no coordination.

When a peer's updates arrive, each operation is re-checked through MVT's view-revision mechanism. A view is read *at* one version (here, the operation's own application version) *from* a later one (the current frontier): the rules are evaluated at the version where the operation was authored, now seeing everything concurrent that is visible from the frontier. Whatever breaks the schema in that view is discarded — a write whose permission was concurrently revoked, an insert whose foreign-key target was concurrently deleted, a row that violates a concurrently deployed restriction. Every honest replica reaches the same verdict.

Authority is the schema, not the message: a writer cannot evade a constraint by omitting it. Reconciliation is coordination-free.

## Content addressing

Schemas, table groups, tables and databases are identified by the hash of their creation operations. As they mutate, they are versioned by hash-linked causal history, so integrity of operations can be fully verified, and versioning reduces to the frontier-set of hashes.

## Identities

Operations are signed. An identity is a public key; its id is the key's hash. The signing suite is selectable — Ed25519, ML-DSA, or a hybrid requiring both — so post-quantum identities are opt-in. A group's identity provider, a table mapping key ids to public keys (its own or a bound group's), is where it finds an author's key: every operation that claims an author must verify through it at validation, or it is rejected. The provider is fixed when the group is created.

A group without an identity provider is anonymous. It has no key source for row operations, bundles and observations, so it accepts them only unsigned. None of its restrictions or observation gates may read `$author`, and updates and deletes are open by default rather than reserved to the row's author. Deploys are the exception; see [Deploy authority](#deploy-authority).

Permissions are data. A capability is a row; restrictions gate operations on positive existence predicates ("allowed if a live row grants it to the author"). Granting is an insert, revoking a delete, and delegation chains follow from re-evaluating each grant at use. There is no privileged table and no access-control server.

## Table groups

A table group is the unit of atomicity, snapshot, observation, and composition. Its member tables share one causal history, so a single position is a consistent snapshot of every table at once, and a multi-table write is one atomic operation.

A group pins one schema version. The schema is a separate object; the group observes it at a fixed version, advanced forward only (a deploy). Pinning at the group is the only path from group to schema: every table is interpreted under the same version, and tables cannot drift onto different schema versions through different references.

Groups are not created one by one. A database is deployed from a **catalog**, and its groups are computed from the catalog's releases (see [Databases, catalogs and releases](#databases-catalogs-and-releases)).

## Foreign references

A group depends on another — a cross-group foreign key, a shared capability table — by observing it at a chosen version and advancing that observation forward. The dependency is recorded as data, never implicit. This is MVT's [State-Observation-as-Data](../mvt#composability-soad-architecture) pattern: the observed group is unaware of its observers, the dependency graph stays acyclic, and data is the integration surface between applications.

Observed targets are **groups and schemas** (roots in the replica map). Member tables are nested handles (`getTable`); FK/exists name a bound group plus table, not a registered `RTable` object.

The same mechanism covers data and schemas. A schema is referenced by hash and reused as a module; a group's pinned schema is an observation, like its foreign-data references. Reusing a schema, sharing a capability system, and composing applications are one operation: a forward-only observation of a content-addressed object.

## Deltas & Projections

A delta reports how the database differs between two versions, on three channels:

- **Schema** — how the schema evolved: added columns and defaults, dropped tables, changed foreign keys, restrictions, flags, and table/column **reincarnations** (a same-shape drop+re-add whose resolved def is unchanged but whose incarnation reset, so a consumer must clear and re-materialize rather than diff in place).
- **Row (data)** — rows whose live values changed (materialized projection diff).
- **Op** — group DAG entries whose at-use void verdict flipped (reconciliation mind-changed), including gated observes when they void. Each flip carries a structured void reason at the voided horizon (`start` when un-voided, `end` when became voided): restriction failure, dangling FK, observe-gate failure, or authorization cycle.

A row-channel liveness transition pinpoints operations discarded by reconciliation: an insert that never went live, or a row revoked when a concurrent revoke or deploy came into view, appears as a row going from live to dead. The op channel names the underlying entry-level void flip and explains *why* at the voided horizon; the row channel does not.

Deltas project the database into ordinary SQL. The delta from the version an application last saw to the latest one projects current state into a plain local relational database, queried with normal SQL. [rdb_adapter](../rdb_adapter) projects deltas into a conventional store; [rdb_projection](../rdb_projection) is the reactive supervisor over a whole `RDb`.

## C-SQL

Rdb is driven through **C-SQL** (causal SQL), a SQL-like language with causal extensions: versions and views (`AT` / `FROM`), allow-rules, foreign-group bindings, and identity-aware authorship. It is implemented in [rdb_lang](../rdb_lang); [rdb_tools](../rdb_tools) provides a REPL and CLI.

```sql
CREATE SCHEMA shop VERSION '1.0.0' AS (
  TABLE products (
    sku string PUB READONLY,
    name string
  ) ALLOW insert IF EXISTS users.caps WHERE label = 'writer' AND grantee = $author
);

CREATE CATALOG store VERSION '1.0.0' AS (
  TABLEGROUP users USING SCHEMA users_schema USING IDENTITIES identities,
  TABLEGROUP shop_prod USING SCHEMA shop AT LATEST
    BIND users => users
    USING IDENTITIES users.identities
    ALLOW DEPLOY IF EXISTS users.caps WHERE label = 'deployer' AND grantee = $author
) BY $dev;

CREATE DATABASE store_prod USING CATALOG store AT '1.0.0' CREATORS ($admin) BY $admin;

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

Rdb is eight content-addressed MVT types. C-SQL and the adapter are the intended interfaces; the types are the vocabulary the rest of the docs use.

- **RSchema** — the specification for one table group: tables, columns, foreign keys, restrictions, and migration rules. A standalone object with its own versioned history (every entry carries a semver that increases along every causal path); it evolves independently and is reusable by many groups. Spec authority belongs to its signed creators.
- **RCatalog** — a developer-signed DAG of semver releases describing a database's table groups: which schema each group uses at which version, its bindings, gates, identity provider and genesis rows (with deploy-time `:params`). See [Databases, catalogs and releases](#databases-catalogs-and-releases).
- **RTableGroup** — the unit of atomicity, snapshot, observation, and composition. Pins a schema version, binds and observes foreign groups, and is where deploys and cross-group references happen.
- **RTable** — a member table on a scoped projection of its group's history. Rows are write-once identities with permanent deletes; both row liveness and per-field column values are pinned to the structural table/column incarnation active at write time (per-field last-writer-wins within that incarnation), so a drop+re-add resets the table or column. See [Schema evolution and incarnations](#schema-evolution-and-incarnations).
- **RDb** — the deployment sync root: records which catalog releases were deployed and with which params, computes its member groups from them, and keeps the catalog, its schemas and the members present and syncing in the replica.
- **RDeployGate** — a replica-local record of the schema versions one group has adopted. Never synced; see [Adoption and RDeployGate](#adoption-and-rdeploygate).
- **RBlobStore** and **RFileMap** — a FILES member's bytes and its folder: content-addressed upload chains, and the files at their paths. See [Files](#files).

All of them are `RObject`s, so a consumer can observe advances through `subscribe` and pull deltas in response — the mechanism [rdb_projection](../rdb_projection) uses to stay in sync without polling. See [mvt Reactivity](../mvt#reactivity).

## Databases, catalogs and releases

A database's structure is published by its developer as a catalog, deployed by an admin into an RDb, and adopted by each client. The three steps have three verbs:

- **Release** (developer, `CREATE CATALOG` / `ALTER CATALOG`). Every catalog entry is signed by one of the catalog's creators; the genesis is the first release. A release is a diff against its parents (the maximal releases below its position): the group definitions it adds, and the versions it sets for existing groups. A merge of several parents must set every group they disagree on. Its semver must exceed every parent's. A release that introduces schemas is preceded by a signed, dependency-free `declare` entry naming them, so a replica discovers the new schemas from validated catalog state and fetches them before the release that pins them validates.
- **Deploy** (admin, `CREATE DATABASE ... USING CATALOG`, `UPDATE CATALOG ... ON db`). The RDb records the deployed release and the params it first needs (signed by a database creator when the RDb declares creators). A deploy never moves backwards: the release must not be at or below one already deployed. A release concurrent with a deployed one merges with it: both stay deployed, and each member's target is the union of its versions in them, so an instance can run "2.0.0 + 1.5.1" until a release above both is deployed. The catalog planner then creates any new member groups and deploys the new schema versions to existing ones, bound groups first (advancing their dependents' refs), and records the release last, as the commit point.
- **Adopt** (client, automatic). Each replica admits deployed releases into its members' local deploy gates, within the database's adoption range (`^<major>` of the create release by default: patches and minors flow, a new major waits until the app widens the range; a host built for major `M` sets `<(M+1).0.0`, every release below its next major). A group deploy synced from a peer waits until its version is adopted.

Membership is computed, never stored: every replica derives the same group ids from the deployed releases, the params and the database id, so group creates are never served by peers. Two concurrently deployed definitions with the same name are told apart deterministically: the definition from the higher release keeps the name, then the larger definition hash; the other gets a `_<hex8>` suffix.

Schemas don't belong to the RDb: it reaches them only through its catalog's group definitions. `catalogStatus(rdb)` reports how a replica's database stands against its catalog: the released, deployed, adopted and held releases, and for each member its target version, current version and adopted version.

### Deploy authority

Who may deploy a schema version to a group is its `canDeploy` predicate (`ALLOW DEPLOY IF` in the catalog). Without one, a group of a database with creators accepts deploys by those creators only: `canDeploy` defaults to an `$author` predicate over them, and their keys are embedded in the group (`deployKeys`), so their deploys verify even in a group without an identity provider. A deploy's author key resolves through the group's identity provider first, then `deployKeys`; a signed deploy whose key resolves through neither is rejected, and a `canDeploy` that references `$author` needs one of the two (in a catalog, an explicit `ALLOW DEPLOY IF` over `$author` needs `USING IDENTITIES`). The planner signs a deploy only when `canDeploy` reads `$author`; a `canDeploy` that doesn't takes unsigned deploys.

### Adoption and RDeployGate

Each group has a replica-local RDeployGate, derived from `{group, schema}`, that mirrors the part of the schema DAG this replica has adopted (each mirror entry tagged with its schema entry's hash). A group deploy carries the precomputed gate hashes of its target version; they are the deploy's sync dependencies on the gate, and validation recomputes them from the schema DAG rather than trusting them. So a synced deploy is applied only once the adoption policy admits the version into the local gate, while local deploys never wait. The gate never enters a group's view: it gates when a deploy arrives, not what the group means.

## Files

A catalog `FILES` definition (see [rdb_lang](../rdb_lang#catalog-files)) gives every database that deploys it two objects, computed like groups from the database id and the definition hash (`deriveFilesSeed`, `instantiateFiles`): an **RBlobStore** (`hhs/rblob_store_v1`) for the bytes and an **RFileMap** (`hhs/rfile_map_v1`) for the files at their paths. Both are bound to one member group, and name its identity table and a write predicate:

```sql
FILES media
  USING IDENTITIES user.identities
  ALLOW WRITE IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author
```

- **Access.** Every op is signed. A writer's key comes from the identity table, and `canWrite` holds, at the group version the op observes: the latest signed `ref-advance` of the group in its past, or the group's genesis. So a key the genesis rows admit writes right away, and the writers append a ref-advance when the group has moved. Admission is checked when an op is validated; a key that loses access later can still append at a position from before (no view-time voiding).
- **RBlobStore.** A file is an upload chain: a signed `file` header (`size`, `first` link, `fileHash`), then 128 KiB `chunk` ops, each on the one before it, each carrying the link of the next. `fileHash = H("hhs3-blob-file-v1" || u64be(size) || first)` commits to every byte, and each chunk is checked on arrival against its predecessor alone. A file is complete once its last chunk arrives; `putFile` resumes an interrupted chain and skips files the store has. Uploads spread over six lanes for parallelism.
- **RFileMap.** Elements `(section, owner, path, fileHash)` with add/remove semantics (`remove` is a barrier). `common` is shared by every writer; `key` elements belong to their owner, who must author them. Several hashes at one path coexist; a replace is a remove plus an add. Paths follow portable rules (NFC, no `\ : * ? " < > |`, no reserved Windows names, 1024 bytes, 32 segments). The map never waits on the store: an element can arrive before its bytes.
- **Membership.** `getMemberFiles()` lists the FILES members; groups and FILES share one namespace per database, with the same tie-break for concurrent names. The RDb creates both objects after their group and syncs them with the rest; `catalogStatus` lists them under `files`.

A FILES definition is immutable: a release can add new ones, never change or remove one. It is checked when it is added, against its group's schema at the group's version in that release: the identity table is an `IDENTITY PROVIDER`, and every table `canWrite` reads exists, with its `WHERE` columns PUB. Later releases are not checked against it, since concurrent releases deployed together would bypass any such check anyway. A group change that removes what a FILES reads leaves it **read-only**: at the group versions without it, no key resolves or `canWrite` is false (never an error), so reads keep working, writes and ref-advances are refused, and mounts report `writable: false` and keep local changes waiting. Publish a new FILES to take its place. [rdb_projection](../rdb_projection#file-mounts) mounts them as folders.

## Schema evolution and incarnations

Schema evolution is coordination-free: `add-table` / `drop-table` / `add-column` / `drop-column` / `set-fks` / `set-restrictions` / `set-concurrent-deletes` are per-slot writes resolved by last-writer-wins, so two replicas can migrate concurrently and still converge. To make that convergence *meaningful* — and to make a drop+re-add genuinely reset a table or column — every table and column carries a **structural incarnation id**.

Every schema entry also carries the schema's own **version** (strict semver: the create's is the first, `0.0.1` unless given; each update's must be above every version at its position, so versions increase along every causal path). The version decides last-writer-wins between *concurrent* writes to one slot: the write from the entry with the higher version wins, and only equal versions fall back to the larger entry hash. A causally later write wins regardless. So when two lines of a schema (say a 1.x patch and 2.0.0) both change the same slot, the schema's author's numbering says which one an instance that merges them keeps. `RSchemaView.getVersions()` reports the versions at a view's position, highest first.

An incarnation id is a content hash of the birth definition plus a **drop generation** (the number of causal-ancestor drop tombstones for that slot). Three consequences follow directly:

- **Structurally-identical concurrent adds converge.** Two branches that independently `add-table t` (or `add-column c`) with a byte-identical definition compute the *same* incarnation id, so neither masks the other and independently-run identical migrations are safe to merge. For a converged `add-table`, both branches' rows *and* column values merge under ordinary LWW; for a converged `add-column`, both branches' values for that column merge.
- **Structurally-different concurrent adds pick a winner.** Different definitions (say a different default) yield different ids; the per-slot LWW picks one winner (the higher schema version, then the larger entry hash) and the loser's writes are masked (resolve to the winning default / absent), exactly as a losing concurrent value write would.
- **Drop + re-add resets.** A re-add is always causally after the drop it follows, so its drop generation is higher and its id differs — even for a byte-identical definition. The old incarnation's rows and column values are masked; the table/column starts empty. Because the generation is a *count* (not a set of drop hashes), two replicas that independently run the same drop+re-add migration still converge.

Row **liveness** is incarnation-scoped, not just column values: a row belongs to the table incarnation it was written under, so a table reset makes prior rows non-live and lets a `rowId` be re-inserted under the new incarnation. A delta reports a same-shape reset as `reincarnated` on the table (or column) change; a projection consumer treats it as drop + create + backfill (see [rdb_adapter](../rdb_adapter)). Object identity is unchanged — a table's id stays `hash(groupId, name)`; the incarnation is a filter *within* that name-keyed scope. See `src/rschema/incarnation.ts`.

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
| `identity` | string | non-empty key-hash string | *(none)* |

`bigint` is an arbitrary-precision signed integer for finance-grade counters and ids; `decimal` is exact fixed-point (never a float); `bytes` is opaque binary. `integer` is now bounded to the JS safe-integer range — use `bigint` beyond it. `identity` holds a key hash (the id of a signing key); the group's identity provider is its default association. Equality (`=` / `!=`) is defined against other identity values and against `string` key-hash columns; ordering and LIKE are not. A projection renders it as a numeric id referencing a shared keys table (see [rdb_adapter](../rdb_adapter)).

### Constraints and the per-type allowlist

`min` / `max` are **canonical strings** (so bigint / decimal bounds are exact) and are inclusive. `constraints` is validated with a strict per-type allowlist: **any constraint key that does not apply to the column's type is a hard reject** (there is no silent, ignored option — this prevents schema fungibility). `decimal` requires `scale >= 0`; if `precision` is present it must be `>= scale`; `min` must be `<= max`; and a column `default` must itself satisfy the type and constraints.

### Reject, never round

Value validation is a synchronous Layer-1 write-time gate (`columnValueValid`): a value that is non-canonical, out of range, or (for `decimal`) carries more fractional digits than the column scale is **hard-rejected at write time — never rounded or coerced**. Comparisons and ordering on `integer` / `float` / `bigint` / `decimal` are numeric (bigint via `BigInt`, decimal via scaled-integer), not lexical; `bytes` supports equality only. `add` / `sub` / `mul` are exact on `integer`, `bigint`, and `decimal` (operands must share a type family).

`like` (`{ p: 'like', value, pattern }`, in restrictions and queries) is SQL `LIKE` over strings: `%` matches any run of characters (including none), `_` matches exactly one Unicode code point, and `\` makes the next character literal (`'100\%'`). Matching is case-sensitive and covers the whole value, so a pattern without wildcards is an equality test. The pattern may be a literal or a column; a literal pattern ending in a lone `\` fails validation, and a malformed pattern read from a column matches nothing.

Deeper notes: [CAPABILITIES.md](./CAPABILITIES.md) (capabilities from rows and at-use predicates), [VOID_SEMANTICS.md](./VOID_SEMANTICS.md) (discarding rule-breaking operations under concurrency), [mvt](../mvt) (the underlying type system and SOaD), [rdb_lang](../rdb_lang) (the C-SQL reference).

## Tests

```
npm test
```

## Example: revoking an insert gate

[`examples/editor.sql`](./examples/editor.sql) releases an `editor` catalog with a `user` group containing capabilities and a `doc` group that observes it, and deploys it as the `app` database. Page inserts require a live `writer` capability:

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
rdb:app:-> insert into user.identities (keyId, publicKey, name) values ($santi, publicKey($santi), 'Santi');
inserted osPHT/Qq (niR/TD+S)
updated ref on doc to #0XQOqMlp
```

The identity is now available to the signing rules:

```text
rdb:app:-> select * from user.identities;
rowId    | keyId  | publicKey       | name
---------+--------+-----------------+------
LzJMa+ww | $admin | AAAAB2VkMjU1MTm | Admin
osPHT/Qq | $santi | AAAAB2VkMjU1MTn | Santi
```

Without a `writer` capability, the insert gate rejects `$santi`:

```text
rdb:app:-> insert into doc.pages (title, deleted) values ('No dice', false) by $santi;
<input>:1:1: error VALIDATION_REJECTED: row envelope rejected
(object A0bzGQCM55iFHvguG0VHNRB73xtHcJ8o0Xl98n3lTJc=):
pages insert on row 'oITlMHy/egymP9j7nhQhCVRNR+u20bvMIvQ9K8T5I/o=' does not satisfy
ALLOW insert IF EXISTS user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author
```

Grant `$santi` the `writer` capability:

```text
rdb:app:-> insert into user.caps (grantee, label) values ($santi, 'writer') by $admin;
inserted PNOfPL/+ (atbLbavD)
updated ref on doc to #FN7PWhKt
```

The capability table now contains the grant:

```text
rdb:app:-> select * from user.caps;
rowId    | rowAuthor | label   | grantee
---------+-----------+---------+--------
PNOfPL/+ | $admin    | writer  | $santi
lbz7VOYL |           | manager | $admin
```

The gate now permits `$santi` to insert two pages:

```text
rdb:app:-> insert into doc.pages (title, deleted) values ('hi', false) by $santi;
inserted NNXMJ00Z (0XQ3qXkC)
rdb:app:-> insert into doc.pages (title, deleted) values ('bye', false) by $santi;
inserted WqSuhMmR (HhtcgvFn)
```

Both rows are live:

```text
rdb:app:-> select * from doc.pages;
rowId    | rowAuthor | title | deleted
---------+-----------+-------+--------
NNXMJ00Z | $santi    | hi    | false
WqSuhMmR | $santi    | bye   | false
```

The logs provide the versions used below. The user history contains the identity and capability inserts:

```text
rdb:app:-> log user;
hash      | prev      | op                                | status
----------+-----------+-----------------------------------+-------
#p0N+nsa8 | -         | -- TABLEGROUP user USING SCHEM... |
#niR/TD+S | #p0N+nsa8 | INSERT INTO identities (uuid, ... | OK
#atbLbavD | #niR/TD+S | INSERT INTO caps (uuid, grante... | OK
```

The document history identifies `#0XQ3qXkC` as the version after the first page insert and before the second:

```text
rdb:app:-> log doc;
hash      | prev      | op                                | status
----------+-----------+-----------------------------------+-------
#A0bzGQCM | -         | -- TABLEGROUP doc USING SCHEMA... |
#0XQOqMlp | #A0bzGQCM | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#FN7PWhKt | #0XQOqMlp | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#0XQ3qXkC | #FN7PWhKt | INSERT INTO pages (uuid, title... | OK
#HhtcgvFn | #0XQ3qXkC | INSERT INTO pages (uuid, title... | OK
```

Delete the capability:

```text
rdb:app:-> delete from user.caps where rowId = #PNO;
Delete needs $admin. Sign and retry? [Y/n] y
deleted PNOfPL/+ (AkpW2DhH)
updated ref on doc to #OBXGt1O5
```

The `writer` row is gone:

```text
rdb:app:-> select * from user.caps;
rowId    | rowAuthor | label   | grantee
---------+-----------+---------+--------
lbz7VOYL |           | manager | $admin
```

The document log now ends at `#OBXGt1O5`, whose reference observes the revocation:

```text
rdb:app:-> log doc;
hash      | prev      | op                                | status
----------+-----------+-----------------------------------+-------
#A0bzGQCM | -         | -- TABLEGROUP doc USING SCHEMA... |
#0XQOqMlp | #A0bzGQCM | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#FN7PWhKt | #0XQOqMlp | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#0XQ3qXkC | #FN7PWhKt | INSERT INTO pages (uuid, title... | OK
#HhtcgvFn | #0XQ3qXkC | INSERT INTO pages (uuid, title... | OK
#OBXGt1O5 | #HhtcgvFn | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
```

Advancing the reference at the current tip does not rewrite the earlier application views:

```text
rdb:app:-> select * from doc.pages;
rowId    | rowAuthor | title | deleted
---------+-----------+-------+--------
NNXMJ00Z | $santi    | hi    | false
WqSuhMmR | $santi    | bye   | false
```

Use the logged `#0XQ3` prefix to place the latest `user` reference immediately after the first insert, concurrent with the second:

```text
rdb:app:-> update ref user to latest on doc at #0XQ3 by $admin;
updated ref user on A0bzGQCM (R1tDyESK)
```

The second insert now sees the revoked capability from its frontier. Its insert gate is false, so reconciliation cancels the operation and removes its row:

```text
rdb:app:-> select * from doc.pages;
rowId    | rowAuthor | title | deleted
---------+-----------+-------+--------
NNXMJ00Z | $santi    | hi    | false
```

The log records the cancelled operation. The first insert precedes the concurrent reference update and remains live:

```text
rdb:app:-> log doc;
hash      | prev      | op                                | status
----------+-----------+-----------------------------------+----------
#A0bzGQCM | -         | -- TABLEGROUP doc USING SCHEMA... |
#0XQOqMlp | #A0bzGQCM | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#FN7PWhKt | #0XQOqMlp | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#0XQ3qXkC | #FN7PWhKt | INSERT INTO pages (uuid, title... | OK
#HhtcgvFn | #0XQ3qXkC | INSERT INTO pages (uuid, title... | Cancelled
#OBXGt1O5 | #HhtcgvFn | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
#R1tDyESK | #0XQ3qXkC | UPDATE REF #p0N+nsa85uTC7o93fh... | OK
```


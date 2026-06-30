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

A delta reports how the database differs between two versions, on two channels:

- **Data** — rows whose live values changed.
- **Schema** — how the schema evolved: added columns and defaults, dropped tables, changed foreign keys, restrictions, flags.

A delta records liveness transitions, so it also pinpoints operations discarded by reconciliation: an insert that never went live, or a row revoked when a concurrent revoke or deploy came into view, appears as a row going from live to dead.

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

- `**RSchema**` — the specification for one table group: tables, columns, foreign keys, restrictions, and migration rules. A standalone object with its own history; it evolves independently and is reusable by many groups. Spec authority belongs to its signed creators.
- `**RTableGroup**` — the unit of atomicity, snapshot, observation, and composition. Pins a schema version, binds and observes foreign groups, and is where deploys and cross-group references happen.
- `**RTable**` — a member table on a scoped projection of its group's history. Rows are write-once identities with permanent deletes and per-field last-writer-wins updates.
- `**RDb**` — the deployment sync root: records member schemas and groups and ensures they and their transitive references are present and syncing in the replica.

Deeper notes: [CAPABILITIES.md](./CAPABILITIES.md) (capabilities from rows and at-use predicates), [VOID_SEMANTICS.md](./VOID_SEMANTICS.md) (discarding rule-breaking operations under concurrency), [mvt](../mvt) (the underlying type system and SOaD), [rdb_lang](../rdb_lang) (the C-SQL reference).

## Tests

```
npm test
```


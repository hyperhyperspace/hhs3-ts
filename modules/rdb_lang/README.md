# hhs3_rdb_lang

`hhs3_rdb_lang` is the reusable SQL-like language layer for RDb. It parses command text, binds names and session values through caller-provided resolvers, compiles creation statements into RDb create payloads, executes local DDL/DML/query/history statements against already-resolved RDb objects, and renders known RDb payloads back to SQL-like text.

It does not own persistence, SQLite files, workspace root-name metadata, key storage, sync, mesh, a REPL, terminal formatting, or CLI behavior.

## Public Flow

```typescript
parseScript(sql)
  -> bind(statement, bindContext)
  -> execute(boundStatement)
```

Creation statements return create plans. Hosts decide when to call `RContext.createObject(plan.payload)`.

## Supported Statements

Creation:

```sql
CREATE DATABASE mydb;
CREATE SCHEMA shop AS (
  TABLE products (
    sku string PUB READONLY,
    name string
  ) ALLOW insert IF EXISTS users.caps WHERE label = 'writer' AND grantee = $author
);
CREATE TABLEGROUP shop_prod USING SCHEMA shop AT {#schemaVersion}
  BIND users => users
  USING IDENTITIES users.identities
  CAN DEPLOY SCHEMA IF EXISTS users.caps WHERE label = 'deployer' AND grantee = $author;
```

DDL and refs:

```sql
ALTER SCHEMA shop AS (
  ADD COLUMN products.price integer DEFAULT 0,
  SET CONCURRENT DELETES products true,
  SET ALLOW RULES products (
    ALLOW insert IF EXISTS users.caps WHERE label = 'writer' AND grantee = $author,
    ALLOW update IF rowAuthor = $author
  )
);

DEPLOY SCHEMA shop AT {#schemaVersion} ON shop_prod;
UPDATE REF users TO LATEST ON shop_prod;
```

Deployment membership (advisory, monotonic add-only on the RDb):

```sql
ADD SCHEMA shop TO mydb;
ADD TABLEGROUP shop_prod TO mydb NOTE 'main deployment';
```

A schema or tablegroup can belong to several databases; `ADD ... TO <db>` records
the membership link by id (it never moves or copies the object). The optional
`NOTE` is free-form bookkeeping, never resolved.

DML and bundles:

```sql
INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');
UPDATE shop_prod.products SET name = 'Widget 2' WHERE rowId = #row;
DELETE FROM shop_prod.products WHERE rowId = #row;

BUNDLE ON shop_prod (
  INSERT INTO products (sku, name) VALUES ('B', 'Gadget');
  UPDATE products SET name = 'Gadget 2' WHERE rowId = #row;
);
```

Queries and history:

```sql
SET VIEW AT {#at} FROM {#from};
SELECT sku, name FROM shop_prod.products WHERE name LIKE 'Wid%' ORDER BY sku LIMIT 10;
LOG shop_prod LIMIT 20;
```

## Allow Rules

Table allow rules are positive gates over row operations:

```sql
ALLOW insert IF EXISTS users.caps
  WHERE label = 'writer'
  AND grantee = $author
```

The operation is allowed only when its predicate is true. Each table or `SET ALLOW RULES` block accepts at most one expression per operation. Use `ALLOW all IF ...` for a shared insert/update/delete rule; do not combine it with operation-specific rules in the same block.

Omitted rules use RDb defaults: inserts are allowed, while updates and deletes require `rowAuthor = $author` (the row's insert author must equal the op signer). To explicitly open every operation, write `ALLOW all IF true`.

## Authorship

Authored statements — `INSERT`, `UPDATE`, `DELETE`, `BUNDLE`, `DEPLOY SCHEMA`, `UPDATE REF`, and `ALTER SCHEMA` — sign as an author identity. The author is chosen in this order:

1. an explicit trailing `BY` clause, if present;
2. otherwise the host's default author (`currentAuthor()`), which may itself be unset.

```sql
INSERT INTO users.caps (label, grantee) VALUES ('writer', $bob) BY $alice;
UPDATE docs SET title = 'x' WHERE rowId = #ab BY #c0ffee AT LATEST;
DELETE FROM docs WHERE rowId = #ab BY NOBODY;   -- explicitly unauthored
```

The author is `$name` (an unlocked identity, resolved by the host) or `#keyid` (by key-id prefix). The bareword `NOBODY` forces an unauthored op even when a default author is set — useful for anonymous writes. Because `NOBODY` is a keyword, an identity literally named `nobody` is still referenced as `$nobody`.

`BY` sits alongside the optional `AT <version>` clause and is written before it. `$author` and `$me` in value position resolve to the statement's effective author, so `VALUES ($author)` agrees with the identity chosen by `BY`. A `BUNDLE` is a single signed op: put `BY` on the `BUNDLE`, not on its inner writes (a `BY` on an inner write is a parse error). `ALTER SCHEMA` requires an author (explicit or default); the others fall back to an unauthored op when neither is present.

## Deploy Gates

Tablegroup deploy authority uses a positive deploy gate:

```sql
CAN DEPLOY SCHEMA IF EXISTS users.caps
  WHERE label = 'deployer'
  AND grantee = $author
```

The gate is evaluated when deploying a schema version on the tablegroup. It uses object context: `$author` is available, but there is no subject row.

## Ref Update Gates

A bound foreign group can gate who may advance its observed version via `UPDATE REF`:

```sql
CAN UPDATE REF users IF EXISTS users.caps
  WHERE label = 'manager'
  AND grantee = $author
```

The gate is evaluated against the named binding in object context (`$author`, no subject row). A gated binding requires an authored `UPDATE REF ... BY ...`; ungated bindings still accept `BY NOBODY`.

## Identity Providers

Declare provider columns on the table that maps key ids to public keys. When the columns are named `keyId` and `publicKey`, the column list can be omitted:

```sql
TABLE identities (
  keyId string PUB READONLY,
  publicKey string PUB READONLY,
  name string NULL PUB
) IDENTITY PROVIDER
```

Use `USING IDENTITIES` on a tablegroup to select a local or bound foreign provider for signature verification:

```sql
CREATE TABLEGROUP app_group USING SCHEMA app_schema
  BIND users => users
  USING IDENTITIES users.identities;
```

`publicKey($admin)` returns the canonical serialized public key for an identity or public-key record. Plain `$admin` remains the key id string in row values.

```sql
CREATE SCHEMA users_schema CREATORS ($admin) AS (
  TABLE identities (
    keyId string PUB READONLY,
    publicKey string PUB READONLY,
    name string NULL PUB
  ) IDENTITY PROVIDER ALLOW insert IF true,
  TABLE caps (
    label string PUB READONLY,
    grantee string PUB READONLY
  ) CONCURRENT DELETES
    ALLOW insert IF EXISTS caps WHERE label = 'manager' AND grantee = $author
    ALLOW delete IF grantee = $author OR EXISTS caps WHERE label = 'manager' AND grantee = $author
);

CREATE TABLEGROUP users
  USING SCHEMA users_schema
  USING IDENTITIES identities
  WITH ROWS (
    identities (keyId = $admin, publicKey = publicKey($admin), name = 'Admin'),
    caps (label = 'manager', grantee = $admin)
  );
```

## Binding Boundary

`LangBindContext` supplies all host-owned behavior:

- workspace name resolution for schemas, groups, tables, and log targets,
- hash-prefix and version resolution,
- session variables such as `$me`, `$admin`, and `$author`,
- default author identity (`currentAuthor`),
- explicit `BY` author resolution to an unlocked signing identity (`resolveAuthor`),
- UUID and seed generation.

The language layer validates and applies language semantics, but it does not persist workspace metadata or manage keys.

## Reverse Rendering

Reverse helpers render known payloads and DAG histories:

- `renderCreateDatabase`
- `renderCreateSchema`
- `renderCreateTableGroup`
- `renderAddSchema`
- `renderAddGroup`
- `renderSchemaUpdate`
- `renderRowOp`
- `renderRefOp`
- `renderBundle`
- `renderOp`
- `dumpSchema`
- `dumpGroup`
- `dumpDatabase`

Unknown payloads render as stable SQL comments instead of being dropped.

## Diagnostics

Language mistakes return `{ ok: false, diagnostics }` where possible. Diagnostics carry a code, message, severity, and source span. Infrastructure errors from underlying RDb/DAG operations are surfaced as execution diagnostics.

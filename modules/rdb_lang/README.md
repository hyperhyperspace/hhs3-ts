# C-SQL (causal SQL for Rdb)

Implementation of **C-SQL** (*causal SQL*): a language for RDb. It parses C-SQL command text, binds names and session values through caller-provided resolvers, compiles creation statements into RDb create payloads, executes local DDL/DML/query/history statements against already-resolved RDb objects, and renders known RDb payloads back to C-SQL text.

It does not own persistence, SQLite files, workspace root-name metadata, key storage, sync, mesh, a REPL, terminal formatting, or CLI behavior.

## Allow-rule column references

Schema `ALLOW` predicates and catalog group gates (`ALLOW DEPLOY IF`, `ALLOW UPDATE REF`) resolve column names by scope:

- Unqualified names are allowed when they refer to exactly one in-scope table.
- Use `table.column` (or `group.table.column` for cross-group `EXISTS` targets) when a bare name would be ambiguous.
- `$author` names the operation author. The old `$row.column` surface syntax is removed; correlate to the gated row with `gatedTable.column` instead (lowered to `$row.column` in the IR).
- Self-referential `EXISTS` (same table as the gated table) requires `EXISTS table AS alias WHERE alias.column = ...`.

## Quoted identifiers

A name that spells a keyword (`identity`, `table`, `note`, ...) is written in double quotes: `"identity"`, and `""` inside the quotes stands for one `"`. Each part of a qualified name is quoted on its own: `endpoints."identity"`, `users."identity".keyId`. Quoted names are always names, so `"length"(x)` is not the `length` function and `"string"` is not a type. The reverse renderer quotes exactly the name parts that collide with keywords.


## Public Flow

```typescript
parseScript(sql)
  -> bind(statement, bindContext)
  -> execute(boundStatement)
```

Creation statements return create plans. Hosts decide when to call `RContext.createObject(plan.payload)`; a `create-database` plan also carries `afterCreate(object)`, which the host runs on the new RDb to create its table groups (and deploy any whose release version is above their pin). `USE DATABASE` returns a `use-database` result that the host applies, as it does `set-view`.

## Supported Statements

Creation. A database is deployed from a catalog: the developer releases table groups in a catalog, and `CREATE DATABASE` deploys one release, creating its groups:

```sql
CREATE SCHEMA shop AS (
  TABLE products (
    sku string PUB READONLY,
    name string
  ) ALLOW insert IF EXISTS users.caps WHERE label = 'writer' AND grantee = $author
);

CREATE CATALOG store CREATORS ($dev) VERSION '1.0.0'
  PARAMS (:admin identity)
AS (
  TABLEGROUP users USING SCHEMA users_schema AT LATEST
    USING IDENTITIES identities
    WITH ROWS (identities (keyId = :admin, publicKey = publicKey(:admin), name = 'Admin'),
               caps (label = 'manager', grantee = :admin)),
  TABLEGROUP shop_prod USING SCHEMA shop AT {#schemaVersion}
    BIND users => users USING IDENTITIES users.identities
    ALLOW UPDATE REF users IF EXISTS caps WHERE caps.grantee = $author
    ALLOW DEPLOY IF EXISTS users.caps WHERE label = 'deployer' AND grantee = $author
) NOTE 'initial' BY $dev;

CREATE DATABASE store_prod USING CATALOG store AT '1.0.0'
  CREATORS ($admin) WITH PARAMS (:admin = $admin) BY $admin;
```

- `CREATORS` of a catalog (default: the author) are the only keys that may sign its entries; every entry is signed, and the genesis is the first release.
- `BIND alias => group` names another group of the same catalog (by name, or `#hash` of its definition when a merge joined two definitions with the same name). `BIND a => x, b => y` lists several.
- `:param` values in `WITH ROWS` are filled from `WITH PARAMS` when the release is deployed; `publicKey(:p)` takes an identity param's public key. Params are declared with `PARAMS (:name type, ...)` and may only appear in catalog rows. Literal values (`$admin`, `'Admin'`) are fixed in the catalog. Genesis rows get derived uuids, so `uuid` is not allowed there.
- `ALLOW DEPLOY IF` gates schema deploys to the group. Without it, a database with `CREATORS` accepts deploys from those creators only. A predicate over `$author` needs `USING IDENTITIES` to verify deploy signatures.
- `CREATE DATABASE ... AT` selects a release: a version string or `LATEST` (the default) must match exactly one release; `{#hash}` names one. `WITH PARAMS` supplies every param the release declares. `CREATORS` restrict who may deploy releases into the database. `BY` signs the deploys the planner makes for groups whose release version is above their pin. The new database becomes the current one.

Column types and constraints:

```sql
CREATE SCHEMA shop AS (
  TABLE ledger (
    id        string(64) READONLY,        -- string(n): maxLength
    memo      string NULL,
    payload   bytes(4096),                 -- bytes(n): decoded byte-length cap
    amount    decimal(18, 2) MIN '0.00' MAX '1000000.00',
    seq       bigint MIN '0',              -- arbitrary-precision integer
    qty       integer MIN 0 MAX 1000
  )
);

ALTER SCHEMA shop AS (
  ADD COLUMN ledger.fee decimal(12, 4) DEFAULT '0.0000'
);
```

The base types are `string`, `integer`, `float`, `boolean`, `json`, `bigint`, `decimal`, `bytes`, and `identity`. Parenthesized parameters and `MIN` / `MAX` modifiers map to the column's `constraints`:

- `string(n)` / `bytes(n)` set `maxLength` (bytes counts decoded bytes).
- `decimal(p, s)` sets `precision` = `p` and `scale` = `s` (SQL-standard order; both required).
- `MIN` / `MAX` set inclusive bounds and apply only to `integer` / `bigint` / `decimal`. `bigint` and `decimal` bounds and values are written as quoted strings so they stay exact (a bare number literal would lose precision); the binder canonically encodes them. A `decimal` value with more fractional digits than the column scale, or any out-of-range / non-canonical value, is **rejected, never rounded**. Constraints that do not apply to a type (e.g. `MIN` on a `string`) are rejected.

A `json` value is written `JSON '<json text>'`, where the JSON text is an ordinary quoted string (double a `'` inside it): `DEFAULT JSON '{"tags": ["x", "it''s"]}'`. Top-level strings, numbers and booleans can also be written as plain literals (`'abc'`, `3`, `true`), and an array without strings as brackets (`[1, 2.5, true]`). `null` is not allowed anywhere inside a JSON value; write `NULL` for a missing value. The reverse renderer writes arrays and objects as `JSON '...'`.

`identity` stores a key hash and takes no parameters. Insert its value as `$name` (an unlocked identity) or `#keyIdPrefix`, the same forms `BY` accepts. Compare with `=` / `!=` (including against a `string` key-hash column); ordering and LIKE are not defined.

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

UPDATE REF users TO LATEST ON shop_prod;
```

Releases and deploys. A schema change reaches a database in two steps: the developer releases it in the catalog, and the admin deploys the release:

```sql
ALTER CATALOG store VERSION '1.1.0'
  PARAMS (:support identity)
AS (
  UPDATE SCHEMA shop TO LATEST ON shop_prod,
  ADD TABLEGROUP comments USING SCHEMA comments_schema AT LATEST
    BIND shop => shop_prod, users => users USING IDENTITIES users.identities
) NOTE 'comments' BY $dev;

UPDATE CATALOG store TO '1.1.0' ON store_prod WITH PARAMS (:support = $bob) BY $admin;
USE DATABASE store_prod;
```

- An `ALTER CATALOG` release is a diff against its parents: `UPDATE SCHEMA s TO v ON group` sets a group's version in the release (it is not a deploy), and `ADD TABLEGROUP` adds a group. The version must be greater than every parent's. A release that introduces schemas is preceded by an implied, signed declare entry.
- The trailing `AT` is the insertion point, as for other authored statements, and takes release hashes (`{#h1, #h2}`) or `LATEST` only, never a version string: concurrent releases can share a version. It defaults to the catalog frontier. Several maximal releases there make the release a merge, which must `UPDATE SCHEMA` every group whose version differs across its parents.
- `UPDATE CATALOG c TO r ON db` deploys a later release (forward only): the planner creates the release's new groups, deploys the new schema versions (bound groups first, advancing their dependents' refs), then records the release. `WITH PARAMS` supplies the params the release adds; params already set cannot change. A database with `CREATORS` requires an author from among them. Other replicas adopt the release when it is within their adoption range.
- There is no group-level `UPDATE SCHEMA ... ON group`, `CREATE TABLEGROUP` or `ADD SCHEMA` / `ADD TABLEGROUP`: groups exist only through catalogs, and the catalog is the only deploy path.

Names. Tables are `db.group.table`, `group.table` or `table` (in the current group); groups are `db.group` or `group`:

- A database qualifier always resolves exactly.
- With a current database (`USE DATABASE`, `CREATE DATABASE`), a bare group resolves only inside it; a group that exists only elsewhere is an error that names the qualified form.
- With no current database, a bare group resolves when exactly one database has a group by that name.
- `LOG a.b` resolves when exactly one reading (group.table or db.group) matches.

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
LOG shop_prod LIMIT 20;  -- table/vertical: truncated reverse-render op preview; JSON: raw payload only
EXPLAIN LOG shop_prod LIMIT 20;  -- adds reason column for Cancelled group/table ops
```

## Expressions

Conditions (`WHERE`, `ALLOW ... IF`, `ALLOW DEPLOY IF`, `ALLOW UPDATE REF ... IF`) and the values inside them share one grammar, loosest-binding first:

```ebnf
condition  = or ;
or         = and { "OR" and } ;
and        = not { "AND" not } ;
not        = "NOT" not | compare ;
compare    = sum [ cmp_op sum | "LIKE" sum [ "ESCAPE" string ] ] ;
cmp_op     = "=" | "!=" | "<" | "<=" | ">" | ">=" ;
sum        = product { ( "+" | "-" ) product } ;
product    = unary { "*" unary } ;
unary      = "-" unary | primary ;
primary    = "(" condition ")"
           | "EXISTS" table [ "AS" alias ] "WHERE" condition
           | "length" "(" sum ")"
           | column | literal | variable | hash | "publicKey" "(" value ")" | json ;
column     = [ qualifier "." ] name ;
literal    = string | number | "TRUE" | "FALSE" | "NULL" ;
json       = "JSON" string | "[" ... "]" ;
```

- A condition must be a comparison, `LIKE`, `EXISTS`, `TRUE`/`FALSE`, or a `NOT`/`AND`/`OR` of conditions; a bare value (`WHERE name`) is an error, and so is a condition used as a value (`a = (b > 1)`).
- `+`, `-` and `*` are left-associative; `a - b - c` means `(a - b) - c`. `length(x)` is the string length in UTF-16 code units, like JavaScript's `.length`. In payloads they become the `add`, `sub`, `mul` and `len` operand functions.
- Unary minus applies only to numeric literals (`-1`, `-(2)`, `-1.5e-3`); for anything else write `0 - x`. Numbers are otherwise unsigned, with an optional fraction and exponent.
- `EXISTS ... WHERE` takes the rest of the condition, so an `EXISTS` inside `AND` / `OR` needs parentheses: `(EXISTS t WHERE t.a = $author) OR b = 1`.
- An unparenthesized chain like `A AND B AND C` is one group; parenthesized groups keep their nesting, so `(A AND B) AND C` compiles to a group inside a group.
- `ESCAPE` and `length` are contextual, not reserved: they are ordinary names outside these positions.
- In allow rules a quoted string may not start with `$`: payload strings starting with `$` are terms (`$author`, `$row.<col>`), so write `$author`, not `'$author'`. Queries have no terms, so `WHERE tag = '$x'` is a plain string comparison.

### LIKE

`value LIKE pattern [ESCAPE 'c']` is SQL `LIKE`, in both `SELECT ... WHERE` and allow rules:

- `%` matches any run of characters (including none) and `_` matches exactly one character (one Unicode code point).
- `\` makes the next character literal: `'100\%'` matches only `100%`, and `'a\\b'` matches `a\b`.
- Matching is case-sensitive and covers the whole value, so `name LIKE 'Widget'` is an exact match.
- The pattern may be a string literal or a column (`name LIKE t.namePattern`). A literal pattern ending in a lone `\` is rejected.
- `ESCAPE 'c'` (literal patterns only) picks a different escape character, and `ESCAPE ''` disables escaping. The compiler rewrites the pattern to the `\` form, so `'100!%' ESCAPE '!'` stores the pattern `100\%`.

```sql
SELECT sku FROM shop_prod.products WHERE sku LIKE 'A-___';          -- 'A-' plus exactly three characters
SELECT sku FROM shop_prod.products WHERE name LIKE '%50!%%' ESCAPE '!';  -- contains '50%'
```

## Foreign Keys

Mark a column as a foreign key with `REFERENCES <table>`. FK values are `rowId`s of the referenced table:

```sql
CREATE SCHEMA shop AS (
  TABLE orders (id string PUB) ALLOW all IF true,
  TABLE lines (orderRef string REFERENCES orders, qty integer) ALLOW all IF true
);
```

Cross-group FKs point at a bound group's table (`REFERENCES <binding>.<table>`); the target group must be bound and observed:

```sql
TABLE profiles (ownerId string REFERENCES users.identities, label string)
```

Insert FK values as a `#prefix` hash of the target row's `rowId`; the binder expands it to the full `rowId`:

```sql
INSERT INTO shop_prod.lines (orderRef, qty) VALUES (#a1b2c3, 1);
```

`#prefix` only binds on `REFERENCES` columns (using it elsewhere errors), and an unknown prefix is rejected at bind time.

Add or change FKs on an existing schema with `SET FKS`:

```sql
ALTER SCHEMA shop AS (
  SET FKS lines (orderRef REFERENCES orders)
);
```

## Allow Rules

Allow rules are positive gates: an operation is permitted only when its predicate is true.

### Table allow rules

Row operations on a table:

```sql
ALLOW insert IF EXISTS users.caps
  WHERE label = 'writer'
  AND grantee = $author
```

Each table or `SET ALLOW RULES` block accepts at most one expression per operation. Use `ALLOW all IF ...` for a shared insert/update/delete rule; do not combine it with operation-specific rules in the same block.

Omitted rules use RDb defaults: inserts are allowed, while updates and deletes require `rowAuthor = $author` (the row's insert author must equal the op signer). To explicitly open every operation, write `ALLOW all IF true`.

### Tablegroup gates

Deploy authority and ref-update authority are gates on a catalog group definition (`TABLEGROUP` / `ADD TABLEGROUP`):

```sql
ALLOW DEPLOY IF EXISTS users.caps
  WHERE label = 'deployer'
  AND grantee = $author

ALLOW UPDATE REF users IF EXISTS users.caps
  WHERE label = 'manager'
  AND grantee = $author
```

`ALLOW DEPLOY IF ...` is evaluated when a schema version is deployed to the group (by the catalog planner, for `UPDATE CATALOG`). Without it, the group of a database with creators accepts deploys from those creators only, whose keys the group embeds for verification. `ALLOW UPDATE REF <binding> IF ...` gates who may advance the observed version of a bound foreign group via `UPDATE REF`. Both use object context: `$author` is available, but there is no subject row. A gated binding requires an authored `UPDATE REF ... BY ...`; ungated bindings still accept `BY NOBODY`.

## Authorship

Authored statements — `INSERT`, `UPDATE`, `DELETE`, `BUNDLE`, `UPDATE REF`, `ALTER SCHEMA`, `CREATE CATALOG`, `ALTER CATALOG`, `UPDATE CATALOG` and `CREATE DATABASE` (for the deploys it makes) — sign as an author identity. The author is chosen in this order:

1. an explicit trailing `BY` clause, if present;
2. otherwise the host's default author (`currentAuthor()`), which may itself be unset.

```sql
INSERT INTO users.caps (label, grantee) VALUES ('writer', $bob) BY $alice;
UPDATE docs SET title = 'x' WHERE rowId = #ab BY #c0ffee AT LATEST;
DELETE FROM docs WHERE rowId = #ab BY NOBODY;   -- explicitly unauthored
```

The author is `$name` (an unlocked identity, resolved by the host) or `#keyid` (by key-id prefix). The bareword `NOBODY` forces an unauthored op even when a default author is set — useful for anonymous writes. Because `NOBODY` is a keyword, an identity literally named `nobody` is still referenced as `$nobody`.

`BY` sits alongside the optional `AT <version>` clause and is written before it. `$author` and `$me` in value position resolve to the statement's effective author, so `VALUES ($author)` agrees with the identity chosen by `BY`. A `BUNDLE` is a single signed op: put `BY` on the `BUNDLE`, not on its inner writes (a `BY` on an inner write is a parse error). `ALTER SCHEMA`, `CREATE CATALOG` and `ALTER CATALOG` require an author (explicit or default), a catalog creator for the catalog statements; `UPDATE CATALOG` requires one when the database declares creators. The others fall back to an unauthored op when neither is present.

## Identity Providers

Declare provider columns on the table that maps key ids to public keys. When the columns are named `keyId` and `publicKey`, the column list can be omitted:

```sql
TABLE identities (
  keyId string PUB READONLY,
  publicKey string PUB READONLY,
  name string NULL PUB
) IDENTITY PROVIDER
```

Use `USING IDENTITIES` on a catalog tablegroup to select a local or bound foreign provider for signature verification:

```sql
TABLEGROUP app_group USING SCHEMA app_schema
  BIND users => users
  USING IDENTITIES users.identities
```

`publicKey($admin)` returns the canonical serialized public key for an identity or public-key record. Plain `$admin` remains the key id string in row values.

`CREATORS` also accepts `#keyIdPrefix` or a full key-id string literal when the key is present in the host keystore (for example when replaying dumped schema SQL).

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
    ALLOW insert IF EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
    ALLOW delete IF grantee = $author OR EXISTS caps AS c WHERE c.label = 'manager' AND c.grantee = $author
);

CREATE CATALOG users_catalog VERSION '1.0.0' PARAMS (:admin identity) AS (
  TABLEGROUP users
    USING SCHEMA users_schema
    USING IDENTITIES identities
    WITH ROWS (
      identities (keyId = :admin, publicKey = publicKey(:admin), name = 'Admin'),
      caps (label = 'manager', grantee = :admin)
    )
);
CREATE DATABASE users_db USING CATALOG users_catalog WITH PARAMS (:admin = $admin);
```


## Binding Boundary

`LangBindContext` supplies all host-owned behavior:

- workspace name resolution for schemas, catalogs, databases, groups (`group` / `db.group`), tables, and log targets (`resolveCatalog`, `resolveGroup`, `resolveTable`, ...),
- the current database for bare group names (`resolveDefaultDatabase`) and the current group for bare table names (`resolveDefaultGroup`),
- labels for a group's deploys in LOG, from the database that has the group (`resolveDeployLabels`; `deployLabelsFor(db, groupId)` computes them),
- hash-prefix and version resolution (`#prefix` in `AT {…}`; bare names in `AT {…}` resolve via session version aliases),
- session scoped aliases (`key`, `schema`, `catalog`, `group`, `db`, `version`) via the REPL `\\alias` command; `$name` for identities only,
- session variables such as `$me`, `$admin`, and `$author` (identity scope: alias then keystore label),
- keystore public-key lookup for `CREATORS (#prefix)` and key-id literals (`resolvePublicKey`),
- default author identity (`currentAuthor`),
- explicit `BY` author resolution to an unlocked signing identity (`resolveAuthor`),
- UUID and seed generation.

Optional deterministic identity on create/write statements:

```sql
CREATE CATALOG store SEED 'fixed-catalog-seed' VERSION '1.0.0' AS (TABLEGROUP shop_prod USING SCHEMA shop);
CREATE DATABASE app SEED 'fixed-db-seed' USING CATALOG store;
INSERT INTO products (uuid, sku, name) VALUES ('fixed-row-uuid', 'A', 'Widget');
```

The `uuid` identifier is a reserved pseudo-column on `INSERT` (not a schema column). When omitted, the host generates fresh seeds/uuids. A database's groups are derived from the database id and their catalog definitions, and their genesis rows get derived uuids, so group ids never depend on the host.

The C-SQL layer validates and applies language semantics, but it does not persist workspace metadata or manage keys.

## Reverse Rendering

Reverse helpers render known payloads and DAG histories:

- `renderCreateSchema`, `renderSchemaUpdate`
- `renderCreateCatalog` (the genesis), `renderAlterCatalog` (a release, rendered literally: each `changes` entry becomes `UPDATE SCHEMA s TO {#v} ON group`, each `add` definition `ADD TABLEGROUP`, and the trailing `AT` is always explicit hashes)
- `renderCreateDatabase` (`USING CATALOG ... AT {#release} ... WITH PARAMS`), `renderUpdateCatalog`, `renderUseDatabase`
- `renderRowOp`, `renderRefOp`, `renderBundle`, `renderOp`
- `dumpSchema`, `dumpCatalog`, `dumpGroup`, `dumpDatabase`, `sortMemberGroupsByBindings`

Entries with no C-SQL statement render as comment lines: a group genesis (`-- TABLEGROUP doc USING SCHEMA ... (created by its database)`), a group's schema deploy (`-- deploy hhs:doc TO {#v} (editor 1.1.0)`, labeled with the deployed release that pins the version when the host passes `deployLabels`, or noting that none does), and a catalog declare (`-- declare schemas {#s1, #s2}`).

`dumpCatalog(catalog, { loadSchema })` emits the referenced schemas, then `CREATE CATALOG`, then every release as `ALTER CATALOG` in topological order. Declares are implied, so a release that sits on a declare renders the declare's insertion point, and replaying regenerates the declare. Replaying re-signs every entry, so it needs the developer's keys; signatures are deterministic, so the replayed catalog has the same entry hashes.

`dumpDatabase(db, { mode, loadSchema, loadGroup })` emits the catalog dump, `CREATE DATABASE`, `USE DATABASE`, and then (full mode) the members' history in rounds. A deploy is made by the statement that deploys its release (`CREATE DATABASE` or `UPDATE CATALOG`), so group histories are split at their deploys: each round holds the ops that depend on that release's deploys and follows its `UPDATE CATALOG`. Deploys and the planner's own ref advances after a bound group's deploy are regenerated on replay and not rendered. A deploy to a version no deployed release pins cannot be replayed; it renders as a comment with a warning.

Modes:

- `full` **(default, clone):** includes `SEED`, `uuid` pseudo-column, and `#hash` refs for replay with stable ids. Row writes and group-scoped ops render with the group's member name (`INSERT INTO shop_prod.products`, `ON shop_prod`), resolved within the dump's `USE DATABASE`; a standalone `dumpGroup` renders `ON #groupId` / `BUNDLE ON #groupId`.
- `schema` **(bootstrap):** omits `SEED`/`uuid` and uses names for catalogs, schemas and bindings; no row or ref ops, and the deploys are the `UPDATE CATALOG` statements themselves. Schema migrations keep their causal `AT`.

`aliasMode` **(opt-in via** `RenderOptions`**, enabled by** `rdb_tools` ****`\\dump`**):** emits `\alias` preamble lines (always with the full hash as target) immediately before the first statement that needs each alias, then renders readable names instead of raw hashes in `BY` (`BY $name`), `CREATORS ($name, ...)`, `AT`/`TO` version sets (`AT {schema_ver1}`), release selections, and object refs in full profile (`USING CATALOG store`, `USE DATABASE app`, etc.). `WITH ROWS` values for registered key aliases render as `$name` / `publicKey($name)` instead of repeated literals. Version aliases are allocated lazily on first reference (`{objectName}_ver{N}` per owning DAG). Keys are always aliased for portable replay even when a keystore label exists. `rowId` prefixes are unchanged. With `aliasMode: false` (default), output uses `#hash` refs.

Generated scripts never rely on workspace-wide name uniqueness: they select their database with `USE DATABASE` or use hashes. Unknown payloads render as stable SQL comments instead of being dropped.

## Diagnostics

C-SQL mistakes return `{ ok: false, diagnostics }` where possible. Diagnostics carry a code, message, severity, and source span. Infrastructure errors from underlying RDb/DAG operations are surfaced as execution diagnostics.
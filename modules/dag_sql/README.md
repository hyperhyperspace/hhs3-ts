# DAG SQL

SQL-backed persistent storage for the **`dag`** module. Implements `DagStore` and the index store interfaces (`TopoIndexStore`, `LevelIndexStore`) over an abstract `SqlConnection`, making it portable across any SQL database that supports the connection interface.

## Architecture

The module is split into a database-agnostic layer (this module) and concrete bindings:

- **`dag_sql`** — Schema management, `SqlDagStore`, `SqlTopoIndexStore`, `SqlLevelIndexStore`. Depends only on the `SqlConnection` interface.
- **`dag_sqlite`** [[local]](../dag_sqlite) — Provides a `SqlConnection` implementation backed by native SQLite.

This separation allows the same SQL storage logic to be reused with other databases (e.g. PostgreSQL, MySQL) by implementing `SqlConnection`.

## `SqlConnection` interface

```typescript
type SqlRow = Record<string, unknown>;

type SqlConnection = {
    query(sql: string, params?: unknown[]): Promise<SqlRow[]>;
    execute(sql: string, params?: unknown[]): Promise<number>;
    transaction<T>(fn: (conn: SqlConnection) => Promise<T>): Promise<T>;
};
```

## Schema

The schema is initialized via `initSchema(conn)` and versioned for forward compatibility. It manages multiple DAGs in a single database, each identified by a hash and an index type (`level` or `topo`).

```typescript
await initSchema(conn);
const dagId = await getOrCreateDag(conn, dagHash, 'level');
```

Tables: `schema_version`, `dags`, `entries`, `frontier`, `entry_info`, `level_preds`, `level_succs`, `topo_index`, `topo_preds`.

## Usage

```typescript
import { initSchema, getOrCreateDag, SqlDagStore, SqlLevelIndexStore } from '@hyper-hyper-space/hhs3_dag_sql';
import { sha256 } from '@hyper-hyper-space/hhs3_crypto';
import * as dag from '@hyper-hyper-space/hhs3_dag';

// conn: a SqlConnection (e.g. from dag_sqlite)
await initSchema(conn);
const dagId = await getOrCreateDag(conn, 'my-dag-hash', 'level');

const store = new SqlDagStore(conn, dagId);
const indexStore = new SqlLevelIndexStore(conn, dagId, { levelFactor: 8 });
const index = dag.idx.level.createDagLevelIndex(indexStore);
const myDag = dag.create(store, index, sha256);
```

## Building

To build, run the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```

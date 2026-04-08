# DAG SQLite

SQLite bindings for the **`dag_sql`** storage layer. Provides a concrete `SqlConnection` using [`better-sqlite3`](https://github.com/WiseLibs/better-sqlite3) and a convenience `SqliteDagDb` class that manages schema initialization, DAG creation, and caching in a single file-backed database.

## Usage

```typescript
import { SqliteDagDb } from '@hyper-hyper-space/hhs3_dag_sqlite';
import { sha256 } from '@hyper-hyper-space/hhs3_crypto';

const db = await SqliteDagDb.open('./my-data.db');

// Create a new DAG (or reopen an existing one with the same hash)
const myDag = await db.createDag('my-dag-hash', 'level', sha256);

await myDag.append({ greeting: 'hello' }, {});

// Later, reopen by hash
const same = await db.openDag('my-dag-hash', sha256);

db.close();
```

`SqliteDagDb` supports both `level` and `topo` index types and caches `Dag` instances by hash so repeated opens are free.

## Low-level access

For direct control over the SQL connection:

```typescript
import { openSqliteConnection } from '@hyper-hyper-space/hhs3_dag_sqlite';
import { initSchema, getOrCreateDag, SqlDagStore, SqlLevelIndexStore } from '@hyper-hyper-space/hhs3_dag_sql';

const { conn, close } = openSqliteConnection('./my-data.db');
await initSchema(conn);
// ... use conn with dag_sql stores directly
close();
```

The connection is configured with WAL journal mode and a 5-second busy timeout.

## Building

To build, run the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```

# DAG IndexedDB

Browser-native [IndexedDB](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API) storage backend for the HHSv3 **`dag`** layer. It implements the `DagStore`, `LevelIndexStore`, and `TopoIndexStore` interfaces directly on IndexedDB object stores (no SQL engine), and uses a [`BroadcastChannel`](https://developer.mozilla.org/en-US/docs/Web/API/BroadcastChannel) to detect DAG growth from other browsing contexts (tabs).

Multiple DAGs share a single IndexedDB database, keyed by a numeric `dagId`.

## Usage

```typescript
import { IdbDagDb } from '@hyper-hyper-space/hhs3_dag_idb';
import { sha256 } from '@hyper-hyper-space/hhs3_crypto';

const db = await IdbDagDb.open('my-data', { hashSuite: sha256 });

// Create a new DAG (or reopen an existing one with the same hash)
const { dag } = await db.getOrCreateDag('my-dag-hash', { type: 'my/dag', idxType: 'level' });

await dag.append({ greeting: 'hello' }, {});

// Later, reopen by hash
const same = await db.openDag('my-dag-hash');

db.close();
```

`IdbDagDb` supports both `level` and `topo` index types and caches `Dag` instances by hash so repeated opens are free.

## Transactions and cross-tab correctness

IndexedDB transactions auto-commit once control returns to the event loop without a pending request, so a raw `IDBTransaction` cannot span the DAG's append flow (which interleaves reads, index computation, and writes). Instead, each unit of work buffers its writes and flushes them in a single `readwrite` transaction.

The DAG is grow-only and content-addressed, so almost every write is conflict-free across tabs. The only shared, non-commutative state is the dense topological counter (and the entry-existence check for idempotency). Both are resolved *inside* the flush transaction, where IndexedDB's native per-store locking serializes concurrent flushes across tabs. No cross-tab lock (e.g. Web Locks) is required.

## Testing

Tests run under Node using [`fake-indexeddb`](https://github.com/dumbmatter/fakeIndexedDB) to provide the IndexedDB globals. They reuse the shared conformance, parity, and growth-event suites from `dag_test`, plus a two-instance cross-tab simulation suite.

## Building

To build, run the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```

# Rdb Projection Adapter for IndexedDb

IndexedDB [`MaterializationTarget`](../rdb_adapter) for [rdb_adapter](../rdb_adapter): a self-contained backend that materializes Rdb groups into a browser-native [IndexedDB](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API) database and captures local edits for ingestion back into Rdb.

`IdbTarget.open(name)` opens one IndexedDB database of that name (creating it on first use). The physical schema is fixed at version 1: projected tables are documents inside it, so replica schema changes do not bump the IndexedDB version.

## What it does

- Applies schema actions, row actions, and the **per-group checkpoint** in one IndexedDB transaction (`IdbTarget.apply`). Local ids and key ids are assigned inside that transaction, so concurrent connections serialize on IndexedDB's per-store locking.
- Owns the shared `rdb_keys` side table and implements `KeyIndex`.
- Implements `MaterializedChangeSource` (an outbox of local edits to the projected tables) and `RowIdentityIndex`.
- Implements `IndexTarget`: projection-local indexes declared in an index spec (see [rdb_adapter](../rdb_adapter#indexes)) become read-only, native-shaped indexes on `target.database` (see [Indexes](#indexes)).
- Implements `ChangeSignalSource` via [`BroadcastChannel`](https://developer.mozilla.org/en-US/docs/Web/API/BroadcastChannel) (`hhs3-rdb-idb:${dbName}`): an empty, at-least-once ping; observers re-drain. Listeners post on the same channel they subscribed with, so the ping reaches other tabs and other `IdbTarget`s sharing the name.

## Reading and writing

Rdb tables and columns change as `SchemaAction`s on the replica. Since IndexedDB object stores are created in a `versionchange` upgrade (the database version bumps, other connections have to close before the upgrade can proceed), projecting each table as a native object store would turn every replica schema change into such a migration. This package keeps a fixed physical schema and uses IndexedDB as a document store. Projected tables live as documents in `rows`, keyed `[table, id]`. `create-table` / `drop-table` / `add-column` / `drop-column` update `table_meta` (and rewrite those documents) in the same `apply()` transaction as the rows and the group checkpoint.

The physical stores are `table_meta`, `checkpoint`, `rows`, `sync`, `keys`, `outbox`, `op_events`, `index_entries`, `index_meta`, `index_spec`, …. App code reads and writes the projected tables through an IDB-shaped API, `target.database`, on the `IdbTarget` returned by `open`. `objectStoreNames` lists the materialized tables plus `rdb_keys`.

### Interface

The objects below match IndexedDB's `IDBDatabase` / `IDBTransaction` / `IDBRequest` / `IDBObjectStore` / `IDBIndex` / `IDBCursor` callback protocol. Schema (`createObjectStore`, `createIndex`, `versionchange`) is owned by `apply()` and the index spec, not by this handle.

**Database** (`target.database`) is the entry point: it names the materialized tables and opens transactions on them.

- `name`, `version` (always `1`)
- `objectStoreNames` — materialized table names plus `rdb_keys`; `length`, `contains(name)`, `item(i)`, iterable
- `transaction(storeNames, mode?)` — `mode` defaults to `'readonly'`
- `createObjectStore` / `deleteObjectStore` — throw `InvalidStateError`; schema goes through `apply()`
- `close()` — no-op; call `target.close()` to close the connection

**Transaction** is a batch of reads and writes that commit or abort together. `'readonly'` is reads only; `'readwrite'` may also `add` / `put` / `delete`.

- `mode`, `db`, `error`
- `oncomplete`, `onabort`, `onerror`
- `objectStore(name)`
- `abort()`, `commit()`

**Request** is one asynchronous operation (`get`, `add`, …). The result arrives on `onsuccess`; failures on `onerror`.

- `onsuccess`, `onerror`, `result`, `error`, `readyState`

**Object store** is one projected table (or `rdb_keys`). Rows are keyed by numeric `id` (`keyPath` is `'id'`; `autoIncrement` is `false`).

- `get(id)` — document, or `undefined`
- `getAll(query?, count?)` — documents, `id` order; `query` is an `id` or an `IDBKeyRange` over ids
- `getAllKeys(query?, count?)` — local ids
- `count(query?)`
- `add(value, key?)` — allocates `id` when omitted; request result is the id
- `put(value, key?)` — whole document
- `delete(id)`, `clear()`
- `indexNames` — the table's projection indexes, sorted; `index(name)` — the index, or `NotFoundError`
- `createIndex` / `deleteIndex` — throw `InvalidStateError`; indexes come from the index spec

**Index** (`store.index(name)`) is one projection index. It is read-only; writes to the store maintain it.

- `name`, `objectStore`, `keyPath`, `unique` / `multiEntry` (both `false`)
- `get(query)`, `getKey(query)` — the first match, as a document or its `id`
- `getAll(query?, count?)`, `getAllKeys(query?, count?)`, `count(query?)`
- `openCursor(query?, direction?)`, `openKeyCursor(query?, direction?)` — all four directions

**Cursor** walks one index. The request fires `onsuccess` once per step; `result` is `null` at the end.

- `key`, `primaryKey`, `direction`, `source`, `request`, and `value` (the document) on `openCursor`
- `continue(key?)`, `continuePrimaryKey(key, primaryKey)` (`next` / `prev` only), `advance(n)`
- `update` / `delete` — throw `ReadOnlyError`

**Document** is one projected row as the app sees it.

- `{ id, …columns }` with `author_key_id` when the table has an author
- local FKs as integer companions (`post_id` is the parent's `id`)
- `rdb_keys`: `{ id, keyHash, publicKey }`

### Open and project

`IdbTarget.open` takes a database name (and, in Node or tests, an `indexedDB` factory). `captureChanges: true` provisions the outbox on first open; later opens of the same name reuse that capture config.

```typescript
import { projectGroup } from '@hyper-hyper-space/hhs3_rdb_adapter';
import { IdbTarget } from '@hyper-hyper-space/hhs3_rdb_adapter_idb';

const target = await IdbTarget.open('my-projection', { captureChanges: true });
await projectGroup(group, target);

const db = target.database;
```

For a whole `RDb`, [rdb_projection](../rdb_projection) is the usual supervisor: `RdbProjection.open(rdb, ctx, target, { writer })` materializes every member under group-qualified store names and keeps both directions in sync. You still read and write `target.database`.

The same database name opened with `indexedDB.open` shows the physical stores (`rows`, `sync`, `table_meta`, …).

### Write

`add()` without an `id` allocates `nextId` in that transaction. `put` writes the whole document — read-merge-write when you are editing a few columns. `delete` takes the local `id`.

A transaction is a batch. Either every write in it lands, or none of them do (`abort()`, or a failed request). IndexedDB runs each request asynchronously: `add` / `put` / `get` / `delete` return a request, and the result arrives on `onsuccess`. The transaction itself stays alive only while a request is in flight. As soon as your code returns to the event loop with nothing pending — `await fetch(...)`, `setTimeout`, a call into another API — IndexedDB commits (or aborts) the batch. That is why the pattern is: listen for `oncomplete` the moment the transaction is created, then `await` only those requests, then `await` completion.

```typescript
function waitReq<T>(req: IDBRequest<T>): Promise<T> {
    return new Promise((resolve, reject) => {
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error ?? new Error('idb request failed'));
    });
}

function waitTx(tx: IDBTransaction): Promise<void> {
    return new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve();
        tx.onabort = () => reject(tx.error ?? new Error('transaction aborted'));
    });
}

const tx = db.transaction(['tags', 'ledger'], 'readwrite');
const done = waitTx(tx);

const tagId = await waitReq(tx.objectStore('tags').add({ code: 'urgent' }));

const ledger = tx.objectStore('ledger');
const row = await waitReq(ledger.get(1));
await waitReq(ledger.put({ ...row, memo: 'paid' }));
await waitReq(ledger.delete(2));

await done;
```

The same `'readwrite'` batch can read and write: later `get`s see `add`/`put`s already issued on this transaction.

Two writers whose scopes overlap take turns: the second transaction starts after the first commits. Native IndexedDB defines that scope as the object stores named in `transaction(...)`. Here those names are projected tables we model ourselves. Every row lives in physical `rows`, and every `target.database` transaction opens that store (plus `table_meta`, `keys`, `outbox`, `index_meta`, and `index_entries`, which `add`/`put`/`delete` also touch). So two `'readwrite'` transactions — even on different app tables — run one after the other. A `'readonly'` transaction overlaps the same physical stores as a writer, so a long read of `tags` and a write to `ledger` also take turns. Two `'readonly'` transactions may run at the same time.

Because the native lock is already on `rows`, `db.transaction('tags').objectStore('ledger')` succeeds as long as `ledger` is materialized. Native IndexedDB would reject that: a store has to be listed in `transaction(...)` to be opened. The list you pass here still has to be known table names; it just does not narrow the lock.

A query on an app table's `getAll` / `getAllKeys` / `count` (an `id`, or an `IDBKeyRange` over ids) is translated to the physical `[table, id]` keys, so it selects what a native store would.

`tx.abort()` drops the row writes, their index entries, and their outbox records together.

Errors behave as they do natively. A failed request fires `error` at the request, then at the transaction's `onerror`, and the transaction aborts unless a handler calls `preventDefault()`. `tx.error` is then the request's error, and requests still pending fail with `AbortError`. The failures you can recover from this way are:

- `ConstraintError`: `add` on an `id` that exists;
- `NotFoundError`: the table is gone;
- `InvalidStateError`: an index request after the index was dropped (e.g. by another tab).

An exception thrown by an `onsuccess` or `onerror` handler aborts with `AbortError`, as natively. Mistakes in the call itself throw at once, before any request exists:

- `ReadOnlyError`: a write on a `'readonly'` transaction;
- `DataError`: an invalid key or `id`;
- `DataCloneError`: a value that cannot be cloned;
- `TransactionInactiveError`: the transaction has finished or is aborting.

The value is cloned when you call `add` / `put`, so changing the object afterwards does not change what is written.

One difference from native IndexedDB: a native request is atomic, so any failed request can be recovered with `preventDefault()`. Here, an `add` / `put` / `delete` / `clear` takes several native steps (the row, its index entries, the outbox record, `nextId`). If IndexedDB itself fails one of those steps (quota, a storage error) after an earlier step has written, the transaction aborts even if a handler calls `preventDefault()`: a half-applied write is never committed.

### Read

A single lookup or a full table scan:

```typescript
const tags = await waitReq(
    db.transaction('tags').objectStore('tags').getAll());   // id order

const one = await waitReq(
    db.transaction('ledger', 'readonly').objectStore('ledger').get(1));
```

Several reads that need to match go on one `'readonly'` transaction. IndexedDB will not let a writer commit on the overlapping stores between the first `get` and the last, so the results are one consistent view — parent and child, or `ledger` and `tags`, as they were together. There is nothing to roll back if you `abort`; you just stop reading.

```typescript
const tx = db.transaction(['ledger', 'tags'], 'readonly');
const done = waitTx(tx);

const entry = await waitReq(tx.objectStore('ledger').get(1));
const labels = await waitReq(tx.objectStore('tags').getAll());

await done;
```

That view is the whole `rows` store, so it covers every projected table, not only the names you listed. Use `'readwrite'` when the same batch also inserts, updates, or deletes, as in the previous section.

### Indexes

Projection-local indexes are declared in an index spec and installed with `reconcileIndexes` (see [rdb_adapter](../rdb_adapter#indexes)). A declaration `{ table, name, columns }` behaves as if the app had called `store.createIndex(name, keyPath)` on a native store holding the table's documents:

- **Key path.** One column gives a key path of that target column name; several give the array of names, e.g. `'memo'` or `['customer', 'author_key_id']`. A document's key is what IndexedDB would compute from that key path.
- **Sparse.** A document whose key is not a valid IndexedDB key has no entry, as natively. Numbers (not NaN), strings, and arrays of valid keys are keys; `null`, a missing value, a boolean, or a JSON object is not. A JSON column holding an array is an array key.
- **Order.** Native IndexedDB order: by key, then by `id`. Decimal, bigint, and bytes values compare as their stored strings.
- **No options.** A declaration with `options` is rejected. There is no descending order, collation, partial index, `unique`, or `multiEntry`.

```typescript
const tx = db.transaction('ledger');
const byMemo = tx.objectStore('ledger').index('by_memo');       // keyPath ['memo', 'ref']

const paid = await waitReq(byMemo.getAll(IDBKeyRange.bound(['paid'], ['paid', []])));   // every memo 'paid'

const cursor = byMemo.openCursor(null, 'prevunique');
cursor.onsuccess = () => {
    const c = cursor.result;
    if (c === null) return;
    console.log(c.key, c.primaryKey, c.value);
    c.continue();
};
```

Entries live in the `index_entries` store, keyed `[table, name, key, id]`, and are maintained in the same transaction as every write that touches the rows: `apply()`, ingest reverts, and `add` / `put` / `delete` / `clear` on `target.database`. Each write reads the table's index records inside its own transaction, so an index installed from another tab is maintained even before this tab's `indexNames` has caught up. A request on an index dropped in the meantime fails with `InvalidStateError`. `apply()` also builds, drops, and moves indexes as the schema changes: a dropped column or table takes its indexes with it, and a declaration waiting for a missing column is built when the column arrives.

### Capture

With capture provisioned, a `put` / `add` / `delete` on `target.database` writes the row and an outbox record in the same native transaction; `oncomplete` posts `hhs3-rdb-idb:${dbName}`. `apply()` and ingest reverts write the physical stores, and the outbox stays the record of those app mutations.

From there, ingest as usual — `ingestChanges(group, target, { writer })`, or let `RdbProjection` drain on the change signal:

```typescript
target.addChangeListener(() => {
    void ingestChanges(group, target, { writer });
});
```

`setCaptureEnabled(false)` pauses recording; `target.close()` closes the channel and the IndexedDB connection.

## Test

Runs the shared conformance, ingestion, and keys suites from [rdb_adapter_test](../rdb_adapter_test). The ingestion harness mutates through `target.database`. A few IDB-only cases cover apply vs the outbox, a transaction abort, reopen, BroadcastChannel across two `IdbTarget`s, and concurrent id allocation. The index cases include a differential test against native `IDBIndex`es over the same documents (every request, and cursor walks in all four directions), plus the index lifecycle, facade writes, and cross-tab maintenance. The error cases compare the facade with a native store: request errors with and without `preventDefault()`, throwing handlers, aborts, and the checks that throw when a request is issued. They also inject native failures in the middle of a write, and check that it rolls back and that every request settles. Tests use [`fake-indexeddb`](https://github.com/dumbmatter/fakeIndexedDB). `npm test` includes a smoke projection-vs-rdb sweep (shared [generator](../rdb_adapter_test_gen) vs rdb as oracle, not idb-vs-sqlite); heavier profiles:

```
npm test
npm run test:projection:fast
npm run test:projection:full
```

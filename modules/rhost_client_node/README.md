# rhost_client_node

Reaches an [rhost](../rhost) host from a Node app, given the host's folder. It implements the [rhost_client](../rhost_client) interface without the replication stack. It depends only on crypto, mvt and better-sqlite3.

```ts
import Database from 'better-sqlite3';
import { SqliteTarget } from '@hyper-hyper-space/hhs3_rdb_adapter_sqlite';
import { openClient, projectionPath } from '@hyper-hyper-space/hhs3_rhost_client_node';

const dir = 'my-app/hosts/default';
const db = new Database(projectionPath(dir));        // my-app/hosts/default/db/data.sqlite by default
const client = openClient(dir, { db });

const me = await client.me();                        // { label, keyId, publicKey, id }
const writer = new SqliteTarget(db, { dbPath: projectionPath(dir) }).openWriter();
if (await client.lookupKey(bobsPublicKey) === undefined) {    // no row names Bob's key yet
    await writer.insert('user_identities', { key_id: { pk: bobsPublicKey }, name: 'Bob' });
}

client.events.watch((events) => { /* rejected and voided writes */ });
const status = await client.status();                // live while the host runs
await client.close();
```

## Where each function runs

| Function | Source | While stopped |
| --- | --- | --- |
| `me()` | `host.json`'s key, `rdb_keys` | works |
| `registerKey(publicKey)` | `rdb_keys` in the projection | works |
| `lookupKey(publicKey)` | `rdb_keys` in the projection | works |
| `events.since` / `events.watch` | `rdb_op_events` in the projection; `watch` polls | works |
| `status()` / `watchStatus()` | `run/rhost.sock` | a `StoppedStatus` from `host.json` |
| `watchCommits()` | `run/rhost.sock` | reconnects when the host starts |

- `projectionPath(dir)` is the projection file: `projection.path` from `host.json` when it overrides that section, otherwise from the app's `app.json` two folders up, resolved in the host folder. It reads both synchronously. Without `{ db }`, the client opens that file.
- A write goes through the projection's writer, over a `SqliteTarget` opened on the same file ([Writing](../rdb_adapter#writing)). The host's projection ingests it.
- `registerKey` checks the key with mvt's `validateKey`, computes its key id with the hash suite the projection recorded in its writer record (`rdb_writer_record`), the replica's, and interns it the way the projection's target does, in an immediate transaction. The same key gets the same id whichever side interns it first. A malformed key throws, and nothing is stored. Without a writer record it throws, naming the record: the suite is unknown then, and a guess could intern the key under a hash rdb doesn't use.
- `lookupKey` checks and hashes the key the same way, and reads its id without writing, or gives `undefined` when the key table doesn't have it.
- Before the host's first start there is no projection file. Then `events` are empty, and `me`, `registerKey` and `lookupKey` fail, because the key table doesn't exist yet.
- `watchStatus` reports a `StoppedStatus` when the socket goes away, and reconnects when the host starts again.
- `watchCommits(listener)` (on `NodeRhostClient`, over `run/rhost.sock`) delivers each `local`, `apply`, and `settle` commit to `listener`, and calls `listener({ source: 'reconnect' })` when the host acknowledges the watch. Passing those events into a local target is the caller's job: `client.watchCommits((commit) => target.notifyExternalCommit(commit))`. A `SqliteTarget` opened on the same file keeps its own WAL watcher, so a local write is still announced when the socket is down.
- `hostKey(dir)` is the host's key as `host.json` records it: label, key id and public key. It needs neither the keystore nor a passphrase. `rhost whoami` uses it.

## Test

```
npm test
```

The tests run a host with [rhost_node](../rhost_node) and need to bind its Unix socket.

**HIPs:** [20](../../hips/0020-unified-write-api.md), [29](../../hips/0029-checkpoint-consistency.md)

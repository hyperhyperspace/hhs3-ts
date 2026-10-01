# rhost_client_node

Reaches an [rhost](../rhost) host from a Node app, given the host's folder. It implements the [rhost_client](../rhost_client) interface without the replication stack. It depends only on crypto, mvt and better-sqlite3.

```ts
import Database from 'better-sqlite3';
import { openClient, projectionPath } from '@hyper-hyper-space/hhs3_rhost_client_node';

const dir = 'my-app/hosts/default';
const db = new Database(projectionPath(dir));        // my-app/hosts/default/db/data.sqlite by default
const client = openClient(dir, { db });

const me = await client.me();                        // { label, keyId, publicKey, id }
const bob = await client.registerKey(bobsPublicKey); // an rdb_keys id, for key columns
db.prepare("INSERT INTO user_identities (key_id, name) VALUES (?, 'Bob')").run(bob);

client.events.watch((events) => { /* rejected and voided writes */ });
const status = await client.status();                // live while the host runs
await client.close();
```

## Where each function runs

| Function | Source | While stopped |
| --- | --- | --- |
| `me()` | `host.json`'s key, `rdb_keys` | works |
| `registerKey(publicKey)` | `rdb_keys` in the projection | works |
| `events.since` / `events.watch` | `rdb_op_events` in the projection; `watch` polls | works |
| `status()` / `watchStatus()` | `run/rhost.sock` | a `StoppedStatus` from `host.json` |

- `projectionPath(dir)` is the projection file: `projection.path` from `host.json` when it overrides that section, otherwise from the app's `app.json` two folders up, resolved in the host folder. It reads both synchronously. Without `{ db }`, the client opens that file.
- `registerKey` computes the key id with `keyIdFromPublicKey`, and interns it the way the projection's target does, in an immediate transaction. The same key gets the same id whichever side interns it first.
- Before the host's first start there is no projection file. Then `events` are empty, and `me` and `registerKey` fail, because the key table doesn't exist yet.
- `watchStatus` reports a `StoppedStatus` when the socket goes away, and reconnects when the host starts again.
- `hostKey(dir)` is the host's key as `host.json` records it: label, key id and public key. It needs neither the keystore nor a passphrase. `rhost whoami` uses it.

## Test

```
npm test
```

The tests run a host with [rhost_node](../rhost_node) and need to bind its Unix socket.

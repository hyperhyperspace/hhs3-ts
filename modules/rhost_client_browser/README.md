# rhost_client_browser

Reaches an [rhost](../rhost) host from another browser tab. It implements the [rhost_client](../rhost_client) interface and does not run the host. It depends only on crypto and mvt.

```ts
import { openClient } from '@hyper-hyper-space/hhs3_rhost_client_browser';

const client = openClient('editor', 'default');
const me = await client.me();                        // { label, keyId, publicKey, id }
const bob = await client.lookupKey(bobsPublicKey);   // an rdb_keys id, or undefined: no row names the key
client.events.watch((events) => { /* rejected and voided writes */ });
const status = await client.status();                // live while a tab is serving
await client.close();
```

## Where each function runs

| Function | Source | While stopped |
| --- | --- | --- |
| `me()` | the host record's key, `keys` in the projection | works |
| `registerKey(publicKey)` | the projection's `keys` store | works |
| `lookupKey(publicKey)` | the projection's `keys` store | works |
| `events.since` / `events.watch` | the projection's `op_events` store; `watch` polls | works |
| `status()` / `watchStatus()` | the client BroadcastChannel | a `StoppedStatus` from the host record |

- The names are `browserLockName`, `browserClientChannel`, `browserHostsDatabase` and `browserProjectionDatabase` in [rhost_client](../rhost_client). The channel is `rhost-<app>-<host>`. No answer within a second means the host is stopped. A watch reports that, then keeps listening until a tab is serving again.
- `me` reads label, key id and public key from the host record. It needs neither the keystore nor a passphrase, and interns the key the way `IdbTarget` does.
- `registerKey` checks the key with mvt's `validateKey`, computes its key id with the hash suite the projection recorded in its writer record, the replica's, and interns it in one readwrite transaction, with the same counter as the projection. The same key gets the same id whichever side interns it first. A malformed key throws, and nothing is stored. Without a writer record it throws, naming the record: the suite is unknown then, and a guess could intern the key under a hash rdb doesn't use.
- `lookupKey` checks and hashes the key the same way, and reads its id without writing, or gives `undefined` when the keys store doesn't have it.
- Before the host's first `project` or `start` there is no projection database. Then `events` are empty, and `me`, `registerKey` and `lookupKey` fail.
- A tab writes through the writer of an `IdbTarget` it opens on the projection database ([Writing](../rdb_adapter#writing)); the tab that serves the host has `host.projection.openWriter()`.
- A tab that never called `serve` is only a client: live status while a tab answers, stopped status when none does.

## Test

```
npm test
```

**HIPs:** [20](../../hips/0020-unified-write-api.md), [29](../../hips/0029-checkpoint-consistency.md)

# rhost_client_browser

Reaches an [rhost](../rhost) host from another browser tab. It implements the [rhost_client](../rhost_client) interface and does not run the host. It depends only on crypto and mvt.

```ts
import { openClient } from '@hyper-hyper-space/hhs3_rhost_client_browser';

const client = openClient('editor', 'default');
const me = await client.me();                        // { label, keyId, publicKey, id }
const bob = await client.registerKey(bobsPublicKey); // an rdb_keys id, for key columns
client.events.watch((events) => { /* rejected and voided writes */ });
const status = await client.status();                // live while a tab is serving
await client.close();
```

## Where each function runs

| Function | Source | While stopped |
| --- | --- | --- |
| `me()` | the host record's key, `keys` in the projection | works |
| `registerKey(publicKey)` | the projection's `keys` store | works |
| `events.since` / `events.watch` | the projection's `op_events` store; `watch` polls | works |
| `status()` / `watchStatus()` | the client BroadcastChannel | a `StoppedStatus` from the host record |

- The names are `browserLockName`, `browserClientChannel`, `browserHostsDatabase` and `browserProjectionDatabase` in [rhost_client](../rhost_client). The channel is `rhost-<app>-<host>`. No answer within a second means the host is stopped. A watch reports that, then keeps listening until a tab is serving again.
- `me` reads label, key id and public key from the host record. It needs neither the keystore nor a passphrase, and interns the key the way `IdbTarget` does.
- `registerKey` computes the key id with `keyIdFromPublicKey` and interns it in one readwrite transaction, with the same counter as the projection. The same key gets the same id whichever side interns it first.
- Before the host's first `project` or `start` there is no projection database. Then `events` are empty, and `me` and `registerKey` fail.
- A tab that never called `serve` is only a client: live status while a tab answers, stopped status when none does.

## Test

```
npm test
```

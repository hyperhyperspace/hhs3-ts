# rhost_browser

The browser platform for [rhost](../rhost). One app on an origin keeps each host's record, replica and projection, and the app's keys, in IndexedDB. File mounts live in the origin private file system. The mesh is [`createBrowserMesh`](../mesh_browser) on the host's merged `mesh` document. A Web Lock is `run/rhost.lock`. A BroadcastChannel is `run/rhost.sock`.

```ts
import { Rhost } from '@hyper-hyper-space/hhs3_rhost';
import { browserPlatform, serve } from '@hyper-hyper-space/hhs3_rhost_browser';
import { parseReleaseFile } from '@hyper-hyper-space/hhs3_rpack';

const text = await (await fetch('/catalogs/editor-2.0.1-5d0e4b19.rpack')).text();
const app = await Rhost.open(config, browserPlatform({
    name: 'editor',
    releases: [parseReleaseFile(text)],
    prompt: async (key) => askTheUser(key.label),
}));
if ((await app.hosts()).length === 0) {
    await app.create('default', { key: 'me', mesh: { scope: 'internet' } });
}
await serve(app, 'default');
```

`name` namespaces every database and file mount, so two apps on one origin stay apart.

## What the platform keeps

| | Name |
| --- | --- |
| Web Lock and client channel | `rhost-<app>-<host>` |
| Keys | `rhost-<app>-keys` |
| Host records | `rhost-<app>-hosts` |
| Replica (`IdbDagDb`, default `level` index) | `rhost-<app>-<host>-store` |
| Projection (`IdbTarget`, capture on) | `rhost-<app>-<host>-data` |
| File mount | origin private file system `rhost/<app>/<host>/<mount path>` |

`projection.path` is not an IndexedDB name. Indexes and `indexPub` are installed by rhost through `IdbTarget`, which refuses a declaration that sets `options`. File mounts use `HandleDirectory`. `common/` and `keys/<id>/` are the same as on Node. A mount stays `pending` until the deployed catalog has that FILES member.

The client channel is not the mesh's BroadcastChannel (`hhs3-mesh-bc`) and not the projection's ping (`hhs3-rdb-idb:<database>`).

## Serving and standby

`serve(app, host)` is what a tab calls.

- The tab that gets the lock runs `host.start()`: update, sync from the merged mesh document, projection, file mounts, and the client listener. `serve` resolves once that tab is serving, and holds the lock until `stop` or the tab closes.
- A tab that finds the lock held stays queued. `serve` resolves once the current host has answered status. When the holder closes, the next queued tab starts the host. A tab that never calls `serve` does not take the lock.
- `stop()` writes a stopped flag, tells the queued tabs, and releases the lock. They do not start. The next `serve()` clears the flag.

Web Locks do not reenter. While `serve` holds the lock, `start` and `project` on that same tab take a hold whose release does not free the lock. `project` from a tab that is only waiting fails instead of joining the queue.

A key stored without a passphrase unlocks with `''`. `prompt` calls the `prompt` callback, or throws if the app did not pass one. `env:<VAR>` throws: a browser has no environment variables. `transports.ws.listen` and folder discovery fail when the mesh is built, with the browser mesh factory's error.

Other tabs use [rhost_client_browser](../rhost_client_browser).

## Test

```
npm test
```

The tests run on the editor catalog at 1.0.0, with `fake-indexeddb`, an in-memory BroadcastChannel, a queuing lock manager, and in-memory file directories.

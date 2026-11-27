# rhost_client

The interface an app programs against to talk to an [rhost](../rhost) host, and the wire protocol that carries part of it between processes. It has no dependencies: an app that only talks to a host doesn't install the replication stack.

## Interface

```ts
interface RhostClient {
    me(): Promise<ClientKey>;                        // { label, keyId, publicKey, id }
    registerKey(publicKey: string): Promise<number>; // an rdb_keys id, for key columns
    lookupKey(publicKey: string): Promise<number | undefined>; // the same id, or undefined, storing nothing
    events: {
        since(afterId?: number, limit?: number): Promise<ClientEvent[]>;
        watch(listener: (events: ClientEvent[]) => void): () => void;
    };
    status(): Promise<ClientStatus>;
    watchStatus(listener: (status: ClientStatus) => void): () => void;
    close(): Promise<void>;
}
```

- **`ClientStatus`** is a `HostStatus` or a `StoppedStatus`; `isHostStatus` tells them apart.
  - `HostStatus` is the status rhost computes from the replica: the `host` name, deployed, adopted and held releases, members, `upgradeRequired`, `hostBehind` and `running`.
  - A host that's behind adds `notDeployed`, why the shipped release isn't deployed (`autoDeploy` is `none`, a major bump, the key isn't a creator, or a missing param), and `missingParams`, each `{ name, type }`, when params are why.
  - While it runs, it adds `peers`, `bootstrapping` (`true` while a host that joined has not yet found its own key on the app's `allow` list, and so admits any peer; see rhost's [Allow lists](../rhost/README.md#allow-lists)), `files` (one `FilesMountStatus` per file mount: `mounted` with its file, missing and local-only counts and whether this key can write, or `pending`) and `lastError`.
  - `StoppedStatus` is what a client in another process knows of a host that isn't running: its name, database, catalog, and whether it was created here.
- **`ClientEvent`** is a rejected local write, or a concurrent change that voided or reinstated one. It has the same fields as the projection's op-event log, plus its id.
- **Keys.** `registerKey` interns a public key, the base64 of the serialized key, and gives its id in the key table. `lookupKey` gives that id without storing the key, or `undefined` when the projection doesn't have it, which no row names then: a binding that compares a public key with rows before it writes asks it first, and writes the key as a `{ pk }` ([Writing](../rdb_adapter#key-columns)). Both hash the key with the replica's hash suite, and both throw on a key that mvt's `validateKey` refuses, storing nothing. A form can call `validateKey` itself to say why a pasted key is refused.

There are three implementations:

- in process, `host.client` in rhost;
- over `run/rhost.sock`, [rhost_client_node](../rhost_client_node);
- over a BroadcastChannel, [rhost_client_browser](../rhost_client_browser).

## Protocol

One JSON object per line. It is read-only: it carries the host's live state, and nothing that signs.

```
client -> host   { id, method: 'status' }
                 { id, method: 'watchStatus' }
                 { id, method: 'watchCommits' }
                 { id, method: 'unwatch', watch: <id of the watch request> }
host -> client   { id, result }   { id, error }
                 { id, event }    (a status push on watchStatus)
                 { id, commit }   (a commit push on watchCommits)
```

`watchCommits` is used by the Node client to stream the host projection's `local`, `apply`, and `settle` commit events over `run/rhost.sock` (browser tabs receive commit events directly on `IdbTarget`'s `BroadcastChannel`, so `RhostClient` itself stays unchanged).

- `encodeMessage`, `decodeRequest` and `decodeServerMessage` encode and parse messages.
- `LineDecoder` splits a stream of text chunks into lines.
- `socketPathFor(hostDir, platform)` is where a running host listens: `run/rhost.sock` under its folder (`SOCKET_FILE`), or a named pipe on Windows.
- `browserLockName(app, host)` is the Web Lock and the client BroadcastChannel (`rhost-<app>-<host>`). `browserKeysDatabase`, `browserHostsDatabase`, `browserReplicaDatabase`, `browserProjectionDatabase` and `browserFilePath` are the IndexedDB databases and the origin-private file path for that app and host.

## Test

```
npm test
```

**HIPs:** [20](../../hips/0020-unified-write-api.md), [23](../../hips/0023-join-bootstrap.md)

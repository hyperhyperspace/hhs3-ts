# rhost_client

The interface an app programs against to talk to an [rhost](../rhost) host, and the wire protocol that carries part of it between processes. It has no dependencies: an app that only talks to a host doesn't install the replication stack.

## Interface

```ts
interface RhostClient {
    me(): Promise<ClientKey>;                        // { label, keyId, publicKey, id }
    registerKey(publicKey: string): Promise<number>; // an rdb_keys id, for key columns
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
  - While it runs, it adds `peers`, `files` (one `FilesMountStatus` per file mount: `mounted` with its file, missing and local-only counts and whether this key can write, or `pending`) and `lastError`.
  - `StoppedStatus` is what a client in another process knows of a host that isn't running: its name, database, catalog, and whether it was created here.
- **`ClientEvent`** is a rejected local write, or a concurrent change that voided or reinstated one. It has the same fields as the projection's op-event log, plus its id.

There are three implementations:

- in process, `host.client` in rhost;
- over `run/rhost.sock`, [rhost_client_node](../rhost_client_node);
- a browser transport, in step 7.

## Protocol

One JSON object per line. It is read-only: it carries the host's live state, and nothing that signs.

```
client -> host   { id, method: 'status' }
                 { id, method: 'watchStatus' }
                 { id, method: 'unwatch', watch: <id of the watchStatus request> }
host -> client   { id, result }   { id, error }   { id, event }   (a push on a watch)
```

- `encodeMessage`, `decodeRequest` and `decodeServerMessage` encode and parse messages.
- `LineDecoder` splits a stream of text chunks into lines.
- `socketPathFor(hostDir, platform)` is where a running host listens: `run/rhost.sock` under its folder (`SOCKET_FILE`), or a named pipe on Windows.

## Test

```
npm test
```

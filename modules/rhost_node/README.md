# rhost_node

The Node platform for [rhost](../rhost): an app folder, with a SQLite replica and a SQLite projection per host, the keystore, the Node mesh, a lock file, and the `run/rhost.sock` server.

```
my-app/
  app.json  catalogs/
  hosts/<name>/
    host.json          the database, catalog, key, network settings, and any overrides
    rdb/replica.rdb    the replica
    db/data.sqlite     the projection (projection.path; this is rhost init's default)
    files/<mount>/     one folder per projection.files entry, at its path
    run/               rhost.lock, rhost.sock and rhost.log
```

Each file is opened at a path under the host folder, and its parent folder is created if it's missing, so a configured path such as `custom/app.sqlite` needs no setup.

Keys stay in the user's keystore: `RDB_KEYSTORE`, else `$RDB_HOME/keys.json`, else `~/.rdb/keys.json`, the same file `rdb` and `rkeys` use. An app that keeps its own sets `keystore` in `app.json`, a path relative to the app folder. The app folder holds no keys of its own.

## Library

```ts
import { openApp } from '@hyper-hyper-space/hhs3_rhost_node';

const app = await openApp('./my-app', { passphrase: async (key) => '...' });
await app.create('default', { key: 'me', passphrase: 'env:EDITOR_PASS', sync: { scope: 'internet' } });
await app.start();
```

- **`openApp(dir, options?)`** reads `app.json` (`readAppConfig`) and opens an `Rhost` on `nodePlatform(dir)`, with `app.json`'s `keystore`. The options are:
  - `keystore`, which overrides `app.json`'s;
  - `passphrase(key)`, which overrides every passphrase source; it gets the host's key as `host.json` records it;
  - `passphraseStdin`, which reads the passphrase from stdin once;
  - a `prompter`, for `key.passphrase: "prompt"`;
  - a `meshFactory`, `env`, and `report`.
- **`initApp(dir, flags, prompter?)`** is `rhost init`. It asks for each param the releases declare (an identity defaults to `$me`), refuses names they don't declare, and writes `app.json` and `catalogs/`. It writes no keys.
- **`chooseKey`, `chooseScope` and `askParam`** are the prompts `rhost create`, `join` and `deploy` share with it. `chooseKey` lists the keystore's keys and offers "Create new key pair". `scriptedPrompter` and `NON_INTERACTIVE` drive them without a terminal.
- **`nodePlatform(dir, options?)`** is the `HostPlatform`:
  - it publishes host folders by renaming a pending folder into place;
  - each host gets a `SqliteDagDb` on its `rdb/replica.rdb` and a `SqliteTarget` with capture on;
  - each file mount gets a [`NodeDirectory`](../rdb_files_node) at its path in the host folder;
  - the app has one `KeyStore`, on `appKeystorePath(dir, keystore)`, which is also its `keystoreLocation`;
  - passphrases come from `env:<VAR>`, stdin, or the prompter, which asks once per key id; `rememberPassphrase(keyId, passphrase)` supplies one the caller already has, such as a new key's;
  - `acquireLock` uses `run/rhost.lock`, holding the pid; a stale pid is taken over;
  - `listen` serves `run/rhost.sock`.
- **`run/rhost.sock`** is a Unix socket under the host folder, mode `0600`, answering rhost's read-only client protocol. It is created when the host starts and removed when it stops; a stale file is replaced, since the lock is held. On Windows the same path maps to a named pipe (untested). A socket path longer than the system's limit (103 bytes on macOS, 107 on Linux) is refused with the limit named. `socketPath(dir, name)` gives the path.
- Also here, and used by rdb_tools:
  - `KeyStore`, `defaultKeystorePath()` and the identity encoders;
  - `createNodeSyncMeshFactory()`;
  - `acquireLock` and `lockHolder`;
  - `hostDir(dir, name)`, `checkHostName(name)`, and `readLock(dir, name)`, which returns the live pid serving a host.

## Test

```
npm test
```

The tests keep their keys in temporary keystores, named by `app.json`'s `keystore` or `RDB_KEYSTORE`, never in `~/.rdb`. The socket test binds a Unix socket, which a sandbox that blocks socket binds rejects with `EPERM`.

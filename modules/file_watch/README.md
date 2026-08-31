# file_watch

A tiny, dependency-free, kernel-driven file change watcher for HHSv3.

`watchFile(filePath, notify)` observes writes to a single file via `fs.watch`, letting the process sleep when idle instead of interval polling. It watches the file directly **and** its parent directory, so it survives files that are deleted and recreated rather than modified in place (e.g. a SQLite WAL under `journal_mode=WAL` truncate mode): when the file reappears, the direct watcher is rearmed.

## Contract

- **Over-notify / at-least-once.** `notify` means "something may have changed, come look." A spurious call is harmless; callers must re-read authoritatively.
- **`close()` is idempotent.** Both the file and directory watchers are torn down.
- **Optimization, not a guarantee.** `fs.watch` reliability varies by platform and filesystem. Callers that need a universal guarantee (in-memory stores, network filesystems) should keep a polling fallback and use this only for local files.

## Usage

```ts
import { watchFile } from "@hyper-hyper-space/hhs3_file_watch";

const handle = watchFile("/path/to/db.sqlite-wal", () => {
    // wake up and re-read
});

// later
handle.close();
```

Consumers: [dag_sqlite](../dag_sqlite)'s `WatcherSqliteDagStore` (WAL wake-up for external DAG writes) and [rdb_adapter_sqlite](../rdb_adapter_sqlite)'s `SqliteTarget` (WAL wake-up for the change-capture outbox).

## Test

```
npm test
```

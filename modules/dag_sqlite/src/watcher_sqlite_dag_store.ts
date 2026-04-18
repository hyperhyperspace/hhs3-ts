import * as fs from "node:fs";
import * as path from "node:path";
import { SqlConnection, SqlDagStore } from "@hyper-hyper-space/hhs3_dag_sql";

// Concrete SqlDagStore for Node.js + better-sqlite3: observes external writes
// by watching the SQLite WAL file via fs.watch. This is kernel-driven and lets
// the process sleep when idle, avoiding the battery cost of interval polling.
//
// Implementation notes:
//   - Watches `${dbPath}-wal` directly so writes from other processes or this
//     process are both detected.
//   - Also watches the parent directory, since the WAL file is deleted and
//     recreated on checkpoint (particularly in PRAGMA journal_mode=WAL's
//     truncate mode). When it comes back, the direct watcher is rearmed.
//   - Does not attempt to debounce or dedupe: the DagStore.addListener contract
//     is at-least-once; consumers re-read getFrontier() anyway.
//   - Local appends also fire via SqlDagStore.append(); fs.watch will often
//     fire a second time for the same change. That is expected and harmless.

type WatcherHandle = {
    walWatcher: fs.FSWatcher | undefined;
    dirWatcher: fs.FSWatcher | undefined;
    closed: boolean;
};

export class WatcherSqliteDagStore extends SqlDagStore {

    private dbPath: string;

    constructor(conn: SqlConnection, dagId: number, dbPath: string) {
        super(conn, dagId);
        this.dbPath = dbPath;
    }

    protected startExternalObserver(notify: () => void): unknown {
        const walPath = `${this.dbPath}-wal`;
        const dir = path.dirname(this.dbPath);
        const walBasename = path.basename(walPath);

        const handle: WatcherHandle = {
            walWatcher: undefined,
            dirWatcher: undefined,
            closed: false,
        };

        const armWalWatcher = () => {
            if (handle.closed) return;
            if (handle.walWatcher !== undefined) return;

            try {
                handle.walWatcher = fs.watch(walPath, () => {
                    if (handle.closed) return;
                    notify();
                });
                handle.walWatcher.on('error', () => {
                    if (handle.walWatcher !== undefined) {
                        try { handle.walWatcher.close(); } catch (_e) { /* ignore */ }
                        handle.walWatcher = undefined;
                    }
                });
            } catch (_e) {
                // WAL file may not exist yet (will appear on first write);
                // the directory watcher will rearm us when it does.
            }
        };

        try {
            handle.dirWatcher = fs.watch(dir, (_event, filename) => {
                if (handle.closed) return;
                if (filename === walBasename) {
                    if (handle.walWatcher === undefined) {
                        armWalWatcher();
                    }
                    notify();
                }
            });
            handle.dirWatcher.on('error', () => { /* ignore */ });
        } catch (_e) {
            // Directory watch failed (unusual); fall back to WAL-only watch.
        }

        armWalWatcher();

        return handle;
    }

    protected stopExternalObserver(handle: unknown): void {
        const h = handle as WatcherHandle;
        h.closed = true;
        if (h.walWatcher !== undefined) {
            try { h.walWatcher.close(); } catch (_e) { /* ignore */ }
            h.walWatcher = undefined;
        }
        if (h.dirWatcher !== undefined) {
            try { h.dirWatcher.close(); } catch (_e) { /* ignore */ }
            h.dirWatcher = undefined;
        }
    }
}

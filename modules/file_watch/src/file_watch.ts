import * as fs from "node:fs";
import * as path from "node:path";

// A kernel-driven file change watcher: observes writes to a single file via
// fs.watch, letting the process sleep when idle (no interval polling). This is
// the generic mechanism behind SQLite WAL wake-up, but it is not SQLite- or
// DAG-specific: it watches an arbitrary path and calls `notify` whenever that
// file may have changed.
//
// Implementation notes:
//   - Watches `filePath` directly so writes from any process are detected.
//   - Also watches the parent directory, since many files are deleted and
//     recreated rather than modified in place (e.g. a SQLite WAL under
//     journal_mode=WAL truncate mode). When the file reappears, the direct
//     watcher is rearmed.
//   - Does NOT debounce or dedupe: the contract is at-least-once / over-notify,
//     so callers must treat `notify` as "something may have changed, come
//     look" and re-read authoritatively. A spurious call is harmless.
//   - fs.watch reliability varies by platform / filesystem; callers that need a
//     universal guarantee (in-memory stores, network filesystems) should retain
//     a polling fallback and use this only as an optimization for local files.

export type FileWatchHandle = {
    // Idempotent: safe to call more than once.
    close(): void;
};

// Note: named to avoid shadowing node:fs `watchFile` (a distinct polling API);
// this uses the event-based fs.watch under the hood.
export function watchFile(filePath: string, notify: () => void): FileWatchHandle {
    const dir = path.dirname(filePath);
    const basename = path.basename(filePath);

    let fileWatcher: fs.FSWatcher | undefined;
    let dirWatcher: fs.FSWatcher | undefined;
    let closed = false;

    const armFileWatcher = (): void => {
        if (closed) return;
        if (fileWatcher !== undefined) return;

        try {
            fileWatcher = fs.watch(filePath, () => {
                if (closed) return;
                notify();
            });
            fileWatcher.on('error', () => {
                if (fileWatcher !== undefined) {
                    try { fileWatcher.close(); } catch (_e) { /* ignore */ }
                    fileWatcher = undefined;
                }
            });
        } catch (_e) {
            // The file may not exist yet (it will appear on first write); the
            // directory watcher will rearm us when it does.
        }
    };

    // Rebind the direct watcher onto the file's CURRENT inode. A deleted +
    // recreated file (e.g. a WAL under checkpoint truncate) leaves the old
    // watcher bound to a stale inode that no longer delivers content events, so
    // we drop it and re-arm. Called on every directory event for our basename
    // (create / delete / rename) - not on content appends, which reach the
    // direct watcher instead - so this is not hot.
    const rearmFileWatcher = (): void => {
        if (closed) return;
        if (fileWatcher !== undefined) {
            try { fileWatcher.close(); } catch (_e) { /* ignore */ }
            fileWatcher = undefined;
        }
        armFileWatcher();
    };

    try {
        dirWatcher = fs.watch(dir, (_event, filename) => {
            if (closed) return;
            if (filename === basename) {
                rearmFileWatcher();
                notify();
            }
        });
        dirWatcher.on('error', () => { /* ignore */ });
    } catch (_e) {
        // Directory watch failed (unusual); fall back to file-only watch.
    }

    armFileWatcher();

    return {
        close(): void {
            closed = true;
            if (fileWatcher !== undefined) {
                try { fileWatcher.close(); } catch (_e) { /* ignore */ }
                fileWatcher = undefined;
            }
            if (dirWatcher !== undefined) {
                try { dirWatcher.close(); } catch (_e) { /* ignore */ }
                dirWatcher = undefined;
            }
        },
    };
}

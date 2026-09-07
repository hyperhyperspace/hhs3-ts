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
    // Resolves once the direct file watcher is guaranteed registered with the
    // kernel. Kernel registration is deferred to the next event-loop poll phase
    // on some platforms (libuv's kqueue backend on Darwin appends the watcher to
    // loop->watcher_queue and only issues the kevent EV_ADD at the top of the
    // next uv__io_poll; the vnode filter is edge-triggered from that point, so a
    // write landing before it is never observed). inotify on Linux registers
    // synchronously, so awaiting this is a harmless near-instant no-op there.
    // Await it before performing a write whose wake you must observe.
    ready: Promise<void>;
};

// Note: named to avoid shadowing node:fs `watchFile` (a distinct polling API);
// this uses the event-based fs.watch under the hood.
export function watchFile(filePath: string, notify: () => void): FileWatchHandle {
    const dir = path.dirname(filePath);
    const basename = path.basename(filePath);

    let fileWatcher: fs.FSWatcher | undefined;
    let dirWatcher: fs.FSWatcher | undefined;
    let closed = false;
    // The inode the direct watcher is currently bound to (undefined when the
    // file was absent at arm time). Used to distinguish a real delete/recreate
    // from an in-place content write when a directory event arrives.
    let armedInode: number | undefined;

    const currentInode = (): number | undefined => {
        try { return fs.statSync(filePath).ino; } catch (_e) { return undefined; }
    };

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
            armedInode = currentInode();
        } catch (_e) {
            // The file may not exist yet (it will appear on first write); the
            // directory watcher will rearm us when it does.
            armedInode = undefined;
        }
    };

    // Rebind the direct watcher onto the file's CURRENT inode. A deleted +
    // recreated file (e.g. a WAL under checkpoint truncate) leaves the old
    // watcher bound to a stale inode that no longer delivers content events, so
    // we drop it and re-arm.
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
            if (filename !== basename) return;
            // Rearm the direct watcher ONLY when the file's INODE changed - i.e.
            // a genuine delete/recreate (e.g. a WAL under checkpoint truncate),
            // not an in-place content append. On Darwin, FSEvents surfaces every
            // directory event - including plain appends - as an `event` of
            // 'rename' with our basename, so the event type cannot distinguish
            // them; the inode can (an append keeps it, a recreate changes it). A
            // blind rearm on every event would close and reopen the kqueue
            // watcher on each write, reintroducing the deferred-registration
            // window each time. (A delete resets armedInode to undefined, so a
            // recreate that happens to reuse the inode number still rearms,
            // since undefined !== the reused inode.)
            if (currentInode() !== armedInode) rearmFileWatcher();
            notify();
        });
        dirWatcher.on('error', () => { /* ignore */ });
    } catch (_e) {
        // Directory watch failed (unusual); fall back to file-only watch.
    }

    armFileWatcher();

    // Two setImmediate hops guarantee the loop has passed through at least one
    // poll-phase flush (where libuv issues the deferred kevent EV_ADD)
    // regardless of which phase watchFile was called from: a single hop is
    // insufficient when called from a poll-phase I/O callback, whose
    // setImmediate runs in the very next check phase without an intervening
    // poll.
    const ready = new Promise<void>((resolve) => {
        setImmediate(() => setImmediate(() => resolve()));
    });

    return {
        ready,
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

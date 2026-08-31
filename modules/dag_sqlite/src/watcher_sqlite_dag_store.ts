import { SqlConnection, SqlDagStore } from "@hyper-hyper-space/hhs3_dag_sql";
import { watchFile, FileWatchHandle } from "@hyper-hyper-space/hhs3_file_watch";

// Concrete SqlDagStore for Node.js + better-sqlite3: observes external writes
// by watching the SQLite WAL file. This is kernel-driven and lets the process
// sleep when idle, avoiding the battery cost of interval polling.
//
// The watching mechanism itself (watch the WAL file + its parent dir, rearm
// when the WAL is deleted/recreated on checkpoint truncate) lives in the
// generic @hyper-hyper-space/hhs3_file_watch helper; this class just points it
// at `${dbPath}-wal` and forwards its at-least-once notifications to the base
// class, which reads new entries and fires listeners. Local appends also fire
// via SqlDagStore.append(); fs.watch will often fire a second time for the same
// change. That is expected and harmless (the base re-reads authoritatively).

export class WatcherSqliteDagStore extends SqlDagStore {

    private dbPath: string;

    constructor(conn: SqlConnection, dagId: number, dbPath: string) {
        super(conn, dagId);
        this.dbPath = dbPath;
    }

    protected startExternalObserver(notify: () => void): unknown {
        return watchFile(`${this.dbPath}-wal`, notify);
    }

    protected stopExternalObserver(handle: unknown): void {
        (handle as FileWatchHandle).close();
    }
}

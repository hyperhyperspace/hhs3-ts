import { SqlConnection } from "./sql_connection.js";
import { SqlDagStore } from "./sql_dag_store.js";

const DEFAULT_INTERVAL_MS = 1000;

// Generic SQL-backed DagStore that detects external writes by polling
// MAX(rowid) on the entries table for its dagId. Works on any SqlConnection
// (including browser/Capacitor SQL plugins). Use a more specific subclass
// when a lower-overhead notification mechanism is available (e.g. fs.watch
// on the WAL file for better-sqlite3).

export class PollingSqlDagStore extends SqlDagStore {

    private intervalMs: number;

    constructor(conn: SqlConnection, dagId: number, intervalMs: number = DEFAULT_INTERVAL_MS) {
        super(conn, dagId);
        this.intervalMs = intervalMs;
    }

    protected startExternalObserver(notify: () => void): unknown {
        let lastMax = -1;
        let running = false;

        const tick = async () => {
            if (running) return;
            running = true;
            try {
                const rows = await this.conn.query(
                    `SELECT COALESCE(MAX(rowid), 0) AS m FROM entries WHERE dag_id = ?`,
                    [this.dagId]
                );
                const m = Number(rows[0]?.m ?? 0);
                if (lastMax === -1) {
                    lastMax = m;
                } else if (m > lastMax) {
                    lastMax = m;
                    notify();
                }
            } catch (_e) {
                // ignore transient errors; try again next tick
            } finally {
                running = false;
            }
        };

        return setInterval(tick, this.intervalMs);
    }

    protected stopExternalObserver(handle: unknown): void {
        clearInterval(handle as ReturnType<typeof setInterval>);
    }
}

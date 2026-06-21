import Database from "better-sqlite3";
import { SqlConnection, SqlRow } from "@hyper-hyper-space/hhs3_dag_sql";

export type SqliteHandle = {
    conn: SqlConnection;
    close(): void;
};

export function openSqliteConnection(path: string): SqliteHandle {
    const db = new Database(path);
    db.pragma('journal_mode = WAL');
    db.pragma('busy_timeout = 5000');

    let transactionQueue: Promise<unknown> = Promise.resolve();

    async function runTransaction<T>(fn: (conn: SqlConnection) => Promise<T>): Promise<T> {
        let began = false;
        try {
            db.exec("BEGIN IMMEDIATE");
            began = true;
            const result = await fn(makeConn());
            db.exec("COMMIT");
            began = false;
            return result;
        } catch (e) {
            if (began) {
                try { db.exec("ROLLBACK"); } catch (_rollbackError) { /* ignore rollback failure */ }
            }
            throw e;
        }
    }

    function makeConn(): SqlConnection {
        return {
            query(sql: string, params: unknown[] = []): Promise<SqlRow[]> {
                return Promise.resolve(db.prepare(sql).all(...params) as SqlRow[]);
            },
            execute(sql: string, params: unknown[] = []): Promise<number> {
                return Promise.resolve(db.prepare(sql).run(...params).changes);
            },
            transaction<T>(fn: (conn: SqlConnection) => Promise<T>): Promise<T> {
                const run = transactionQueue.then(() => runTransaction(fn));
                transactionQueue = run.catch(() => undefined);
                return run;
            }
        };
    }

    return { conn: makeConn(), close: () => db.close() };
}

import Database from "better-sqlite3";
import { SqlConnection, SqlRow } from "../src/sql_connection.js";

export function createSqliteConnection(path: string): SqlConnection {
    const db = new Database(path);
    db.pragma('journal_mode = WAL');

    // Transactions on the connection run one at a time, as dag_sqlite's do
    // (dag_sqlite/src/sqlite_connection.ts): the SQL store relies on it.
    let transactionQueue: Promise<unknown> = Promise.resolve();

    async function runTransaction<T>(fn: (conn: SqlConnection) => Promise<T>): Promise<T> {
        db.exec("BEGIN");
        try {
            const result = await fn(makeConn());
            db.exec("COMMIT");
            return result;
        } catch (e) {
            db.exec("ROLLBACK");
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

    return makeConn();
}

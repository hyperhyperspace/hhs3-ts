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

    function makeConn(): SqlConnection {
        return {
            query(sql: string, params: unknown[] = []): Promise<SqlRow[]> {
                return Promise.resolve(db.prepare(sql).all(...params) as SqlRow[]);
            },
            execute(sql: string, params: unknown[] = []): Promise<number> {
                return Promise.resolve(db.prepare(sql).run(...params).changes);
            },
            async transaction<T>(fn: (conn: SqlConnection) => Promise<T>): Promise<T> {
                db.exec("BEGIN IMMEDIATE");
                try {
                    const result = await fn(makeConn());
                    db.exec("COMMIT");
                    return result;
                } catch (e) {
                    db.exec("ROLLBACK");
                    throw e;
                }
            }
        };
    }

    return { conn: makeConn(), close: () => db.close() };
}

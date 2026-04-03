import Database from "better-sqlite3";
import { SqlConnection, SqlRow } from "../src/sql_connection";

export function createSqliteConnection(path: string): SqlConnection {
    const db = new Database(path);
    db.pragma('journal_mode = WAL');

    function makeConn(): SqlConnection {
        return {
            query(sql: string, params: unknown[] = []): Promise<SqlRow[]> {
                return Promise.resolve(db.prepare(sql).all(...params) as SqlRow[]);
            },
            execute(sql: string, params: unknown[] = []): Promise<number> {
                return Promise.resolve(db.prepare(sql).run(...params).changes);
            },
            async transaction<T>(fn: (conn: SqlConnection) => Promise<T>): Promise<T> {
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
        };
    }

    return makeConn();
}

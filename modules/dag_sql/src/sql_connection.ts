export type SqlRow = Record<string, unknown>;

export type SqlConnection = {
    query(sql: string, params?: unknown[]): Promise<SqlRow[]>;
    execute(sql: string, params?: unknown[]): Promise<number>;
    transaction<T>(fn: (conn: SqlConnection) => Promise<T>): Promise<T>;
};

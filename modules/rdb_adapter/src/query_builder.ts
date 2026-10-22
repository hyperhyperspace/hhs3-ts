// Immutable single-table query descriptors. A ColumnSetRef parameter is a
// dependency edge, not a literal, so it stays out of the fingerprint.

import type { json } from "@hyper-hyper-space/hhs3_json";

import type { BoundWhereExpr, CursorBound, OrderBySpec, OrderDirection, ProjectedRow, SingleTableQueryPlan } from "./types.js";
import { bindWhere, parseWhere, type BoundParam, type WhereTemplate } from "./where_parser.js";

export type ParamValue = json.Literal | null | Array<json.Literal | null> | Set<json.Literal | null> | ColumnSetRef;

export class ColumnSetRef {
    constructor(readonly query: TableQuery, readonly column: string) {}
}

export type QuerySource = {
    selectFrom(table: string): TableQuery;
    monitor(query: TableQuery | { [name: string]: TableQuery }): QuerySubscription<ProjectedRow[] | { [name: string]: ProjectedRow[] }>;
};

export type QuerySnapshot<T> = {
    data: T;
    events: import("./types.js").OpEvent[];
    loading: boolean;
    error?: unknown;
};

export interface QuerySubscription<T> {
    getSnapshot(): QuerySnapshot<T>;
    subscribe(listener: () => void): () => void;
    whenReady(): Promise<QuerySnapshot<T>>;
}

type Fields = {
    select?: string[];
    whereText?: string;
    whereParams: { [name: string]: ParamValue };
    orderBy: OrderBySpec[];
    cursor?: { side: 'before' | 'after'; id: number };
    limit?: number;
};

export class TableQuery {
    constructor(
        readonly source: QuerySource,
        readonly table: string,
        private readonly fields: Fields = { whereParams: {}, orderBy: [] },
    ) {}

    select(...columns: string[]): TableQuery {
        return this.copy({ select: [...columns] });
    }

    where(expr: string, params: { [name: string]: ParamValue } = {}): TableQuery {
        return this.copy({ whereText: expr, whereParams: params });
    }

    orderBy(column: string, direction: OrderDirection = 'asc'): TableQuery {
        return this.copy({ orderBy: [...this.fields.orderBy, { column, direction }] });
    }

    before(id: number): TableQuery {
        if (this.fields.cursor !== undefined) throw new Error('a query has one of BEFORE or AFTER');
        return this.copy({ cursor: { side: 'before', id } });
    }

    after(id: number): TableQuery {
        if (this.fields.cursor !== undefined) throw new Error('a query has one of BEFORE or AFTER');
        return this.copy({ cursor: { side: 'after', id } });
    }

    limit(count: number): TableQuery {
        return this.copy({ limit: count });
    }

    col(column: string): ColumnSetRef {
        return new ColumnSetRef(this, column);
    }

    results(): Promise<ProjectedRow[]> {
        return this.source.monitor(this).whenReady().then((snapshot) => {
            if (snapshot.error !== undefined) throw snapshot.error;
            return snapshot.data as ProjectedRow[];
        });
    }

    template(): WhereTemplate | undefined {
        return this.fields.whereText === undefined ? undefined : parseWhere(this.fields.whereText);
    }

    dependencies(): ColumnSetRef[] {
        return Object.values(this.fields.whereParams).filter((value): value is ColumnSetRef => value instanceof ColumnSetRef);
    }

    fingerprint(): string {
        const literals: { [name: string]: json.Literal | null | Array<json.Literal | null> } = {};
        const refs: { [name: string]: string } = {};
        for (const [name, value] of Object.entries(this.fields.whereParams)) {
            if (value instanceof ColumnSetRef) refs[name] = `${value.query.fingerprint()}#${value.column}`;
            else if (value instanceof Set) literals[name] = [...value].sort(compareLiteral);
            else if (Array.isArray(value)) literals[name] = [...value].sort(compareLiteral);
            else literals[name] = value;
        }
        return JSON.stringify({
            table: this.table,
            select: this.fields.select,
            where: this.fields.whereText ?? null,
            literals,
            refs,
            orderBy: this.fields.orderBy,
            cursor: this.fields.cursor ?? null,
            limit: this.fields.limit ?? null,
        });
    }

    plan(keySets: Map<ColumnSetRef, Array<json.Literal | null>>, idColumn: string): SingleTableQueryPlan {
        const values: { [name: string]: BoundParam } = {};
        for (const [name, value] of Object.entries(this.fields.whereParams)) {
            if (value instanceof ColumnSetRef) values[name] = keySets.get(value) ?? [];
            else if (value instanceof Set) values[name] = [...value];
            else values[name] = value;
        }
        const where = this.template() === undefined ? undefined : bindWhere(this.template()!, values);
        const exported = this.dependencies().length > 0 || this.exportedColumns().length > 0;
        const select = this.fields.select === undefined ? undefined : unique([
            idColumn,
            ...this.fields.select,
            ...this.fields.orderBy.map((item) => item.column),
            ...this.exportedColumns(),
            ...(exported ? [] : []),
        ]);
        const plan: SingleTableQueryPlan = { table: this.table };
        if (select !== undefined && this.fields.select !== undefined) plan.select = select;
        if (where !== undefined) plan.where = where;
        if (this.fields.orderBy.length > 0) plan.orderBy = this.fields.orderBy;
        if (this.fields.cursor !== undefined) plan.cursor = this.fields.cursor;
        if (this.fields.limit !== undefined) plan.limit = this.fields.limit;
        return plan;
    }

    exportedColumns(): string[] {
        return [];
    }

    cursorBound(cached?: { [column: string]: json.Literal | null }): CursorBound | undefined {
        if (this.fields.cursor === undefined) return undefined;
        const bound: CursorBound = { ...this.fields.cursor };
        if (cached !== undefined) bound.cachedValues = cached;
        return bound;
    }

    whereExpr(keySets: Map<ColumnSetRef, Array<json.Literal | null>>): BoundWhereExpr | undefined {
        return this.plan(keySets, 'id').where;
    }

    private copy(patch: Partial<Fields>): TableQuery {
        return new TableQuery(this.source, this.table, { ...this.fields, ...patch });
    }
}

function unique(values: string[]): string[] {
    return [...new Set(values)];
}

function compareLiteral(a: json.Literal | null, b: json.Literal | null): number {
    return JSON.stringify(a) < JSON.stringify(b) ? -1 : JSON.stringify(a) > JSON.stringify(b) ? 1 : 0;
}

export class QuerySourceImpl implements QuerySource {
    constructor(private readonly engine: {
        selectFrom(table: string): TableQuery;
        monitor(query: TableQuery | { [name: string]: TableQuery }): QuerySubscription<ProjectedRow[] | { [name: string]: ProjectedRow[] }>;
    }) {}

    selectFrom(table: string): TableQuery {
        return this.engine.selectFrom(table);
    }

    monitor(query: TableQuery | { [name: string]: TableQuery }): QuerySubscription<ProjectedRow[] | { [name: string]: ProjectedRow[] }> {
        return this.engine.monitor(query);
    }
}

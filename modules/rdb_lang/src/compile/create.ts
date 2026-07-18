import type { json } from "@hyper-hyper-space/hhs3_json";
import {
    ColumnConstraints, ColumnDef, CreateRDbPayload, CreateRSchemaPayload, CreateTableGroupPayload, FKs,
    InsertRowPayload, Predicate, RDbImpl, RSchemaImpl, RTableGroupImpl, Restriction, TableDef,
    deriveRowId, normalizeBigint, normalizeDecimal,
} from "@hyper-hyper-space/hhs3_rdb";

import type { ColumnDecl, ColumnTypeName, TableDecl, ValueExpr } from "../syntax/ast.js";
import { canonicalEncodeValue } from "../bind/values.js";
import type { BoundCreateDatabase, BoundCreateSchema, BoundCreateStatement, BoundCreateTableGroup } from "../bind/bind.js";
import {
    columnSetFromTableDecl,
    columnsOfFromTableDecls,
    columnsOfFromSchemaView,
    type RuleScope,
} from "./rule_scope.js";
import { lowerRestrictionPredicate } from "./query.js";

export type CreatePlan =
    | { kind: 'create-database'; name: string; payload: CreateRDbPayload }
    | { kind: 'create-schema'; name: string; payload: CreateRSchemaPayload }
    | { kind: 'create-tablegroup'; name: string; payload: CreateTableGroupPayload };

export async function compileCreate(bound: BoundCreateStatement): Promise<CreatePlan> {
    if (bound.kind === 'create-database') return compileCreateDatabase(bound);
    if (bound.kind === 'create-schema') return compileCreateSchema(bound);
    return compileCreateTableGroup(bound);
}

async function compileCreateDatabase(bound: BoundCreateDatabase): Promise<CreatePlan> {
    const payload = await RDbImpl.create({
        seed: bound.seed,
        name: bound.ast.name,
        ...(bound.creators.length > 0 ? { creators: bound.creators } : {}),
    });
    return { kind: 'create-database', name: bound.ast.name, payload };
}

async function compileCreateSchema(bound: BoundCreateSchema): Promise<CreatePlan> {
    const columnsOf = columnsOfFromTableDecls(bound.ast.tables);
    const payload = await RSchemaImpl.create({
        name: bound.ast.name,
        creators: bound.creators,
        tables: bound.ast.tables.map((table) => compileTable(table, {
            gated: { name: table.name, columns: columnSetFromTableDecl(table) },
            columnsOf,
        })),
    });
    return { kind: 'create-schema', name: bound.ast.name, payload };
}

async function compileCreateTableGroup(bound: BoundCreateTableGroup): Promise<CreatePlan> {
    const initialRows: { [table: string]: json.Literal[] } = {};
    for (const row of bound.initialRows) {
        const payload: InsertRowPayload = {
            action: 'insert',
            rowId: deriveRowId(row.uuid),
            uuid: row.uuid,
            values: row.values,
        };
        if (initialRows[row.table] === undefined) initialRows[row.table] = [];
        initialRows[row.table].push(payload as unknown as json.Literal);
    }

    let columnsOf = columnsOfFromTableDecls([]);
    if (bound.schema.schema !== undefined) {
        const view = await bound.schema.schema.getView(bound.schemaVersion, bound.schemaVersion);
        columnsOf = columnsOfFromSchemaView(view);
    }
    const gateScope: RuleScope = { columnsOf };

    const canObserve: { [binding: string]: Predicate } = {};
    for (const clause of bound.ast.canObserve) {
        canObserve[clause.binding] = lowerRestrictionPredicate(clause.predicate, gateScope);
    }

    const payload = await RTableGroupImpl.create({
        name: bound.ast.name,
        seed: bound.seed,
        schemaRef: bound.schema.id,
        schemaVersion: bound.schemaVersion,
        ...(Object.keys(bound.bindings).length > 0 ? { bindings: bound.bindings } : {}),
        ...(bound.ast.idProvider !== undefined ? { idProvider: bound.ast.idProvider } : {}),
        ...(bound.ast.canDeploy !== undefined ? { canDeploy: lowerRestrictionPredicate(bound.ast.canDeploy, gateScope) } : {}),
        ...(bound.ast.canObserve.length > 0 ? { canObserve } : {}),
        ...(Object.keys(initialRows).length > 0 ? { initialRows } : {}),
    });

    return { kind: 'create-tablegroup', name: bound.ast.name, payload };
}

export function compileTable(table: TableDecl, scope?: RuleScope): TableDef {
    const columns: { [column: string]: ColumnDef } = {};
    const fks: FKs = {};
    const restrictions: Restriction[] = [];
    let concurrentDeletes: boolean | undefined;
    let idProvider: TableDef['idProvider'];

    for (const column of table.columns) {
        columns[column.name] = compileColumn(column);
        if (column.references !== undefined) fks[column.name] = column.references;
    }

    const ruleScope: RuleScope = scope ?? {
        gated: { name: table.name, columns: columnSetFromTableDecl(table) },
        columnsOf: columnsOfFromTableDecls([table]),
    };

    for (const option of table.options) {
        switch (option.kind) {
            case 'concurrent-deletes':
                concurrentDeletes = option.value;
                break;
            case 'identity-provider':
                idProvider = { keyIdColumn: option.keyIdColumn, publicKeyColumn: option.publicKeyColumn };
                break;
            case 'allow-rule':
                restrictions.push({ on: option.op, rule: lowerRestrictionPredicate(option.predicate, ruleScope) });
                break;
        }
    }

    const def: TableDef = { name: table.name, columns };
    if (Object.keys(fks).length > 0) def.fks = fks;
    if (restrictions.length > 0) def.restrictions = restrictions;
    if (concurrentDeletes !== undefined) def.concurrentDeletes = concurrentDeletes;
    if (idProvider !== undefined) def.idProvider = idProvider;
    return def;
}

export function compileColumn(column: ColumnDecl): ColumnDef {
    const def: ColumnDef = { type: column.type };
    if (column.nullable) def.nullable = true;

    const constraints = compileColumnConstraints(column);
    if (constraints !== undefined) def.constraints = constraints;

    if (column.defaultValue !== undefined) {
        if (column.defaultValue.kind !== 'literal') throw new Error('column DEFAULT must be a literal');
        if (column.defaultValue.value === null) throw new Error('column DEFAULT NULL is not supported by RDb json.Literal payloads');
        def.default = canonicalEncodeValue(column.defaultValue.value, def);
    }
    if (column.pub) def.pub = true;
    if (column.readonly) def.readonly = true;
    return def;
}

// Map the parsed constraint expression to the payload ColumnConstraints,
// canonically encoding MIN / MAX bounds against the column type. Per-type
// applicability (e.g. MIN on a string column) is enforced by validateColumnDef
// in rdb core; here we only produce the canonical shapes.
function compileColumnConstraints(column: ColumnDecl): ColumnConstraints | undefined {
    const c = column.constraints;
    if (c === undefined) return undefined;
    const out: ColumnConstraints = {};
    if (c.maxLength !== undefined) out.maxLength = c.maxLength;
    if (c.precision !== undefined) out.precision = c.precision;
    if (c.scale !== undefined) out.scale = c.scale;
    if (c.min !== undefined) out.min = encodeBound(c.min, column.type, c.scale, 'MIN');
    if (c.max !== undefined) out.max = encodeBound(c.max, column.type, c.scale, 'MAX');
    return out;
}

function encodeBound(expr: ValueExpr, type: ColumnTypeName, scale: number | undefined, which: string): string {
    if (expr.kind !== 'literal') throw new Error(`column ${which} must be a literal`);
    const v = expr.value;
    if (type === 'integer' || type === 'bigint') {
        if (typeof v !== 'string' && typeof v !== 'number') throw new Error(`${which} must be an integer bound`);
        const s = normalizeBigint(v);
        if (s === undefined) throw new Error(`${which} '${String(v)}' is not a valid integer bound`);
        return s;
    }
    if (type === 'decimal') {
        if (scale === undefined) throw new Error('DECIMAL column requires (precision, scale)');
        if (typeof v !== 'string' && typeof v !== 'number') throw new Error(`${which} must be a numeric bound`);
        const s = normalizeDecimal(v, scale);
        if (s === undefined) throw new Error(`${which} '${String(v)}' is not a valid decimal bound at scale ${scale}`);
        return s;
    }
    // Inapplicable type: keep a string form so validateColumnDef rejects it as
    // a non-applicable constraint (anti-fungibility) with a clear message.
    return typeof v === 'string' ? v : String(v);
}

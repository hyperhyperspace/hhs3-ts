import type { RObject } from "@hyper-hyper-space/hhs3_mvt";
import {
    CatalogUpdateResult, ColumnConstraints, ColumnDef, CreateRCatalogPayload, CreateRDbPayload, CreateRSchemaPayload, FKs,
    RCatalogImpl, RDbImpl, RSchemaImpl, Restriction, TableDef,
    deployCatalogRelease, normalizeBigint, normalizeDecimal,
} from "@hyper-hyper-space/hhs3_rdb";

import type { ColumnDecl, ColumnTypeName, TableDecl, ValueExpr } from "../syntax/ast.js";
import { canonicalEncodeValue } from "../bind/values.js";
import type { BoundCreateCatalog, BoundCreateDatabase, BoundCreateSchema, BoundCreateStatement } from "../bind/bind.js";
import {
    columnSetFromTableDecl,
    columnsOfFromTableDecls,
    type RuleScope,
} from "./rule_scope.js";
import { lowerRestrictionPredicate } from "./query.js";

// The host creates the root object from `payload`, then runs `afterCreate`
// on it when present.
export type CreatePlan =
    | {
        kind: 'create-database';
        name: string;
        payload: CreateRDbPayload;
        // Creates the member groups and their gates, and deploys members
        // whose release version is above their pin.
        afterCreate: (object: RObject) => Promise<CatalogUpdateResult>;
    }
    | { kind: 'create-schema'; name: string; payload: CreateRSchemaPayload }
    | { kind: 'create-catalog'; name: string; payload: CreateRCatalogPayload };

export async function compileCreate(bound: BoundCreateStatement): Promise<CreatePlan> {
    if (bound.kind === 'create-database') return compileCreateDatabase(bound);
    if (bound.kind === 'create-schema') return compileCreateSchema(bound);
    return compileCreateCatalog(bound);
}

async function compileCreateDatabase(bound: BoundCreateDatabase): Promise<CreatePlan> {
    const payload = await RDbImpl.create({
        seed: bound.seed,
        name: bound.ast.name,
        catalog: bound.catalog.id,
        release: bound.release,
        ...(Object.keys(bound.params).length > 0 ? { params: bound.params } : {}),
        ...(bound.creators.length > 0 ? { creators: bound.creators } : {}),
        ...(bound.ast.hashAlgorithm !== undefined ? { hashAlgorithm: bound.ast.hashAlgorithm } : {}),
    });
    const release = bound.release;
    const author = bound.author;
    return {
        kind: 'create-database',
        name: bound.ast.name,
        payload,
        afterCreate: (object) => deployCatalogRelease(object as RDbImpl, { release, ...(author !== undefined ? { author } : {}) }),
    };
}

async function compileCreateCatalog(bound: BoundCreateCatalog): Promise<CreatePlan> {
    const payload = await RCatalogImpl.create({
        name: bound.ast.name,
        creators: bound.creators,
        author: bound.author,
        version: bound.version,
        ...(bound.add.length > 0 ? { add: bound.add } : {}),
        ...(bound.files.length > 0 ? { files: bound.files } : {}),
        ...(bound.params.length > 0 ? { params: bound.params } : {}),
        ...(bound.ast.note !== undefined ? { note: bound.ast.note } : {}),
        ...(bound.ast.seed !== undefined ? { seed: bound.ast.seed } : {}),
        ...(bound.ast.hashAlgorithm !== undefined ? { hashAlgorithm: bound.ast.hashAlgorithm } : {}),
    });
    return { kind: 'create-catalog', name: bound.ast.name, payload };
}

async function compileCreateSchema(bound: BoundCreateSchema): Promise<CreatePlan> {
    const columnsOf = columnsOfFromTableDecls(bound.ast.tables);
    const payload = await RSchemaImpl.create({
        name: bound.ast.name,
        version: bound.version,
        creators: bound.creators,
        tables: bound.ast.tables.map((table) => compileTable(table, {
            gated: { name: table.name, columns: columnSetFromTableDecl(table) },
            columnsOf,
        })),
        ...(bound.ast.hashAlgorithm !== undefined ? { hashAlgorithm: bound.ast.hashAlgorithm } : {}),
    });
    return { kind: 'create-schema', name: bound.ast.name, payload };
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

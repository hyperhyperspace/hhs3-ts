import type { B64Hash, KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { json } from "@hyper-hyper-space/hhs3_json";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import { deriveRowId } from "@hyper-hyper-space/hhs3_rdb";
import type { ColumnDef, InsertRowPayload, MigrationRule, RowOpPayload, RowQuery } from "@hyper-hyper-space/hhs3_rdb";

import { DiagnosticBag, err, ok, Result } from "../diagnostics.js";
import type {
    AddMemberStatement, AlterSchemaStatement, AstStatement, AuthorExpr, BundleStatement, BundleWriteStatement,
    CreateDatabaseStatement, CreateSchemaStatement, CreateTableGroupStatement,
    DeleteStatement, InsertStatement, LogStatement,
    NameOrHashRef, SelectStatement, SetViewStatement, TableRef, UpdateRefStatement, UpdateSchemaStatement, UpdateStatement,
    ValueExpr,
} from "../syntax/ast.js";
import { compileMigrationRules } from "../compile/ddl.js";
import { buildAlterColumnsOf } from "../compile/rule_scope.js";
import { lowerSelectQuery } from "../compile/query.js";
import type {
    LangBindContext, LangValue, ResolvedDatabaseRef, ResolvedGroupRef, ResolvedLogTarget, ResolvedSchemaRef,
    ResolvedTableRef, VersionScope,
} from "./context.js";
import { asIdentity, asJsonLiteral, asKeyId, canonicalEncodeRowValues, resolveCreator, resolveValue } from "./values.js";

/** Reserved INSERT / WITH ROWS pseudo-column; not a schema column. */
export const PSEUDO_COLUMN_UUID = 'uuid';

export type BoundStatement =
    | BoundCreateDatabase
    | BoundCreateSchema
    | BoundCreateTableGroup
    | BoundAddMember
    | BoundAlterSchema
    | BoundUpdateSchema
    | BoundUpdateRef
    | BoundInsert
    | BoundUpdate
    | BoundDelete
    | BoundBundle
    | BoundSetView
    | BoundSelect
    | BoundLog;

export type BoundCreateStatement = BoundCreateDatabase | BoundCreateSchema | BoundCreateTableGroup;
export type BoundExecutableStatement =
    | BoundAddMember
    | BoundAlterSchema
    | BoundUpdateSchema
    | BoundUpdateRef
    | BoundInsert
    | BoundUpdate
    | BoundDelete
    | BoundBundle
    | BoundSetView
    | BoundSelect
    | BoundLog;

export type BoundCreateDatabase = {
    kind: 'create-database';
    ast: CreateDatabaseStatement;
    seed: string;
    creators: { keyId: KeyId; publicKey: PublicKey }[];
};

export type BoundCreateSchema = {
    kind: 'create-schema';
    ast: CreateSchemaStatement;
    creators: { keyId: KeyId; publicKey: PublicKey }[];
};

export type BoundCreateTableGroup = {
    kind: 'create-tablegroup';
    ast: CreateTableGroupStatement;
    seed: string;
    schema: ResolvedSchemaRef;
    schemaVersion: Version;
    bindings: { [name: string]: B64Hash };
    initialRows: BoundInitialRow[];
};

export type BoundInitialRow = {
    table: string;
    uuid: string;
    values: { [column: string]: json.Literal };
};

export type BoundAddMember = {
    kind: 'add-member';
    ast: AddMemberStatement;
    member: 'schema' | 'tablegroup';
    database: ResolvedDatabaseRef;
    memberId: B64Hash;
    note?: string;
    author?: OwnIdentity;
    at: Version;
};

export type BoundInsert = {
    kind: 'insert';
    ast: InsertStatement;
    table: ResolvedTableRef;
    values: { [column: string]: json.Literal };
    author?: OwnIdentity;
    at: Version;
    uuid: string;
};

export type BoundUpdate = {
    kind: 'update';
    ast: UpdateStatement;
    table: ResolvedTableRef;
    values: { [column: string]: json.Literal };
    rowId: B64Hash;
    author?: OwnIdentity;
    at: Version;
};

export type BoundDelete = {
    kind: 'delete';
    ast: DeleteStatement;
    table: ResolvedTableRef;
    rowId: B64Hash;
    author?: OwnIdentity;
    at: Version;
};

export type BoundBundle = {
    kind: 'bundle';
    ast: BundleStatement;
    group: ResolvedGroupRef;
    writes: BoundBundleWrite[];
    author?: OwnIdentity;
    at: Version;
};

export type BoundBundleWrite = {
    table: string;
    op: RowOpPayload;
};

export type BoundSetView = {
    kind: 'set-view';
    ast: SetViewStatement;
    at: SetViewStatement['at'];
    from?: SetViewStatement['from'];
};

export type BoundAlterSchema = {
    kind: 'alter-schema';
    ast: AlterSchemaStatement;
    schema: ResolvedSchemaRef;
    rules: MigrationRule[];
    author: OwnIdentity;
    at: Version;
};

export type BoundUpdateSchema = {
    kind: 'update-schema';
    ast: UpdateSchemaStatement;
    group: ResolvedGroupRef;
    version: Version;
    author?: OwnIdentity;
    at: Version;
};

export type BoundUpdateRef = {
    kind: 'update-ref';
    ast: UpdateRefStatement;
    group: ResolvedGroupRef;
    ref: string;
    version: Version;
    author: OwnIdentity | undefined;
    at: Version;
};

export type BoundSelect = {
    kind: 'select';
    ast: SelectStatement;
    table: ResolvedTableRef;
    query: RowQuery;
    at: Version;
    from: Version;
};

export type BoundLog = {
    kind: 'log';
    ast: LogStatement;
    target: ResolvedLogTarget;
    at: Version;
    from: Version;
    explain: boolean;
};

export async function bind(statement: AstStatement, context: LangBindContext): Promise<Result<BoundStatement>> {
    const diagnostics = new DiagnosticBag();
    try {
        switch (statement.kind) {
            case 'create-database':
                return ok(await bindCreateDatabase(statement, context));
            case 'create-schema':
                return ok(await bindCreateSchema(statement, context));
            case 'create-tablegroup':
                return ok(await bindCreateTableGroup(statement, context));
            case 'add-member':
                return ok(await bindAddMember(statement, context));
            case 'alter-schema':
                return ok(await bindAlterSchema(statement, context));
            case 'update-schema':
                return ok(await bindUpdateSchema(statement, context));
            case 'update-ref':
                return ok(await bindUpdateRef(statement, context));
            case 'insert':
                return ok(await bindInsert(statement, context));
            case 'update':
                return ok(await bindUpdate(statement, context));
            case 'delete':
                return ok(await bindDelete(statement, context));
            case 'bundle':
                return ok(await bindBundle(statement, context));
            case 'set-view':
                return ok(bindSetView(statement));
            case 'select':
                return ok(await bindSelect(statement, context));
            case 'log':
                return ok(await bindLog(statement, context));
        }
    } catch (e) {
        diagnostics.add('BIND_UNKNOWN_NAME', e instanceof Error ? e.message : String(e), statement.span);
        return err(diagnostics.all());
    }
}

async function bindCreateDatabase(ast: CreateDatabaseStatement, context: LangBindContext): Promise<BoundCreateDatabase> {
    const creators: { keyId: KeyId; publicKey: PublicKey }[] = [];
    for (const expr of ast.creators) {
        creators.push(await resolveCreator(expr, context));
    }
    return { kind: 'create-database', ast, seed: ast.seed ?? context.createSeed('rdb', ast.name), creators };
}

async function bindCreateSchema(ast: CreateSchemaStatement, context: LangBindContext): Promise<BoundCreateSchema> {
    for (const table of ast.tables) {
        for (const column of table.columns) {
            if (column.name === PSEUDO_COLUMN_UUID) {
                throw new Error("column name 'uuid' is reserved");
            }
        }
    }
    const creators: { keyId: KeyId; publicKey: PublicKey }[] = [];
    for (const expr of ast.creators) {
        creators.push(await resolveCreator(expr, context));
    }
    return { kind: 'create-schema', ast, creators };
}

async function bindCreateTableGroup(ast: CreateTableGroupStatement, context: LangBindContext): Promise<BoundCreateTableGroup> {
    const schema = await context.resolveSchema(ast.schema);
    const schemaVersion = await context.resolveVersion(ast.schemaVersion, { kind: 'schema', id: schema.id, schema: schema.schema });
    const bindings: { [name: string]: B64Hash } = {};
    for (const binding of ast.bindings) {
        bindings[binding.name] = (await context.resolveGroup(binding.group)).id;
    }
    const initialRows: BoundInitialRow[] = [];
    const schemaView = schema.schema !== undefined
        ? await schema.schema.getView(schemaVersion, schemaVersion)
        : undefined;
    for (const row of ast.initialRows) {
        const { uuid, values } = await bindInitialRowValues(row.values, context);
        const columns = schemaView?.getTable(row.table)?.columns ?? {};
        initialRows.push({ table: row.table, uuid, values: canonicalEncodeRowValues(values, columns) });
    }
    return {
        kind: 'create-tablegroup',
        ast,
        seed: ast.seed ?? context.createSeed('group', ast.name),
        schema,
        schemaVersion,
        bindings,
        initialRows,
    };
}

async function bindAddMember(ast: AddMemberStatement, context: LangBindContext): Promise<BoundAddMember> {
    const database = await context.resolveDatabase(ast.database);
    const memberId = ast.member === 'schema'
        ? (await context.resolveSchema(ast.target)).id
        : (await context.resolveGroup(ast.target)).id;
    const at = await context.resolveVersion(ast.at, { kind: 'object', id: database.id, object: database.db });
    if (database.db !== undefined && database.db.getCreators().length > 0 && ast.author === undefined) {
        throw new Error(`ADD ${ast.member === 'schema' ? 'SCHEMA' : 'TABLEGROUP'} requires BY when the database declares creators`);
    }
    const author = await resolveEffectiveAuthor(ast.author, context);
    if (database.db !== undefined && database.db.getCreators().length > 0 && author === undefined) {
        throw new Error(`ADD ${ast.member === 'schema' ? 'SCHEMA' : 'TABLEGROUP'} requires an author when the database declares creators`);
    }
    const bound: BoundAddMember = { kind: 'add-member', ast, member: ast.member, database, memberId, at };
    if (ast.note !== undefined) bound.note = ast.note;
    if (author !== undefined) bound.author = author;
    return bound;
}

// Resolve the effective author of an authored statement: an explicit `BY`
// clause wins (`NOBODY` forces unauthored), otherwise the session's default
// author is used (which may itself be undefined / anonymous).
async function resolveEffectiveAuthor(expr: AuthorExpr | undefined, context: LangBindContext): Promise<OwnIdentity | undefined> {
    if (expr === undefined) return context.currentAuthor();
    if (expr.kind === 'nobody') return undefined;
    if (expr.kind === 'variable') return context.resolveAuthor({ kind: 'variable', name: expr.name });
    return context.resolveAuthor({ kind: 'hash', prefix: expr.prefix });
}

// A context view in which `$author` / `$me` resolve to the statement's effective
// author rather than the session default, so values like `VALUES ($author)`
// agree with the signer chosen by `BY`.
function contextWithAuthor(context: LangBindContext, author: OwnIdentity | undefined): LangBindContext {
    return {
        ...context,
        resolveVariable: (name: string): Promise<LangValue> => {
            if (name === 'me' || name === 'author') {
                if (author !== undefined) return Promise.resolve(author);
                throw new Error(`$${name} has no value: the statement has no author (BY NOBODY or no default author)`);
            }
            return context.resolveVariable(name);
        },
    };
}

async function bindInsert(ast: InsertStatement, context: LangBindContext): Promise<BoundInsert> {
    if (ast.columns.length !== ast.values.length) {
        throw new Error(`INSERT column count (${ast.columns.length}) does not match value count (${ast.values.length})`);
    }
    const table = await resolveTableRef(ast.table, context);
    const author = await resolveEffectiveAuthor(ast.author, context);
    const valueContext = contextWithAuthor(context, author);
    const at = await context.resolveVersion(ast.at, { kind: 'group', id: table.groupId, group: table.group });
    const { uuid, values } = await bindInsertColumns(ast.columns, ast.values, context, table, at, valueContext);
    const bound: BoundInsert = { kind: 'insert', ast, table, values, at, uuid };
    if (author !== undefined) bound.author = author;
    return bound;
}

async function bindUpdate(ast: UpdateStatement, context: LangBindContext): Promise<BoundUpdate> {
    const table = await resolveTableRef(ast.table, context);
    const author = await resolveEffectiveAuthor(ast.author, context);
    const valueContext = contextWithAuthor(context, author);
    const rawValues: { [column: string]: json.Literal } = {};
    for (const v of ast.values) rawValues[v.column] = asJsonLiteral(await resolveValue(v.value, valueContext));
    const at = await context.resolveVersion(ast.at, { kind: 'group', id: table.groupId, group: table.group });
    const values = canonicalEncodeRowValues(rawValues, await resolveColumnDefs(table, at));
    const rowId = await bindRowId(ast.rowId, context, table, at);
    const bound: BoundUpdate = { kind: 'update', ast, table, values, rowId, at };
    if (author !== undefined) bound.author = author;
    return bound;
}

async function bindDelete(ast: DeleteStatement, context: LangBindContext): Promise<BoundDelete> {
    const table = await resolveTableRef(ast.table, context);
    const author = await resolveEffectiveAuthor(ast.author, context);
    const at = await context.resolveVersion(ast.at, { kind: 'group', id: table.groupId, group: table.group });
    const rowId = await bindRowId(ast.rowId, context, table, at);
    const bound: BoundDelete = { kind: 'delete', ast, table, rowId, at };
    if (author !== undefined) bound.author = author;
    return bound;
}

async function bindBundle(ast: BundleStatement, context: LangBindContext): Promise<BoundBundle> {
    const group = await context.resolveGroup(ast.group);
    if (group.group === undefined) throw new Error('BUNDLE target group is not loaded');
    const author = await resolveEffectiveAuthor(ast.author, context);
    const valueContext = contextWithAuthor(context, author);
    const at = await context.resolveVersion(ast.at, { kind: 'group', id: group.id, group: group.group });
    const writes: BoundBundleWrite[] = [];
    for (const write of ast.writes) writes.push(await bindBundleWrite(write, valueContext, at, author?.keyId));
    const bound: BoundBundle = { kind: 'bundle', ast, group, writes, at };
    if (author !== undefined) bound.author = author;
    return bound;
}

async function bindBundleWrite(write: BundleWriteStatement, context: LangBindContext, at: Version, author?: KeyId): Promise<BoundBundleWrite> {
    if (write.kind === 'insert') {
        if (write.columns.length !== write.values.length) throw new Error('BUNDLE INSERT column count does not match value count');
        const table = await context.resolveTable(write.table);
        const { uuid, values } = await bindInsertColumns(write.columns, write.values, context, table, at, context);
        const op: InsertRowPayload = { action: 'insert', rowId: deriveRowId(uuid, author), uuid, values };
        return { table: write.table.table, op };
    }
    if (write.kind === 'update') {
        const table = await context.resolveTable(write.table);
        const rawValues: { [column: string]: json.Literal } = {};
        for (const v of write.values) rawValues[v.column] = asJsonLiteral(await resolveValue(v.value, context));
        const values = canonicalEncodeRowValues(rawValues, await resolveColumnDefs(table, at));
        return { table: write.table.table, op: { action: 'update', rowId: await bindRowId(write.rowId, context, table, at), values } };
    }
    const table = await context.resolveTable(write.table);
    return { table: write.table.table, op: { action: 'delete', rowId: await bindRowId(write.rowId, context, table, at) } };
}

function bindSetView(ast: SetViewStatement): BoundSetView {
    const bound: BoundSetView = { kind: 'set-view', ast, at: ast.at };
    if (ast.from !== undefined) bound.from = ast.from;
    return bound;
}

async function bindAlterSchema(ast: AlterSchemaStatement, context: LangBindContext): Promise<BoundAlterSchema> {
    const schema = await context.resolveSchema(ast.schema);
    if (schema.schema === undefined) throw new Error('ALTER SCHEMA target is not loaded');
    const author = await resolveEffectiveAuthor(ast.author, context);
    if (author === undefined) throw new Error('ALTER SCHEMA requires an author identity');
    const at = await context.resolveVersion(ast.at, { kind: 'schema', id: schema.id, schema: schema.schema });
    const view = await schema.schema.getView(at, at);
    const columnsOf = buildAlterColumnsOf(view, ast.rules);
    return { kind: 'alter-schema', ast, schema, rules: compileMigrationRules(ast.rules, columnsOf), author, at };
}

async function bindUpdateSchema(ast: UpdateSchemaStatement, context: LangBindContext): Promise<BoundUpdateSchema> {
    const group = await context.resolveGroup(ast.group);
    if (group.group === undefined) throw new Error('UPDATE SCHEMA target group is not loaded');
    const schema = await context.resolveSchema(ast.schema);
    const version = await context.resolveVersion(ast.version, { kind: 'schema', id: schema.id, schema: schema.schema });
    const author = await resolveEffectiveAuthor(ast.author, context);
    const at = await context.resolveVersion(ast.at, { kind: 'group', id: group.id, group: group.group });
    const bound: BoundUpdateSchema = { kind: 'update-schema', ast, group, version, at };
    if (author !== undefined) bound.author = author;
    return bound;
}

async function bindUpdateRef(ast: UpdateRefStatement, context: LangBindContext): Promise<BoundUpdateRef> {
    const group = await context.resolveGroup(ast.group);
    if (group.group === undefined) throw new Error('UPDATE REF target group is not loaded');
    const ref = await resolveBoundGroupRef(ast.ref, group, context);
    const refVersion = await context.resolveVersion(ast.version, { kind: 'group', id: ref.foreign.id, group: ref.foreign.group });
    const at = await context.resolveVersion(ast.at, { kind: 'group', id: group.id, group: group.group });
    const author = await resolveEffectiveAuthor(ast.author, context);
    return { kind: 'update-ref', ast, group, ref: ref.observeRef, version: refVersion, author, at };
}

async function resolveBoundGroupRef(
    ref: NameOrHashRef,
    group: ResolvedGroupRef,
    context: LangBindContext,
): Promise<{ observeRef: string; foreign: ResolvedGroupRef }> {
    if (group.group === undefined) throw new Error('UPDATE REF target group is not loaded');
    const bindings = group.group.getBindings();

    let groupId: B64Hash;
    let observeRef: string;
    if (ref.kind === 'name') {
        if (ref.parts.length !== 1) throw new Error('UPDATE REF expects a bound group name, not group.table');
        const bound = bindings[ref.text];
        if (bound === undefined) throw new Error(`'${ref.text}' is not a bound group of '${group.id}'`);
        groupId = bound;
        observeRef = ref.text;
    } else {
        const matches = [...new Set(Object.values(bindings))].filter((id) => id.startsWith(ref.prefix));
        if (matches.length === 0) throw new Error(`Unknown bound group hash prefix '#${ref.prefix}'`);
        if (matches.length > 1) throw new Error(`Ambiguous bound group hash prefix '#${ref.prefix}'`);
        groupId = matches[0];
        observeRef = groupId;
    }

    const foreign = await context.resolveGroup({ kind: 'name', text: groupId, parts: [groupId], span: ref.span });
    return { observeRef, foreign };
}

async function bindSelect(ast: SelectStatement, context: LangBindContext): Promise<BoundSelect> {
    if (ast.from !== undefined && ast.at === undefined) {
        throw new Error('SELECT FROM version requires AT');
    }
    const table = await resolveTableRef(ast.table, context);
    const scope = { kind: 'group' as const, id: table.groupId, group: table.group };
    const defaultView = ast.at === undefined ? await context.resolveDefaultView?.(scope) : undefined;
    const at = ast.at === undefined && defaultView !== undefined
        ? defaultView.at
        : await context.resolveVersion(ast.at, scope);
    const from = ast.at === undefined && defaultView?.from !== undefined
        ? defaultView.from
        : await context.resolveVersion(ast.from ?? ast.at, scope);
    const query = await lowerSelectQuery(ast, context);
    return { kind: 'select', ast, table, query, at, from };
}

async function resolveTableRef(ref: TableRef, context: LangBindContext): Promise<ResolvedTableRef> {
    if (ref.group !== undefined) return context.resolveTable(ref);
    const group = await context.resolveDefaultGroup?.();
    if (group === undefined) {
        throw new Error(`Table '${ref.table}' requires a group qualifier; use group.table or set a current group`);
    }
    return context.resolveTable({ ...ref, group });
}

function versionScopeForLogTarget(target: ResolvedLogTarget): VersionScope {
    switch (target.kind) {
        case 'group':
            return { kind: 'group', id: target.id, group: target.object };
        case 'table':
            return { kind: 'table', groupId: target.groupId, tableName: target.tableName, table: target.object };
        case 'schema':
            return { kind: 'schema', id: target.id, schema: target.object };
        case 'database':
            return { kind: 'object', id: target.id, object: target.object };
    }
}

async function bindLog(ast: LogStatement, context: LangBindContext): Promise<BoundLog> {
    if (ast.from !== undefined && ast.at === undefined) {
        throw new Error('LOG FROM version requires AT');
    }
    const target = await context.resolveLogTarget(ast.target);
    const scope = versionScopeForLogTarget(target);
    const fromScope = target.kind === 'table'
        ? { kind: 'group' as const, id: target.groupId, group: target.group }
        : scope;
    const defaultView = ast.at === undefined ? await context.resolveDefaultView?.(scope) : undefined;
    const at = ast.at === undefined && defaultView !== undefined
        ? defaultView.at
        : await context.resolveVersion(ast.at, scope);
    const from = ast.at === undefined && defaultView?.from !== undefined
        ? defaultView.from
        : await context.resolveVersion(ast.from ?? ast.at, fromScope);
    return { kind: 'log', ast, target, at, from, explain: ast.explain === true };
}

async function bindInsertColumns(
    columns: string[],
    valueExprs: ValueExpr[],
    context: LangBindContext,
    table: ResolvedTableRef,
    at: Version,
    valueContext: LangBindContext,
): Promise<{ uuid: string; values: { [column: string]: json.Literal } }> {
    let uuid: string | undefined;
    const values: { [column: string]: json.Literal } = {};
    for (let i = 0; i < columns.length; i += 1) {
        const column = columns[i];
        const expr = valueExprs[i];
        if (column === PSEUDO_COLUMN_UUID) {
            const lit = asJsonLiteral(await resolveValue(expr, valueContext));
            if (typeof lit !== 'string') throw new Error('uuid pseudo-column requires a string value');
            uuid = lit;
            continue;
        }
        if (expr.kind === 'hash') {
            if (context.resolveFkRowId === undefined) {
                throw new Error('FK #prefix resolution is not available in this host');
            }
            values[column] = await context.resolveFkRowId(expr.prefix, table, column, at, at);
        } else {
            values[column] = asJsonLiteral(await resolveValue(expr, valueContext));
        }
    }
    const columnDefs = await resolveColumnDefs(table, at);
    return { uuid: uuid ?? context.createUuid(), values: canonicalEncodeRowValues(values, columnDefs) };
}

async function bindInitialRowValues(
    pairs: { column: string; value: ValueExpr }[],
    context: LangBindContext,
): Promise<{ uuid: string; values: { [column: string]: json.Literal } }> {
    let uuid: string | undefined;
    const values: { [column: string]: json.Literal } = {};
    for (const pair of pairs) {
        if (pair.column === PSEUDO_COLUMN_UUID) {
            const lit = asJsonLiteral(await resolveValue(pair.value, context));
            if (typeof lit !== 'string') throw new Error('uuid pseudo-column requires a string value');
            uuid = lit;
            continue;
        }
        values[pair.column] = asJsonLiteral(await resolveValue(pair.value, context));
    }
    return { uuid: uuid ?? context.createUuid(), values };
}

// The declared column defs of a group table at a version, for canonical value
// encoding. An unresolved schema (or absent table) yields an empty map, in
// which case values pass through unchanged (the engine still gates on write).
async function resolveColumnDefs(table: ResolvedTableRef, at: Version): Promise<{ [column: string]: ColumnDef }> {
    const view = await table.group.getView(at, at);
    const def = view.getSchemaView().getTable(table.tableName);
    return def?.columns ?? {};
}

async function bindRowId(
    expr: UpdateStatement['rowId'] | DeleteStatement['rowId'],
    context: LangBindContext,
    table: ResolvedTableRef,
    at: Version,
): Promise<B64Hash> {
    if (expr.kind === 'hash') {
        if (context.resolveRowId === undefined) {
            throw new Error('rowId #prefix resolution is not available in this host');
        }
        return context.resolveRowId(expr, table, at, at);
    }
    if (expr.kind === 'name') {
        return expr.text;
    }
    const value = await resolveValue(expr, context);
    if (typeof value === 'string') return value;
    if (typeof value === 'object' && value !== null && (value as { kind?: unknown }).kind === 'key-id') {
        return (value as { keyId: B64Hash }).keyId;
    }
    throw new Error('rowId must resolve to a hash string');
}

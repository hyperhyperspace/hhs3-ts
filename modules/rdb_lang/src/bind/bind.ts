import type { B64Hash, KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { json } from "@hyper-hyper-space/hhs3_json";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import { deriveRowId } from "@hyper-hyper-space/hhs3_rdb";
import type { InsertRowPayload, MigrationRule, RowOpPayload, RowQuery } from "@hyper-hyper-space/hhs3_rdb";

import { DiagnosticBag, err, ok, Result } from "../diagnostics.js";
import type {
    AlterSchemaStatement, AstStatement, BundleStatement, BundleWriteStatement,
    CreateDatabaseStatement, CreateSchemaStatement, CreateTableGroupStatement,
    DeleteStatement, DeploySchemaStatement, InsertStatement, LogStatement,
    NameOrHashRef, SelectStatement, SetViewStatement, TableRef, UpdateRefStatement, UpdateStatement,
} from "../syntax/ast.js";
import { compileMigrationRules } from "../compile/ddl.js";
import { lowerSelectQuery } from "../compile/query.js";
import type {
    LangBindContext, LangValue, ResolvedGroupRef, ResolvedLogTarget, ResolvedSchemaRef,
    ResolvedTableRef,
} from "./context.js";
import { asIdentity, asJsonLiteral, asKeyId, resolveValue } from "./values.js";

export type BoundStatement =
    | BoundCreateDatabase
    | BoundCreateSchema
    | BoundCreateTableGroup
    | BoundAlterSchema
    | BoundDeploySchema
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
    | BoundAlterSchema
    | BoundDeploySchema
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

export type BoundDeploySchema = {
    kind: 'deploy-schema';
    ast: DeploySchemaStatement;
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
    at?: Version;
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
            case 'alter-schema':
                return ok(await bindAlterSchema(statement, context));
            case 'deploy-schema':
                return ok(await bindDeploySchema(statement, context));
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
    return { kind: 'create-database', ast, seed: context.createSeed('rdb', ast.name) };
}

async function bindCreateSchema(ast: CreateSchemaStatement, context: LangBindContext): Promise<BoundCreateSchema> {
    const creators: { keyId: KeyId; publicKey: PublicKey }[] = [];
    for (const expr of ast.creators) {
        const value = await resolveValue(expr, context);
        if (!isCreatorValue(value)) throw new Error('CREATORS values must resolve to identities or creator records');
        creators.push({ keyId: value.keyId, publicKey: value.publicKey });
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
    for (const row of ast.initialRows) {
        const values: { [column: string]: json.Literal } = {};
        for (const v of row.values) values[v.column] = asJsonLiteral(await resolveValue(v.value, context));
        const bound: BoundInitialRow = { table: row.table, uuid: context.createUuid(), values };
        initialRows.push(bound);
    }
    return {
        kind: 'create-tablegroup',
        ast,
        seed: context.createSeed('group', ast.name),
        schema,
        schemaVersion,
        bindings,
        initialRows,
    };
}

async function bindInsert(ast: InsertStatement, context: LangBindContext): Promise<BoundInsert> {
    if (ast.columns.length !== ast.values.length) {
        throw new Error(`INSERT column count (${ast.columns.length}) does not match value count (${ast.values.length})`);
    }
    const table = await resolveTableRef(ast.table, context);
    const values: { [column: string]: json.Literal } = {};
    for (let i = 0; i < ast.columns.length; i += 1) {
        values[ast.columns[i]] = asJsonLiteral(await resolveValue(ast.values[i], context));
    }
    const author = await context.currentAuthor();
    const at = await context.resolveVersion(ast.at, { kind: 'group', id: table.groupId, group: table.group });
    const bound: BoundInsert = { kind: 'insert', ast, table, values, at, uuid: context.createUuid() };
    if (author !== undefined) bound.author = author;
    return bound;
}

async function bindUpdate(ast: UpdateStatement, context: LangBindContext): Promise<BoundUpdate> {
    const table = await resolveTableRef(ast.table, context);
    const values: { [column: string]: json.Literal } = {};
    for (const v of ast.values) values[v.column] = asJsonLiteral(await resolveValue(v.value, context));
    const author = await context.currentAuthor();
    const at = await context.resolveVersion(ast.at, { kind: 'group', id: table.groupId, group: table.group });
    const rowId = await bindRowId(ast.rowId, context, table, at);
    const bound: BoundUpdate = { kind: 'update', ast, table, values, rowId, at };
    if (author !== undefined) bound.author = author;
    return bound;
}

async function bindDelete(ast: DeleteStatement, context: LangBindContext): Promise<BoundDelete> {
    const table = await resolveTableRef(ast.table, context);
    const author = await context.currentAuthor();
    const at = await context.resolveVersion(ast.at, { kind: 'group', id: table.groupId, group: table.group });
    const rowId = await bindRowId(ast.rowId, context, table, at);
    const bound: BoundDelete = { kind: 'delete', ast, table, rowId, at };
    if (author !== undefined) bound.author = author;
    return bound;
}

async function bindBundle(ast: BundleStatement, context: LangBindContext): Promise<BoundBundle> {
    const group = await context.resolveGroup(ast.group);
    if (group.group === undefined) throw new Error('BUNDLE target group is not loaded');
    const author = await context.currentAuthor();
    const at = await context.resolveVersion(ast.at, { kind: 'group', id: group.id, group: group.group });
    const writes: BoundBundleWrite[] = [];
    for (const write of ast.writes) writes.push(await bindBundleWrite(write, context, at, author?.keyId));
    const bound: BoundBundle = { kind: 'bundle', ast, group, writes, at };
    if (author !== undefined) bound.author = author;
    return bound;
}

async function bindBundleWrite(write: BundleWriteStatement, context: LangBindContext, at: Version, author?: KeyId): Promise<BoundBundleWrite> {
    if (write.kind === 'insert') {
        if (write.columns.length !== write.values.length) throw new Error('BUNDLE INSERT column count does not match value count');
        const values: { [column: string]: json.Literal } = {};
        for (let i = 0; i < write.columns.length; i += 1) values[write.columns[i]] = asJsonLiteral(await resolveValue(write.values[i], context));
        const uuid = context.createUuid();
        const op: InsertRowPayload = { action: 'insert', rowId: deriveRowId(uuid, author), uuid, values };
        return { table: write.table.table, op };
    }
    if (write.kind === 'update') {
        const table = await context.resolveTable(write.table);
        const values: { [column: string]: json.Literal } = {};
        for (const v of write.values) values[v.column] = asJsonLiteral(await resolveValue(v.value, context));
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
    const author = await context.currentAuthor();
    if (author === undefined) throw new Error('ALTER SCHEMA requires an author identity');
    const at = await context.resolveVersion(ast.at, { kind: 'schema', id: schema.id, schema: schema.schema });
    return { kind: 'alter-schema', ast, schema, rules: compileMigrationRules(ast.rules), author, at };
}

async function bindDeploySchema(ast: DeploySchemaStatement, context: LangBindContext): Promise<BoundDeploySchema> {
    const group = await context.resolveGroup(ast.group);
    if (group.group === undefined) throw new Error('DEPLOY SCHEMA target group is not loaded');
    const schema = await context.resolveSchema(ast.schema);
    const version = await context.resolveVersion(ast.version, { kind: 'schema', id: schema.id, schema: schema.schema });
    const author = await context.currentAuthor();
    const at = await context.resolveVersion(ast.at, { kind: 'group', id: group.id, group: group.group });
    const bound: BoundDeploySchema = { kind: 'deploy-schema', ast, group, version, at };
    if (author !== undefined) bound.author = author;
    return bound;
}

async function bindUpdateRef(ast: UpdateRefStatement, context: LangBindContext): Promise<BoundUpdateRef> {
    const group = await context.resolveGroup(ast.group);
    if (group.group === undefined) throw new Error('UPDATE REF target group is not loaded');
    const ref = await resolveBoundGroupRef(ast.ref, group, context);
    const refVersion = await context.resolveVersion(ast.version, { kind: 'group', id: ref.foreign.id, group: ref.foreign.group });
    const at = await context.resolveVersion(ast.at, { kind: 'group', id: group.id, group: group.group });
    return { kind: 'update-ref', ast, group, ref: ref.observeRef, version: refVersion, at };
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

async function bindLog(ast: LogStatement, context: LangBindContext): Promise<BoundLog> {
    const target = await context.resolveLogTarget(ast.target);
    const at = ast.at !== undefined
        ? await context.resolveVersion(ast.at, { kind: 'object', id: target.id, object: target.object })
        : undefined;
    const bound: BoundLog = { kind: 'log', ast, target };
    if (at !== undefined) bound.at = at;
    return bound;
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

function isCreatorValue(value: LangValue): value is { keyId: KeyId; publicKey: PublicKey } {
    return typeof value === 'object' && value !== null && 'keyId' in value && 'publicKey' in value;
}

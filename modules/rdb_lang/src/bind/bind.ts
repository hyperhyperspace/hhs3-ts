import type { B64Hash, KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import type { json } from "@hyper-hyper-space/hhs3_json";
import { serializePublicKeyToBase64, type Version } from "@hyper-hyper-space/hhs3_mvt";
import { compareSemver, deriveRowId, isValidSemver, paramTypeFits } from "@hyper-hyper-space/hhs3_rdb";
import type {
    CatalogGroupDef, CatalogParamDecl, CatalogReleaseSpec, ColumnDef, InsertRowPayload, MigrationRule, ParamValue,
    RCatalogImpl, RDbImpl, RowOpPayload, RowQuery,
} from "@hyper-hyper-space/hhs3_rdb";

import { DiagnosticBag, err, ok, Result } from "../diagnostics.js";
import type {
    AlterCatalogStatement, AlterSchemaStatement, AstStatement, AuthorExpr, BundleStatement, BundleWriteStatement,
    CreateCatalogStatement, CreateDatabaseStatement, CreateSchemaStatement,
    DeleteStatement, InsertStatement, LogStatement, NameOrHashRef, ParamAssignment, ReleaseSelector,
    SelectStatement, SetViewStatement, TableRef, UpdateCatalogStatement, UpdateRefStatement, UpdateStatement,
    UseDatabaseStatement, ValueExpr,
} from "../syntax/ast.js";
import { compileMigrationRules } from "../compile/ddl.js";
import { buildAlterColumnsOf } from "../compile/rule_scope.js";
import { lowerSelectQuery } from "../compile/query.js";
import { CatalogDefScope, compileCatalogGroup, compileParamDecls, compileReleaseChanges } from "../compile/catalog.js";
import type {
    LangBindContext, LangValue, ResolvedDatabaseRef, ResolvedGroupRef, ResolvedLogTarget, ResolvedSchemaRef,
    ResolvedTableRef, VersionScope,
} from "./context.js";
import { asJsonLiteral, canonicalEncodeRowValues, canonicalEncodeValue, resolveCreator, resolveValue } from "./values.js";

/** Reserved INSERT pseudo-column; not a schema column. */
export const PSEUDO_COLUMN_UUID = 'uuid';

export type BoundStatement =
    | BoundCreateDatabase
    | BoundCreateSchema
    | BoundCreateCatalog
    | BoundAlterCatalog
    | BoundUpdateCatalog
    | BoundUseDatabase
    | BoundAlterSchema
    | BoundUpdateRef
    | BoundInsert
    | BoundUpdate
    | BoundDelete
    | BoundBundle
    | BoundSetView
    | BoundSelect
    | BoundLog;

export type BoundCreateStatement = BoundCreateDatabase | BoundCreateSchema | BoundCreateCatalog;
export type BoundExecutableStatement =
    | BoundAlterCatalog
    | BoundUpdateCatalog
    | BoundUseDatabase
    | BoundAlterSchema
    | BoundUpdateRef
    | BoundInsert
    | BoundUpdate
    | BoundDelete
    | BoundBundle
    | BoundSetView
    | BoundSelect
    | BoundLog;

export type LoadedCatalogRef = { id: B64Hash; catalog: RCatalogImpl };
export type LoadedDatabaseRef = { id: B64Hash; db: RDbImpl };

export type BoundCreateDatabase = {
    kind: 'create-database';
    ast: CreateDatabaseStatement;
    seed: string;
    creators: { keyId: KeyId; publicKey: PublicKey }[];
    catalog: LoadedCatalogRef;
    release: B64Hash;
    params: { [name: string]: ParamValue };
    // Signs the deploys that bring members from their pins to the release.
    author?: OwnIdentity;
};

export type BoundCreateSchema = {
    kind: 'create-schema';
    ast: CreateSchemaStatement;
    creators: { keyId: KeyId; publicKey: PublicKey }[];
};

export type BoundCreateCatalog = {
    kind: 'create-catalog';
    ast: CreateCatalogStatement;
    creators: { keyId: KeyId; publicKey: PublicKey }[];
    author: OwnIdentity;
    add: CatalogGroupDef[];
    params: CatalogParamDecl[];
};

export type BoundAlterCatalog = {
    kind: 'alter-catalog';
    ast: AlterCatalogStatement;
    catalog: LoadedCatalogRef;
    spec: CatalogReleaseSpec;
    author: OwnIdentity;
    at: Version;
};

export type BoundUpdateCatalog = {
    kind: 'update-catalog';
    ast: UpdateCatalogStatement;
    catalog: LoadedCatalogRef;
    database: LoadedDatabaseRef;
    release: B64Hash;
    params: { [name: string]: ParamValue };
    note?: string;
    author?: OwnIdentity;
};

export type BoundUseDatabase = {
    kind: 'use-database';
    ast: UseDatabaseStatement;
    database: ResolvedDatabaseRef;
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
    note?: string;
    author: OwnIdentity;
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
    deployLabels?: { [versionKey: string]: string };
};

export async function bind(statement: AstStatement, context: LangBindContext): Promise<Result<BoundStatement>> {
    const diagnostics = new DiagnosticBag();
    try {
        switch (statement.kind) {
            case 'create-database':
                return ok(await bindCreateDatabase(statement, context));
            case 'create-schema':
                return ok(await bindCreateSchema(statement, context));
            case 'create-catalog':
                return ok(await bindCreateCatalog(statement, context));
            case 'alter-catalog':
                return ok(await bindAlterCatalog(statement, context));
            case 'update-catalog':
                return ok(await bindUpdateCatalog(statement, context));
            case 'use-database':
                return ok({ kind: 'use-database', ast: statement, database: await context.resolveDatabase(statement.database) });
            case 'alter-schema':
                return ok(await bindAlterSchema(statement, context));
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
    const catalog = await loadCatalog(ast.catalog, context);
    const release = await selectRelease(ast.release, catalog, context);
    const decls = (await catalog.catalog.getIndex()).releaseState(release).params;
    const params = await bindParamAssignments(ast.params, decls, new Set(), context);
    const author = await resolveEffectiveAuthor(ast.author, context);
    const bound: BoundCreateDatabase = {
        kind: 'create-database', ast, seed: ast.seed ?? context.createSeed('rdb', ast.name), creators, catalog, release, params,
    };
    if (author !== undefined) bound.author = author;
    return bound;
}

async function bindCreateCatalog(ast: CreateCatalogStatement, context: LangBindContext): Promise<BoundCreateCatalog> {
    const author = await resolveEffectiveAuthor(ast.author, context);
    if (author === undefined) throw new Error('CREATE CATALOG requires an author identity: every catalog entry is signed');
    const creators: { keyId: KeyId; publicKey: PublicKey }[] = [];
    for (const expr of ast.creators) creators.push(await resolveCreator(expr, context));
    if (creators.length === 0) creators.push({ keyId: author.keyId, publicKey: author.publicKey });
    if (!creators.some((c) => c.keyId === author.keyId)) {
        throw new Error('the CREATE CATALOG author must be one of its CREATORS');
    }
    if (!isValidSemver(ast.version)) throw new Error(`VERSION '${ast.version}' is not a semver (major.minor.patch)`);

    const params = compileParamDecls(ast.params);
    const paramMap = new Map(params.map((p) => [p.name, p]));
    const scope = new CatalogDefScope();
    const add: CatalogGroupDef[] = [];
    for (const group of ast.groups) {
        const def = await compileCatalogGroup(group, context, scope, paramMap);
        if (add.some((d) => d.name === def.name)) throw new Error(`TABLEGROUP ${def.name} is defined twice`);
        scope.add(def);
        add.push(def);
    }
    return { kind: 'create-catalog', ast, creators, author, add, params };
}

async function bindAlterCatalog(ast: AlterCatalogStatement, context: LangBindContext): Promise<BoundAlterCatalog> {
    const catalog = await loadCatalog(ast.catalog, context);
    const author = await resolveEffectiveAuthor(ast.author, context);
    if (author === undefined) throw new Error('ALTER CATALOG requires an author identity: every catalog entry is signed');
    if (!catalog.catalog.isCreator(author.keyId)) throw new Error('the ALTER CATALOG author must be one of the catalog creators');
    if (!isValidSemver(ast.version)) throw new Error(`VERSION '${ast.version}' is not a semver (major.minor.patch)`);

    const at = await context.resolveVersion(ast.at ?? { kind: 'latest', span: ast.span }, {
        kind: 'object', id: catalog.id, object: catalog.catalog,
    });
    const index = await catalog.catalog.getIndex();
    const parents = index.parentsOf(at);
    if (parents.length === 0) throw new Error('ALTER CATALOG ... AT must be at or above the catalog genesis');
    for (const parent of parents) {
        const parentVersion = index.releaseState(parent).version;
        if (compareSemver(ast.version, parentVersion) <= 0) {
            throw new Error(`VERSION '${ast.version}' must be greater than the parent release '${parentVersion}' (#${parent.slice(0, 8)})`);
        }
    }
    const fold = index.foldParents(parents);

    const params = compileParamDecls(ast.params);
    const paramMap = new Map(fold.params);
    for (const decl of params) {
        if (paramMap.has(decl.name)) throw new Error(`param ':${decl.name}' is already declared by an earlier release`);
        paramMap.set(decl.name, decl);
    }
    const { changes, add } = await compileReleaseChanges(ast.changes, fold, context, paramMap);

    const spec: CatalogReleaseSpec = { version: ast.version };
    if (Object.keys(changes).length > 0) spec.changes = changes;
    if (add.length > 0) spec.add = add;
    if (params.length > 0) spec.params = params;
    if (ast.note !== undefined) spec.note = ast.note;
    return { kind: 'alter-catalog', ast, catalog, spec, author, at };
}

async function bindUpdateCatalog(ast: UpdateCatalogStatement, context: LangBindContext): Promise<BoundUpdateCatalog> {
    const catalog = await loadCatalog(ast.catalog, context);
    const resolved = await context.resolveDatabase(ast.database);
    if (resolved.db === undefined) throw new Error(`database '${resolved.id}' is not loaded`);
    const database: LoadedDatabaseRef = { id: resolved.id, db: resolved.db };
    if (database.db.getCatalogRef() !== catalog.id) {
        throw new Error(`database '${database.db.getName() ?? database.id}' uses catalog '${database.db.getCatalogRef()}', not '${catalog.id}'`);
    }
    const release = await selectRelease(ast.release, catalog, context);
    const author = await resolveEffectiveAuthor(ast.author, context);
    if (database.db.getCreators().length > 0 && author === undefined) {
        throw new Error('UPDATE CATALOG requires an author when the database declares creators');
    }
    const decls = (await catalog.catalog.getIndex()).releaseState(release).params;
    const existing = new Set(Object.keys(await database.db.getParams()));
    const params = await bindParamAssignments(ast.params, decls, existing, context);
    const bound: BoundUpdateCatalog = { kind: 'update-catalog', ast, catalog, database, release, params };
    if (ast.note !== undefined) bound.note = ast.note;
    if (author !== undefined) bound.author = author;
    return bound;
}

async function loadCatalog(ref: NameOrHashRef, context: LangBindContext): Promise<LoadedCatalogRef> {
    const resolved = await context.resolveCatalog(ref);
    if (resolved.catalog === undefined) throw new Error(`catalog '${resolved.id}' is not loaded`);
    return { id: resolved.id, catalog: resolved.catalog };
}

// A semver or LATEST must match exactly one release; hashes name a release.
async function selectRelease(
    selector: ReleaseSelector | undefined,
    catalog: LoadedCatalogRef,
    context: LangBindContext,
): Promise<B64Hash> {
    const index = await catalog.catalog.getIndex();
    const frontier = await (await catalog.catalog.getScopedDag()).getFrontier();
    const describe = (hashes: B64Hash[]) => hashes
        .map((h) => `'${index.releaseState(h).version}' (#${h.slice(0, 8)})`).join(', ');

    if (selector === undefined || (selector.kind === 'version' && selector.version.kind === 'latest')) {
        const maximal = index.maximalReleasesAt(frontier);
        if (maximal.length !== 1) {
            throw new Error(`the catalog has ${maximal.length} latest releases: ${describe(maximal)}; select one by hash`);
        }
        return maximal[0];
    }
    if (selector.kind === 'semver') {
        const matches = index.findReleasesByVersion(selector.version, frontier);
        if (matches.length === 0) throw new Error(`the catalog has no release '${selector.version}'`);
        if (matches.length > 1) {
            throw new Error(`release '${selector.version}' is ambiguous: ${matches.map((h) => `#${h.slice(0, 8)}`).join(', ')}; select one by hash`);
        }
        return matches[0];
    }
    const version = await context.resolveVersion(selector.version, { kind: 'object', id: catalog.id, object: catalog.catalog });
    if (version.size !== 1) throw new Error('a release selection names exactly one release');
    const hash = [...version][0];
    if (!index.isRelease(hash)) throw new Error(`'#${hash.slice(0, 8)}' is not a release of the catalog`);
    return hash;
}

// Params supplied at deploy time: each must be declared by the release, not
// already set on the database, and of its declared type; together with the
// existing ones they must cover every declaration.
async function bindParamAssignments(
    assignments: ParamAssignment[],
    decls: Map<string, CatalogParamDecl>,
    existing: Set<string>,
    context: LangBindContext,
): Promise<{ [name: string]: ParamValue }> {
    const out: { [name: string]: ParamValue } = {};
    for (const assignment of assignments) {
        const decl = decls.get(assignment.name);
        if (decl === undefined) throw new Error(`param ':${assignment.name}' is not declared by the release`);
        if (existing.has(assignment.name)) throw new Error(`param ':${assignment.name}' is already set on the database`);
        if (out[assignment.name] !== undefined) throw new Error(`param ':${assignment.name}' is set twice`);
        out[assignment.name] = await bindParamValue(assignment.value, decl, context);
    }
    const missing = [...decls.keys()].filter((name) => !existing.has(name) && out[name] === undefined).sort();
    if (missing.length > 0) {
        throw new Error(`the release needs ${missing.map((n) => `:${n}`).join(', ')}; add WITH PARAMS (${missing.map((n) => `:${n} = ...`).join(', ')})`);
    }
    return out;
}

async function bindParamValue(expr: ValueExpr, decl: CatalogParamDecl, context: LangBindContext): Promise<ParamValue> {
    if (decl.type === 'identity') {
        const creator = await resolveCreator(expr, context);
        return { identity: { keyId: creator.keyId, publicKey: serializePublicKeyToBase64(creator.publicKey) } };
    }
    const literal = asJsonLiteral(await resolveValue(expr, context));
    const value: ParamValue = { value: canonicalEncodeValue(literal, { type: decl.type } as ColumnDef) };
    if (!paramTypeFits(decl, value)) throw new Error(`param ':${decl.name}' needs a ${decl.type} value`);
    return value;
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
    const bound: BoundAlterSchema = { kind: 'alter-schema', ast, schema, rules: compileMigrationRules(ast.rules, columnsOf), author, at };
    if (ast.note !== undefined) bound.note = ast.note;
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
    // A bare table name resolves in the current group.
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
        case 'catalog':
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
    const bound: BoundLog = { kind: 'log', ast, target, at, from, explain: ast.explain === true };
    if (target.kind === 'group' && context.resolveDeployLabels !== undefined) {
        const labels = await context.resolveDeployLabels(target.id);
        if (labels !== undefined) bound.deployLabels = labels;
    }
    return bound;
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
    const columnDefs = await resolveColumnDefs(table, at);
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
            if (columnDefs[column]?.type === 'identity') {
                if (context.resolvePublicKey === undefined) {
                    throw new Error('identity #prefix resolution is not available in this host');
                }
                values[column] = (await context.resolvePublicKey(expr.prefix)).keyId;
            } else {
                if (context.resolveFkRowId === undefined) {
                    throw new Error('FK #prefix resolution is not available in this host');
                }
                values[column] = await context.resolveFkRowId(expr.prefix, table, column, at, at);
            }
        } else {
            values[column] = asJsonLiteral(await resolveValue(expr, valueContext));
        }
    }
    return { uuid: uuid ?? context.createUuid(), values: canonicalEncodeRowValues(values, columnDefs) };
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

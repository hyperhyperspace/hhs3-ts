// Compiles catalog bodies (CREATE CATALOG / ALTER CATALOG) into group
// definitions, FILES definitions and release diffs.
//
// A definition's bindings point at other definitions by hash, so a binding can
// only name a definition that is already known: one in the release's parents,
// or one added earlier in the same statement. A FILES binds a group of the
// release, so FILES items compile after every group of the statement.

import { json } from "@hyper-hyper-space/hhs3_json";
import { createBasicCrypto, HASH_SHA256, type B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { version as versionOf, type Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    catalogGroupHash, filesAccessReason, filesViewReason, groupIdentityReason, groupViewReason, rowsTableReason, rowTemplateReason,
} from "@hyper-hyper-space/hhs3_rdb";
import type {
    CatalogFilesDef, CatalogGroupChange, CatalogGroupDef, CatalogParamDecl, CatalogRowTemplate, ParentFold, Predicate,
} from "@hyper-hyper-space/hhs3_rdb";

import type {
    CatalogChangeExpr, CatalogFilesExpr, CatalogGroupExpr, CatalogParamDeclExpr, NameOrHashRef, ValueExpr,
} from "../syntax/ast.js";
import type { LangBindContext } from "../bind/context.js";
import { asJsonLiteral, canonicalEncodeValue, resolveValue } from "../bind/values.js";
import { SpannedError } from "../diagnostics.js";
import { columnSetFromTableDef, columnsOfFromSchemaView, splitExistsTableRef, type RuleScope } from "./rule_scope.js";
import { lowerRestrictionPredicate } from "./query.js";

const PSEUDO_COLUMN_UUID = 'uuid';

// The definitions a catalog body can refer to: by name (unique among them) or
// by definition-hash prefix.
export class CatalogDefScope {
    private readonly defs = new Map<B64Hash, CatalogGroupDef>();

    constructor(initial?: Map<B64Hash, CatalogGroupDef>) {
        for (const [hash, def] of initial ?? []) this.defs.set(hash, def);
    }

    add(def: CatalogGroupDef): B64Hash {
        const hash = catalogGroupHash(def);
        this.defs.set(hash, def);
        return hash;
    }

    get(hash: B64Hash): CatalogGroupDef | undefined {
        return this.defs.get(hash);
    }

    names(): Set<string> {
        return new Set([...this.defs.values()].map((def) => def.name));
    }

    resolve(ref: NameOrHashRef, what: string): B64Hash {
        if (ref.kind === 'hash') {
            const matches = [...this.defs.keys()].filter((h) => h.startsWith(ref.prefix));
            if (matches.length === 1) return matches[0];
            if (matches.length === 0) throw new Error(`${what}: no catalog group definition matches '#${ref.prefix}'`);
            throw new Error(`${what}: ambiguous catalog group definition prefix '#${ref.prefix}'`);
        }
        if (ref.parts.length !== 1) throw new Error(`${what}: expected a catalog group name, got '${ref.text}'`);
        const matches = [...this.defs.entries()].filter(([, def]) => def.name === ref.text).map(([h]) => h);
        if (matches.length === 1) return matches[0];
        if (matches.length === 0) throw new Error(`${what}: no catalog group named '${ref.text}' is defined before this point`);
        throw new Error(`${what}: '${ref.text}' names several catalog groups; use #hash (${matches.map((h) => `#${h.slice(0, 8)}`).join(', ')})`);
    }
}

export function compileParamDecls(exprs: CatalogParamDeclExpr[]): CatalogParamDecl[] {
    const seen = new Set<string>();
    return exprs.map((expr) => {
        if (seen.has(expr.name)) throw new Error(`param ':${expr.name}' is declared twice`);
        seen.add(expr.name);
        return { name: expr.name, type: expr.type };
    });
}

export async function compileCatalogGroup(
    expr: CatalogGroupExpr,
    context: LangBindContext,
    scope: CatalogDefScope,
    params: Map<string, CatalogParamDecl>,
): Promise<CatalogGroupDef> {
    const label = `TABLEGROUP ${expr.name}`;
    const schema = await context.resolveSchema(expr.schema);
    if (schema.schema === undefined) throw new Error(`${label}: schema '${schema.id}' is not loaded`);
    const pin = await context.resolveVersion(
        expr.schemaVersion ?? { kind: 'latest', span: expr.span },
        { kind: 'schema', id: schema.id, schema: schema.schema },
    );
    const view = await schema.schema.getView(pin, pin);
    const gateScope: RuleScope = { columnsOf: columnsOfFromSchemaView(view) };

    const def: CatalogGroupDef = {
        name: expr.name,
        seedSource: 'rdb',
        schemaRef: schema.id,
        schemaVersion: json.toSet([...pin]),
    };

    if (expr.bindings.length > 0) {
        const bindings: { [alias: string]: B64Hash } = {};
        for (const binding of expr.bindings) {
            if (bindings[binding.name] !== undefined) throw new Error(`${label}: binding '${binding.name}' is declared twice`);
            bindings[binding.name] = scope.resolve(binding.group, `${label} BIND ${binding.name}`);
        }
        def.bindings = bindings;
    }
    if (expr.idProvider !== undefined) def.idProvider = expr.idProvider;

    if (expr.canDeploy !== undefined) def.canDeploy = lowerRestrictionPredicate(expr.canDeploy, gateScope);
    if (expr.canObserve.length > 0) {
        const canObserve: { [binding: string]: Predicate } = {};
        for (const clause of expr.canObserve) {
            if (def.bindings?.[clause.binding] === undefined) {
                throw new Error(`${label}: ALLOW UPDATE REF names '${clause.binding}', which is not a binding`);
            }
            canObserve[clause.binding] = lowerRestrictionPredicate(clause.predicate, gateScope);
        }
        def.canObserve = canObserve;
    }

    const schemaName = schema.schema.getName();
    const viewReason = groupViewReason(def, schemaName, view);
    if (viewReason !== undefined) throw new SpannedError(`${label}: ${viewReason}`, expr.span, 'VALIDATION_REJECTED');

    if (expr.initialRows.length > 0) {
        const hashSuite = context.hashSuite?.() ?? createBasicCrypto().hash(HASH_SHA256);
        const initialRows: { [table: string]: CatalogRowTemplate[] } = {};
        for (const row of expr.initialRows) {
            const rejected = (reason: string) => new SpannedError(`${label}: ${reason}`, row.span, 'VALIDATION_REJECTED');
            const tableDef = view.getTable(row.table);
            if (tableDef === undefined) throw rejected(rowsTableReason(row.table, schemaName));
            const index = initialRows[row.table]?.length ?? 0;
            const values: { [column: string]: json.Literal } = {};
            const rowParams: NonNullable<CatalogRowTemplate['params']> = {};
            for (const { column, value } of row.values) {
                if (column === PSEUDO_COLUMN_UUID) {
                    throw new Error(`${label}: catalog rows get derived uuids; WITH ROWS cannot set 'uuid'`);
                }
                const param = rowParamOf(value, params, `${label} ${row.table}.${column}`);
                if (param !== undefined) {
                    rowParams[column] = param;
                    continue;
                }
                const literal = asJsonLiteral(await resolveValue(value, context));
                const columnDef = tableDef.columns[column];
                if (columnDef === undefined) {
                    values[column] = literal;
                    continue;
                }
                try {
                    values[column] = canonicalEncodeValue(literal, columnDef);
                } catch (e) {
                    const message = e instanceof Error ? e.message : String(e);
                    throw rejected(`${row.table} row ${index + 1} in WITH ROWS: ${column} (${columnDef.type}): ${message}`);
                }
            }
            const template: CatalogRowTemplate = { values };
            if (Object.keys(rowParams).length > 0) template.params = rowParams;
            const reason = rowTemplateReason(row.table, index, template, tableDef, view.getIdProvider(row.table), params, hashSuite);
            if (reason !== undefined) throw rejected(reason);
            (initialRows[row.table] ??= []).push(template);
        }
        def.initialRows = initialRows;
    }

    return def;
}

// A FILES item: its one binding (the group name when there is no BIND), its
// identity table and its ALLOW WRITE IF, checked against the bound group's
// schema at `groupVersion(hash)`, the group's version in this release. There
// are no local tables, so every table is `alias.<table>`.
export async function compileCatalogFiles(
    expr: CatalogFilesExpr,
    context: LangBindContext,
    scope: CatalogDefScope,
    groupVersion: (hash: B64Hash) => Version,
): Promise<CatalogFilesDef> {
    const label = `FILES ${expr.name}`;
    const rejected = (reason: string, span = expr.span) => new SpannedError(`${label}: ${reason}`, span, 'VALIDATION_REJECTED');

    const provider = splitExistsTableRef(expr.idProvider);
    if (provider.group === undefined) {
        throw rejected(`USING IDENTITIES ${expr.idProvider} must name <group>.<table>: a FILES has no tables of its own`, expr.idProviderSpan);
    }
    const alias = expr.binding?.name ?? provider.group;
    if (provider.group !== alias) {
        throw rejected(`USING IDENTITIES ${expr.idProvider} must be qualified with ${alias}, the one group a FILES binds`, expr.idProviderSpan);
    }
    const groupRef: NameOrHashRef = expr.binding?.group ?? { kind: 'name', text: alias, parts: [alias], span: expr.idProviderSpan };
    const hash = scope.resolve(groupRef, expr.binding === undefined ? label : `${label} BIND ${alias}`);
    const group = scope.get(hash)!;

    const schema = await context.resolveSchema({ kind: 'hash', prefix: group.schemaRef, span: expr.span });
    if (schema.schema === undefined) throw new Error(`${label}: schema '${group.schemaRef}' is not loaded`);
    const at = groupVersion(hash);
    const view = await schema.schema.getView(at, at);

    const columnsOf = (tableRef: string) => {
        const { group: qualifier, table } = splitExistsTableRef(tableRef);
        if (qualifier !== alias) return undefined;
        const def = view.getTable(table);
        return def === undefined ? undefined : columnSetFromTableDef(def);
    };
    const canWrite = lowerRestrictionPredicate(expr.canWrite, { columnsOf });
    for (const atom of existsAtoms(canWrite)) {
        const { group: qualifier, table } = splitExistsTableRef(atom.table);
        if (qualifier === undefined) {
            throw rejected(`ALLOW WRITE IF reads ${atom.table}: qualify it as ${alias}.${atom.table}, a FILES has no tables of its own`, expr.canWrite.span);
        }
        if (qualifier !== alias) {
            throw rejected(`ALLOW WRITE IF reads ${atom.table}: tables must be qualified with ${alias}, the one group a FILES binds`, expr.canWrite.span);
        }
        const columns = columnsOf(atom.table);
        if (columns === undefined) throw rejected(`ALLOW WRITE IF reads ${atom.table}: schema ${view.getName()} has no table ${table}`, expr.canWrite.span);
        for (const column of Object.keys(atom.where)) {
            if (!columns.has(column)) throw rejected(`ALLOW WRITE IF reads ${atom.table}.${column}, which ${table} doesn't have`, expr.canWrite.span);
        }
    }

    const def: CatalogFilesDef = { name: expr.name, bindings: { [alias]: hash }, idProvider: expr.idProvider, canWrite };
    const reason = filesAccessReason(def) ?? filesViewReason(def, view);
    if (reason !== undefined) throw rejected(reason);
    return def;
}

function existsAtoms(predicate: Predicate): { table: string; where: { [column: string]: unknown } }[] {
    const p = predicate as { p: string; table?: string; where?: { [column: string]: unknown }; args?: Predicate[] };
    if (p.p === 'exists') return [{ table: p.table!, where: p.where ?? {} }];
    if (p.p === 'and' || p.p === 'or') return (p.args ?? []).flatMap(existsAtoms);
    return [];
}

// Every FILES item of a statement, after its groups: names must be free among
// the release's groups and FILES (`taken`).
export async function compileCatalogFilesItems(
    exprs: CatalogFilesExpr[],
    context: LangBindContext,
    scope: CatalogDefScope,
    groupVersion: (hash: B64Hash) => Version,
    taken: Set<string>,
): Promise<CatalogFilesDef[]> {
    const names = new Set(taken);
    const out: CatalogFilesDef[] = [];
    for (const expr of exprs) {
        if (names.has(expr.name)) {
            throw new SpannedError(`FILES ${expr.name}: name '${expr.name}' is already used by a TABLEGROUP or FILES`, expr.span, 'VALIDATION_REJECTED');
        }
        names.add(expr.name);
        out.push(await compileCatalogFiles(expr, context, scope, groupVersion));
    }
    return out;
}

// A group definition's pin as a version.
export function pinOf(def: CatalogGroupDef): Version {
    return versionOf(...json.fromSet(def.schemaVersion));
}

// `:p` or `publicKey(:p)` in a WITH ROWS value; undefined for a literal value.
function rowParamOf(
    value: ValueExpr,
    params: Map<string, CatalogParamDecl>,
    where: string,
): { param: string; fn?: 'publicKey' } | undefined {
    const declared = (name: string) => {
        const decl = params.get(name);
        if (decl === undefined) throw new Error(`${where}: param ':${name}' is not declared (add it to PARAMS)`);
        return decl;
    };
    if (value.kind === 'param') {
        declared(value.name);
        return { param: value.name };
    }
    if (value.kind === 'call' && value.name === 'publicKey' && value.args.length === 1 && value.args[0].kind === 'param') {
        const name = value.args[0].name;
        if (declared(name).type !== 'identity') throw new Error(`${where}: publicKey(:${name}) needs an identity param`);
        return { param: name, fn: 'publicKey' };
    }
    return undefined;
}

// The body of an ALTER CATALOG, relative to the fold of its parents. A merge
// must set every group whose version differs across the parents.
export async function compileReleaseChanges(
    changes: CatalogChangeExpr[],
    fold: ParentFold,
    context: LangBindContext,
    params: Map<string, CatalogParamDecl>,
): Promise<{ changes: { [hash: string]: CatalogGroupChange }; add: CatalogGroupDef[]; files: CatalogFilesDef[] }> {
    const parentScope = new CatalogDefScope(fold.defs);
    const scope = new CatalogDefScope(fold.defs);
    const out: { [hash: string]: CatalogGroupChange } = {};
    const add: CatalogGroupDef[] = [];
    const filesExprs: CatalogFilesExpr[] = [];

    for (const change of changes) {
        if (change.kind === 'add-group') {
            const def = await compileCatalogGroup(change.group, context, scope, params);
            scope.add(def);
            add.push(def);
            continue;
        }
        if (change.kind === 'add-files') {
            filesExprs.push(change.files);
            continue;
        }
        const hash = parentScope.resolve(change.group, 'UPDATE SCHEMA ... ON');
        const def = fold.defs.get(hash)!;
        if (out[hash] !== undefined) throw new Error(`UPDATE SCHEMA ... ON ${def.name}: the group is set twice`);
        const schema = await context.resolveSchema(change.schema);
        if (schema.id !== def.schemaRef) {
            throw new Error(`UPDATE SCHEMA ... ON ${def.name}: the group uses schema '${def.schemaRef}', not '${schema.id}'`);
        }
        const version: Version = await context.resolveVersion(change.version, { kind: 'schema', id: schema.id, schema: schema.schema });
        if (schema.schema !== undefined) {
            const identityReason = groupIdentityReason(def, await schema.schema.getView(version, version));
            if (identityReason !== undefined) {
                throw new SpannedError(`UPDATE SCHEMA ... ON ${def.name}: ${identityReason}`, change.span, 'VALIDATION_REJECTED');
            }
        }
        out[hash] = { schema: schema.id, version: json.toSet([...version]) };
    }

    const unresolved = [...fold.conflicts.keys()].filter((hash) => out[hash] === undefined);
    if (unresolved.length > 0) {
        const names = unresolved.map((hash) => fold.defs.get(hash)?.name ?? hash).sort();
        throw new Error(`the parent releases disagree on the version of ${names.map((n) => `'${n}'`).join(', ')}; `
            + `add UPDATE SCHEMA schema TO version ON group for each`);
    }

    const groupVersion = (hash: B64Hash): Version => {
        const changed = out[hash];
        if (changed !== undefined) return versionOf(...json.fromSet(changed.version));
        const parent = fold.groups.get(hash);
        if (parent !== undefined) return versionOf(...parent.version);
        return pinOf(scope.get(hash)!);
    };
    const taken = new Set([...scope.names(), ...[...fold.files.values()].map((def) => def.name)]);
    const files = await compileCatalogFilesItems(filesExprs, context, scope, groupVersion, taken);

    return { changes: out, add, files };
}

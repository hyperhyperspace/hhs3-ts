// Compiles catalog bodies (CREATE CATALOG / ALTER CATALOG) into group
// definitions and release diffs.
//
// A definition's bindings point at other definitions by hash, so a binding can
// only name a definition that is already known: one in the release's parents,
// or one added earlier in the same statement.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    catalogGroupHash, predicateReferencesAuthor,
} from "@hyper-hyper-space/hhs3_rdb";
import type {
    CatalogGroupChange, CatalogGroupDef, CatalogParamDecl, CatalogRowTemplate, ParentFold, Predicate,
} from "@hyper-hyper-space/hhs3_rdb";

import type {
    CatalogChangeExpr, CatalogGroupExpr, CatalogParamDeclExpr, NameOrHashRef, ValueExpr,
} from "../syntax/ast.js";
import type { LangBindContext } from "../bind/context.js";
import { asJsonLiteral, canonicalEncodeRowValues, resolveValue } from "../bind/values.js";
import { columnsOfFromSchemaView, type RuleScope } from "./rule_scope.js";
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

    if (expr.canDeploy !== undefined) {
        const canDeploy = lowerRestrictionPredicate(expr.canDeploy, gateScope);
        if (expr.idProvider === undefined && predicateReferencesAuthor(canDeploy)) {
            throw new Error(`${label}: ALLOW DEPLOY IF references $author, which needs USING IDENTITIES to verify deploy signatures`);
        }
        def.canDeploy = canDeploy;
    }
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

    if (expr.initialRows.length > 0) {
        const initialRows: { [table: string]: CatalogRowTemplate[] } = {};
        for (const row of expr.initialRows) {
            const columns = view.getTable(row.table)?.columns;
            if (columns === undefined) throw new Error(`${label}: WITH ROWS names unknown table '${row.table}'`);
            const values: { [column: string]: json.Literal } = {};
            const rowParams: NonNullable<CatalogRowTemplate['params']> = {};
            for (const { column, value } of row.values) {
                if (column === PSEUDO_COLUMN_UUID) {
                    throw new Error(`${label}: catalog rows get derived uuids; WITH ROWS cannot set 'uuid'`);
                }
                const param = rowParamOf(value, params, `${label} ${row.table}.${column}`);
                if (param !== undefined) {
                    rowParams[column] = param;
                } else {
                    values[column] = asJsonLiteral(await resolveValue(value, context));
                }
            }
            const template: CatalogRowTemplate = { values: canonicalEncodeRowValues(values, columns) };
            if (Object.keys(rowParams).length > 0) template.params = rowParams;
            (initialRows[row.table] ??= []).push(template);
        }
        def.initialRows = initialRows;
    }

    return def;
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
): Promise<{ changes: { [hash: string]: CatalogGroupChange }; add: CatalogGroupDef[] }> {
    const parentScope = new CatalogDefScope(fold.defs);
    const scope = new CatalogDefScope(fold.defs);
    const out: { [hash: string]: CatalogGroupChange } = {};
    const add: CatalogGroupDef[] = [];

    for (const change of changes) {
        if (change.kind === 'add-group') {
            const def = await compileCatalogGroup(change.group, context, scope, params);
            scope.add(def);
            add.push(def);
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
        out[hash] = { schema: schema.id, version: json.toSet([...version]) };
    }

    const unresolved = [...fold.conflicts.keys()].filter((hash) => out[hash] === undefined);
    if (unresolved.length > 0) {
        const names = unresolved.map((hash) => fold.defs.get(hash)?.name ?? hash).sort();
        throw new Error(`the parent releases disagree on the version of ${names.map((n) => `'${n}'`).join(', ')}; `
            + `add UPDATE SCHEMA schema TO version ON group for each`);
    }

    return { changes: out, add };
}

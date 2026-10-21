// Validation for RSchema payloads and schema model elements.
//
//   validateRSchemaPayloadFormat — json.Format + position-independent semantics
//   validate_ops.ts              — position-dependent semantics (signatures, applicability)

import { json } from "@hyper-hyper-space/hhs3_json";
import { validationFailure, validationOk, ValidationResult } from "@hyper-hyper-space/hhs3_mvt";

import {
    createRSchemaFormat, CreateRSchemaPayload,
    schemaUpdateFormat, SchemaUpdatePayload,
    ColumnDef, ColumnConstraints, ColumnType, FKs, IdProvider, IdTerm, MigrationRule, Predicate, PredicateContext,
    Restriction, RuleOperand, SchemaParamDecl, TableDef,
    MAX_FKS, MAX_NAME_LENGTH, MAX_QUALIFIED_NAME_LENGTH, MAX_RESTRICTIONS,
    ID_TERMS, CMP_OPS, CmpOp,
} from "./payload.js";

import { splitTableRef, parseParamTerm, parseRowFieldTerm } from "./payload.js";
import type { RSchemaView } from "./interfaces.js";
import { cmpTypesOk, likeTypesOk, isValidLikePattern } from "./expr.js";
import { isValidSemver } from "../semver.js";
import {
    isCanonicalBigint, isCanonicalDecimal, isCanonicalBase64, base64ByteLen,
    normalizeBigint, normalizeDecimal, intInRange, bigintInRange, decInRange, compareNumericStr,
} from "./canonical.js";

export const MAX_WHERE_FIELDS = 64;
export const MAX_EXPR_DEPTH = 16;
export const MAX_EXPR_ARGS = 64;

// Table and column names: identifier-like, to keep them exportable to real
// relational databases later (rdb_adapter). Table references may be qualified
// with an RDb-namespace group name: 'group.table'.
const NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
const QUALIFIED_NAME_REGEX = /^([a-zA-Z_][a-zA-Z0-9_]*\.)?[a-zA-Z_][a-zA-Z0-9_]*$/;
const SCHEMA_NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*(:[a-zA-Z_][a-zA-Z0-9_]*)*$/;

export function isValidName(name: string): boolean {
    return name.length <= MAX_NAME_LENGTH && NAME_REGEX.test(name);
}

export function isValidSchemaName(name: string): boolean {
    return name.length <= MAX_NAME_LENGTH && SCHEMA_NAME_REGEX.test(name);
}

export function isValidTableRef(ref: string): boolean {
    return ref.length <= MAX_QUALIFIED_NAME_LENGTH && QUALIFIED_NAME_REGEX.test(ref);
}

// undefined = valid; string = human-readable reason
export type ValidateReason = string | undefined;

function invalidNameReason(name: string, kind: 'table' | 'column'): string {
    if (name.includes(':')) {
        return `invalid ${kind} name '${name}' (':' is only allowed in schema names)`;
    }
    if (name.length > MAX_NAME_LENGTH) {
        return `invalid ${kind} name '${name}' (exceeds max length of ${MAX_NAME_LENGTH})`;
    }
    return `invalid ${kind} name '${name}'`;
}

// Coarse type check (carrier only; no constraints). For decimal it can only
// confirm the carrier is a string, since the canonical form needs the column
// scale — use columnValueValid for the authoritative, constraint-aware check.
export function columnValueMatchesType(value: json.Literal, type: ColumnType): boolean {
    switch (type) {
        case 'string': return typeof value === 'string';
        case 'integer': return typeof value === 'number' && Number.isSafeInteger(value);
        case 'float': return typeof value === 'number' && Number.isFinite(value);
        case 'boolean': return typeof value === 'boolean';
        case 'json': return json.isLiteral(value);
        case 'bigint': return typeof value === 'string' && isCanonicalBigint(value);
        case 'decimal': return typeof value === 'string';
        case 'bytes': return typeof value === 'string' && isCanonicalBase64(value);
        case 'identity': return typeof value === 'string' && value.length > 0;
    }
}

// Names the carrier (JS runtime shape) of a value, for "expected X, got Y"
// diagnostics.
function carrierName(value: json.Literal): string {
    if (value === undefined) return 'undefined';
    if (value === null) return 'null';
    if (Array.isArray(value)) return 'array';
    return typeof value;   // 'string' | 'number' | 'boolean' | 'object'
}

// Renders an inclusive [min, max] range for range-violation diagnostics.
function rangeDesc(c: ColumnConstraints | undefined): string {
    return `[${c?.min ?? '-inf'}, ${c?.max ?? '+inf'}]`;
}

// Authoritative value check that returns a human-readable reason when a value
// is rejected (undefined = valid). This is the Layer-1 write-time gate — it
// distinguishes carrier mismatch, non-canonical form, length, range, and scale
// failures so callers can surface a precise diagnostic instead of a generic
// "does not match declared type". See ../rtable/validate_ops.ts.
export function columnValueValidReason(value: json.Literal, def: ColumnDef): ValidateReason {
    const c = def.constraints;
    switch (def.type) {
        case 'string':
            if (typeof value !== 'string') return `expected a string, got ${carrierName(value)}`;
            if (c?.maxLength !== undefined && value.length > c.maxLength) {
                return `string length ${value.length} exceeds maxLength ${c.maxLength}`;
            }
            return undefined;
        case 'integer':
            if (typeof value !== 'number' || !Number.isSafeInteger(value)) {
                return `expected a safe integer, got ${carrierName(value)}`;
            }
            if (!intInRange(value, c)) return `integer ${value} is out of range ${rangeDesc(c)}`;
            return undefined;
        case 'float':
            if (typeof value !== 'number' || !Number.isFinite(value)) {
                return `expected a finite number, got ${carrierName(value)}`;
            }
            return undefined;
        case 'boolean':
            if (typeof value !== 'boolean') return `expected a boolean, got ${carrierName(value)}`;
            return undefined;
        case 'json':
            if (value === undefined || value === null) return `expected a JSON value, got ${carrierName(value)}`;
            if (!json.isLiteral(value)) return 'JSON values cannot contain null';
            return undefined;
        case 'bigint':
            if (typeof value !== 'string') return `expected a bigint string, got ${carrierName(value)}`;
            if (!isCanonicalBigint(value)) {
                return `'${value}' is not a canonical bigint (expected an integer with no leading zeros, '+', or '-0')`;
            }
            if (!bigintInRange(value, c)) return `bigint ${value} is out of range ${rangeDesc(c)}`;
            return undefined;
        case 'decimal':
            if (typeof value !== 'string') return `expected a decimal string, got ${carrierName(value)}`;
            if (c?.scale === undefined) return `decimal column is missing its scale constraint`;
            if (!isCanonicalDecimal(value, c.scale, c.precision)) {
                const p = c.precision !== undefined ? `, precision=${c.precision}` : '';
                return `'${value}' is not a canonical decimal(scale=${c.scale}${p})`;
            }
            if (!decInRange(value, c)) return `decimal ${value} is out of range ${rangeDesc(c)}`;
            return undefined;
        case 'bytes':
            if (typeof value !== 'string') return `expected a base64 bytes string, got ${carrierName(value)}`;
            if (!isCanonicalBase64(value)) return `value is not canonical base64`;
            if (c?.maxLength !== undefined && base64ByteLen(value) > c.maxLength) {
                return `byte length ${base64ByteLen(value)} exceeds maxLength ${c.maxLength}`;
            }
            return undefined;
        case 'identity':
            if (typeof value !== 'string') return `expected an identity (key hash string), got ${carrierName(value)}`;
            if (value.length === 0) return `identity key hash must be non-empty`;
            return undefined;
    }
}

// Authoritative boolean check: carrier + canonical form + constraints. Thin
// wrapper over columnValueValidReason for callers that only need a verdict.
export function columnValueValid(value: json.Literal, def: ColumnDef): boolean {
    return columnValueValidReason(value, def) === undefined;
}

function isValidTerm(value: json.Literal, context: PredicateContext): boolean {
    if (!ID_TERMS.includes(value as IdTerm)) return false;
    return true;
}

// A `where` value that is a reserved ($-prefixed) string must be either a valid
// identity term ($author), a subject-row field term ($row.<col>), or a schema
// parameter term ($param.<name>); $row.* and $param.* are row-context only.
// Non-$ values are plain literals (checked here as "not reserved", i.e. accepted).
function isValidWhereValue(value: json.Literal, context: PredicateContext): boolean {
    if (typeof value !== 'string') return json.isLiteral(value);
    if (!value.startsWith('$')) return true;
    if (ID_TERMS.includes(value as IdTerm)) return isValidTerm(value, context);
    if (parseRowFieldTerm(value) !== undefined) return context === 'row';
    if (parseParamTerm(value) !== undefined) return context === 'row';
    return false;   // any other $-string is reserved and not a known term
}

// Structural check for a value-expression operand. `{col}` ($row.<col>) and
// `{param}` (:param) are row-context only; arithmetic fns are integer-only and
// len takes one arg.
function validateOperand(op: json.Literal, context: PredicateContext, depth: number): boolean {
    if (depth > MAX_EXPR_DEPTH) return false;
    if (typeof op !== 'object' || op === null || Array.isArray(op)) return false;

    const e = op as { [key: string]: json.Literal };
    const keys = Object.keys(e);

    if ('lit' in e) {
        if (keys.length !== 1) return false;
        // $-strings are reserved for terms, as in exists where-values
        const lit = e['lit'];
        if (typeof lit !== 'string') return json.isLiteral(lit);
        return !lit.startsWith('$') || isValidTerm(lit, context);
    }
    if ('col' in e) {
        if (context !== 'row') return false;   // no subject row
        return keys.length === 1 && typeof e['col'] === 'string' && isValidName(e['col']);
    }
    if ('param' in e) {
        if (context !== 'row') return false;   // schema params are row-context only
        return keys.length === 1 && typeof e['param'] === 'string' && isValidName(e['param']);
    }
    if ('fn' in e) {
        if (keys.length !== 2 || !Array.isArray(e['args'])) return false;
        const args = e['args'] as json.Literal[];
        const fn = e['fn'];
        if (fn === 'add' || fn === 'sub' || fn === 'mul') {
            if (args.length !== 2) return false;
        } else if (fn === 'len') {
            if (args.length !== 1) return false;
        } else {
            return false;
        }
        return args.every((a) => validateOperand(a, context, depth + 1));
    }
    return false;
}

export function validatePredicate(pred: json.Literal, context: PredicateContext = 'row', depth: number = 0): boolean {
    if (depth > MAX_EXPR_DEPTH) return false;
    if (typeof pred !== 'object' || pred === null || Array.isArray(pred)) return false;

    const e = pred as { [key: string]: json.Literal };
    const keys = Object.keys(e);

    switch (e['p']) {
        case 'true':
        case 'false':
            return keys.length === 1;
        case 'exists': {
            if (typeof e['table'] !== 'string' || !isValidTableRef(e['table'])) return false;
            if (typeof e['where'] !== 'object' || e['where'] === null || Array.isArray(e['where'])) return false;
            const where = e['where'] as json.LiteralMap;
            const fields = Object.keys(where);
            if (fields.length === 0 || fields.length > MAX_WHERE_FIELDS) return false;
            for (const field of fields) {
                if (!isValidName(field)) return false;
                // strings starting with '$' are reserved for terms
                // ($author / $row.<col>); others are literals
                if (!isValidWhereValue(where[field], context)) return false;
            }
            for (const key of keys) {
                if (!['p', 'table', 'where'].includes(key)) return false;
            }
            return true;
        }
        case 'cmp': {
            if (keys.length !== 4) return false;
            if (!CMP_OPS.includes(e['cmp'] as CmpOp)) return false;
            return validateOperand(e['left'], context, depth + 1)
                && validateOperand(e['right'], context, depth + 1);
        }
        case 'like': {
            if (keys.length !== 3) return false;
            if (!validateOperand(e['value'], context, depth + 1)) return false;
            if (!validateOperand(e['pattern'], context, depth + 1)) return false;
            const pattern = e['pattern'] as { [key: string]: json.Literal };
            return !('lit' in pattern) || typeof pattern['lit'] !== 'string' || isValidLikePattern(pattern['lit']);
        }
        case 'and':
        case 'or': {
            if (keys.length !== 2 || !Array.isArray(e['args'])) return false;
            const args = e['args'] as json.Literal[];
            // a one-member group means the same as its member: not canonical
            if (args.length < 2 || args.length > MAX_EXPR_ARGS) return false;
            return args.every((a) => validatePredicate(a, context, depth + 1));
        }
        default:
            return false;
    }
}

function operandReferencesAuthor(op: RuleOperand): boolean {
    if ('lit' in op) return op.lit === '$author';
    if ('fn' in op) return op.args.some(operandReferencesAuthor);
    return false;
}

// Whether a predicate reads `$author`, through a `{lit: '$author'}` operand or
// a `'$author'` exists where-value. Such a predicate is only sound when the
// author is signature-verified, so it needs a key source.
export function predicateReferencesAuthor(pred: Predicate): boolean {
    switch (pred.p) {
        case 'cmp':
            return operandReferencesAuthor(pred.left) || operandReferencesAuthor(pred.right);
        case 'like':
            return operandReferencesAuthor(pred.value) || operandReferencesAuthor(pred.pattern);
        case 'exists':
            return Object.values(pred.where ?? {}).some((value) => value === '$author');
        case 'and':
        case 'or':
            return pred.args.some(predicateReferencesAuthor);
        default:
            return false;
    }
}

// The first restriction a schema view declares that reads `$author`, or
// undefined. Default rules are not declared, so they never count. A group
// without an idProvider admits only anonymous ops, so such a rule could never
// pass there.
export function findAuthorRestriction(view: RSchemaView): { table: string; restriction: Restriction } | undefined {
    for (const table of view.getTableNames()) {
        for (const restriction of view.getTable(table)?.restrictions ?? []) {
            if (predicateReferencesAuthor(restriction.rule)) return { table, restriction };
        }
    }
    return undefined;
}

export type ExistsAtom = Extract<Predicate, { p: 'exists' }>;
type CmpAtom = Extract<Predicate, { p: 'cmp' }>;
type LikeAtom = Extract<Predicate, { p: 'like' }>;

export function collectExistsAtoms(pred: Predicate, out: ExistsAtom[] = []): ExistsAtom[] {
    if (pred.p === 'exists') out.push(pred);
    if (pred.p === 'and' || pred.p === 'or') {
        for (const arg of pred.args) collectExistsAtoms(arg, out);
    }
    return out;
}

export function collectCmpLikeAtoms(pred: Predicate, out: (CmpAtom | LikeAtom)[] = []): (CmpAtom | LikeAtom)[] {
    if (pred.p === 'cmp' || pred.p === 'like') out.push(pred);
    if (pred.p === 'and' || pred.p === 'or') {
        for (const arg of pred.args) collectCmpLikeAtoms(arg, out);
    }
    return out;
}

function collectOperandCols(op: RuleOperand, out: Set<string>): void {
    if ('col' in op) out.add(op.col);
    else if ('fn' in op) for (const a of op.args) collectOperandCols(a, out);
}

function collectOperandParams(op: RuleOperand, out: Set<string>): void {
    if ('param' in op) out.add(op.param);
    else if ('fn' in op) for (const a of op.args) collectOperandParams(a, out);
}

// Every subject-row column referenced by the predicate: via `{col}` operands in
// cmp/like atoms and via `$row.<col>` exists where-values.
export function collectRowFieldRefs(pred: Predicate, out: Set<string> = new Set()): Set<string> {
    switch (pred.p) {
        case 'cmp':
            collectOperandCols(pred.left, out);
            collectOperandCols(pred.right, out);
            break;
        case 'like':
            collectOperandCols(pred.value, out);
            collectOperandCols(pred.pattern, out);
            break;
        case 'exists':
            for (const value of Object.values(pred.where ?? {})) {
                if (typeof value === 'string') {
                    const col = parseRowFieldTerm(value);
                    if (col !== undefined) out.add(col);
                }
            }
            break;
        case 'and':
        case 'or':
            for (const arg of pred.args) collectRowFieldRefs(arg, out);
            break;
    }
    return out;
}

// Every schema parameter referenced by the predicate: via `{param}` operands
// in cmp/like atoms and via `$param.<name>` exists where-values.
export function collectParamRefs(pred: Predicate, out: Set<string> = new Set()): Set<string> {
    switch (pred.p) {
        case 'cmp':
            collectOperandParams(pred.left, out);
            collectOperandParams(pred.right, out);
            break;
        case 'like':
            collectOperandParams(pred.value, out);
            collectOperandParams(pred.pattern, out);
            break;
        case 'exists':
            for (const value of Object.values(pred.where ?? {})) {
                if (typeof value === 'string') {
                    const param = parseParamTerm(value);
                    if (param !== undefined) out.add(param);
                }
            }
            break;
        case 'and':
        case 'or':
            for (const arg of pred.args) collectParamRefs(arg, out);
            break;
    }
    return out;
}

// Column-type lookup for the declaring table (operand type-checking).
function columnTypeOf(columns: { [c: string]: ColumnDef }): (column: string) => ColumnType | undefined {
    return (column) => column === 'rowAuthor' ? 'string' : columns[column]?.type;
}

function whereColumnTypeMatches(a: ColumnType, b: ColumnType): boolean {
    if (a === b) return true;
    return (a === 'identity' && b === 'string') || (a === 'string' && b === 'identity');
}

// Tier 1+2 column-level checks for one restriction rule declared on `def`:
//   - every $row.<col> reference names an existing READONLY column of `def`;
//   - every $param.<name> / {param} reference names a declared schema param;
//   - cmp/like operand types are coherent over `def`'s columns and schema params;
//   - an exists where-value $row.<col> or $param.<name> matches the (local)
//     target field's type (identity also matches string; the target field's
//     pub-ness is enforced separately by the exists check).
// Foreign (group.table) exists targets are skipped (checked at binding time).
export function checkPredicateColumns(
    def: TableDef,
    rule: Predicate,
    resolveLocalTable: (table: string) => TableDef | undefined,
    resolveParam?: (name: string) => ColumnType | undefined,
): ValidateReason {
    for (const col of collectRowFieldRefs(rule)) {
        if (col === 'rowAuthor') continue;
        const cd = def.columns[col];
        if (cd === undefined) return `$row field '${col}' not found in table '${def.name}'`;
        if (!(cd.readonly ?? false)) return `$row field '${col}' in table '${def.name}' is not readonly`;
    }

    for (const param of collectParamRefs(rule)) {
        if (resolveParam?.(param) === undefined) {
            return `parameter ':${param}' not declared in schema (referenced in table '${def.name}')`;
        }
    }

    const typeOf = columnTypeOf(def.columns);
    for (const atom of collectCmpLikeAtoms(rule)) {
        if (atom.p === 'cmp') {
            if (!cmpTypesOk(atom.cmp, atom.left, atom.right, typeOf, resolveParam)) {
                return `cmp operand types are incompatible in table '${def.name}'`;
            }
        } else if (!likeTypesOk(atom.value, atom.pattern, typeOf, resolveParam)) {
            return `like operand types are incompatible in table '${def.name}'`;
        }
    }

    for (const atom of collectExistsAtoms(rule)) {
        const [group, table] = splitTableRef(atom.table);
        if (group !== undefined) continue;
        const target = resolveLocalTable(table);
        if (target === undefined) return `exists target table '${table}' not found in schema`;
        for (const [field, value] of Object.entries(atom.where ?? {})) {
            if (typeof value !== 'string') continue;
            const col = parseRowFieldTerm(value);
            if (col !== undefined) {
                const subjectCol = def.columns[col];
                const targetCol = target.columns[field];
                if (subjectCol === undefined) return `$row field '${col}' not found in table '${def.name}'`;
                if (targetCol === undefined) return `exists where field '${field}' not found in table '${table}'`;
                if (!whereColumnTypeMatches(subjectCol.type, targetCol.type)) {
                    return `$row field '${col}' type '${subjectCol.type}' does not match exists where field '${field}' type '${targetCol.type}'`;
                }
                continue;
            }
            const param = parseParamTerm(value);
            if (param !== undefined) {
                const paramType = resolveParam?.(param);
                if (paramType === undefined) return `parameter ':${param}' not declared in schema (referenced in table '${def.name}')`;
                if (field === 'rowAuthor') {
                    if (!whereColumnTypeMatches(paramType, 'string')) {
                        return `parameter ':${param}' type '${paramType}' does not match exists where field 'rowAuthor'`;
                    }
                    continue;
                }
                const targetCol = target.columns[field];
                if (targetCol === undefined) return `exists where field '${field}' not found in table '${table}'`;
                if (!whereColumnTypeMatches(paramType, targetCol.type)) {
                    return `parameter ':${param}' type '${paramType}' does not match exists where field '${field}' type '${targetCol.type}'`;
                }
            }
        }
    }

    return undefined;
}

// Which constraint keys are applicable per column type. Any OTHER key present
// (with a defined value) is a hard reject, to prevent silent fungibility.
const ALLOWED_CONSTRAINTS: { [t in ColumnType]: (keyof ColumnConstraints)[] } = {
    string: ['maxLength'],
    bytes: ['maxLength'],
    integer: ['min', 'max'],
    bigint: ['min', 'max'],
    decimal: ['scale', 'precision', 'min', 'max'],
    float: [],
    boolean: [],
    json: [],
    identity: [],
};

// A min/max bound must be a canonical value of the column type.
function validateBound(def: ColumnDef, bound: string | undefined, which: 'min' | 'max'): ValidateReason {
    if (bound === undefined) return undefined;
    switch (def.type) {
        case 'integer':
        case 'bigint':
            if (normalizeBigint(bound) !== bound) return `constraint '${which}' must be a canonical integer string`;
            return undefined;
        case 'decimal': {
            const scale = def.constraints?.scale ?? 0;
            if (normalizeDecimal(bound, scale) !== bound) {
                return `constraint '${which}' must be a canonical decimal string at scale ${scale}`;
            }
            return undefined;
        }
        default:
            return undefined;
    }
}

function compareBound(def: ColumnDef, a: string, b: string): number {
    return compareNumericStr(a, b, def.type === 'decimal' ? 'decimal' : 'bigint');
}

// Omitting a flag already means false, so an explicit false is a second
// spelling of the same column def and is rejected.
const COLUMN_FLAGS = ['nullable', 'pub', 'readonly'] as const;

export function validateColumnDef(def: ColumnDef): ValidateReason {
    for (const flag of COLUMN_FLAGS) {
        if (def[flag] === false) return `flag '${flag}' must be omitted rather than set to false`;
    }
    const c = def.constraints;
    if (c !== undefined) {
        const allowed = ALLOWED_CONSTRAINTS[def.type];
        for (const key of Object.keys(c) as (keyof ColumnConstraints)[]) {
            if (c[key] === undefined) continue;
            if (!allowed.includes(key)) {
                return `constraint '${key}' is not applicable to column type '${def.type}'`;
            }
        }
        if (c.maxLength !== undefined && (!Number.isInteger(c.maxLength) || c.maxLength <= 0)) {
            return `constraint 'maxLength' must be a positive integer`;
        }
        if (def.type === 'decimal') {
            if (c.scale === undefined || !Number.isInteger(c.scale) || c.scale < 0) {
                return `decimal column requires an integer constraints.scale >= 0`;
            }
            if (c.precision !== undefined) {
                if (!Number.isInteger(c.precision) || c.precision < 1) {
                    return `constraint 'precision' must be a positive integer`;
                }
                if (c.precision < c.scale) {
                    return `constraint 'precision' must be >= scale`;
                }
            }
        }
        const boundReason = validateBound(def, c.min, 'min') ?? validateBound(def, c.max, 'max');
        if (boundReason !== undefined) return boundReason;
        if (c.min !== undefined && c.max !== undefined && compareBound(def, c.min, c.max) > 0) {
            return `constraint 'min' must be <= 'max'`;
        }
    } else if (def.type === 'decimal') {
        return `decimal column requires an integer constraints.scale >= 0`;
    }

    if (def.default !== undefined && !columnValueValid(def.default, def)) {
        return `default value does not satisfy column type '${def.type}'${c !== undefined ? ' and its constraints' : ''}`;
    }
    return undefined;
}

// `columns` is the declaring (subject) table's columns: FK columns must exist in it.
export function validateFKs(fks: FKs, columns?: { [column: string]: ColumnDef }): ValidateReason {
    const entries = Object.entries(fks);
    if (entries.length > MAX_FKS) return `too many foreign keys (max ${MAX_FKS})`;
    for (const [column, target] of entries) {
        if (!isValidName(column)) return invalidNameReason(column, 'column');
        if (!isValidTableRef(target)) return `invalid FK target '${target}'`;
        if (columns !== undefined && columns[column] === undefined) return `FK column '${column}' does not exist`;
    }
    return undefined;
}

// At most one restriction per op tag, and 'all' is exclusive: every op has at
// most one governing rule, so a rule set has a single canonical form.
export function validateRestrictions(restrictions: Restriction[]): ValidateReason {
    const seen = new Set<string>();
    for (const [index, restriction] of restrictions.entries()) {
        if (!validatePredicate(restriction.rule)) return `invalid restriction predicate at index ${index}`;
        if (seen.has(restriction.on)) return `duplicate ALLOW ${restriction.on} rule`;
        seen.add(restriction.on);
    }
    if (seen.has('all') && seen.size > 1) {
        return 'ALLOW all cannot be combined with operation-specific ALLOW rules';
    }
    return undefined;
}

// An identity provider designates two columns of the SAME table as the keyId
// and publicKey columns. Both must exist, be string-typed, be pub AND readonly
// (a mutable provider key would make at-append signature verification depend
// on resolution order; pub so the keyId is searchable in the cover read), and
// be distinct.
export function validateIdProvider(provider: IdProvider, columns: { [column: string]: ColumnDef }): ValidateReason {
    if (!isValidName(provider.keyIdColumn)) return invalidNameReason(provider.keyIdColumn, 'column');
    if (!isValidName(provider.publicKeyColumn)) return invalidNameReason(provider.publicKeyColumn, 'column');
    if (provider.keyIdColumn === provider.publicKeyColumn) return 'idProvider keyIdColumn and publicKeyColumn must differ';

    for (const column of [provider.keyIdColumn, provider.publicKeyColumn]) {
        const def = columns[column];
        if (def === undefined) return `idProvider column '${column}' does not exist`;
        if (def.type !== 'string') return `idProvider column '${column}' must be string-typed`;
        if (!(def.pub ?? false)) return `idProvider column '${column}' must be pub`;
        if (!(def.readonly ?? false)) return `idProvider column '${column}' must be readonly`;
    }
    return undefined;
}

export function validateTableDef(def: TableDef): ValidateReason {
    if (!isValidName(def.name)) return invalidNameReason(def.name, 'table');

    const columnNames = Object.keys(def.columns);
    if (columnNames.length === 0) return `table '${def.name}' has no columns`;

    for (const column of columnNames) {
        if (!isValidName(column)) return `${invalidNameReason(column, 'column')} in table '${def.name}'`;
        const colReason = validateColumnDef(def.columns[column]);
        if (colReason !== undefined) return `column '${column}' in table '${def.name}': ${colReason}`;
    }

    if (def.fks !== undefined) {
        const fkReason = validateFKs(def.fks, def.columns);
        if (fkReason !== undefined) return `table '${def.name}': ${fkReason}`;
    }
    if (def.restrictions !== undefined) {
        const restReason = validateRestrictions(def.restrictions);
        if (restReason !== undefined) return `table '${def.name}': ${restReason}`;
    }
    if (def.idProvider !== undefined) {
        const idReason = validateIdProvider(def.idProvider, def.columns);
        if (idReason !== undefined) return `table '${def.name}': ${idReason}`;
    }

    return undefined;
}

export function validateSchemaParams(params: SchemaParamDecl[], requireDefault: boolean): ValidateReason {
    const seen = new Set<string>();
    for (const decl of params) {
        if (!isValidName(decl.name)) return `invalid parameter name '${decl.name}'`;
        if (decl.type === 'decimal') return `parameter ':${decl.name}' cannot be decimal: a param has no scale`;
        if (seen.has(decl.name)) return `duplicate parameter name ':${decl.name}'`;
        seen.add(decl.name);
        if (requireDefault && decl.default === undefined) {
            return `parameter ':${decl.name}' declared by a schema update requires a default value`;
        }
        if (decl.default !== undefined) {
            if (!columnValueMatchesType(decl.default, decl.type)) {
                return `default value for parameter ':${decl.name}' does not match type '${decl.type}'`;
            }
            if (typeof decl.default === 'string' && decl.default.startsWith('$')) {
                return `default value for parameter ':${decl.name}' cannot start with '$'`;
            }
        }
    }
    return undefined;
}

// Validates a full set of table defs: each def is valid, names are unique,
// local (unqualified) FK and exists-atom targets exist in the set, and exists
// `where` fields are pub columns of the local target.
export function validateSchemaTables(tables: TableDef[], params?: SchemaParamDecl[]): ValidateReason {
    const names = new Set<string>();
    const byName = new Map<string, TableDef>();
    const paramTypes = new Map<string, ColumnType>((params ?? []).map((p) => [p.name, p.type]));
    const resolveParam = (name: string) => paramTypes.get(name);

    for (const def of tables) {
        const defReason = validateTableDef(def);
        if (defReason !== undefined) return defReason;
        if (names.has(def.name)) return `duplicate table name '${def.name}'`;
        names.add(def.name);
        byName.set(def.name, def);
    }

    const checkExistsAtom = (atom: ExistsAtom): ValidateReason => {
        const [group, table] = splitTableRef(atom.table);
        if (group !== undefined) return undefined;   // checked at binding time

        const target = byName.get(table);
        if (target === undefined) return `exists target table '${table}' not found in schema`;

        if (atom.where !== undefined) {
            for (const field of Object.keys(atom.where)) {
                if (field === 'rowAuthor') continue;
                const column = target.columns[field];
                if (column === undefined) return `exists where field '${field}' not found in table '${table}'`;
                if (!(column.pub ?? false)) return `exists where field '${field}' in table '${table}' is not pub`;
            }
        }
        return undefined;
    };

    for (const def of tables) {
        for (const target of Object.values(def.fks ?? {})) {
            const [group, table] = splitTableRef(target);
            if (group === undefined && !names.has(table)) {
                return `FK target '${table}' not found in schema (referenced from table '${def.name}')`;
            }
        }

        for (const restriction of def.restrictions ?? []) {
            for (const atom of collectExistsAtoms(restriction.rule)) {
                const atomReason = checkExistsAtom(atom);
                if (atomReason !== undefined) return `table '${def.name}': ${atomReason}`;
            }
            const predReason = checkPredicateColumns(def, restriction.rule, (t) => byName.get(t), resolveParam);
            if (predReason !== undefined) return `table '${def.name}': ${predReason}`;
        }
    }

    return undefined;
}

export function validateMigrationRule(rule: MigrationRule): ValidateReason {
    switch (rule.rule) {
        case 'add-table': {
            return validateTableDef(rule.def);
        }
        case 'drop-table': {
            if (!isValidName(rule.table)) return invalidNameReason(rule.table, 'table');
            return undefined;
        }
        case 'add-column': {
            if (!isValidName(rule.table)) return invalidNameReason(rule.table, 'table');
            if (!isValidName(rule.column)) return invalidNameReason(rule.column, 'column');
            const colReason = validateColumnDef(rule.def);
            if (colReason !== undefined) return `column '${rule.column}': ${colReason}`;
            // a new non-nullable column needs a default to revise old rows
            if (!(rule.def.nullable ?? false) && rule.def.default === undefined) {
                return `adding non-nullable column '${rule.column}' without default is not allowed`;
            }
            return undefined;
        }
        case 'drop-column': {
            if (!isValidName(rule.table)) return invalidNameReason(rule.table, 'table');
            if (!isValidName(rule.column)) return invalidNameReason(rule.column, 'column');
            return undefined;
        }
        case 'set-concurrent-deletes': {
            if (!isValidName(rule.table)) return invalidNameReason(rule.table, 'table');
            return undefined;
        }
        case 'set-fks': {
            if (!isValidName(rule.table)) return invalidNameReason(rule.table, 'table');
            return validateFKs(rule.fks);
        }
        case 'set-restrictions': {
            if (!isValidName(rule.table)) return invalidNameReason(rule.table, 'table');
            return validateRestrictions(rule.restrictions);
        }
        default:
            return `unknown migration rule '${String((rule as MigrationRule).rule)}'`;
    }
}

export function validateRSchemaPayloadFormat(payload: json.Literal): ValidationResult {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
        return validationFailure("RSchema payload must be an object");
    }

    const action = (payload as json.LiteralMap)['action'];

    if (action === 'create') {
        if (!json.checkFormat(createRSchemaFormat, payload)) return validationFailure("RSchema create payload format is invalid");
        const create = payload as CreateRSchemaPayload;
        if (!isValidSemver(create.version)) return validationFailure(`RSchema create version '${create.version}' is not a semver`);
        if (create.creators.length === 0) return validationFailure("RSchema create payload must have at least one creator");
        if (create.params !== undefined) {
            if (create.params.length === 0) return validationFailure("RSchema create params must be omitted when empty");
            const paramsReason = validateSchemaParams(create.params, false);
            if (paramsReason !== undefined) return validationFailure(`RSchema create params are invalid: ${paramsReason}`);
        }
        const tablesReason = validateSchemaTables(create.tables, create.params);
        if (tablesReason !== undefined) {
            return validationFailure(`RSchema create tables are invalid: ${tablesReason}`);
        }
        return validationOk();
    }

    if (action === 'schema-update') {
        if (!json.checkFormat(schemaUpdateFormat, payload)) return validationFailure("schema-update payload format is invalid");
        const update = payload as SchemaUpdatePayload;
        if (!isValidSemver(update.version)) return validationFailure(`schema-update version '${update.version}' is not a semver`);
        if (update.params !== undefined) {
            if (update.params.length === 0) return validationFailure("schema-update params must be omitted when empty");
            const paramsReason = validateSchemaParams(update.params, true);
            if (paramsReason !== undefined) return validationFailure(`schema-update params are invalid: ${paramsReason}`);
        }
        if (update.migration.length === 0 && (update.params === undefined || update.params.length === 0)) {
            return validationFailure("schema-update migration is empty");
        }
        for (const [index, rule] of update.migration.entries()) {
            const ruleReason = validateMigrationRule(rule);
            if (ruleReason !== undefined) {
                return validationFailure(`schema-update migration rule ${index} is invalid: ${ruleReason}`);
            }
        }
        return validationOk();
    }

    return validationFailure(`unknown RSchema action '${String(action)}'`);
}

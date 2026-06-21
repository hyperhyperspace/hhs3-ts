// Validation for RSchema payloads and schema model elements.
//
//   validateRSchemaPayloadFormat — json.Format + position-independent semantics
//   validate_ops.ts              — position-dependent semantics (signatures, applicability)

import { json } from "@hyper-hyper-space/hhs3_json";
import { validationFailure, validationOk, ValidationResult } from "@hyper-hyper-space/hhs3_mvt";

import {
    createRSchemaFormat, CreateRSchemaPayload,
    schemaUpdateFormat, SchemaUpdatePayload,
    ColumnDef, ColumnType, FKs, IdProvider, IdTerm, MigrationRule, Operand, Predicate, PredicateContext,
    Restriction, TableDef,
    MAX_FKS, MAX_NAME_LENGTH, MAX_QUALIFIED_NAME_LENGTH, MAX_RESTRICTIONS,
    ID_TERMS, CMP_OPS, STR_OPS, CmpOp, StrOp,
} from "./payload.js";

import { splitTableRef, parseRowFieldTerm } from "./payload.js";
import { cmpTypesOk, strTypesOk } from "./expr.js";

export const MAX_WHERE_FIELDS = 64;
export const MAX_EXPR_DEPTH = 16;
export const MAX_EXPR_ARGS = 64;

// Table and column names: identifier-like, to keep them exportable to real
// relational databases later (rdb_adapter). Table references may be qualified
// with an RDb-namespace group name: 'group.table'.
const NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
const QUALIFIED_NAME_REGEX = /^([a-zA-Z_][a-zA-Z0-9_]*\.)?[a-zA-Z_][a-zA-Z0-9_]*$/;

export function isValidName(name: string): boolean {
    return name.length <= MAX_NAME_LENGTH && NAME_REGEX.test(name);
}

export function isValidTableRef(ref: string): boolean {
    return ref.length <= MAX_QUALIFIED_NAME_LENGTH && QUALIFIED_NAME_REGEX.test(ref);
}

export function columnValueMatchesType(value: json.Literal, type: ColumnType): boolean {
    switch (type) {
        case 'string': return typeof value === 'string';
        case 'integer': return typeof value === 'number' && Number.isInteger(value);
        case 'float': return typeof value === 'number' && Number.isFinite(value);
        case 'boolean': return typeof value === 'boolean';
        case 'json': return value !== undefined && value !== null;
    }
}

function isValidTerm(value: json.Literal, context: PredicateContext): boolean {
    if (!ID_TERMS.includes(value as IdTerm)) return false;
    return true;
}

// A `where` value that is a reserved ($-prefixed) string must be either a valid
// identity term ($author) or a subject-row field term ($row.<col>);
// $row.* is row-context only. Non-$ values are plain literals (checked here as
// "not reserved", i.e. accepted).
function isValidWhereValue(value: json.Literal, context: PredicateContext): boolean {
    if (typeof value !== 'string' || !value.startsWith('$')) return true;
    if (ID_TERMS.includes(value as IdTerm)) return isValidTerm(value, context);
    if (parseRowFieldTerm(value) !== undefined) return context === 'row';
    return false;   // any other $-string is reserved and not a known term
}

// Structural check for a value-expression operand. `{col}` ($row.<col>) is
// row-context only; arithmetic fns are integer-only and len takes one arg.
function validateOperand(op: json.Literal, context: PredicateContext, depth: number): boolean {
    if (depth > MAX_EXPR_DEPTH) return false;
    if (typeof op !== 'object' || op === null || Array.isArray(op)) return false;

    const e = op as { [key: string]: json.Literal };
    const keys = Object.keys(e);

    if ('lit' in e) {
        return keys.length === 1;
    }
    if ('col' in e) {
        if (context !== 'row') return false;   // no subject row
        return keys.length === 1 && typeof e['col'] === 'string' && isValidName(e['col']);
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
        case 'str': {
            if (keys.length !== 4) return false;
            if (!STR_OPS.includes(e['str'] as StrOp)) return false;
            return validateOperand(e['value'], context, depth + 1)
                && validateOperand(e['sub'], context, depth + 1);
        }
        case 'and':
        case 'or': {
            if (keys.length !== 2 || !Array.isArray(e['args'])) return false;
            const args = e['args'] as json.Literal[];
            if (args.length === 0 || args.length > MAX_EXPR_ARGS) return false;
            return args.every((a) => validatePredicate(a, context, depth + 1));
        }
        default:
            return false;
    }
}

type ExistsAtom = Extract<Predicate, { p: 'exists' }>;
type CmpAtom = Extract<Predicate, { p: 'cmp' }>;
type StrAtom = Extract<Predicate, { p: 'str' }>;

export function collectExistsAtoms(pred: Predicate, out: ExistsAtom[] = []): ExistsAtom[] {
    if (pred.p === 'exists') out.push(pred);
    if (pred.p === 'and' || pred.p === 'or') {
        for (const arg of pred.args) collectExistsAtoms(arg, out);
    }
    return out;
}

export function collectCmpStrAtoms(pred: Predicate, out: (CmpAtom | StrAtom)[] = []): (CmpAtom | StrAtom)[] {
    if (pred.p === 'cmp' || pred.p === 'str') out.push(pred);
    if (pred.p === 'and' || pred.p === 'or') {
        for (const arg of pred.args) collectCmpStrAtoms(arg, out);
    }
    return out;
}

function collectOperandCols(op: Operand, out: Set<string>): void {
    if ('col' in op) out.add(op.col);
    else if ('fn' in op) for (const a of op.args) collectOperandCols(a, out);
}

// Every subject-row column referenced by the predicate: via `{col}` operands in
// cmp/str atoms and via `$row.<col>` exists where-values.
export function collectRowFieldRefs(pred: Predicate, out: Set<string> = new Set()): Set<string> {
    switch (pred.p) {
        case 'cmp':
            collectOperandCols(pred.left, out);
            collectOperandCols(pred.right, out);
            break;
        case 'str':
            collectOperandCols(pred.value, out);
            collectOperandCols(pred.sub, out);
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

// Column-type lookup for the declaring table (operand type-checking).
function columnTypeOf(columns: { [c: string]: ColumnDef }): (column: string) => ColumnType | undefined {
    return (column) => column === 'author' ? 'string' : columns[column]?.type;
}

// Tier 1+2 column-level checks for one restriction rule declared on `def`:
//   - every $row.<col> reference names an existing READONLY column of `def`;
//   - cmp/str operand types are coherent over `def`'s columns;
//   - an exists where-value $row.<col> matches the (local) target field's type
//     (the target field's pub-ness is enforced separately by the exists check).
// Foreign (group.table) exists targets are skipped (checked at binding time).
export function checkPredicateColumns(
    def: TableDef,
    rule: Predicate,
    resolveLocalTable: (table: string) => TableDef | undefined,
): boolean {
    for (const col of collectRowFieldRefs(rule)) {
        if (col === 'author') continue;
        const cd = def.columns[col];
        if (cd === undefined || !(cd.readonly ?? false)) return false;
    }

    const typeOf = columnTypeOf(def.columns);
    for (const atom of collectCmpStrAtoms(rule)) {
        if (atom.p === 'cmp') {
            if (!cmpTypesOk(atom.cmp, atom.left, atom.right, typeOf)) return false;
        } else if (!strTypesOk(atom.value, atom.sub, typeOf)) {
            return false;
        }
    }

    for (const atom of collectExistsAtoms(rule)) {
        const [group, table] = splitTableRef(atom.table);
        if (group !== undefined) continue;
        const target = resolveLocalTable(table);
        if (target === undefined) return false;
        for (const [field, value] of Object.entries(atom.where ?? {})) {
            if (typeof value !== 'string') continue;
            const col = parseRowFieldTerm(value);
            if (col === undefined) continue;
            const subjectCol = def.columns[col];
            const targetCol = target.columns[field];
            if (subjectCol === undefined || targetCol === undefined) return false;
            if (subjectCol.type !== targetCol.type) return false;
        }
    }

    return true;
}

export function validateColumnDef(def: ColumnDef): boolean {
    if (def.default !== undefined && !columnValueMatchesType(def.default, def.type)) return false;
    return true;
}

// `columns` is the declaring (subject) table's columns: FK columns must exist in it.
export function validateFKs(fks: FKs, columns?: { [column: string]: ColumnDef }): boolean {
    const entries = Object.entries(fks);
    if (entries.length > MAX_FKS) return false;
    for (const [column, target] of entries) {
        if (!isValidName(column)) return false;
        if (!isValidTableRef(target)) return false;
        if (columns !== undefined && columns[column] === undefined) return false;
    }
    return true;
}

export function validateRestrictions(restrictions: Restriction[]): boolean {
    for (const restriction of restrictions) {
        if (!validatePredicate(restriction.rule)) return false;
    }
    return true;
}

// An identity provider designates two columns of the SAME table as the keyId
// and publicKey columns. Both must exist, be string-typed, be pub AND readonly
// (a mutable provider key would make at-append signature verification depend
// on resolution order; pub so the keyId is searchable in the cover read), and
// be distinct.
export function validateIdProvider(provider: IdProvider, columns: { [column: string]: ColumnDef }): boolean {
    if (!isValidName(provider.keyIdColumn) || !isValidName(provider.publicKeyColumn)) return false;
    if (provider.keyIdColumn === provider.publicKeyColumn) return false;

    for (const column of [provider.keyIdColumn, provider.publicKeyColumn]) {
        const def = columns[column];
        if (def === undefined) return false;
        if (def.type !== 'string') return false;
        if (!(def.pub ?? false)) return false;
        if (!(def.readonly ?? false)) return false;
    }
    return true;
}

export function validateTableDef(def: TableDef): boolean {
    if (!isValidName(def.name)) return false;

    const columnNames = Object.keys(def.columns);
    if (columnNames.length === 0) return false;

    for (const column of columnNames) {
        if (!isValidName(column)) return false;
        if (!validateColumnDef(def.columns[column])) return false;
    }

    if (def.fks !== undefined && !validateFKs(def.fks, def.columns)) return false;
    if (def.restrictions !== undefined && !validateRestrictions(def.restrictions)) return false;
    if (def.idProvider !== undefined && !validateIdProvider(def.idProvider, def.columns)) return false;

    return true;
}

// Validates a full set of table defs: each def is valid, names are unique,
// local (unqualified) FK and exists-atom targets exist in the set, and exists
// `where` fields are pub columns of the local target.
export function validateSchemaTables(tables: TableDef[]): boolean {
    const names = new Set<string>();
    const byName = new Map<string, TableDef>();

    for (const def of tables) {
        if (!validateTableDef(def)) return false;
        if (names.has(def.name)) return false;
        names.add(def.name);
        byName.set(def.name, def);
    }

    const checkExistsAtom = (atom: ExistsAtom): boolean => {
        const [group, table] = splitTableRef(atom.table);
        if (group !== undefined) return true;   // checked at binding time

        const target = byName.get(table);
        if (target === undefined) return false;

        if (atom.where !== undefined) {
            for (const field of Object.keys(atom.where)) {
                if (field === 'author') continue;
                const column = target.columns[field];
                if (column === undefined) return false;
                if (!(column.pub ?? false)) return false;
            }
        }
        return true;
    };

    for (const def of tables) {
        for (const target of Object.values(def.fks ?? {})) {
            const [group, table] = splitTableRef(target);
            if (group === undefined && !names.has(table)) return false;
        }

        for (const restriction of def.restrictions ?? []) {
            for (const atom of collectExistsAtoms(restriction.rule)) {
                if (!checkExistsAtom(atom)) return false;
            }
            if (!checkPredicateColumns(def, restriction.rule, (t) => byName.get(t))) return false;
        }
    }

    return true;
}

export function validateMigrationRule(rule: MigrationRule): boolean {
    switch (rule.rule) {
        case 'add-table':
            return validateTableDef(rule.def);
        case 'drop-table':
            return isValidName(rule.table);
        case 'add-column':
            if (!isValidName(rule.table) || !isValidName(rule.column)) return false;
            if (!validateColumnDef(rule.def)) return false;
            // a new non-nullable column needs a default to revise old rows
            if (!(rule.def.nullable ?? false) && rule.def.default === undefined) return false;
            return true;
        case 'drop-column':
            return isValidName(rule.table) && isValidName(rule.column);
        case 'set-concurrent-deletes':
            return isValidName(rule.table);
        case 'set-fks':
            return isValidName(rule.table) && validateFKs(rule.fks);
        case 'set-restrictions':
            return isValidName(rule.table) && validateRestrictions(rule.restrictions);
        default:
            return false;
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
        if (create.creators.length === 0) return validationFailure("RSchema create payload must have at least one creator");
        return validateSchemaTables(create.tables)
            ? validationOk()
            : validationFailure("RSchema create tables are invalid");
    }

    if (action === 'schema-update') {
        if (!json.checkFormat(schemaUpdateFormat, payload)) return validationFailure("schema-update payload format is invalid");
        const update = payload as SchemaUpdatePayload;
        if (update.migration.length === 0) return validationFailure("schema-update migration is empty");
        for (const [index, rule] of update.migration.entries()) {
            if (!validateMigrationRule(rule)) return validationFailure(`schema-update migration rule ${index} is invalid`);
        }
        return validationOk();
    }

    return validationFailure(`unknown RSchema action '${String(action)}'`);
}

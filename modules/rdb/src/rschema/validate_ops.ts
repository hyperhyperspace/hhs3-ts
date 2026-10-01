// Semantic (position-dependent) validation for RSchema payloads, layered on
// top of the format checks in validate.ts.
//
//   create        - format + every creator's keyId matches its public key
//   schema-update - format + signature by one of the creators + a version
//                   above every version at `at` (so versions increase along
//                   every causal path) + per-rule applicability against the
//                   resolved schema at the entry's parent frontier `at`. Rules
//                   within one update apply sequentially: later rules see the
//                   effect of earlier ones.
//
// Applicability is checked at `at` only: slot conflicts across forks (e.g.
// concurrent add-table of the same name on two branches) are NOT validity
// errors — they merge by per-slot LWW at resolution time.

import { json } from "@hyper-hyper-space/hhs3_json";
import { KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import {
    RContext, Version,
    validationFailure, validationOk, ValidationResult,
} from "@hyper-hyper-space/hhs3_mvt";
import { verifyPayloadSignature, deserializePublicKeyFromBase64, computeKeyId } from "@hyper-hyper-space/hhs3_mvt";

import { CreateRSchemaPayload, SchemaUpdatePayload, SchemaCreator } from "./payload.js";
import { validateMigrationRule, validateRSchemaPayloadFormat, type ValidateReason } from "./validate.js";
import { TableDef, MigrationRule } from "./payload.js";
import { collectExistsAtoms, collectRowFieldRefs, checkPredicateColumns, isValidSchemaName } from "./validate.js";
import { splitTableRef } from "./payload.js";
import type { RSchema, RSchemaView } from "./interfaces.js";
import { compareSemver } from "../semver.js";

export type RSchemaValidationContext =
    | { mode: 'create'; ctx: RContext }
    | { mode: 'op'; schema: RSchema; at: Version };

export async function validateRSchemaPayload(payload: json.Literal, context: RSchemaValidationContext): Promise<ValidationResult> {
    const formatResult = validateRSchemaPayloadFormat(payload);
    if (!formatResult.valid) return formatResult;

    if (context.mode === 'create') {
        return validateCreate(payload as CreateRSchemaPayload, context.ctx);
    }

    return validateUpdate(payload as SchemaUpdatePayload, context.schema, context.at);
}

function validateCreate(create: CreateRSchemaPayload, ctx: RContext): ValidationResult {
    if (create.action !== 'create') return validationFailure("RSchema creation action must be 'create'");
    if (!isValidSchemaName(create.name)) return validationFailure(`invalid schema name '${create.name}'`);

    const hashSuite = ctx.getHashSuite();
    const seen = new Set<KeyId>();

    for (const creator of create.creators) {
        if (seen.has(creator.keyId)) return validationFailure(`duplicate schema creator '${creator.keyId}'`);
        seen.add(creator.keyId);
        try {
            const pk = deserializePublicKeyFromBase64(creator.publicKey);
            if (computeKeyId(pk, hashSuite) !== creator.keyId) {
                return validationFailure(`schema creator keyId '${creator.keyId}' does not match public key`);
            }
        } catch {
            return validationFailure(`schema creator '${creator.keyId}' public key is invalid`);
        }
    }

    return validationOk();
}

function creatorKeyLookup(creators: SchemaCreator[]): (keyId: KeyId) => Promise<PublicKey | undefined> {
    return async (keyId: KeyId) => {
        const creator = creators.find((c) => c.keyId === keyId);
        if (creator === undefined) return undefined;
        try {
            return deserializePublicKeyFromBase64(creator.publicKey);
        } catch {
            return undefined;
        }
    };
}

async function validateUpdate(update: SchemaUpdatePayload, schema: RSchema, at: Version): Promise<ValidationResult> {
    if (update.action !== 'schema-update') return validationFailure("RSchema op action must be 'schema-update'");

    const view = await schema.getView(at, at);

    if (!view.isCreator(update.author)) return validationFailure(`schema-update author '${update.author}' is not a creator`);
    if (!await verifyPayloadSignature(update as unknown as json.LiteralMap, at, creatorKeyLookup(view.getCreators()))) {
        return validationFailure(`schema-update signature from '${update.author}' could not be verified`);
    }

    // The entries at `at` are the position's maxima, and versions increase
    // along every causal path, so checking them covers the whole past.
    for (const current of view.getVersions()) {
        if (compareSemver(update.version, current) <= 0) {
            return validationFailure(`schema-update version '${update.version}' is not above '${current}'`);
        }
    }

    return rulesApplicableAt(update.migration, view)
        ? validationOk()
        : validationFailure("schema-update migration is not applicable at this version");
}

// Per-rule applicability, applied sequentially over a working copy of the
// resolved tables at `at`. Exported for the schema-update author path.

export function rulesApplicableAt(rules: MigrationRule[], view: RSchemaView): boolean {
    const tables = new Map<string, TableDef>();
    for (const name of view.getTableNames()) {
        tables.set(name, view.getTable(name)!);
    }

    for (const rule of rules) {
        if (applyRule(tables, rule) !== undefined) return false;
    }

    return true;
}

export type MigrationCheck = { ok: true } | { ok: false; index: number; reason: string };

// Each rule's format check, then its applicability, in order over `tables`,
// which ends as the migrated table set (up to the first failing rule). For
// tools that plan updates: the same checks a schema-update gets, with the
// reason the first failing rule is refused.
export function applyMigrationRules(tables: Map<string, TableDef>, rules: MigrationRule[]): MigrationCheck {
    for (let index = 0; index < rules.length; index++) {
        const reason = validateMigrationRule(rules[index]) ?? applyRule(tables, rules[index]);
        if (reason !== undefined) return { ok: false, index, reason };
    }
    return { ok: true };
}

// `where` fields of local exists atoms must be pub columns of the target
// (foreign targets are checked at group binding time, as for create).
function checkLocalPredicateTargets(def: TableDef, tables: Map<string, TableDef>): ValidateReason {
    for (const target of Object.values(def.fks ?? {})) {
        const [group, table] = splitTableRef(target);
        if (group === undefined && !tables.has(table)) return `table '${def.name}': FK target '${table}' does not exist`;
    }

    for (const restriction of def.restrictions ?? []) {
        for (const atom of collectExistsAtoms(restriction.rule)) {
            const [group, table] = splitTableRef(atom.table);
            if (group !== undefined) continue;

            const target = tables.get(table);
            if (target === undefined) return `table '${def.name}': EXISTS target '${table}' does not exist`;

            for (const field of Object.keys(atom.where ?? {})) {
                const column = target.columns[field];
                if (column === undefined || !(column.pub ?? false)) {
                    return `table '${def.name}': EXISTS field '${table}.${field}' is not a pub column`;
                }
            }
        }
        const reason = checkPredicateColumns(def, restriction.rule, (t) => tables.get(t));
        if (reason !== undefined) return `table '${def.name}': ${reason}`;
    }

    return undefined;
}

// A local FK or exists atom of another table that still names `table`.
function referenceTo(tables: Map<string, TableDef>, table: string): string | undefined {
    for (const [name, def] of tables) {
        if (name === table) continue;
        for (const [column, target] of Object.entries(def.fks ?? {})) {
            const [group, t] = splitTableRef(target);
            if (group === undefined && t === table) return `the FK ${name}.${column}`;
        }
        for (const restriction of def.restrictions ?? []) {
            for (const atom of collectExistsAtoms(restriction.rule)) {
                const [group, t] = splitTableRef(atom.table);
                if (group === undefined && t === table) return `an EXISTS in ${name}'s ${restriction.on} restriction`;
            }
        }
    }
    return undefined;
}

// Checks one rule against the working table set and applies it. Returns the
// reason the rule is not applicable, if it isn't.
function applyRule(tables: Map<string, TableDef>, rule: MigrationRule): ValidateReason {
    switch (rule.rule) {
        case 'add-table': {
            if (tables.has(rule.def.name)) return `table '${rule.def.name}' already exists`;
            const withNew = new Map(tables);
            withNew.set(rule.def.name, rule.def);
            const reason = checkLocalPredicateTargets(rule.def, withNew);
            if (reason !== undefined) return reason;
            tables.set(rule.def.name, rule.def);
            return undefined;
        }
        case 'drop-table': {
            if (!tables.has(rule.table)) return `table '${rule.table}' does not exist`;
            // best-effort durability: refuse to drop a table still referenced
            // by another table's local FK or exists atom. This is an `at`-only
            // check (per-slot LWW merges can still produce a dangling local
            // target on another branch — a later FK write against it is then
            // voided at-use, an exists over it is false).
            const reference = referenceTo(tables, rule.table);
            if (reference !== undefined) return `table '${rule.table}' is still referenced by ${reference}`;
            tables.delete(rule.table);
            return undefined;
        }
        case 'add-column': {
            const def = tables.get(rule.table);
            if (def === undefined) return `table '${rule.table}' does not exist`;
            if (def.columns[rule.column] !== undefined) return `column '${rule.table}.${rule.column}' already exists`;
            tables.set(rule.table, { ...def, columns: { ...def.columns, [rule.column]: rule.def } });
            return undefined;
        }
        case 'drop-column': {
            const def = tables.get(rule.table);
            if (def === undefined) return `table '${rule.table}' does not exist`;
            if (def.columns[rule.column] === undefined) return `column '${rule.table}.${rule.column}' does not exist`;
            const columns = { ...def.columns };
            delete columns[rule.column];
            if (Object.keys(columns).length === 0) return `column '${rule.table}.${rule.column}' is the table's last column`;
            if ((def.fks ?? {})[rule.column] !== undefined) {
                return `column '${rule.table}.${rule.column}' still has an FK (drop it first with set-fks)`;
            }
            // best-effort durability: refuse if any exists atom (local target
            // = this table) still references the column as a where-field
            for (const [name, other] of tables) {
                for (const restriction of other.restrictions ?? []) {
                    for (const atom of collectExistsAtoms(restriction.rule)) {
                        const [group, t] = splitTableRef(atom.table);
                        if (group === undefined && t === rule.table && (atom.where ?? {})[rule.column] !== undefined) {
                            return `column '${rule.table}.${rule.column}' is still used by an EXISTS in ${name}'s ${restriction.on} restriction`;
                        }
                    }
                }
            }
            // refuse if THIS table's own restrictions still reference the column
            // as a subject-row field ($row.<col>, in cmp/like operands or as an
            // exists where-value)
            for (const restriction of def.restrictions ?? []) {
                if (collectRowFieldRefs(restriction.rule).has(rule.column)) {
                    return `column '${rule.table}.${rule.column}' is still used by the table's ${restriction.on} restriction`;
                }
            }
            tables.set(rule.table, { ...def, columns });
            return undefined;
        }
        case 'set-concurrent-deletes': {
            const def = tables.get(rule.table);
            if (def === undefined) return `table '${rule.table}' does not exist`;
            tables.set(rule.table, { ...def, concurrentDeletes: rule.value });
            return undefined;
        }
        case 'set-fks': {
            const def = tables.get(rule.table);
            if (def === undefined) return `table '${rule.table}' does not exist`;
            for (const [column, target] of Object.entries(rule.fks)) {
                if (def.columns[column] === undefined) return `FK column '${rule.table}.${column}' does not exist`;
                const [group, table] = splitTableRef(target);
                if (group === undefined && !tables.has(table)) return `FK target '${table}' of '${rule.table}.${column}' does not exist`;
            }
            tables.set(rule.table, { ...def, fks: rule.fks });
            return undefined;
        }
        case 'set-restrictions': {
            const def = tables.get(rule.table);
            if (def === undefined) return `table '${rule.table}' does not exist`;
            const updated = { ...def, restrictions: rule.restrictions };
            const reason = checkLocalPredicateTargets(updated, tables);
            if (reason !== undefined) return reason;
            tables.set(rule.table, updated);
            return undefined;
        }
    }
}

// Semantic (position-dependent) validation for RSchema payloads, layered on
// top of the format checks in validate.ts.
//
//   create        - format + every creator's keyId matches its public key
//   schema-update - format + signature by one of the creators + per-rule
//                   applicability against the resolved schema at the entry's
//                   parent frontier `at`. Rules within one update apply
//                   sequentially: later rules see the effect of earlier ones.
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
import { validateRSchemaPayloadFormat } from "./validate.js";
import { TableDef, MigrationRule } from "./payload.js";
import { collectExistsAtoms, collectRowFieldRefs, checkPredicateColumns, isValidSchemaName } from "./validate.js";
import { splitTableRef } from "./payload.js";
import type { RSchema, RSchemaView } from "./interfaces.js";

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
    if (!await verifyPayloadSignature(update as unknown as json.LiteralMap, creatorKeyLookup(view.getCreators()))) {
        return validationFailure(`schema-update signature from '${update.author}' could not be verified`);
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
        if (!applyRule(tables, rule)) return false;
    }

    return true;
}

// `where` fields of local exists atoms must be pub columns of the target
// (foreign targets are checked at group binding time, as for create).
function checkLocalPredicateTargets(def: TableDef, tables: Map<string, TableDef>): boolean {
    for (const target of Object.values(def.fks ?? {})) {
        const [group, table] = splitTableRef(target);
        if (group === undefined && !tables.has(table)) return false;
    }

    for (const restriction of def.restrictions ?? []) {
        for (const atom of collectExistsAtoms(restriction.rule)) {
            const [group, table] = splitTableRef(atom.table);
            if (group !== undefined) continue;

            const target = tables.get(table);
            if (target === undefined) return false;

            for (const field of Object.keys(atom.where ?? {})) {
                const column = target.columns[field];
                if (column === undefined || !(column.pub ?? false)) return false;
            }
        }
        if (checkPredicateColumns(def, restriction.rule, (t) => tables.get(t)) !== undefined) return false;
    }

    return true;
}

// Checks one rule against the working table set and applies it. Returns
// false if the rule is not applicable.
function applyRule(tables: Map<string, TableDef>, rule: MigrationRule): boolean {
    switch (rule.rule) {
        case 'add-table': {
            if (tables.has(rule.def.name)) return false;
            const withNew = new Map(tables);
            withNew.set(rule.def.name, rule.def);
            if (!checkLocalPredicateTargets(rule.def, withNew)) return false;
            tables.set(rule.def.name, rule.def);
            return true;
        }
        case 'drop-table': {
            if (!tables.has(rule.table)) return false;
            // best-effort durability: refuse to drop a table still referenced
            // by another table's local FK or exists atom. This is an `at`-only
            // check (per-slot LWW merges can still produce a dangling local
            // target on another branch — a later FK write against it is then
            // voided at-use, an exists over it is false).
            for (const [name, def] of tables) {
                if (name === rule.table) continue;
                for (const target of Object.values(def.fks ?? {})) {
                    const [group, t] = splitTableRef(target);
                    if (group === undefined && t === rule.table) return false;
                }
                for (const restriction of def.restrictions ?? []) {
                    for (const atom of collectExistsAtoms(restriction.rule)) {
                        const [group, t] = splitTableRef(atom.table);
                        if (group === undefined && t === rule.table) return false;
                    }
                }
            }
            tables.delete(rule.table);
            return true;
        }
        case 'add-column': {
            const def = tables.get(rule.table);
            if (def === undefined) return false;
            if (def.columns[rule.column] !== undefined) return false;
            tables.set(rule.table, { ...def, columns: { ...def.columns, [rule.column]: rule.def } });
            return true;
        }
        case 'drop-column': {
            const def = tables.get(rule.table);
            if (def === undefined) return false;
            if (def.columns[rule.column] === undefined) return false;
            const columns = { ...def.columns };
            delete columns[rule.column];
            if (Object.keys(columns).length === 0) return false;   // a table needs at least one column
            if ((def.fks ?? {})[rule.column] !== undefined) return false;   // drop the FK first (set-fks)
            // best-effort durability: refuse if any exists atom (local target
            // = this table) still references the column as a where-field
            for (const other of tables.values()) {
                for (const restriction of other.restrictions ?? []) {
                    for (const atom of collectExistsAtoms(restriction.rule)) {
                        const [group, t] = splitTableRef(atom.table);
                        if (group === undefined && t === rule.table && (atom.where ?? {})[rule.column] !== undefined) return false;
                    }
                }
            }
            // refuse if THIS table's own restrictions still reference the column
            // as a subject-row field ($row.<col>, in cmp/str operands or as an
            // exists where-value)
            for (const restriction of def.restrictions ?? []) {
                if (collectRowFieldRefs(restriction.rule).has(rule.column)) return false;
            }
            tables.set(rule.table, { ...def, columns });
            return true;
        }
        case 'set-concurrent-deletes': {
            const def = tables.get(rule.table);
            if (def === undefined) return false;
            tables.set(rule.table, { ...def, concurrentDeletes: rule.value });
            return true;
        }
        case 'set-fks': {
            const def = tables.get(rule.table);
            if (def === undefined) return false;
            for (const [column, target] of Object.entries(rule.fks)) {
                if (def.columns[column] === undefined) return false;
                const [group, table] = splitTableRef(target);
                if (group === undefined && !tables.has(table)) return false;
            }
            tables.set(rule.table, { ...def, fks: rule.fks });
            return true;
        }
        case 'set-restrictions': {
            const def = tables.get(rule.table);
            if (def === undefined) return false;
            const updated = { ...def, restrictions: rule.restrictions };
            if (!checkLocalPredicateTargets(updated, tables)) return false;
            tables.set(rule.table, updated);
            return true;
        }
    }
}

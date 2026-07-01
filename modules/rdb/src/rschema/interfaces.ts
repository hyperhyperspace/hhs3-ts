// Public RSchema interfaces.

import type { B64Hash, KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RObject, Version, View } from "@hyper-hyper-space/hhs3_mvt";

import type {
    TableDef, FKs, IdProvider, Predicate, MigrationRule, SchemaCreator,
} from "./payload.js";
import type { ColumnIncarnationId } from "./resolve.js";

export interface RSchema extends RObject {

    // The only writer beyond creation. Builds a rules-only schema-update,
    // signs it (updates must be authored by one of the schema's creators)
    // and appends it at `at` (defaults to the current frontier).
    updateSchema(migration: MigrationRule[], author: OwnIdentity, note?: string, at?: Version): Promise<B64Hash>;

    getView(at?: Version, from?: Version): Promise<RSchemaView>;

    getName(): string;
    hashAlgorithm(): string | undefined;
}

// A view over the effective schema at a version: the result of the per-slot
// LWW resolution of all create / schema-update entries at or below `at`.
//
// The RSchema DAG has no barriers (drops are slot tombstones, not barriers),
// so the effective schema is a pure function of `at`: the resolution ignores
// the view horizon, and its result is immutable and cacheable per version.
// Accessors are synchronous over that resolved state; all DAG access happens
// when the view is built. `from` only matters for the span-dependent reads
// (computeDelta on the object).

export interface RSchemaView extends View {
    getObject(): RSchema;

    // Immutable create-time facts
    getName(): string;
    getCreators(): SchemaCreator[];
    isCreator(keyId: KeyId): boolean;

    // The effective table set at this view's version. Dropped tables are
    // absent. getTable returns the resolved def (never a carried payload).
    getTableNames(): string[];
    hasTable(name: string): boolean;
    getTable(name: string): TableDef | undefined;

    // Per-slot accessors with defaults applied (what validators call).
    // getRestriction and-combines all declared restrictions matching the op
    // (its own tag or 'all'), falling back to defaultRestrictionRule.
    getConcurrentDeletes(table: string): boolean;
    getFKs(table: string): FKs;
    getRestriction(table: string, op: 'insert' | 'update' | 'delete'): Predicate;
    getPubColumns(table: string): string[];

    // The identity-provider designation of a table, or undefined if the table
    // is not a provider (or does not exist at this version).
    getIdProvider(table: string): IdProvider | undefined;

    // The live column incarnation id at this version (the birth write of the
    // winning column slot), or undefined when the column is not live.
    getColumnIncarnation(table: string, column: string): ColumnIncarnationId | undefined;
}

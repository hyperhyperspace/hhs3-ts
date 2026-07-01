// RSchemaView: synchronous accessors over a resolved SchemaState.
//
// All DAG access happens when the view is built (RSchemaImpl resolves and
// caches the state per version); the view itself is a pure reader.

import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { Version } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchema, RSchemaView } from "./interfaces.js";
import type { SchemaCreator } from "./payload.js";
import { ColumnIncarnationId, SchemaState } from "./resolve.js";
import {
    TableDef, FKs, IdProvider, Predicate,
    DEFAULT_CONCURRENT_DELETES, defaultRestrictionRule,
} from "./payload.js";

export class RSchemaViewImpl implements RSchemaView {

    private target: RSchema;
    private state: SchemaState;
    private at: Version;
    private from: Version;

    constructor(target: RSchema, state: SchemaState, at: Version, from: Version) {
        this.target = target;
        this.state = state;
        this.at = at;
        this.from = from;
    }

    getObject(): RSchema {
        return this.target;
    }

    getVersion(): Version {
        return this.at;
    }

    getFromVersion(): Version {
        return this.from;
    }

    async getReferences(): Promise<B64Hash[]> {
        return [];
    }

    async resolveRefVersion(_refId: B64Hash): Promise<Version> {
        throw new Error("RSchema holds no references");
    }

    // Immutable create-time facts

    getName(): string {
        return this.state.name;
    }

    getCreators(): SchemaCreator[] {
        return this.state.creators;
    }

    isCreator(keyId: KeyId): boolean {
        return this.state.creators.some((c) => c.keyId === keyId);
    }

    // The effective table set

    getTableNames(): string[] {
        return [...this.state.tables.keys()];
    }

    hasTable(name: string): boolean {
        return this.state.tables.has(name);
    }

    getTable(name: string): TableDef | undefined {
        return this.state.tables.get(name);
    }

    private requireTable(name: string): TableDef {
        const def = this.state.tables.get(name);
        if (def === undefined) {
            throw new Error(`Table '${name}' does not exist at this version`);
        }
        return def;
    }

    // Per-slot accessors with defaults applied

    getConcurrentDeletes(table: string): boolean {
        return this.requireTable(table).concurrentDeletes ?? DEFAULT_CONCURRENT_DELETES;
    }

    getFKs(table: string): FKs {
        return this.requireTable(table).fks ?? {};
    }

    getRestriction(table: string, op: 'insert' | 'update' | 'delete'): Predicate {
        const def = this.requireTable(table);
        const matching = (def.restrictions ?? [])
            .filter((r) => r.on === op || r.on === 'all')
            .map((r) => r.rule);

        if (matching.length === 0) return defaultRestrictionRule(op);
        if (matching.length === 1) return matching[0];
        return { p: 'and', args: matching };
    }

    getPubColumns(table: string): string[] {
        const def = this.requireTable(table);
        return Object.keys(def.columns).filter((c) => def.columns[c].pub ?? false);
    }

    getIdProvider(table: string): IdProvider | undefined {
        return this.state.tables.get(table)?.idProvider;
    }

    getColumnIncarnation(table: string, column: string): ColumnIncarnationId | undefined {
        return this.state.columnIncarnations.get(table)?.get(column);
    }
}

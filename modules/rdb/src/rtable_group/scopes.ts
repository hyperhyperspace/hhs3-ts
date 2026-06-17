// TableScope: the DagScope that projects one member table out of its group's
// physical DAG (cf. RSet's NestedElementScope).
//
// Payloads: a row op appended through a member RTable is wrapped into a
// `{action: 'row', table, op}` envelope. Reading back, an envelope unwraps to
// its inner op; a group `create` (initial rows) or `bundle` entry unwraps to a
// `{action: 'rows', ops}` slice carrying this table's ops only.
//
// Meta scheme (group DAG entries; all derived from the payload + the
// effective schema at the entry position, and re-checked at validation since
// meta is unhashed):
//
//   tables: [touched table names]            - every row-carrying entry
//   t-<table>-rows: [rowId]                  - row cover queries (liveness)
//   t-<table>-cols: ['<rowId>:<column>']     - column writes (inserts and
//                                              updates): the per-column LWW
//                                              cover queries. rowId and column
//                                              share one value so multi-op
//                                              entries (create, bundles) can't
//                                              cross-match a row with a column
//                                              touched only on another row
//   t-<table>-pub-<column>: [normalized]     - pub export (inserts and updates;
//                                              stale values in old entries are
//                                              candidate noise, filtered by the
//                                              resolved-value re-check in
//                                              findRowIds)
//   barrier: ['t']                           - deletes in concurrentDeletes
//                                              tables (and schema deploys;
//                                              tagged in group.ts)
//
// `barrier` stays an OUTER (unprefixed) key: it is the global revision tag.
// wrapMeta hoists an inner barrier to the outer key, and wrapFilter passes it
// through unprefixed, so table-scoped concurrent-barrier queries see it.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { EntryMetaFilter, MetaContainsValues, MetaProps, Position, position } from "@hyper-hyper-space/hhs3_dag";
import type { DagScope, Version } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchemaView } from "../rschema/interfaces.js";
import type { RowOpPayload, InsertRowPayload } from "../rtable/payload.js";
import type { CreateTableGroupPayload, RowEnvelopePayload, BundlePayload } from "./payload.js";

// The unwrapped shape of a multi-row entry (group create / bundle) within a
// table scope: this table's slice of the entry's ops.
export type RowsSlicePayload = {
    action: 'rows';
    ops: RowOpPayload[];
};

// What the group must expose to its table scopes (implemented by
// RTableGroupImpl; kept minimal to avoid an import cycle).
export type TableScopeHost = {
    getId(): B64Hash;
    selfValidate(): boolean;
    validatePayload(payload: json.Literal, at: Version): Promise<boolean>;
    resolveSchemaView(at: Version, from?: Version): Promise<RSchemaView>;
};

// The combined (rowId, column) meta value for column-write cover queries.
// B64 hashes never contain ':', so the encoding is unambiguous.
export function colTag(rowId: B64Hash, column: string): string {
    return rowId + ':' + column;
}

// Extract one table's row ops from a GROUP-level entry payload (row envelope,
// group create, or bundle). Mirrors TableScope.unwrapPayload's op selection;
// used by delta accumulation, which walks the raw group DAG (group-level
// payloads) rather than a table scope. A ref-advance / unknown action yields no
// ops.
export function tableOpsFromGroupPayload(payload: json.Literal, table: string): RowOpPayload[] {
    const p = payload as json.LiteralMap;

    switch (p['action']) {
        case 'row': {
            const envelope = p as RowEnvelopePayload;
            return envelope.table === table ? [envelope.op as RowOpPayload] : [];
        }
        case 'create': {
            const create = p as CreateTableGroupPayload;
            return (create.initialRows?.[table] ?? []) as RowOpPayload[];
        }
        case 'bundle': {
            const bundle = p as BundlePayload;
            return bundle.writes.filter((w) => w.table === table).map((w) => w.op as RowOpPayload);
        }
        default:
            return [];
    }
}

// Inner (table-scope) meta for one row op. `rows` indexes the touched row;
// inserts and updates tag every carried column (`cols`) and export their
// carried pub column values; deletes are ALWAYS barrier-tagged. Whether that
// barrier is HONORED (reaches concurrent branches) is decided at view time by
// the (at, from)-resolved concurrentDeletes flag, NOT here: the tag is baked
// permanently at write, but the flag is mutable + resolved at the horizon, so
// gating honoring on the tag's write-time value would make enabling the flag
// unable to honor a delete authored while it was off. Tagging unconditionally
// makes concurrentDeletes resolve fully at the view horizon (like restrictions
// / FKs); see liveInsert / baseLiveInsert in ../rtable/view.ts.
export function deriveRowOpInnerMeta(op: RowOpPayload, pubColumns: string[]): MetaProps {
    const meta: MetaProps = { rows: json.toSet([op.rowId]) };

    if (op.action === 'insert' || op.action === 'update') {
        const columns = Object.keys(op.values);
        if (columns.length > 0) {
            meta['cols'] = json.toSet(columns.map((column) => colTag(op.rowId, column)));
        }
        for (const column of pubColumns) {
            const value = op.values[column];
            if (value !== undefined) {
                meta['pub-' + column] = json.toSet([json.toStringNormalized(value)]);
            }
        }
    }

    if (op.action === 'delete') {
        meta['barrier'] = json.toSet(['t']);
    }

    return meta;
}

// Outer meta for one table's inner meta: `t-<table>-` prefix, except for the
// global `barrier` key, which is hoisted (merged) to the outer key.
function wrapInnerMeta(outer: MetaProps, table: string, innerMeta: MetaProps): void {
    for (const key of Object.keys(innerMeta)) {
        if (key === 'barrier') {
            outer['barrier'] = innerMeta[key];
        } else {
            outer['t-' + table + '-' + key] = innerMeta[key];
        }
    }
}

// Outer meta for a row envelope. Used by the group's applyPayload (sync
// ingestion) and by validation re-derivation; the local append path produces
// the identical result through TableScope.wrapMeta.
export function deriveEnvelopeMeta(envelope: RowEnvelopePayload, schemaView: RSchemaView): MetaProps {
    const meta: MetaProps = { tables: json.toSet([envelope.table]) };
    const op = envelope.op as RowOpPayload;
    wrapInnerMeta(meta, envelope.table,
        deriveRowOpInnerMeta(op, schemaView.getPubColumns(envelope.table)));
    return meta;
}

// Outer meta for a multi-row entry (group create / bundle): every touched
// table is tagged, and each op contributes its rows / cols / pub / barrier
// meta (per-table merged) so table scopes surface their slices. `tablesOps`
// maps each touched table to its ops IN ORDER.
function deriveMultiRowMeta(tablesOps: { [table: string]: RowOpPayload[] }, schemaView: RSchemaView): MetaProps {
    const meta: MetaProps = {};
    const tables = Object.keys(tablesOps);
    if (tables.length === 0) return meta;

    meta['tables'] = json.toSet(tables);

    for (const table of tables) {
        const pubColumns = schemaView.getPubColumns(table);
        const inner: MetaProps = {};

        for (const op of tablesOps[table]) {
            const opMeta = deriveRowOpInnerMeta(op, pubColumns);
            for (const key of Object.keys(opMeta)) {
                const existing = inner[key];
                inner[key] = existing === undefined
                    ? opMeta[key]
                    : json.toSet([...json.fromSet(existing), ...json.fromSet(opMeta[key])]);
            }
        }

        wrapInnerMeta(meta, table, inner);
    }

    return meta;
}

// Outer meta for a group create entry: every initial table is tagged, and each
// initial row contributes its rows / cols / pub meta so table scopes surface
// their genesis rows.
export function deriveCreateMeta(create: CreateTableGroupPayload, schemaView: RSchemaView): MetaProps {
    const initial = create.initialRows ?? {};
    const tablesOps: { [table: string]: RowOpPayload[] } = {};
    for (const table of Object.keys(initial)) {
        tablesOps[table] = initial[table] as RowOpPayload[];
    }
    return deriveMultiRowMeta(tablesOps, schemaView);
}

// Outer meta for a bundle entry: the ordered writes grouped by table (order
// within a table preserved), each table's ops merged like a create.
export function deriveBundleMeta(bundle: BundlePayload, schemaView: RSchemaView): MetaProps {
    const tablesOps: { [table: string]: RowOpPayload[] } = {};
    for (const write of bundle.writes) {
        (tablesOps[write.table] ??= []).push(write.op as RowOpPayload);
    }
    return deriveMultiRowMeta(tablesOps, schemaView);
}

export class TableScope implements DagScope {

    private host: TableScopeHost;
    private table: string;

    constructor(host: TableScopeHost, table: string) {
        this.host = host;
        this.table = table;
    }

    startAt(): Position {
        return position(this.host.getId());   // the group's create entry
    }

    startEmpty(): boolean {
        return false;   // the create entry may seed rows
    }

    baseFilter(): EntryMetaFilter {
        return { containsValues: { tables: [this.table] } };
    }

    wrapPayload(payload: json.Literal, _at: Position): json.Literal {
        const envelope: RowEnvelopePayload = {
            action: 'row',
            table: this.table,
            op: payload,
        };
        return envelope;
    }

    unwrapPayload(payload: json.Literal, _at: Position): json.Literal {
        const outer = payload as json.LiteralMap;

        switch (outer['action']) {
            case 'row': {
                const envelope = outer as RowEnvelopePayload;
                if (envelope.table !== this.table) {
                    throw new Error(`Row envelope for table '${envelope.table}' unwrapped in scope of '${this.table}'`);
                }
                return envelope.op;
            }
            case 'create': {
                const create = outer as CreateTableGroupPayload;
                const slice: RowsSlicePayload = {
                    action: 'rows',
                    ops: (create.initialRows?.[this.table] ?? []) as RowOpPayload[],
                };
                return slice;
            }
            case 'bundle': {
                const bundle = outer as BundlePayload;
                const ops: RowOpPayload[] = [];
                for (const write of bundle.writes) {
                    if (write.table === this.table) ops.push(write.op as RowOpPayload);
                }
                return { action: 'rows', ops };
            }
            default:
                throw new Error(`Invalid payload action in TableScope.unwrapPayload: ${outer['action']}`);
        }
    }

    wrapMeta(innerMeta: MetaProps, _wrappedPayload: json.Literal, _at: Position): MetaProps {
        const outerMeta: MetaProps = { tables: json.toSet([this.table]) };
        wrapInnerMeta(outerMeta, this.table, innerMeta);
        return outerMeta;
    }

    unwrapMeta(outerMeta: MetaProps, _wrappedPayload: json.Literal, _at: Position): MetaProps {
        const innerMeta: MetaProps = {};

        const prefix = 't-' + this.table + '-';
        for (const key of Object.keys(outerMeta)) {
            if (key.startsWith(prefix)) {
                innerMeta[key.substring(prefix.length)] = outerMeta[key];
            } else if (key === 'barrier') {
                innerMeta['barrier'] = outerMeta[key];
            }
        }

        return innerMeta;
    }

    wrapFilter(filter: EntryMetaFilter): EntryMetaFilter {
        const wrapped: EntryMetaFilter = {};

        if (filter.containsKeys !== undefined) {
            wrapped.containsKeys = filter.containsKeys.map((key) =>
                key === 'barrier' ? key : 't-' + this.table + '-' + key);
        }

        if (filter.containsValues !== undefined) {
            wrapped.containsValues = {} as MetaContainsValues;
            for (const key of Object.keys(filter.containsValues)) {
                const wrappedKey = key === 'barrier' ? key : 't-' + this.table + '-' + key;
                wrapped.containsValues[wrappedKey] = filter.containsValues[key];
            }
        }

        return wrapped;
    }

    // Local-append check: the payload is validated through the group (when
    // selfValidate is on), and the carried meta must equal the re-derived
    // meta (meta is unhashed; a divergence here would forge visibility).
    async validateWrappedPayload(wrappedPayload: json.Literal, wrappedMeta: MetaProps, at: Position): Promise<boolean> {
        const envelope = wrappedPayload as RowEnvelopePayload;
        if (envelope.action !== 'row') return false;

        const schemaView = await this.host.resolveSchemaView(at);
        const expected = deriveEnvelopeMeta(envelope, schemaView);
        if (json.toStringNormalized(wrappedMeta as json.Literal) !== json.toStringNormalized(expected as json.Literal)) {
            return false;
        }

        if (!this.host.selfValidate()) return true;
        return this.host.validatePayload(wrappedPayload, at);
    }
}

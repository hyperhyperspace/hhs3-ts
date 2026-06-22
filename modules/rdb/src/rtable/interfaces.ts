// Public RTable interfaces.
//
// An RTable is one member table of an RTableGroup: a nested RObject living on
// a scoped projection of the group's DAG (the nested-RSet-element pattern).
// Tables exist by schema (no creation op) and are always loaded through their
// group (group.getTable(name)), never standalone or via the type registry.

import type { B64Hash, KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { RObject, Version, View } from "@hyper-hyper-space/hhs3_mvt";

import type { RowQuery } from "./query.js";

export type RowValues = { [column: string]: json.Literal };

export type Row = {
    rowId: B64Hash;
    uuid: string;
    author?: KeyId;       // insert author; absent for unauthored/genesis rows
    values: RowValues;    // per-field LWW-resolved at the view position
};

// Delta support (see src/rtable/delta.ts): a row's enforced liveness + author +
// LWW-resolved WRITTEN column values, with NO schema-default fallback. `written`
// holds only columns carrying an actual write; `live === false` => author
// undefined and written empty.
export type DeltaRowState = {
    live: boolean;
    author: KeyId | undefined;
    written: RowValues;
};

export interface RTable extends RObject {
    getTableName(): string;
    getGroupId(): B64Hash;

    // Row writers. Returns the entry hash; the rowId is deriveRowId(uuid).
    // rowIds are WRITE-ONCE identities: deletes are permanent, and a deleted
    // rowId can never be re-inserted (insert a new row with a fresh uuid
    // instead). `author` is optional: an unauthored op is anonymous and passes
    // authentication trivially, but cannot satisfy $author restrictions
    // (when the group has a provider, an authored op's signature is verified at
    // validation). `at` defaults to the GROUP frontier (not the table-scope
    // frontier): by default a write extends the group's consistent snapshot.
    insert(uuid: string, values: RowValues, author?: OwnIdentity, at?: Version): Promise<B64Hash>;
    // partial update: only the changed values; per-field LWW with entry-hash
    // tiebreak; the row must be live at `at`; readonly columns rejected
    update(rowId: B64Hash, values: RowValues, author?: OwnIdentity, at?: Version): Promise<B64Hash>;
    delete(rowId: B64Hash, author?: OwnIdentity, at?: Version): Promise<B64Hash>;

    getView(at?: Version, from?: Version): Promise<RTableView>;
}

export interface RTableView extends View {
    getObject(): RTable;

    // Liveness has two layers: (1) permanent-delete state — an insert in the
    // row's history, no delete in it (deletes are permanent), no concurrent
    // delete barrier visible from `from` honored at-use by the concurrentDeletes
    // flag at the delete's position; (2) op validity — ops from entries whose
    // restriction predicates fail OR whose written FK columns do not reach a
    // live target are invisible (drop-on-void, at-use, bundles all-or-nothing).
    // See src/rtable/view.ts.
    hasRow(rowId: B64Hash): Promise<boolean>;
    getRow(rowId: B64Hash): Promise<Row | undefined>;
    getAuthor(rowId: B64Hash): Promise<KeyId | undefined>;   // undefined: anonymous or not live

    // Every live rowId at this view's horizon (full enforced liveness). The
    // unfiltered row enumeration behind the add-fk deploy prerequisite; prefer
    // findRowIds when a pub/author filter is available.
    liveRowIds(): Promise<B64Hash[]>;

    // Search over pub columns via meta-indexed cover queries — the machinery
    // behind exists predicates. All `where` fields must be pub columns of
    // this table or the implicit `rowAuthor` system field; matches are exact
    // (normalized value equality) against the RESOLVED values at the view
    // position (pub columns are mutable unless also readonly; stale meta hits
    // are filtered).
    findRowIds(where: { [pubColumn: string]: json.Literal }): Promise<B64Hash[]>;

    // Single-table SELECT-style read at this view's horizon (no joins). A
    // local read: voids nothing, agrees with no replica. Validates the query
    // against the schema (throws on user mistakes — unknown columns, malformed
    // filters, type-incoherent comparisons, bad limit/offset), selects
    // candidates (pub-eq / author index pushdown, else full scan), re-checks the
    // FULL filter over enforced-live RESOLVED rows, then applies
    // orderBy / offset / limit / select. See src/rtable/query.ts.
    query(q: RowQuery): Promise<Row[]>;

    // Delta support (see src/rtable/delta.ts). getColumns: the schema column
    // names at this horizon. deltaRowState: a row's enforced liveness, author,
    // and LWW-resolved WRITTEN values (no default fallback) for `columns`.
    // Written values resolve schema-independently (a dropped column still
    // yields its frozen value), so the caller diffs over the UNION of both
    // horizons' columns, which keeps uniform schema effects (defaults/drops)
    // out of the per-row diff — those live in the schema sub-delta channel.
    getColumns(): Promise<string[]>;
    deltaRowState(rowId: B64Hash, columns: string[]): Promise<DeltaRowState>;
}

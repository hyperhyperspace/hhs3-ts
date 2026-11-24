// Phase 1 vocabulary for the rdb -> relational projection: adapter config, the
// closed set of schema actions the mapper emits, and the interface concrete
// backends implement to materialize them.
//
// Anchor: ONE RTableGroup per checkpoint/target (per-group materialization).
// Table names are therefore unique within the group and actions carry bare
// (un-qualified) table names. Multi-group (RDb-level) projection is a later
// phase that orchestrates several per-group projections at compatible versions.
//
// Anchor: the projection reads like a normal database. App tables carry no
// hashes and no underscore-prefixed columns — just an integer `id` PK, an
// integer `author_key_id` (key-ref into `rdb_keys`), and the business columns.
// Crypto material (key hash + public key) lives ONLY in the shared `rdb_keys`
// side table. The content-addressed rowId / uuid live in a per-table sync table
// (`<table>_sync`, a target-side convention) which also allocates `id` stably
// across void flips. `_id` is the row-serial suffix (PK / FK companions);
// `_key_id` is the rdb_keys intern suffix (author / identity columns).

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash, KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { ColumnDef, OpVoidDetail, OpVerdictKind } from "@hyper-hyper-space/hhs3_rdb";
import type { Version, ValidationFailure } from "@hyper-hyper-space/hhs3_mvt";

export const DEFAULT_ID_COLUMN = 'id';
export const DEFAULT_AUTHOR_COLUMN = 'author_key_id';
export const DEFAULT_SYNC_TABLE_SUFFIX = '_sync';
export const DEFAULT_FK_ID_SUFFIX = '_id';
export const DEFAULT_CROSS_GROUP_FK_SUFFIX = '_row_hash';
export const DEFAULT_KEY_TABLE = 'rdb_keys';
export const DEFAULT_KEY_ID_COLUMN = 'key_id';
export const DEFAULT_KEY_REF_SUFFIX = '_key_id';
// The single global key-space domain used by KeyIndex in v1 (kept as a param
// for symmetry with RowIdentityIndex's table/domain and for future namespacing).
export const DEFAULT_KEY_DOMAIN = 'keys';

// Fully-customizable naming. Renames are optional per table and per column;
// anything not listed passes through unchanged (rdb identifiers are already
// SQL-identifier-safe). All keys are rdb-side names.
export type AdapterConfig = {
    tableNames?: { [rdbTable: string]: string };
    // The projected name of a table `tableNames` doesn't list:
    // `<tablePrefix>_<table>`. A shared projection sets it to the group's
    // prefix, so a table that a schema change adds after the projection opened
    // is named like the tables the group had then.
    tablePrefix?: string;
    columnNames?: { [rdbTable: string]: { [rdbColumn: string]: string } };
    // Projection-local integer primary key column. Allocated once per rowId via
    // the per-table sync table and stable across void flips. Defaults to 'id'.
    idColumn?: string;
    // In-row author column: an integer id referencing `rdb_keys`. Authorship
    // carries application semantics. Defaults to 'author_key_id'; set to false
    // to omit it entirely. The raw key hash / public key are recovered via
    // KeyIndex.
    authorColumn?: string | false;
    // Shared per-target keys side table (`rdb_keys`). Holds (id, key_hash,
    // public_key); author_key_id / key_id / identity `<col>_key_id` columns
    // reference its id.
    keyTable?: string;
    // Projected name for an identity-provider table's keyIdColumn. Fixed
    // (never `<keyIdCol>_id`) so joins read as `identities.key_id`.
    keyIdColumn?: string;
    // Per-table sync-table naming convention. The sync tables themselves are a
    // target-side concern; the mapper only reserves these names in its collision
    // check so an rdb table cannot silently collide with the convention.
    syncTableSuffix?: string;
    // FK column projection suffixes. A LOCAL foreign key column projects as an
    // integer companion `<col><fkIdSuffix>` referencing the target table's serial
    // `id` (an advisory DB FK); a CROSS-GROUP foreign key column projects as a
    // text `<col><crossGroupFkSuffix>` carrying the foreign rowId (row_hash)
    // verbatim. Business rows therefore never carry a rowId/hash column of their
    // own. Default to '_id' and '_row_hash'.
    fkIdSuffix?: string;
    crossGroupFkSuffix?: string;
    // Identity-typed business columns project as integer `<col><keyRefSuffix>`
    // referencing `rdb_keys` (a key-ref, not a row-serial FK). Independent of
    // `fkIdSuffix`. Defaults to '_key_id'. The provider keyIdColumn is NOT
    // suffixed — it stays the fixed `key_id`.
    keyRefSuffix?: string;

    // --- inbound (change ingestion) ---

    // The identity every ingested op is authored (and signed) by. Required by
    // ingestChanges; the projected DB attributes all local edits to this single
    // "adapter writer" (per-principal inbound authorship is out of scope). A
    // group without an idProvider admits only anonymous ops, so its ingested
    // writes are anonymous; ref-advances are signed only when the binding's
    // canObserve reads $author.
    writer?: OwnIdentity;

    // What an insert gets when it leaves the author column empty, in a group
    // with an identity provider (HIP-20's Authorship): on (default), capture
    // fills in the writer key's id, and the insert is signed with it; off, it
    // is anonymous, and an insert is signed as its author column says. Ignored
    // in an anonymous group.
    autoAuthor?: boolean;

    // --- cross-group (replica-wide) projection ---

    // Cross-group resolution context. Present only when several groups of one
    // RDb are materialized into a SINGLE shared target. A cross-group FK (a
    // qualified 'binding.table' ref) whose referenced group is co-projected then
    // resolves to that table's serial `id` (an integer `<col><fkIdSuffix>`
    // companion with fk metadata), exactly like a local FK, instead of an opaque
    // text `<col><crossGroupFkSuffix>` passthrough. The resolver maps a qualified
    // rdb ref to the referenced table's TARGET name in the shared target, or
    // returns undefined when that group is NOT co-projected (row_hash fallback).
    crossGroup?: CrossGroupResolver;
};

// Maps a qualified cross-group FK ref ('binding.table') to the referenced
// table's TARGET table name in the shared projection, or undefined when the
// referenced group is not co-projected.
export type CrossGroupResolver = (qualifiedRef: string) => string | undefined;

// ---------------------------------------------------------------------------
// Inbound (change ingestion) vocabulary: the target-side mirror of RowAction.
// A concrete backend captures local mutations of the projected DB and reports
// them here in target vocabulary (target table + local PK + target column
// names); the core inverse planner translates them back into rdb ops.
// ---------------------------------------------------------------------------

// A backend-supplied, COMMIT-ORDERED opaque token identifying one captured
// change. The core treats it only as an ordered ack handle - a serial in
// SQLite (safe because writers serialize), a commit LSN in Postgres. Never
// interpreted as an insert serial.
export type CapturedChangeId = string | number;

// The columns of a captured insert or update, by target column name, as
// LOGICAL values. A `null` is a column the app left or set to NULL.
export type CapturedValues = { [column: string]: json.Literal | null };

// One local mutation observed in the projected DB, in TARGET vocabulary.
// `localId` is the projection-local integer PK (`id`); the core resolves it to
// a content-addressed rowId via the sync table. `values` (insert/update) uses
// target column names and carries LOGICAL values; update carries only the
// columns that changed. A column set to NULL is carried as `null`: an update
// to NULL becomes a null write in rdb. An insert may name its empty columns
// with `null` or leave them out, as the target's capture allows; the planner
// keeps an insert's null only where the column has a default (see
// translateNet in ./ingest.ts). `unreadable` names the columns the target
// could not read back as logical values, with the reason, such as text that
// isn't json in a json column: they are left out of `values`, and ingestion
// refuses them, so the drain never throws on a stored value.
// `record` is what ingestion wrote into the change's outbox row, if anything
// (HIP-29, see ChangeRecord).
export type CapturedChange =
    | { id: CapturedChangeId; kind: 'insert'; table: string; localId: number;
        values: CapturedValues; unreadable?: { [column: string]: string }; record?: ChangeRecord }
    | { id: CapturedChangeId; kind: 'update'; table: string; localId: number;
        values: CapturedValues; unreadable?: { [column: string]: string }; record?: ChangeRecord }
    | { id: CapturedChangeId; kind: 'delete'; table: string; localId: number; record?: ChangeRecord };

// What became of a captured change, written into its outbox row as a pass
// goes (HIP-29): the observes written for it, each recorded before its
// append, and then one outcome: the hash of its entry, recorded before the
// append, its failure event, or the mark of nothing to write. Each observe,
// like the entry, is an entry of the change's own group; `group` is the bound
// group it observes. A write merges into the row's record: its observes go
// after the ones there, and an outcome it gives replaces the one there.
export type ChangeRecord = {
    observes?: { group: B64Hash; hash: B64Hash }[];
    entry?: B64Hash;
    failure?: OpEvent;
    nothing?: true;
};

// The record a write leaves in an outbox row that holds `prev` (see ChangeRecord).
export function mergeChangeRecord(prev: ChangeRecord | undefined, write: ChangeRecord): ChangeRecord {
    const merged: ChangeRecord = {};
    const observes = [...(prev?.observes ?? []), ...(write.observes ?? [])];
    if (observes.length > 0) merged.observes = observes;
    const outcome = write.entry !== undefined || write.failure !== undefined || write.nothing !== undefined ? write : prev;
    if (outcome?.entry !== undefined) merged.entry = outcome.entry;
    if (outcome?.failure !== undefined) merged.failure = outcome.failure;
    if (outcome?.nothing !== undefined) merged.nothing = outcome.nothing;
    return merged;
}

// The changes drained in one ingestion pass, in COMMIT order. Reproducing the
// app's transaction boundaries is not required (bundling is dependency-driven),
// so a batch is simply the ordered drained set.
export type CapturedBatch = { changes: CapturedChange[] };

// The lifecycle status recorded on each per-table sync row. `active`: the row
// is (or was) a live projected/ingested row. `deleted`: the row was deleted
// (normally); the sync record is kept for id stability. `ingestion_failure`:
// an insert that rdb rejected and was reverted (its app row removed) - the sync
// record is kept so the failure is auditable and the id is never reused.
export type SyncStatus = 'active' | 'deleted' | 'ingestion_failure';

// The mapping between a projection-local row and its rdb identity, kept in the
// per-table sync table. Written by projection, and by the settle for the
// inserts a pass handled; read via resolveRow. `table` is the TARGET table.
// `status` defaults to 'active'.
export type SyncMapping = {
    table: string;
    localId: number;
    rowId: B64Hash;
    uuid: string;
    author?: KeyId;
    status?: SyncStatus;
};

// ---------------------------------------------------------------------------
// Op-event log: the single, durable, app-observable channel for BOTH ingestion
// failures (a local change rdb rejected) and p2p concurrency verdict flips (an
// op voided or reinstated by a later barrier). The app inspects the log
// (`drainOpEvents` / `opEvents`) and optionally subscribes to new ids; the
// supervisor does not persist the app's cursor. Append-only, idempotent by
// (opHash, direction).
// ---------------------------------------------------------------------------

// Why an op-event fired. A concurrency flip carries the STRUCTURED OpVoidDetail
// the group computed (fetch the op itself via loadEntry(opHash)); an ingestion
// failure carries the bundle() ValidationFailure chain (bundle never produces
// an OpVoidDetail - hard validation and at-use void are different code paths).
export type OpEventReason =
    | { source: 'void'; detail: OpVoidDetail }
    | { source: 'validation'; failure: ValidationFailure };

// One durable op-event.
//   - origin 'ingestion', direction 'failure': a local change rdb rejected (a
//     hard validation reject, or a translate-time reject like a dangling FK /
//     readonly edit / unknown key). `opHash` is a SYNTHETIC content hash of the
//     constructed (never-appended) op and `op` carries its full JSON so the
//     app can recover the intended values; `rowId`/`localId`/`table` name the
//     affected row (reverted from rdb truth).
//   - origin 'concurrency', direction 'void' | 'reinstate': a p2p verdict flip.
//     `opHash` is the REAL DAG entry hash (the op is fetchable via loadEntry);
//     `op` is omitted. Both directions are logged (a void, then a later
//     reinstate, are two distinct events on the same op).
export type OpEvent = {
    origin: 'ingestion' | 'concurrency';
    direction: 'failure' | 'void' | 'reinstate';
    groupId: B64Hash;
    opHash: B64Hash;
    kind: OpVerdictKind;
    table?: string;
    rowId?: B64Hash;
    localId?: number;
    author?: KeyId;
    op?: json.Literal;
    reason?: OpEventReason;
};

// An op-event as stored, carrying its durable monotonic id (the push cursor).
export type StoredOpEvent = { id: number; event: OpEvent };

// Inspect query over the durable op-event log. Bounds are exclusive and
// independent of sort: `id > afterId` (default 0 = no floor) and, when set,
// `id < beforeId` (omit = no ceiling). `order` only chooses ASC vs DESC — it
// is never inferred from the other fields. Default `order: 'asc'` is the
// catch-up cursor. Range endpoints are the same query with `limit: 1`:
//   low  = { order: 'asc',  limit: 1 }
//   high = { order: 'desc', limit: 1 }
export type OpEventOrder = 'asc' | 'desc';
export type OpEventQuery = {
    afterId?: number;
    beforeId?: number;
    limit?: number;
    order?: OpEventOrder;
};

export type ResolvedOpEventQuery = {
    afterId: number;
    beforeId: number | undefined;
    limit: number | undefined;
    order: OpEventOrder;
};

function requireNonNegInt(value: number | undefined, label: string): number {
    if (value === undefined || !Number.isSafeInteger(value) || value < 0) {
        throw new Error(`op-event query: ${label} must be a non-negative integer, got ${String(value)}`);
    }
    return value;
}

// Accept the options bag, or a numeric `afterId` (legacy positional drain).
export function resolveOpEventQuery(opts?: OpEventQuery | number): ResolvedOpEventQuery {
    const q: OpEventQuery = typeof opts === 'number' ? { afterId: opts } : (opts ?? {});
    const afterId = q.afterId === undefined ? 0 : requireNonNegInt(q.afterId, 'afterId');
    const beforeId = q.beforeId === undefined ? undefined : requireNonNegInt(q.beforeId, 'beforeId');
    const order = q.order ?? 'asc';
    if (order !== 'asc' && order !== 'desc') {
        throw new Error(`op-event query: order must be 'asc' or 'desc', got '${String(order)}'`);
    }
    let limit: number | undefined;
    if (q.limit !== undefined) {
        limit = requireNonNegInt(q.limit, 'limit');
    }
    return { afterId, beforeId, limit, order };
}

export function opEventInRange(id: number, q: ResolvedOpEventQuery): boolean {
    if (id <= q.afterId) return false;
    if (q.beforeId !== undefined && id >= q.beforeId) return false;
    return true;
}

// In-memory page of a pre-loaded log (MemoryTarget / IdbTarget). Rows need
// not be pre-sorted; the result is a new array in `order`.
export function pageOpEvents(rows: StoredOpEvent[], opts?: OpEventQuery | number): StoredOpEvent[] {
    const q = resolveOpEventQuery(opts);
    const filtered = rows.filter((e) => opEventInRange(e.id, q));
    filtered.sort((a, b) => q.order === 'desc' ? b.id - a.id : a.id - b.id);
    return q.limit === undefined ? filtered : filtered.slice(0, q.limit);
}

// Everything the settle transaction persists atomically at the end of an
// ingestion pass (see MaterializedChangeSource.commitIngest), in HIP-29's
// order. The target also stores the failure events it reads from the records
// of the `consumed` rows, before `events`. Every write it makes is under echo
// suppression, so none re-enters the outbox.
export type IngestSettle = {
    // The drained change ids to ack (delete from the outbox): the handled rows.
    consumed: CapturedChangeId[];
    // The op-events of the pass's deltas (INSERT-OR-IGNORE on (opHash, direction)).
    events?: OpEvent[];
    //
    // Each group's checkpoint as the pass found it, checked first, and the one
    // to set. A group whose chain is empty gives the same version twice. A
    // moved checkpoint throws CheckpointMovedError, and nothing is written.
    checkpoints?: { groupId: B64Hash; expected: Version; next: Version }[];
    // The mappings of the handled inserts: `active`, or `ingestion_failure` for
    // a refused insert whose local id had no identity yet.
    inserts?: SyncMapping[];
    // The rows of accepted deletes, marked `deleted` after the inserts'
    // mappings are written.
    deletes?: { table: string; rowId: B64Hash }[];
    // Refused inserts whose local id had no identity yet: the row is removed,
    // with its changes still in the outbox put back on top.
    orphans?: { table: string; localId: number }[];
    // The rows set to rdb's row at the end of the chain, with their changes
    // still in the outbox on top (see RowRevert).
    reverts?: RowRevert[];
    // SQLite: the json columns of handled changes, rewritten in canonical form.
    // A column a change still in the outbox writes is skipped, and so is a row
    // that is gone. Other targets ignore them.
    jsonWrites?: { table: string; localId: number; column: string }[];
};

// A row the settle sets to what the invariant says it holds (HIP-29's
// Reverts): rdb's row at the end of the chain, written as projection writes
// it, or no row when `row` is absent, then each of the row's changes still in
// the outbox, as capture recorded it. An update writes its columns, a delete
// removes the row, and an insert on the row's local id writes its whole row;
// an update or a delete of a row that isn't there does nothing. A row removed
// this way has its mapping marked `deleted` when it was `active`. `localId` is
// absent for a row named by its row hash only, one the delta reinstates or an
// own insert an observe voided: the target finds it by `rowId` after it writes
// the inserts' mappings, and a reinstated row with no mapping gets a new id.
export type RowRevert = {
    table: string;
    rowId: B64Hash;
    localId?: number;
    row?: Extract<RowAction, { kind: 'upsert-row' }>;
};

// The capture/read side of a bidirectional backend; pairs with
// MaterializationTarget (the write side). MECHANISM-AGNOSTIC: it only requires
// that writes originating from MaterializationTarget.apply never surface in
// drainChanges (echo suppression), leaving the mechanism (a session flag, a
// replication origin, ...) entirely to the backend.
export interface MaterializedChangeSource {
    // Drain captured-but-not-yet-ingested local changes, in commit order.
    drainChanges(): Promise<CapturedBatch>;

    // Whether the outbox holds a change, of any group. A pure read: unlike
    // drainChanges, it announces nothing. False on a target without capture.
    hasPendingChanges(): Promise<boolean>;

    // Resolve an existing projection-local row to its rdb identity (for
    // update/delete AND crash-replay uuid read-back), or undefined when the
    // local row has no sync mapping yet.
    resolveRow(table: string, localId: number): Promise<SyncMapping | undefined>;

    // Write into a change's outbox row what became of it (HIP-29), merged into
    // the record there (mergeChangeRecord). It signals no change and moves no
    // checkpoint. A row no longer in the outbox is left alone.
    writeRecord(id: CapturedChangeId, record: ChangeRecord): Promise<void>;

    // Add an observe the end of a pass wrote to the list beside the group's
    // checkpoint (HIP-29), and read that list, in the order recorded. Neither
    // signals a change or moves a checkpoint. Every settle clears every
    // group's list.
    recordEndObserve(groupId: B64Hash, hash: B64Hash): Promise<void>;
    getEndObserves(groupId: B64Hash): Promise<B64Hash[]>;

    // Atomically SETTLE an ingestion pass (IngestSettle): all or nothing. A
    // crash before the commit leaves the outbox and its records as they were,
    // and the next cycle's end of a pass settles them.
    commitIngest(settle: IngestSettle): Promise<void>;

    // Read the durable op-event log (cursor-based, non-destructive). Filter is
    // `id > afterId` (default 0) and, when set, `id < beforeId`. `order`
    // (default 'asc') is explicit and never inferred. A numeric argument is
    // `{ afterId }`. Live subscribe snapshots high-water with
    // `{ order: 'desc', limit: 1 }` (empty → 0).
    drainOpEvents(opts?: OpEventQuery | number): Promise<StoredOpEvent[]>;
}

// A backend that both materializes and captures (bidirectional).
export type BidirectionalTarget = MaterializationTarget & MaterializedChangeSource;

// A change the inverse planner / a bundle submission / a cross-group ref-advance
// could not accept. `change` names the offending captured change when the
// failure is tied to one (a planner reject, a bundle throw, or a ref-advance
// flushed just before a dependent write); a ref-advance failure during the
// end-of-pass drain has no originating change and instead carries `groupId`
// (the observing group whose ref could not be advanced).
export type IngestRejection = { change?: CapturedChange; groupId?: B64Hash; reason: string };

// The outcome of one ingestChanges pass: how many row ops were submitted, and
// the changes that were rejected (the reconciliation signal - the next
// projectGroup converges the local DB to rdb's verdict).
export type IngestResult = {
    accepted: number;
    rejected: IngestRejection[];
};

// FK projection metadata carried on a projected column with an integer id
// companion. The column is an integer id referencing `targetTable`'s serial
// `id`. For a LOCAL FK this is declared as an advisory, unenforced DB FK. For a
// co-projected CROSS-GROUP FK (`crossGroup: true`) the id is still translated at
// apply time, but NO DB-level FOREIGN KEY is declared (the referenced table
// belongs to another group and may be created in a different apply, so a DDL FK
// would impose a cross-table creation order). Non-co-projected cross-group FKs
// carry no metadata at all - they project as a plain text row_hash passthrough.
export type FkColumnInfo = { targetTable: string; crossGroup?: boolean };

// A materialized column: its target name plus the ColumnDef the target should
// realize. For a plain column this is the rdb ColumnDef verbatim; for an FK
// column it is the RETYPED companion (integer for local, text for cross-group)
// and `fk` names the referenced target table when local. For a key-ref column
// (`keyRef: true`) it is an integer id referencing `rdb_keys`; the incoming
// VALUE is a key hash to intern. The target owns the mapping of ColumnDef
// (type + constraints) to native SQL types.
export type SchemaActionColumn = {
    name: string;
    def: ColumnDef;
    fk?: FkColumnInfo;
    keyRef?: true;
};

// The ordered vocabulary the mapper emits. Names are already target names
// (renames applied); tables are bare. A column whose type changes in rdb (a new
// incarnation) is expressed as drop-column + add-column, never an in-place type
// change, matching the rdb schema model. `create-table` names its system columns
// (`primaryKey`, and `authorColumn` when present) so targets need no config.
// `create-table` / `drop-table` also name the per-table sync table (`syncTable`)
// so the target owns no naming config; the mapper resolves it from the rename
// config + sync-table suffix. The author column (when present) is an integer
// key-ref into `rdb_keys` — not a text KeyId.
//
// `drop-index` / `ensure-index` maintain projection-local secondary indexes
// (see "Projection-local indexes" below). The planner orders every drop-index
// BEFORE the table/column actions and every ensure-index AFTER them; a target
// may defer ensures further (e.g. past the row channel) within the same apply.
export type SchemaAction =
    // `schemaName`: the table's name in the group's schema, which the target
    // keeps in the table's meta with the group the apply is for (HIP-20).
    // Absent, as tests write them: the projected name is the schema's.
    | { kind: 'create-table'; table: string; syncTable: string; primaryKey: string;
        authorColumn?: string; columns: SchemaActionColumn[]; schemaName?: string }
    | { kind: 'drop-table'; table: string; syncTable: string }
    | { kind: 'add-column'; table: string; column: string; def: ColumnDef;
        fk?: FkColumnInfo; keyRef?: true }
    | { kind: 'drop-column'; table: string; column: string }
    | { kind: 'ensure-index'; index: ResolvedIndex }
    | { kind: 'drop-index'; table: string; name: string };

// ---------------------------------------------------------------------------
// Projection-local indexes. An index is a property of ONE projection, not of
// the replicated schema: each target stores one installed IndexSpec (written
// in rdb names) plus a record of the indexes it actually materialized (in
// target names). The core owns the ordered column list and the lifecycle
// (name resolution, completeness, drop/recreate across schema deltas); each
// target owns its `options` format (direction, collation, ...), which the core
// never inspects. A spec is therefore written for one target kind. Indexes
// never enforce anything (no UNIQUE): uniqueness is an rdb concern and a
// projection only accelerates reads.
// ---------------------------------------------------------------------------

// The pseudo-column naming the projected author column (`author_key_id`).
export const AUTHOR_INDEX_COLUMN = '@author';

export type IndexDecl = {
    // Unique within its group.
    name: string;
    // The rdb group name (RTableGroup.getName()).
    group: string;
    // The rdb table name.
    table: string;
    // Ordered rdb column names (or '@author'). The index is dropped whenever
    // any of these dies.
    columns: string[];
    // Target-defined and opaque to the core; validated by
    // IndexTarget.validateIndexOptions.
    options?: json.Literal;
};

export type IndexSpec = {
    indexes: IndexDecl[];
    // Default policy: one single-column index per `pub` column of every
    // projected table (named `pub__<column>`), in addition to `indexes`.
    indexPub?: boolean;
};

// An IndexDecl resolved against one group's schema view + AdapterConfig:
// target table/column names, ready for a target to realize.
export type ResolvedIndex = {
    name: string;
    groupId: B64Hash;
    table: string;
    columns: { rdb: string; target: string }[];
    options?: json.Literal;
    // Canonical JSON of name, table, columns and options: two resolutions are
    // the same physical index iff their fingerprints match.
    fingerprint: string;
};

export type IndexState = {
    spec?: IndexSpec;
    specFingerprint?: string;
    // What the target has actually built (all groups).
    materialized: ResolvedIndex[];
};

// Optional target capability (detected by duck typing, like KeyIndex). A
// target without it never receives index actions.
export interface IndexTarget {
    getIndexState(): Promise<IndexState>;
    // A reason string when `decl.options` is not acceptable to this target.
    validateIndexOptions(decl: IndexDecl): string | undefined;
    // Atomically: check `expect` (compare-and-set on the installed spec
    // fingerprint and on each listed group's checkpoint; on mismatch throw
    // CheckpointMovedError and change nothing), apply the index actions, and
    // store `spec` as the installed spec.
    installIndexSpec(
        spec: IndexSpec, specFingerprint: string, actions: SchemaAction[],
        expect: { specFingerprint: string | undefined; checkpoints: Map<B64Hash, Version> },
    ): Promise<void>;
}

export function isIndexTarget(t: object): t is IndexTarget {
    const c = t as Partial<IndexTarget>;
    return typeof c.getIndexState === 'function' && typeof c.installIndexSpec === 'function';
}

// The data-side vocabulary, the sibling of SchemaAction. Rows are addressed by
// their content-addressed `rowId`; the target maps that to the projection-local
// integer `id` through the per-table sync table (allocating on first sight,
// reusing across void-flip reinstatement). The planner never sees an `id`.
//
// `liveAfter` from the rdb RowChange drives the kind: a live row upserts, a
// non-live row (deleted OR voided by an at-use verdict flip) deletes. `values`
// keys and `table` are already TARGET names; `author` is carried out-of-band as
// a key hash so the target interns it into `author_key_id` (or drops it). For a
// provider / identity key-ref column the VALUE in `values` is still the key
// hash; `keyMaterial` optionally supplies the matching public key so the target
// can populate `rdb_keys.public_key` while interning (the public key itself is
// never a projected column). A `null` in `values` means "write NULL": the
// column holds no value in rdb, so the target clears it and does not fill in
// its default. A column left out of `values` is not touched in an existing
// row, and takes its default in a new one. An upsert without `author` is for a
// row rdb has no author for: the target writes NULL to its author column.
export type RowAction =
    | { kind: 'upsert-row'; table: string; rowId: B64Hash; author?: KeyId;
        values: { [column: string]: json.Literal | null };
        keyMaterial?: { [keyHash: string]: string } }
    | { kind: 'delete-row'; table: string; rowId: B64Hash };

// What concrete backends (SQLite / Postgres / IndexedDB / ...) implement. Each
// engine is a full, self-contained target (no shared dialect layer): there are
// deeper per-engine nuances best kept isolated rather than abstracted.
//
// A target is GROUP-AWARE: it can materialize several rdb groups of one RDb into
// one physical store (shared sync tables / id space, so cross-group FKs resolve
// to serial ids). Table/sync names are globally unique (group-qualified by the
// projection), so the only per-group state is the checkpoint, keyed by group id.
export interface MaterializationTarget {
    // Apply the ordered schema actions THEN the ordered row actions and persist
    // `checkpoint` as the new materialized version FOR `groupId`, ALL IN ONE
    // transaction (both channels + the checkpoint commit atomically, or
    // nothing). A crash must never leave the target claiming a checkpoint it
    // does not reflect. `events` (concurrency void/reinstate flips derived from
    // the delta's opVerdictChanges) are appended to the op-event log in the SAME
    // transaction, so a flip can never be lost across the checkpoint advance.
    //
    // `expectFrom` is a compare-and-set guard for concurrent projectors sharing
    // one store: `null` expects NO stored checkpoint (initial materialization), a
    // `Version` expects set-equality with the stored one, `undefined` skips the
    // check. On mismatch the target throws CheckpointMovedError and applies
    // NOTHING (the whole transaction rolls back) - another projector advanced the
    // group, and the caller must recompute the delta from the new checkpoint. The
    // compare happens under the write lock so two projectors can never both win.
    // A lagging replica whose frontier does not extend the new checkpoint will
    // then fail computeDelta until it syncs; that is intended.
    //
    // `expectIndexSpec` is the same kind of guard for an IndexTarget: the
    // fingerprint of the installed index spec the planner resolved against
    // (`null` = none installed, `undefined` = skip). On mismatch (a reconcile
    // installed a new spec meanwhile) throw CheckpointMovedError, apply nothing.
    apply(
        groupId: B64Hash, schemaActions: SchemaAction[], rowActions: RowAction[],
        checkpoint: Version, events?: OpEvent[], expectFrom?: Version | null,
        expectIndexSpec?: string | null,
    ): Promise<void>;

    // The last materialized version for `groupId`, or undefined when that group
    // has never been materialized (drives the initial-vs-delta decision).
    getCheckpoint(groupId: B64Hash): Promise<Version | undefined>;

    // Optional: release engine resources. `RdbProjection.stop()` calls this
    // after in-flight sync finishes. SqliteTarget closes the better-sqlite3
    // handle (checkpointing WAL/SHM); IdbTarget closes the IDB connection.
    // Idempotent. Absent on MemoryTarget (nothing to release).
    close?(): void | Promise<void>;
}

// Thrown by apply() when its `expectFrom` compare-and-set fails: another
// projector advanced this group's checkpoint since the delta was computed. The
// caller recomputes from the current checkpoint and retries. Nothing was applied.
export class CheckpointMovedError extends Error {
    constructor(readonly groupId: B64Hash) {
        super(`checkpoint for group '${groupId}' moved since the delta was computed`);
        this.name = 'CheckpointMovedError';
    }
}

// ---------------------------------------------------------------------------
// The writer record (HIP-20's What the projection records in the target): what
// a projection records in its target when it opens and whenever its groups
// change, so that capture, and a writer in another process, read it inside
// their own transactions. Capture fills in an empty author column with the
// writer key's id where the table's group mode is `on` (HIP-29).
// ---------------------------------------------------------------------------

// A group's authorship mode: `anonymous` for a group without an identity
// provider, else `on` or `off`, from `autoAuthor`.
export type AuthorshipMode = 'on' | 'off' | 'anonymous';

export type WriterRecord = {
    // The writer key's id in the key table, when the projection has a writer.
    writerKeyId?: number;
    // The replica's hash suite, by name.
    hashSuite: string;
    // Each projected group's member name and authorship mode, by group id.
    groups: { [groupId: string]: { name: string; mode: AuthorshipMode } };
};

export interface WriterRecordStore {
    // Replaces the record.
    recordWriter(record: WriterRecord): Promise<void>;
    // The record, or undefined when no projection has recorded one.
    readWriterRecord(): Promise<WriterRecord | undefined>;
}

export function isWriterRecordStore(t: object): t is WriterRecordStore {
    const c = t as Partial<WriterRecordStore>;
    return typeof c.recordWriter === 'function' && typeof c.readWriterRecord === 'function';
}

// Thrown by apply() while the outbox holds a change, of any group (HIP-29):
// projecting over a pending change would overwrite it, so the pass waits until
// ingestion has taken it. Nothing was applied. It is no error: the cycle that
// meets it ends, and the next one ingests the change first.
export class PendingChangesError extends Error {
    constructor(readonly groupId: B64Hash) {
        super(`a local change is pending: group '${groupId}' isn't projected until it is ingested`);
        this.name = 'PendingChangesError';
    }
}

// The delta of an ingestion chain changes the schema (HIP-29). A chain holds
// the pass's row ops and observes, and neither changes a group's schema, so
// this says rdb or the target is not what the pass assumed: the projection halts.
export class ChainSchemaChangeError extends Error {
    constructor(readonly groupId: B64Hash) {
        super(`the ingestion chain of group '${groupId}' changes its schema, which no observe or row op does`);
        this.name = 'ChainSchemaChangeError';
    }
}

// Set-equality of two checkpoints (a Version is an unordered Set of entry
// hashes). Used by targets to evaluate apply()'s expectFrom compare-and-set;
// `undefined` on either side means "no checkpoint", equal only to itself.
export function versionsEqual(a: Version | undefined, b: Version | undefined): boolean {
    if (a === undefined || b === undefined) return a === b;
    if (a.size !== b.size) return false;
    for (const h of a) if (!b.has(h)) return false;
    return true;
}

// ---------------------------------------------------------------------------
// Inbound reactivity: the optional local->rdb push signal. A capture-provisioned
// backend MAY implement this to notify observers that its outbox has advanced
// (i.e. local edits are waiting to be ingested), so a projection runtime can
// ingest reactively instead of polling. Shaped after dag's DagGrowthListener:
// at-least-once, dedup-yourself (an ingest pass is idempotent - it drains the
// outbox and re-derives from the persisted checkpoint), and lazily armed (a
// backend with no listeners pays no monitoring cost). A file-watch hint (e.g.
// SQLite WAL) must still be filtered to outbox growth: adapter writes that
// dirty the WAL without inserting into the outbox are not a signal.
// ---------------------------------------------------------------------------

// Opaque wake-up: the outbox advanced. Carries no payload; the observer reacts
// by running an ingest pass, which drains and de-duplicates authoritatively.
export type ChangeSignal = Record<string, never>;
export type ChangeSignalListener = (signal: ChangeSignal) => void;

export interface ChangeSignalSource {
    // Register a listener, arming any underlying monitor on the first one.
    addChangeListener(listener: ChangeSignalListener): void;
    // Remove a listener, disarming the monitor on the last one.
    removeChangeListener(listener: ChangeSignalListener): void;
}

// The backend-owned bijection between a row's projection-local integer `id` and
// its content-addressed rowId (`row_hash`). Exposed so an application holding a
// local id can obtain the stable, non-local identity of the row (and vice
// versa) - e.g. to build a cross-group FK reference by hand.
export interface RowIdentityIndex {
    // The rowId (row_hash) for a projection-local id, or undefined when the id
    // is unknown in `table`.
    rowHashForLocalId(table: string, id: number): Promise<string | undefined>;
    // The projection-local id for a rowId, or undefined when the rowId was never
    // seen in `table` (survives delete: the sync mapping outlives the app row).
    localIdForRowHash(table: string, rowHash: string): Promise<number | undefined>;
}

// ---------------------------------------------------------------------------
// Key index: the bijection between a projection-local key id (`rdb_keys.id`)
// and the crypto material (key hash + public key). Duplicates of the same key
// hash across provider rows collapse to ONE id. `domain` is the key-space
// namespace (v1 uses the single DEFAULT_KEY_DOMAIN); kept for symmetry with
// RowIdentityIndex's table/domain and for future namespacing.
// ---------------------------------------------------------------------------

export interface KeyIndex {
    // Get-or-allocate a key id for (keyHash, publicKey). Public key is
    // MANDATORY: rdb cannot ingest an identity without it (self-certification),
    // and it is what lets ingest reconstruct the rdb provider row. Idempotent
    // on keyHash; backfills public_key when previously NULL.
    registerKey(domain: string, keyHash: string, publicKey: string): Promise<number>;
    // The key hash for a projected key id, or undefined when unknown.
    keyHashForId(domain: string, id: number): Promise<string | undefined>;
    // The public key for a projected key id, or undefined when unknown / not
    // yet populated (an author-only intern without registerKey).
    publicKeyForId(domain: string, id: number): Promise<string | undefined>;
    // The projected key id for a key hash, or undefined when never seen.
    idForKeyHash(domain: string, keyHash: string): Promise<number | undefined>;
}

// ---------------------------------------------------------------------------
// Reactive queries. A target that can run a single-table plan and emit commit
// events. `local` names tables only. `apply` carries full post-images.
// ---------------------------------------------------------------------------

export type OrderDirection = 'asc' | 'desc';
export type OrderBySpec = { column: string; direction: OrderDirection };

export type BoundValue = json.Literal | null;

export type BoundOperand =
    | { kind: 'col'; name: string }
    | { kind: 'lit'; value: BoundValue }
    | { kind: 'arith'; op: '+' | '-' | '*'; left: BoundOperand; right: BoundOperand }
    | { kind: 'length'; arg: BoundOperand };

export type BoundWhereExpr =
    | { kind: 'cmp'; op: '=' | '!=' | '<' | '<=' | '>' | '>='; left: BoundOperand; right: BoundOperand }
    | { kind: 'isNull'; arg: BoundOperand; not: boolean }
    | { kind: 'in'; arg: BoundOperand; values: BoundValue[]; not: boolean }
    | { kind: 'like'; arg: BoundOperand; pattern: BoundOperand }
    | { kind: 'not'; arg: BoundWhereExpr }
    | { kind: 'and'; args: BoundWhereExpr[] }
    | { kind: 'or'; args: BoundWhereExpr[] };

export type CursorBound = {
    side: 'before' | 'after';
    id: number;
    cachedValues?: { [column: string]: json.Literal | null };
};

export type SingleTableQueryPlan = {
    table: string;
    select?: string[];
    where?: BoundWhereExpr;
    orderBy?: OrderBySpec[];
    cursor?: CursorBound;
    limit?: number;
};

export type ProjectedRow = {
    id: number;
    [column: string]: json.Literal | null;
};

export interface QueryableTarget {
    executeQuery(plan: SingleTableQueryPlan): Promise<ProjectedRow[]>;
}

export function isQueryableTarget(t: object): t is QueryableTarget {
    return typeof (t as Partial<QueryableTarget>).executeQuery === 'function';
}

export type TargetRowChange =
    | { kind: 'insert' | 'update'; table: string; localId: number; values: { [column: string]: json.Literal | null } }
    | { kind: 'delete'; table: string; localId: number };

export type TargetCommitEvent =
    | { source: 'local'; tables: string[] }
    | { source: 'apply'; groupId: B64Hash; schemaTables: string[]; changes: TargetRowChange[]; events: StoredOpEvent[] }
    | { source: 'settle'; revertedTables: string[]; events: StoredOpEvent[] }
    | { source: 'reconnect' };

export type TargetCommitListener = (commit: TargetCommitEvent) => void;

export interface ReactiveChangeTarget {
    addCommitListener(listener: TargetCommitListener): void;
    removeCommitListener(listener: TargetCommitListener): void;
    notifyExternalCommit(commit: TargetCommitEvent): void;
}

export function isReactiveChangeTarget(t: object): t is ReactiveChangeTarget {
    return typeof (t as Partial<ReactiveChangeTarget>).addCommitListener === 'function'
        && typeof (t as Partial<ReactiveChangeTarget>).notifyExternalCommit === 'function';
}

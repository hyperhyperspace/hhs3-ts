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
    // "adapter writer" (per-principal inbound authorship is out of scope).
    writer?: OwnIdentity;
    // Whether to bundle CONSECUTIVE same-group ops joined by an explicit FK arc
    // into ONE atomic entry (so an app's parent+child inserts become visible
    // together). Only consecutive, FK-linked ops bundle; nothing is reordered.
    // Default on; set false to emit every op as its own single-op entry. The
    // developer contract: to get parent-child atomicity, make the inserts
    // consecutive. On a bundle reject the orchestrator falls back to submitting
    // the constituent ops individually, in order.
    fkBundling?: boolean;
    // Whether to OPTIMISTICALLY merge a run of CONSECUTIVE update changes on the
    // SAME row whose column sets are DISJOINT into ONE rdb update op (so a
    // per-column capture stream folds back into a single write and cross-column
    // restrictions see the whole post-image). A repeated column, a different
    // row, or a non-update breaks the run. If the merged op fails validation the
    // orchestrator falls back to the per-change update ops, in order, each
    // validated independently. Default on; set false to emit one op per update
    // change (still order-preserving). Never folds an insert with a later
    // update, and never cancels an insert+delete - those are faithfully submitted.
    updateMerge?: boolean;

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

// One local mutation observed in the projected DB, in TARGET vocabulary.
// `localId` is the projection-local integer PK (`id`); the core resolves it to
// a content-addressed rowId via the sync table. `values` (insert/update) uses
// target column names and carries LOGICAL values; update carries only the
// columns that changed.
export type CapturedChange =
    | { id: CapturedChangeId; kind: 'insert'; table: string; localId: number;
        values: { [column: string]: json.Literal } }
    | { id: CapturedChangeId; kind: 'update'; table: string; localId: number;
        values: { [column: string]: json.Literal } }
    | { id: CapturedChangeId; kind: 'delete'; table: string; localId: number };

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
// per-table sync table. Reserved (for minted inserts) via reserveMint BEFORE
// the rdb append, and read (for updates/deletes and crash-replay read-back) via
// resolveRow. `table` is the TARGET table. `status` defaults to 'active'.
export type SyncMapping = {
    table: string;
    localId: number;
    rowId: B64Hash;
    uuid: string;
    author?: KeyId;
    status?: SyncStatus;
};

// A sync-row status transition applied during the settle transaction (e.g. a
// reverted insert orphan -> 'ingestion_failure'). Keyed by (target table, rowId).
export type SyncStatusUpdate = { table: string; rowId: B64Hash; status: SyncStatus };

// ---------------------------------------------------------------------------
// Op-event log: the single, durable, app-observable channel for BOTH ingestion
// failures (a local change rdb rejected) and p2p concurrency verdict flips (an
// op voided or reinstated by a later barrier). The app monitors it (via the
// projection's push channel, or by reading rdb_op_events directly) to learn
// that a change it made - or observed - was voided, exactly as it learns of a
// concurrency void. Append-only, idempotent by (opHash, direction).
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

// Everything the settle transaction persists atomically at the end of an
// ingestion pass (see MaterializedChangeSource.commitIngest).
export type IngestSettle = {
    // The drained change ids to ack (delete from the outbox). Processed-based,
    // NOT a high-water mark, so late-committing rows are never lost.
    consumed: CapturedChangeId[];
    // Sync mappings to (re)persist. Minted-insert identities are already durable
    // via reserveMint; these are redundant-but-idempotent confirmations.
    mappings?: SyncMapping[];
    // Op-events to append (INSERT-OR-IGNORE on (opHash, direction)).
    events?: OpEvent[];
    // Targeted full-row re-materializations undoing genuinely-failed local ops
    // (upsert-row from rdb truth, or delete-row for an insert orphan). Applied
    // under echo-suppression so they never re-enter the outbox.
    reverts?: RowAction[];
    // Sync-row status transitions applied AFTER reverts (e.g. an orphaned insert
    // -> 'ingestion_failure').
    statuses?: SyncStatusUpdate[];
};

// The capture/read side of a bidirectional backend; pairs with
// MaterializationTarget (the write side). MECHANISM-AGNOSTIC: it only requires
// that writes originating from MaterializationTarget.apply never surface in
// drainChanges (echo suppression), leaving the mechanism (a session flag, a
// replication origin, ...) entirely to the backend.
export interface MaterializedChangeSource {
    // Drain captured-but-not-yet-ingested local changes, in commit order.
    drainChanges(): Promise<CapturedBatch>;

    // Resolve an existing projection-local row to its rdb identity (for
    // update/delete AND crash-replay uuid read-back), or undefined when the
    // local row has no sync mapping yet.
    resolveRow(table: string, localId: number): Promise<SyncMapping | undefined>;

    // Durably persist reserved sync rows (row_hash, id, uuid, status='active')
    // BEFORE the rdb append, in their OWN transaction. Called for every minted
    // insert of a pass (including ones that will reject). So if a crash lands
    // between append and settle, a replay reads the SAME uuid back (via
    // resolveRow) and reproduces the identical insert payload / rowId, letting
    // the reappended op be recognized as already-applied instead of duplicated.
    // Idempotent (upsert by row_hash).
    reserveMint(reservations: SyncMapping[]): Promise<void>;

    // Atomically SETTLE an ingestion pass: (re)persist mappings, append
    // op-events (idempotent by (opHash, direction)), apply reverts under
    // echo-suppression, transition sync statuses, and ack (delete) the consumed
    // outbox rows - all or nothing. A crash before commit leaves the outbox
    // intact for a deterministic replay; there is no cross-store transaction
    // with the rdb DAG, so idempotent replay + idempotent logging bridge the gap.
    commitIngest(settle: IngestSettle): Promise<void>;

    // Read op-events logged after `sinceId` (cursor-based, non-destructive),
    // oldest id first. The supervisor advances its cursor by the max returned
    // id; the durable backlog is re-read on restart.
    drainOpEvents(sinceId?: number): Promise<StoredOpEvent[]>;
}

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
export type SchemaAction =
    | { kind: 'create-table'; table: string; syncTable: string; primaryKey: string;
        authorColumn?: string; columns: SchemaActionColumn[] }
    | { kind: 'drop-table'; table: string; syncTable: string }
    | { kind: 'add-column'; table: string; column: string; def: ColumnDef;
        fk?: FkColumnInfo; keyRef?: true }
    | { kind: 'drop-column'; table: string; column: string };

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
// never a projected column).
export type RowAction =
    | { kind: 'upsert-row'; table: string; rowId: B64Hash; author?: KeyId;
        values: { [column: string]: json.Literal };
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
    apply(
        groupId: B64Hash, schemaActions: SchemaAction[], rowActions: RowAction[],
        checkpoint: Version, events?: OpEvent[],
    ): Promise<void>;

    // The last materialized version for `groupId`, or undefined when that group
    // has never been materialized (drives the initial-vs-delta decision).
    getCheckpoint(groupId: B64Hash): Promise<Version | undefined>;
}

// ---------------------------------------------------------------------------
// Inbound reactivity: the optional local->rdb push signal. A capture-provisioned
// backend MAY implement this to notify observers that its outbox has advanced
// (i.e. local edits are waiting to be ingested), so a projection runtime can
// ingest reactively instead of polling. Shaped after dag's DagGrowthListener:
// at-least-once, dedup-yourself (an ingest pass is idempotent - it drains the
// outbox and re-derives from the persisted checkpoint), and lazily armed (a
// backend with no listeners pays no monitoring cost).
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

// RTableView: row reads at one (at, from) horizon over the table's scoped
// projection of the group DAG.
//
// The effective schema is itself resolved at (at, from): a schema
// deploy is a barrier ref-advance, so a deploy concurrent to `at` visible from
// `from` revises the schema at the merged frontier — newly added restrictions
// and columns (with defaults) activate, exactly like a concurrent row barrier.
//
// Liveness is INCARNATION-SCOPED: identity ops (insert and delete) are
// meta-tagged with the TABLE incarnation active at write time
// (`rows: ['<tableIncarnationId>:<rowId>']`, see ../rtable_group/scopes.ts).
// Updates do not carry `rows` — they are column writes (`cols`). All liveness
// cover queries filter on the CURRENT table incarnation at this horizon. So a
// table drop+re-add (or a losing concurrent-create fork) starts a fresh row
// namespace: prior-incarnation inserts/deletes never match, old rows go
// non-live, and a rowId may be re-inserted under the new incarnation. If the
// table does not exist at this horizon the row is not live. This mirrors
// column-value incarnation scoping (below); together they make a table reset
// behave like a fresh table.
//
// Liveness has two layers:
//
//   1. Permanent-delete state (rowIds are write-once identities WITHIN a table
//      incarnation): some VALID insert at or below `at`, no valid delete at or
//      below `at`, and no valid delete barrier concurrent to `at` visible from
//      `from` whose table has concurrentDeletes enabled. Deletes are ALWAYS
//      barrier-tagged at write; the concurrentDeletes flag is resolved AT-USE,
//      per concurrent delete, at THAT delete's own position observed from
//      `from`. A concurrent delete tagged for a DIFFERENT incarnation cannot
//      kill or barrier this row. A causally-later flip of the flag never
//      revises an old delete; a flip concurrent to the delete does. No revival,
//      ever. A schema deploy that adopts an FK is a delete of each row it
//      strands: it carries the rows' identity tags and the barrier tag, and
//      unwraps to a `kills` marker (opsFor), so both walks above read it like a
//      delete op (see deriveDeployMeta in ../rtable_group/group.ts).
//   2. Op validity (drop-on-void): an entry is VOID when any row op it carries
//      fails its restriction predicate OR writes an FK column whose target is
//      not live, both evaluated AT-USE at the op's own position observed from
//      this view's `from` (a witness / FK target barrier-deleted concurrently
//      with the use voids it; a causally-later delete, or a causally-later
//      add-fk / drop-fk, does not). FK targets resolve recursively across
//      tables; a `group.table` target resolves through the bound foreign group
//      at the version observed at the op's position (resolveForeignTableView); a missing reference (unbound name or absent
//      foreign table) voids the op. An FK reference cycle is void: nothing
//      outside it supports it. Bundles void all-or-nothing.
//      Voided ops are invisible: a voided insert never lives, a voided delete
//      does not kill, a voided update (or dangling FK write) contributes no
//      writes (LWW reverts). The computation is the group's (it spans tables);
//      see isEntryVoided in ../rtable_group/group.ts.
//
// Values are resolved per-field LWW through cover queries: every insert and
// update tags its carried columns in entry meta (`cols: ['<rowId>:<incarnationId>:<column>']`,
// see ../rtable_group/scopes.ts), keyed by the schema birth write active at
// write time. At read, the cover is scoped to the live incarnation for that
// column name at this horizon, so drop/re-add and losing concurrent add-column
// forks do not resurrect stale writes. Voided entries are see-through in the
// DAG cover (`findCoverWithFilter` with a liveness predicate): their writes
// don't count, and they must not mask live writes below. Concurrent maxima
// tiebreak by larger entry hash; untouched columns fall back to schema
// defaults. Updates never affect liveness.
//
// Inside a verdict evaluation an entry of the component being solved can be
// undecided. Each read below is one see-through walk that treats undecided
// entries as void and records them (VOID_SEMANTICS.md section 4): liveness is
// the exact Kleene value of inserted, not deleted, not killed; a column read
// that meets an undecided write is undecided. The reads predicates use return
// Truth; the public reads require a decided result, which every top-level
// view has.
//
// Pub search (findRowIds) resolves: pub meta is exported by inserts AND
// updates, so stale values in old entries are candidate noise — every
// candidate row's RESOLVED values are re-checked before it qualifies. A
// payload-only prefilter drops rows that no write could make match, before
// any verdict is read.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { EntryMetaFilter, EntryPredicate, Position, position } from "@hyper-hyper-space/hhs3_dag";
import { version, Version, ScopedDag } from "@hyper-hyper-space/hhs3_mvt";
import { deserializePublicKeyFromBase64 } from "@hyper-hyper-space/hhs3_mvt";
import {
    TRUE, FALSE, truthOf, verdictTruth, notTruth, allTruth, anyTruth, everyTruth, someTruth,
} from "@hyper-hyper-space/hhs3_mvt";
import type { Truth, Undecided, Verdict, VerdictEvaluation } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchemaView } from "../rschema/interfaces.js";
import type { IncarnationId } from "../rschema/incarnation.js";
import type { ColumnType, Operand } from "../rschema/payload.js";
import { colTag, rowTag, type RowsSlicePayload } from "../rtable_group/scopes.js";
import type { OpVoidDetail } from "../rtable_group/op_void.js";

import type { RTable, RTableView, Row, RowValues, DeltaRowState } from "./interfaces.js";
import type { InsertRowPayload, RowOpPayload } from "./payload.js";
import type { ColumnTypes, RowFilter, RowQuery } from "./query.js";
import { evalRowFilter, orderRows, projectRow, validateRowQuery } from "./query.js";

// What the view needs from its table beyond the public contract. Entry voiding
// is threaded with the view's VerdictEvaluation (see
// ../rtable_group/verdict_evaluation.ts): a view built inside a void
// computation carries that computation's evaluation, so recursive voiding
// shares one stack and one memo. (resolveForeignTableView is intentionally
// absent: the view never does a foreign lookup that mints its own evaluation —
// that path lives entirely in the group's helpers that receive one.)
export type TableViewTarget = RTable & {
    resolveSchemaView(at: Version, from?: Version): Promise<RSchemaView>;
    entryStatusIn(evaluation: VerdictEvaluation<OpVoidDetail>, entryHash: B64Hash, from: Version): Promise<Verdict<OpVoidDetail>>;
};

// A column read: the resolved value (undefined when no live write carries the
// column; callers fall back to the schema default), or undecided.
export type ColumnValue = { status: 'decided'; value: json.Literal | undefined } | Undecided;

// The three-valued reads predicates evaluate over (see
// ../rtable_group/predicates.ts). `author` is the row's insert author, known
// whenever some insert of the row was met.
export interface TruthTableView {
    rowLiveness(rowId: B64Hash): Promise<{ truth: Truth; author?: KeyId }>;
    columnValue(rowId: B64Hash, column: string): Promise<ColumnValue>;
    existsMatching(where: { [field: string]: json.Literal }): Promise<Truth>;
}

function decided(truth: Truth, read: string): boolean {
    if (truth.status === 'undecided') throw new Error(`${read} is undecided outside the component being solved`);
    return truth.status === 'true';
}

function decidedValue(read: ColumnValue, what: string): json.Literal | undefined {
    if (read.status === 'undecided') throw new Error(`${what} is undecided outside the component being solved`);
    return read.value;
}

function sameValue(a: json.Literal, b: json.Literal): boolean {
    return json.toStringNormalized(a) === json.toStringNormalized(b);
}

// Enumerate ALL entries matching `filter` at or below `at` (covers only
// return the causally-maximal matches): descend from the cover through each
// match's predecessors.
async function findAllWithFilter(dag: ScopedDag, at: Version, filter: EntryMetaFilter): Promise<Set<B64Hash>> {
    const found = new Set<B64Hash>();
    const queue: B64Hash[] = [...(await dag.findCoverWithFilter(at, filter))];

    while (queue.length > 0) {
        const hash = queue.shift()!;
        if (found.has(hash)) continue;
        found.add(hash);

        const entry = await dag.loadEntry(hash);
        if (entry === undefined) continue;
        const preds = position(...json.fromSet(entry.header.prevEntryHashes));
        queue.push(...(await dag.findCoverWithFilter(preds, filter)));
    }

    return found;
}

// Extract the ops for `rowId` carried by an unwrapped table-scope payload:
// a plain row op, a 'rows' slice (group create / bundle), or the 'kills'
// marker of a schema deploy, which deletes every row whose tag it carries (the
// entry reached this row only through such a tag).
function opsFor(payload: json.Literal, rowId: B64Hash): RowOpPayload[] {
    const p = payload as json.LiteralMap;

    if (p['action'] === 'rows') {
        return (p as RowsSlicePayload).ops.filter((op) => op.rowId === rowId);
    }

    if (p['action'] === 'kills') {
        return [{ action: 'delete', rowId }];
    }

    if (p['rowId'] === rowId) {
        return [p as RowOpPayload];
    }

    return [];
}

export class RTableViewImpl implements RTableView, TruthTableView {

    private target: TableViewTarget;
    private at: Version;
    private from: Version;
    private evaluation: VerdictEvaluation<OpVoidDetail>;
    private exclude: B64Hash | undefined;

    private _schemaView: RSchemaView | undefined;

    // `evaluation` is mandatory: every view is built inside some void computation
    // (top-level callers mint a fresh one), so entry-voiding recursion always
    // has an evaluation to thread. See verdict_evaluation.ts. `exclude` is the
    // entry being diagnosed through this view: an op is not its own witness or
    // barrier, so its own entry reads as void here.
    constructor(target: TableViewTarget, at: Version, from: Version, evaluation: VerdictEvaluation<OpVoidDetail>, exclude?: B64Hash) {
        this.target = target;
        this.at = at;
        this.from = from;
        this.evaluation = evaluation;
        this.exclude = exclude;
    }

    getObject(): RTable {
        return this.target;
    }

    getVersion(): Version {
        return this.at;
    }

    getFromVersion(): Version {
        return this.from;
    }

    async getReferences(): Promise<B64Hash[]> {
        return [];   // references (schema, bindings) belong to the group
    }

    async resolveRefVersion(_refId: B64Hash): Promise<Version> {
        throw new Error("RTable holds no references (see the group view)");
    }

    private async schemaView(): Promise<RSchemaView> {
        if (this._schemaView === undefined) {
            // the effective schema at this horizon is revised by deploy
            // barriers concurrent to `at` visible from `from`: a
            // concurrent schema deploy activates new restrictions / FKs /
            // columns at the merged frontier, like any other barrier.
            this._schemaView = await this.target.resolveSchemaView(this.at, this.from);
        }
        return this._schemaView;
    }

    // Every call below is sequentially awaited on purpose: the evaluation's
    // stack and verdict memo assume one traversal per evaluation. Do not
    // Promise.all per-row work on a single view without minting an evaluation
    // per branch.
    private async entryTruth(entryHash: B64Hash): Promise<Truth> {
        if (entryHash === this.exclude) return FALSE;
        return verdictTruth(await this.target.entryStatusIn(this.evaluation, entryHash, this.from));
    }

    // One see-through cover walk: void and undecided matches are seen through,
    // so they cannot mask live writes below. Returns the cover of live matches
    // and the undecided matches the walk met that no live member of it
    // dominates (a dominated one stays below that member in every completion).
    private async walk(
        dag: ScopedDag, cover: (predicate: EntryPredicate) => Promise<Position>,
    ): Promise<{ live: Position; undecided: Map<B64Hash, Undecided> }> {
        const met = new Map<B64Hash, Undecided>();
        const live = await cover(async (hash) => {
            const truth = await this.entryTruth(hash);
            if (truth.status === 'undecided') {
                met.set(hash, truth);
                return false;
            }
            return truth.status === 'true';
        });

        const undecided = new Map<B64Hash, Undecided>();
        for (const [hash, truth] of met) {
            if (live.size === 0 || (await dag.findMinimalCover(position(...live, hash))).has(hash)) undecided.set(hash, truth);
        }
        return { live, undecided };
    }

    private async opsAt(dag: ScopedDag, hash: B64Hash, rowId: B64Hash): Promise<RowOpPayload[]> {
        const entry = await dag.loadEntry(hash);
        return entry === undefined ? [] : opsFor(entry.payload, rowId);
    }

    // Whether a delete barrier concurrent to `at` (visible from `from`) kills
    // `rowId`: honored per-delete, AT-USE, iff the concurrentDeletes flag is
    // enabled at THAT delete's own position observed from `from`. A causally-
    // later flip of the flag never revises an old delete; a flip concurrent to
    // the delete does. Deletes are always barrier-tagged. A voided delete is
    // seen through (it does not kill). An undecided honored delete may kill; an
    // undecided unhonored one may sit above a live honored delete and hide it,
    // so it leaves that kill undecided.
    private async killedByConcurrentDelete(rowId: B64Hash, table: string, incarnation: IncarnationId): Promise<Truth> {
        const dag = await this.target.getScopedDag();
        const { live, undecided } = await this.walk(dag, (predicate) => dag.findConcurrentCoverWithFilter(
            this.from, this.at,
            { containsValues: { barrier: ['t'], rows: [rowTag(incarnation, rowId)] } },
            predicate));

        const honored = async (hash: B64Hash): Promise<boolean> => {
            const schemaAtDelete = await this.target.resolveSchemaView(version(hash), this.from);
            return schemaAtDelete.hasTable(table) && schemaAtDelete.getConcurrentDeletes(table);
        };
        const mayKill: Truth[] = [];
        const mayHide: Truth[] = [];
        for (const [hash, truth] of undecided) (await honored(hash) ? mayKill : mayHide).push(truth);

        for (const hash of live) {
            if (await honored(hash)) return mayHide.length === 0 ? TRUE : anyTruth(mayHide);
        }
        return anyTruth(mayKill);
    }

    // Whether `rowId` is live at this horizon, with an insert of the row. It
    // implements both liveness layers (see header): permanent-delete state
    // (incl. at-use concurrentDeletes) and drop-on-void op filtering
    // (restriction + FK reach, folded into entry voiding). Live means
    // inserted, not deleted, and not killed by a concurrent delete, each the
    // `or` over its entries; every entry appears once, so this is decided
    // exactly when every way of deciding the undecided entries agrees.
    // Duplicate concurrent inserts of the same rowId (same uuid + author) are
    // the SAME incarnation; the largest live entry hash provides the insert
    // deterministically (their column writes participate in per-column
    // resolution like any other write). Without a live insert, an undecided
    // one is returned (every insert of a rowId carries its uuid and author).
    private async rowState(rowId: B64Hash): Promise<{ truth: Truth; insert?: InsertRowPayload }> {
        const dag = await this.target.getScopedDag();
        const table = this.target.getTableName();

        // liveness is scoped to the CURRENT table incarnation: ops tagged for a
        // prior incarnation (before a drop+re-add, or a losing concurrent-create
        // fork) never match, so the table truly resets.
        const incarnation = (await this.schemaView()).getTableIncarnation(table);
        if (incarnation === undefined) return { truth: FALSE };   // table not live here

        // identity cover among live inserts/deletes: a voided identity op must
        // not mask a live insert or delete below it. Updates do not match
        // `rows`, so they are not walked. A delete can only sit above inserts,
        // so the cover holds a delete exactly when some delete is live.
        const { live, undecided } = await this.walk(dag, (predicate) => dag.findCoverWithFilter(
            this.at,
            { containsValues: { rows: [rowTag(incarnation, rowId)] } },
            predicate));

        let winner: InsertRowPayload | undefined;
        let winnerHash: B64Hash | undefined;
        for (const hash of live) {
            for (const op of await this.opsAt(dag, hash, rowId)) {
                if (op.action === 'delete') return { truth: FALSE };   // permanent: the row is dead
                if (op.action === 'insert' && (winnerHash === undefined || hash > winnerHash)) {
                    winner = op;
                    winnerHash = hash;
                }
            }
        }

        const undecidedInserts: Truth[] = [];
        const undecidedDeletes: Truth[] = [];
        let undecidedInsert: InsertRowPayload | undefined;
        for (const [hash, truth] of undecided) {
            for (const op of await this.opsAt(dag, hash, rowId)) {
                if (op.action === 'delete') undecidedDeletes.push(truth);
                if (op.action === 'insert') {
                    undecidedInserts.push(truth);
                    undecidedInsert = undecidedInsert ?? op;
                }
            }
        }

        const inserted = winner !== undefined ? TRUE : anyTruth(undecidedInserts);
        if (inserted.status === 'false') return { truth: FALSE };

        // a live delete barrier concurrent to `at`, visible from `from`, kills
        // the row even though it is not in the row's history, honored at-use
        // per the concurrentDeletes flag at the delete's position (see
        // killedByConcurrentDelete).
        const killed = await this.killedByConcurrentDelete(rowId, table, incarnation);
        return {
            truth: allTruth([inserted, notTruth(anyTruth(undecidedDeletes)), notTruth(killed)]),
            insert: winner ?? undecidedInsert,
        };
    }

    async rowLiveness(rowId: B64Hash): Promise<{ truth: Truth; author?: KeyId }> {
        const { truth, insert } = await this.rowState(rowId);
        return { truth, author: insert?.author };
    }

    // The LWW-resolved value for one column of a row: undefined if no live
    // write at or below `at` carries it for the column's live incarnation at
    // this horizon. The see-through cover of the incarnation-scoped write meta
    // (`cols: ['<rowId>:<incarnationId>:<column>']`) is the causal maxima
    // among live writes. Concurrent maxima tiebreak by larger entry hash. A
    // walk that meets an undecided write not below a live one is undecided
    // (any other write lies below a live write, so it cannot change the
    // value). Does NOT check liveness.
    async columnValue(rowId: B64Hash, column: string): Promise<ColumnValue> {
        const schemaView = await this.schemaView();
        const table = this.target.getTableName();
        const incarnation = schemaView.getColumnIncarnation(table, column);
        if (incarnation === undefined) return { status: 'decided', value: undefined };

        const dag = await this.target.getScopedDag();
        const filter: EntryMetaFilter = { containsValues: { cols: [colTag(rowId, incarnation, column)] } };
        const { live, undecided } = await this.walk(dag, (predicate) => dag.findCoverWithFilter(this.at, filter, predicate));
        if (undecided.size > 0) return anyTruth(undecided.values()) as Undecided;

        let winnerHash: B64Hash | undefined;
        let winnerValue: json.Literal | undefined;

        for (const hash of live) {
            for (const op of await this.opsAt(dag, hash, rowId)) {
                if (op.action !== 'insert' && op.action !== 'update') continue;
                const value = op.values[column];
                if (value === undefined) continue;

                if (winnerHash === undefined || hash > winnerHash) {
                    winnerHash = hash;
                    winnerValue = value;
                }
            }
        }

        return { status: 'decided', value: winnerValue };
    }

    // Per-field LWW resolution over the schema's columns; untouched columns
    // fall back to schema defaults.
    private async resolveRow(rowId: B64Hash): Promise<{ insert: InsertRowPayload; values: RowValues } | undefined> {
        const insert = await this.liveInsert(rowId);
        if (insert === undefined) return undefined;

        const schemaView = await this.schemaView();
        const def = schemaView.getTable(this.target.getTableName());

        const values: RowValues = {};
        for (const column of Object.keys(def?.columns ?? {})) {
            const written = decidedValue(await this.columnValue(rowId, column), 'a column value');
            if (written !== undefined) {
                values[column] = written;
            } else if (def!.columns[column].default !== undefined) {
                values[column] = def!.columns[column].default!;
            }
        }

        return { insert, values };
    }

    private async liveInsert(rowId: B64Hash): Promise<InsertRowPayload | undefined> {
        const { truth, insert } = await this.rowState(rowId);
        return decided(truth, 'row liveness') ? insert : undefined;
    }

    async hasRow(rowId: B64Hash): Promise<boolean> {
        return (await this.liveInsert(rowId)) !== undefined;
    }

    async getRow(rowId: B64Hash): Promise<Row | undefined> {
        const resolved = await this.resolveRow(rowId);
        if (resolved === undefined) return undefined;

        const row: Row = { rowId: resolved.insert.rowId, uuid: resolved.insert.uuid, values: resolved.values };
        if (resolved.insert.author !== undefined) row.author = resolved.insert.author;
        return row;
    }

    async getAuthor(rowId: B64Hash): Promise<KeyId | undefined> {
        return (await this.liveInsert(rowId))?.author;
    }

    // Delta support (see ./delta.ts). The schema columns at this horizon.
    async getColumns(): Promise<string[]> {
        const schemaView = await this.schemaView();
        const def = schemaView.getTable(this.target.getTableName());
        return Object.keys(def?.columns ?? {});
    }

    // Delta support (see ./delta.ts). Enforced liveness + author + the LWW
    // WRITTEN value of each requested column, with NO schema-default fallback:
    // columnValue is incarnation-scoped at this horizon, so a column dropped
    // or re-added between horizons yields no written value for the old
    // incarnation, and a column's default never appears as a per-row write.
    // The delta asks only for the columns it compares (the end horizon's, and
    // at start those with the same incarnation).
    async deltaRowState(rowId: B64Hash, columns: string[]): Promise<DeltaRowState> {
        const insert = await this.liveInsert(rowId);
        if (insert === undefined) return { live: false, author: undefined, written: {} };

        const written: RowValues = {};
        for (const column of columns) {
            const value = decidedValue(await this.columnValue(rowId, column), 'a column value');
            if (value !== undefined) written[column] = value;
        }
        return { live: true, author: insert.author, written };
    }

    // Every live rowId at this horizon: enumerate identity ops (inserts and
    // deletes now carry `rows`; updates do not; a schema deploy's `kills`
    // marker carries no insert), collect candidate insert rowIds, then re-check
    // enforced liveness. No pub/author index is available, so this is a full
    // table scan (used once per FK-adopting deploy, see deriveDeployMeta).
    async liveRowIds(): Promise<B64Hash[]> {
        const dag = await this.target.getScopedDag();
        const candidates = await findAllWithFilter(dag, this.at, { containsKeys: ['rows'] });

        const candidateRowIds = new Set<B64Hash>();
        for (const hash of candidates) {
            const entry = await dag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as json.LiteralMap;
            const ops: RowOpPayload[] = p['action'] === 'rows'
                ? (p as RowsSlicePayload).ops
                : [p as RowOpPayload];
            for (const op of ops) {
                if (op.action === 'insert') candidateRowIds.add(op.rowId);
            }
        }

        const live: B64Hash[] = [];
        for (const rowId of candidateRowIds) {
            if ((await this.liveInsert(rowId)) !== undefined) live.push(rowId);
        }
        return live.sort();
    }

    // Liveness-BYPASSED provider read: the publicKey registered for `keyId`
    // under this table's idProvider designation, at this view's `at`, or
    // undefined. This is the KeyLookup behind signature verification, so it must
    // NOT route through liveInsert / entry voiding / FK reach (that would recurse
    // authentication through authorization). Registration is self-certifying and
    // grants no authority, so reading an "unenrolled" / "revoked" identity is
    // harmless. Any insert carrying the keyId is authoritative (keyId ==
    // hash(publicKey) is enforced at insert; collisions reduce to the hash's).
    async rawProviderPublicKey(keyId: KeyId): Promise<PublicKey | undefined> {
        const schemaView = await this.schemaView();
        const table = this.target.getTableName();
        const provider = schemaView.getIdProvider(table);
        if (provider === undefined) return undefined;

        const dag = await this.target.getScopedDag();
        const filter: EntryMetaFilter = { containsValues: {
            ['pub-' + provider.keyIdColumn]: [json.toStringNormalized(keyId)],
        } };

        const matches = await findAllWithFilter(dag, this.at, filter);
        for (const hash of matches) {
            const entry = await dag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as json.LiteralMap;
            const ops: RowOpPayload[] = p['action'] === 'rows'
                ? (p as RowsSlicePayload).ops
                : [p as RowOpPayload];
            for (const op of ops) {
                if (op.action !== 'insert') continue;
                if (op.values[provider.keyIdColumn] !== keyId) continue;
                const pkVal = op.values[provider.publicKeyColumn];
                if (typeof pkVal !== 'string') continue;
                try {
                    return deserializePublicKeyFromBase64(pkVal);
                } catch {
                    return undefined;
                }
            }
        }
        return undefined;
    }

    async findRowIds(where: { [pubColumn: string]: json.Literal }): Promise<B64Hash[]> {
        const candidates = await this.candidateRowIds(where);
        const rowIds: B64Hash[] = [];
        for (const rowId of candidates) {
            if (decided(await this.candidateMatch(rowId, where), 'a findRowIds candidate')) rowIds.push(rowId);
        }
        return rowIds;
    }

    // Whether some row matches `where`: the `or` over the candidates of
    // findRowIds, stopping at the first that surely matches.
    async existsMatching(where: { [field: string]: json.Literal }): Promise<Truth> {
        const candidates = await this.candidateRowIds(where);
        return someTruth(candidates, (rowId) => this.candidateMatch(rowId, where));
    }

    // Rows some write of which could make them match, before any verdict is
    // read. Indexed candidates via pub meta, or rowAuthor system meta: pub
    // values are mutable, so ONE field drives the index query. Every other
    // field must be carried by some insert or update of the row too, unless the
    // column's default equals the searched value (a row that never wrote it
    // still matches). The rowId binds the insert's author, so rowAuthor, which
    // drives the index whenever it is searched, needs no further check.
    private async candidateRowIds(where: { [field: string]: json.Literal }): Promise<B64Hash[]> {
        const schemaView = await this.schemaView();
        const table = this.target.getTableName();
        const pubColumns = new Set(schemaView.getPubColumns(table));

        const fields = Object.keys(where);
        if (fields.length === 0) {
            throw new Error("findRowIds requires at least one where field");
        }
        for (const field of fields) {
            if (field !== 'rowAuthor' && !pubColumns.has(field)) {
                throw new Error(`'${field}' is not a pub column of table '${table}'`);
            }
        }

        const indexField = fields.includes('rowAuthor') ? 'rowAuthor' : fields[0];
        if (indexField === 'rowAuthor' && typeof where[indexField] !== 'string') {
            throw new Error("'rowAuthor' search requires a key-id string");
        }

        const dag = await this.target.getScopedDag();
        const candidates = await this.carriers(dag, indexField, where[indexField]);
        const def = schemaView.getTable(table);
        for (const field of fields) {
            if (field === indexField) continue;
            const fallback = def?.columns[field]?.default;
            if (fallback !== undefined && sameValue(fallback, where[field])) continue;
            const carrying = await this.carriers(dag, field, where[field]);
            for (const rowId of [...candidates]) {
                if (!carrying.has(rowId)) candidates.delete(rowId);
            }
        }
        return [...candidates].sort();
    }

    // Rows with an insert or update carrying `value` for `field` (rowAuthor:
    // an insert by that author), found through the meta index.
    private async carriers(dag: ScopedDag, field: string, value: json.Literal): Promise<Set<B64Hash>> {
        const indexValue = field === 'rowAuthor' ? value as string : json.toStringNormalized(value);
        const filter: EntryMetaFilter = { containsValues: {
            // surfaced `rowAuthor` indexes the internal meta key `author`
            [field === 'rowAuthor' ? 'author' : 'pub-' + field]: [indexValue],
        } };

        const rowIds = new Set<B64Hash>();
        for (const hash of await findAllWithFilter(dag, this.at, filter)) {
            const entry = await dag.loadEntry(hash);
            if (entry === undefined) continue;

            const p = entry.payload as json.LiteralMap;
            const ops: RowOpPayload[] = p['action'] === 'rows'
                ? (p as RowsSlicePayload).ops
                : [p as RowOpPayload];

            for (const op of ops) {
                if (op.action !== 'insert' && op.action !== 'update') continue;
                const carried = field === 'rowAuthor' && op.action === 'insert'
                    ? op.author
                    : op.values[field];
                if (carried !== undefined && sameValue(carried, value)) rowIds.add(op.rowId);
            }
        }
        return rowIds;
    }

    // A candidate matches when every searched column resolves to the searched
    // value and the row is live: a Kleene `and`, values first, so a row whose
    // value already rules it out never has its liveness read.
    private async candidateMatch(rowId: B64Hash, where: { [field: string]: json.Literal }): Promise<Truth> {
        const def = (await this.schemaView()).getTable(this.target.getTableName());
        const checks: (() => Promise<Truth>)[] = [];
        for (const field of Object.keys(where)) {
            if (field === 'rowAuthor') continue;
            checks.push(async () => {
                const read = await this.columnValue(rowId, field);
                if (read.status === 'undecided') return read;
                const value = read.value ?? def?.columns[field]?.default;
                return truthOf(value !== undefined && sameValue(value, where[field]));
            });
        }
        checks.push(async () => (await this.rowLiveness(rowId)).truth);
        return everyTruth(checks, (check) => check());
    }

    // Single-table query at this horizon (see ./query.ts). A LOCAL read: it
    // reuses the same enforced-liveness + LWW resolution as getRow/findRowIds,
    // so its result is exactly the live rows satisfying the filter — never a
    // stale index hit. Validation is user-facing (throws on mistakes).
    async query(q: RowQuery): Promise<Row[]> {
        const schemaView = await this.schemaView();
        const table = this.target.getTableName();
        const def = schemaView.getTable(table);

        const columns: ColumnTypes = {};
        for (const [name, cdef] of Object.entries(def?.columns ?? {})) {
            columns[name] = cdef.type;
        }
        validateRowQuery(q, columns);

        // Candidate selection: push ONE top-level AND conjunct down to an
        // index (pub-eq or author), else full scan. findRowIds / liveRowIds
        // already apply enforced liveness + resolved re-check; the residual
        // evalRowFilter below re-checks the FULL filter, so an index hit that
        // is stale for the OTHER conjuncts is dropped, exactly like findRowIds
        // re-checks its own searched fields.
        const pubColumns = new Set(schemaView.getPubColumns(table));
        const pushable = this.pushableConjunct(q.where, pubColumns);

        let candidates: B64Hash[];
        if (pushable?.kind === 'pub') {
            candidates = await this.findRowIds({ [pushable.column]: pushable.value });
        } else if (pushable?.kind === 'author') {
            candidates = await this.findRowIds({ rowAuthor: pushable.author });
        } else {
            candidates = await this.liveRowIds();
        }

        const typeOf = (column: string): ColumnType | undefined => column === 'rowAuthor' ? 'string' : columns[column];

        const rows: Row[] = [];
        for (const rowId of candidates) {
            const row = await this.getRow(rowId);
            if (row === undefined) continue;   // not live at this horizon
            if (q.where === undefined || evalRowFilter(q.where, row, typeOf)) rows.push(row);
        }

        const ordered = q.orderBy !== undefined && q.orderBy.length > 0
            ? orderRows(rows, q.orderBy, typeOf)
            : rows.sort((a, b) => (a.rowId < b.rowId ? -1 : a.rowId > b.rowId ? 1 : 0));

        const offset = q.offset ?? 0;
        let sliced = ordered.slice(offset);
        if (q.limit !== undefined) sliced = sliced.slice(0, q.limit);

        return q.select !== undefined ? sliced.map((r) => projectRow(r, q.select!)) : sliced;
    }

    // Pick one pushable conjunct from the top-level AND (or the whole filter):
    // a `cmp eq` of a pub/system column against a literal -> index. undefined -> full scan. The
    // residual filter re-check makes any choice here safe (it never changes the
    // result, only the candidate set).
    private pushableConjunct(
        where: RowFilter | undefined, pubColumns: Set<string>,
    ): { kind: 'pub'; column: string; value: json.Literal } | { kind: 'author'; author: string } | undefined {
        if (where === undefined) return undefined;
        const conjuncts = where.p === 'and' ? where.args : [where];

        for (const c of conjuncts) {
            if (c.p === 'cmp' && c.cmp === 'eq') {
                const pair = colLitPair(c.left, c.right);
                if (pair !== undefined && pair.column === 'rowAuthor' && typeof pair.value === 'string') {
                    return { kind: 'author', author: pair.value };
                }
                if (pair !== undefined && pubColumns.has(pair.column)) {
                    return { kind: 'pub', column: pair.column, value: pair.value };
                }
            }
        }
        return undefined;
    }
}

// A column-vs-literal operand pair (either order), for index pushdown.
function colLitPair(a: Operand, b: Operand): { column: string; value: json.Literal } | undefined {
    if ('col' in a && 'lit' in b) return { column: a.col, value: b.lit };
    if ('lit' in a && 'col' in b) return { column: b.col, value: a.lit };
    return undefined;
}

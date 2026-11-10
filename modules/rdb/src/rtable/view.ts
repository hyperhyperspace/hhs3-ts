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
// Pub search (findRowIds, existsMatching) resolves: pub meta is exported by
// inserts AND updates, so stale values in old entries are candidate noise —
// every candidate row's RESOLVED values are re-checked before it qualifies. A
// payload-only prefilter drops rows that no write could make match, before
// any verdict is read. existsMatching reads the `where` tree of an exists
// atom, leaves under `and` / `or`, the same way: candidates are the tree over
// sets of rows, and on each candidate a leaf the row's writes cannot satisfy
// is false with nothing read.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { EntryMetaFilter, EntryPredicate, Position, position } from "@hyper-hyper-space/hhs3_dag";
import { version, Version, ScopedDag } from "@hyper-hyper-space/hhs3_mvt";
import type { CausalDag, PendingRefAdvance } from "@hyper-hyper-space/hhs3_mvt";
import { deserializePublicKeyFromBase64 } from "@hyper-hyper-space/hhs3_mvt";
import {
    TRUE, FALSE, truthOf, verdictTruth, notTruth, allTruth, anyTruth, everyTruth, someTruth, seeThroughCover,
} from "@hyper-hyper-space/hhs3_mvt";
import type { Truth, Undecided, Verdict, VerdictEvaluation } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchemaView } from "../rschema/interfaces.js";
import type { IncarnationId } from "../rschema/incarnation.js";
import type { ColumnType, Operand, TableDef } from "../rschema/payload.js";
import { columnSlot, existenceSlot } from "../rschema/resolve.js";
import { colTag, rowTag, type RowsSlicePayload } from "../rtable_group/scopes.js";
import type { OpVoidDetail } from "../rtable_group/op_void.js";

import type { RTable, RTableView, Row, RowValues, DeltaRowState } from "./interfaces.js";
import type { InsertRowPayload, RowOpPayload } from "./payload.js";
import type { ColumnTypes, RowFilter, RowQuery } from "./query.js";
import { evalRowFilter, orderRows, projectRow, validateRowQuery } from "./query.js";
import { TouchTest } from "./touch.js";

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
    getCausalDag(): Promise<CausalDag>;
    schemaSlotsBetween(at: Version, wider: Version, from: Version): Promise<Set<string>>;
};

// A column read: the resolved value (undefined when no live write carries the
// column; callers fall back to the schema default), or undecided.
export type ColumnValue = { status: 'decided'; value: json.Literal | undefined } | Undecided;

// The `where` tree of an exists atom as a view reads it, with every term
// resolved to its value. A leaf holds when every field equals its value.
// `row` marks a leaf that names a subject-row term. `aside` marks a leaf set
// aside because a subject value it names is undecided, with that value's
// support: the reads take the leaf as false and never look at its fields.
export type ReadLeaf = { fields: { [field: string]: json.Literal }; row: boolean; aside?: Undecided };
export type ReadWhere = ReadLeaf | { and: ReadWhere[] } | { or: ReadWhere[] };

export function readLeaves(where: ReadWhere, out: ReadLeaf[] = []): ReadLeaf[] {
    if ('fields' in where) {
        out.push(where);
    } else {
        for (const member of 'and' in where ? where.and : where.or) readLeaves(member, out);
    }
    return out;
}

// What a view answers about a `where` tree (see existsTruth in
// ../rtable_group/predicates.ts, which combines it with the subject row).
//
// `touched` holds the leaves a pending observe touches, each with the touching
// observes. The reads take a touched leaf as false, like a leaf set aside.
//
// `read` is the `or` over the rows that match:
//   'all'    the whole tree (R);
//   'plain'  the tree with every `row` leaf false too (R0);
//   'row'    only the matches through a `row` leaf (R$): a `row` leaf matches
//            through itself, an `or` when a member does, and an `and` when one
//            member does and its other members match.
export type ExistsReads = {
    touched: Map<ReadLeaf, Undecided>;
    read(which: 'all' | 'plain' | 'row'): Promise<Truth>;
};

// The three-valued reads predicates evaluate over (see
// ../rtable_group/predicates.ts). `author` is the row's insert author, known
// whenever some insert of the row was met.
export interface TruthTableView {
    rowLiveness(rowId: B64Hash): Promise<{ truth: Truth; author?: KeyId }>;
    columnValue(rowId: B64Hash, column: string): Promise<ColumnValue>;
    existsMatching(where: ReadWhere): Promise<ExistsReads>;
}

// The rows that carry a field or satisfy a node on "carried" alone. 'all'
// stands for every inserted row without listing them: a field whose value is
// the column's default, which a row that never wrote the column holds too.
type RowSet = ReadonlySet<B64Hash> | 'all';

const NO_ROWS: RowSet = new Set<B64Hash>();

// An intersection ignores 'all'.
function intersectRows(sets: RowSet[]): RowSet {
    let rows: RowSet = 'all';
    for (const set of sets) {
        if (set === 'all') continue;
        if (rows === 'all') {
            rows = set;
        } else {
            const both = new Set<B64Hash>();
            for (const rowId of rows) if (set.has(rowId)) both.add(rowId);
            rows = both;
        }
    }
    return rows;
}

// A union with an 'all' member is 'all'.
function uniteRows(sets: RowSet[]): RowSet {
    const rows = new Set<B64Hash>();
    for (const set of sets) {
        if (set === 'all') return 'all';
        for (const rowId of set) rows.add(rowId);
    }
    return rows;
}

// What the reads of one `where` tree share: the table at this horizon, the
// rows that carry each leaf, and the candidates of the whole tree, sorted.
type ReadPlan = {
    def: TableDef | undefined;
    carriedBy: Map<ReadLeaf, RowSet>;
    candidates: B64Hash[];
};

// The reads made on one candidate row, kept across the passes of one tree.
type RowReads = {
    columns: Map<string, ColumnValue>;
    leaves: Map<ReadLeaf, Truth>;
    live?: Truth;
};

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
    private pending: readonly PendingRefAdvance[];

    private _schemaView: RSchemaView | undefined;
    private _touch: TouchTest | undefined;

    // `evaluation` is mandatory: every view is built inside some void computation
    // (top-level callers mint a fresh one), so entry-voiding recursion always
    // has an evaluation to thread. See verdict_evaluation.ts. `exclude` is the
    // entry being diagnosed through this view: an op is not its own witness or
    // barrier, so its own entry reads as void here.
    //
    // `pending` is non-empty only for a bound group read through the
    // observer's three-valued fold while some of the observer's observes of it
    // are undecided: `at` is then D, the version the live observes give, and
    // each read applies the touch rule (./touch.ts) before answering at D.
    constructor(
        target: TableViewTarget, at: Version, from: Version, evaluation: VerdictEvaluation<OpVoidDetail>,
        exclude?: B64Hash, pending: readonly PendingRefAdvance[] = [],
    ) {
        this.target = target;
        this.at = at;
        this.from = from;
        this.evaluation = evaluation;
        this.exclude = exclude;
        this.pending = pending;
    }

    // The touch test of this view's reads, or undefined when nothing is
    // pending, where every read is today's read at `at`.
    private touchTest(): TouchTest | undefined {
        if (this.pending.length === 0) return undefined;
        if (this._touch === undefined) {
            this._touch = new TouchTest(this.target, this.at, this.from, this.pending, this.evaluation, async (hash) => {
                const truth = await this.entryTruth(hash);
                if (truth.status === 'undecided') {
                    throw new Error(`entry '${hash}' of a bound group is undecided while the observer's read is open`);
                }
                return truth.status === 'true';
            });
        }
        return this._touch;
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
        return seeThroughCover(
            (p) => dag.findMinimalCover(p),
            (accept) => cover((hash) => accept(hash)),
            (hash) => this.entryTruth(hash),
        );
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
        const touch = this.touchTest();

        // With pending observes, the existence slot comes first: it fixes the
        // incarnation the identity tags carry (the touch rule, ./touch.ts).
        if (touch !== undefined) {
            const touched = await touch.slotsTouched([existenceSlot(table)]);
            if (touched !== undefined) return { truth: touched };
        }

        // liveness is scoped to the CURRENT table incarnation: ops tagged for a
        // prior incarnation (before a drop+re-add, or a losing concurrent-create
        // fork) never match, so the table truly resets.
        const incarnation = (await this.schemaView()).getTableIncarnation(table);
        if (incarnation === undefined) return { truth: FALSE };   // table not live here

        // identity cover among live inserts/deletes: a voided identity op must
        // not mask a live insert or delete below it. Updates do not match
        // `rows`, so they are not walked. A delete can only sit above inserts,
        // so the cover holds a delete exactly when some delete is live.
        const identity: EntryMetaFilter = { containsValues: { rows: [rowTag(incarnation, rowId)] } };
        if (touch !== undefined) {
            const touched = await touch.touched({
                slots: [],
                history: [identity],
                concurrent: [{ containsValues: { barrier: ['t'], rows: [rowTag(incarnation, rowId)] } }],
            });
            if (touched !== undefined) return { truth: touched };
        }
        const { live, undecided } = await this.walk(dag, (predicate) => dag.findCoverWithFilter(this.at, identity, predicate));

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
        const touch = this.touchTest();
        if (touch !== undefined) {
            const touched = await touch.slotsTouched([existenceSlot(table), columnSlot(table, column)]);
            if (touched !== undefined) return touched;
        }
        const incarnation = schemaView.getColumnIncarnation(table, column);
        if (incarnation === undefined) return { status: 'decided', value: undefined };

        const dag = await this.target.getScopedDag();
        const filter: EntryMetaFilter = { containsValues: { cols: [colTag(rowId, incarnation, column)] } };
        if (touch !== undefined) {
            const touched = await touch.touched({ slots: [], history: [filter], concurrent: [] });
            if (touched !== undefined) return touched;
        }
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

    // Every live rowId at this horizon: the inserted rowIds, re-checked for
    // enforced liveness. No pub/author index is available, so this is a full
    // table scan (used once per FK-adopting deploy, see deriveDeployMeta).
    async liveRowIds(): Promise<B64Hash[]> {
        const live: B64Hash[] = [];
        for (const rowId of await this.insertedRowIds(await this.target.getScopedDag(), this.at)) {
            if ((await this.liveInsert(rowId)) !== undefined) live.push(rowId);
        }
        return live.sort();
    }

    // Every rowId with an insert at or below `at`, before any verdict is read:
    // enumerate identity ops (inserts and deletes carry `rows`; updates do not;
    // a schema deploy's `kills` marker carries no insert).
    private async insertedRowIds(dag: ScopedDag, at: Version): Promise<Set<B64Hash>> {
        const rowIds = new Set<B64Hash>();
        for (const hash of await findAllWithFilter(dag, at, { containsKeys: ['rows'] })) {
            const entry = await dag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as json.LiteralMap;
            const ops: RowOpPayload[] = p['action'] === 'rows'
                ? (p as RowsSlicePayload).ops
                : [p as RowOpPayload];
            for (const op of ops) {
                if (op.action === 'insert') rowIds.add(op.rowId);
            }
        }
        return rowIds;
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

    // The live rows whose resolved values match `where`, one leaf: every field
    // equals its value. A field that is not a pub column (or a non-string
    // rowAuthor) throws.
    async findRowIds(where: { [pubColumn: string]: json.Literal }): Promise<B64Hash[]> {
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
        if (fields.includes('rowAuthor') && typeof where['rowAuthor'] !== 'string') {
            throw new Error("'rowAuthor' search requires a key-id string");
        }

        const leaf: ReadLeaf = { fields: where, row: false };
        const plan = await this.readPlan(leaf, () => true, this.at);
        const rowIds: B64Hash[] = [];
        if (plan === undefined) return rowIds;
        for (const rowId of plan.candidates) {
            if (decided(await this.rowMatch(rowId, leaf, plan, 'all', () => true, this.rowReads()), 'a findRowIds candidate')) rowIds.push(rowId);
        }
        return rowIds;
    }

    // The reads of a `where` tree (see ExistsReads): the `or` over the rows
    // that match it, stopping at the first that surely matches.
    //
    // A field is CARRIED by a row when an insert or update of the row carries
    // its value, or when the value is the column's default: a row that never
    // wrote the column holds the default and left no meta for it. `rowAuthor`
    // is carried exactly by the rows that author inserted (the rowId binds the
    // insert's author). A field that is missing or not a pub column at this
    // horizon (or a non-string rowAuthor) is carried by no row, so its leaf
    // fails closed.
    //
    // CANDIDATES, the rows some write of which could make them match, are the
    // tree evaluated over sets of rows before any verdict is read: a field is
    // its carriers, through the meta index, a leaf intersects its fields, an
    // `and` intersects its members and an `or` unites them. A field at its
    // default is every inserted row, kept as a symbol: an intersection ignores
    // it and a union with it is every inserted row, which are listed only when
    // that is the root's set.
    //
    // Each candidate is then evaluated in two passes (rowMatch): on "carried"
    // alone, where a leaf or group that is false is false on the row with
    // nothing read, and then with the three-valued reads on what is left.
    //
    // With pending observes (./touch.ts), each leaf's slots are checked first:
    // the table's existence slot and the slot of every column the leaf names.
    // A touched leaf is false in the reads and reported in `touched`; a leaf
    // set aside is not checked. The answers at D hold only while those slots
    // are untouched, so the reads use untouched columns alone. Candidates are
    // then gathered at D plus every pending version, which holds every
    // completion's candidates, "carried" is judged at that same version, and
    // each candidate's column reads and liveness are touched or not on their
    // own.
    async existsMatching(where: ReadWhere): Promise<ExistsReads> {
        const table = this.target.getTableName();
        const touch = this.touchTest();

        const touched = new Map<ReadLeaf, Undecided>();
        if (touch !== undefined) {
            for (const leaf of readLeaves(where)) {
                if (leaf.aside !== undefined || Object.keys(leaf.fields).length === 0) continue;
                const slots = [
                    existenceSlot(table),
                    ...Object.keys(leaf.fields).filter((f) => f !== 'rowAuthor').map((f) => columnSlot(table, f)),
                ];
                const touching = await touch.slotsTouched(slots);
                if (touching !== undefined) touched.set(leaf, touching);
            }
        }
        const isRead = (leaf: ReadLeaf): boolean => leaf.aside === undefined && !touched.has(leaf);

        let plan: Promise<ReadPlan | undefined> | undefined;
        const reads = new Map<B64Hash, RowReads>();
        const readsOf = (rowId: B64Hash): RowReads => {
            let rowReads = reads.get(rowId);
            if (rowReads === undefined) {
                rowReads = this.rowReads();
                reads.set(rowId, rowReads);
            }
            return rowReads;
        };

        return {
            touched,
            read: async (which) => {
                // nothing to read when every leaf is set aside or touched
                if (!readLeaves(where).some(isRead)) return FALSE;
                plan ??= (async () => this.readPlan(where, isRead, touch === undefined ? this.at : await touch.gatherVersion()))();
                const ready = await plan;
                if (ready === undefined) return FALSE;
                return someTruth(ready.candidates, (rowId) => this.rowMatch(rowId, where, ready, which, isRead, readsOf(rowId)));
            },
        };
    }

    private rowReads(): RowReads {
        return { columns: new Map(), leaves: new Map() };
    }

    // The table at this horizon, the rows that carry each leaf `isRead` keeps,
    // and the tree's candidates. Writes are gathered at `at`, which is the
    // view's own version unless existsMatching widens it; pub columns and
    // defaults are this view's. undefined when the table does not exist here:
    // no row matches.
    private async readPlan(where: ReadWhere, isRead: (leaf: ReadLeaf) => boolean, at: Version): Promise<ReadPlan | undefined> {
        const schemaView = await this.schemaView();
        const table = this.target.getTableName();
        if (!schemaView.hasTable(table)) return undefined;
        const pubColumns = new Set(schemaView.getPubColumns(table));
        const def = schemaView.getTable(table);
        const dag = await this.target.getScopedDag();

        // one index lookup per distinct field and value
        const carriers = new Map<string, RowSet>();
        const fieldRows = async (field: string, value: json.Literal): Promise<RowSet> => {
            if (field === 'rowAuthor') {
                if (typeof value !== 'string') return NO_ROWS;
            } else if (!pubColumns.has(field)) {
                return NO_ROWS;
            }
            const fallback = def?.columns[field]?.default;
            if (fallback !== undefined && sameValue(fallback, value)) return 'all';
            const key = JSON.stringify([field, json.toStringNormalized(value)]);
            let rows = carriers.get(key);
            if (rows === undefined) {
                rows = await this.carriers(dag, field, value, at);
                carriers.set(key, rows);
            }
            return rows;
        };

        const carriedBy = new Map<ReadLeaf, RowSet>();
        const rowsOf = async (node: ReadWhere): Promise<RowSet> => {
            if ('fields' in node) {
                const fields = Object.keys(node.fields);
                if (!isRead(node) || fields.length === 0) return NO_ROWS;
                const sets: RowSet[] = [];
                for (const field of fields) sets.push(await fieldRows(field, node.fields[field]));
                const rows = intersectRows(sets);
                carriedBy.set(node, rows);
                return rows;
            }
            const sets: RowSet[] = [];
            for (const member of 'and' in node ? node.and : node.or) sets.push(await rowsOf(member));
            return 'and' in node ? intersectRows(sets) : uniteRows(sets);
        };

        const rows = await rowsOf(where);
        const candidates = rows === 'all' ? await this.insertedRowIds(dag, at) : rows;
        return { def, carriedBy, candidates: [...candidates].sort() };
    }

    // Rows with an insert or update at or below `at` carrying `value` for
    // `field` (rowAuthor: an insert by that author), found through the meta
    // index.
    private async carriers(dag: ScopedDag, field: string, value: json.Literal, at: Version): Promise<Set<B64Hash>> {
        const indexValue = field === 'rowAuthor' ? value as string : json.toStringNormalized(value);
        const filter: EntryMetaFilter = { containsValues: {
            // surfaced `rowAuthor` indexes the internal meta key `author`
            [field === 'rowAuthor' ? 'author' : 'pub-' + field]: [indexValue],
        } };

        const rowIds = new Set<B64Hash>();
        for (const hash of await findAllWithFilter(dag, at, filter)) {
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

    // One candidate row against the tree: the `and` of the tree and the row's
    // liveness, values first, so a row whose values already rule it out never
    // has its liveness read.
    //
    // The tree is evaluated in two passes. First on "carried" alone: a leaf is
    // possible when the row carries every field of it, and a leaf or group
    // that is not possible is false with nothing read. Then what is left with
    // the three-valued reads: a field is the column read compared with its
    // value, with the schema default when no live write gives the column one
    // (rowAuthor needs no read). `which` picks the read (see ExistsReads).
    private async rowMatch(
        rowId: B64Hash, where: ReadWhere, plan: ReadPlan, which: 'all' | 'plain' | 'row',
        isRead: (leaf: ReadLeaf) => boolean, reads: RowReads,
    ): Promise<Truth> {
        const carried = (leaf: ReadLeaf): boolean => {
            const rows = plan.carriedBy.get(leaf);
            return rows !== undefined && (rows === 'all' || rows.has(rowId));
        };
        const counted = which === 'plain' ? (leaf: ReadLeaf) => isRead(leaf) && !leaf.row : isRead;

        const possible = (node: ReadWhere): boolean => {
            if ('fields' in node) return counted(node) && carried(node);
            return 'and' in node ? node.and.every(possible) : node.or.some(possible);
        };
        const possibleThroughRow = (node: ReadWhere): boolean => {
            if ('fields' in node) return node.row && possible(node);
            if ('or' in node) return node.or.some(possibleThroughRow);
            return node.and.every(possible) && node.and.some(possibleThroughRow);
        };

        const column = async (name: string): Promise<ColumnValue> => {
            let read = reads.columns.get(name);
            if (read === undefined) {
                read = await this.columnValue(rowId, name);
                reads.columns.set(name, read);
            }
            return read;
        };
        const leafValue = async (leaf: ReadLeaf): Promise<Truth> => {
            let truth = reads.leaves.get(leaf);
            if (truth === undefined) {
                truth = await everyTruth(Object.keys(leaf.fields).filter((f) => f !== 'rowAuthor'), async (field) => {
                    const read = await column(field);
                    if (read.status === 'undecided') return read;
                    const value = read.value ?? plan.def?.columns[field]?.default;
                    return truthOf(value !== undefined && sameValue(value, leaf.fields[field]));
                });
                reads.leaves.set(leaf, truth);
            }
            return truth;
        };

        const value = async (node: ReadWhere): Promise<Truth> => {
            if (!possible(node)) return FALSE;
            if ('fields' in node) return leafValue(node);
            return 'and' in node ? everyTruth(node.and, value) : someTruth(node.or, value);
        };
        const throughRow = async (node: ReadWhere): Promise<Truth> => {
            if (!possibleThroughRow(node)) return FALSE;
            if ('fields' in node) return leafValue(node);
            if ('or' in node) return someTruth(node.or, throughRow);
            const members = [...node.and.keys()];
            return someTruth(members, (through) =>
                everyTruth(members, (i) => i === through ? throughRow(node.and[i]) : value(node.and[i])));
        };

        const tree = await (which === 'row' ? throughRow(where) : value(where));
        if (tree.status === 'false') return FALSE;
        reads.live ??= (await this.rowLiveness(rowId)).truth;
        return allTruth([tree, reads.live]);
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

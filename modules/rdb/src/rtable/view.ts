// RTableView: row reads at one (at, from) horizon over the table's scoped
// projection of the group DAG.
//
// The effective schema is itself resolved at (at, from): a schema
// deploy is a barrier ref-advance, so a deploy concurrent to `at` visible from
// `from` revises the schema at the merged frontier — newly added restrictions
// and columns (with defaults) activate, exactly like a concurrent row barrier.
//
// Liveness has two layers:
//
//   1. Permanent-delete state (rowIds are write-once identities): some VALID
//      insert at or below `at`, no valid delete at or below `at`, and no valid
//      delete barrier concurrent to `at` visible from `from` whose table has
//      concurrentDeletes enabled. Deletes are ALWAYS barrier-tagged at write;
//      the concurrentDeletes flag is resolved AT-USE, per concurrent delete,
//      at THAT delete's own position observed from `from`. A causally-later
//      flip of the flag never revises an old delete; a flip concurrent to the
//      delete does. No revival, ever.
//   2. Op validity (drop-on-void): an entry is VOID when any row op it carries
//      fails its restriction predicate OR writes an FK column whose target is
//      not live, both evaluated AT-USE at the op's own position observed from
//      this view's `from` (a witness / FK target barrier-deleted concurrently
//      with the use voids it; a causally-later delete, or a causally-later
//      add-fk / drop-fk, does not). FK targets resolve recursively across
//      tables; a `group.table` target resolves through the bound foreign group
//      at the version observed at the op's position (resolveForeignTableView); a missing reference (unbound name or absent
//      foreign table) voids the op. An FK reference cycle resolves to DENY
//      (the group's least-fixpoint void guard). Bundles void all-or-nothing.
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
// forks do not resurrect stale writes. Voided entries in the cover are
// descended through (their writes don't count, but they must not mask valid
// writes below); candidates surfaced that way are pruned if dominated by
// another candidate. Concurrent maxima tiebreak by larger entry hash;
// untouched columns fall back to schema defaults. Updates never affect
// liveness.
//
// Pub search (findRowIds) resolves: pub meta is exported by inserts AND
// updates, so stale values in old entries are candidate noise — every
// candidate row's RESOLVED values are re-checked before it qualifies.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { EntryMetaFilter, position } from "@hyper-hyper-space/hhs3_dag";
import { version, Version, ScopedDag } from "@hyper-hyper-space/hhs3_mvt";
import { deserializePublicKeyFromBase64 } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchemaView } from "../rschema/interfaces.js";
import type { ColumnType, Operand } from "../rschema/payload.js";
import { colTag, type RowsSlicePayload } from "../rtable_group/scopes.js";

import type { RTable, RTableView, Row, RowValues, DeltaRowState } from "./interfaces.js";
import type { InsertRowPayload, RowOpPayload } from "./payload.js";
import type { ColumnTypes, RowFilter, RowQuery } from "./query.js";
import { evalRowFilter, orderRows, projectRow, validateRowQuery } from "./query.js";

// What the view needs from its table beyond the public contract.
export type TableViewTarget = RTable & {
    resolveSchemaView(at: Version, from?: Version): Promise<RSchemaView>;
    isEntryVoided(entryHash: B64Hash, from: Version): Promise<boolean>;
    // Resolve a `group.table` FK target through the bound foreign group at the
    // foreign version observed at (at, from). undefined = unbound name or table
    // absent at that version (a missing reference -> target not live); throws
    // on a missing bound object.
    resolveForeignTableView(
        group: string, table: string, at: Version, from: Version,
    ): Promise<RTableView | undefined>;
};

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
// a plain row op, or a 'rows' slice (group create / bundle).
function opsFor(payload: json.Literal, rowId: B64Hash): RowOpPayload[] {
    const p = payload as json.LiteralMap;

    if (p['action'] === 'rows') {
        return (p as RowsSlicePayload).ops.filter((op) => op.rowId === rowId);
    }

    if (p['rowId'] === rowId) {
        return [p as RowOpPayload];
    }

    return [];
}

export class RTableViewImpl implements RTableView {

    private target: TableViewTarget;
    private at: Version;
    private from: Version;

    private _schemaView: RSchemaView | undefined;

    constructor(target: TableViewTarget, at: Version, from: Version) {
        this.target = target;
        this.at = at;
        this.from = from;
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

    private entryVoided(entryHash: B64Hash): Promise<boolean> {
        return this.target.isEntryVoided(entryHash, this.from);
    }

    // The winning insert by BASE delete-state liveness only: inserted at or
    // below `at`, not deleted (deletes are permanent and count whether voided
    // or not), no concurrent delete barrier (concurrentDeletes tables). This
    // ignores view-time restriction rechecks and FK reach — it is the
    // write-time identity check (an FK-hidden but undeleted row is still a
    // valid update target, so it can be repaired). Reads use the enforced
    // `liveInsert` instead.
    private async baseLiveInsert(rowId: B64Hash): Promise<InsertRowPayload | undefined> {
        const dag = await this.target.getScopedDag();
        const table = this.target.getTableName();

        const history = await findAllWithFilter(dag, this.at, { containsValues: { rows: [rowId] } });

        let winner: InsertRowPayload | undefined;
        let winnerHash: B64Hash | undefined;

        for (const hash of history) {
            const entry = await dag.loadEntry(hash);
            if (entry === undefined) continue;
            for (const op of opsFor(entry.payload, rowId)) {
                if (op.action === 'delete') return undefined;   // permanent
                if (op.action !== 'insert') continue;
                if (winnerHash === undefined || hash > winnerHash) {
                    winner = op;
                    winnerHash = hash;
                }
            }
        }

        if (winner === undefined) return undefined;

        // concurrentDeletes is resolved AT-USE, per delete barrier (base
        // liveness ignores view-time restriction/FK rechecks — deletes count
        // whether voided or not).
        if (await this.killedByConcurrentDelete(rowId, table, false)) return undefined;

        return winner;
    }

    // Whether a delete barrier concurrent to `at` (visible from `from`) kills
    // `rowId`: honored per-delete, AT-USE, iff the concurrentDeletes flag is
    // enabled at THAT delete's own position observed from `from`. A causally-
    // later flip of the flag never revises an old delete; a flip concurrent to
    // the delete does. Deletes are always barrier-tagged. When `checkVoided` is
    // set a voided delete is skipped (the enforced path); base liveness counts
    // deletes regardless of voiding.
    private async killedByConcurrentDelete(rowId: B64Hash, table: string, checkVoided: boolean): Promise<boolean> {
        const dag = await this.target.getScopedDag();
        const concurrentBarriers = await dag.findConcurrentCoverWithFilter(
            this.from, this.at,
            { containsValues: { barrier: ['t'], rows: [rowId] } });

        for (const hash of concurrentBarriers) {
            if (checkVoided && await this.entryVoided(hash)) continue;
            const schemaAtDelete = await this.target.resolveSchemaView(version(hash), this.from);
            if (schemaAtDelete.hasTable(table) && schemaAtDelete.getConcurrentDeletes(table)) return true;
        }
        return false;
    }

    // Base delete-state liveness (see baseLiveInsert): the write-time identity
    // check for updates and deletes.
    async hasRowBase(rowId: B64Hash): Promise<boolean> {
        return (await this.baseLiveInsert(rowId)) !== undefined;
    }

    // The winning insert for rowId at this horizon, or undefined if the row
    // is not live. Implements both liveness layers (see header): valid
    // permanent-delete state (incl. at-use concurrentDeletes) and drop-on-void
    // op filtering (restriction + FK reach, folded into entry voiding).
    // Duplicate concurrent inserts of the same rowId (same uuid + author) are
    // the SAME incarnation; the largest entry hash provides uuid/author
    // deterministically (their column writes participate in per-column
    // resolution like any other write).
    private async liveInsert(rowId: B64Hash): Promise<InsertRowPayload | undefined> {
        const dag = await this.target.getScopedDag();
        const table = this.target.getTableName();

        // full history (not just the cover): a voided entry in the cover
        // must not mask valid ops below it
        const history = await findAllWithFilter(dag, this.at, { containsValues: { rows: [rowId] } });

        let winner: InsertRowPayload | undefined;
        let winnerHash: B64Hash | undefined;

        for (const hash of history) {
            const entry = await dag.loadEntry(hash);
            if (entry === undefined) continue;

            const ops = opsFor(entry.payload, rowId);
            if (ops.length === 0) continue;
            if (await this.entryVoided(hash)) continue;   // drop-on-void (restriction + FK)

            for (const op of ops) {
                if (op.action === 'delete') return undefined;   // permanent: the row is dead
                if (op.action !== 'insert') continue;

                if (winnerHash === undefined || hash > winnerHash) {
                    winner = op;
                    winnerHash = hash;
                }
            }
        }

        if (winner === undefined) return undefined;

        // a valid (non-voided) delete barrier concurrent to `at`, visible from
        // `from`, kills the row even though it is not in the row's history,
        // honored at-use per the concurrentDeletes flag at the delete's
        // position (see killedByConcurrentDelete).
        if (await this.killedByConcurrentDelete(rowId, table, true)) return undefined;

        return winner;
    }

    // The LWW-resolved value for one column of a row, or undefined if no
    // valid write at or below `at` carries it for the column's live
    // incarnation at this horizon. The cover of the incarnation-scoped write
    // meta (`cols: ['<rowId>:<incarnationId>:<column>']`) is the causal maxima set;
    // voided cover entries are descended through (and candidates surfaced
    // below them are pruned if dominated by another candidate). Concurrent
    // maxima tiebreak by larger entry hash. Does NOT check liveness.
    private async resolveColumn(rowId: B64Hash, column: string): Promise<json.Literal | undefined> {
        const schemaView = await this.schemaView();
        const table = this.target.getTableName();
        const incarnation = schemaView.getColumnIncarnation(table, column);
        if (incarnation === undefined) return undefined;

        const dag = await this.target.getScopedDag();
        const filter: EntryMetaFilter = { containsValues: { cols: [colTag(rowId, incarnation, column)] } };

        // cover among NON-VOIDED entries: descend through voided elements
        const candidates: B64Hash[] = [];
        const seen = new Set<B64Hash>();
        const queue: B64Hash[] = [...(await dag.findCoverWithFilter(this.at, filter))];
        let descended = false;

        while (queue.length > 0) {
            const hash = queue.shift()!;
            if (seen.has(hash)) continue;
            seen.add(hash);

            if (await this.entryVoided(hash)) {
                descended = true;
                const entry = await dag.loadEntry(hash);
                if (entry === undefined) continue;
                const preds = position(...json.fromSet(entry.header.prevEntryHashes));
                queue.push(...(await dag.findCoverWithFilter(preds, filter)));
            } else {
                candidates.push(hash);
            }
        }

        // candidates reached below a voided entry may be dominated by
        // another candidate (the plain cover never is)
        let maxima = candidates;
        if (descended && candidates.length > 1) {
            maxima = [];
            const causalDag = await this.target.getCausalDag();
            for (const hash of candidates) {
                const others = candidates.filter((o) => o !== hash);
                const fork = await causalDag.findForkPosition(version(hash), version(...others));
                if (fork.forkA.size > 0) maxima.push(hash);   // not below the others
            }
        }

        let winnerHash: B64Hash | undefined;
        let winnerValue: json.Literal | undefined;

        for (const hash of maxima) {
            const entry = await dag.loadEntry(hash);
            if (entry === undefined) continue;

            for (const op of opsFor(entry.payload, rowId)) {
                if (op.action !== 'insert' && op.action !== 'update') continue;
                const value = op.values[column];
                if (value === undefined) continue;

                if (winnerHash === undefined || hash > winnerHash) {
                    winnerHash = hash;
                    winnerValue = value;
                }
            }
        }

        return winnerValue;
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
            const written = await this.resolveColumn(rowId, column);
            if (written !== undefined) {
                values[column] = written;
            } else if (def!.columns[column].default !== undefined) {
                values[column] = def!.columns[column].default!;
            }
        }

        return { insert, values };
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
    // resolveColumn is incarnation-scoped at this horizon, so a column dropped
    // or re-added between horizons yields no written value for the old
    // incarnation (hence diffs correctly against defaults / new writes) and a
    // column's default never appears as a per-row write. Callers pass the
    // union of both horizons' columns.
    async deltaRowState(rowId: B64Hash, columns: string[]): Promise<DeltaRowState> {
        const insert = await this.liveInsert(rowId);
        if (insert === undefined) return { live: false, author: undefined, written: {} };

        const written: RowValues = {};
        for (const column of columns) {
            const value = await this.resolveColumn(rowId, column);
            if (value !== undefined) written[column] = value;
        }
        return { live: true, author: insert.author, written };
    }

    // Every live rowId at this horizon: enumerate the table's row entries,
    // collect candidate insert rowIds, then re-check enforced liveness. No
    // pub/author index is available, so this is a full table scan (used by the
    // one-time add-fk deploy prerequisite).
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

        const dag = await this.target.getScopedDag();
        const candidateRowIds = new Set<B64Hash>();

        {
            // Indexed candidates via pub meta, or rowAuthor system meta. Pub
            // values are mutable, so a multi-field filter would miss rows
            // whose current values come from different entries: ONE field
            // drives the index query, and resolved values are re-checked.
            const indexField = fields.includes('rowAuthor') ? 'rowAuthor' : fields[0];
            if (indexField === 'rowAuthor' && typeof where[indexField] !== 'string') {
                throw new Error("'rowAuthor' search requires a key-id string");
            }
            const indexValue = indexField === 'rowAuthor'
                ? where[indexField] as string
                : json.toStringNormalized(where[indexField]);
            const filter: EntryMetaFilter = { containsValues: {
                // surfaced `rowAuthor` indexes the internal meta key `author`
                [indexField === 'rowAuthor' ? 'author' : 'pub-' + indexField]: [indexValue],
            } };

            const candidates = await findAllWithFilter(dag, this.at, filter);

            for (const hash of candidates) {
                const entry = await dag.loadEntry(hash);
                if (entry === undefined) continue;

                const p = entry.payload as json.LiteralMap;
                const ops: RowOpPayload[] = p['action'] === 'rows'
                    ? (p as RowsSlicePayload).ops
                    : [p as RowOpPayload];

                for (const op of ops) {
                    if (op.action !== 'insert' && op.action !== 'update') continue;
                    const carried = indexField === 'rowAuthor' && op.action === 'insert'
                        ? op.author
                        : op.values[indexField];
                    if (carried !== undefined &&
                        json.toStringNormalized(carried) === json.toStringNormalized(where[indexField])) {
                        candidateRowIds.add(op.rowId);
                    }
                }
            }
        }

        // re-check: liveness (enforced), author, then per-field resolved
        // values (only the searched fields are resolved)
        const def = schemaView.getTable(table);
        const rowIds = new Set<B64Hash>();

        for (const rowId of candidateRowIds) {
            const insert = await this.liveInsert(rowId);
            if (insert === undefined) continue;

            let matchesAll = true;
            for (const field of fields) {
                const value = field === 'rowAuthor'
                    ? insert.author
                    : (await this.resolveColumn(rowId, field)) ?? def?.columns[field]?.default;
                if (value === undefined ||
                    json.toStringNormalized(value) !== json.toStringNormalized(where[field])) {
                    matchesAll = false;
                    break;
                }
            }
            if (matchesAll) rowIds.add(rowId);
        }

        return [...rowIds].sort();
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

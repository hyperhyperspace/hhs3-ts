// Per-slot LWW resolution of an RSchema's effective state at a version.
//
// The RSchema DAG has no barriers, so the effective schema is a pure function
// of the position `at`: collect the create + schema-update entries at or
// below `at`, decompose them into slot writes, and resolve each slot by LWW
// (the causally-maximal write wins; concurrent maxima tiebreak by entry hash;
// within one entry, the later migration rule wins; drops are tombstones).
//
// Slots:
//   - (table) existence          written by add-table / drop-table (tombstone)
//   - (table) concurrentDeletes  written by add-table (base) / set-concurrent-deletes
//   - (table) fks                written by add-table (base) / set-fks
//   - (table) restrictions       written by add-table (base) / set-restrictions
//   - (table, column) def        written by add-table (base) / add-column /
//                                drop-column (tombstone)
//
// Incarnation masking: the winning add-table write is a RESET of the whole
// table namespace. Subordinate writes that are causally at-or-below the
// winning add-table entry (other than the base writes it carries itself)
// belong to a previous incarnation and are masked; base writes carried by
// LOSING concurrent add-table entries are masked as well (a table def never
// merges fields from two independent creations). Subordinate rule writes
// (add/drop-column, set-*) that are concurrent with or after the winning
// add-table participate normally.
//
// Schema DAGs are small: the resolution loads every entry, computes ancestor
// sets directly, and does no meta indexing. Results are immutable per `at`
// and cached by RSchemaImpl.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Entry, Position } from "@hyper-hyper-space/hhs3_dag";

import {
    TableDef, ColumnDef, FKs, Restriction,
    MigrationRule,
} from "./payload.js";
import { CreateRSchemaPayload, SchemaUpdatePayload, SchemaCreator } from "./payload.js";

// The resolved, effective schema at a position.

export type SchemaState = {
    seed: string;
    name?: string;
    creators: SchemaCreator[];
    hashAlgorithm?: string;
    tables: Map<string, TableDef>;
};

// A single slot write, tagged with its origin for LWW resolution.

type SlotValue =
    | { kind: 'table'; def: TableDef }                     // add-table (existence + base)
    | { kind: 'column'; def: ColumnDef }
    | { kind: 'concurrent-deletes'; value: boolean | undefined }
    | { kind: 'fks'; fks: FKs | undefined }
    | { kind: 'restrictions'; restrictions: Restriction[] | undefined }
    | { kind: 'tombstone' };

type SlotWrite = {
    entryHash: B64Hash;
    entryIdx: number;      // topological index of the entry
    ruleIndex: number;     // position within the entry's migration rules
    value: SlotValue;
};

// Slot keys. Table and column names cannot contain '/' or ':' (isValidName),
// so these are collision-free.

function existenceSlot(table: string): string { return `t:${table}`; }
function modeSlot(table: string): string { return `m:${table}`; }
function fksSlot(table: string): string { return `f:${table}`; }
function restrictionsSlot(table: string): string { return `r:${table}`; }
function columnSlot(table: string, column: string): string { return `c:${table}:${column}`; }

// Decompose one migration rule into its slot writes. Exported for the
// validation and delta code, which need to know which slots a rule touches.

export function slotsTouchedByRule(rule: MigrationRule): string[] {
    switch (rule.rule) {
        case 'add-table': {
            const slots = [existenceSlot(rule.def.name), modeSlot(rule.def.name),
                fksSlot(rule.def.name), restrictionsSlot(rule.def.name)];
            for (const column of Object.keys(rule.def.columns)) {
                slots.push(columnSlot(rule.def.name, column));
            }
            return slots;
        }
        case 'drop-table':
            return [existenceSlot(rule.table)];
        case 'add-column':
        case 'drop-column':
            return [columnSlot(rule.table, rule.column)];
        case 'set-concurrent-deletes':
            return [modeSlot(rule.table)];
        case 'set-fks':
            return [fksSlot(rule.table)];
        case 'set-restrictions':
            return [restrictionsSlot(rule.table)];
    }
}

type WritesBySlot = Map<string, SlotWrite[]>;

function pushWrite(writes: WritesBySlot, slot: string, write: SlotWrite): void {
    let list = writes.get(slot);
    if (list === undefined) {
        list = [];
        writes.set(slot, list);
    }
    list.push(write);
}

// add-table writes the existence slot plus base writes for every subordinate
// slot of the table (the reset). The base writes share the rule's position.
function pushTableWrites(writes: WritesBySlot, def: TableDef, origin: { entryHash: B64Hash; entryIdx: number; ruleIndex: number }): void {
    pushWrite(writes, existenceSlot(def.name), { ...origin, value: { kind: 'table', def } });
    pushWrite(writes, modeSlot(def.name), { ...origin, value: { kind: 'concurrent-deletes', value: def.concurrentDeletes } });
    pushWrite(writes, fksSlot(def.name), { ...origin, value: { kind: 'fks', fks: def.fks } });
    pushWrite(writes, restrictionsSlot(def.name), { ...origin, value: { kind: 'restrictions', restrictions: def.restrictions } });
    for (const [column, columnDef] of Object.entries(def.columns)) {
        pushWrite(writes, columnSlot(def.name, column), { ...origin, value: { kind: 'column', def: columnDef } });
    }
}

function pushRuleWrites(writes: WritesBySlot, rule: MigrationRule, origin: { entryHash: B64Hash; entryIdx: number; ruleIndex: number }): void {
    switch (rule.rule) {
        case 'add-table':
            pushTableWrites(writes, rule.def, origin);
            break;
        case 'drop-table':
            pushWrite(writes, existenceSlot(rule.table), { ...origin, value: { kind: 'tombstone' } });
            break;
        case 'add-column':
            pushWrite(writes, columnSlot(rule.table, rule.column), { ...origin, value: { kind: 'column', def: rule.def } });
            break;
        case 'drop-column':
            pushWrite(writes, columnSlot(rule.table, rule.column), { ...origin, value: { kind: 'tombstone' } });
            break;
        case 'set-concurrent-deletes':
            pushWrite(writes, modeSlot(rule.table), { ...origin, value: { kind: 'concurrent-deletes', value: rule.value } });
            break;
        case 'set-fks':
            pushWrite(writes, fksSlot(rule.table), { ...origin, value: { kind: 'fks', fks: rule.fks } });
            break;
        case 'set-restrictions':
            pushWrite(writes, restrictionsSlot(rule.table), { ...origin, value: { kind: 'restrictions', restrictions: rule.restrictions } });
            break;
    }
}

// Causal structure over the included entries: topo index + ancestor sets.

type Causality = {
    indexOf: Map<B64Hash, number>;
    ancestors: Set<number>[];      // ancestors[i] = topo indices strictly below entry i
};

// strictly-before: a < b in the causal order
function before(c: Causality, a: SlotWrite, b: SlotWrite): boolean {
    return a.entryHash !== b.entryHash && c.ancestors[b.entryIdx].has(a.entryIdx);
}

// LWW: causal maxima first; among concurrent maxima the larger entry hash
// wins; within one entry the later rule wins.
function resolveSlot(c: Causality, writes: SlotWrite[]): SlotWrite | undefined {
    if (writes.length === 0) return undefined;

    // collapse per entry: the later rule in the same entry wins
    const perEntry = new Map<B64Hash, SlotWrite>();
    for (const write of writes) {
        const prev = perEntry.get(write.entryHash);
        if (prev === undefined || write.ruleIndex > prev.ruleIndex) {
            perEntry.set(write.entryHash, write);
        }
    }

    const candidates = [...perEntry.values()];
    const maxima = candidates.filter((w) => !candidates.some((other) => before(c, w, other)));

    let winner = maxima[0];
    for (const w of maxima) {
        if (w.entryHash > winner.entryHash) winner = w;
    }
    return winner;
}

// Collect the entries at or below `at` (by hash ancestry) from the full,
// topologically ordered entry list, and compute their causal structure.

function includeAt(entries: Entry[], at: Position): { included: Entry[]; causality: Causality } {
    const byHash = new Map<B64Hash, Entry>();
    for (const entry of entries) byHash.set(entry.hash, entry);

    const includedHashes = new Set<B64Hash>();
    const pending = [...at];
    while (pending.length > 0) {
        const hash = pending.pop()!;
        if (includedHashes.has(hash)) continue;
        const entry = byHash.get(hash);
        if (entry === undefined) throw new Error(`resolveSchemaState: entry '${hash}' not found`);
        includedHashes.add(hash);
        for (const prev of json.fromSet(entry.header.prevEntryHashes)) {
            pending.push(prev);
        }
    }

    // entries arrive in topo order; preserve it
    const included = entries.filter((e) => includedHashes.has(e.hash));

    const indexOf = new Map<B64Hash, number>();
    included.forEach((e, i) => indexOf.set(e.hash, i));

    const ancestors: Set<number>[] = [];
    for (const entry of included) {
        const set = new Set<number>();
        for (const prev of json.fromSet(entry.header.prevEntryHashes)) {
            const prevIdx = indexOf.get(prev);
            if (prevIdx === undefined) throw new Error(`resolveSchemaState: topological order violated at '${entry.hash}'`);
            set.add(prevIdx);
            for (const a of ancestors[prevIdx]) set.add(a);
        }
        ancestors.push(set);
    }

    return { included, causality: { indexOf, ancestors } };
}

// Resolve the effective schema at `at`. `entries` must be the full entry list
// of the schema's (scoped) DAG in topological order; exactly one of them must
// be the create entry.

export function resolveSchemaState(entries: Entry[], at: Position): SchemaState {
    const { included, causality } = includeAt(entries, at);

    let create: CreateRSchemaPayload | undefined;
    const writes: WritesBySlot = new Map();

    for (const entry of included) {
        const payload = entry.payload as json.LiteralMap;
        const entryIdx = causality.indexOf.get(entry.hash)!;

        if (payload['action'] === 'create') {
            if (create !== undefined) throw new Error("resolveSchemaState: multiple create entries");
            create = payload as CreateRSchemaPayload;
            create.tables.forEach((def, i) => {
                pushTableWrites(writes, def, { entryHash: entry.hash, entryIdx, ruleIndex: i });
            });
        } else if (payload['action'] === 'schema-update') {
            const update = payload as SchemaUpdatePayload;
            update.migration.forEach((rule, i) => {
                pushRuleWrites(writes, rule, { entryHash: entry.hash, entryIdx, ruleIndex: i });
            });
        } else {
            throw new Error(`resolveSchemaState: unknown action '${payload['action']}'`);
        }
    }

    if (create === undefined) throw new Error("resolveSchemaState: create entry not at or below the requested position");

    // Resolve existence slots first: the winning add-table is the table's
    // current incarnation and masks subordinate writes at-or-below it.
    const tables = new Map<string, TableDef>();

    for (const [slot, slotWrites] of writes) {
        if (!slot.startsWith('t:')) continue;
        const table = slot.slice(2);

        const winner = resolveSlot(causality, slotWrites);
        if (winner === undefined || winner.value.kind !== 'table') continue;   // tombstone or absent

        const incarnation = winner;
        const base = incarnation.value as Extract<SlotValue, { kind: 'table' }>;

        // a subordinate write survives the reset iff it is NOT causally
        // at-or-below the incarnation write (base writes from the incarnation
        // entry itself are included by their rule index)
        const survives = (w: SlotWrite): boolean => {
            if (w.entryHash === incarnation.entryHash) return w.ruleIndex >= incarnation.ruleIndex;
            return !causality.ancestors[incarnation.entryIdx].has(w.entryIdx);
        };

        // base writes from LOSING add-table entries must not bleed through:
        // they are 'table'-kind only for the existence slot, but their
        // subordinate base writes share the losing entry hash + rule index.
        // Identify losing add-table origins to mask them below.
        const losingOrigins = new Set<string>();
        for (const w of slotWrites) {
            if (w.value.kind === 'table' && w.entryHash !== incarnation.entryHash) {
                losingOrigins.add(`${w.entryHash}#${w.ruleIndex}`);
            }
        }
        const fromLosingAddTable = (w: SlotWrite): boolean =>
            losingOrigins.has(`${w.entryHash}#${w.ruleIndex}`);

        const resolveSubordinate = (subSlot: string): SlotWrite | undefined => {
            const subWrites = (writes.get(subSlot) ?? []).filter((w) => survives(w) && !fromLosingAddTable(w));
            return resolveSlot(causality, subWrites);
        };

        const columns: { [column: string]: ColumnDef } = {};
        for (const subSlot of writes.keys()) {
            if (!subSlot.startsWith(`c:${table}:`)) continue;
            const column = subSlot.slice(`c:${table}:`.length);
            const columnWinner = resolveSubordinate(subSlot);
            if (columnWinner !== undefined && columnWinner.value.kind === 'column') {
                columns[column] = columnWinner.value.def;
            }
        }

        const def: TableDef = { name: base.def.name, columns };

        // idProvider is structural: written only by the incarnation add-table,
        // never by a set-* rule, so it rides on the winning base def.
        if (base.def.idProvider !== undefined) def.idProvider = base.def.idProvider;

        const modeWinner = resolveSubordinate(modeSlot(table));
        if (modeWinner !== undefined && modeWinner.value.kind === 'concurrent-deletes' && modeWinner.value.value !== undefined) {
            def.concurrentDeletes = modeWinner.value.value;
        }

        const fksWinner = resolveSubordinate(fksSlot(table));
        if (fksWinner !== undefined && fksWinner.value.kind === 'fks' && fksWinner.value.fks !== undefined) {
            def.fks = fksWinner.value.fks;
        }

        const restrictionsWinner = resolveSubordinate(restrictionsSlot(table));
        if (restrictionsWinner !== undefined && restrictionsWinner.value.kind === 'restrictions' && restrictionsWinner.value.restrictions !== undefined) {
            def.restrictions = restrictionsWinner.value.restrictions;
        }

        tables.set(table, def);
    }

    const state: SchemaState = {
        seed: create.seed,
        creators: create.creators,
        tables,
    };
    if (create.name !== undefined) state.name = create.name;
    if (create.hashAlgorithm !== undefined) state.hashAlgorithm = create.hashAlgorithm;

    return state;
}

// A stable cache key for a position (order-insensitive).

export function positionKey(at: Position): string {
    return [...at].sort().join('|');
}

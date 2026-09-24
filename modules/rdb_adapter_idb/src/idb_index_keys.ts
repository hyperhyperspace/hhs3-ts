// Keys and ranges for the shadow index store. No encoding: an entry's `key` is
// exactly the key a native IDBIndex would compute from its key path, so every
// comparison is IndexedDB's own.
//
// Physical entry keys are [table, name, key, id]. `id` is always a number and
// `[]` (an array) sorts after every number, so a logical bound on `key` maps
// to an exact physical bound:
//   whole index        [t, n]            .. [t, n + '\0')
//   key k              [t, n, k]         .. [t, n, k, [])
//   lower L, closed    [t, n, L]
//   lower L, open      [t, n, L, []]
//   upper U, closed    [t, n, U, []]  (open)
//   upper U, open      [t, n, U]      (open)
// `name + '\0'` is the smallest string after `name`, and the logical key is one
// nested component, so compound keys and the `bound(['a'], ['a', []])` prefix
// idiom compare natively.

import type { json } from "@hyper-hyper-space/hhs3_json";
import type { ResolvedIndex } from "@hyper-hyper-space/hhs3_rdb_adapter";

import type { IndexEntryRecord, RowRecord } from "./idb_schema.js";

export const AFTER_ANY_ID: IDBValidKey = [];

// A json value that is a valid IndexedDB key: a number (not NaN), a string, or
// an array of valid keys. null, booleans and objects are not keys.
export function isValidJsonKey(v: unknown): v is IDBValidKey {
    if (typeof v === 'number') return !Number.isNaN(v);
    if (typeof v === 'string') return true;
    if (Array.isArray(v)) return v.every(isValidJsonKey);
    return false;
}

// The native key path of an index: one target column, or the column array.
export function indexKeyPath(index: Pick<ResolvedIndex, 'columns'>): string | string[] {
    const cols = index.columns.map((c) => c.target);
    return cols.length === 1 ? cols[0]! : cols;
}

// The key a native index would extract from `row`, or undefined when the row
// is not indexed (sparse: any component that is not a valid key).
export function indexKeyOf(
    row: { [column: string]: json.Literal | undefined } | undefined, index: Pick<ResolvedIndex, 'columns'>,
): IDBValidKey | undefined {
    if (row === undefined) return undefined;
    if (index.columns.length === 1) {
        const v = row[index.columns[0]!.target];
        return isValidJsonKey(v) ? v : undefined;
    }
    const parts: IDBValidKey[] = [];
    for (const c of index.columns) {
        const v = row[c.target];
        if (!isValidJsonKey(v)) return undefined;
        parts.push(v);
    }
    return parts;
}

export type IndexQuery = IDBValidKey | IDBKeyRange | null | undefined;

// Throws DataError (natively) when `key` is not a valid key.
export function validateKey(cmp: IDBFactory['cmp'], key: unknown): IDBValidKey {
    cmp(key, key);
    return key as IDBValidKey;
}

function asRange(cmp: IDBFactory['cmp'], query: IndexQuery): IDBKeyRange | undefined {
    if (query === undefined || query === null) return undefined;
    if (query instanceof IDBKeyRange) return query;
    return IDBKeyRange.only(validateKey(cmp, query));
}

// The physical range of index (table, name) for a logical query.
export function indexEntryRange(cmp: IDBFactory['cmp'], table: string, name: string, query: IndexQuery): IDBKeyRange {
    const range = asRange(cmp, query);
    const p: IDBValidKey[] = [table, name];
    const lower = range?.lower === undefined
        ? { key: p, open: false }
        : range.lowerOpen
            ? { key: [...p, range.lower, AFTER_ANY_ID], open: true }
            : { key: [...p, range.lower], open: false };
    const upper = range?.upper === undefined
        ? { key: [table, name + '\u0000'], open: true }
        : range.upperOpen
            ? { key: [...p, range.upper], open: true }
            : { key: [...p, range.upper, AFTER_ANY_ID], open: true };
    return IDBKeyRange.bound(lower.key, upper.key, lower.open, upper.open);
}

// Every entry of one logical key (all its primary keys).
export function entryKeyRange(table: string, name: string, key: IDBValidKey): IDBKeyRange {
    return IDBKeyRange.bound([table, name, key], [table, name, key, AFTER_ANY_ID], false, true);
}

export function wholeIndexRange(table: string, name: string): IDBKeyRange {
    return IDBKeyRange.bound([table, name], [table, name + '\u0000'], false, true);
}

// Every record whose key starts with `table` ([table, ...]).
export function wholeTableRange(table: string): IDBKeyRange {
    return IDBKeyRange.bound([table], [table + '\u0000'], false, true);
}

// The physical `rows` range ([table, id]) for a query on a virtual store's id.
export function rowRange(cmp: IDBFactory['cmp'], table: string, query: IndexQuery): IDBKeyRange {
    const range = asRange(cmp, query);
    const lower = range?.lower === undefined ? { key: [table], open: false } : { key: [table, range.lower], open: range.lowerOpen };
    const upper = range?.upper === undefined
        ? { key: [table, AFTER_ANY_ID], open: true }
        : { key: [table, range.upper], open: range.upperOpen };
    return IDBKeyRange.bound(lower.key, upper.key, lower.open, upper.open);
}

// Issue, on `entries`, the deletes of the old entries and the puts of the new
// ones for every index of a row's table. A row whose key is not valid has no
// entry (sparse, like a native index). Requests run in order, so an unchanged
// key is deleted and put back.
export function syncEntryRequests(
    entries: IDBObjectStore, indexes: ResolvedIndex[], id: number,
    oldRow: RowRecord | undefined, newRow: RowRecord | undefined,
): IDBRequest[] {
    const reqs: IDBRequest[] = [];
    for (const index of indexes) {
        const oldKey = indexKeyOf(oldRow, index);
        const newKey = indexKeyOf(newRow, index);
        if (oldKey !== undefined) reqs.push(entries.delete([index.table, index.name, oldKey, id]));
        if (newKey !== undefined) {
            reqs.push(entries.put({ table: index.table, name: index.name, key: newKey, id } satisfies IndexEntryRecord));
        }
    }
    return reqs;
}

export function deleteRange(tx: IDBTransaction, store: string, range: IDBKeyRange): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        const req = tx.objectStore(store).delete(range);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
    });
}

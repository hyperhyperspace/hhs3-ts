import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Entry } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import type { BoundLog } from "../bind/bind.js";
import type { LogLangResult, LogRow } from "./result.js";

export async function executeLog(bound: BoundLog): Promise<LogLangResult> {
    const dag = await bound.target.object.getScopedDag();
    const allEntries: Entry[] = [];
    for await (const entry of dag.loadAllEntries()) allEntries.push(entry);

    const visible = bound.at === undefined ? allEntries : filterAt(allEntries, bound.at);
    const offset = bound.ast.offset ?? 0;
    const limited = bound.ast.limit === undefined
        ? visible.slice(offset)
        : visible.slice(offset, offset + bound.ast.limit);
    const prefixes = uniquePrefixes(hashesForPrefixes(allEntries));

    return {
        kind: 'log',
        target: targetName(bound),
        rows: limited.map((entry) => toLogRow(entry, prefixes)),
    };
}

function filterAt(entries: Entry[], at: Version): Entry[] {
    const byHash = new Map(entries.map((e) => [e.hash, e]));
    const reachable = new Set<B64Hash>();
    const visit = (hash: B64Hash) => {
        if (reachable.has(hash)) return;
        const entry = byHash.get(hash);
        if (entry === undefined) return;
        reachable.add(hash);
        for (const prev of json.fromSet(entry.header.prevEntryHashes) as B64Hash[]) visit(prev);
    };
    for (const hash of at) visit(hash);
    return entries.filter((entry) => reachable.has(entry.hash));
}

function toLogRow(entry: Entry, prefixes: Map<B64Hash, string>): LogRow {
    const payload = entry.payload;
    const action = payloadAction(payload);
    const type = payloadType(payload);
    const row: LogRow = {
        hash: prefixes.get(entry.hash) ?? entry.hash,
        fullHash: entry.hash,
        prev: (json.fromSet(entry.header.prevEntryHashes) as B64Hash[]).map((h) => prefixes.get(h) ?? h),
        summary: summarizePayload(payload),
        payload,
    };
    if (action !== undefined) row.action = action;
    if (type !== undefined) row.type = type;
    return row;
}

function payloadAction(payload: json.Literal): string | undefined {
    return isObject(payload) && typeof payload['action'] === 'string' ? payload['action'] : undefined;
}

function payloadType(payload: json.Literal): string | undefined {
    return isObject(payload) && typeof payload['type'] === 'string' ? payload['type'] : undefined;
}

function summarizePayload(payload: json.Literal): string {
    if (!isObject(payload)) return typeof payload;
    const action = payloadAction(payload);
    if (action === 'create') {
        const name = typeof payload['name'] === 'string' ? ` ${payload['name']}` : '';
        return `create${name}`;
    }
    if (action === 'row' && typeof payload['table'] === 'string') {
        const op = isObject(payload['op']) && typeof payload['op']['action'] === 'string' ? payload['op']['action'] : 'row';
        return `${op} ${payload['table']}`;
    }
    if (action === 'bundle') return 'bundle';
    if (action === 'schema-update') return 'schema-update';
    if (action !== undefined) return action;
    return 'unknown';
}

function hashesForPrefixes(entries: Entry[]): B64Hash[] {
    const hashes = new Set<B64Hash>();
    for (const entry of entries) {
        hashes.add(entry.hash);
        for (const prev of json.fromSet(entry.header.prevEntryHashes) as B64Hash[]) hashes.add(prev);
    }
    return [...hashes];
}

function uniquePrefixes(hashes: B64Hash[]): Map<B64Hash, string> {
    const out = new Map<B64Hash, string>();
    for (const hash of hashes) {
        for (let len = 8; len <= hash.length; len += 1) {
            const prefix = hash.slice(0, len);
            if (hashes.filter((h) => h.startsWith(prefix)).length === 1) {
                out.set(hash, prefix);
                break;
            }
        }
        if (!out.has(hash)) out.set(hash, hash);
    }
    return out;
}

function targetName(bound: BoundLog): string {
    if (bound.target.kind === 'table') return `${bound.target.groupId}.${bound.target.tableName}`;
    return bound.target.id;
}

function isObject(value: json.Literal): value is json.LiteralMap {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

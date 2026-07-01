// Op channel for group delta: entry-level void verdict flips between start and end
// view horizons. Streams the same walk as the row channel; retains only flips.
// Each flip may carry a structured void reason at the voided horizon.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import {
    Version,
    isRefAdvancePayload,
    extractRefVersion,
    extractAuthor,
} from "@hyper-hyper-space/hhs3_mvt";

import type { RowOpPayload } from "../rtable/payload.js";
import type { RowEnvelopePayload, BundlePayload } from "./payload.js";
import type { OpVoidDetail, OpVoidHorizon } from "./op_void.js";

export type { OpVoidDetail, OpVoidHorizon } from "./op_void.js";
export { formatOpVoidDetail } from "./op_void.js";

export type OpVerdictHost = {
    isEntryVoided(entryHash: B64Hash, from: Version): Promise<boolean>;
    explainEntryVoided(entryHash: B64Hash, from: Version): Promise<OpVoidDetail | undefined>;
    getSchemaRef(): B64Hash;
    getBindings(): { [name: string]: B64Hash };
};

export type OpVerdictKind = 'insert' | 'update' | 'delete' | 'bundle' | 'observe' | 'schema-deploy';

export type OpVerdictWrite = {
    table: string;
    action: RowOpPayload['action'];
    rowId: B64Hash;
};

export type OpVerdictChange = {
    entry: B64Hash;
    kind: OpVerdictKind;
    voidBefore: boolean;
    voidAfter: boolean;
    voidHorizon?: OpVoidHorizon;
    reason?: OpVoidDetail;
    table?: string;
    rowId?: B64Hash;
    author?: KeyId;
    binding?: string;
    refVersion?: B64Hash[];
    writes?: OpVerdictWrite[];
};

export function isVoidCheckable(payload: json.Literal): boolean {
    if (isRefAdvancePayload(payload)) return true;
    const p = payload as json.LiteralMap;
    return p['action'] === 'row' || p['action'] === 'bundle';
}

function bindingNameFor(host: OpVerdictHost, refId: B64Hash): string | undefined {
    for (const [name, id] of Object.entries(host.getBindings())) {
        if (id === refId) return name;
    }
    return undefined;
}

export function describeGroupEntry(
    entry: dag.Entry,
    host: OpVerdictHost,
    voidBefore: boolean,
    voidAfter: boolean,
): OpVerdictChange {
    const payload = entry.payload as json.LiteralMap;
    const base: OpVerdictChange = {
        entry: entry.hash,
        kind: 'insert',
        voidBefore,
        voidAfter,
    };

    if (isRefAdvancePayload(payload)) {
        const refId = (payload as { refId: B64Hash }).refId;
        const refVersion = [...extractRefVersion(payload as Parameters<typeof extractRefVersion>[0])];
        const author = extractAuthor(payload as json.LiteralMap);
        if (refId === host.getSchemaRef()) {
            return {
                ...base,
                kind: 'schema-deploy',
                refVersion,
                ...(author !== undefined ? { author } : {}),
            };
        }
        const binding = bindingNameFor(host, refId);
        return {
            ...base,
            kind: 'observe',
            refVersion,
            ...(binding !== undefined ? { binding } : {}),
            ...(author !== undefined ? { author } : {}),
        };
    }

    if (payload['action'] === 'row') {
        const envelope = payload as RowEnvelopePayload;
        const op = envelope.op as RowOpPayload;
        return {
            ...base,
            kind: op.action,
            table: envelope.table,
            rowId: op.rowId,
            ...(op.author !== undefined ? { author: op.author } : {}),
        };
    }

    if (payload['action'] === 'bundle') {
        const bundle = payload as BundlePayload;
        const writes: OpVerdictWrite[] = bundle.writes.map((w) => {
            const op = w.op as RowOpPayload;
            return { table: w.table, action: op.action, rowId: op.rowId };
        });
        writes.sort((a, b) =>
            a.table.localeCompare(b.table)
            || a.rowId.localeCompare(b.rowId)
            || (a.action < b.action ? -1 : a.action > b.action ? 1 : 0));
        return { ...base, kind: 'bundle', writes };
    }

    return base;
}

export async function computeOpVerdictFlips(
    host: OpVerdictHost,
    entries: dag.Entry[],
    start: Version,
    end: Version,
): Promise<OpVerdictChange[]> {
    const changes: OpVerdictChange[] = [];

    for (const entry of entries) {
        if (!isVoidCheckable(entry.payload)) continue;

        const voidBefore = await host.isEntryVoided(entry.hash, start);
        const voidAfter = await host.isEntryVoided(entry.hash, end);
        if (voidBefore === voidAfter) continue;

        const voidHorizon: OpVoidHorizon = voidAfter ? 'end' : 'start';
        const horizon = voidAfter ? end : start;
        const reason = await host.explainEntryVoided(entry.hash, horizon);

        changes.push({
            ...describeGroupEntry(entry, host, voidBefore, voidAfter),
            voidHorizon,
            ...(reason !== undefined ? { reason } : {}),
        });
    }

    changes.sort((a, b) => a.entry.localeCompare(b.entry));
    return changes;
}

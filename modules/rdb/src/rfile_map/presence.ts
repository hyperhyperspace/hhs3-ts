// Element presence in an RFileMap, with RSet's barrier-delete rule: at
// View(at, from), element e is present iff the cover of e's ops at `at` holds
// an add and no remove, and no remove of e in past(from) is concurrent with
// `at`. A remove is concurrent with a multi-head `at` unless it is after every
// head. Concurrency is with the point being read, not with an add: a remove
// in the past of `at` that an add on its branch followed does not kill an add
// on another branch.
//
// For at == from nothing in past(from) is concurrent with `at`, so the cover
// decides.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Version } from "@hyper-hyper-space/hhs3_mvt";

import { sameVersion } from "../rfiles/walk.js";
import { META_ELEMENT, FileMapElementPayload, elementIdOf, elementOf } from "./payload.js";
import type { ListedFile } from "./interfaces.js";

const BARRIER = 'barrier';

export function elementMeta(payload: FileMapElementPayload): dag.MetaProps {
    const meta: dag.MetaProps = { [META_ELEMENT]: json.toSet([elementIdOf(payload)]) };
    if (payload.action === 'remove') meta[BARRIER] = json.toSet(['t']);
    return meta;
}

function opsFilter(id: B64Hash): dag.EntryMetaFilter {
    return { containsValues: { [META_ELEMENT]: [id] } };
}

function removesFilter(id: B64Hash): dag.EntryMetaFilter {
    return { containsValues: { [META_ELEMENT]: [id], [BARRIER]: ['t'] } };
}

export async function isPresent(rawDag: dag.Dag, id: B64Hash, at: Version, from: Version): Promise<boolean> {
    const cover = await rawDag.findCoverWithFilter(at, opsFilter(id));
    if (cover.size === 0) return false;
    for (const h of cover) {
        const entry = await rawDag.loadEntry(h);
        if ((entry?.payload as json.LiteralMap | undefined)?.['action'] !== 'add') return false;
    }
    if (sameVersion(at, from)) return true;
    return (await rawDag.findConcurrentCoverWithFilter(from, at, removesFilter(id))).size === 0;
}

// Every element present at View(at, from). Elements with no remove in past(at)
// are present at (at, at) without further checks.
export async function listPresent(rawDag: dag.Dag, at: Version, from: Version): Promise<ListedFile[]> {
    type Ops = { element: ListedFile; removed: boolean };
    const byId = new Map<B64Hash, Ops>();

    const visited = new Set<B64Hash>();
    const pending: B64Hash[] = [...at];
    while (pending.length > 0) {
        const hash = pending.pop()!;
        if (visited.has(hash)) continue;
        visited.add(hash);
        const entry = await rawDag.loadEntry(hash);
        if (entry === undefined) continue;
        const p = entry.payload as json.LiteralMap;
        if (p['action'] === 'add' || p['action'] === 'remove') {
            const op = entry.payload as unknown as FileMapElementPayload;
            const id = elementIdOf(op);
            let ops = byId.get(id);
            if (ops === undefined) {
                ops = { element: { ...elementOf(op), id }, removed: false };
                byId.set(id, ops);
            }
            if (op.action === 'remove') ops.removed = true;
        }
        for (const prev of json.fromSet(entry.header.prevEntryHashes)) {
            if (!visited.has(prev)) pending.push(prev);
        }
    }

    const same = sameVersion(at, from);
    const listed: ListedFile[] = [];
    for (const [id, ops] of byId) {
        if ((same && !ops.removed) || await isPresent(rawDag, id, at, from)) listed.push(ops.element);
    }
    return sortListed(listed);
}

export function sortListed(files: ListedFile[]): ListedFile[] {
    const key = (f: ListedFile) => `${f.section}\u0000${f.owner ?? ''}\u0000${f.path}\u0000${f.fileHash}`;
    return files.sort((a, b) => (key(a) < key(b) ? -1 : key(a) > key(b) ? 1 : 0));
}

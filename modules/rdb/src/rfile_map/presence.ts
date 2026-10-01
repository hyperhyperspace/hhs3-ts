// Element presence in an RFileMap, with RSet's barrier-remove rule: at
// View(at, from), element e is present iff some add of e in the cover of e's
// ops at `at` has no remove of e in past(from) concurrent to it.
//
// For at == from, removes in past(at) are either below that add or concurrent
// to it, so the check reduces to the cover itself (a remove in the cover kills
// every add in it) and, when there are several adds, to the maximal removes.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Version, version } from "@hyper-hyper-space/hhs3_mvt";

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

async function isBelow(rawDag: dag.Dag, lower: B64Hash, upper: B64Hash): Promise<boolean> {
    const fork = await rawDag.findForkPosition(version(lower), version(upper));
    return fork.forkA.size === 0;
}

export async function isPresent(rawDag: dag.Dag, id: B64Hash, at: Version, from: Version): Promise<boolean> {
    const cover = await rawDag.findCoverWithFilter(at, opsFilter(id));
    const adds: B64Hash[] = [];
    let removed = false;
    for (const h of cover) {
        const entry = await rawDag.loadEntry(h);
        if ((entry?.payload as json.LiteralMap | undefined)?.['action'] === 'add') adds.push(h);
        else removed = true;
    }
    if (adds.length === 0) return false;

    if (sameVersion(at, from)) {
        if (removed) return false;
        if (adds.length === 1) return true;
        const removes = await rawDag.findCoverWithFilter(at, removesFilter(id));
        for (const add of adds) {
            let alive = true;
            for (const r of removes) {
                if (!await isBelow(rawDag, r, add)) { alive = false; break; }
            }
            if (alive) return true;
        }
        return false;
    }

    for (const add of adds) {
        const concurrent = await rawDag.findConcurrentCoverWithFilter(from, version(add), removesFilter(id));
        if (concurrent.size === 0) return true;
    }
    return false;
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

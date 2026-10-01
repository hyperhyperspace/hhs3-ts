// RFileMap deltas: the elements whose presence differs between View(start,
// start) and View(end, end).
//
// Presence depends only on the ops of an element in the causal past, so only
// elements with an op in one past and not the other can change. Those are
// found by walking both differences (each entry loaded once), then presence is
// computed at both ends.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { DeltaAccumulator, DeltaChanges, Version } from "@hyper-hyper-space/hhs3_mvt";

import { walkNewEntries } from "../rfiles/walk.js";
import type { FileMapChanges, FileMapDelta, ListedFile } from "./interfaces.js";
import { RFILE_MAP_TYPE_ID, FileMapElementPayload, elementIdOf, elementOf } from "./payload.js";
import { isPresent, sortListed } from "./presence.js";

export class FileMapDeltaAccumulator implements DeltaAccumulator<FileMapChanges> {

    private readonly touched = new Map<B64Hash, ListedFile>();

    constructor(private readonly getDag: () => Promise<dag.Dag>, private readonly start: Version, private readonly end: Version) {}

    async ingest(entry: dag.Entry): Promise<boolean> {
        const p = entry.payload as json.LiteralMap;
        if (p['action'] !== 'add' && p['action'] !== 'remove') return false;
        const op = entry.payload as unknown as FileMapElementPayload;
        const id = elementIdOf(op);
        if (!this.touched.has(id)) this.touched.set(id, { ...elementOf(op), id });
        return true;
    }

    async finalize(): Promise<DeltaChanges<FileMapChanges>> {
        const added: ListedFile[] = [];
        const removed: ListedFile[] = [];
        const rawDag = await this.getDag();
        for (const [id, element] of this.touched) {
            const before = await isPresent(rawDag, id, this.start, this.start);
            const after = await isPresent(rawDag, id, this.end, this.end);
            if (after && !before) added.push(element);
            else if (before && !after) removed.push(element);
        }
        return {
            type: RFILE_MAP_TYPE_ID,
            changes: { added: sortListed(added), removed: sortListed(removed) },
            nested: new Map(),
        };
    }
}

export async function computeFileMapDelta(
    rawDag: dag.Dag, start: Version, end: Version, accumulator: FileMapDeltaAccumulator,
): Promise<FileMapDelta> {
    const ingest = async (entry: dag.Entry) => { await accumulator.ingest(entry); };
    const revisionBound = await walkNewEntries(rawDag, start, end, ingest);
    await walkNewEntries(rawDag, end, start, ingest);
    const changes = await accumulator.finalize();
    return { ...changes, start, end, revisionBound };
}

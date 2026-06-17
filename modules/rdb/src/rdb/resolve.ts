// Resolve RDb deployment membership at a position.
//
// The RDb DAG is add-only and has no barriers: membership is the union of
// every add-schema / add-group op at or below `at`, keyed purely by
// schema / group id. Order of addition is irrelevant to the set, and there is
// no removal in v1, so the resolution is a plain confluent union. Optional
// `note`s are NOT collected: they are free-form comments, never resolved and
// never keys.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Entry, Position } from "@hyper-hyper-space/hhs3_dag";

import { AddSchemaPayload, AddGroupPayload } from "./payload.js";

export type RDbMembers = {
    schemaIds: B64Hash[];
    groupIds: B64Hash[];
};

// `entries` must be the full entry list of the RDb's (scoped) DAG; exactly one
// of them must be the create entry. Member arrays preserve first-seen
// topological order (entries arrive in topo order) and are de-duplicated.
export function resolveMembers(entries: Entry[], at: Position): RDbMembers {
    const byHash = new Map<B64Hash, Entry>();
    for (const entry of entries) byHash.set(entry.hash, entry);

    const includedHashes = new Set<B64Hash>();
    const pending = [...at];
    while (pending.length > 0) {
        const hash = pending.pop()!;
        if (includedHashes.has(hash)) continue;
        const entry = byHash.get(hash);
        if (entry === undefined) throw new Error(`resolveMembers: entry '${hash}' not found`);
        includedHashes.add(hash);
        for (const prev of json.fromSet(entry.header.prevEntryHashes)) {
            pending.push(prev);
        }
    }

    const schemaIds: B64Hash[] = [];
    const groupIds: B64Hash[] = [];
    const seenSchemas = new Set<B64Hash>();
    const seenGroups = new Set<B64Hash>();

    let sawCreate = false;
    for (const entry of entries) {
        if (!includedHashes.has(entry.hash)) continue;
        const action = (entry.payload as json.LiteralMap)['action'];

        if (action === 'create') {
            sawCreate = true;
        } else if (action === 'add-schema') {
            const id = (entry.payload as AddSchemaPayload).schemaId;
            if (!seenSchemas.has(id)) { seenSchemas.add(id); schemaIds.push(id); }
        } else if (action === 'add-group') {
            const id = (entry.payload as AddGroupPayload).groupId;
            if (!seenGroups.has(id)) { seenGroups.add(id); groupIds.push(id); }
        } else {
            throw new Error(`resolveMembers: unknown action '${action}'`);
        }
    }

    if (!sawCreate) throw new Error("resolveMembers: create entry not at or below the requested position");

    return { schemaIds, groupIds };
}

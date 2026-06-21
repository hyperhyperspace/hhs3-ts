import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { extractCreatePayloadType, Payload, RObject } from "@hyper-hyper-space/hhs3_mvt";
import type { Replica, DagBackend, DagEntry } from "@hyper-hyper-space/hhs3_replica";

import { kindFromType, RootIndex } from "./root_index.js";

export type RehydratedRoot = {
    id: B64Hash;
    type: string;
    payload: Payload;
    object?: RObject;
};

export async function readRootPayloads(backend: DagBackend): Promise<RehydratedRoot[]> {
    const dagEntries = await backend.listDags();
    const roots: RehydratedRoot[] = [];

    for (const entry of dagEntries) {
        const dag = await backend.openDag(entry.id);
        if (dag === undefined) continue;
        const createEntry = await dag.loadEntry(entry.id);
        if (createEntry === undefined) continue;
        const type = extractCreatePayloadType(createEntry.payload);
        if (type === undefined) continue;
        roots.push({ id: entry.id, type, payload: createEntry.payload });
    }

    return roots;
}

export async function rehydrateRoots(replica: Replica, backend: DagBackend, index: RootIndex): Promise<RehydratedRoot[]> {
    const roots = await readRootPayloads(backend);
    for (const root of roots) {
        index.upsert({
            id: root.id,
            type: root.type,
            kind: kindFromType(root.type),
            name: payloadName(root.payload),
        });
    }

    const loaded: RehydratedRoot[] = [];
    const loadable = [...roots].sort((a, b) => priority(a.type) - priority(b.type));
    const pending = new Set(loadable);
    let progressed = true;

    while (pending.size > 0 && progressed) {
        progressed = false;
        for (const root of [...pending]) {
            try {
                const object = await replica.createObject(root.payload);
                root.object = object;
                index.registerObject(root.id, object, payloadName(root.payload));
                loaded.push(root);
                pending.delete(root);
                progressed = true;
            } catch (e) {
                if (!isDependencyError(e)) throw e;
            }
        }
    }

    if (pending.size > 0) {
        const ids = [...pending].map((root) => `${root.id} (${root.type})`).join(', ');
        throw new Error(`Could not rehydrate roots with unresolved dependencies: ${ids}`);
    }

    return loaded;
}

export function payloadName(payload: Payload): string | undefined {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return undefined;
    const name = (payload as json.LiteralMap)['name'];
    return typeof name === 'string' ? name : undefined;
}

function priority(type: string): number {
    if (type === 'hhs/rschema_v1') return 0;
    if (type === 'hhs/rtable_group_v1') return 1;
    if (type === 'hhs/rdb_v1') return 2;
    return 3;
}

function isDependencyError(e: unknown): boolean {
    const message = e instanceof Error ? e.message : String(e);
    return message.includes('is not present in the replica');
}

export function dagEntryFromRoot(root: RehydratedRoot): DagEntry {
    return { id: root.id, type: root.type, createdAt: 0 };
}

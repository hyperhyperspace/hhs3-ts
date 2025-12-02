import { Hash, sha } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Entry, EntryMetaFilter, ForkPosition, Header, MetaProps, Position } from "./dag_defs";
import { DagIndex } from "./idx/dag_idx";
import { DagStore } from "./store/dag_store";

export * from "./dag_defs";
export * as store from "./store";
export * as idx from "./idx";

// DAG definition

export type Dag = {
    append(payload: json.Literal, meta: MetaProps, after?: Position): Promise<Hash>;
    
    loadEntry(h: Hash): Promise<Entry|undefined>;
    loadHeader(h: Hash): Promise<Header|undefined>;

    getFrontier(): Promise<Position>;

    // Latest position where history hasn't forked yet
    findForkPosition(first: Position, second: Position): Promise<ForkPosition>;
    findMinimalCover(p: Position): Promise<Position>;

    // The following two are used for finding entries with specific properties

    // This one is for reading a value at a specific version, by finding the last changes on that value
    findCoverWithFilter(from: Position, meta: EntryMetaFilter): Promise<Position>;

    // This is useful for finding barrier ops that should be applied to concurrent changes
    findConcurrentCoverWithFilter(from: Position, concurrentTo: Position, meta: EntryMetaFilter): Promise<Position>;
    
    loadAllEntries(): AsyncIterable<Entry>; // in topo order

    getStore(): DagStore;
    getIndex(): DagIndex;
};

export async function createHeader(payload: json.Literal, after?: Position): Promise<Header> {
    const payloadHash: Hash = await sha.sha256(json.toStringNormalized(payload));
    const prevEntryHashes = json.toSet(after ?? new Set<Hash>());
    const header: Header = { payloadHash, prevEntryHashes};
    return header;
}

export async function createEntry(payload: json.Literal, meta: MetaProps, after?: Position): Promise<Entry> {
    const header = await createHeader(payload, after);
    const hash: Hash = await sha.sha256(json.toStringNormalized(header));
    const entry: Entry = { hash, header, payload, meta };

    return entry;
}

export function position(...hashes: Hash[]): Position {
    return new Set(hashes);
}

export async function computeEntryHash(payload: json.Literal, after?: Position): Promise<Hash> {
    const entry = await createEntry(payload, {}, after);
    return entry.hash;
}

export function create(
                    store: DagStore,
                    index: DagIndex,
                ): Dag {

    return {
        append: async (payload, meta, after) => {
        
            const e = await createEntry(payload, meta, after);

            for (const prev of after||[]) {
                if (await store.loadHeader(prev) === undefined) {
                    throw new Error('cannot add ' + e.hash + ' before ' + prev);
                }
            }

            await index.index(e.hash, after);
            await store.append(e);
            return e.hash;
        },

        loadEntry: (h) => store.loadEntry(h),
        loadHeader: (h) => store.loadHeader(h),

        getFrontier: () => store.getFrontier(),

        findMinimalCover: async(p) => index.findMinimalCover(p),
        findForkPosition: async (first, second) => index.findForkPosition(first, second),

        findCoverWithFilter: async (from, filter) => index.findCoverWithFilter(from, filter),
        findConcurrentCoverWithFilter: async (from, concurrentTo, filter) => index.findConcurrentCoverWithFilter(from, concurrentTo, filter),

        loadAllEntries: () => store.loadAllEntries(),

        getStore: () => store,
        getIndex: () => index
  };
}

export async function copy(from: Dag, to:Dag): Promise<void> {    
    for await (const e of from.loadAllEntries()) {
        await to.append(e.payload, e.meta, new Set(json.fromSet(e.header.prevEntryHashes)));
    }
}
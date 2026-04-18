import { B64Hash, HashSuite, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Entry, EntryMetaFilter, ForkPosition, Header, MetaProps, Position } from "./dag_defs.js";
import { DagIndex } from "./idx/dag_idx.js";
import { DagGrowthListener, DagStore } from "./store/dag_store.js";

export { HashSuite } from "@hyper-hyper-space/hhs3_crypto";
export * from "./dag_defs.js";
export * as store from "./store/index.js";
export * as idx from "./idx/index.js";

// DAG definition

export type Dag = {
    append(payload: json.Literal, meta: MetaProps, after?: Position): Promise<B64Hash>;
    
    // like append, but just computes the hash (useful for pre-flights, etc.)
    computeEntryHash(payload: json.Literal, after?: Position): Promise<B64Hash>;
    
    loadEntry(h: B64Hash): Promise<Entry|undefined>;
    loadHeader(h: B64Hash): Promise<Header|undefined>;

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

    // Growth events. See DagStore.addListener for the contract (at-least-once,
    // no payload -- consumers should re-read getFrontier()).
    addListener(listener: DagGrowthListener): void;
    removeListener(listener: DagGrowthListener): void;

    getStore(): DagStore<any>;
    getIndex(): DagIndex<any>;
};

export function createHeader(payload: json.Literal, after: Position | undefined, hash: HashSuite): Header {
    const payloadHash: B64Hash = hash.hashToB64(stringToUint8Array(json.toStringNormalized(payload)));
    const prevEntryHashes = json.toSet(after ?? new Set<B64Hash>());
    const header: Header = { payloadHash, prevEntryHashes};
    return header;
}

export function createEntry(payload: json.Literal, meta: MetaProps, after: Position | undefined, hash: HashSuite): Entry {
    const header = createHeader(payload, after, hash);
    const entryHash: B64Hash = hash.hashToB64(stringToUint8Array(json.toStringNormalized(header)));
    const entry: Entry = { hash: entryHash, header, payload, meta };

    return entry;
}

export function position(...hashes: B64Hash[]): Position {
    return new Set(hashes);
}

export function create<Tx = void>(
                    store: DagStore<Tx>,
                    index: DagIndex<Tx>,
                    hash: HashSuite,
                ): Dag {

    // Read-only view: avoids deferred-conditional-type errors when Tx is generic
    const ro = store as DagStore<any>;

    return {
        append: async (payload, meta, after) => {
        
            const e = createEntry(payload, meta, after, hash);

            await store.withTransaction(async (...tx) => {
                for (const prev of after||[]) {
                    if (await store.loadHeader(prev, ...tx) === undefined) {
                        throw new Error('cannot add ' + e.hash + ' before ' + prev);
                    }
                }
                await index.index(e.hash, after, ...tx);
                await store.append(e, ...tx);
                return { fireListeners: true };
            });
            return e.hash;
        },

        computeEntryHash: async (payload, after) => {
            const e = createEntry(payload, {}, after, hash);
            return e.hash;
        },

        loadEntry: (h) => ro.loadEntry(h),
        loadHeader: (h) => ro.loadHeader(h),

        getFrontier: () => ro.getFrontier(),

        findMinimalCover: async(p) => index.findMinimalCover(p),
        findForkPosition: async (first, second) => index.findForkPosition(first, second),

        findCoverWithFilter: async (from, filter) => index.findCoverWithFilter(from, filter),
        findConcurrentCoverWithFilter: async (from, concurrentTo, filter) => index.findConcurrentCoverWithFilter(from, concurrentTo, filter),

        loadAllEntries: () => ro.loadAllEntries(),

        addListener: (cb) => ro.addListener(cb),
        removeListener: (cb) => ro.removeListener(cb),

        getStore: () => store,
        getIndex: () => index
  };
}

export async function copy(from: Dag, to:Dag): Promise<void> {    
    for await (const e of from.loadAllEntries()) {
        await to.append(e.payload, e.meta, new Set(json.fromSet(e.header.prevEntryHashes)));
    }
}
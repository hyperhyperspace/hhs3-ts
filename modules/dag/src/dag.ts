import { Hash, sha } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Entry, ForkPosition, Header, Position } from "dag_defs";
import { DagIndex } from "idx/dag_idx";
import { DagStore } from "store/dag_store";

export * from "dag_defs";
export * as store from "store";
export * as idx from "idx";

// DAG definition

export type Dag = {
  append(payload: json.Literal, meta: json.Literal, after?: Position): Promise<Hash>;
  loadEntry(h: Hash): Promise<Entry|undefined>;
  loadHeader(h: Hash): Promise<Header|undefined>;

  getFrontier(): Promise<Position>;

  // latest position where history hasn't forked yet
  findForkPosition(first: Position, second: Position): Promise<ForkPosition>;
  findMinimalCover(p: Position): Promise<Position>;

  loadAllEntries(): AsyncIterable<Entry>; // in topo order

  getStore(): DagStore;
  getIndex(): DagIndex;
};

export async function entry(payload: json.Literal, meta: json.Literal, after?: Position): Promise<Entry> {
    const payloadHash: Hash = await sha.sha256(json.toStringNormalized(payload));
    const prevEntryHashes = json.toSet(after ?? new Set<Hash>());
    const header: Header = { payloadHash, prevEntryHashes };
    const hash: Hash = await sha.sha256(json.toStringNormalized(header));
    const entry: Entry = { hash, header, payload, meta: meta };

    return entry;
}

export function position(...hashes: Hash[]): Position {
    return new Set(hashes);
}

export function create(
                    store: DagStore,
                    index: DagIndex,
                ): Dag {

    return {
        append: async (payload, meta, after) => {
        
            const e = await entry(payload, meta, after);

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
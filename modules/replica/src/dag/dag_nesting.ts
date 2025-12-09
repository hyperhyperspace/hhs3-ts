import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { checkFilter, Dag, Entry, EntryMetaFilter, ForkPosition, Header, joinFilters, MetaProps, position, Position } from "@hyper-hyper-space/hhs3_dag";
import { DagIndex } from "@hyper-hyper-space/hhs3_dag/dist/idx";
import { DagStore } from "@hyper-hyper-space/hhs3_dag/dist/store";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Literal } from "@hyper-hyper-space/hhs3_json/dist/literal";
import { iterators } from "@hyper-hyper-space/hhs3_util";

// A wrapper that restricts the DAG to the subset of entries that affect a given
// sub-object. This works mostly transparently, wrapping/unwrapping payloads and
// metadata.

// Note: the DAG structure still contains all the entries from the parent object,
//       so some aspects will not be fully transparen:
//       - the predecessers are not wrapped, so they will refer to unrelated operations
//       - findForkPosition will search over the full, unfiltered DAG
//       - maybe other things?

export type SubDagCreator = (createOpPayload: Literal, createOpMeta: MetaProps, at: Position) => Promise<Dag>;


export interface DagScope {
    startAt(): Position;
    startEmpty(): boolean;
    baseFilter(): EntryMetaFilter;

    wrapPayload(payload: Literal, at: Position): Literal;
    unwrapPayload(payload: Literal, at: Position): Literal;

    wrapMeta(meta: MetaProps, wrappedPayload: Literal, at: Position): MetaProps;
    unwrapMeta(meta: MetaProps, wrappedPayload: Literal, at: Position): MetaProps;
    
    wrapFilter(filter: EntryMetaFilter): EntryMetaFilter;
}


export class SubDag implements Dag {

    private dag: Dag;
    private scope: DagScope;
    private empty: boolean;

    constructor(dag: Dag, scope: DagScope) {
        this.dag = dag;
        this.scope = scope;
        this.empty = scope.startEmpty();
    }
    
    async append(payload: Literal, meta: MetaProps, after?: Position): Promise<Hash> {

        if (after === undefined) {
            if (this.empty) {
                after = this.scope.startAt();
            } else {
                after = await this.dag.getFrontier();
            }
        } else {
            if (after.size === 0) {
                after = this.scope.startAt();
            }
        }

        this.empty = false;

        const wrappedPayload = this.scope.wrapPayload(payload, after);
        const wrappedMeta = this.scope.wrapMeta(meta, wrappedPayload, after);
        return this.dag.append(wrappedPayload, wrappedMeta, after);
    }
    

    computeEntryHash(payload: Literal, after?: Position): Promise<Hash> {
       
        if (after === undefined || after.size === 0) {
            after = this.scope.startAt();
        }

        return this.dag.computeEntryHash(this.scope.wrapPayload(payload, after), after);
    }

    async loadEntry(h: Hash): Promise<Entry | undefined> {
        let entry = await this.dag.loadEntry(h);
        if (entry === undefined) {
            return undefined;
        }

        const at = position(...json.fromSet(entry.header.prevEntryHashes))
        const unwrappedPayload = this.scope.unwrapPayload(entry.payload, at);
        const unwrappedMeta = this.scope.unwrapMeta(entry.meta, unwrappedPayload, at);

        return {
            ...entry,
            payload: unwrappedPayload,
            meta: unwrappedMeta,
        };
    }
    
    loadHeader(h: Hash): Promise<Header | undefined> {
        return this.dag.loadHeader(h);
    }
    
    async getFrontier(): Promise<Position> {
        return this.dag.findCoverWithFilter(await this.dag.getFrontier(), this.scope.baseFilter());
    }
    
    findForkPosition(first: Position, second: Position): Promise<ForkPosition> {
        return this.dag.findForkPosition(first, second);
    }
    
    findMinimalCover(p: Position): Promise<Position> {
        return this.dag.findCoverWithFilter(p, this.scope.baseFilter());
    }
    
    findCoverWithFilter(from: Position, meta: EntryMetaFilter): Promise<Position> {
        return this.dag.findCoverWithFilter(from, joinFilters(this.scope.baseFilter(), this.scope.wrapFilter(meta)));
    }
    
    findConcurrentCoverWithFilter(from: Position, concurrentTo: Position, meta: EntryMetaFilter): Promise<Position> {
        return this.dag.findConcurrentCoverWithFilter(from, concurrentTo, joinFilters(this.scope.baseFilter(), this.scope.wrapFilter(meta)));
    }
    
    loadAllEntries(): AsyncIterable<Entry> {
        return this.dag.loadAllEntries();

       // return iterators.filter(this.dag.loadAllEntries(), (entry: Entry) => checkFilter(entry.meta, this.scope.baseFilter()));
    }
    
    getStore(): DagStore {
        return this.dag.getStore();
    }
    
    getIndex(): DagIndex {
        return this.dag.getIndex();
    }
}
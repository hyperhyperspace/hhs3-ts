import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Dag, Entry, EntryMetaFilter, ForkPosition, joinFilters, MetaProps, position, Position } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Literal } from "@hyper-hyper-space/hhs3_json/dist/literal.js";

// ScopedDag: the object's logical history surface.
// For root objects, backed by RootScopedDag; for nested objects, backed by NestedScopedDag.

export type ScopedDag = {
    append(payload: Literal, meta: MetaProps, after?: Position): Promise<Hash>;
    computeEntryHash(payload: Literal, after?: Position): Promise<Hash>;
    loadEntry(h: Hash): Promise<Entry | undefined>;
    getFrontier(): Promise<Position>;
    findCoverWithFilter(from: Position, meta: EntryMetaFilter): Promise<Position>;
    findConcurrentCoverWithFilter(from: Position, concurrentTo: Position, meta: EntryMetaFilter): Promise<Position>;
    findMinimalCover(p: Position): Promise<Position>;
};

// CausalDag: the broader causal structure, read-only from the object's perspective.
// Always backed by the real enclosing DAG.

export type CausalDag = {
    findForkPosition(first: Position, second: Position): Promise<ForkPosition>;
};

// RootScopedDag: wraps a full Dag exposing only ScopedDag methods at runtime,
// ensuring root-level objects have the same method surface as nested objects.

export class RootScopedDag implements ScopedDag {
    private dag: Dag;

    constructor(dag: Dag) {
        this.dag = dag;
    }

    append(payload: Literal, meta: MetaProps, after?: Position): Promise<Hash> {
        return this.dag.append(payload, meta, after);
    }

    computeEntryHash(payload: Literal, after?: Position): Promise<Hash> {
        return this.dag.computeEntryHash(payload, after);
    }

    loadEntry(h: Hash): Promise<Entry | undefined> {
        return this.dag.loadEntry(h);
    }

    getFrontier(): Promise<Position> {
        return this.dag.getFrontier();
    }

    findCoverWithFilter(from: Position, meta: EntryMetaFilter): Promise<Position> {
        return this.dag.findCoverWithFilter(from, meta);
    }

    findConcurrentCoverWithFilter(from: Position, concurrentTo: Position, meta: EntryMetaFilter): Promise<Position> {
        return this.dag.findConcurrentCoverWithFilter(from, concurrentTo, meta);
    }

    findMinimalCover(p: Position): Promise<Position> {
        return this.dag.findMinimalCover(p);
    }
}


export interface DagScope {
    startAt(): Position;
    startEmpty(): boolean;
    baseFilter(): EntryMetaFilter;

    wrapPayload(payload: Literal, at: Position): Literal;
    unwrapPayload(payload: Literal, at: Position): Literal;

    wrapMeta(meta: MetaProps, wrappedPayload: Literal, at: Position): MetaProps;
    unwrapMeta(meta: MetaProps, wrappedPayload: Literal, at: Position): MetaProps;
    
    wrapFilter(filter: EntryMetaFilter): EntryMetaFilter;
    validateWrappedPayload?(wrappedPayload: Literal, wrappedMeta: MetaProps, at: Position): Promise<boolean>;
}


export class NestedScopedDag implements ScopedDag {

    private dag: ScopedDag;
    private scope: DagScope;
    private empty: boolean;

    constructor(dag: ScopedDag, scope: DagScope) {
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

        const wrappedPayload = this.scope.wrapPayload(payload, after);
        const wrappedMeta = this.scope.wrapMeta(meta, wrappedPayload, after);

        if (this.scope.validateWrappedPayload !== undefined) {
            const valid = await this.scope.validateWrappedPayload(wrappedPayload, wrappedMeta, after);
            if (!valid) {
                throw new Error("Attempted to append an invalid wrapped payload");
            }
        }

        const hash = await this.dag.append(wrappedPayload, wrappedMeta, after);
        this.empty = false;
        return hash;
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
    
    async getFrontier(): Promise<Position> {
        return this.dag.findCoverWithFilter(await this.dag.getFrontier(), this.scope.baseFilter());
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
}

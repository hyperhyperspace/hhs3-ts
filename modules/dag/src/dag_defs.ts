import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Literal } from "@hyper-hyper-space/hhs3_json/dist/literal";
import { MultiMap } from "@hyper-hyper-space/hhs3_util";

// basic data types for reasoning about DAGs

export type Position = Set<Hash>; // a point in virtual time, partially ordered

export const emptyPosition: () => Position = () => new Set<Hash>();

// a Header tells us what payload to insert in which position in a DAG.

export type Header = Readonly<{
    payloadHash: Hash,
    prevEntryHashes: json.Set
}>;

// its metadata is used to index and query the DAG.

export type MetaProps = {[key: string]: json.Set};

// To enable DAG indexing, I'm using only monotonic types for MetaProps (only sets of strings, for now)
// Other monotonic types could be added: integers + integer ranges, etc.

// Important note: prevEntryHashes is assumed to be a minimal cover
// (see below for definition)

// an entry for the DAG:

export type Entry = Readonly<{
    hash: Hash,
    header: Header,
    payload: json.Literal,
    meta: MetaProps
}>;

// meta is information we do not want to hash, for whatever reason.

export type EntryId = number;

export type Fragment = {
    end: Position,
    start: Position,
    
    prev: MultiMap<Hash, Hash>;
    next: MultiMap<Hash, Hash>;
}

// Function signatures for DAG operations

// Given a position p, a minimal cover is the smallest subset p' of p
// such that:
//      history(p') = history(p)
// where history is a closure over the predecessor relationship in the DAG
// i.e. history(p) includes p and all the elements that come before them.

export type FindMinimalCoverFn = (p: Position) => Promise<Position>;

// Given two Positions A, B, we define a fork position:

export type ForkPosition = {
  commonFrontier: Position,
  common: Position,
  forkA: Position,
  forkB: Position
};

// Conceptually this represents the position in the DAG where A and B diverged.

// commonFrontier: the minimal cover of the intersection of history(A) and
//                 history(B).
// common: all the elements in the intersection of history(A) and history(B)
//         that have a successor that is only in A or only in B.
// forkA: all the elements that are only in A's history, and either have a
//        predecessor in the intersection of A and B's histories, or have no
//        predecessors at all.
// forkB: same as above, but for B.

export type FindForkPositionFn = (first: Position, second: Position) => Promise<ForkPosition>;

// Using metadata to find covers with certain properties 
// - this is how we materalize !

export type MetaKeys = string[];
export type MetaContainsValues = {[key: string]: Array<string>};
export type EntryMetaFilter = { containsKeys?: MetaKeys, containsValues?: MetaContainsValues };

export type FindCoverWithFilterFn = (from: Position, filter: EntryMetaFilter) => Promise<Position>;
export type FindConcurrentCoverWithFilterFn = (from: Position, concurrentTo: Position, filter: EntryMetaFilter) => Promise<Position>;
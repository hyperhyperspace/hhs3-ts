// Public RBlobStore interfaces.

import type { B64Hash, KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Delta, RObject, Version, View } from "@hyper-hyper-space/hhs3_mvt";

// A re-readable byte source: putFile reads it twice (hashing, then upload).
export type FileSource = {
    size: number;
    read(): AsyncIterable<Uint8Array>;
};

export type PutFileOptions = {
    lane: number;
    // Continue an upload chain this writer already started: its header, and
    // the last chunk appended (omitted when no chunk made it).
    resume?: { header: B64Hash; tail?: B64Hash };
    // Skip the upload when findFile already finds a complete chain.
    dedup?: boolean;
};

export type StoredFile = {
    fileHash: B64Hash;
    size: number;
    header: B64Hash;
    tail: B64Hash;     // the op that completes the chain: its last chunk, or the header of an empty file
};

// Where one upload chain stands: `last` is its latest op (the header when no
// chunk arrived), a resume point for its author.
export type UploadProgress = {
    header: B64Hash;
    author: KeyId;
    size: number;
    lane: number;
    received: number;
    chunks: number;
    last: B64Hash;
    complete: boolean;
};

// Progress of one upload chain, as seen in a delta.
export type UploadChain = {
    header: B64Hash;
    fileHash: B64Hash;
    size: number;
    lane: number;
    author: KeyId;
};

export type ChunkArrival = {
    hash: B64Hash;
    header: B64Hash;
    index: number;
    length: number;
    complete: boolean;   // this chunk is the chain's last
};

export type BlobStoreChanges = {
    headers: UploadChain[];
    chunks: ChunkArrival[];
    refAdvances: B64Hash[];
};

export type BlobStoreDelta = Delta<BlobStoreChanges>;

export interface RBlobStoreView extends View {
    getObject(): RBlobStore;
}

export interface RBlobStore extends RObject {
    getName(): string;
    getGroupId(): B64Hash;

    // A signed ref-advance of the bound group to its local frontier (or `to`),
    // on `lane`.
    refAdvance(author: OwnIdentity, lane: number, to?: Version): Promise<B64Hash>;

    putFile(source: FileSource, writer: OwnIdentity, opts: PutFileOptions): Promise<StoredFile>;

    // Streams the bytes of the chain ending at `tail` (a last chunk, or the
    // header of an empty file).
    readFile(tail: B64Hash): AsyncIterable<Uint8Array>;

    // A complete chain for `fileHash`, found by walking the DAG meta tags.
    findFile(fileHash: B64Hash): Promise<StoredFile | undefined>;
    findChains(fileHash: B64Hash): Promise<UploadProgress[]>;

    // The lane an entry was appended on (from its meta tag).
    laneOf(entryHash: B64Hash): Promise<number | undefined>;
    laneCover(lane: number): Promise<Version>;

    getView(at?: Version, from?: Version): Promise<RBlobStoreView>;
    computeDelta(start: Version, end: Version): Promise<BlobStoreDelta>;
}

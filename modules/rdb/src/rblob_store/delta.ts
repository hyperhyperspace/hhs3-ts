// RBlobStore deltas: the entries in end's causal past and not in start's, as
// summaries without bytes (each new entry is loaded once, see
// ../rfiles/walk.ts). The store has no barriers, so nothing below start
// changes meaning and the revision bound is the fork's common frontier.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dag } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { DeltaAccumulator, DeltaChanges, Version } from "@hyper-hyper-space/hhs3_mvt";

import type { BlobStoreChanges, BlobStoreDelta, ChunkArrival, UploadChain } from "./interfaces.js";
import { RBLOB_STORE_TYPE_ID, ChunkPayload, FileHeaderPayload, chunkCount } from "./payload.js";
import { base64DecodedLength } from "../rfiles/hashes.js";
import { walkNewEntries } from "../rfiles/walk.js";

export class BlobStoreDeltaAccumulator implements DeltaAccumulator<BlobStoreChanges> {

    private readonly headers: UploadChain[] = [];
    private readonly chunks: ChunkArrival[] = [];
    private readonly refAdvances: B64Hash[] = [];
    // header -> size, for headers seen in this delta
    private readonly sizes = new Map<B64Hash, number>();
    private readonly pendingChunks: { hash: B64Hash; chunk: ChunkPayload }[] = [];

    constructor(private readonly headerSize: (header: B64Hash) => Promise<number | undefined>) {}

    async ingest(entry: dag.Entry): Promise<boolean> {
        const p = entry.payload as json.LiteralMap;
        switch (p['action']) {
            case 'ref-advance':
                this.refAdvances.push(entry.hash);
                return true;
            case 'file': {
                const header = entry.payload as unknown as FileHeaderPayload;
                this.sizes.set(entry.hash, header.size);
                this.headers.push({
                    header: entry.hash, fileHash: header.fileHash, size: header.size,
                    lane: header.lane, author: header.author,
                });
                return true;
            }
            case 'chunk':
                this.pendingChunks.push({ hash: entry.hash, chunk: entry.payload as unknown as ChunkPayload });
                return true;
            default:
                return false;
        }
    }

    async finalize(): Promise<DeltaChanges<BlobStoreChanges>> {
        for (const { hash, chunk } of this.pendingChunks) {
            let size = this.sizes.get(chunk.header);
            if (size === undefined) {
                size = await this.headerSize(chunk.header);
                if (size !== undefined) this.sizes.set(chunk.header, size);
            }
            this.chunks.push({
                hash, header: chunk.header, index: chunk.index,
                length: base64DecodedLength(chunk.bytes),
                complete: size !== undefined && chunk.index === chunkCount(size) - 1,
            });
        }
        this.pendingChunks.length = 0;

        this.headers.sort((a, b) => (a.header < b.header ? -1 : a.header > b.header ? 1 : 0));
        this.chunks.sort((a, b) => (a.header < b.header ? -1 : a.header > b.header ? 1 : a.index - b.index));
        this.refAdvances.sort();

        return {
            type: RBLOB_STORE_TYPE_ID,
            changes: { headers: this.headers, chunks: this.chunks, refAdvances: this.refAdvances },
            nested: new Map(),
        };
    }
}

export async function computeBlobStoreDelta(
    rawDag: dag.Dag, start: Version, end: Version, accumulator: BlobStoreDeltaAccumulator,
): Promise<BlobStoreDelta> {
    const revisionBound = await walkNewEntries(rawDag, start, end, async (entry) => { await accumulator.ingest(entry); });
    const changes = await accumulator.finalize();
    return { ...changes, start, end, revisionBound };
}

// Positional validation for RBlobStore ops.
//
//   ref-advance   monotonic, author admitted at the new version (access.ts)
//   file          author admitted at the observed version of the header
//   chunk         exactly one predecessor: the header for chunk 0, otherwise
//                 chunk index-1 of the same header; index below the chunk
//                 count; full length except the last, which holds the
//                 remainder; hashes to the link its predecessor promised
//                 (header.first, or the predecessor's next); the last chunk's
//                 next is END; same author as the header, signed with the
//                 header's key
//
// A chunk's causal past is its upload chain plus the header's past, so no
// ref-advance sits between a chunk and its header: its observed version is
// exactly the header's. Chunks reuse the header's verdict and key, memoized by
// header entry hash, instead of resolving the observation again.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import {
    ScopedDag, Version, ValidationResult, RefAdvancePayload,
    validationFailure, validationOk, wrapValidationFailure, verifyPayloadSignature,
} from "@hyper-hyper-space/hhs3_mvt";

import type { FilesAccessControl } from "../rfiles/access.js";
import { END_LINK, chunkLink } from "../rfiles/hashes.js";
import { checkBlobStoreOp } from "./validate.js";
import { ChunkPayload, FileHeaderPayload, chunkCount, chunkLength } from "./payload.js";

class BoundedMap<K, V> {
    private readonly map = new Map<K, V>();
    constructor(private readonly capacity: number) {}

    get(key: K): V | undefined {
        const value = this.map.get(key);
        if (value !== undefined) {
            this.map.delete(key);
            this.map.set(key, value);
        }
        return value;
    }

    set(key: K, value: V): void {
        this.map.delete(key);
        this.map.set(key, value);
        if (this.map.size > this.capacity) this.map.delete(this.map.keys().next().value!);
    }
}

type HeaderInfo = {
    header: FileHeaderPayload;
    verdict: ValidationResult;
    key?: PublicKey;
};

type ChunkLink = { header: B64Hash; index: number; next: B64Hash };

export class BlobStoreValidator {

    private readonly headers = new BoundedMap<B64Hash, HeaderInfo>(1024);
    private readonly chunks = new BoundedMap<B64Hash, ChunkLink>(1024);

    constructor(
        private readonly getDag: () => Promise<ScopedDag>,
        private readonly access: FilesAccessControl,
    ) {}

    async validate(payload: json.Literal, at: Version): Promise<ValidationResult> {
        const checked = checkBlobStoreOp(payload, this.access.groupId);
        if (!checked.valid) return { valid: false, why: checked.why };
        if (at.size === 0) return validationFailure("RBlobStore ops must follow the create entry");

        const dag = await this.getDag();
        const op = checked.op;

        if (op.action === 'ref-advance') {
            const result = await this.access.validateRefAdvance(dag, op.payload as unknown as RefAdvancePayload & json.LiteralMap, at, []);
            return wrapValidationFailure('RBlobStore ref-advance rejected', result);
        }

        if (op.action === 'file') {
            const info = await this.admitHeader(dag, op.payload, at);
            this.headers.set(await dag.computeEntryHash(payload, at), info);
            return wrapValidationFailure('RBlobStore file header rejected', info.verdict);
        }

        return wrapValidationFailure('RBlobStore chunk rejected', await this.validateChunk(dag, op.payload, op.bytes, at));
    }

    // Called with every applied op, so the next chunk of a chain finds its
    // predecessor without reloading it.
    remember(hash: B64Hash, payload: json.Literal): void {
        const p = payload as json.LiteralMap;
        if (p['action'] === 'chunk') {
            const chunk = payload as unknown as ChunkPayload;
            this.chunks.set(hash, { header: chunk.header, index: chunk.index, next: chunk.next });
        }
    }

    private async admitHeader(dag: ScopedDag, header: FileHeaderPayload, at: Version): Promise<HeaderInfo> {
        const observed = await this.access.observedVersion(dag, at);
        const admission = await this.access.admit(header as unknown as json.LiteralMap, at, observed, []);
        return admission.valid ? { header, verdict: validationOk(), key: admission.key } : { header, verdict: admission };
    }

    private async headerInfo(dag: ScopedDag, hash: B64Hash): Promise<HeaderInfo | undefined> {
        const cached = this.headers.get(hash);
        if (cached !== undefined) return cached;

        const entry = await dag.loadEntry(hash);
        if (entry === undefined) return undefined;
        const header = entry.payload as json.LiteralMap;
        if (header['action'] !== 'file') return undefined;

        const at: Version = new Set(json.fromSet(entry.header.prevEntryHashes));
        const info = await this.admitHeader(dag, header as unknown as FileHeaderPayload, at);
        this.headers.set(hash, info);
        return info;
    }

    private async chunkLinkOf(dag: ScopedDag, hash: B64Hash): Promise<ChunkLink | undefined> {
        const cached = this.chunks.get(hash);
        if (cached !== undefined) return cached;

        const entry = await dag.loadEntry(hash);
        if (entry === undefined) return undefined;
        const p = entry.payload as json.LiteralMap;
        if (p['action'] !== 'chunk') return undefined;
        const chunk = entry.payload as unknown as ChunkPayload;
        const link = { header: chunk.header, index: chunk.index, next: chunk.next };
        this.chunks.set(hash, link);
        return link;
    }

    private async validateChunk(dag: ScopedDag, chunk: ChunkPayload, bytes: Uint8Array, at: Version): Promise<ValidationResult> {
        if (at.size !== 1) return validationFailure('a chunk must have exactly one predecessor');
        const pred = at.values().next().value!;

        let prevNext: B64Hash | undefined;
        if (chunk.index === 0) {
            if (pred !== chunk.header) return validationFailure('chunk 0 must follow its header');
        } else {
            const prev = await this.chunkLinkOf(dag, pred);
            if (prev === undefined || prev.header !== chunk.header || prev.index !== chunk.index - 1) {
                return validationFailure(`chunk ${chunk.index} must follow chunk ${chunk.index - 1} of the same header`);
            }
            prevNext = prev.next;
        }

        const info = await this.headerInfo(dag, chunk.header);
        if (info === undefined) return validationFailure('chunk header not found');
        const header = info.header;

        const count = chunkCount(header.size);
        if (chunk.index >= count) return validationFailure(`chunk index ${chunk.index} is past the file's ${count} chunks`);
        if (bytes.length !== chunkLength(header.size, chunk.index)) return validationFailure(`chunk ${chunk.index} has the wrong length`);
        if (chunk.index === count - 1 && chunk.next !== END_LINK) return validationFailure("the last chunk's next link must be END");

        const expected = chunk.index === 0 ? header.first : prevNext;
        if (chunkLink(bytes, chunk.next) !== expected) return validationFailure(`chunk ${chunk.index} does not match its link`);

        if (chunk.author !== header.author) return validationFailure('a chunk must have the same author as its header');
        if (!info.verdict.valid || info.key === undefined) return wrapValidationFailure('chunk header is not admitted', info.verdict);

        const key = info.key;
        const signed = await verifyPayloadSignature(chunk as unknown as json.LiteralMap, at, async (id) => (id === chunk.author ? key : undefined), []);
        return signed ? validationOk() : validationFailure('bad signature');
    }
}

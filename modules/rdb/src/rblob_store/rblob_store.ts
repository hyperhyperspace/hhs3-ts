// RBlobStore: file contents as signed, hash-linked chunk chains (a standalone
// RObject in its own DAG).
//
// ACTIONS (see payload.ts for formats):
//
//   create        name, seed, the access fields of ../rfiles/access.ts, and
//                 the fixed lane count and chunk size
//   ref-advance   a signed, non-barrier observation of the bound group, on a lane
//   file          the header of one upload: fileHash, size, first link
//   chunk         one chunk of an upload chain (see validate_ops.ts)
//
// A file is complete when the last chunk of any of its upload chains is
// present (or when the header of an empty file is). Every chunk is verified on
// arrival, so reading a complete chain needs no re-check. Uploads are deduped
// per file by the writer: one that already has the fileHash skips the upload.
//
// RBlobStore does not sync on its own: the RDb that computes it as a member
// orchestrates its sync.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256, base64 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { dag, MetaProps, position } from "@hyper-hyper-space/hhs3_dag";

import {
    Payload, RObjectFactory, RContext, LoadObjectOptions, Version, version, ForeignDep,
    formatValidationFailure, ValidationRejectedError, ValidationResult,
    createRefAdvancePayload, createRefAdvanceMeta, signPayload,
} from "@hyper-hyper-space/hhs3_mvt";
import { RootScopedDag, ScopedDag, CausalDag, ScopedDagSubscription } from "@hyper-hyper-space/hhs3_mvt";

import { FilesAccessControl, accessOf } from "../rfiles/access.js";
import type { FilesAccess } from "../rfiles/access.js";
import { END_LINK, chunkHash, linkHash, encodeBase64, fileHashOf, CONTENT_HASH_BYTES } from "../rfiles/hashes.js";
import type {
    RBlobStore as RBlobStoreContract, RBlobStoreView, FileSource, PutFileOptions, StoredFile, BlobStoreDelta, UploadProgress,
} from "./interfaces.js";
import {
    CreateBlobStorePayload, FileHeaderPayload, ChunkPayload, BlobRefAdvancePayload,
    RBLOB_STORE_TYPE_ID, LANES, CHUNK_BYTES, MAX_FILE_BYTES, META_LANE, META_FILE, META_DONE, chunkCount,
} from "./payload.js";
import { validateBlobStoreCreate } from "./validate.js";
import { BlobStoreValidator } from "./validate_ops.js";
import { BlobStoreDeltaAccumulator, computeBlobStoreDelta } from "./delta.js";
import { RBlobStoreViewImpl } from "./view.js";

export { RBLOB_STORE_TYPE_ID } from "./payload.js";

function isObject(payload: Payload): payload is json.LiteralMap {
    return typeof payload === 'object' && payload !== null && !Array.isArray(payload);
}

function boundGroupOf(payload: Payload): B64Hash | undefined {
    if (!isObject(payload)) return undefined;
    const bindings = payload['bindings'];
    if (!isObject(bindings)) return undefined;
    const ids = Object.values(bindings);
    return ids.length === 1 && typeof ids[0] === 'string' ? ids[0] : undefined;
}

export const rBlobStoreFactory: RObjectFactory = {

    computeRootObjectId: async (payload: Payload, ctx: RContext) => {
        return dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256)).hash;
    },

    validateCreationPayload: async (payload: Payload) => validateBlobStoreCreate(payload),

    executeCreationPayload: async (payload: Payload, _ctx: RContext, scopedDag: ScopedDag) => {
        return await scopedDag.append(payload, {}, position());
    },

    loadObject: async (id: B64Hash, ctx: RContext, opts?: LoadObjectOptions) => {
        if (opts?.parent !== undefined) throw new Error("RBlobStore is a standalone object (no nesting parent)");
        const backendLabel = opts?.backendLabel ?? 'default';
        const rawDag = await ctx.getDag(id, backendLabel);
        if (rawDag === undefined) throw new Error(`DAG '${id}' not found`);
        const createOp = (await rawDag.loadEntry(id))!.payload as unknown as CreateBlobStorePayload;
        return new RBlobStoreImpl(id, createOp, ctx, backendLabel);
    },

    extractCreationForeignDeps: async (payload: Payload) => {
        const groupId = boundGroupOf(payload);
        return groupId === undefined ? undefined : [{ objectId: groupId, requiredHashes: [] }];
    },
};

// Re-chunks a byte stream into CHUNK_BYTES pieces (the last one shorter).
async function* rechunk(stream: AsyncIterable<Uint8Array>): AsyncIterable<Uint8Array> {
    let buffer = new Uint8Array(CHUNK_BYTES);
    let filled = 0;
    for await (const piece of stream) {
        let offset = 0;
        while (offset < piece.length) {
            const take = Math.min(CHUNK_BYTES - filled, piece.length - offset);
            buffer.set(piece.subarray(offset, offset + take), filled);
            filled += take;
            offset += take;
            if (filled === CHUNK_BYTES) {
                yield buffer;
                buffer = new Uint8Array(CHUNK_BYTES);
                filled = 0;
            }
        }
    }
    if (filled > 0) yield buffer.slice(0, filled);
}

export class RBlobStoreImpl implements RBlobStoreContract {

    static create = (options: {
        name: string;
        seed: string;
        access: FilesAccess;
    }): CreateBlobStorePayload => ({
        action: 'create',
        type: RBLOB_STORE_TYPE_ID,
        name: options.name,
        seed: options.seed,
        bindings: options.access.bindings,
        idProvider: options.access.idProvider,
        canWrite: options.access.canWrite,
        lanes: LANES,
        chunkBytes: CHUNK_BYTES,
    });

    static typeId = RBLOB_STORE_TYPE_ID;

    readonly createOpId: B64Hash;
    readonly createOp: CreateBlobStorePayload;
    readonly access: FilesAccessControl;
    private readonly ctx: RContext;
    private readonly backendLabel: string;
    private readonly validator: BlobStoreValidator;

    private _scopedDag: ScopedDag | undefined;
    private _rawDag: dag.Dag | undefined;

    constructor(createOpId: B64Hash, createOp: CreateBlobStorePayload, ctx: RContext, backendLabel: string = 'default') {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.ctx = ctx;
        this.backendLabel = backendLabel;
        this.access = new FilesAccessControl(ctx, accessOf(createOp));
        this.validator = new BlobStoreValidator(() => this.getScopedDag(), this.access);
    }

    getId(): B64Hash { return this.createOpId; }
    getType(): string { return RBlobStoreImpl.typeId; }
    getBackendLabel(): string { return this.backendLabel; }
    getName(): string { return this.createOp.name; }
    getGroupId(): B64Hash { return this.access.groupId; }

    private selfValidate(): boolean {
        return this.ctx.getConfig().selfValidate || false;
    }

    private async appendValidated(payload: json.LiteralMap, at: Version): Promise<B64Hash> {
        if (this.selfValidate()) {
            const result = await this.validatePayload(payload, at);
            if (!result.valid) throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
        }
        return this.applyPayload(payload, at);
    }

    // --- Lanes ---

    async laneOf(entryHash: B64Hash): Promise<number | undefined> {
        const entry = await (await this.rawDag()).loadEntry(entryHash);
        const tag = entry?.meta[META_LANE];
        if (tag === undefined) return undefined;
        const [lane] = json.fromSet(tag);
        return lane === undefined ? undefined : Number(lane);
    }

    // The frontier entries on `lane`, or the create entry when there are none.
    async laneCover(lane: number): Promise<Version> {
        const cover = version();
        for (const h of await (await this.getScopedDag()).getFrontier()) {
            if (await this.laneOf(h) === lane) cover.add(h);
        }
        return cover.size > 0 ? cover : version(this.createOpId);
    }

    // --- Writers ---

    async refAdvance(author: OwnIdentity, lane: number, to?: Version): Promise<B64Hash> {
        this.checkLane(lane);
        const group = await this.access.loadGroup();
        const target = to ?? await (await group.getScopedDag()).getFrontier();
        const at = await this.laneCover(lane);
        const base = { ...createRefAdvancePayload(this.access.groupId, target), lane } as unknown as json.LiteralMap;
        const signed = await signPayload(base, author, at);
        return this.appendValidated(signed, at);
    }

    async putFile(source: FileSource, writer: OwnIdentity, opts: PutFileOptions): Promise<StoredFile> {
        this.checkLane(opts.lane);
        const size = source.size;
        const { fileHash, first, hashes, linkAt } = await hashSource(source, 'putFile');

        if (opts.dedup) {
            const found = await this.findFile(fileHash);
            if (found !== undefined) return found;
        }

        const scopedDag = await this.getScopedDag();

        // the chain to extend: a resumed one, or a new header after a fresh
        // observation of the group on this lane
        let header: B64Hash;
        let start: number;
        let at: Version;
        if (opts.resume !== undefined) {
            const entry = await scopedDag.loadEntry(opts.resume.header);
            const p = entry?.payload as FileHeaderPayload | undefined;
            if (p === undefined || p.action !== 'file' || p.fileHash !== fileHash || p.author !== writer.keyId) {
                throw new Error('putFile: the chain to resume is not this file by this writer');
            }
            header = opts.resume.header;
            if (opts.resume.tail !== undefined) {
                const tail = (await scopedDag.loadEntry(opts.resume.tail))?.payload as ChunkPayload | undefined;
                if (tail === undefined || tail.action !== 'chunk' || tail.header !== header) {
                    throw new Error('putFile: the resume tail is not a chunk of the resumed chain');
                }
                start = tail.index + 1;
                at = version(opts.resume.tail);
            } else {
                start = 0;
                at = version(header);
            }
        } else {
            const observed = await this.refAdvance(writer, opts.lane);
            const base: json.LiteralMap = { action: 'file', lane: opts.lane, fileHash, size, first };
            const signedHeader = await signPayload(base, writer, version(observed));
            header = await this.appendValidated(signedHeader, version(observed));
            start = 0;
            at = version(header);
        }

        // pass 2: the chunks, each on the one before it
        let tail = start === 0 ? header : [...at][0];
        let index = 0;
        for await (const chunk of rechunk(source.read())) {
            const j = index++;
            if (j < start) continue;
            const expected = hashes.subarray(j * CONTENT_HASH_BYTES, (j + 1) * CONTENT_HASH_BYTES);
            if (!sameBytes(chunkHash(chunk), expected)) throw new Error('putFile: the source changed between passes');
            const base: json.LiteralMap = { action: 'chunk', header, index: j, bytes: encodeBase64(chunk), next: linkAt(j + 1) };
            const signed = await signPayload(base, writer, at);
            tail = await this.appendValidated(signed, at);
            at = version(tail);
        }

        return { fileHash, size, header, tail };
    }

    private checkLane(lane: number): void {
        if (!Number.isInteger(lane) || lane < 0 || lane >= LANES) throw new Error(`lane ${lane} is out of range 0..${LANES - 1}`);
    }

    // --- Readers ---

    async *readFile(tail: B64Hash): AsyncIterable<Uint8Array> {
        const rawDag = await this.rawDag();
        const last = await rawDag.loadEntry(tail);
        if (last === undefined) throw new Error(`readFile: '${tail}' is not in the store`);
        const p = last.payload as json.LiteralMap;
        if (p['action'] === 'file') {
            if ((last.payload as unknown as FileHeaderPayload).size !== 0) throw new Error('readFile: the chain has no chunks yet');
            return;
        }
        if (p['action'] !== 'chunk') throw new Error(`readFile: '${tail}' is not a chunk`);

        const chunk = last.payload as unknown as ChunkPayload;
        const headerEntry = await rawDag.loadEntry(chunk.header);
        const header = headerEntry?.payload as unknown as FileHeaderPayload | undefined;
        if (header === undefined || chunk.index !== chunkCount(header.size) - 1) throw new Error('readFile: the chain is not complete');

        // walk back over payload-free headers, then stream forward
        const chain: B64Hash[] = new Array(chunk.index + 1);
        chain[chunk.index] = tail;
        for (let i = chunk.index; i > 0; i--) {
            const prevs = [...json.fromSet((await rawDag.loadHeader(chain[i]))!.prevEntryHashes)];
            if (prevs.length !== 1) throw new Error('readFile: broken chain');
            chain[i - 1] = prevs[0];
        }
        const firstPrevs = [...json.fromSet((await rawDag.loadHeader(chain[0]))!.prevEntryHashes)];
        if (firstPrevs.length !== 1 || firstPrevs[0] !== chunk.header) throw new Error('readFile: broken chain');

        for (let i = 0; i < chain.length; i++) {
            const entry = i === chain.length - 1 ? last : await rawDag.loadEntry(chain[i]);
            const bytes = (entry!.payload as unknown as ChunkPayload).bytes;
            yield new Uint8Array(base64.toArrayBuffer(bytes));
        }
    }

    // The latest upload chains of `fileHash` (the cover of its headers and
    // chunks): each one's header, author, size, chunks so far and last op. A
    // chain that a later chain of the same file builds on is not listed.
    async findChains(fileHash: B64Hash): Promise<UploadProgress[]> {
        const scopedDag = await this.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(
            await scopedDag.getFrontier(), { containsValues: { [META_FILE]: [fileHash] } });
        const out: UploadProgress[] = [];
        for (const hash of [...cover].sort()) {
            const p = (await scopedDag.loadEntry(hash))!.payload as json.LiteralMap;
            const isChunk = p['action'] === 'chunk';
            const header = isChunk ? (p as unknown as ChunkPayload).header : hash;
            const h = isChunk ? (await scopedDag.loadEntry(header))!.payload as unknown as FileHeaderPayload : p as unknown as FileHeaderPayload;
            const received = isChunk ? (p as unknown as ChunkPayload).index + 1 : 0;
            const chunks = chunkCount(h.size);
            out.push({ header, author: h.author, size: h.size, lane: h.lane, received, chunks, last: hash, complete: received === chunks });
        }
        return out;
    }

    async findFile(fileHash: B64Hash): Promise<StoredFile | undefined> {
        const scopedDag = await this.getScopedDag();
        const done = await scopedDag.findCoverWithFilter(
            await scopedDag.getFrontier(), { containsValues: { [META_DONE]: [fileHash] } });
        const tails = [...done].sort();
        if (tails.length === 0) return undefined;

        const tail = tails[0];
        const entry = await scopedDag.loadEntry(tail);
        const p = entry!.payload as json.LiteralMap;
        const header = p['action'] === 'chunk' ? (p as unknown as ChunkPayload).header : tail;
        const headerPayload = p['action'] === 'chunk'
            ? (await scopedDag.loadEntry(header))!.payload as unknown as FileHeaderPayload
            : p as unknown as FileHeaderPayload;
        return { fileHash, size: headerPayload.size, header, tail };
    }

    // --- RObject interface ---

    async validatePayload(payload: Payload, at: Version): Promise<ValidationResult> {
        return this.validator.validate(payload, at);
    }

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        const hash = await scopedDag.append(payload, await this.metaFor(payload), at);
        this.validator.remember(hash, payload);
        return hash;
    }

    private async metaFor(payload: Payload): Promise<MetaProps> {
        if (!isObject(payload)) throw new Error('RBlobStore payload must be an object');
        switch (payload['action']) {
            case 'ref-advance': {
                const p = payload as unknown as BlobRefAdvancePayload;
                return { ...createRefAdvanceMeta(p.refId, { barrier: false }), [META_LANE]: json.toSet([String(p.lane)]) };
            }
            case 'file': {
                const p = payload as unknown as FileHeaderPayload;
                const meta: MetaProps = { [META_LANE]: json.toSet([String(p.lane)]), [META_FILE]: json.toSet([p.fileHash]) };
                if (p.size === 0) meta[META_DONE] = json.toSet([p.fileHash]);
                return meta;
            }
            case 'chunk': {
                const p = payload as unknown as ChunkPayload;
                const entry = await (await this.getScopedDag()).loadEntry(p.header);
                const header = entry?.payload as unknown as FileHeaderPayload | undefined;
                if (header === undefined || header.action !== 'file') throw new Error(`chunk header '${p.header}' not found`);
                const meta: MetaProps = { [META_LANE]: json.toSet([String(header.lane)]), [META_FILE]: json.toSet([header.fileHash]) };
                if (p.index === chunkCount(header.size) - 1) meta[META_DONE] = json.toSet([header.fileHash]);
                return meta;
            }
            default:
                throw new Error(`unknown RBlobStore action '${String(payload['action'])}'`);
        }
    }

    async getView(at?: Version, from?: Version): Promise<RBlobStoreView> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();
        from = from ?? await scopedDag.getFrontier();
        return new RBlobStoreViewImpl(this, at, from);
    }

    async computeDelta(start: Version, end: Version): Promise<BlobStoreDelta> {
        return computeBlobStoreDelta(await this.rawDag(), start, end, this.createDeltaAccumulator(start, end));
    }

    createDeltaAccumulator(_start: Version, _end: Version): BlobStoreDeltaAccumulator {
        return new BlobStoreDeltaAccumulator(async (header) => {
            const entry = await (await this.rawDag()).loadEntry(header);
            const p = entry?.payload as unknown as FileHeaderPayload | undefined;
            return p?.action === 'file' ? p.size : undefined;
        });
    }

    extractForeignDeps(payload: Payload, _at: Version): ForeignDep[] | undefined {
        const groupId = this.access.groupId;
        if (isObject(payload) && payload['action'] === 'ref-advance' && payload['refId'] === groupId && isObject(payload['refVersion'])) {
            return [{ objectId: groupId, requiredHashes: Object.keys(payload['refVersion']) }];
        }
        return [{ objectId: groupId, requiredHashes: [] }];
    }

    private _subscription: ScopedDagSubscription | undefined;

    private subscription(): ScopedDagSubscription {
        if (this._subscription === undefined) this._subscription = new ScopedDagSubscription(() => this.getScopedDag());
        return this._subscription;
    }

    subscribe(callback: (version: Version) => void): Promise<void> {
        return this.subscription().subscribe(callback);
    }

    unsubscribe(callback: (version: Version) => void): void {
        this.subscription().unsubscribe(callback);
    }

    private async rawDag(): Promise<dag.Dag> {
        if (this._rawDag === undefined) {
            const rawDag = await this.ctx.getDag(this.createOpId, this.backendLabel);
            if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);
            this._rawDag = rawDag;
        }
        return this._rawDag;
    }

    async getScopedDag(): Promise<ScopedDag> {
        if (this._scopedDag === undefined) this._scopedDag = new RootScopedDag(await this.rawDag());
        return this._scopedDag;
    }

    async getCausalDag(): Promise<CausalDag> {
        return this.rawDag();
    }

    async destroy(): Promise<void> {
        this._scopedDag = undefined;
        this._rawDag = undefined;
    }
}

// Pass 1 of an upload: the chunk hashes, then the links from the end, and the
// file hash they commit to.
async function hashSource(source: FileSource, label: string) {
    const size = source.size;
    if (!Number.isInteger(size) || size < 0 || size > MAX_FILE_BYTES) throw new Error(`${label}: size ${size} is out of range`);
    const count = chunkCount(size);

    const hashes = new Uint8Array(count * CONTENT_HASH_BYTES);
    let read = 0;
    let index = 0;
    for await (const chunk of rechunk(source.read())) {
        if (index >= count) throw new Error(`${label}: the source is longer than its size`);
        hashes.set(chunkHash(chunk), index * CONTENT_HASH_BYTES);
        read += chunk.length;
        index++;
    }
    if (read !== size) throw new Error(`${label}: read ${read} bytes, expected ${size}`);

    const links = new Uint8Array((count + 1) * CONTENT_HASH_BYTES);
    links.set(new Uint8Array(base64.toArrayBuffer(END_LINK)), count * CONTENT_HASH_BYTES);
    for (let j = count - 1; j >= 0; j--) {
        const h = hashes.subarray(j * CONTENT_HASH_BYTES, (j + 1) * CONTENT_HASH_BYTES);
        const next = links.subarray((j + 1) * CONTENT_HASH_BYTES, (j + 2) * CONTENT_HASH_BYTES);
        links.set(linkHash(h, next), j * CONTENT_HASH_BYTES);
    }
    const linkAt = (j: number): B64Hash => encodeBase64(links.subarray(j * CONTENT_HASH_BYTES, (j + 1) * CONTENT_HASH_BYTES));
    const first = linkAt(0);
    return { fileHash: fileHashOf(size, first), first, hashes, linkAt };
}

// The file hash `source` would be stored under (the upload's first pass).
export async function hashFileSource(source: FileSource): Promise<B64Hash> {
    return (await hashSource(source, 'hashFileSource')).fileHash;
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
    return true;
}

export { RBlobStoreViewImpl } from "./view.js";

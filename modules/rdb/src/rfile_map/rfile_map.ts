// RFileMap: which file contents sit at which path (a standalone RObject in its
// own DAG, paired with an RBlobStore that holds the contents).
//
// ACTIONS (see payload.ts for formats):
//
//   create        name, seed, the access fields of ../rfiles/access.ts, and
//                 the id of the paired blob store
//   ref-advance   a signed, non-barrier observation of the bound group
//   add           an element (section, owner, path, fileHash)
//   remove        a barrier remove of an element (see presence.ts)
//
// RFileMap does not sync on its own: the RDb that computes it as a member
// orchestrates its sync.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256, KeyId, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
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
import { sameVersion } from "../rfiles/walk.js";
import type { RFileMap as RFileMapContract, RFileMapView, ListedFile, FileMapDelta } from "./interfaces.js";
import { CreateFileMapPayload, FileElement, FileMapElementPayload, RFILE_MAP_TYPE_ID, elementOf } from "./payload.js";
import { validateFileMapCreate } from "./validate.js";
import { validateFileMapOp } from "./validate_ops.js";
import { FileMapDeltaAccumulator, computeFileMapDelta } from "./delta.js";
import { elementMeta, listPresent } from "./presence.js";
import { RFileMapViewImpl } from "./view.js";

export { RFILE_MAP_TYPE_ID } from "./payload.js";

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

export const rFileMapFactory: RObjectFactory = {

    computeRootObjectId: async (payload: Payload, ctx: RContext) => {
        return dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256)).hash;
    },

    validateCreationPayload: async (payload: Payload) => validateFileMapCreate(payload),

    executeCreationPayload: async (payload: Payload, _ctx: RContext, scopedDag: ScopedDag) => {
        return await scopedDag.append(payload, {}, position());
    },

    loadObject: async (id: B64Hash, ctx: RContext, opts?: LoadObjectOptions) => {
        if (opts?.parent !== undefined) throw new Error("RFileMap is a standalone object (no nesting parent)");
        const backendLabel = opts?.backendLabel ?? 'default';
        const rawDag = await ctx.getDag(id, backendLabel);
        if (rawDag === undefined) throw new Error(`DAG '${id}' not found`);
        const createOp = (await rawDag.loadEntry(id))!.payload as unknown as CreateFileMapPayload;
        return new RFileMapImpl(id, createOp, ctx, backendLabel);
    },

    extractCreationForeignDeps: async (payload: Payload) => {
        const groupId = boundGroupOf(payload);
        return groupId === undefined ? undefined : [{ objectId: groupId, requiredHashes: [] }];
    },
};

export class RFileMapImpl implements RFileMapContract {

    static create = (options: {
        name: string;
        seed: string;
        access: FilesAccess;
        blobStore: B64Hash;
    }): CreateFileMapPayload => ({
        action: 'create',
        type: RFILE_MAP_TYPE_ID,
        name: options.name,
        seed: options.seed,
        bindings: options.access.bindings,
        idProvider: options.access.idProvider,
        canWrite: options.access.canWrite,
        blobStore: options.blobStore,
    });

    static typeId = RFILE_MAP_TYPE_ID;

    readonly createOpId: B64Hash;
    readonly createOp: CreateFileMapPayload;
    readonly access: FilesAccessControl;
    private readonly ctx: RContext;
    private readonly backendLabel: string;

    private _scopedDag: ScopedDag | undefined;
    private _rawDag: dag.Dag | undefined;

    constructor(createOpId: B64Hash, createOp: CreateFileMapPayload, ctx: RContext, backendLabel: string = 'default') {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.ctx = ctx;
        this.backendLabel = backendLabel;
        this.access = new FilesAccessControl(ctx, accessOf(createOp));
    }

    getId(): B64Hash { return this.createOpId; }
    getType(): string { return RFileMapImpl.typeId; }
    getBackendLabel(): string { return this.backendLabel; }
    getName(): string { return this.createOp.name; }
    getGroupId(): B64Hash { return this.access.groupId; }
    getBlobStoreId(): B64Hash { return this.createOp.blobStore; }

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

    // --- Writers ---

    add(element: FileElement, author: OwnIdentity): Promise<B64Hash> {
        return this.writeElement('add', element, author);
    }

    remove(element: FileElement, author: OwnIdentity): Promise<B64Hash> {
        return this.writeElement('remove', element, author);
    }

    async refAdvance(author: OwnIdentity, to?: Version): Promise<B64Hash> {
        const target = to ?? await this.groupFrontier();
        const at = await (await this.getScopedDag()).getFrontier();
        const base = createRefAdvancePayload(this.access.groupId, target) as unknown as json.LiteralMap;
        return this.appendValidated(await signPayload(base, author, at), at);
    }

    private async writeElement(action: 'add' | 'remove', element: FileElement, author: OwnIdentity): Promise<B64Hash> {
        const at = await this.observeGroup(author);
        const base: json.LiteralMap = { action, ...elementOf(element) };
        return this.appendValidated(await signPayload(base, author, at), at);
    }

    // The position for a new op: the frontier, after a ref-advance when the
    // bound group has moved past the version observed there.
    private async observeGroup(author: OwnIdentity): Promise<Version> {
        const scopedDag = await this.getScopedDag();
        const frontier = await scopedDag.getFrontier();
        const groupFrontier = await this.groupFrontier();
        const observed = await this.access.observedVersion(scopedDag, frontier);
        if (sameVersion(observed, groupFrontier)) return frontier;
        return version(await this.refAdvance(author, groupFrontier));
    }

    private async groupFrontier(): Promise<Version> {
        return (await (await this.access.loadGroup()).getScopedDag()).getFrontier();
    }

    // --- Readers ---

    async list(at?: Version): Promise<ListedFile[]> {
        const v = at ?? await (await this.getScopedDag()).getFrontier();
        return listPresent(await this.rawDag(), v, v);
    }

    async authorKey(author: KeyId, at?: Version): Promise<PublicKey | undefined> {
        const scopedDag = await this.getScopedDag();
        const observed = await this.access.observedVersion(scopedDag, at ?? await scopedDag.getFrontier());
        return this.access.authorKey(author, observed);
    }

    async canWrite(author: KeyId, at?: Version): Promise<boolean> {
        const scopedDag = await this.getScopedDag();
        const observed = await this.access.observedVersion(scopedDag, at ?? await scopedDag.getFrontier());
        if (await this.access.authorKey(author, observed) === undefined) return false;
        return this.access.canWriteAt(author, observed);
    }

    async authorKeyNow(author: KeyId): Promise<PublicKey | undefined> {
        return this.access.authorKey(author, await this.groupFrontier());
    }

    async canWriteNow(author: KeyId): Promise<boolean> {
        const v = await this.groupFrontier();
        if (await this.access.authorKey(author, v) === undefined) return false;
        return this.access.canWriteAt(author, v);
    }

    // --- RObject interface ---

    async validatePayload(payload: Payload, at: Version): Promise<ValidationResult> {
        return validateFileMapOp(payload, at, await this.getScopedDag(), this.access);
    }

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {
        return (await this.getScopedDag()).append(payload, this.metaFor(payload), at);
    }

    private metaFor(payload: Payload): MetaProps {
        if (!isObject(payload)) throw new Error('RFileMap payload must be an object');
        switch (payload['action']) {
            case 'ref-advance':
                return createRefAdvanceMeta(payload['refId'] as B64Hash, { barrier: false });
            case 'add':
            case 'remove':
                return elementMeta(payload as unknown as FileMapElementPayload);
            default:
                throw new Error(`unknown RFileMap action '${String(payload['action'])}'`);
        }
    }

    async getView(at?: Version, from?: Version): Promise<RFileMapView> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();
        from = from ?? await scopedDag.getFrontier();
        return new RFileMapViewImpl(this, await this.rawDag(), at, from);
    }

    async computeDelta(start: Version, end: Version): Promise<FileMapDelta> {
        return computeFileMapDelta(await this.rawDag(), start, end, this.createDeltaAccumulator(start, end));
    }

    createDeltaAccumulator(start: Version, end: Version): FileMapDeltaAccumulator {
        return new FileMapDeltaAccumulator(() => this.rawDag(), start, end);
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

export { RFileMapViewImpl } from "./view.js";

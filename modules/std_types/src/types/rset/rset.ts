

// A Replicable Set that implements Monotonic Views.

// This class implements the RObject interface, and can be used as a top-level object in a Replica, or as
// a nested element inside other DAG-based RObjects.

// The set supports two modes of operation: if contentType is missing, the set is a simple set of literals.
// If contentType is present, the set works as a container for nested DAG-based RObjects. In this case, each
// time an element is added, its contents are used as the payload for the creation operation of the nested RObject's
// type. A new instance of the nested RObject is created with each Add, and its nested DAG is rooted at the position
// in the DAG where the Add operation is inserted. The returned hash for the element is the hash of the Add 
// operation, that has the creation op's payload as its content. Therefore there is no aliasing of contents: if the
// same creation payload is passed to two Add operations rooted in different DAG positions, two different nested
// elements will be created (with identical initialization params, since they have the same creation payload).
// Since the position affects the hash of the Add entry in the DAG, their hashes will be different.

// This type uses a sub-dag abstraction. When an element is added in nested mode, a sub-dag is created that will
// automatically wrap any updates to the nested type into RSet update operations, and will also automatically wrap
// any metadata and filtering parameters. When an element is read, the payload is automatically unwrapped.

// Notice that this wrapping process is not 100% transparent: some operations (like fork poistion finding) will
// return DAG entries from the outer, full DAG. So types that support being inserted as nested elements must be
// aware of this caveats.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import type { KeyId, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { dag, MetaProps, position, EntryMetaFilter, Position, MetaContainsValues } from "@hyper-hyper-space/hhs3_dag";

import { Payload, RObject, RObjectFactory, RObjectInit, RContext, NestingParent, Version, ForeignDep, LoadObjectOptions } from "@hyper-hyper-space/hhs3_mvt";
import { DagScope, NestedScopedDag, RootScopedDag, ScopedDag, CausalDag } from "@hyper-hyper-space/hhs3_mvt";
import { isRefAdvancePayload, extractRefVersion, prepareRefAdvance, createRefAdvanceMeta } from "@hyper-hyper-space/hhs3_mvt";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";
import { set } from "@hyper-hyper-space/hhs3_util";

import type { Mesh, Swarm } from "@hyper-hyper-space/hhs3_mesh";
import { createSyncSession } from "@hyper-hyper-space/hhs3_sync";
import type { SyncSession, SyncTarget } from "@hyper-hyper-space/hhs3_sync";

import { signPayload as signPayloadHelper } from "../../authorship.js";
import { RCap } from "../rcap/rcap.js";
import type { RSet as RSetContract, RSetView as RSetViewContract } from "./interfaces.js";
import { validateRSetPayload } from "./validate.js";
import { hashElement } from "./hash.js";
import { RSetViewImpl } from "./view.js";
import { RSetDelta, RSetDeltaStrategy, RSetDeltaAccumulator, computeRSetDelta } from "./delta.js";

import { RAddEvent, RDeleteEvent, RSetEvent } from "./events.js";

import { CreateSetPayload } from "./payload.js";
import { AddElmtPayload } from "./payload.js";
import { DeleteElmtPayload } from "./payload.js";
import { UpdateElmtPayload } from "./payload.js";

import { SetPayload } from "./payload.js";

export const rSetFactory: RObjectFactory = {

    computeRootObjectId: async (payload: json.Literal, ctx: RContext, _parent?: NestingParent) => {

        const entry = dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256));
        return entry.hash;
    },

    validateCreationPayload: async (payload: json.Literal, _ctx: RContext, parent?: NestingParent) =>
        validateRSetPayload(payload, { mode: 'create', parent }),

    executeCreationPayload: async (payload: json.Literal, _ctx: RContext, scopedDag: ScopedDag) => {

        const meta: MetaProps = {};
        const createPayload = payload as CreateSetPayload;

        if (createPayload['contentType'] === undefined) {
            meta['elmts'] = json.toSet(await Promise.all(createPayload['initialElements'].map(async (e: json.Literal) => hashElement(e))));
        }
        
        return await scopedDag.append(createPayload, meta, position());
    },

    loadObject: async (id: B64Hash, ctx: RContext, opts?: LoadObjectOptions) => {

        let scopedDag: ScopedDag;
        const parent = opts?.parent;

        if (parent !== undefined) {
            scopedDag = await parent.getScopedDagForChild(id);
        } else {
            const backendLabel = opts?.backendLabel ?? 'default';
            const rawDag = await ctx.getDag(id, backendLabel);
            if (rawDag === undefined) throw new Error(`DAG '${id}' not found`);
            scopedDag = new RootScopedDag(rawDag);
        }

        const createOp = (await scopedDag.loadEntry(id))!.payload as CreateSetPayload;
        return new RSetImpl(id, createOp, ctx, parent, opts?.backendLabel);
    }

}

export type RSetOptions = {
    seed: string;
    contentType?: string;
    initialElements: Array<json.Literal>;
    acceptRedundantAdd?: boolean;
    acceptRedundantDelete?: boolean;
    acceptRedundantUpdate?: boolean;
    supportBarrierAdd?: boolean;
    supportBarrierDelete?: boolean;
    hashAlgorithm?: string;
    parent?: B64Hash;
    capabilityRef?: B64Hash;
    capRequirements?: { add?: string; delete?: string; refAdvance?: string[]; refAdvanceCreators?: boolean };
}

export type RSetRuntimeConfig = {
    meshLabel?: string;
}

export class RSetImpl<T extends json.Literal = json.Literal> implements RSetContract<T> {

    static create = async (options: RSetOptions) => {

        if (options.initialElements != undefined &&
            options.initialElements.length > 0 &&
            options.contentType !== undefined) {

            throw new Error("Sets of RObjects cannot have initial elements");
        }

        if (options.contentType !== undefined &&
            options.acceptRedundantAdd !== undefined) {

            throw new Error("acceptRedundantAdd cannot be set if contentType is present");
            
        }

        const createPayload: CreateSetPayload = {
            action: 'create',
            seed: options.seed,
            initialElements: options.initialElements || [],
            hashAlgorithm: options.hashAlgorithm || 'sha256',
            acceptRedundantDelete: options.acceptRedundantDelete || false,
        };


        if (options.acceptRedundantAdd !== undefined) {
            createPayload.acceptRedundantAdd = options.acceptRedundantAdd;
        }

        if (options.supportBarrierAdd && options.supportBarrierDelete) {
            throw new Error("supportBarrierAdd and supportBarrierDelete are mutually exclusive");
        }

        if (options.supportBarrierAdd !== undefined) {
            createPayload.supportBarrierAdd = options.supportBarrierAdd;
        }

        if (options.supportBarrierDelete !== undefined) {
            createPayload.supportBarrierDelete = options.supportBarrierDelete;
        }

        if (options.contentType !== undefined) {
            createPayload['contentType'] = options.contentType;
            createPayload['acceptUpdateForDeleted'] = options.acceptRedundantUpdate || false;
        }

        if (options.parent !== undefined) {
            createPayload['parent'] = options.parent;
        }

        if (options.capabilityRef !== undefined) {
            createPayload['capabilityRef'] = options.capabilityRef;
            createPayload['capRequirements'] = options.capRequirements;
        }

        return {type: RSetImpl.typeId, payload: createPayload} as RObjectInit;
    }

    static typeId = "hhs/set_v1";

    createOpId: B64Hash;
    createOp: CreateSetPayload;
    private ctx: RContext;
    private parentObj: NestingParent | undefined;

    private _scopedDag: ScopedDag | undefined;
    private _causalDag: CausalDag | undefined;
    private _swarm: Swarm | undefined;
    private _syncSession: SyncSession | undefined;
    private readonly backendLabel: string | undefined;
    private meshConfig: RSetRuntimeConfig = { meshLabel: 'default' };

    constructor(
        createOpId: B64Hash,
        createOp: CreateSetPayload,
        ctx: RContext,
        parent?: NestingParent,
        backendLabel?: string,
    ) {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.ctx = ctx;
        this.parentObj = parent;
        this.backendLabel = backendLabel;
    }

    getBackendLabel(): string {
        if (this.parentObj !== undefined) {
            return this.parentObj.getBackendLabel();
        }
        return this.backendLabel ?? 'default';
    }

    getId(): string {
        return this.createOpId;
    }

    getType(): string {
        return RSetImpl.typeId;
    }

    // Set operations

    async add(element: T, at?: Version): Promise<B64Hash> {
        if (this.isPermissioned()) throw new Error("Use addSigned() for permissioned sets");
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        const payload = await this.createAddPayload(element, false, at);

        return this.applyValidatedPayload(payload, at);
    }
    
    async addWithBarrier(element: T, at?: Version): Promise<B64Hash> {
        if (this.isPermissioned()) throw new Error("Use addWithBarrierSigned() for permissioned sets");
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        const payload = await this.createAddPayload(element, true, at);
        return this.applyValidatedPayload(payload, at);
    }

    private async createAddPayload(element: T, barrier: boolean, at: Version): Promise<AddElmtPayload> {

        if (this.contentType() === undefined) {
            if (!this.acceptRedundantAdd()) {
                const view = await this.getView(at);
                const hasElmt = await view.has(element);
                if (hasElmt) {
                    throw new Error("Element already exists in set, and redundant adds are not accepted");
                }            
            }
        }

        if (!this.supportBarrierAdd() && barrier) {
            throw new Error("Barrier add is not supported by this set");
        }

        if (this.supportBarrierAdd()) {
            return {action: 'add', element, barrier};
        } else {
            return {action: 'add', element};
        }        
    }

    async delete(element: T, at?: Version): Promise<B64Hash> {
        if (this.isPermissioned()) throw new Error("Use deleteSigned() for permissioned sets");

        if (this.contentType() !== undefined) {
            throw new Error("RSet.delete(element) is not well defined when contentType is present, please use deleteByHash");
        }

        const elementHash = await hashElement(element);
        return this.deleteByHash(elementHash, at);
    }

    async deleteByHash(elementHash: B64Hash, at?: Version): Promise<B64Hash> {
        if (this.isPermissioned()) throw new Error("Use deleteByHashSigned() for permissioned sets");
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        const payload: DeleteElmtPayload = await this.createDeletePayload(elementHash, false, at);
        return this.applyValidatedPayload(payload, at);
        
    }

    async deleteWithBarrier(element: T, at?: Version): Promise<B64Hash> {
        if (this.isPermissioned()) throw new Error("Use deleteWithBarrierSigned() for permissioned sets");
        const elementHash = await hashElement(element);
        return this.deleteWithBarrierByHash(elementHash, at);
    }

    async deleteWithBarrierByHash(elementHash: B64Hash, at?: Version): Promise<B64Hash> {
        if (this.isPermissioned()) throw new Error("Use deleteWithBarrierByHashSigned() for permissioned sets");
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        const payload: DeleteElmtPayload = await this.createDeletePayload(elementHash, true, at);
        return this.applyValidatedPayload(payload, at);
    }

    // Signed convenience methods for permissioned sets

    async addSigned(element: T, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        const d = await this.getScopedDag();
        at = at || await d.getFrontier();
        const base = await this.createAddPayload(element, false, at);
        const signed = await signPayloadHelper(base as json.LiteralMap, author);
        return this.applyValidatedPayload(signed, at);
    }

    async addWithBarrierSigned(element: T, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        const d = await this.getScopedDag();
        at = at || await d.getFrontier();
        const base = await this.createAddPayload(element, true, at);
        const signed = await signPayloadHelper(base as json.LiteralMap, author);
        return this.applyValidatedPayload(signed, at);
    }

    async deleteSigned(element: T, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        if (this.contentType() !== undefined) {
            throw new Error("Use deleteByHashSigned for sets with contentType");
        }
        const elementHash = await hashElement(element);
        return this.deleteByHashSigned(elementHash, author, at);
    }

    async deleteByHashSigned(elementHash: B64Hash, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        const d = await this.getScopedDag();
        at = at || await d.getFrontier();
        const base = await this.createDeletePayload(elementHash, false, at);
        const signed = await signPayloadHelper(base as json.LiteralMap, author);
        return this.applyValidatedPayload(signed, at);
    }

    async deleteWithBarrierSigned(element: T, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        const elementHash = await hashElement(element);
        return this.deleteWithBarrierByHashSigned(elementHash, author, at);
    }

    async deleteWithBarrierByHashSigned(elementHash: B64Hash, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        const d = await this.getScopedDag();
        at = at || await d.getFrontier();
        const base = await this.createDeletePayload(elementHash, true, at);
        const signed = await signPayloadHelper(base as json.LiteralMap, author);
        return this.applyValidatedPayload(signed, at);
    }

    async refAdvance(refVersion: Version, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        if (!this.isPermissioned()) throw new Error("refAdvance is only for permissioned sets");
        const d = await this.getScopedDag();
        at = at || await d.getFrontier();
        const { payload: base } = prepareRefAdvance(this.capabilityRef()!, refVersion);
        const signed = await signPayloadHelper(base as unknown as json.LiteralMap, author);
        return this.applyValidatedPayload(signed, at);
    }

    private async createDeletePayload(elementHash: B64Hash, barrier: boolean, at: Version): Promise<DeleteElmtPayload> {

        if (!this.acceptRedundantDelete()) {
            const view = await this.getView(at);
            const hasElmt = await view.hasByHash(elementHash);
            if (!hasElmt) {
                throw new Error("Element does not exist in set, and redundant deletes are not accepted");
            }            
        }

        if (!this.supportBarrierDelete() && barrier) {
            throw new Error("Barrier delete is not supported by this set");
        }

        if (this.supportBarrierDelete()) {
            return {action: 'delete', elementHash, barrier};
        } else {
            return {action: 'delete', elementHash};
        }        
    }

    private async applyValidatedPayload(payload: Payload, at: Version): Promise<B64Hash> {
        if (this.selfValidate() && !await this.validatePayload(payload, at)) {
            throw new Error("Attempted to apply an invalid payload");
        }

        return this.applyPayload(payload, at);
    }

    // RObject interface

    async validatePayload(payload: json.Literal, at: Version): Promise<boolean> {
        return validateRSetPayload(payload, { mode: 'op', set: this, at });
    }

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {

        if (isRefAdvancePayload(payload)) {
            const refPayload = payload as unknown as RefAdvancePayload;
            const meta = createRefAdvanceMeta(refPayload.refId);
            const scopedDag = await this.getScopedDag();
            return await scopedDag.append(payload, meta, at);
        }

        const setPayload = payload as unknown as SetPayload;
        
        if (setPayload['action'] === 'update') {
            const innerFactory = await this.ctx.getRegistry().lookup(this.contentType()!);
            const innerRObject = await this.loadChildObject(innerFactory, setPayload['elementHash']);
            return await innerRObject.applyPayload(setPayload['updatePayload'], at);
        } else {
            const meta: MetaProps = {};

            switch (setPayload['action']) {
                case 'create':
                    throw new Error("Create operation is not supposed to be applied _to_ a set");
                case 'add':
                    if (this.contentType() !== undefined) {
                        const scopedDag = await this.getScopedDag();
                        const elementHash = await scopedDag.computeEntryHash(setPayload, at);
                        const creationDag = await this.getCreationDagForChild(elementHash, at, setPayload);
                        const innerFactory = await this.ctx.getRegistry().lookup(this.contentType()!);
                        await innerFactory.executeCreationPayload(setPayload['element'], this.ctx, creationDag);
                        return elementHash;

                    } else {
                        addMetaPropsForSetOp(setPayload, meta, await hashElement(setPayload['element']));
                    }    
                
                    break;
                case 'delete':
                    addMetaPropsForSetOp(setPayload, meta, setPayload['elementHash']);
                    break;
                default:
                    throw new Error("Invalid set action in payload: " + setPayload['action']);
            }
            
            const scopedDag = await this.getScopedDag();
    
            return await scopedDag.append(payload, meta, at);
        }
        
        
    }

    async getView(at?: Version, from?: Version): Promise<RSetViewContract<T>> {
        
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        from = from || await dag.getFrontier();


        return new RSetViewImpl<T>(this, at, from);
    }

    getContext(): RContext {
        return this.ctx;
    }

    configure(config: RSetRuntimeConfig): void {
        this.meshConfig = { ...this.meshConfig, ...config };
    }

    async loadChildObject(innerFactory: RObjectFactory, elementHash: B64Hash): Promise<RObject> {
        return innerFactory.loadObject(elementHash, this.ctx, { parent: this });
    }

    seed(): string {
        return this.createOp['seed'];
    }

    contentType(): string | undefined {
        return this.createOp['contentType'];
    }
    
    acceptRedundantAdd(): boolean {
        return this.createOp['acceptRedundantAdd'] ?? true;
    }

    acceptRedundantDelete(): boolean {
        return this.createOp['acceptRedundantDelete'];
    }

    acceptUpdateForDeleted(): boolean {
        return this.createOp['acceptUpdateForDeleted'] || false;
    }

    supportBarrierAdd(): boolean {
        return this.createOp['supportBarrierAdd'] || false;
    }

    supportBarrierDelete(): boolean {
        return this.createOp['supportBarrierDelete'] || false;
    }

    isPermissioned(): boolean {
        return this.createOp['capabilityRef'] !== undefined;
    }

    capabilityRef(): B64Hash | undefined {
        return this.createOp['capabilityRef'];
    }

    capRequirementForAdd(): string | undefined {
        return this.createOp['capRequirements']?.['add'];
    }

    capRequirementForDelete(): string | undefined {
        return this.createOp['capRequirements']?.['delete'];
    }

    refAdvanceCaps(): string[] {
        return this.createOp['capRequirements']?.['refAdvance'] ?? [];
    }

    refAdvanceCreators(): boolean {
        return this.createOp['capRequirements']?.['refAdvanceCreators'] ?? true;
    }

    selfValidate(): boolean {
        return this.ctx.getConfig().selfValidate || false;
    }

    extractForeignDeps(payload: Payload, _at: Version): ForeignDep[] | undefined {
        const ref = this.capabilityRef();
        if (ref === undefined) return undefined;

        if (isRefAdvancePayload(payload)) {
            const refPayload = payload as RefAdvancePayload;
            if (refPayload.refId !== ref) return undefined;
            return [{
                dagId: ref,
                requiredHashes: [...extractRefVersion(refPayload)],
            }];
        }

        return [{ dagId: ref, requiredHashes: [] }];
    }

    async loadRCap(): Promise<RCap | undefined> {
        const ref = this.capabilityRef();
        if (ref === undefined) return undefined;
        const obj = await this.ctx.getObject(ref);
        if (obj === undefined) return undefined;
        return obj as RCap;
    }

    subscribe(callback: (event: RAddEvent | RDeleteEvent) => void): void {
        throw new Error("Method not implemented.");
    }

    unsubscribe(callback: (event: RAddEvent | RDeleteEvent) => void): void {
        throw new Error("Method not implemented.");
    }

    private deltaStrategy: RSetDeltaStrategy = 'bounded';

    setDeltaStrategy(strategy: RSetDeltaStrategy): void {
        this.deltaStrategy = strategy;
    }

    async computeDelta(start: Version, end: Version): Promise<RSetDelta> {
        // computeDelta is root-only orchestration (bounds analysis + walk + compose). A
        // nested set does not lead a delta; it participates via createDeltaAccumulator.
        if (this.parentObj !== undefined) {
            throw new Error("computeDelta is not supported on nested RSet");
        }
        const rawDag = await this.ctx.getDag(this.createOpId, this.getBackendLabel());
        if (rawDag === undefined) throw new Error("DAG not found");
        return computeRSetDelta(this, rawDag, this.deltaStrategy, start, end);
    }

    createDeltaAccumulator(start: Version, end: Version): RSetDeltaAccumulator {
        return new RSetDeltaAccumulator(this, start, end);
    }

    async getScopedDag(): Promise<ScopedDag> {
        if (this._scopedDag === undefined) {
            if (this.parentObj !== undefined) {
                this._scopedDag = await this.parentObj.getScopedDagForChild(this.createOpId);
            } else {
                const rawDag = await this.ctx.getDag(this.createOpId, this.getBackendLabel());
                if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);
                this._scopedDag = new RootScopedDag(rawDag);
            }
        }
        return this._scopedDag;
    }

    async getCausalDag(): Promise<CausalDag> {
        if (this._causalDag === undefined) {
            if (this.parentObj !== undefined) {
                this._causalDag = await this.parentObj.getCausalDag();
            } else {
                const rawDag = await this.ctx.getDag(this.createOpId, this.getBackendLabel());
                if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);
                this._causalDag = rawDag;
            }
        }
        return this._causalDag;
    }

    // NestingParent interface

    async getScopedDagForChild(childId: B64Hash): Promise<ScopedDag> {
        const parentScopedDag = await this.getScopedDag();
        const scope = new ElementUpdateScope(this, childId, position(childId));
        return new NestedScopedDag(parentScopedDag, scope);
    }

    async getCreationDagForChild(childId: B64Hash, at: Version, addPayload: Payload): Promise<ScopedDag> {
        const parentScopedDag = await this.getScopedDag();
        const scope = new ElementAddScope(this, childId, at, addPayload as AddElmtPayload);
        return new NestedScopedDag(parentScopedDag, scope);
    }

    // SyncableObject interface

    async startSync(): Promise<void> {
        if (this.parentObj !== undefined) return;
        if (this._syncSession !== undefined) return;

        const mesh = this.ctx.getMesh(this.meshConfig.meshLabel ?? 'default') as Mesh;
        this._swarm = mesh.createSwarm(this.createOpId);

        const rawDag = await this.ctx.getDag(this.createOpId, this.getBackendLabel());
        if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);

        const target: SyncTarget = {
            dagId: this.createOpId,
            dag: rawDag,
            rObject: this,
            hashSuite: this.ctx.getHashSuite(),
            resolveRefDag: async (refId) => {
                const label = await this.ctx.getBackendLabel(refId);
                return this.ctx.getDag(refId, label);
            },
        };

        this._syncSession = createSyncSession(target, [this._swarm]);
        this._swarm.activate();
    }

    async stopSync(): Promise<void> {
        if (this.parentObj !== undefined) return;

        if (this._syncSession !== undefined) {
            this._syncSession.destroy();
            this._syncSession = undefined;
        }

        if (this._swarm !== undefined) {
            this._swarm.destroy();
            this._swarm = undefined;
        }
    }

    async destroy(): Promise<void> {
        await this.stopSync();
        this._scopedDag = undefined;
        this._causalDag = undefined;
    }
}

const addMetaPropsForSetOp = (payload: SetPayload, meta: MetaProps, elmtHash: B64Hash): void => {
    const elmts = json.toSet([elmtHash]);
    meta['elmts'] = elmts;
    if (payload['action'] === 'add' || payload['action'] === 'delete') {
        if (payload['barrier'] || false) {
            meta['barrier'] = json.toSet(['t']);
        }
    }
}

class NestedElementScope implements DagScope {

    private parent: RSetImpl;
    private elementHash: B64Hash;
    private start: Position;
    private executeAddOp?: AddElmtPayload;

    constructor(parent: RSetImpl, elementHash: B64Hash, startAt: Position, executeAddOp?: AddElmtPayload) {
        this.parent = parent;
        this.start = startAt;
        this.elementHash = elementHash;
        this.executeAddOp = executeAddOp;
    }

    startAt(): Position {
        return this.start;
    }

    startEmpty(): boolean {
        return this.executeAddOp !== undefined;
    }

    baseFilter(): EntryMetaFilter {
        return {containsValues: {"inner-elmts": [this.elementHash]}};
    }

    wrapPayload(payload: json.Literal, at: Position): json.Literal {


        if (this.executeAddOp !== undefined && set.eq(at, this.start)) {

            if (json.toStringNormalized(this.executeAddOp['element']) !== json.toStringNormalized(payload)) {
                throw new Error("An inner element in an RSet has modified its creation payload - this is not supported.");
            }

            return this.executeAddOp;
        } else {
            const outerPayload: UpdateElmtPayload = {
                action: 'update',
                elementHash: this.elementHash,
                updatePayload: payload,
            }

            return outerPayload;
        }
    }

    unwrapPayload(payload: json.Literal): json.Literal {
        
        const outerPayload = payload as SetPayload;

        if (outerPayload['action'] === 'update') {
            return outerPayload['updatePayload'];
        } else if (outerPayload['action'] === 'add') {
            return outerPayload['element'];
        } else {
            throw new Error("Invalid payload type in unwrapPayload for RSet: " + outerPayload['action']);
        }
    }

    wrapMeta(innerMeta: MetaProps, wrappedPayload: json.Literal, at: Position): MetaProps {

        const setPayload = wrappedPayload as SetPayload;

        const outerMeta: MetaProps = {};

        if (setPayload['action'] === 'add') {

            addMetaPropsForSetOp(setPayload, outerMeta, this.elementHash);

        }

        outerMeta["inner-elmts"] = json.toSet([this.elementHash]);

        for (const key in innerMeta) {
            outerMeta["inner-" + this.elementHash + "-" + key] = innerMeta[key];
        }

        return outerMeta;
    }

    unwrapMeta(outerMeta: MetaProps, wrappedPayload: json.Literal, at: Position): MetaProps {
        
        const innerMeta: MetaProps = {};

        const lengthToSkip = ("inner-" + this.elementHash + "-").length;
        for (const key in outerMeta) {
            if (key.startsWith("inner-" + this.elementHash + "-")) {
                innerMeta[key.substring(lengthToSkip)] = outerMeta[key];
            }
        }

        return innerMeta;
    }

    wrapFilter(filter: EntryMetaFilter): EntryMetaFilter {
        
        const wrappedFilter: EntryMetaFilter = {};

        if (filter.containsKeys !== undefined) {
            wrappedFilter.containsKeys = filter.containsKeys.map(key => "inner-" + this.elementHash + "-" + key);
        }

        if (filter.containsValues !== undefined) {
            
            wrappedFilter.containsValues = {} as MetaContainsValues;
            
            for (const key in filter.containsValues) {
                wrappedFilter.containsValues['inner-' + this.elementHash + '-' + key] = filter.containsValues[key];    
            }
        }
        
        return wrappedFilter;
    }

    async validateWrappedPayload(wrappedPayload: json.Literal, _wrappedMeta: MetaProps, at: Position): Promise<boolean> {
        if (!this.parent.selfValidate()) {
            return true;
        }

        return this.parent.validatePayload(wrappedPayload, at);
    }
}

class ElementAddScope extends NestedElementScope implements DagScope {

    constructor(parent: RSetImpl, elementHash: B64Hash, startAt: Position, executeAddOp: AddElmtPayload) {
        super(parent, elementHash, startAt, executeAddOp);
    }
}

class ElementUpdateScope extends NestedElementScope implements DagScope {

    constructor(parent: RSetImpl, elementHash: B64Hash, startAt: Position) {
        super(parent, elementHash, startAt);
    }
}

export { RSetViewImpl } from "./view.js";
export * from "./delta.js";

export interface RSet<T extends json.Literal = json.Literal> extends RSetContract<T> {}
export interface RSetView<T extends json.Literal = json.Literal> extends RSetViewContract<T> {}
export const RSet = RSetImpl;
export const RSetView = RSetViewImpl;

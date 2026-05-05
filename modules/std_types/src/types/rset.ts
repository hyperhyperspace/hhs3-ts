

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
import { B64Hash, HASH_SHA256, sha256, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";
import { dag, MetaProps, position, EntryMetaFilter, Position, MetaContainsValues } from "@hyper-hyper-space/hhs3_dag";

import { Payload, RObject, RObjectFactory, RObjectInit, RContext, RObjectConfig, SyncableObject, NestingParent, version, Version, View } from "@hyper-hyper-space/hhs3_mvt";
import { DagScope, NestedScopedDag, RootScopedDag, ScopedDag, CausalDag } from "@hyper-hyper-space/hhs3_mvt";
import { set } from "@hyper-hyper-space/hhs3_util";

import type { Mesh, Swarm } from "@hyper-hyper-space/hhs3_mesh";
import { createSyncSession } from "@hyper-hyper-space/hhs3_sync";
import type { SyncSession, SyncTarget } from "@hyper-hyper-space/hhs3_sync";

import { RAddEvent, RDeleteEvent, RSetEvent } from "./rset/events.js";

import { createSetFormat, CreateSetPayload } from "./rset/payload.js";
import { addElmtFormat, AddElmtPayload } from "./rset/payload.js";
import { deleteElmtFormat, DeleteElmtPayload } from "./rset/payload.js";
import { updateElmtFormat, UpdateElmtPayload } from "./rset/payload.js";

import { SetPayload } from "./rset/payload.js";

export const rSetFactory: RObjectFactory = {

    computeRootObjectId: async (payload: json.Literal, ctx: RContext, _parent?: NestingParent) => {

        const entry = dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256));
        return entry.hash;
    },

    validateCreationPayload: async (payload: json.Literal, _ctx: RContext, parent?: NestingParent) => {
        
        if (!json.checkFormat(createSetFormat, payload)) {
            console.log('fmt')
            console.log(payload)
            return false;
        }
            
        const createPayload = payload as CreateSetPayload;

        if (createPayload['parent'] !== undefined && parent !== undefined) {
            if (createPayload['parent'] !== parent.getId()) {
                return false;
            }
        }
        
        if (createPayload['contentType'] === undefined && createPayload['acceptUpdateForDeleted'] !== undefined) {
            console.log('acceptUpdateForDeleted only makes sense if contentType is present')
            return false;
        }

        if (createPayload['contentType'] !== undefined && createPayload['acceptRedundantAdd'] !== undefined) {
            console.log('acceptRedundantAdd only makes sense if contentType is not present')
            return false;
        }

        if (createPayload['contentType'] !== undefined) {
            if (createPayload['initialElements'].length > 0) {
                console.log('initialElements must be empty if contentType is present')
                return false;
            }
        }
        
        return true;
    },

    executeCreationPayload: async (payload: json.Literal, _ctx: RContext, scopedDag: ScopedDag) => {

        const meta: MetaProps = {};
        const createPayload = payload as CreateSetPayload;

        if (createPayload['contentType'] === undefined) {
            meta['elmts'] = json.toSet(await Promise.all(createPayload['initialElements'].map(async (e: json.Literal) => hashElement(e))));
        }
        
        return await scopedDag.append(createPayload, meta, position());
    },

    loadObject: async (id: B64Hash, ctx: RContext, parent?: NestingParent) => {

        let scopedDag: ScopedDag;

        if (parent !== undefined) {
            scopedDag = await parent.getScopedDagForChild(id);
        } else {
            const rawDag = await ctx.getDag(id);
            scopedDag = new RootScopedDag(rawDag);
        }

        const createOp = (await scopedDag.loadEntry(id))!.payload as CreateSetPayload;
        return new RSet(id, createOp, ctx, parent);
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
}

export type RSetRuntimeConfig = {
    meshLabel?: string;
    backendLabel?: string;
}

export class RSet<T extends json.Literal = json.Literal> implements RObject, SyncableObject, NestingParent {

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

        return {type: RSet.typeId, payload: createPayload} as RObjectInit;
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
    private runtimeConfig: RSetRuntimeConfig = { meshLabel: 'default', backendLabel: 'default' };

    constructor(createOpId: B64Hash, createOp: CreateSetPayload, ctx: RContext, parent?: NestingParent) {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.ctx = ctx;
        this.parentObj = parent;
    }

    getId(): string {
        return this.createOpId;
    }

    getType(): string {
        return RSet.typeId;
    }

    // Set operations

    async add(element: T, at?: Version): Promise<B64Hash> {
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        const payload = await this.createAddPayload(element, false, at);

        return this.applyValidatedPayload(payload, at);
    }
    
    async addWithBarrier(element: T, at?: Version): Promise<B64Hash> {
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

        if (this.contentType() !== undefined) {
            throw new Error("RSet.delete(element) is not well defined when contentType is present, please use deleteByHash");
        }

        const elementHash = await hashElement(element);
        return this.deleteByHash(elementHash, at);
    }

    async deleteByHash(elementHash: B64Hash, at?: Version): Promise<B64Hash> {
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        const payload: DeleteElmtPayload = await this.createDeletePayload(elementHash, false, at);
        return this.applyValidatedPayload(payload, at);
        
    }

    async deleteWithBarrier(element: T, at?: Version): Promise<B64Hash> {
        const elementHash = await hashElement(element);
        return this.deleteWithBarrierByHash(elementHash, at);
    }

    async deleteWithBarrierByHash(elementHash: B64Hash, at?: Version): Promise<B64Hash> {
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        const payload: DeleteElmtPayload = await this.createDeletePayload(elementHash, true, at);
        return this.applyValidatedPayload(payload, at);
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
        if (!this.selfValidate()) {
            return this.applyPayload(payload, at);
        }

        if (!await this.validatePayload(payload, at)) {
            throw new Error("Attempted to apply an invalid payload");
        }

        return this.applyPayload(payload, at);
    }

    // RObject interface

    async validatePayload(payload: json.Literal, at: Version): Promise<boolean> {

        if (typeof(payload) !== 'object' || Array.isArray(payload)) {
            return false;
        }

        if (typeof(payload['action']) !== 'string') {
            return false;
        }

        const action = payload['action'];

        let valid = false;

        switch (action) {
            case 'add':
                valid = await this.validateAddElmtPayload(payload, at);
                break;
            case 'delete':
                valid = await this.validateDeleteElmtPayload(payload, at);
                break;
            case 'update':
                valid = await this.validateUpdateElmtPayload(payload, at);
                break;
        }

        return valid;
    }

    private async validateAddElmtPayload(payload: Payload, at: Version): Promise<boolean> {
        if (!json.checkFormat(addElmtFormat, payload)) {
            return false;
        } else {
            const addPayload = payload as AddElmtPayload;
            if (!this.acceptRedundantAdd()) {
                const view = await this.getView(at, at);
                const hasElmt = await view.hasByHash(await hashElement(addPayload['element']));
                if (hasElmt) {
                    return false;
                }
                return true;
            }

            if (!this.supportBarrierAdd() && json.hasKey(addPayload, 'barrier')) {
                return false;
            }

            return true;
        }
    }

    private async validateDeleteElmtPayload(payload: Payload, at: Version): Promise<boolean> {
        if (!json.checkFormat(deleteElmtFormat, payload)) {
            return false;
        } else {
            const deletePayload = payload as DeleteElmtPayload;
            if (!this.acceptRedundantDelete()) {
                const view = await this.getView(at, at);
                const hasElmt = await view.hasByHash(deletePayload['elementHash']);
                if (!hasElmt) {
                    return false;
                }
                return true;
            }

            if (!this.supportBarrierDelete() && json.hasKey(deletePayload, 'barrier')) {
                return false;
            }

            return true;
        }
    }

    private async validateUpdateElmtPayload(payload: Payload, at: Version): Promise<boolean> {
        
        if (!json.checkFormat(updateElmtFormat, payload)) {
            return false;
        } else {

            if (this.contentType() == undefined) {
                return false;
            }

            const updatePayload = payload as UpdateElmtPayload;
            
            if (!this.acceptUpdateForDeleted()) {
                const view = await this.getView(at, at);
                const hasElmt = await view.hasByHash(updatePayload['elementHash']);
                if (!hasElmt) {
                    return false;
                }
            }

            const contentType = this.contentType();

            if (contentType !== undefined) {
                const innerFactory = await this.ctx.getRegistry().lookup(contentType);
                const innerRObject = await this.loadChildObject(innerFactory, updatePayload['elementHash']);
                if (!await innerRObject.validatePayload(updatePayload['updatePayload'], at)) {
                    return false;
                }
            }

            return true;
        }
    } 

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {

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

    async getView(at?: Version, from?: Version): Promise<RSetView<T>> {
        
        const dag = await this.getScopedDag();

        at = at || await dag.getFrontier();
        from = from || await dag.getFrontier();


        return new RSetView<T>(this, at, from);
    }

    getContext(): RContext {
        return this.ctx;
    }

    configure(config: RSetRuntimeConfig): void {
        this.runtimeConfig = { ...this.runtimeConfig, ...config };
        this._scopedDag = undefined;
        this._causalDag = undefined;
    }

    async loadChildObject(innerFactory: RObjectFactory, elementHash: B64Hash): Promise<RObject> {
        return innerFactory.loadObject(elementHash, this.ctx, this);
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

    selfValidate(): boolean {
        return this.ctx.getConfig().selfValidate || false;
    }

    subscribe(callback: (event: RAddEvent | RDeleteEvent) => void): void {
        throw new Error("Method not implemented.");
    }

    unsubscribe(callback: (event: RAddEvent | RDeleteEvent) => void): void {
        throw new Error("Method not implemented.");
    }

    async getScopedDag(): Promise<ScopedDag> {
        if (this._scopedDag === undefined) {
            if (this.parentObj !== undefined) {
                this._scopedDag = await this.parentObj.getScopedDagForChild(this.createOpId);
            } else {
                const rawDag = await this.ctx.getDag(this.createOpId, this.runtimeConfig.backendLabel);
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
                const rawDag = await this.ctx.getDag(this.createOpId, this.runtimeConfig.backendLabel);
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

        const mesh = this.ctx.getMesh(this.runtimeConfig.meshLabel ?? 'default') as Mesh;
        this._swarm = mesh.createSwarm(this.createOpId);

        const rawDag = await this.ctx.getDag(this.createOpId, this.runtimeConfig.backendLabel);

        const target: SyncTarget = {
            dagId: this.createOpId,
            dag: rawDag,
            rObject: this,
            hashSuite: this.ctx.getHashSuite(),
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

export class RSetView<T  extends json.Literal> implements View {

    private target: RSet<T>;
    private at: Version;
    private from: Version;

    constructor(target: RSet<T>, at: Version, from: Version) {
        this.target = target;
        this.at = at;
        this.from = from;
    }

    getObject(): RSet<T> {
        return this.target;
    }

    getVersion(): Version {
        return this.at;
    }

    getFromVersion(): Version {
        return this.from;
    }

    async has(element: T): Promise<boolean> {

        if (this.target.contentType() !== undefined) {
            throw new Error("RSetView.has(element) is not well defined when contentType is present, please use hasByHash");
        }

        return this.hasByHash(await hashElement(element));
    }

    async hasByHash(elementHash: B64Hash): Promise<boolean> {

        let barriers = version();

        const dag = await this.target.getScopedDag();

        if (this.target.supportBarrierAdd() || this.target.supportBarrierDelete()) {
            barriers = await dag.findConcurrentCoverWithFilter(this.from, this.at, {containsValues: {barrier: ['t'], elmts: [elementHash]}});
        }

        let deleteBarriers = new Set<B64Hash>();

        for (const barrierHash of barriers) {
            const barrier = (await dag.loadEntry(barrierHash))!.payload as unknown as SetPayload;

            if (barrier['action'] === 'add') {
                return true;
            } else if (barrier['action'] === 'delete') {
                deleteBarriers.add(barrierHash);
            }
        }

        const cover = await dag.findCoverWithFilter(this.at, {containsValues: {elmts: [elementHash]}});
        const adds = new Set<B64Hash>();

        for (const hash of cover) {
            const payload = (await dag.loadEntry(hash))!.payload as unknown as SetPayload;

            if (payload['action'] === 'add' || payload['action'] === 'create') {
                if (!this.target.supportBarrierDelete()) {
                    return true;
                } else {
                    adds.add(hash);
                }
            }
        }

        if (adds.size > 0) {
            if (deleteBarriers.size === 0) {
                return true;
            } else {
                const causalDag = await this.target.getCausalDag();
                const fork = await causalDag.findForkPosition(adds, deleteBarriers);
                return fork.forkA.size > 0;
            }
        } else {
            return false;
        }
    }

    async loadRObjectByHash(elementHash: B64Hash): Promise<RObject | undefined> {

        if (this.target.contentType() === undefined) {
            throw new Error("RSetView.getRObjectByHash is not supported when RSet has no contentType");
        }
        
        const innerFactory = await this.target.getContext().getRegistry().lookup(this.target.contentType()!);
        return innerFactory.loadObject(elementHash, this.target.getContext(), this.target);
    }
}

class NestedElementScope implements DagScope {

    private parent: RSet;
    private elementHash: B64Hash;
    private start: Position;
    private executeAddOp?: AddElmtPayload;

    constructor(parent: RSet, elementHash: B64Hash, startAt: Position, executeAddOp?: AddElmtPayload) {
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

    constructor(parent: RSet, elementHash: B64Hash, startAt: Position, executeAddOp: AddElmtPayload) {
        super(parent, elementHash, startAt, executeAddOp);
    }
}

class ElementUpdateScope extends NestedElementScope implements DagScope {

    constructor(parent: RSet, elementHash: B64Hash, startAt: Position) {
        super(parent, elementHash, startAt);
    }
}

function hashElement<T extends json.Literal>(element: T): B64Hash {
    return sha256.hashToB64(stringToUint8Array(json.toStringNormalized(element)));
}

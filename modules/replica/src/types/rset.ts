

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
import { Hash, sha, BasicCrypto } from "@hyper-hyper-space/hhs3_crypto";
import { dag, MetaProps, position, EntryMetaFilter, Position, MetaContainsValues } from "@hyper-hyper-space/hhs3_dag";

import { Payload, BasicProvider, RObject, RObjectFactory, RObjectTypeRegistry, RObjectInit, Replica, version, Version, View } from "../replica";
import { DagCapability } from "dag/dag_resource";
import { DagScope, NestedScopedDag, ScopedDag, CausalDag } from "dag/dag_nesting";
import { set } from "@hyper-hyper-space/hhs3_util";

import { RAddEvent, RDeleteEvent, RSetEvent } from "./rset/events";

import { createSetFormat, CreateSetPayload } from "./rset/payload";
import { addElmtFormat, AddElmtPayload } from "./rset/payload";
import { deleteElmtFormat, DeleteElmtPayload } from "./rset/payload";
import { updateElmtFormat, UpdateElmtPayload } from "./rset/payload";

import { SetPayload } from "./rset/payload";

export type RSetProvider = BasicProvider & DagCapability;

type RSetResources = {
    replica: Replica<any>;
    registry: RObjectTypeRegistry<any>;
    getCrypto: () => BasicCrypto;
    getScopedDag: (tag?: string) => Promise<ScopedDag>;
    getCausalDag: (tag?: string) => Promise<CausalDag>;
};

// RSet factory: initial creation and validation of set creation ops.

export const rSetFactory: RObjectFactory<RSetProvider> = {

    computeRootObjectId: async (payload: json.Literal, provider: RSetProvider) => {

        const entry = await dag.createEntry(payload, {}, position(), provider.getCrypto().hash.sha256);
        return entry.hash;
    },

    validateCreationPayload: async (payload: json.Literal, provider: RSetProvider) => {
        
        if (!json.checkFormat(createSetFormat, payload)) {
            console.log('fmt')
            console.log(payload)
            return false;
        }
            
        const createPayload = payload as CreateSetPayload;
        
        // acceptUpdateForDeleted only makes sense if contentType is present
        if (createPayload['contentType'] === undefined && createPayload['acceptUpdateForDeleted'] !== undefined) {
            console.log('acceptUpdateForDeleted only makes sense if contentType is present')
            return false;
        }

        // acceptRedundantAdd only makes sense if contentType is not present
        // (otherwise, each add creates a new instance rooted in that place in the DAG)
        if (createPayload['contentType'] !== undefined && createPayload['acceptRedundantAdd'] !== undefined) {
            console.log('acceptRedundantAdd only makes sense if contentType is not present')
            return false;
        }

        if (createPayload['contentType'] !== undefined) {
            // TODO: it'd be nice to check that we have a factory for this type.
            if (createPayload['initialElements'].length > 0) {
                console.log('initialElements must be empty if contentType is present')
                return false;
            }
        }
        
        return true;
    },

    executeCreationPayload: async (payload: json.Literal, provider: RSetProvider) => {

        const scopedDag = await provider.getScopedDag();

        const meta: MetaProps = {};
        const createPayload = payload as CreateSetPayload;

        if (createPayload['contentType'] === undefined) {
            meta['elmts'] = json.toSet(await Promise.all(createPayload['initialElements'].map(async (e: json.Literal) => hashElement(e))));
        }
        
        
        return await scopedDag.append(createPayload, meta, position());
    },

    loadObject: async (id: Hash, provider: RSetProvider) => {
        const scopedDag = await provider.getScopedDag();
        const createOp = (await scopedDag.loadEntry(id))!.payload as CreateSetPayload;
        return new RSet(id, createOp, provider);
    }

}

export type RSetOptions = {
    seed: string;
    contentType?: string;  // <------------- | If present, literals in elements are treated 
    initialElements: Array<json.Literal>; // | as creation ops for the given type
    acceptRedundantAdd?: boolean;
    acceptRedundantDelete?: boolean;
    acceptRedundantUpdate?: boolean;
    supportBarrierAdd?: boolean;
    supportBarrierDelete?: boolean;
    hashAlgorithm?: string;
}

export class RSet<T extends json.Literal = json.Literal> implements RObject {

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

        return {type: RSet.typeId, payload: createPayload} as RObjectInit;
    }

    static typeId = "hhs/set_v1";

    createOpId: Hash;
    createOp: CreateSetPayload;
    private resources: RSetResources;

    constructor(createOpId: Hash, createOp: CreateSetPayload, provider: RSetProvider) {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.resources = {
            replica: provider.getReplica(),
            registry: provider.getRegistry(),
            getCrypto: () => provider.getCrypto(),
            getScopedDag: (tag?) => provider.getScopedDag(tag),
            getCausalDag: (tag?) => provider.getCausalDag(tag),
        };
    }

    // RObject identity and type

    getId(): string {
        return this.createOpId;
    }

    getType(): string {
        return RSet.typeId;
    }

    // Set operations

    async add(element: T, at?: Version): Promise<Hash> {
        const dag = await this.scopedDag();

        at = at || await dag.getFrontier();
        const payload = await this.createAddPayload(element, false, at);

        return this.applyValidatedPayload(payload, at);
    }
    
    async addWithBarrier(element: T, at?: Version): Promise<Hash> {
        const dag = await this.scopedDag();

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

    async delete(element: T, at?: Version): Promise<Hash> {

        if (this.contentType() !== undefined) {
            throw new Error("RSet.delete(element) is not well defined when contentType is present, please use deleteByHash");
        }

        const elementHash = await hashElement(element);
        return this.deleteByHash(elementHash, at);
    }

    async deleteByHash(elementHash: Hash, at?: Version): Promise<Hash> {
        const dag = await this.scopedDag();

        at = at || await dag.getFrontier();
        const payload: DeleteElmtPayload = await this.createDeletePayload(elementHash, false, at);
        return this.applyValidatedPayload(payload, at);
        
    }

    async deleteWithBarrier(element: T, at?: Version): Promise<Hash> {
        const elementHash = await hashElement(element);
        return this.deleteWithBarrierByHash(elementHash, at);
    }

    async deleteWithBarrierByHash(elementHash: Hash, at?: Version): Promise<Hash> {
        const dag = await this.scopedDag();

        at = at || await dag.getFrontier();
        const payload: DeleteElmtPayload = await this.createDeletePayload(elementHash, true, at);
        return this.applyValidatedPayload(payload, at);
    }

    private async createDeletePayload(elementHash: Hash, barrier: boolean, at: Version): Promise<DeleteElmtPayload> {

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

    private async applyValidatedPayload(payload: Payload, at: Version): Promise<Hash> {
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
                const innerFactory = await this.resources.registry.lookup(contentType);
                const scope = new ElementUpdateScope(this, updatePayload['elementHash'], position(updatePayload['elementHash']));
                const innerProvider = this.createChildProvider(updatePayload['elementHash'], scope);
                const innerRObject = await innerFactory.loadObject(updatePayload['elementHash'], innerProvider);
                if (!await innerRObject.validatePayload(updatePayload['updatePayload'], at)) {
                    return false;
                }
            }

            return true;
        }
    } 

    async applyPayload(payload: Payload, at: Version): Promise<Hash> {

        const setPayload = payload as unknown as SetPayload;
        
        if (setPayload['action'] === 'update') {
            const scope = new ElementUpdateScope(this, setPayload['elementHash'], position(setPayload['elementHash']));
            const innerProvider = this.createChildProvider(setPayload['elementHash'], scope);
            const innerFactory = await this.resources.registry.lookup(this.contentType()!);
            const innerRObject = await innerFactory.loadObject(setPayload['elementHash'], innerProvider);
            return await innerRObject.applyPayload(setPayload['updatePayload'], at);
        } else {
            const meta: MetaProps = {};

            switch (setPayload['action']) {
                case 'create':
                    throw new Error("Create operation is not supposed to be applied _to_ a set");
                case 'add':
                    if (this.contentType() !== undefined) {
                        const dag = await this.scopedDag();

                        const elementHash = await dag.computeEntryHash(setPayload, at)

                        const scope = new ElementAddScope(this, elementHash, at, setPayload);
                        const innerProvider = this.createChildProvider(elementHash, scope);
                        const innerFactory = await this.resources.registry.lookup(this.contentType()!);
                
                        await innerFactory.executeCreationPayload(setPayload['element'], innerProvider);

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
            
            const dag = await this.scopedDag();
    
            return await dag.append(payload, meta, at);
        }
        
        
    }

    async getView(at?: Version, from?: Version): Promise<RSetView<T>> {
        
        const dag = await this.scopedDag();

        at = at || await dag.getFrontier();
        from = from || await dag.getFrontier();


        return new RSetView<T>(this, at, from);
    }

    getRegistry(): RObjectTypeRegistry<any> {
        return this.resources.registry;
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
        return this.resources.replica.config.selfValidate || false;
    }

    subscribe(callback: (event: RAddEvent | RDeleteEvent) => void): void {
        throw new Error("Method not implemented.");
    }

    unsubscribe(callback: (event: RAddEvent | RDeleteEvent) => void): void {
        throw new Error("Method not implemented.");
    }

    scopedDag(): Promise<ScopedDag> {
        return this.resources.getScopedDag();
    }

    causalDag(): Promise<CausalDag> {
        return this.resources.getCausalDag();
    }

    createChildProvider(elementHash: Hash, scope: DagScope): RSetProvider {
        return {
            getReplica: () => this.resources.replica,
            getRegistry: () => this.resources.registry,
            getCrypto: () => this.resources.getCrypto(),
            getScopedDag: async (tag?) => new NestedScopedDag(await this.resources.getScopedDag(tag), scope),
            getCausalDag: (tag?) => this.resources.getCausalDag(tag),
        };
    }
}

const addMetaPropsForSetOp = (payload: SetPayload, meta: MetaProps, elmtHash: Hash): void => {
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

    async hasByHash(elementHash: Hash): Promise<boolean> {

        let barriers = version();

        const dag = await this.target.scopedDag();

        if (this.target.supportBarrierAdd() || this.target.supportBarrierDelete()) {
            barriers = await dag.findConcurrentCoverWithFilter(this.from, this.at, {containsValues: {barrier: ['t'], elmts: [elementHash]}});
        }

        let deleteBarriers = new Set<Hash>();

        for (const barrierHash of barriers) {
            const barrier = (await dag.loadEntry(barrierHash))!.payload as unknown as SetPayload;

            if (barrier['action'] === 'add') {
                return true;
            } else if (barrier['action'] === 'delete') {
                deleteBarriers.add(barrierHash);
            }
        }

        const cover = await dag.findCoverWithFilter(this.at, {containsValues: {elmts: [elementHash]}});
        const adds = new Set<Hash>();

        for (const hash of cover) {
            const payload = (await dag.loadEntry(hash))!.payload as unknown as SetPayload;

            if (payload['action'] === 'add' || payload['action'] === 'create') { // create is included to handle the initial elements
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
                const causalDag = await this.target.causalDag();
                const fork = await causalDag.findForkPosition(adds, deleteBarriers);
                return fork.forkA.size > 0;
            }
        } else {
            return false;
        }
    }

    async loadRObjectByHash(elementHash: Hash): Promise<RObject | undefined> {

        if (this.target.contentType() === undefined) {
            throw new Error("RSetView.getRObjectByHash is not supported when RSet has no contentType");
        }
        
        const scope = new ElementUpdateScope(this.target, elementHash, position(elementHash));
        const innerProvider = this.target.createChildProvider(elementHash, scope);
        const innerFactory = await this.target.getRegistry().lookup(this.target.contentType()!);

        return innerFactory.loadObject(elementHash, innerProvider);
    }
}

class NestedElementScope implements DagScope {

    private parent: RSet;
    private elementHash: Hash;
    private start: Position;
    private executeAddOp?: AddElmtPayload;

    constructor(parent: RSet, elementHash: Hash, startAt: Position, executeAddOp?: AddElmtPayload) {
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

    constructor(parent: RSet, elementHash: Hash, startAt: Position, executeAddOp: AddElmtPayload) {
        super(parent, elementHash, startAt, executeAddOp);
    }
}

class ElementUpdateScope extends NestedElementScope implements DagScope {

    constructor(parent: RSet, elementHash: Hash, startAt: Position) {
        super(parent, elementHash, startAt);
    }
}

async function hashElement<T extends json.Literal>(element: T): Promise<Hash> {
    return await sha.sha256(json.toStringNormalized(element));
}
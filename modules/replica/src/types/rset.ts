

// A Convergent Concurrency Control (CCR) enabled Replicable Set

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

// Notice that this wrapping process is not 100% transparetn: some operations (like fork poistion finding) will
// return DAG entries from the outer, full DAG. So types that support being inserted as nested elements must be
// aware of this caveats.

import { json } from "@hyper-hyper-space/hhs3_json";
import { Hash, sha } from "@hyper-hyper-space/hhs3_crypto";
import { Dag, MetaProps, position, EntryMetaFilter, Position, MetaContainsValues } from "@hyper-hyper-space/hhs3_dag";

import { Event, MAX_TYPE_LENGTH, Payload, Replica, ResourcesBase, RObject, RObjectFactory, RObjectInit, version, Version, View } from "../replica";
import { DagResource } from "dag/dag_resource";
import { DagScope, SubDag } from "dag/dag_nesting";
import { set } from "@hyper-hyper-space/hhs3_util";

export const MAX_SEED_LENGTH = 1024;
export const MAX_HASH_LENGTH = 128;
export const MAX_ELEMENTS_TYPE_ID_LENGTH = 256;
export const MAX_INITIAL_ELEMENTS = 1024;
export const MAX_HASH_ALGORITHM_LENGTH = 256;

// Events are still unimplemented, but here are some stubs for now

export type RAddEvent = Event & {
    type(): "add";
    element(): json.Literal;
}

export type RDeleteEvent = Event &{
    type(): "delete";
    element(): json.Literal;
}

export type RSetEvent = RAddEvent | RDeleteEvent;

// Actual payload for RSet operations, and their format validators.

type SetPayload = CreateSetPayload | AddElmtPayload | DeleteElmtPayload | UpdateElmtPayload;

// Create a set:

// Note: Sets of RObjects (when contentType !== undefined) cannot have initial elements.
//       In that case, initialElements MUST be an empty array.

type CreateSetPayload = {
    action: 'create';
    seed: string;
    contentType?: string;
    initialElements: Array<json.Literal>;
    acceptRedundantAdd?: boolean;
    acceptRedundantDelete: boolean;
    acceptUpdateForDeleted?: boolean;
    supportBarrierAdd?: boolean;
    supportBarrierDelete?: boolean;
    hashAlgorithm?: string;
}
 
const createSetFormat: json.Format = {
    action: [json.Type.Constant, 'create'],
    seed: [json.Type.BoundedString, MAX_SEED_LENGTH],
    contentType: [json.Type.Option, [json.Type.BoundedString, MAX_TYPE_LENGTH]],
    initialElements: [json.Type.BoundedArray, json.Type.String, MAX_INITIAL_ELEMENTS],
    acceptRedundantAdd: [json.Type.Option, json.Type.Boolean],
    acceptRedundantDelete: json.Type.Boolean,
    acceptUpdateForDeleted: [json.Type.Option, json.Type.Boolean],
    supportBarrierAdd: [json.Type.Option, json.Type.Boolean],
    supportBarrierDelete: [json.Type.Option, json.Type.Boolean],
    hashAlgorithm: [json.Type.Option, [json.Type.BoundedString, MAX_HASH_ALGORITHM_LENGTH]],
};

// Add an element:

type AddElmtPayload = {
    action: 'add';
    element: json.Literal;
    barrier?: boolean;
    type?: string;
};

const addElmtFormat: json.Format = {
    action: [json.Type.Constant, 'add'],
    element: json.Type.Something,
    barrier: [json.Type.Option, json.Type.Boolean],
};

// Delete an element:

type DeleteElmtPayload = {
    action: 'delete';
    elementHash: Hash;
    barrier?: boolean;
};

const deleteElmtFormat: json.Format = {
    action: [json.Type.Constant, 'delete'],
    elementHash: [json.Type.BoundedString, MAX_HASH_LENGTH],
    barrier: [json.Type.Option, json.Type.Boolean],
};

// Update an element (*):

// (*) Used by the DAG wrapper automatically when the contained element is updated

type UpdateElmtPayload = {
    action: 'update';
    elementHash: Hash;
    updatePayload: json.Literal;
}

const updateElmtFormat: json.Format = {
    action: [json.Type.Constant, 'update'],
    elementHash: [json.Type.BoundedString, MAX_HASH_LENGTH],
    updatePayload: json.Type.Something,
};

// Resources required by RSet:
//   - DAG storage.
//   - (in the future, we may add dyamic crypto resources here)

export type RSetResources = ResourcesBase & DagResource;

// RSet factory: initial creation and validation of set creation ops.

export const rSetFactory: RObjectFactory<RSetResources> = {

    computeObjectId: async (payload: json.Literal, resources: RSetResources) => {

        const dag = await resources.dag.get();
        return dag.computeEntryHash(payload, position());
    },

    validateCreationPayload: async (payload: json.Literal, resources: RSetResources) => {
        
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

    executeCreationPayload: async (payload: json.Literal, resources: RSetResources) => {

        const dag = await resources.dag.get();


        const meta: MetaProps = {};
        const createPayload = payload as CreateSetPayload;
        const initialElements = createPayload['initialElements'] || [];

        if (createPayload['contentType'] === undefined) {
            // Guard against missing initialElements in nested creation payloads.
            meta['elmts'] = json.toSet(await Promise.all(initialElements.map(async (e: json.Literal) => hashElement(e))));
        }
        
        
        return await dag.append(createPayload, meta, position());
    },

    loadObject: async (id: Hash, resources: RSetResources) => {
        const dag = await resources.dag.get();
        const createOp = (await dag.loadEntry(id))!.payload as CreateSetPayload;
        return new RSet(id, createOp, resources);
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
    createOp:CreateSetPayload;
    resources: RSetResources;

    constructor(createOpId: Hash, createOp: CreateSetPayload, resources: RSetResources) {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.resources = resources;    
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
        const dag = await this.dag();

        at = at || await dag.getFrontier();
        const payload = await this.createAddPayload(element, false, at);

        return this.applyPayload(payload, at);
    }
    
    async addWithBarrier(element: T, at?: Version): Promise<Hash> {
        const dag = await this.dag();

        at = at || await dag.getFrontier();
        const payload: AddElmtPayload = {action: 'add', element, barrier: true};
        return this.applyPayload(payload, at);
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
        const dag = await this.dag();

        at = at || await dag.getFrontier();
        const payload: DeleteElmtPayload = await this.createDeletePayload(elementHash, false, at);
        return this.applyPayload(payload, at);
        
    }

    async deleteWithBarrier(element: T, at?: Version): Promise<Hash> {
        const elementHash = await hashElement(element);
        return this.deleteWithBarrierByHash(elementHash, at);
    }

    async deleteWithBarrierByHash(elementHash: Hash, at?: Version): Promise<Hash> {
        const dag = await this.dag();

        at = at || await dag.getFrontier();
        const payload: DeleteElmtPayload = await this.createDeletePayload(elementHash, true, at);
        return this.applyPayload(payload, at);
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
                const hasElmt = await view.hasByHash(await hashElement(deletePayload['elementHash']));
                if (!hasElmt) {
                    return false;
                }
                return true;
            }

            if (!this.supportBarrierAdd() && json.hasKey(deletePayload, 'barrier')) {
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
                const hasElmt = await view.hasByHash(await hashElement(updatePayload['elementHash']));
                if (!hasElmt) {
                    return false;
                }
            }

            const contentType = this.contentType();

            if (contentType !== undefined) {
                const innerFactory = await this.resources.registry.lookup(contentType);
                const innerResources = await this.resources.replica.getResourcesForPreflight();
                if (!await innerFactory.validateCreationPayload(updatePayload['updatePayload'], innerResources)) {
                    return false;
                }
            }

            return true;
        }
    } 

    async applyPayload(payload: Payload, at: Version): Promise<Hash> {

        const setPayload = payload as unknown as SetPayload;
        
        if (setPayload['action'] === 'update') {
            const innerResources = { ...this.resources };
            const dag = await this.resources.dag.get();
            const scope = new ElementUpdateScope(setPayload['elementHash'], position(setPayload['elementHash']));
            innerResources.dag = {get: async () => new SubDag(dag, scope)};
            const innerFactory = await this.resources.registry.lookup(this.contentType()!);
            const innerRObject = await innerFactory.loadObject(setPayload['elementHash'], innerResources);
            return await innerRObject.applyPayload(setPayload['updatePayload'], at);
        } else {
            const meta: MetaProps = {};

            switch (setPayload['action']) {
                case 'create':
                    throw new Error("Create operation is not supposed to be applied _to_ a set");
                case 'add':
                    if (this.contentType() !== undefined) {
                        const dag = await this.dag();

                        const elementHash = await dag.computeEntryHash(setPayload, at)

                        const scope = new ElementAddScope(elementHash, at, setPayload);
                        
                        const innerResources = { ...this.resources };
                        innerResources.dag = {get: async () => new SubDag(dag, scope)};
                        const innerFactory = await this.resources.registry.lookup(this.contentType()!);
                
                        await innerFactory.executeCreationPayload(setPayload['element'], innerResources);

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
            
            const dag = await this.dag();
    
            return await dag.append(payload, meta, at);
        }
        
        
    }

    async getView(at?: Version, from?: Version): Promise<RSetView<T>> {
        
        const dag = await this.dag();

        at = at || await dag.getFrontier();
        from = from || await dag.getFrontier();


        return new RSetView<T>(this, at, from);
    }

    seed(): string {
        return this.createOp['seed'];
    }

    contentType(): string | undefined {
        return this.createOp['contentType'];
    }
    
    acceptRedundantAdd(): boolean {
        return this.createOp['acceptRedundantAdd'] || true;
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

    subscribe(callback: (event: RAddEvent | RDeleteEvent) => void): void {
        throw new Error("Method not implemented.");
    }

    unsubscribe(callback: (event: RAddEvent | RDeleteEvent) => void): void {
        throw new Error("Method not implemented.");
    }

    dag(): Promise<Dag> {
        return this.resources.dag.get();
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

        const dag = await this.target.dag();

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
                if (!this.target.supportBarrierDelete) {
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
                const fork = await dag.findForkPosition(adds, deleteBarriers);
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
        
        const dag = await this.target.dag();
        const scope = new ElementUpdateScope(elementHash, position(elementHash));
        
        const innerResources = { ...this.target.resources };
        innerResources.dag = {get: async () => new SubDag(dag, scope)};
        const innerFactory = await this.target.resources.registry.lookup(this.target.contentType()!);

        return innerFactory.loadObject(elementHash, innerResources);
    }
}

class NestedElementScope implements DagScope {

    private elementHash: Hash;
    private start: Position;
    private executeAddOp?: AddElmtPayload;

    constructor(elementHash: Hash, startAt: Position, executeAddOp?: AddElmtPayload) {
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
}

class ElementAddScope extends NestedElementScope implements DagScope {

    constructor(elementHash: Hash, startAt: Position, executeAddOp: AddElmtPayload) {
        super(elementHash, startAt, executeAddOp);
    }
}

class ElementUpdateScope extends NestedElementScope implements DagScope {

    constructor(elementHash: Hash, startAt: Position) {
        super(elementHash, startAt);
    }
}

async function hashElement<T extends json.Literal>(element: T): Promise<Hash> {
    return await sha.sha256(json.toStringNormalized(element));
}
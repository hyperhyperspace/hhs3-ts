

// A CCR replicable set - Convergent Concurrency Control

import { json } from "@hyper-hyper-space/hhs3_json";
import { Hash, sha } from "@hyper-hyper-space/hhs3_crypto";
import { Dag, MetaProps, position, computeEntryHash } from "@hyper-hyper-space/hhs3_dag";

import { Event, Payload, Replica, ResourcesBase, RObject, version, Version, View } from "../replica";
import { DagResource } from "dag/dag_replica";
import { copySet } from "@hyper-hyper-space/hhs3_json/dist/set";

export const MAX_SEED_LENGTH = 1024;
export const MAX_HASH_LENGTH = 128;
export const MAX_ELEMENTS_TYPE_ID_LENGTH = 256;
export const MAX_INITIAL_ELEMENTS = 1024;
export const MAX_HASH_ALGORITHM_LENGTH = 256;
export type RAddEvent = Event & {
    type(): "add";
    element(): json.Literal;
}

export type RDeleteEvent = Event &{
    type(): "delete";
    element(): json.Literal;
}

export type RSetEvent = RAddEvent | RDeleteEvent;

type CreateSetPayload = {
    action: 'create';
    seed: string;
    elementsTypeId: string;
    elements: Array<json.Literal>;
    acceptRedundantAdd?: boolean;
    acceptRedundantDelete?: boolean;
    supportBarrierAdd?: boolean;
    supportBarrierDelete?: boolean;
    hashAlgorithm?: string;
}
 
const createSetFormat: json.Format = {
    action: [json.Type.Constant, 'create'],
    seed: [json.Type.BoundedString, MAX_SEED_LENGTH],
    elementsTypeId: [json.Type.BoundedString, MAX_ELEMENTS_TYPE_ID_LENGTH],
    elements: [json.Type.BoundedArray, json.Type.String, MAX_INITIAL_ELEMENTS],
    elementHashes: [json.Type.BoundedArray, [json.Type.BoundedString, MAX_HASH_LENGTH], MAX_INITIAL_ELEMENTS],
    acceptRedundantAdd: [json.Type.Option, json.Type.Boolean],
    acceptRedundantDelete: [json.Type.Option, json.Type.Boolean],
    supportBarrierAdd: [json.Type.Option, json.Type.Boolean],
    supportBarrierDelete: [json.Type.Option, json.Type.Boolean],
    hashAlgorithm: [json.Type.Option, [json.Type.BoundedString, MAX_HASH_ALGORITHM_LENGTH]],
};

type AddElmtPayload = {
    action: 'add';
    element: json.Literal;
    barrier?: boolean;
};

const addElmtFormat: json.Format = {
    action: [json.Type.Constant, 'add'],
    element: json.Type.Something,
    barrier: [json.Type.Option, json.Type.Boolean],
};

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

type SetPayload = CreateSetPayload | AddElmtPayload | DeleteElmtPayload;

async function hashElement<T extends json.Literal>(element: T): Promise<Hash> {
    return await sha.sha256(json.toStringNormalized(element));
}

export type RSetOptions = {
    seed: string;
    elementsTypeId: string;
    elements: Array<json.Literal>;
    acceptRedundantAdd?: boolean;
    acceptRedundantDelete?: boolean;
    supportBarrierAdd?: boolean;
    supportBarrierDelete?: boolean;
    hashAlgorithm?: string;
}

export type RSetResources = ResourcesBase & DagResource;

export class RSet<T extends json.Literal = json.Literal> implements RObject {

    static create = async (options: RSetOptions, replica: Replica<RSetResources>) => {
        return await RSet.createImpl(options, replica);
    }

    static createNested = async (options: RSetOptions, replica: Replica<RSetResources>, dagWrapper: (dag: Dag) => Promise<Dag>, payloadWrapper: (payload: json.Literal) => Promise<json.Literal>, at: Version) => {
        return await RSet.createImpl(options, replica, dagWrapper, payloadWrapper, at);
    }


    private static createImpl = async (options: RSetOptions, replica: Replica<RSetResources>, dagWrapper?: (dag: Dag) => Promise<Dag>, payloadWrapper?: (payload: json.Literal) => Promise<json.Literal>, at?: Version) => {
        const createPayload: CreateSetPayload = {
            action: 'create',
            seed: options.seed,
            elementsTypeId: options.elementsTypeId || 'literal:json',
            elements: options.elements || [],
            hashAlgorithm: options.hashAlgorithm || 'sha256',
            acceptRedundantAdd: options.acceptRedundantAdd || false,
            acceptRedundantDelete: options.acceptRedundantDelete || false,
            supportBarrierAdd: options.supportBarrierAdd || false,
            supportBarrierDelete: options.supportBarrierDelete || false,
        };

        const createOpId = await computeEntryHash(payloadWrapper? await payloadWrapper(createPayload) : createPayload, at || position());
        const resources = await replica.getResources(createOpId);

        const dag = dagWrapper ? await dagWrapper(await resources.dag.get()) : await resources.dag.get();
        await dag.append(createPayload, {}, at || position());
        return new RSet(createOpId, createPayload, resources);
    }

    static load = async (createOpId: Hash, resources: RSetResources) => {
        const dag = await resources.dag.get();
        const createOp = (await dag.loadEntry(createOpId))!;
        return new RSet(createOpId, createOp.payload as CreateSetPayload, resources);
    }

    static validateCreatePayload = async (opHash: Hash, payload: Payload): Promise<boolean> => {
        return json.checkFormat(createSetFormat, payload);
    }

    static typeId = "hhs/set_v1";

    createOpId: Hash;
    resources: RSetResources;

    seed: string;
    elementsTypeId: string;
    acceptRedundantAdd: boolean;
    acceptRedundantDelete: boolean;
    supportBarrierAdd: boolean;
    supportBarrierDelete: boolean;
    private hashAlgorithm: string;

    constructor(createOpId: Hash, createOp: CreateSetPayload, resources: RSetResources) {
        this.createOpId = createOpId;
        this.resources = resources;

        this.seed = createOp.seed;
        this.elementsTypeId = createOp.elementsTypeId;
        this.acceptRedundantAdd = createOp.acceptRedundantAdd || false;
        this.acceptRedundantDelete = createOp.acceptRedundantDelete || false;
        this.supportBarrierAdd = createOp.supportBarrierAdd || false;
        this.supportBarrierDelete = createOp.supportBarrierDelete || false;
        this.hashAlgorithm = createOp.hashAlgorithm || 'sha256';
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

        if (!this.acceptRedundantAdd) {
            const view = await this.getView(at);
            const hasElmt = await view.has(element);
            if (hasElmt) {
                throw new Error("Element already exists in set, and redundant adds are not accepted");
            }            
        }

        if (!this.supportBarrierAdd && barrier) {
            throw new Error("Barrier add is not supported by this set");
        }

        if (this.supportBarrierAdd) {
            return {action: 'add', element, barrier};
        } else {
            return {action: 'add', element};
        }        
    }

    async delete(element: T, at?: Version): Promise<Hash> {
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

        if (!this.acceptRedundantDelete) {
            const view = await this.getView(at);
            const hasElmt = await view.hasByHash(elementHash);
            if (!hasElmt) {
                throw new Error("Element does not exist in set, and redundant deletes are not accepted");
            }            
        }

        if (!this.supportBarrierDelete && barrier) {
            throw new Error("Barrier delete is not supported by this set");
        }

        if (this.supportBarrierDelete) {
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
            case 'create':
                valid = await this.validateCreatePayload(payload);
                break;
            case 'add':
                valid = await this.validateAddElmtPayload(payload, at);
                break;
            case 'delete':
                valid = await this.validateDeleteElmtPayload(payload, at);
                break;
        }

        return valid;
    }

    private async validateCreatePayload(payload: Payload): Promise<boolean> {
        return json.checkFormat(createSetFormat, payload);
    }

    private async validateAddElmtPayload(payload: Payload, at: Version): Promise<boolean> {
        if (!json.checkFormat(addElmtFormat, payload)) {
            return false;
        } else {
            const addPayload = payload as AddElmtPayload;
            if (!this.acceptRedundantAdd) {
                const view = await this.getView(at, at);
                const hasElmt = await view.hasByHash(await hashElement(addPayload['element']));
                if (hasElmt) {
                    return false;
                }
                return true;
            }

            if (!this.supportBarrierAdd && json.hasKey(addPayload, 'barrier')) {
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
            if (!this.acceptRedundantDelete) {
                const view = await this.getView(at, at);
                const hasElmt = await view.hasByHash(await hashElement(deletePayload['elementHash']));
                if (!hasElmt) {
                    return false;
                }
                return true;
            }

            if (!this.supportBarrierAdd && json.hasKey(deletePayload, 'barrier')) {
                return false;
            }

            return true;
        }
    }

    async applyPayload(payload: Payload, at: Version): Promise<Hash> {

        const setPayload = payload as unknown as SetPayload;
        const meta: MetaProps = {};

        switch (setPayload['action']) {
            case 'create':
                throw new Error("Create operation is not supposed to be applied _to_ a set");
            case 'add':
                meta['elmts'] = json.toSet([await hashElement(setPayload['element'])]);
                break;
            case 'delete':
                meta['elmts'] = json.toSet([setPayload['elementHash']]);       
                break;
            default:
                throw new Error("Invalid set action in payload: " + setPayload['action']);
        }

        if (setPayload['barrier'] || false) {
            meta['barrier'] = json.toSet(['t']);
        }

        const dag = await this.dag();

        return await dag.append(payload, meta, at);
    }

    async getView(at?: Version, from?: Version): Promise<RSetView<T>> {
        
        const dag = await this.dag();

        at = at || await dag.getFrontier();
        from = from || await dag.getFrontier();


        return new RSetView<T>(this, at, from);
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
        return this.hasByHash(await hashElement(element));
    }

    async hasByHash(elementHash: Hash): Promise<boolean> {

        let barriers = version();

        const dag = await this.target.dag();

        if (this.target.supportBarrierAdd || this.target.supportBarrierDelete) {
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

            if (payload['action'] === 'add') {
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
}
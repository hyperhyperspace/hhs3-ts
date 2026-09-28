// RDeployGate: the replica-local record of which schema versions a table
// group has adopted (a standalone RObject, never synced).
//
// ACTIONS (see payload.ts for formats):
//
//   create   {group, schema}; the gate id derives from it (mirror.ts).
//   admit    mirrors one schema entry. Its predecessors are the mirrors of
//            the schema entry's predecessors (the gate genesis for the schema
//            genesis), so its hash is the canonical mirror hash, and DAG
//            append rejects an admit whose predecessors are missing: the
//            admitted set is always downward closed. Each entry is tagged
//            with the mirrored schema entry (meta `src`), which admit() uses
//            to find what is already admitted without walking history.
//
// The gate is never synced and never read on a group's view path: a group's
// synced deploy only depends on it (extractForeignDeps), which affects when
// the deploy is applied, not what any view computes. Because it is never
// synced, local appends are the only time its validation can run, so admit()
// validates every entry regardless of selfValidate.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { dag, position } from "@hyper-hyper-space/hhs3_dag";

import {
    Payload, RObjectFactory, RContext, LoadObjectOptions,
    Version, version, ForeignDep, Delta, DeltaAccumulator, View, RObject,
    formatValidationFailure, validationFailure, validationOk, ValidationRejectedError, ValidationResult,
    validateCreatePayloadType,
} from "@hyper-hyper-space/hhs3_mvt";
import { RootScopedDag, ScopedDag, CausalDag, ScopedDagSubscription } from "@hyper-hyper-space/hhs3_mvt";

import type { RDeployGate as RDeployGateContract } from "./interfaces.js";
import {
    CreateDeployGatePayload, AdmitPayload, createDeployGateFormat, admitFormat,
    RDEPLOY_GATE_TYPE_ID, MIRROR_SOURCE_META,
} from "./payload.js";
import { admitPayload, deployGateCreatePayload } from "./mirror.js";

export { RDEPLOY_GATE_TYPE_ID } from "./payload.js";

function validateCreate(payload: Payload): ValidationResult {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
        return validationFailure("RDeployGate create payload must be an object");
    }
    const typeResult = validateCreatePayloadType(payload, RDEPLOY_GATE_TYPE_ID);
    if (!typeResult.valid) return typeResult;
    return json.checkFormat(createDeployGateFormat, payload)
        ? validationOk()
        : validationFailure("RDeployGate create payload format is invalid");
}

export const rDeployGateFactory: RObjectFactory = {

    computeRootObjectId: async (payload: Payload, ctx: RContext) => {
        const entry = dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256));
        return entry.hash;
    },

    validateCreationPayload: async (payload: Payload) => validateCreate(payload),

    executeCreationPayload: async (payload: Payload, _ctx: RContext, scopedDag: ScopedDag) => {
        return await scopedDag.append(payload, {}, position());
    },

    loadObject: async (id: B64Hash, ctx: RContext, opts?: LoadObjectOptions) => {
        if (opts?.parent !== undefined) {
            throw new Error("RDeployGate is a standalone object (no nesting parent)");
        }

        const backendLabel = opts?.backendLabel ?? 'default';
        const rawDag = await ctx.getDag(id, backendLabel);
        if (rawDag === undefined) throw new Error(`DAG '${id}' not found`);

        const scopedDag = new RootScopedDag(rawDag);
        const createOp = (await scopedDag.loadEntry(id))!.payload as CreateDeployGatePayload;
        return new RDeployGateImpl(id, createOp, ctx, backendLabel);
    },
};

export class RDeployGateImpl implements RDeployGateContract {

    static create(groupId: B64Hash, schemaId: B64Hash): CreateDeployGatePayload {
        return deployGateCreatePayload(groupId, schemaId);
    }

    static typeId = RDEPLOY_GATE_TYPE_ID;

    createOpId: B64Hash;
    createOp: CreateDeployGatePayload;
    private ctx: RContext;
    private readonly backendLabel: string;

    private _scopedDag: ScopedDag | undefined;
    private _causalDag: CausalDag | undefined;

    constructor(createOpId: B64Hash, createOp: CreateDeployGatePayload, ctx: RContext, backendLabel: string = 'default') {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.ctx = ctx;
        this.backendLabel = backendLabel;
    }

    getId(): B64Hash { return this.createOpId; }
    getType(): string { return RDeployGateImpl.typeId; }
    getBackendLabel(): string { return this.backendLabel; }

    getGroupId(): B64Hash { return this.createOp.group; }
    getSchemaId(): B64Hash { return this.createOp.schema; }

    private async schemaDag(): Promise<ScopedDag> {
        const schema = await this.ctx.getObject(this.getSchemaId());
        if (schema === undefined) throw new Error(`RSchema '${this.getSchemaId()}' is not present in the replica`);
        return schema.getScopedDag();
    }

    async mirrorOf(schemaEntry: B64Hash): Promise<B64Hash | undefined> {
        const scopedDag = await this.getScopedDag();
        const frontier = await scopedDag.getFrontier();
        const cover = await scopedDag.findCoverWithFilter(frontier, { containsValues: { [MIRROR_SOURCE_META]: [schemaEntry] } });
        for (const hash of cover) return hash;
        return undefined;
    }

    async isAdmitted(v: Version): Promise<boolean> {
        for (const h of v) {
            if (await this.mirrorOf(h) === undefined) return false;
        }
        return true;
    }

    async getAdmittedFrontier(): Promise<Version> {
        const scopedDag = await this.getScopedDag();
        const admitted = version();
        for (const h of await scopedDag.getFrontier()) {
            if (h === this.createOpId) continue;
            const entry = await scopedDag.loadEntry(h);
            if (entry !== undefined) admitted.add((entry.payload as AdmitPayload).entry);
        }
        return admitted;
    }

    async admit(target: Version): Promise<B64Hash[]> {
        const schemaDag = await this.schemaDag();

        // post-order walk back from the target, stopping at entries that are
        // already admitted (the meta lookup), so only the delta is visited
        const mirrors = new Map<B64Hash, B64Hash>();
        const prevsOf = new Map<B64Hash, B64Hash[]>();
        const order: B64Hash[] = [];
        const done = new Set<B64Hash>();

        for (const t of target) {
            const stack: B64Hash[] = [t];
            while (stack.length > 0) {
                const h = stack[stack.length - 1];
                if (done.has(h)) { stack.pop(); continue; }
                if (!mirrors.has(h)) {
                    const existing = await this.mirrorOf(h);
                    if (existing !== undefined) {
                        mirrors.set(h, existing);
                        done.add(h);
                        stack.pop();
                        continue;
                    }
                }
                let prevs = prevsOf.get(h);
                if (prevs === undefined) {
                    const entry = await schemaDag.loadEntry(h);
                    if (entry === undefined) throw new Error(`schema entry '${h}' is not present`);
                    prevs = [...json.fromSet(entry.header.prevEntryHashes)];
                    prevsOf.set(h, prevs);
                }
                const pending = prevs.filter((p) => !done.has(p));
                if (pending.length > 0) {
                    stack.push(...pending);
                    continue;
                }
                stack.pop();
                done.add(h);
                order.push(h);
            }
        }

        const appended: B64Hash[] = [];
        for (const h of order) {
            const prevs = prevsOf.get(h)!;
            const at = prevs.length === 0 ? version(this.createOpId) : version(...prevs.map((p) => mirrors.get(p)!));
            const payload = admitPayload(h);
            const result = await this.validatePayload(payload, at);
            if (!result.valid) throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
            const hash = await this.applyPayload(payload, at);
            mirrors.set(h, hash);
            appended.push(hash);
        }
        return appended;
    }

    // --- RObject interface ---

    // An admit at `at` is valid when its entry exists in the schema and `at`
    // mirrors that entry's predecessors exactly (the gate genesis for the
    // schema genesis). Its hash is then the canonical mirror hash.
    async validatePayload(payload: Payload, at: Version): Promise<ValidationResult> {
        const objectHash = this.createOpId;
        if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
            return validationFailure("RDeployGate payload must be an object", { objectHash });
        }
        if ((payload as json.LiteralMap)['action'] !== 'admit' || !json.checkFormat(admitFormat, payload)) {
            return validationFailure("RDeployGate op must be a well-formed admit", { objectHash });
        }
        const admit = payload as AdmitPayload;

        const schemaEntry = await (await this.schemaDag()).loadEntry(admit.entry);
        if (schemaEntry === undefined) return validationFailure(`schema entry '${admit.entry}' does not exist`, { objectHash });
        const schemaPrevs = [...json.fromSet(schemaEntry.header.prevEntryHashes)].sort();

        const scopedDag = await this.getScopedDag();
        const mirrored: B64Hash[] = [];
        for (const h of at) {
            if (h === this.createOpId) {
                if (schemaPrevs.length !== 0 || at.size !== 1) {
                    return validationFailure("only the schema genesis is admitted directly on the gate genesis", { objectHash });
                }
                return validationOk();
            }
            const entry = await scopedDag.loadEntry(h);
            if (entry === undefined) return validationFailure(`gate entry '${h}' does not exist`, { objectHash });
            mirrored.push((entry.payload as AdmitPayload).entry);
        }
        if (schemaPrevs.length === 0) {
            return validationFailure("the schema genesis must be admitted on the gate genesis", { objectHash });
        }
        if (mirrored.sort().join(',') !== schemaPrevs.join(',')) {
            return validationFailure(`the admit of '${admit.entry}' does not mirror its predecessors`, { objectHash });
        }
        return validationOk();
    }

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {
        const admit = payload as AdmitPayload;
        const scopedDag = await this.getScopedDag();
        return await scopedDag.append(payload, { [MIRROR_SOURCE_META]: json.toSet([admit.entry]) }, at);
    }

    async getView(_at?: Version, _from?: Version): Promise<View> {
        throw new Error("RDeployGate has no view: use isAdmitted / getAdmittedFrontier");
    }

    extractForeignDeps(_payload: Payload, _at: Version): ForeignDep[] | undefined {
        return undefined;
    }

    async computeDelta(_start: Version, _end: Version): Promise<Delta> {
        throw new Error("RDeployGate has no delta");
    }

    createDeltaAccumulator(_start: Version, _end: Version): DeltaAccumulator {
        throw new Error("RDeployGate has no delta");
    }

    private _subscription: ScopedDagSubscription | undefined;

    private subscription(): ScopedDagSubscription {
        if (this._subscription === undefined) {
            this._subscription = new ScopedDagSubscription(() => this.getScopedDag());
        }
        return this._subscription;
    }

    subscribe(callback: (version: Version) => void): Promise<void> {
        return this.subscription().subscribe(callback);
    }

    unsubscribe(callback: (version: Version) => void): void {
        this.subscription().unsubscribe(callback);
    }

    async getScopedDag(): Promise<ScopedDag> {
        if (this._scopedDag === undefined) {
            const rawDag = await this.ctx.getDag(this.createOpId, this.backendLabel);
            if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);
            this._scopedDag = new RootScopedDag(rawDag);
        }
        return this._scopedDag;
    }

    async getCausalDag(): Promise<CausalDag> {
        if (this._causalDag === undefined) {
            const rawDag = await this.ctx.getDag(this.createOpId, this.backendLabel);
            if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);
            this._causalDag = rawDag;
        }
        return this._causalDag;
    }

    async destroy(): Promise<void> {
        this._scopedDag = undefined;
        this._causalDag = undefined;
    }
}

// Opens the gate of a group, creating its genesis when this replica has none.
export async function ensureDeployGate(ctx: RContext, groupId: B64Hash, schemaId: B64Hash, backendLabel?: string): Promise<RDeployGateImpl> {
    const payload = RDeployGateImpl.create(groupId, schemaId);
    const obj = await ctx.createObject(payload, backendLabel) as RObject;
    return obj as RDeployGateImpl;
}

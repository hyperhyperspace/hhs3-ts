// RSchema: the spec for one table group, as a standalone RObject in its own
// DAG.
//
// ACTIONS (see payload.ts for formats):
//
//   create
//     Genesis of a schema. Carries: name,
//     `creators` (keyId + publicKey pairs; spec authority — they may sign
//     schema-updates), the initial TableDef[] (columns with types /
//     nullable / default / pub, concurrentDeletes, fks: column -> table with
//     at-use semantics, restrictions: at-use predicates tagged
//     insert / update / delete / all), hash algorithm. No object-op gates:
//     deploy authority is per-instance policy (RTableGroup's canDeploy).
//
//   schema-update
//     Evolves the spec. Carries ONLY the migration rules — the slot writes:
//     add/drop-table, add/drop-column, set-concurrent-deletes,
//     set-fks, set-restrictions. No resulting defs and no result
//     hash: the effective schema is derived by the per-slot LWW resolution
//     and never serialized. Plus an optional note, and a REQUIRED author +
//     signature from one of the creators. Deploying an update is a separate
//     act: each observing RTableGroup barrier-ref-advances to the new
//     RSchema version at its own pace; concurrent deploys resolve by union
//     (the effective schema is the resolution at the merged version).
//
// The DAG has no barriers, so the effective schema is a pure function of the
// position `at` (see resolve.ts); resolved states are cached per normalized
// position. Schema DAGs are small: ops carry no entry meta and resolution
// does a full scan.
//
// RSchema does not sync on its own (no startSync): the RDb orchestrates
// sync for its members later if needed.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import type { KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Entry, position } from "@hyper-hyper-space/hhs3_dag";

import {
    Payload, RObjectFactory, RContext, LoadObjectOptions,
    Version, ForeignDep, Event, Delta, DeltaAccumulator,
    formatValidationFailure, ValidationRejectedError, ValidationResult,
} from "@hyper-hyper-space/hhs3_mvt";
import { RootScopedDag, ScopedDag, CausalDag } from "@hyper-hyper-space/hhs3_mvt";
import { signPayload as signPayloadHelper, serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchema as RSchemaContract, RSchemaView as RSchemaViewContract } from "./interfaces.js";
import { CreateRSchemaPayload, SchemaUpdatePayload, RSCHEMA_TYPE_ID } from "./payload.js";
import { TableDef, MigrationRule } from "./payload.js";
import { validateRSchemaPayload } from "./validate_ops.js";
import { resolveSchemaState, positionKey, SchemaState } from "./resolve.js";
import { RSchemaViewImpl } from "./view.js";
import { RSchemaDelta, RSchemaDeltaAccumulator, computeRSchemaDelta } from "./delta.js";

export { RSCHEMA_TYPE_ID } from "./payload.js";

export const rSchemaFactory: RObjectFactory = {

    computeRootObjectId: async (payload: json.Literal, ctx: RContext) => {
        const entry = dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256));
        return entry.hash;
    },

    validateCreationPayload: async (payload: json.Literal, ctx: RContext) =>
        validateRSchemaPayload(payload, { mode: 'create', ctx }),

    executeCreationPayload: async (payload: json.Literal, _ctx: RContext, scopedDag: ScopedDag) => {
        return await scopedDag.append(payload, {}, position());
    },

    loadObject: async (id: B64Hash, ctx: RContext, opts?: LoadObjectOptions) => {
        if (opts?.parent !== undefined) {
            throw new Error("RSchema is a standalone object (no nesting parent)");
        }

        const backendLabel = opts?.backendLabel ?? 'default';
        const rawDag = await ctx.getDag(id, backendLabel);
        if (rawDag === undefined) throw new Error(`DAG '${id}' not found`);

        const scopedDag = new RootScopedDag(rawDag);
        const createOp = (await scopedDag.loadEntry(id))!.payload as CreateRSchemaPayload;
        return new RSchemaImpl(id, createOp, ctx, backendLabel);
    },
};

export class RSchemaImpl implements RSchemaContract {

    static create = async (options: {
        name: string;
        creators: { keyId: KeyId; publicKey: PublicKey }[];
        tables: TableDef[];
        hashAlgorithm?: string;
    }): Promise<CreateRSchemaPayload> => {

        const createPayload: CreateRSchemaPayload = {
            action: 'create',
            type: RSCHEMA_TYPE_ID,
            name: options.name,
            creators: options.creators.map((c) => ({
                keyId: c.keyId,
                publicKey: serializePublicKeyToBase64(c.publicKey),
            })),
            tables: options.tables,
        };

        if (options.hashAlgorithm !== undefined) createPayload.hashAlgorithm = options.hashAlgorithm;

        return createPayload;
    };

    static typeId = RSCHEMA_TYPE_ID;

    createOpId: B64Hash;
    createOp: CreateRSchemaPayload;
    private ctx: RContext;
    private readonly backendLabel: string;

    private _scopedDag: ScopedDag | undefined;
    private _causalDag: CausalDag | undefined;

    // resolved states are immutable per position; keyed by normalized `at`
    private resolveCache: Map<string, SchemaState> = new Map();

    constructor(createOpId: B64Hash, createOp: CreateRSchemaPayload, ctx: RContext, backendLabel: string = 'default') {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.ctx = ctx;
        this.backendLabel = backendLabel;
    }

    getId(): B64Hash { return this.createOpId; }
    getType(): string { return RSchemaImpl.typeId; }
    getBackendLabel(): string { return this.backendLabel; }

    getName(): string { return this.createOp.name; }
    hashAlgorithm(): string | undefined { return this.createOp.hashAlgorithm; }

    getContext(): RContext { return this.ctx; }

    private selfValidate(): boolean {
        return this.ctx.getConfig().selfValidate || false;
    }

    // The only writer beyond creation: build a rules-only schema-update,
    // sign it and append it at `at` (defaults to the current frontier).
    async updateSchema(migration: MigrationRule[], author: OwnIdentity, note?: string, at?: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        const base: Omit<SchemaUpdatePayload, 'author' | 'signature'> = {
            action: 'schema-update',
            migration,
        };
        if (note !== undefined) base.note = note;

        const signed = await signPayloadHelper(base as unknown as json.LiteralMap, author);

        if (this.selfValidate()) {
            const result = await this.validatePayload(signed, at);
            if (!result.valid) {
                throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
            }
        }

        return this.applyPayload(signed, at);
    }

    // RObject interface

    async validatePayload(payload: Payload, at: Version): Promise<ValidationResult> {
        return validateRSchemaPayload(payload, { mode: 'op', schema: this, at });
    }

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        return await scopedDag.append(payload, {}, at);
    }

    async getView(at?: Version, from?: Version): Promise<RSchemaViewContract> {
        const scopedDag = await this.getScopedDag();

        at = at ?? await scopedDag.getFrontier();
        from = from ?? await scopedDag.getFrontier();

        const state = await this.resolveAt(at);
        return new RSchemaViewImpl(this, state, at, from);
    }

    // Resolve the effective schema at `at`, with a per-version cache (the
    // resolution ignores the view horizon, so the result is immutable).
    async resolveAt(at: Version): Promise<SchemaState> {
        const scopedDag = await this.getScopedDag();
        const normalized = await scopedDag.findMinimalCover(at);

        const key = positionKey(normalized);
        const cached = this.resolveCache.get(key);
        if (cached !== undefined) return cached;

        const entries: Entry[] = [];
        for await (const entry of scopedDag.loadAllEntries()) {
            entries.push(entry);
        }

        const state = resolveSchemaState(entries, normalized);
        this.resolveCache.set(key, state);
        return state;
    }

    extractForeignDeps(_payload: Payload, _at: Version): ForeignDep[] | undefined {
        return undefined;
    }

    async computeDelta(start: Version, end: Version): Promise<RSchemaDelta> {
        const rawDag = await this.ctx.getDag(this.createOpId, this.backendLabel);
        if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);
        return computeRSchemaDelta(this, rawDag, start, end);
    }

    createDeltaAccumulator(start: Version, end: Version): DeltaAccumulator {
        return new RSchemaDeltaAccumulator(this, start, end);
    }

    subscribe(_callback: (event: Event) => void): void {
        throw new Error("Method not implemented.");
    }

    unsubscribe(_callback: (event: Event) => void): void {
        throw new Error("Method not implemented.");
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
        this.resolveCache.clear();
    }
}

export { RSchemaViewImpl } from "./view.js";

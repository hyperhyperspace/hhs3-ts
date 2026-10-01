// RCatalog: a developer-signed catalog of table groups, as a DAG of semver
// releases (a standalone RObject in its own DAG).
//
// ACTIONS (see payload.ts for formats):
//
//   create
//     The genesis, which is also the first release: name, creators (the only
//     keys that may sign catalog entries), the release's version, the group
//     definitions it adds and the params it declares. Signed at the empty
//     position by one of the creators.
//
//   release
//     A signed diff against its parents: added definitions, version changes of
//     existing groups, new params. The parents are the maximal releases below
//     its position; a merge (several parents) must set every group they
//     disagree on.
//
//   declare
//     A signed, dependency-free list of schemas that later releases will
//     reference. It lets the RDb discover a new schema from validated state
//     and fetch it before the release that needs it validates.
//
// The DAG has no barriers: every view is final, and the resolved state of a
// release depends only on the release (see resolve.ts). A release's foreign
// deps are the schemas at the versions it adds or changes; the genesis
// declares the schemas at its pins as creation deps.
//
// RCatalog does not sync on its own (no startSync): the RDb orchestrates sync
// for its catalog.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import type { KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Entry, position } from "@hyper-hyper-space/hhs3_dag";

import {
    Payload, RObjectFactory, RContext, LoadObjectOptions,
    Version, version, ForeignDep, Delta, DeltaAccumulator,
    formatValidationFailure, ValidationRejectedError, ValidationResult,
} from "@hyper-hyper-space/hhs3_mvt";
import { RootScopedDag, ScopedDag, CausalDag, ScopedDagSubscription } from "@hyper-hyper-space/hhs3_mvt";
import { signPayload as signPayloadHelper, serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

import type {
    RCatalog as RCatalogContract, RCatalogView as RCatalogViewContract,
    CatalogReleaseSpec, PublishedRelease,
} from "./interfaces.js";
import {
    CreateRCatalogPayload, CatalogReleasePayload, CatalogDeclarePayload,
    CatalogGroupDef, CatalogFilesDef, CatalogParamDecl, SchemaCreator, RCATALOG_TYPE_ID,
} from "./payload.js";
import { validateRCatalogPayload } from "./validate_ops.js";
import { CatalogIndex, ReleaseState, versionKey } from "./resolve.js";
import { RCatalogViewImpl } from "./view.js";

export { RCATALOG_TYPE_ID } from "./payload.js";

function genesisDeps(create: CreateRCatalogPayload): ForeignDep[] {
    return (create.add ?? []).map((def) => ({
        objectId: def.schemaRef,
        requiredHashes: [...json.fromSet(def.schemaVersion)],
    }));
}

export const rCatalogFactory: RObjectFactory = {

    computeRootObjectId: async (payload: json.Literal, ctx: RContext) => {
        const entry = dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256));
        return entry.hash;
    },

    validateCreationPayload: async (payload: json.Literal, ctx: RContext) =>
        validateRCatalogPayload(payload, { mode: 'create', ctx }),

    executeCreationPayload: async (payload: json.Literal, _ctx: RContext, scopedDag: ScopedDag) => {
        return await scopedDag.append(payload, {}, position());
    },

    loadObject: async (id: B64Hash, ctx: RContext, opts?: LoadObjectOptions) => {
        if (opts?.parent !== undefined) {
            throw new Error("RCatalog is a standalone object (no nesting parent)");
        }

        const backendLabel = opts?.backendLabel ?? 'default';
        const rawDag = await ctx.getDag(id, backendLabel);
        if (rawDag === undefined) throw new Error(`DAG '${id}' not found`);

        const scopedDag = new RootScopedDag(rawDag);
        const createOp = (await scopedDag.loadEntry(id))!.payload as CreateRCatalogPayload;
        return new RCatalogImpl(id, createOp, ctx, backendLabel);
    },

    // The genesis is a release: the schemas at its pins must be present (and
    // advanced that far) before it can be validated.
    extractCreationForeignDeps: async (payload: Payload, _ctx: RContext) =>
        genesisDeps(payload as CreateRCatalogPayload),
};

function releaseBase(spec: CatalogReleaseSpec): json.LiteralMap {
    const base: json.LiteralMap = { version: spec.version };
    if (spec.changes !== undefined && Object.keys(spec.changes).length > 0) base['changes'] = spec.changes as unknown as json.Literal;
    if (spec.add !== undefined && spec.add.length > 0) base['add'] = spec.add as unknown as json.Literal;
    if (spec.files !== undefined && spec.files.length > 0) base['files'] = spec.files as unknown as json.Literal;
    if (spec.params !== undefined && spec.params.length > 0) base['params'] = spec.params as unknown as json.Literal;
    if (spec.note !== undefined) base['note'] = spec.note;
    return base;
}

export class RCatalogImpl implements RCatalogContract {

    static create = async (options: {
        name: string;
        creators: { keyId: KeyId; publicKey: PublicKey }[];
        author: OwnIdentity;
        version: string;
        add?: CatalogGroupDef[];
        files?: CatalogFilesDef[];
        params?: CatalogParamDecl[];
        note?: string;
        seed?: string;
        hashAlgorithm?: string;
    }): Promise<CreateRCatalogPayload> => {

        const base: json.LiteralMap = {
            action: 'create',
            type: RCATALOG_TYPE_ID,
            name: options.name,
            creators: options.creators.map((c) => ({
                keyId: c.keyId,
                publicKey: serializePublicKeyToBase64(c.publicKey),
            })),
            ...releaseBase({ version: options.version, add: options.add, files: options.files, params: options.params, note: options.note }),
        };
        if (options.seed !== undefined) base['seed'] = options.seed;
        if (options.hashAlgorithm !== undefined) base['hashAlgorithm'] = options.hashAlgorithm;

        return await signPayloadHelper(base, options.author, version()) as unknown as CreateRCatalogPayload;
    };

    static typeId = RCATALOG_TYPE_ID;

    createOpId: B64Hash;
    createOp: CreateRCatalogPayload;
    private ctx: RContext;
    private readonly backendLabel: string;

    private _scopedDag: ScopedDag | undefined;
    private _causalDag: CausalDag | undefined;

    // Release states are immutable per hash and shared across index rebuilds.
    private readonly releaseMemo: Map<B64Hash, ReleaseState> = new Map();
    private indexCache: { key: string; index: CatalogIndex } | undefined;

    constructor(createOpId: B64Hash, createOp: CreateRCatalogPayload, ctx: RContext, backendLabel: string = 'default') {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.ctx = ctx;
        this.backendLabel = backendLabel;
    }

    getId(): B64Hash { return this.createOpId; }
    getType(): string { return RCatalogImpl.typeId; }
    getBackendLabel(): string { return this.backendLabel; }

    getName(): string { return this.createOp.name; }
    hashAlgorithm(): string | undefined { return this.createOp.hashAlgorithm; }
    getCreators(): SchemaCreator[] { return [...this.createOp.creators]; }
    isCreator(keyId: KeyId): boolean { return this.createOp.creators.some((c) => c.keyId === keyId); }

    getContext(): RContext { return this.ctx; }

    private selfValidate(): boolean {
        return this.ctx.getConfig().selfValidate || false;
    }

    // The index over every entry currently in the DAG, rebuilt when the
    // frontier moves.
    async getIndex(): Promise<CatalogIndex> {
        const scopedDag = await this.getScopedDag();
        const key = versionKey(await scopedDag.getFrontier());
        if (this.indexCache !== undefined && this.indexCache.key === key) return this.indexCache.index;

        const entries: Entry[] = [];
        for await (const entry of scopedDag.loadAllEntries()) entries.push(entry);
        const index = new CatalogIndex(entries, this.releaseMemo);
        this.indexCache = { key, index };
        return index;
    }

    // --- Writers ---

    async release(spec: CatalogReleaseSpec, author: OwnIdentity, at?: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        const base: json.LiteralMap = { action: 'release', ...releaseBase(spec) };
        const signed = await signPayloadHelper(base, author, at) as unknown as CatalogReleasePayload;
        return this.appendValidated(signed as unknown as json.LiteralMap, at);
    }

    async declare(schemas: B64Hash[], author: OwnIdentity, at?: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        const base: json.LiteralMap = { action: 'declare', schemas: json.toSet(schemas) };
        const signed = await signPayloadHelper(base, author, at) as unknown as CatalogDeclarePayload;
        return this.appendValidated(signed as unknown as json.LiteralMap, at);
    }

    async publishRelease(spec: CatalogReleaseSpec, author: OwnIdentity, at?: Version): Promise<PublishedRelease> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        const referenced = (await this.getIndex()).referencedSchemasAt(at);
        const needed = new Set<B64Hash>();
        for (const def of spec.add ?? []) if (!referenced.has(def.schemaRef)) needed.add(def.schemaRef);
        for (const change of Object.values(spec.changes ?? {})) if (!referenced.has(change.schema)) needed.add(change.schema);

        if (needed.size === 0) return { release: await this.release(spec, author, at) };

        const declare = await this.declare([...needed].sort(), author, at);
        const release = await this.release(spec, author, version(declare));
        return { declare, release };
    }

    private async appendValidated(payload: json.LiteralMap, at: Version): Promise<B64Hash> {
        if (this.selfValidate()) {
            const result = await this.validatePayload(payload, at);
            if (!result.valid) {
                throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
            }
        }
        return this.applyPayload(payload, at);
    }

    // --- RObject interface ---

    async validatePayload(payload: Payload, at: Version): Promise<ValidationResult> {
        return validateRCatalogPayload(payload, { mode: 'op', catalog: this, at });
    }

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        return await scopedDag.append(payload, {}, at);
    }

    async getView(at?: Version, from?: Version): Promise<RCatalogViewContract> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();
        from = from ?? await scopedDag.getFrontier();
        return new RCatalogViewImpl(this, await this.getIndex(), at, from);
    }

    extractForeignDeps(payload: Payload, _at: Version): ForeignDep[] | undefined {
        if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return undefined;
        const p = payload as json.LiteralMap;
        if (p['action'] !== 'release') return undefined;

        const release = payload as unknown as CatalogReleasePayload;
        const deps: ForeignDep[] = [];
        for (const def of release.add ?? []) {
            deps.push({ objectId: def.schemaRef, requiredHashes: [...json.fromSet(def.schemaVersion)] });
        }
        for (const change of Object.values(release.changes ?? {})) {
            deps.push({ objectId: change.schema, requiredHashes: [...json.fromSet(change.version)] });
        }
        return deps;
    }

    async computeDelta(_start: Version, _end: Version): Promise<Delta> {
        throw new Error("RCatalog has no delta in v1");
    }

    createDeltaAccumulator(_start: Version, _end: Version): DeltaAccumulator {
        throw new Error("RCatalog has no delta in v1");
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
        this.indexCache = undefined;
    }
}

export { RCatalogViewImpl } from "./view.js";

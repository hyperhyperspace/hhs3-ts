// RTableGroup: the unit of atomicity, snapshot, observation and composition.
// Owns one physical DAG; member RTables are scoped projections of it, so one
// group position is a consistent snapshot of all member tables.
//
// ACTIONS (see payload.ts for formats):
//
//   create
//     Genesis of a group instance. Carries: seed, `schemaRef` + pinned
//     `schemaVersion` (the deployed RSchema id and version; validating a
//     create resolves the schema at that version — a cross-DAG dependency at
//     genesis), optional `initialRows` (genesis fiat rows keyed by table, in
//     insert-op shape, no authoring: they root permission
//     delegation chains and carry identity public keys), optional `bindings`
//     (qualified-name resolution, group name -> group object id, fixed in
//     v1), optional `canDeploy` (predicate gating schema ref-advances,
//     'object' context: $author only, no subject row; fixed in v1), hash
//     algorithm.
//
//   row
//     Envelope written automatically by a member RTable's DagScope: wraps ONE
//     inner row op (insert / update / delete), tagged with its table name so
//     the table's scope filter picks it back out (the nested-RSet pattern).
//
//   bundle
//     Single-entry atomic multi-table write: `writes` is an ORDERED
//     `{ table, op }[]` (the bundle order, carried explicitly since entry
//     hashing sorts map keys); entry meta is tagged with every touched table
//     (each table's scope filter matches its slice). The parts hash, validate
//     and apply together and can never exist apart, even mid-sync. Op `i`'s FK
//     conditions hold at the sequential cut `at` ∪ the ops before it.
//
//   ref-advance (canonical mvt payload, checked non-strictly)
//     Advances the group's observed version of a referenced object:
//       - the RSchema ref: THE SCHEMA DEPLOY MOMENT, as a barrier. It carries
//         `gate`, the RDeployGate mirror hashes of its target version: a
//         synced deploy depends on them (extractForeignDeps), so a replica
//         applies it only once its local gate admits the version, while
//         validation recomputes them from the schema DAG and never reads the
//         gate. A local deploy never waits on the gate. When the
//         group declares `canDeploy`, that predicate is derived from the
//         group's create payload (mandatory); an authored deploy carries
//         author/signature as extra fields, the signature is verified
//         at validation (against the group's own provider, then its embedded
//         deployKeys) and the predicate is then evaluated against the
//         verified author.
//       - a bound foreign group ref: cross-group FK / exists observation,
//         a barrier advance (see observe). The
//         dependent resolves `group.table` targets through the foreign group
//         at the observed version (resolveForeignTableView); a concurrent
//         observation revises the merged frontier like a deploy.
//
// The effective schema at a group position = the RSchema resolved at the
// create's pinned schemaVersion unioned with ref-advances at or below the
// position. Tables exist by schema: getTable(name) constructs the nested
// RTable on a TableScope projection (tableId = deriveTableId(groupId, name));
// there is no table creation op.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity, KeyId, PublicKey, HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import { dag, position } from "@hyper-hyper-space/hhs3_dag";
import type { MetaProps } from "@hyper-hyper-space/hhs3_dag";

import {
    Payload, RObjectFactory, RContext, LoadObjectOptions, NestingParent,
    Version, version, ForeignDep, DeltaAccumulator, RObject,
    formatValidationFailure, validationFailure, ValidationRejectedError, ValidationResult,
} from "@hyper-hyper-space/hhs3_mvt";
import { RootScopedDag, NestedScopedDag, ScopedDag, CausalDag, ScopedDagSubscription } from "@hyper-hyper-space/hhs3_mvt";
import {
    isRefAdvancePayload, extractRefVersion, extractAuthor, prepareRefAdvance, createRefAdvanceMeta,
    createRefAdvancePayload, resolveRefVersionAtPosition, findConcurrentRefAdvanceBarriers,
    refVersionAtOrAbove,
} from "@hyper-hyper-space/hhs3_mvt";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";
import { signPayload as signPayloadHelper } from "@hyper-hyper-space/hhs3_mvt";
import { TRUE, allTruth } from "@hyper-hyper-space/hhs3_mvt";
import type { Truth, Verdict, VerdictEvaluation } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchema, RSchemaView } from "../rschema/interfaces.js";
import type { IncarnationId } from "../rschema/incarnation.js";
import type { Predicate, SchemaCreator } from "../rschema/payload.js";
import { splitTableRef } from "../rschema/payload.js";
import { predicateReferencesAuthor } from "../rschema/validate.js";
import { computeMirrorHashes, deployGateId } from "../rdeploy_gate/mirror.js";
import { RTableImpl } from "../rtable/rtable.js";
import { deriveTableId } from "../rtable/hash.js";
import type { RowOpPayload } from "../rtable/payload.js";
import type { RTableView } from "../rtable/interfaces.js";
import { RTableViewImpl } from "../rtable/view.js";

import type { RTableGroup as RTableGroupContract, RTableGroupView as RTableGroupViewContract, BundleWrite } from "./interfaces.js";
import { CreateTableGroupPayload, RowEnvelopePayload, BundlePayload, RTABLE_GROUP_TYPE_ID, deployGateSetFormat } from "./payload.js";
import { TableScope, deriveCreateMeta, deriveEnvelopeMeta, deriveBundleMeta, deriveDeployKillMeta } from "./scopes.js";
import type { DeployKills } from "./scopes.js";
import { freshVerdictEvaluation, VERDICT_MAX_INFLIGHT } from "./verdict_evaluation.js";
import { validateTableGroupPayload } from "./validate_ops.js";
import { evaluatePredicateTruth, explainRowOpRestriction, explainRowOpFKReach } from "./predicates.js";
import type { OpVoidDetail } from "./op_void.js";
import { RTableGroupViewImpl } from "./view.js";
import {
    RTableGroupDelta, RTableGroupDeltaStrategy, RTableGroupDeltaAccumulator,
    computeRTableGroupDelta,
} from "./delta.js";

export { RTABLE_GROUP_TYPE_ID } from "./payload.js";

export const rTableGroupFactory: RObjectFactory = {

    computeRootObjectId: async (payload: Payload, ctx: RContext) => {
        const entry = dag.createEntry(payload, {}, position(), ctx.getCrypto().hash(HASH_SHA256));
        return entry.hash;
    },

    validateCreationPayload: async (payload: Payload, ctx: RContext, parent?: NestingParent) => {
        if (parent !== undefined) return validationFailure("table groups are root objects");
        return validateTableGroupPayload(payload, { mode: 'create', ctx });
    },

    executeCreationPayload: async (payload: Payload, ctx: RContext, scopedDag: ScopedDag) => {
        const create = payload as CreateTableGroupPayload;

        // the create entry carries each initial table's rows / pub meta, so
        // table scopes surface their genesis rows
        const schemaObj = await ctx.getObject(create.schemaRef);
        if (schemaObj === undefined) {
            throw new Error(`RSchema '${create.schemaRef}' is not present in the replica`);
        }
        const pinned = version(...json.fromSet(create.schemaVersion));
        const schemaView = await (schemaObj as RSchema).getView(pinned, pinned);

        return await scopedDag.append(create, deriveCreateMeta(create, schemaView), position());
    },

    loadObject: async (id: B64Hash, ctx: RContext, opts?: LoadObjectOptions) => {
        if (opts?.parent !== undefined) {
            throw new Error("RTableGroup is a standalone object (no nesting parent)");
        }

        const backendLabel = opts?.backendLabel ?? 'default';
        const rawDag = await ctx.getDag(id, backendLabel);
        if (rawDag === undefined) throw new Error(`DAG '${id}' not found`);

        const scopedDag = new RootScopedDag(rawDag);
        const createOp = (await scopedDag.loadEntry(id))!.payload as CreateTableGroupPayload;
        return new RTableGroupImpl(id, createOp, ctx, backendLabel);
    },

    // Genesis deps of a group create: the pinned RSchema at its pinned version
    // (which may be post-genesis, so the schema DAG must have advanced that far),
    // and every binding target object (bindings are name -> id only, no version,
    // so object presence suffices). This lives on the factory because it must be
    // evaluated before the group object exists; the op-level extractForeignDeps
    // never sees a 'create' (validatePayload rejects create on an existing group).
    extractCreationForeignDeps: async (payload: Payload, _ctx: RContext) => {
        const create = payload as CreateTableGroupPayload;
        const deps: ForeignDep[] = [{
            objectId: create.schemaRef,
            requiredHashes: [...json.fromSet(create.schemaVersion)],
        }];
        for (const target of Object.values(create.bindings ?? {})) {
            deps.push({ objectId: target, requiredHashes: [] });
        }
        return deps;
    },
};

export class RTableGroupImpl implements RTableGroupContract {

    static create = async (options: {
        name: string;
        seed: string;
        schemaRef: B64Hash;
        schemaVersion: Version;
        initialRows?: { [table: string]: json.Literal[] };
        bindings?: { [name: string]: B64Hash };
        canDeploy?: Predicate;
        canObserve?: { [binding: string]: Predicate };
        idProvider?: string;
        deployKeys?: SchemaCreator[];
        hashAlgorithm?: string;
    }): Promise<CreateTableGroupPayload> => {

        const createPayload: CreateTableGroupPayload = {
            action: 'create',
            type: RTABLE_GROUP_TYPE_ID,
            name: options.name,
            seed: options.seed,
            schemaRef: options.schemaRef,
            schemaVersion: json.toSet([...options.schemaVersion]),
        };

        if (options.initialRows !== undefined) createPayload.initialRows = options.initialRows;
        if (options.bindings !== undefined) createPayload.bindings = options.bindings;
        if (options.canDeploy !== undefined) createPayload.canDeploy = options.canDeploy;
        if (options.canObserve !== undefined) createPayload.canObserve = options.canObserve;
        if (options.idProvider !== undefined) createPayload.idProvider = options.idProvider;
        if (options.deployKeys !== undefined && options.deployKeys.length > 0) createPayload.deployKeys = options.deployKeys;
        if (options.hashAlgorithm !== undefined) createPayload.hashAlgorithm = options.hashAlgorithm;

        return createPayload;
    };

    static typeId = RTABLE_GROUP_TYPE_ID;

    createOpId: B64Hash;
    createOp: CreateTableGroupPayload;
    private ctx: RContext;
    private readonly backendLabel: string;

    private _scopedDag: ScopedDag | undefined;
    private _causalDag: CausalDag | undefined;
    private tables: Map<string, RTableImpl> = new Map();

    // Entry verdicts. Voiding recurses: a restriction / exists (a caps insert
    // gated by exists over caps, whose witness insert is itself gated, ...)
    // and FK reach (a write whose FK target's own liveness depends on another
    // entry) read other entries' verdicts. Every verdict is the well-founded
    // one, computed by a VerdictEvaluation (mvt verdict_evaluation.ts;
    // VOID_SEMANTICS.md section 4): entries that read each other form a
    // component, solved by the alternating fixpoint, and whatever that leaves
    // undecided collapses to void (`undecided-cycle`), bottom-up. So a
    // self-granting op (its own witness) is void — authority must root at a
    // genesis fiat row, which is never voided — an FK reference cycle that
    // nothing outside it supports is void, and of two concurrent revokes of
    // each other's revoker both are void (both caps survive) unless something
    // outside the cycle decides one of them.
    //
    // A verdict is a pure function of the entry and `from`, so every replica
    // agrees regardless of which entry a top-level query touches first.
    //
    // The evaluation is PER COMPUTATION, not on the instance: one is minted at
    // each top-level entry and threaded, mandatory, through every helper that
    // receives it and every RTableViewImpl those helpers build, so
    // interleaved async computations on the same cached group instance never
    // see each other's open components. A method that receives a
    // VerdictEvaluation must pass that same evaluation on. It must never call
    // a minting wrapper: isEntryVoided, explainEntryVoided,
    // resolveForeignTableView, evaluateObserveGate, or the table's getView.
    // Keys are group-namespaced (createOpId | entry | from), so one evaluation
    // flows into bound foreign groups. Bindings form a DAG, so a component
    // never spans two groups.
    //
    // WITHIN one computation the evaluation memoizes completed components
    // (`answers`); without that, an update chain is exponential (each
    // update's getRow re-diagnoses every earlier write of the row).
    //
    // `_verdictInflight` is a FAIL-SAFE, not a verdict: the type system forces
    // an evaluation onto every helper that takes one, but internal code could
    // still call a minting wrapper by mistake, which on cyclic data would
    // recurse forever through fresh evaluations (an unbounded microtask chain
    // no timer can interrupt). The counter (bumped in enterVerdictFrame on each
    // diagnose run, released in finally) throws past VERDICT_MAX_INFLIGHT so
    // such a bug surfaces as an immediate error, not a hang. It is additive
    // under concurrency and never affects a verdict.
    private _verdictInflight = 0;

    // Memoized inverse of the (injective) bindings map: group id -> binding
    // name. Injectivity is enforced at create-validation, so the inverse is a
    // well-defined function; built lazily on first use.
    private _bindingNameById: Map<B64Hash, string> | undefined;

    private deltaStrategy: RTableGroupDeltaStrategy = 'bounded';

    // The gate id and the schema entry -> mirror hash memo: both are fixed by
    // the group id and its (append-only) schema DAG.
    private _deployGateId: B64Hash | undefined;
    private readonly mirrorMemo: Map<B64Hash, B64Hash> = new Map();

    constructor(createOpId: B64Hash, createOp: CreateTableGroupPayload, ctx: RContext, backendLabel: string = 'default') {
        this.createOpId = createOpId;
        this.createOp = createOp;
        this.ctx = ctx;
        this.backendLabel = backendLabel;
    }

    getId(): B64Hash { return this.createOpId; }
    getType(): string { return RTableGroupImpl.typeId; }
    getBackendLabel(): string { return this.backendLabel; }

    getContext(): RContext { return this.ctx; }

    // Create-time facts

    getName(): string { return this.createOp.name; }
    seed(): string { return this.createOp.seed; }
    hashAlgorithm(): string | undefined { return this.createOp.hashAlgorithm; }
    getSchemaRef(): B64Hash { return this.createOp.schemaRef; }

    getPinnedSchemaVersion(): Version {
        return version(...json.fromSet(this.createOp.schemaVersion));
    }

    getBindings(): { [name: string]: B64Hash } {
        return { ...(this.createOp.bindings ?? {}) };
    }

    getCanDeploy(): Predicate | undefined {
        return this.createOp.canDeploy;
    }

    getCanObserve(): { [binding: string]: Predicate } | undefined {
        return this.createOp.canObserve;
    }

    // The binding name a bound group id resolves to, via the injective inverse
    // of getBindings() (undefined if the id is not a bound group).
    bindingNameForId(refId: B64Hash): string | undefined {
        if (this._bindingNameById === undefined) {
            this._bindingNameById = new Map();
            for (const [name, id] of Object.entries(this.getBindings())) {
                this._bindingNameById.set(id, name);
            }
        }
        return this._bindingNameById.get(refId);
    }

    // The canObserve gate for an observation of `refId` (a bound group id), or
    // undefined when the binding is ungated (observation needs no authority).
    observeGateFor(refId: B64Hash): Predicate | undefined {
        const name = this.bindingNameForId(refId);
        if (name === undefined) return undefined;
        return this.getCanObserve()?.[name];
    }

    getIdProvider(): string | undefined {
        return this.createOp.idProvider;
    }

    getDeployKeys(): SchemaCreator[] {
        return [...(this.createOp.deployKeys ?? [])];
    }

    // The id of this group's RDeployGate, derived from the group id and its
    // schema ref (the same on every replica).
    getDeployGateId(): B64Hash {
        if (this._deployGateId === undefined) {
            this._deployGateId = deployGateId(this.createOpId, this.getSchemaRef());
        }
        return this._deployGateId;
    }

    async computeGateHashes(schemaVersion: Version): Promise<B64Hash[]> {
        const schema = await this.getSchemaObject();
        return computeMirrorHashes(await schema.getScopedDag(), this.getDeployGateId(), schemaVersion, this.mirrorMemo);
    }

    getHashSuite(): HashSuite {
        return this.ctx.getHashSuite();
    }

    selfValidate(): boolean {
        return this.ctx.getConfig().selfValidate || false;
    }

    // Schema observation

    async getSchemaObject(): Promise<RSchema> {
        const obj = await this.ctx.getObject(this.getSchemaRef());
        if (obj === undefined) {
            // a missing referenced object is an infrastructure error
            throw new Error(`RSchema '${this.getSchemaRef()}' is not present in the replica`);
        }
        return obj as RSchema;
    }

    // The schema version observed at a group position: the pinned genesis
    // version unioned with ref-advances at or below `at` (widened by barriers
    // concurrent to `at` when observed from `from`). The union with the
    // pinned version makes the create itself an implicit first deploy.
    async resolveSchemaVersion(at: Version, from?: Version): Promise<Version> {
        const scopedDag = await this.getScopedDag();
        const advanced = await resolveRefVersionAtPosition(scopedDag, this.getSchemaRef(), at, from ?? at);

        const resolved = version(...this.getPinnedSchemaVersion());
        for (const hash of advanced) resolved.add(hash);
        return resolved;
    }

    // The effective schema at a group position (RSchemaImpl caches the
    // resolution per normalized version).
    async resolveSchemaView(at: Version, from?: Version): Promise<RSchemaView> {
        const schema = await this.getSchemaObject();
        const resolved = await this.resolveSchemaVersion(at, from);
        return schema.getView(resolved, resolved);
    }

    // Member access

    // Construct (and cache) the nested RTable without an existence check:
    // used by views anchored at arbitrary positions and by validation.
    makeTable(name: string): RTableImpl {
        let table = this.tables.get(name);
        if (table === undefined) {
            table = new RTableImpl(deriveTableId(this.createOpId, name), name, this);
            this.tables.set(name, table);
        }
        return table;
    }

    async getTable(name: string): Promise<RTableImpl> {
        const scopedDag = await this.getScopedDag();
        const schemaView = await this.resolveSchemaView(await scopedDag.getFrontier());
        if (!schemaView.hasTable(name)) {
            throw new Error(`Table '${name}' does not exist in the effective schema at the group frontier`);
        }
        return this.makeTable(name);
    }

    // Cross-group resolution: a bound foreign group's member-table view at the
    // foreign version this group observes at (`at`, `from`). `at`/`from` are
    // THIS group's positions; the foreign version is resolved through
    // ref-advances of the foreign group id (observe ops), then the foreign
    // group resolves its OWN schema at that version. Returns undefined for an
    // unbound name or a table absent at the foreign version (a missing
    // reference: the caller treats the FK target / exists atom as not-live).
    // Throws only if the bound group OBJECT is not present in the replica.
    //
    // FK reach across the group boundary recurses into the foreign group's
    // verdicts within the same evaluation; the group-namespaced keys keep the
    // two groups' entries distinct.
    //
    // Public entry: mints a fresh void evaluation (validation / view / auth call
    // sites are top-level and never filter voided observes, so this wrapper
    // hard-codes filterVoided=false). The internal, evaluation-threaded variant is
    // resolveForeignTableViewIn.
    async resolveForeignTableView(
        groupName: string, table: string, at: Version, from: Version,
    ): Promise<RTableView | undefined> {
        return this.resolveForeignTableViewIn(freshVerdictEvaluation(), groupName, table, at, from, false);
    }

    // Cross-group table resolution threaded with the caller's void evaluation.
    //
    // `filterVoided` (view-time enforcement only): see through observations
    // VOIDED at this `from` in the observed-version fold (Layer 2 of the
    // observe gate), so a back-dated observation by a former principal
    // contributes no foreign state. Only the void computation (diagnoseEntry)
    // enables it; the public wrapper leaves it false (the geometric
    // resolution). Note the observed `from` stays geometric: voided-observe
    // filtering is an `at`-fold property (which versions participate), not a
    // negative-evidence horizon.
    async resolveForeignTableViewIn(
        evaluation: VerdictEvaluation<OpVoidDetail>, groupName: string, table: string, at: Version, from: Version, filterVoided: boolean,
    ): Promise<RTableViewImpl | undefined> {
        const groupId = this.getBindings()[groupName];
        if (groupId === undefined) return undefined;   // unbound name

        const foreign = await this.loadForeignGroup(groupId, groupName);

        const dag = await this.getScopedDag();
        const isLive = filterVoided ? (h: B64Hash) => this.isObserveLive(evaluation, groupId, h, from) : undefined;
        const foreignAt = await resolveRefVersionAtPosition(dag, groupId, at, from, isLive);
        const foreignFrom = await resolveRefVersionAtPosition(dag, groupId, from, from);

        const foreignSchema = await foreign.resolveSchemaView(foreignAt, foreignFrom);
        if (!foreignSchema.hasTable(table)) return undefined;   // missing table

        return new RTableViewImpl(foreign.makeTable(table), foreignAt, foreignFrom, evaluation);
    }

    // Whether the observation entry `entryHash` (a ref-advance of bound group
    // `groupId`) is LIVE at this `from` horizon: ungated bindings are always
    // live; a gated binding consults the at-use observe gate (the observe's
    // verdict). Used as the `isLive` filter for the Layer 2 observed-version
    // fold; threaded with the caller's evaluation.
    private async isObserveLive(evaluation: VerdictEvaluation<OpVoidDetail>, groupId: B64Hash, entryHash: B64Hash, from: Version): Promise<boolean> {
        if (this.observeGateFor(groupId) === undefined) return true;   // ungated
        return this.observeVerdictLive(evaluation, entryHash, from);
    }

    // An observe's verdict never rests on an open component: bindings form a
    // DAG, so its gate reads only the observed group, and its widening reads
    // only observes strictly above it (see resolveObserveGateRefAt).
    private async observeVerdictLive(evaluation: VerdictEvaluation<OpVoidDetail>, entryHash: B64Hash, from: Version): Promise<boolean> {
        const verdict = await this.resolveVerdict(evaluation, entryHash, from);
        if (verdict.status === 'undecided') {
            throw new Error(`observe '${entryHash}' is undecided: an observe is never on a verdict cycle`);
        }
        return verdict.status === 'live';
    }

    // Evaluate a binding's canObserve gate in the OBSERVED group's frame
    // (frame rebasing): the gate's exists / $author atoms read the foreign
    // group's tables at the observed foreign version (refAt, refFrom). 'object'
    // context (no subject row). Returns true when the binding is ungated. Both
    // the validation path and the at-use path call this with their own anchors.
    // Public entry: mints a fresh void evaluation. The internal, evaluation-threaded
    // variant is evaluateObserveGateIn.
    async evaluateObserveGate(
        refId: B64Hash, author: KeyId | undefined, refAt: Version, refFrom: Version,
    ): Promise<boolean> {
        const truth = await this.evaluateObserveGateIn(freshVerdictEvaluation(), refId, author, refAt, refFrom);
        if (truth.status === 'undecided') throw new Error('an observe gate is undecided outside the component being solved');
        return truth.status === 'true';
    }

    async evaluateObserveGateIn(
        evaluation: VerdictEvaluation<OpVoidDetail>, refId: B64Hash, author: KeyId | undefined, refAt: Version, refFrom: Version,
    ): Promise<Truth> {
        const gate = this.observeGateFor(refId);
        if (gate === undefined) return TRUE;   // ungated binding

        const foreign = await this.loadForeignGroup(refId, this.bindingNameForId(refId));
        return evaluatePredicateTruth(gate, {
            getTableView: async (table) => new RTableViewImpl(foreign.makeTable(table), refAt, refFrom, evaluation),
            getForeignTableView: (groupName, table) =>
                foreign.resolveForeignTableViewIn(evaluation, groupName, table, refAt, refFrom, false),
            author,
            context: 'object',
        });
    }

    // G-upward filtered widening of the observed version for the at-use observe
    // gate (Layer 1). Base is the causal (geometric) version published by the
    // observe at `opPos`; concurrent observation barriers widen it ONLY when
    // their published version STRICTLY dominates the base in the foreign DAG
    // AND they are themselves live. Strict G-domination makes the recursion
    // ascend the foreign version, so it is acyclic and terminating by
    // construction (no back-edge: an equal/below or concurrent observe is never
    // a widening candidate), and benign G-incomparable concurrent observes
    // never recurse into each other. A negative edge (a revoke of this observe's
    // author) rides a strictly-dominating version under use-before-revoke, so
    // restricting to G-upward loses no security-relevant widening.
    //
    // The walk sees through void candidates, so a void observe does not hide a
    // live one below it. Liveness is asked only of candidates strictly above
    // the base (the recursion keeps ascending). A candidate that is not
    // strictly above stops the walk and is dropped: an A-predecessor's version
    // is at or below its successor's (write-time monotonicity), so nothing
    // below it can be strictly above either.
    async resolveObserveGateRefAt(evaluation: VerdictEvaluation<OpVoidDetail>, refId: B64Hash, opPos: Version, from: Version): Promise<Version> {
        const dag = await this.getScopedDag();
        const base = await resolveRefVersionAtPosition(dag, refId, opPos, opPos);   // causal base (no widening)

        const foreignDag = await this.getForeignGroupCausalDag(refId);
        const refAt = version(...base);

        const strictlyAbove = new Map<B64Hash, Version | undefined>();
        const widening = async (z: B64Hash, entry: { payload: json.Literal }): Promise<Version | undefined> => {
            if (!strictlyAbove.has(z)) {
                let vz: Version | undefined;
                if (isRefAdvancePayload(entry.payload)) {
                    const candidate = extractRefVersion(entry.payload as RefAdvancePayload);
                    // strictly G-above the base: vz >= base AND base !>= vz
                    if (await refVersionAtOrAbove(foreignDag, candidate, base)
                        && !await refVersionAtOrAbove(foreignDag, base, candidate)) {
                        vz = candidate;
                    }
                }
                strictlyAbove.set(z, vz);
            }
            return strictlyAbove.get(z);
        };

        const concurrent = await findConcurrentRefAdvanceBarriers(dag, refId, opPos, from,
            async (z, entry) => (await widening(z, entry)) === undefined || await this.observeVerdictLive(evaluation, z, from));
        for (const z of concurrent) {
            const entry = await dag.loadEntry(z);
            if (entry === undefined) continue;
            const vz = await widening(z, entry);
            if (vz === undefined) continue;
            for (const h of vz) refAt.add(h);
        }

        return refAt;
    }

    private async loadForeignGroup(groupId: B64Hash, groupName?: string): Promise<RTableGroupImpl> {
        const obj = await this.ctx.getObject(groupId);
        if (obj === undefined) {
            const label = groupName !== undefined ? `'${groupName}' -> '${groupId}'` : `'${groupId}'`;
            throw new Error(`Bound group ${label} is not present in the replica`);
        }
        return obj as RTableGroupImpl;
    }

    // The causal DAG of a bound foreign group, for ref-advance monotonicity
    // (observe validation). A missing object throws (infrastructure error).
    async getForeignGroupCausalDag(groupId: B64Hash): Promise<CausalDag> {
        return (await this.loadForeignGroup(groupId)).getCausalDag();
    }

    // The foreign version this group observes for a bound group at (at, from):
    // the resolved ref-advance version on the group DAG. Used by the group VIEW
    // (resolveRefVersion) and by cross-group resolution.
    async resolveObservedForeignVersion(groupId: B64Hash, at: Version, from: Version): Promise<Version> {
        const dag = await this.getScopedDag();
        return resolveRefVersionAtPosition(dag, groupId, at, from);
    }

    // KeyLookup: the publicKey registered for `keyId` through this group's
    // selected identity provider, anchored at group position `at`, LIVENESS-
    // BYPASSED (a raw provider read; see RTableViewImpl.rawProviderPublicKey).
    // Signature verification calls this at the op's own (at, at) position.
    //   - no provider configured            -> undefined (caller: reject authored ops)
    //   - local provider                    -> raw read of the local provider table
    //   - 'group.table' provider PRESENT but
    //     key absent / table not a provider  -> undefined (caller: fail-closed reject)
    //   - bound provider OBJECT not present  -> THROWS (caller/sync: defer)
    // The verdict is monotone: registration is causal-permanent and read past
    // liveness, so a key resolvable at `at` stays resolvable.
    async resolveAuthorKey(keyId: KeyId, at: Version): Promise<PublicKey | undefined> {
        const providerRef = this.getIdProvider();
        if (providerRef === undefined) return undefined;

        const [groupName, table] = splitTableRef(providerRef);
        if (groupName === undefined) return this.providerPublicKeyAt(table, keyId, at);

        const groupId = this.getBindings()[groupName];
        if (groupId === undefined) return undefined;   // unbound (create-time validated; defensive)
        const foreign = await this.loadForeignGroup(groupId, groupName);   // missing object -> throw -> defer
        const foreignAt = await this.resolveObservedForeignVersion(groupId, at, at);
        return foreign.providerPublicKeyAt(table, keyId, foreignAt);
    }

    // The publicKey registered for `keyId` in this group's provider table
    // `table` at version `at`, liveness-bypassed; undefined when the table is
    // absent or not a provider at `at`. Shared by resolveAuthorKey and by the
    // types that observe a group for their identities (rfiles).
    async providerPublicKeyAt(table: string, keyId: KeyId, at: Version): Promise<PublicKey | undefined> {
        // rawProviderPublicKey never reaches the void guard, but the view
        // constructor requires an evaluation, so mint a throwaway one.
        return new RTableViewImpl(this.makeTable(table), at, at, freshVerdictEvaluation()).rawProviderPublicKey(keyId);
    }

    // Deploy: THE schema deploy moment — a barrier ref-advance of the schema
    // ref. Monotonicity (and at-or-above-pinned) validated; when authored, the
    // deploy signature is verified at validation (the group's own provider,
    // then its deployKeys) and the group's canDeploy predicate is evaluated
    // against the verified author.
    async deploy(refVersion: Version, author?: OwnIdentity, at?: Version): Promise<B64Hash> {
        const prepared = await this.prepareDeploy(refVersion, author, at);

        if (this.selfValidate()) {
            const result = await this.validatePayload(prepared.payload, prepared.at);
            if (!result.valid) {
                throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
            }
        }

        const meta = await this.deriveDeployMeta(prepared.payload as unknown as RefAdvancePayload, prepared.at);
        return (await this.getScopedDag()).append(prepared.payload, meta, prepared.at);
    }

    deployNeedsAuthor(): boolean {
        const canDeploy = this.getCanDeploy();
        return canDeploy !== undefined && predicateReferencesAuthor(canDeploy);
    }

    observeNeedsAuthor(group: string | B64Hash): boolean {
        const gate = this.observeGateFor(this.resolveBoundGroupId(group));
        return gate !== undefined && predicateReferencesAuthor(gate);
    }

    // The deploy payload at `at` (defaults to the frontier), signed when an
    // author is given, without appending it: the catalog planner validates it
    // in a dry run first. The meta carries the ref-advance tags only; deploy()
    // derives the full meta when it appends (deriveDeployMeta).
    async prepareDeploy(refVersion: Version, author?: OwnIdentity, at?: Version): Promise<{ payload: json.LiteralMap; meta: MetaProps; at: Version }> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        if (author === undefined && this.deployNeedsAuthor()) {
            throw new Error("deploy must be authored when the group's canDeploy reads $author");
        }

        const { payload: refAdvance, meta } = prepareRefAdvance(this.getSchemaRef(), refVersion);
        const base: json.LiteralMap = {
            ...(refAdvance as unknown as json.LiteralMap),
            gate: json.toSet(await this.computeGateHashes(refVersion)),
        };
        const payload = author !== undefined
            ? await signPayloadHelper(base, author, at)
            : base;

        return { payload, meta, at };
    }

    // The meta of a schema deploy appended at `at`: the ref-advance tags, plus
    // the rows the deploy deletes because they do not honor an FK it adds or
    // retargets (see fkAdoptionKills and deriveDeployKillMeta). The local
    // deploy and sync ingestion (applyPayload) both derive it, on every
    // replica, and it is stored with the entry. Meta is unhashed and never
    // rewritten, so this derivation is protocol: changing it, or the liveness
    // rules it reads at `at`, needs a store migration that re-applies entries.
    async deriveDeployMeta(payload: RefAdvancePayload, at: Version): Promise<MetaProps> {
        return { ...createRefAdvanceMeta(payload.refId), ...deriveDeployKillMeta(await this.fkAdoptionKills(payload, at)) };
    }

    // The rows a schema deploy at `at` deletes. An FK is adopted when the
    // deploy adds or retargets it on a table it neither creates nor resets,
    // over a column it does not re-create (a new column holds no old values).
    // Each row live at `at` whose value in an adopted FK is not the rowId of a
    // target live at `at` is deleted; a target table the deploy creates or
    // resets holds no old rows. The set is closed over the adopted FKs, so a
    // row pointing at a deleted row through one goes too (chains and
    // self-references). Rows pointing at a deleted row through an FK that
    // already held stay, as after any delete.
    private async fkAdoptionKills(payload: RefAdvancePayload, at: Version): Promise<DeployKills> {
        const before = await this.resolveSchemaView(at);
        const deployed = version(...this.getPinnedSchemaVersion());
        for (const hash of extractRefVersion(payload)) deployed.add(hash);
        const after = await (await this.getSchemaObject()).getView(deployed, deployed);

        const kept = (table: string): boolean => {
            const incarnation = before.getTableIncarnation(table);
            return incarnation !== undefined && incarnation === after.getTableIncarnation(table);
        };

        const adopted: { table: string; incarnation: IncarnationId; fks: [string, string][] }[] = [];
        for (const table of after.getTableNames()) {
            if (!kept(table)) continue;
            const beforeFks = before.getFKs(table);
            const fks = Object.entries(after.getFKs(table)).filter(([column, target]) =>
                beforeFks[column] !== target
                && before.getColumnIncarnation(table, column) === after.getColumnIncarnation(table, column));
            if (fks.length > 0) adopted.push({ table, incarnation: before.getTableIncarnation(table)!, fks });
        }
        if (adopted.length === 0) return new Map();

        const killed = new Map<string, Set<B64Hash>>();
        const queue: [string, B64Hash][] = [];
        const kill = (table: string, rowId: B64Hash): void => {
            let rows = killed.get(table);
            if (rows === undefined) killed.set(table, rows = new Set());
            if (rows.has(rowId)) return;
            rows.add(rowId);
            queue.push([table, rowId]);
        };
        // target table -> target rowId -> the rows pointing at it through an adopted FK
        const dependents = new Map<string, Map<B64Hash, [string, B64Hash][]>>();

        for (const { table, fks } of adopted) {
            const view = await this.makeTable(table).getView(at, at);
            for (const rowId of await view.liveRowIds()) {
                const row = await view.getRow(rowId);
                if (row === undefined) continue;
                for (const [column, target] of fks) {
                    const value = row.values[column];
                    if (value === undefined) continue;   // nullable / absent: unconstrained
                    if (typeof value !== 'string' || !await this.adoptedTargetLive(target, value, at, kept)) {
                        kill(table, rowId);
                        break;
                    }
                    const [groupName, targetTable] = splitTableRef(target);
                    if (groupName !== undefined) continue;   // this deploy deletes no foreign row
                    let byRow = dependents.get(targetTable);
                    if (byRow === undefined) dependents.set(targetTable, byRow = new Map());
                    let pointing = byRow.get(value);
                    if (pointing === undefined) byRow.set(value, pointing = []);
                    pointing.push([table, rowId]);
                }
            }
        }

        while (queue.length > 0) {
            const [table, rowId] = queue.shift()!;
            for (const [dependentTable, dependentRowId] of dependents.get(table)?.get(rowId) ?? []) {
                kill(dependentTable, dependentRowId);
            }
        }

        const kills: DeployKills = new Map();
        for (const { table, incarnation } of adopted) {
            const rowIds = killed.get(table);
            if (rowIds !== undefined) kills.set(table, { incarnation, rowIds });
        }
        return kills;
    }

    private async adoptedTargetLive(targetRef: string, rowId: B64Hash, at: Version, kept: (table: string) => boolean): Promise<boolean> {
        const [groupName, table] = splitTableRef(targetRef);
        if (groupName !== undefined) {
            const view = await this.resolveForeignTableView(groupName, table, at, at);
            return view !== undefined && await view.hasRow(rowId);
        }
        return kept(table) && await (await this.makeTable(table).getView(at, at)).hasRow(rowId);
    }

    // Observe a bound foreign group at `refVersion`: a BARRIER ref-advance of
    // that group's id. This is how cross-group FK / exists targets
    // become visible — the dependent observes the foreign group at a bounded
    // version, and resolves `group.table` through it (see
    // resolveForeignTableView). Barrier: an observation concurrent to a use,
    // visible from the view's `from`, widens the observed foreign version at
    // the merged frontier, so a concurrent foreign revoke / schema deploy voids
    // the cross-group use there (symmetric with intra-group concurrent barrier
    // revoke; the foreign-version resolver already passes (at, from), so this
    // is purely the barrier tag). `group` is a binding name or a bound group id.
    //
    // A claimed author is verified through this group's provider. When the
    // observed binding's canObserve gate reads $author, the observation must
    // be authored; the gate is evaluated at validation (and re-evaluated
    // at-use), exactly like a gated deploy.
    async observe(group: string | B64Hash, refVersion: Version, author?: OwnIdentity, at?: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        const groupId = this.resolveBoundGroupId(group);

        if (author === undefined && this.observeNeedsAuthor(groupId)) {
            throw new Error("observe must be authored when the binding's canObserve reads $author");
        }

        const base = createRefAdvancePayload(groupId, refVersion);
        const payload = author !== undefined
            ? await signPayloadHelper(base as unknown as json.LiteralMap, author, at)
            : base as unknown as json.LiteralMap;
        const meta = createRefAdvanceMeta(groupId);   // barrier (default)

        if (this.selfValidate()) {
            const result = await this.validatePayload(payload, at);
            if (!result.valid) {
                throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
            }
        }

        return scopedDag.append(payload, meta, at);
    }

    private resolveBoundGroupId(group: string | B64Hash): B64Hash {
        const bindings = this.getBindings();
        if (Object.prototype.hasOwnProperty.call(bindings, group)) return bindings[group];
        if (Object.values(bindings).includes(group)) return group;
        throw new Error(`'${group}' is not a bound group of this RTableGroup`);
    }

    // Single-entry atomic multi-table write. `writes` is ORDERED (the bundle
    // order, carried explicitly because entry hashing sorts map keys): op i's
    // FK conditions are checked at the sequential cut `at` ∪ earlier ops.
    // `author` signs the bundle once, as a whole; each op only carries the
    // `author` claim (no signature of its own), so ops cannot be lifted out or
    // recombined. The whole entry validates and applies as a unit
    // (all-or-nothing).
    async bundle(writes: BundleWrite[], author?: OwnIdentity, at?: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        const claimed: BundlePayload['writes'] = writes.map((write) => ({
            table: write.table,
            op: author !== undefined
                ? { ...(write.op as unknown as json.LiteralMap), author: author.keyId }
                : write.op as unknown as json.Literal,
        }));

        const base: BundlePayload = { action: 'bundle', writes: claimed };
        const payload = author !== undefined
            ? await signPayloadHelper(base as unknown as json.LiteralMap, author, at) as unknown as BundlePayload
            : base;

        const result = await this.validatePayload(payload, at);
        if (!result.valid) {
            throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
        }

        const schemaView = await this.resolveSchemaView(at);
        return scopedDag.append(payload, deriveBundleMeta(payload, schemaView), at);
    }

    // Whether an entry is VOID at this `from` horizon: row restrictions already
    // passed hard validation at the parent frontier, but are rechecked here at
    // the op's own position observed from `from` so concurrent barrier revokes
    // or schema/observation revisions can still void the entry. Written FK
    // columns use the same view-time path. Bundles are all-or-nothing. The
    // genesis create entry is fiat (never voided);
    // ref-advances carry no row restrictions. Entries whose verdicts depend on
    // each other are solved together (see the evaluation note above). Public
    // entry: mints a fresh void evaluation; the internal, evaluation-threaded
    // variant is entryStatusIn.
    async isEntryVoided(entryHash: B64Hash, from: Version): Promise<boolean> {
        return (await this.topLevelVerdict(entryHash, from)).status === 'void';
    }

    async explainEntryVoided(entryHash: B64Hash, from: Version): Promise<OpVoidDetail | undefined> {
        const verdict = await this.topLevelVerdict(entryHash, from);
        return verdict.status === 'void' ? verdict.reason : undefined;
    }

    // The entry's verdict within `evaluation`: undecided only while the entry
    // belongs to the component being solved.
    entryStatusIn(evaluation: VerdictEvaluation<OpVoidDetail>, entryHash: B64Hash, from: Version): Promise<Verdict<OpVoidDetail>> {
        return this.resolveVerdict(evaluation, entryHash, from);
    }

    private async topLevelVerdict(entryHash: B64Hash, from: Version): Promise<Verdict<OpVoidDetail>> {
        const verdict = await this.resolveVerdict(freshVerdictEvaluation(), entryHash, from);
        if (verdict.status === 'undecided') throw new Error(`entry '${entryHash}' is undecided at the top level`);
        return verdict;
    }

    // One body behind isEntryVoided / explainEntryVoided / entryStatusIn, so
    // boolean and explain cannot drift.
    private resolveVerdict(
        evaluation: VerdictEvaluation<OpVoidDetail>, entryHash: B64Hash, from: Version,
    ): Promise<Verdict<OpVoidDetail>> {
        // group-namespaced key: one evaluation may span bound foreign groups
        const key = this.createOpId + '|' + entryHash + '|' + [...from].sort().join(',');
        return evaluation.resolve(key, async () => {
            try {
                this.enterVerdictFrame();
                return await this.diagnoseEntry(evaluation, entryHash, from);
            } finally {
                this._verdictInflight--;
            }
        });
    }

    // Fail-safe (not a verdict): a lost evaluation would recurse forever through
    // fresh evaluations — an unbounded microtask chain no timer can interrupt — so
    // bound the in-flight diagnose runs and throw past it, turning the hang into
    // an immediate, self-explaining error. Called as the first statement inside
    // the try, so the caller's finally always balances the increment (even on
    // the throw).
    private enterVerdictFrame(): void {
        if (++this._verdictInflight > VERDICT_MAX_INFLIGHT) {
            throw new Error(
                `verdict recursion exceeded VERDICT_MAX_INFLIGHT (${VERDICT_MAX_INFLIGHT}) frames ` +
                `— a VerdictEvaluation was almost certainly dropped (a helper that received one called a ` +
                `minting wrapper instead of passing it on)`);
        }
    }

    // An entry is live when every op passes its restriction and FK reach, in
    // bundle order: the first check that fails gives the reason; an undecided
    // one leaves the entry undecided unless a later one fails. The views read
    // the entry itself as void: an op is not its own witness, and its own
    // delete does not hide its subject row.
    private async diagnoseEntry(evaluation: VerdictEvaluation<OpVoidDetail>, entryHash: B64Hash, from: Version): Promise<Verdict<OpVoidDetail>> {
        if (entryHash === this.createOpId) return { status: 'live' };   // genesis fiat

        const scopedDag = await this.getScopedDag();
        const entry = await scopedDag.loadEntry(entryHash);
        if (entry === undefined) return { status: 'live' };

        const payload = entry.payload as json.LiteralMap;
        if (isRefAdvancePayload(payload)) {
            return this.diagnoseObserve(evaluation, payload as unknown as RefAdvancePayload, entryHash, from);
        }

        const ops: { table: string; op: RowOpPayload }[] = [];
        const isBundle = payload['action'] === 'bundle';
        if (payload['action'] === 'row') {
            const envelope = payload as RowEnvelopePayload;
            ops.push({ table: envelope.table, op: envelope.op as RowOpPayload });
        } else if (isBundle) {
            for (const write of (payload as BundlePayload).writes) {
                ops.push({ table: write.table, op: write.op as RowOpPayload });
            }
        } else {
            return { status: 'live' };   // create / unknown
        }

        const opPos = version(entryHash);
        const schemaView = await this.resolveSchemaView(opPos, from);
        const getTableView = async (table: string) => new RTableViewImpl(this.makeTable(table), opPos, from, evaluation, entryHash);
        const getForeignTableView = (group: string, table: string) =>
            this.resolveForeignTableViewIn(evaluation, group, table, opPos, from, true);

        const selfInserted = new Map<string, Set<B64Hash>>();
        const selfDeleted = new Map<string, Set<B64Hash>>();
        for (const { table, op } of ops) {
            const into = op.action === 'insert' ? selfInserted : op.action === 'delete' ? selfDeleted : undefined;
            if (into === undefined) continue;
            if (!into.has(table)) into.set(table, new Set());
            into.get(table)!.add(op.rowId);
        }
        const localTargetProvided = (table: string, rowId: B64Hash): boolean | undefined => {
            if (selfDeleted.get(table)?.has(rowId)) return false;
            if (selfInserted.get(table)?.has(rowId)) return true;
            return undefined;
        };

        const authenticated = this.getIdProvider() !== undefined;
        const undecided: Truth[] = [];
        for (const [index, { table, op }] of ops.entries()) {
            const restriction = await explainRowOpRestriction(
                op, table, schemaView, authenticated, getTableView, getForeignTableView,
            );
            if (restriction.failure !== undefined) {
                // row-not-live is an explain alias for restriction failure when
                // enforced liveness is absent; it must not run before restriction
                // diagnosis (valid deletes whose rules pass without a live target
                // row would otherwise be voided incorrectly).
                if ((op.action === 'update' || op.action === 'delete')
                    && localTargetProvided(table, op.rowId) !== true
                    && (await (await getTableView(table)).rowLiveness(op.rowId)).truth.status === 'false') {
                    const detail: OpVoidDetail = {
                        kind: 'row-not-live',
                        table,
                        action: op.action,
                        rowId: op.rowId,
                    };
                    return { status: 'void', reason: isBundle ? { kind: 'bundle', index, detail } : detail };
                }
                const detail: OpVoidDetail = {
                    kind: 'restriction',
                    table: restriction.failure.table,
                    action: restriction.failure.action,
                    rowId: restriction.failure.rowId,
                    rule: restriction.failure.rule,
                };
                return { status: 'void', reason: isBundle ? { kind: 'bundle', index, detail } : detail };
            }
            undecided.push(restriction.truth);

            const fk = await explainRowOpFKReach(
                op, table, schemaView, getTableView, getForeignTableView, localTargetProvided,
            );
            if (fk.failure !== undefined) {
                const detail: OpVoidDetail = {
                    kind: 'fk',
                    table: fk.failure.table,
                    action: fk.failure.action,
                    rowId: fk.failure.rowId,
                    column: fk.failure.column,
                    targetRef: fk.failure.targetRef,
                    targetRowId: fk.failure.targetRowId,
                };
                return { status: 'void', reason: isBundle ? { kind: 'bundle', index, detail } : detail };
            }
            undecided.push(fk.truth);
        }
        const truth = allTruth(undecided);
        return truth.status === 'undecided' ? truth : { status: 'live' };
    }

    private async diagnoseObserve(
        evaluation: VerdictEvaluation<OpVoidDetail>, payload: RefAdvancePayload, entryHash: B64Hash, from: Version,
    ): Promise<Verdict<OpVoidDetail>> {
        const refId = payload.refId;
        if (refId === this.getSchemaRef()) return { status: 'live' };
        const gate = this.observeGateFor(refId);
        if (gate === undefined) return { status: 'live' };

        const dag = await this.getScopedDag();
        const opPos = version(entryHash);
        const refAt = await this.resolveObserveGateRefAt(evaluation, refId, opPos, from);
        const refFrom = await resolveRefVersionAtPosition(dag, refId, from, from);
        const author = extractAuthor(payload as unknown as json.LiteralMap);

        const truth = await this.evaluateObserveGateIn(evaluation, refId, author, refAt, refFrom);
        if (truth.status === 'true') return { status: 'live' };
        if (truth.status === 'undecided') return truth;

        const binding = this.bindingNameForId(refId);
        return { status: 'void', reason: { kind: 'observe-gate', binding: binding ?? refId, rule: gate } };
    }

    // RObject interface

    async validatePayload(payload: Payload, at: Version): Promise<ValidationResult> {
        return validateTableGroupPayload(payload, { mode: 'op', group: this, at });
    }

    async applyPayload(payload: Payload, at: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();

        if (isRefAdvancePayload(payload)) {
            const refPayload = payload as unknown as RefAdvancePayload;
            // both ref-advance flavors are barriers: the schema
            // deploy AND a foreign-group observation revise the merged
            // frontier, so a concurrent deploy / foreign revoke voids a
            // concurrent use there (see observe + view.ts schemaView).
            const meta = refPayload.refId === this.getSchemaRef()
                ? await this.deriveDeployMeta(refPayload, at)
                : createRefAdvanceMeta(refPayload.refId);
            return scopedDag.append(payload, meta, at);
        }

        const action = (payload as json.LiteralMap)['action'];

        if (action === 'row') {
            const envelope = payload as RowEnvelopePayload;
            const schemaView = await this.resolveSchemaView(at);
            return scopedDag.append(payload, deriveEnvelopeMeta(envelope, schemaView), at);
        }

        if (action === 'bundle') {
            const bundle = payload as BundlePayload;
            const schemaView = await this.resolveSchemaView(at);
            return scopedDag.append(payload, deriveBundleMeta(bundle, schemaView), at);
        }

        throw new Error("Invalid table group action in payload: " + action);
    }

    async getView(at?: Version, from?: Version): Promise<RTableGroupViewContract> {
        const scopedDag = await this.getScopedDag();

        at = at ?? await scopedDag.getFrontier();
        from = from ?? await scopedDag.getFrontier();

        // the view resolves the schema eagerly: getSchemaView() is sync
        const schemaVersion = await this.resolveSchemaVersion(at, from);
        const schema = await this.getSchemaObject();
        const schemaView = await schema.getView(schemaVersion, schemaVersion);

        return new RTableGroupViewImpl(this, at, from, schemaVersion, schemaView);
    }

    // Delta strategy (see ./delta.ts): 'bounded' (default; projected-meet walk)
    // or 'full' (genesis recompute, the reference implementation for parity).
    setDeltaStrategy(strategy: RTableGroupDeltaStrategy): void {
        this.deltaStrategy = strategy;
    }

    // The objects this group OBSERVES — its RSchema plus every bound foreign
    // group — the referenced floors of the bounded revision bound (each can
    // revise an at-use verdict below the fork meet, so each is projected and
    // their GLB taken; see combineObserverRevisionBounds).
    async getObservedObjects(): Promise<RObject[]> {
        const objects: RObject[] = [await this.getSchemaObject()];
        for (const groupId of Object.values(this.getBindings())) {
            objects.push(await this.loadForeignGroup(groupId));
        }
        return objects;
    }

    // Root-only delta orchestration (bounds analysis + walk + compose). A
    // member table does not lead a delta; it participates via
    // createDeltaAccumulator (see RTableImpl).
    async computeDelta(start: Version, end: Version): Promise<RTableGroupDelta> {
        const rawDag = await this.ctx.getDag(this.createOpId, this.backendLabel);
        if (rawDag === undefined) throw new Error(`DAG '${this.createOpId}' not found`);
        return computeRTableGroupDelta(this, rawDag, this.deltaStrategy, start, end);
    }

    createDeltaAccumulator(start: Version, end: Version): DeltaAccumulator {
        return new RTableGroupDeltaAccumulator(this, start, end);
    }

    extractForeignDeps(payload: Payload, _at: Version): ForeignDep[] | undefined {
        if (isRefAdvancePayload(payload)) {
            const refPayload = payload as unknown as RefAdvancePayload;
            const deps: ForeignDep[] = [{
                objectId: refPayload.refId,
                requiredHashes: [...extractRefVersion(refPayload)],
            }];
            // A schema deploy also waits for this replica's gate to admit its
            // version. The hashes come from the payload; validation checks
            // they mirror the version. A malformed gate adds no dep (and
            // fails validation).
            const gate = (payload as json.LiteralMap)['gate'];
            if (refPayload.refId === this.getSchemaRef() && gate !== undefined && json.checkFormat(deployGateSetFormat, gate)) {
                deps.push({ objectId: this.getDeployGateId(), requiredHashes: [...json.fromSet(gate as json.Set)] });
            }
            return deps;
        }

        // 'create' is never seen here: validatePayload rejects a create on an
        // existing group, so sync never routes one through the op-level gate. A
        // group's genesis deps (schema at pinned version, binding targets) are
        // declared by rTableGroupFactory.extractCreationForeignDeps instead.

        // Pin-or-inherit invariant: a row / bundle op reads the schema at its own
        // position and may reach bound foreign groups through cross-group FK /
        // exists targets, but every cross-object hash it reads is already pinned
        // either by this op's own prevs (same-DAG, always gated) or by a causally
        // prior gated op -- the create pins the schema genesis, each deploy pins
        // its new schema version, each observe pins the foreign version it reads.
        // So gating on object PRESENCE (schema, bound groups) is sufficient here;
        // re-listing the resolved hashes per op would be redundant work. Any new
        // op type that reads cross-object state MUST preserve this: pin the hashes
        // it reads, or be causally after an op that did.
        const deps: ForeignDep[] = [{ objectId: this.getSchemaRef(), requiredHashes: [] }];
        for (const target of Object.values(this.getBindings())) {
            deps.push({ objectId: target, requiredHashes: [] });
        }
        return deps;
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

    // NestingParent interface

    async getScopedDagForChild(childId: B64Hash): Promise<ScopedDag> {
        const name = await this.tableNameForId(childId);
        const parentScopedDag = await this.getScopedDag();
        return new NestedScopedDag(parentScopedDag, new TableScope(this, name));
    }

    async getCreationDagForChild(_childId: B64Hash, _at: Version, _addPayload: Payload): Promise<ScopedDag> {
        throw new Error("RTables are never created (they exist by schema)");
    }

    private async tableNameForId(childId: B64Hash): Promise<string> {
        // a cached table resolves directly; otherwise match against the
        // effective table set at the frontier
        for (const [name, table] of this.tables) {
            if (table.getId() === childId) return name;
        }

        const scopedDag = await this.getScopedDag();
        const schemaView = await this.resolveSchemaView(await scopedDag.getFrontier());
        for (const name of schemaView.getTableNames()) {
            if (deriveTableId(this.createOpId, name) === childId) return name;
        }

        throw new Error(`No member table matches child id '${childId}'`);
    }

    async destroy(): Promise<void> {
        for (const table of this.tables.values()) {
            await table.destroy();
        }
        this.tables.clear();
        this._scopedDag = undefined;
        this._causalDag = undefined;
    }
}

export { RTableGroupViewImpl } from "./view.js";

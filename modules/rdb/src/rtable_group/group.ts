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
//       - the RSchema ref: THE SCHEMA DEPLOY MOMENT, as a barrier. When the
//         group declares `canDeploy`, that predicate is derived from the
//         group's create payload (mandatory) and the op carries
//         author/signature as extra fields; the deploy signature is verified
//         at validation (against the group's own provider) and the predicate
//         is then evaluated against the verified author.
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

import {
    Payload, RObjectFactory, RContext, LoadObjectOptions, NestingParent,
    Version, version, ForeignDep, Event, DeltaAccumulator, RObject,
    formatValidationFailure, validationFailure, ValidationRejectedError, ValidationResult,
} from "@hyper-hyper-space/hhs3_mvt";
import { RootScopedDag, NestedScopedDag, ScopedDag, CausalDag } from "@hyper-hyper-space/hhs3_mvt";
import {
    isRefAdvancePayload, extractRefVersion, extractAuthor, prepareRefAdvance, createRefAdvanceMeta,
    createRefAdvancePayload, resolveRefVersionAtPosition, findConcurrentRefAdvanceBarriers,
    refVersionAtOrAbove,
} from "@hyper-hyper-space/hhs3_mvt";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";
import { signPayload as signPayloadHelper } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchema, RSchemaView } from "../rschema/interfaces.js";
import type { Predicate } from "../rschema/payload.js";
import { splitTableRef } from "../rschema/payload.js";
import { RTableImpl } from "../rtable/rtable.js";
import { deriveTableId } from "../rtable/hash.js";
import type { RowOpPayload } from "../rtable/payload.js";
import type { RTableView } from "../rtable/interfaces.js";
import { RTableViewImpl } from "../rtable/view.js";

import type { RTableGroup as RTableGroupContract, RTableGroupView as RTableGroupViewContract, BundleWrite } from "./interfaces.js";
import { CreateTableGroupPayload, RowEnvelopePayload, BundlePayload, RTABLE_GROUP_TYPE_ID } from "./payload.js";
import { TableScope, deriveCreateMeta, deriveEnvelopeMeta, deriveBundleMeta } from "./scopes.js";
import { validateTableGroupPayload } from "./validate_ops.js";
import { evaluatePredicate, explainRowOpRestriction, explainRowOpFKReach } from "./predicates.js";
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

    // The void-recursion cycle guard. Voiding can recurse: a restriction/exists
    // (a caps insert gated by exists over caps, whose witness insert is itself
    // gated, ...) AND FK reach (a write whose FK target's own
    // liveness depends on another voided write). Both fold into ONE least-
    // fixpoint: a cycle DENIES (the ENTIRE cycle is treated as VOID). This makes
    // a self-granting op (its own witness) void — authority must root at a
    // genesis fiat row, which is never voided — and an FK reference cycle resolve
    // to DENY (replacing the former assume-live greatest-fixpoint FK guard).
    //
    // Deny-the-whole-cycle is the basic, replica-convergent semantics: it is a
    // pure function of the entry and `from`, so every replica agrees regardless
    // of which node a top-level query first touches. A delete-vs-delete negation
    // cycle (mutual revocation) therefore resolves to BOTH deletes voided (both
    // caps survive) — the conservative least fixpoint, not a single survivor.
    // NOTE: there is deliberately NO memo cache here. A cache keyed only by
    // (entry, from) would leak a traversal-dependent intermediate verdict across
    // independent top-level queries and break replica convergence; the guard
    // below is transient (added before recursing, removed in finally), so it
    // only ever detects a cycle within ONE computation. See VOID_SEMANTICS.md
    // for the logic-program framing and the future stratified/seniority design.
    private _voidVisiting: Set<string> = new Set();

    // Memoized inverse of the (injective) bindings map: group id -> binding
    // name. Injectivity is enforced at create-validation, so the inverse is a
    // well-defined function; built lazily on first use.
    private _bindingNameById: Map<B64Hash, string> | undefined;

    private deltaStrategy: RTableGroupDeltaStrategy = 'bounded';

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
    // FK reach across the group boundary recurses through the foreign group's
    // own least-fixpoint void guard (each group's _voidVisiting self-
    // terminates), so a mutual A->B->A FK ring resolves to DENY without a
    // shared cross-group guard.
    //
    // `filterVoided` (view-time enforcement only): drop observations VOIDED at
    // this `from` from the observed-version fold (Layer 2 of the observe gate),
    // so a back-dated observation by a former principal contributes no foreign
    // state. Validation / view / authentication call sites leave it false (the
    // geometric resolution); only computeEntryVoided enables it. Note the
    // observed `from` stays geometric: voided-observe filtering is an `at`-fold
    // property (which versions participate), not a negative-evidence horizon.
    async resolveForeignTableView(
        groupName: string, table: string, at: Version, from: Version, filterVoided: boolean = false,
    ): Promise<RTableView | undefined> {
        const groupId = this.getBindings()[groupName];
        if (groupId === undefined) return undefined;   // unbound name

        const foreign = await this.loadForeignGroup(groupId, groupName);

        const dag = await this.getScopedDag();
        const isLive = filterVoided ? (h: B64Hash) => this.isObserveLive(groupId, h, from) : undefined;
        const foreignAt = await resolveRefVersionAtPosition(dag, groupId, at, from, isLive);
        const foreignFrom = await resolveRefVersionAtPosition(dag, groupId, from, from);

        const foreignSchema = await foreign.resolveSchemaView(foreignAt, foreignFrom);
        if (!foreignSchema.hasTable(table)) return undefined;   // missing table

        return new RTableViewImpl(foreign.makeTable(table), foreignAt, foreignFrom);
    }

    // Whether the observation entry `entryHash` (a ref-advance of bound group
    // `groupId`) is LIVE at this `from` horizon: ungated bindings are always
    // live; a gated binding consults the at-use observe gate (the negation of
    // isEntryVoided's verdict for the observe). Used as the `isLive` filter for
    // the Layer 2 observed-version fold.
    private async isObserveLive(groupId: B64Hash, entryHash: B64Hash, from: Version): Promise<boolean> {
        if (this.observeGateFor(groupId) === undefined) return true;   // ungated
        return !await this.isEntryVoided(entryHash, from);
    }

    // Evaluate a binding's canObserve gate in the OBSERVED group's frame
    // (frame rebasing): the gate's exists / $author atoms read the foreign
    // group's tables at the observed foreign version (refAt, refFrom). 'object'
    // context (no subject row). Returns true when the binding is ungated. Both
    // the validation path and the at-use path call this with their own anchors.
    async evaluateObserveGate(
        refId: B64Hash, author: KeyId | undefined, refAt: Version, refFrom: Version,
    ): Promise<boolean> {
        const gate = this.observeGateFor(refId);
        if (gate === undefined) return true;   // ungated binding

        const foreign = await this.loadForeignGroup(refId, this.bindingNameForId(refId));
        return evaluatePredicate(gate, {
            getTableView: (table) => foreign.makeTable(table).getView(refAt, refFrom),
            getForeignTableView: (groupName, table) =>
                foreign.resolveForeignTableView(groupName, table, refAt, refFrom),
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
    async resolveObserveGateRefAt(refId: B64Hash, opPos: Version, from: Version): Promise<Version> {
        const dag = await this.getScopedDag();
        const base = await resolveRefVersionAtPosition(dag, refId, opPos, opPos);   // causal base (no widening)

        const foreignDag = await this.getForeignGroupCausalDag(refId);
        const refAt = version(...base);

        const concurrent = await findConcurrentRefAdvanceBarriers(dag, refId, opPos, from);
        for (const z of concurrent) {
            const entry = await dag.loadEntry(z);
            if (entry === undefined || !isRefAdvancePayload(entry.payload)) continue;
            const vz = extractRefVersion(entry.payload as RefAdvancePayload);

            // strictly G-above the base: vz >= base AND base !>= vz
            const above = await refVersionAtOrAbove(foreignDag, vz, base);
            if (!above) continue;
            const below = await refVersionAtOrAbove(foreignDag, base, vz);
            if (below) continue;   // equal / not strict -> not a widening candidate

            if (await this.isEntryVoided(z, from)) continue;   // skip voided concurrent observes

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
    //   - no provider configured            -> undefined (caller: no authentication)
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
        if (groupName === undefined) {
            return new RTableViewImpl(this.makeTable(table), at, at).rawProviderPublicKey(keyId);
        }

        const groupId = this.getBindings()[groupName];
        if (groupId === undefined) return undefined;   // unbound (create-time validated; defensive)
        const foreign = await this.loadForeignGroup(groupId, groupName);   // missing object -> throw -> defer
        const foreignAt = await this.resolveObservedForeignVersion(groupId, at, at);
        return new RTableViewImpl(foreign.makeTable(table), foreignAt, foreignAt).rawProviderPublicKey(keyId);
    }

    // Deploy: THE schema deploy moment — a barrier ref-advance of the schema
    // ref. Monotonicity (and at-or-above-pinned) validated; when authored, the
    // deploy signature is verified at validation (the group's own provider) and
    // the group's canDeploy predicate is evaluated against the verified author.
    async deploy(refVersion: Version, author?: OwnIdentity, at?: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        if (this.getCanDeploy() !== undefined && author === undefined) {
            throw new Error("deploy must be authored when the group declares canDeploy");
        }

        const { payload: base, meta } = prepareRefAdvance(this.getSchemaRef(), refVersion);
        const payload = author !== undefined
            ? await signPayloadHelper(base as unknown as json.LiteralMap, author)
            : base as unknown as json.LiteralMap;

        if (this.selfValidate()) {
            const result = await this.validatePayload(payload, at);
            if (!result.valid) {
                throw new ValidationRejectedError(formatValidationFailure(result.why), result.why);
            }
        }

        return scopedDag.append(payload, meta, at);
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
    // When the observed binding declares a canObserve gate, the observation
    // must be authored: the signature is verified and the gate evaluated at
    // validation (and re-evaluated at-use), exactly like a gated deploy.
    async observe(group: string | B64Hash, refVersion: Version, author?: OwnIdentity, at?: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        const groupId = this.resolveBoundGroupId(group);

        if (this.observeGateFor(groupId) !== undefined && author === undefined) {
            throw new Error("observe must be authored when the binding declares canObserve");
        }

        const base = createRefAdvancePayload(groupId, refVersion);
        const payload = author !== undefined
            ? await signPayloadHelper(base as unknown as json.LiteralMap, author)
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
    // `author` signs every op; the whole entry validates and applies as a
    // unit (all-or-nothing).
    async bundle(writes: BundleWrite[], author?: OwnIdentity, at?: Version): Promise<B64Hash> {
        const scopedDag = await this.getScopedDag();
        at = at ?? await scopedDag.getFrontier();

        const signed: BundlePayload['writes'] = [];
        for (const write of writes) {
            const op = author !== undefined
                ? await signPayloadHelper(write.op as unknown as json.LiteralMap, author)
                : write.op as unknown as json.Literal;
            signed.push({ table: write.table, op });
        }

        const payload: BundlePayload = { action: 'bundle', writes: signed };

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
    // ref-advances carry no row restrictions. Memoized per (entry, from);
    // a cycle on the authorization-recursion stack DENIES (treated as voided —
    // least fixpoint; see the _voidVisiting note above).
    async isEntryVoided(entryHash: B64Hash, from: Version): Promise<boolean> {
        const key = entryHash + '|' + [...from].sort().join(',');

        // A cycle on the authorization-recursion stack: DENY (least fixpoint —
        // the whole cycle is treated as voided). The guard is transient, so
        // this only fires within one top-level computation (see the field note).
        if (this._voidVisiting.has(key)) return true;

        this._voidVisiting.add(key);
        try {
            return (await this.diagnoseEntryVoided(entryHash, from)) !== undefined;
        } finally {
            this._voidVisiting.delete(key);
        }
    }

    async explainEntryVoided(entryHash: B64Hash, from: Version): Promise<OpVoidDetail | undefined> {
        const key = entryHash + '|' + [...from].sort().join(',');

        if (this._voidVisiting.has(key)) return { kind: 'authorization-cycle' };

        this._voidVisiting.add(key);
        try {
            return await this.diagnoseEntryVoided(entryHash, from);
        } finally {
            this._voidVisiting.delete(key);
        }
    }

    private async diagnoseEntryVoided(entryHash: B64Hash, from: Version): Promise<OpVoidDetail | undefined> {
        if (entryHash === this.createOpId) return undefined;   // genesis fiat

        const scopedDag = await this.getScopedDag();
        const entry = await scopedDag.loadEntry(entryHash);
        if (entry === undefined) return undefined;

        const payload = entry.payload as json.LiteralMap;
        if (isRefAdvancePayload(payload)) {
            return this.diagnoseObserveVoided(payload as unknown as RefAdvancePayload, entryHash, from);
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
            return undefined;   // create / unknown
        }

        const opPos = version(entryHash);
        const schemaView = await this.resolveSchemaView(opPos, from);
        const getTableView = (table: string) => this.makeTable(table).getView(opPos, from);
        const getForeignTableView = (group: string, table: string) =>
            this.resolveForeignTableView(group, table, opPos, from, true);

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

        for (const [index, { table, op }] of ops.entries()) {
            const restrictionFailure = await explainRowOpRestriction(
                op, table, schemaView, getTableView, getForeignTableView,
            );
            if (restrictionFailure !== undefined) {
                // row-not-live is an explain alias for restriction failure when
                // enforced liveness is absent; it must not run before restriction
                // diagnosis (valid deletes whose rules pass without a live target
                // row would otherwise be voided incorrectly).
                if ((op.action === 'update' || op.action === 'delete')
                    && localTargetProvided(table, op.rowId) !== true
                    && !(await (await getTableView(table)).hasRow(op.rowId))) {
                    const detail: OpVoidDetail = {
                        kind: 'row-not-live',
                        table,
                        action: op.action,
                        rowId: op.rowId,
                    };
                    return isBundle ? { kind: 'bundle', index, detail } : detail;
                }
                const detail: OpVoidDetail = {
                    kind: 'restriction',
                    table: restrictionFailure.table,
                    action: restrictionFailure.action,
                    rowId: restrictionFailure.rowId,
                    rule: restrictionFailure.rule,
                };
                return isBundle ? { kind: 'bundle', index, detail } : detail;
            }

            const fkFailure = await explainRowOpFKReach(
                op, table, schemaView, getTableView, getForeignTableView, localTargetProvided,
            );
            if (fkFailure !== undefined) {
                const detail: OpVoidDetail = {
                    kind: 'fk',
                    table: fkFailure.table,
                    action: fkFailure.action,
                    rowId: fkFailure.rowId,
                    column: fkFailure.column,
                    targetRef: fkFailure.targetRef,
                    targetRowId: fkFailure.targetRowId,
                };
                return isBundle ? { kind: 'bundle', index, detail } : detail;
            }
        }
        return undefined;
    }

    private async diagnoseObserveVoided(
        payload: RefAdvancePayload, entryHash: B64Hash, from: Version,
    ): Promise<OpVoidDetail | undefined> {
        const refId = payload.refId;
        if (refId === this.getSchemaRef()) return undefined;
        const gate = this.observeGateFor(refId);
        if (gate === undefined) return undefined;

        const dag = await this.getScopedDag();
        const opPos = version(entryHash);
        const refAt = await this.resolveObserveGateRefAt(refId, opPos, from);
        const refFrom = await resolveRefVersionAtPosition(dag, refId, from, from);
        const author = extractAuthor(payload as unknown as json.LiteralMap);

        const ok = await this.evaluateObserveGate(refId, author, refAt, refFrom);
        if (ok) return undefined;

        const binding = this.bindingNameForId(refId);
        return { kind: 'observe-gate', binding: binding ?? refId, rule: gate };
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
            return scopedDag.append(payload, createRefAdvanceMeta(refPayload.refId), at);
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
            return [{
                dagId: refPayload.refId,
                requiredHashes: [...extractRefVersion(refPayload)],
            }];
        }

        const action = (payload as json.LiteralMap)['action'];

        if (action === 'create') {
            const create = payload as CreateTableGroupPayload;
            const deps: ForeignDep[] = [{
                dagId: create.schemaRef,
                requiredHashes: [...json.fromSet(create.schemaVersion)],
            }];
            for (const target of Object.values(create.bindings ?? {})) {
                deps.push({ dagId: target, requiredHashes: [] });
            }
            return deps;
        }

        // row / bundle ops validate against the schema at their position, and
        // may reference bound foreign groups through cross-group FK / exists
        // targets. The schema is needed at the op's version; bound groups need
        // only be present (their observed versions are pinned by the observe
        // ref-advance ops, which carry their own version deps above).
        const deps: ForeignDep[] = [{ dagId: this.getSchemaRef(), requiredHashes: [] }];
        for (const target of Object.values(this.getBindings())) {
            deps.push({ dagId: target, requiredHashes: [] });
        }
        return deps;
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

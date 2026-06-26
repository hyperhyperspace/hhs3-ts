# MVT — Monotone-View Types

A type system for coordination-free replicable objects. MVTs are a formalism in which observations are monotonic but explicitly version-scoped, allowing historical views to be refined as additional information becomes available. They generalize CRDTs by cleanly separating the write path (validated, version-stamped payloads) from the read path (version-scoped views), enabling coordination-free approximations for applications in any domain.

This module defines the core interfaces, a DAG-based nesting mechanism, reference helpers for inter-object observation, and a concrete `RSet` type that exercises all of the above.

## Introduction

The architectural pattern underlying MVTs is:

 - The application operates on a local replica of the state, reading and updating as necessary
 - Any data invariants are checked optimistically against the local replica
 - Synchronization happens asynchronically, depending on peer availability / connectivity
 - If conflicting data is discovered during synchronization, deterministic application-defined rules are used to preserve data invariants & ensure convergence
 
 The last item is of course the most challenging. Let's start by defining two kinds of replication conflicts:
 
 _Accidental_ conflicts are a result of implementation choices, and are not a direct consequence of the problem at hand. For example, in a distributed "shopping cart" implementation, adding and removing an item from the cart concurrently can result in replica divergence if not implemented carefully. CRDTs are excellent at preventing accidental conflicts.
 
 _Essential_ conflicts are inherent to the application domain: a person cannot schedule two meetings at the same time, the balance of an account cannot be negative, a person can only edit the document if they have the right permissions, etc. When operating in a coordination-free environment, like the optimistic replica model described above, they are **technically unavoidable**.
 
Since we cannot, without coordination, prevent these conflicting updates from happening, we're left with two main options:

 - **Automated conflict resolution**: this has been widely explored, mainly in academia. R. Jefferson described in [Virtual time](https://dl.acm.org/doi/10.1145/3916.3988) (1985) a do-undo mechanism that replays updates in a deterministic order, cancelling the ones that break consistency. Terry et al, in [Managing update conflicts in Bayou, a weakly connected replicated storage system](https://people.eecs.berkeley.edu/~brewer/cs262b/update-conflicts.pdf) (1995) enhance that approach by supporting application-provided conflict resolution procedures, instead of cancellation. There are later improvements, but the essence of the idea remains the same.
 
 - **Conflict internalization**: we can make some conflicted states acceptable by the application, and just expose them through the UI where they can be manually resolved (e.g. what git and similar tools do for collaboration on source code).
 
 These approaches can be framed formally by using the [CALM theorem](https://arxiv.org/abs/1901.01930) (2019), that states that the problems with a coordination-free, consistent solution are exactly the same as those solvable in monotonic logic (or stated more informally: solvable by CRDTs). Under this light, we can see that both the automated and the manual conflict resolution strategies are essentially transforming a non-monotonic problem into a closely related one that has a monotonic (coordination-free) solution. We'll call this a _monotonic transformation_.

## Monotone View Types
 
**Monotone View Types** are a family of coordination-free, replicable data types that aim to prevent _accidental conflicts_ and help us address _essential conflicts_ through monotonic transformation. Application developers are not expected to use them directly. Instead, we'll provide tooling that will derive the appropriate replicable data types from specs (in the form of schema definitions, constraints, foreign-key relationships, etc.)

Monotone View Types are operation-based. A _version_ is defined as the set of operations that have been applied so far. Operations themselves are partially ordered by a happens-before relationship, like it's customary. Any set of operations that is downwards closed with respect to the happens-before relationship is considered a valid version.

While most operations are expected to be commutative, _non-commutative operations are allowed_, but they are marked explicitly. We call them **barrier ops**. 

Finally, _state is only inspected using views_, defined like this:

```
let view = obj.getView(at: Version, from: Version);

view.getValue(...);
```

To get a view, we must specify not only the version we want to inspect, but also _from what_ (later) version we want to inspect it. The contents of a view may then be adjusted by updates arriving later on (in the form of non-commutative **barrier ops**). This is naturally captured by the _from_ parameter.

We define `concurrent(to: Version, from: Version)` as the operations in `from` that are concurrent (i.e. not comparable under _happens-before_) with _all_ the operations in `to`.

Finally, we define `getView(at: Version, from: Version)` as the version that contains the union of all the operations in the set `at` and the **barrier operations** in `concurrent(at, from)`. Intuitively, to update the version `at` with anything relevant happening up to `from`, we need to add any barrier ops in `from` that happened concurrently with `at`.

The view mechanism ensures that **Monotone View Types** are coordination-free, and can be safely replicated, even if the underlying type is not (because of the barriers). However, the price we pay is having "non final" views.

To be of practical application, we require that whenever a new operation is applied at version `v`, we can derive a boundary that limits how far in the past other views may be affected:

```
let bound: Version = obj.getScopeBound(op: Operation);
```

This ensures that for any version `w` before `bound`, 

```
obj.getView(w, v) == obj.getView(w, v ∪ {op}) 
```

Therefore, when applying a new update, the set of versions that need to be revised is well specified.

Finally, the operational model with barriers + scoped views generalizes well when different levels of coordination are used. The outcome of a coordinated action can be encoded as a barrier op that rules out any concurrent modifications authored by peers that did not participate in the coordination scheme. The coordination protocol itself can be executed either operationally, modifying the state, or on a different channel.

## Composability: SOaD Architecture

Application state will normally be modeled using a collection of Monotone View Types. We've defined an architectural pattern, **State-Observation-as-Data** (SOaD), to maximize composability and reuse and to simplify reasoning, ensuring monotonicity by construction.

As an example, imagine a system composed by a `Cap` data type, that defines the set of user capabilities, and a series of types `A`, `B`, `C` with operations that are conditional to the right capabilities being present in `Cap`. We'll model concurrency in this system by enriching the state of each of `A`, `B` and `C` with a reference to the last known version of `Cap`. A _reference update_ operation to move this reference forward will also be added to each of them. The procedure to update `Cap` will now need logic to move this reference forward in all the types that _observe_ it, or alternatively this can by done spontaneously whenever any peer that replicates `A`, `B` or `C`discovers the new version of `Cap`.

Since references to foreign versions can only be moved forward, it's easy to see that the _reference update_ operations will be well defined. If two branches of `A`, `B` or `C` have received different versions of `Cap`, they can be reconciled by taking the union of both versions as the branches are merged.

The _reference update_ operations will need to be **barrier ops**, since we do not want to allow a replica that just refuses to sync `Cap` to keep using an old set of capabilities. By making the reference updates barriers, we ensure that when the versions where these untimely capability uses happened are observed from later on (after the reference to 
`Cap` has been merged), they will be re-evaluated using the right capabilities. This is a consequence of the scoped views in MVTs.

This procedure can be generalized to any number of types and state dependencies, as long as the dependency graph is acyclic. In its purest form, the system can be designed with the reference update operations as the _only_ non-commutative barrier ops.

It also fosters re-use of types, since the observed type is unaware of where and how it is being observed. Only the observers are modified, by being enriched with state references. Furthermore, this provides an interesting application interoperability mechanism, where an app can depend on parts of the state of another. This is an interesting data-centric alternative to integration via APIs or other execution-driven schemes.

It's interesting to notice how in traditional systems observation is _implicit_ and managed by the runtime, using metadata that's hidden from the application (locks, snapshots, isolation levels), while in this MVT-based concurrency resolution pattern it is _explicit_ and stored _as data_.

## Implementation

The core MVT interfaces and DAG-based nesting mechanism live in this module. The [**dag** module](../dag) provides the fundamental algorithms (see `findForkPosition`, `findMinimalCover`, `findCoverWithFilter` and `findConcurrentCoverWithFilter`) to support MVTs that work by finding filtered minimal covers over the DAG.

As an example, a Monotone View Replicated Set is provided in the [**std_types** module](../std_types/src/types/rset.ts). It supports both plain and barrier additions and deletions, showcasing how non-monotonic behavior can be transformed through the scoped view mechanism.

Nesting at the DAG level is supported for MVTs. Operations for the inner instance are wrapped and inserted into the outer instance's DAG. The Monotone View Replicated Set was extended to support nesting (sets of sets of arbitrary depth). See the [**std_types** implementation](../std_types/src/types/rset.ts) for details.

## Core interface: `RObject`

An `RObject` (Replicable Object) encapsulates a DAG-backed history log. It provides methods for both writing and reading state:

```typescript
type RObject = {
    getId(): B64Hash;
    getType(): string;

    // writing
    validatePayload(payload: Payload, at: Version): Promise<ValidationResult>;
    applyPayload(payload: Payload, at: Version): Promise<B64Hash>;

    // reading (version-scoped)
    getView(at?: Version, from?: Version): Promise<View>;
    computeDelta(start: Version, end: Version): Promise<Delta>;

    // DAG access (logical history vs broader causal structure)
    getScopedDag(): Promise<ScopedDag>;
    getCausalDag(): Promise<CausalDag>;

    // inter-DAG dependency discovery (used by the sync layer)
    extractForeignDeps(payload: Payload, at: Version): ForeignDep[] | undefined;

    subscribe(callback: (event: Event) => void): void;
    unsubscribe(callback: (event: Event) => void): void;

    getBackendLabel(): string;
    destroy(): Promise<void>;
}
```

- `Version` is a DAG position — a set of entry hashes representing a point in the causal history.
- `Payload` is a JSON literal, the unit of replication.
- `View` is a read-only snapshot of the object's state at a given version, when observed from another version (see below).
- `Event` signals that the object's state has changed.
- `ForeignDep` identifies entries in another object's DAG that must be present before a payload can be validated. The sync layer uses `extractForeignDeps` to defer (rather than reject) entries whose cross-DAG dependencies are not yet available.
- `computeDelta` reports what changed between two versions (type-specific `Delta` implementation).
- `getScopedDag` returns this object's logical history surface (`ScopedDag`), including `loadAllEntries` for full scans at the correct scope.
- `getCausalDag` returns read-only access to the broader enclosing DAG (`findForkPosition` for concurrent-branch reasoning). For nested objects this is typically the parent's causal DAG.

## `View`

A `View` is a read-only snapshot of an `RObject`'s state. It is parameterized by two versions:

- **`at`** — the version being observed (what state to show).
- **`from`** — a later version whose causal context may revise how `at` is interpreted (e.g. barrier operations that retroactively affect concurrent entries).

When `at` equals `from`, the view shows the straightforward state at that version. When they differ, the view applies revision semantics — barriers and other context from `from` may alter the interpretation of state at `at`.

```typescript
type View = {
    getObject(): RObject;
    getVersion(): Version;
    getFromVersion(): Version;

    getReferences(): Promise<B64Hash[]>;
    resolveRefVersion(refId: B64Hash): Promise<Version>;
}
```

- `getReferences()` returns the IDs of other `RObject`s this object currently references. The set is dynamic — it depends on the `(at, from)` context of this view.
- `resolveRefVersion(refId)` resolves the version of a referenced object as seen from this view's context. The resolution logic — including authorization checks and barrier handling — is type-owned (implemented by each type's `View`, not by MVT generically).

## Supporting interfaces

### `RContext`

The runtime context provided to `RObject` instances and factories. Supplies crypto primitives, DAG/object lookup, mesh access, and object creation:

```typescript
type RContext = {
    getCrypto(): BasicCrypto;
    getHashSuite(): HashSuite;
    getConfig(): RObjectConfig;
    getRegistry(): RObjectTypeRegistry;

    getObject(id: B64Hash): Promise<RObject | undefined>;
    getDag(id: B64Hash, backendLabel?: string): Promise<Dag | undefined>;
    getBackendLabel(id: B64Hash): Promise<string | undefined>;
    getMesh(label: string): any;

    createObject(createPayload: Payload, backendLabel?: string): Promise<RObject>;
    unregisterObject(id: B64Hash): Promise<void>;
}
```

`getObject` is the canonical lookup for inter-object references (e.g. permissioned `RSet` resolving its `RCap`). `getBackendLabel` returns the immutable backend label recorded at registration. `unregisterObject` tears down owned (non-root) objects: `destroy()` then registry removal.

### `LoadObjectOptions`

Factory `loadObject` accepts optional `{ parent?: NestingParent; backendLabel?: string }`. Root objects receive `backendLabel` at load time; nested objects inherit the parent's label via `NestingParent.getBackendLabel()`.

### `RObjectFactory`

Defines how to compute IDs, validate creation payloads, execute creation, and load existing objects for a given type:

```typescript
type RObjectFactory = {
    computeRootObjectId: (createPayload: Payload, ctx: RContext, parent?: NestingParent) => Promise<B64Hash>;
    validateCreationPayload: (createPayload: Payload, ctx: RContext, parent?: NestingParent) => Promise<ValidationResult>;
    executeCreationPayload: (createPayload: Payload, ctx: RContext, scopedDag: ScopedDag) => Promise<B64Hash>;
    loadObject: (id: B64Hash, ctx: RContext, opts?: LoadObjectOptions) => Promise<RObject>;
}
```

Genesis create payloads (the DAG entry written by `executeCreationPayload`) MUST include `action: 'create'` and a `type` field equal to the object's MVT type id (the same string as `getType()` and the registry key). `createObject` accepts this payload directly and derives the type via `extractCreatePayloadType`. This makes persisted roots self-describing for cold reopen. Helpers: `validateCreatePayloadType`, `extractCreatePayloadType`, `createPayloadTypeFormat`.

### `NestingParent`

The interface a parent object exposes to its nested children, providing scoped DAG access, backend label inheritance, and causal DAG access for the child's operations.

### `SyncableObject`

An object that can participate in network synchronization via `startSync()` / `stopSync()`. Teardown (`destroy()`) is on `RObject` itself.

### `RObjectTypeRegistry`

A registry that maps type names to their factories, enabling polymorphic object instantiation:

```typescript
type RObjectTypeRegistry = {
    lookup(typeName: string): Promise<RObjectFactory>;
    has(typeName: string): boolean;
    register(typeName: string, factory: RObjectFactory): void;
}
```

A concrete `TypeRegistryMap` implementation backed by a `Map` is included.

## DAG nesting

MVTs support composing objects inside other objects' DAGs through a scoping mechanism. The module provides:

- **`ScopedDag`** — an object's logical history surface, exposing `append`, `loadEntry`, `getFrontier`, `loadAllEntries` (topo order, logical entries), and filtered cover queries. Root objects get a `RootScopedDag` backed by a full DAG; nested objects get a `NestedScopedDag` that wraps/unwraps payloads and metadata transparently and filters iteration to the nested scope.
- **`CausalDag`** — read-only access to the broader causal structure (fork position finding), used by objects that need to reason about concurrent branches.
- **`DagScope`** — the interface a parent object implements to define how a nested object's payloads and metadata are wrapped into the parent DAG and unwrapped on read.

This design lets objects nest arbitrarily without knowing whether they are root-level or embedded inside another object's history.

## Reference helpers (`refs.ts`)

An observer `RObject` can hold versioned references to other `RObject`s and advance those references through ref-advance operations in its own DAG. The `refs` module provides building-block helpers for this:

- **`RefAdvancePayload`** — the canonical payload shape for a ref-advance operation (`action: 'ref-advance'`, `refId`, `refVersion`). Types may embed additional fields alongside these.
- **`refAdvanceFormat`** — a `json.Format` for validating the ref-advance portion of a payload. Designed for non-strict checking so types can extend it.
- **Payload utilities** — `isRefAdvancePayload` (type guard), `createRefAdvancePayload` (constructor), `extractRefVersion` (extracts the target version from a payload), `prepareRefAdvance` (returns `{ payload, meta }` for appending a barrier ref-advance).
- **Metadata** — `createRefAdvanceMeta(refId, opts?)` returns indexed `MetaProps` for append; barrier-tagged by default. Pass `{ barrier: false }` for a non-barrier ref-advance. `prepareRefAdvance` pairs payload + default meta.
- **DAG queries** — `findRefAdvances(dag, refId, at)` finds all ref-advance entries for a reference up to a position; `findConcurrentRefAdvanceBarriers(dag, refId, at, from)` finds ref-advance barrier entries concurrent to a position, used for `(at, from)` revision semantics in the observer DAG.
- **`resolveRefVersionAtPosition(dag, refId, at, from, isLive?)`** — merges causal ref-advances up to `at` with concurrent ref-advance **barriers** reachable from `from` in the **observer** DAG. Observers use this to decide which target version(s) an entry must be checked against. Permissioned `RSet` uses it twice when querying `RCap`: once for the entry position (with barriers) as the referenced `at`, and once at the view frontier with `from === at` as the referenced `from`. The optional `isLive(entryHash)` predicate filters individual ref-advance entries (in both folds): an entry it rejects contributes no version, so a type can drop a ref-advance voided at-use by its own authorization gate. Omitting it is the geometric resolution (every ref-advance counts).
- **`resolveRefVersions(observerDag, refId, entryHash, observerFrom)`** — compositional helper returning `{ refAt, refFrom }` in the referenced object's DAG for checking an observer entry against a foreign object (see permissioned `RSetView`).
- **`refVersionAtOrAbove(referencedDag, newer, older)`** / **`refVersionAtOrBelow(referencedDag, v, ceiling)`** — closed `≥` / `≤` comparisons in the referenced DAG (converses, not negations; concurrent positions fail both).
- **`projectForeignBound(observerDag, refId, referencedDag, localAt, foreignRevisionBound)`** — projects a nested object's revision bound into the observer DAG, lowering the revision bound to the earliest unstable ref-advance(s).
- **`validateRefAdvanceMonotonicity(observerDag, referencedDag, refId, newRefVersion, at)`** — insertion-time check that a proposed ref-advance does not move a reference backward. For each predecessor in `at`, resolves the current reference in the observer DAG and requires the new version to be at or above it in the referenced DAG. Types call this from `validatePayload` (e.g. permissioned `RSet`).

These are thin, generic utilities. Types still own authorization, barrier semantics, and view-time reference resolution.

## Bounded delta helpers (`delta.ts`)

Shared geometry for bounded `computeDelta` between two versions (`start`, `end`):

- **meet** — fork GLB from `fork.common` via `computeForkMeet(rawDag, forkCommon)`.
- **revisionBound** — walk stop and `Delta.getRevisionBound()`; equals the meet for plain types, or lowered by `computeObserverRevisionBound` when a referenced object can revise authorization below the meet.
- **`walkEntriesBackwardsToBound(dag, from, bound)`** — BFS backward from `from`, excluding entries in `bound`; returns delta candidate entries strictly above the revision bound.
- **`computeObserverRevisionBound(observer, observerMeet, observerEnd, referenced)`** — for an observer with a referenced `RObject`: resolve ref versions at meet/end, run `referenced.computeDelta`, project via `projectForeignBound`. Caller sets `setDeltaStrategy('bounded')` on concrete types when needed (not on `RObject`).

Types keep candidate collection and view diffs; these helpers own fork meet, bound projection, and the backward walk.

## Concrete type: `RSet`

`RSet` (Replicable Set) is a fully featured MVT implementation that lives in the [**std_types** module](../std_types). It implements the `RObject` interface and supports:

- **Simple sets**: a set of JSON literals, with add/delete operations and configurable redundancy acceptance.
- **Nested object sets**: when a `contentType` is specified, each element is a nested `RObject` whose creation payload is stored as the add operation's content. Updates to nested elements are transparently routed through the parent DAG via `NestedScopedDag`.
- **Barrier operations**: optional add/delete barriers for fine-grained concurrency control.
- **Version-scoped views**: `RSetView` computes set membership at any version, correctly handling concurrent adds, deletes, and barriers by querying the causal DAG.
- **Permissioned mode**: references `RCap`; authorization at view time is compositional (`RCap.getView(rcapAt, rcapFrom)` per entry) with predicate-aware cover queries peeling void entries.

```typescript
const init = await RSet.create({
    seed: 'my-set',
    initialElements: ['a', 'b'],
    hashAlgorithm: 'sha256',
});

const set = (await ctx.createObject(init)) as RSet;

await set.add('c');

const view = await set.getView();
await view.has('c');  // true
```

For nested sets (sets of `RObject`s):

```typescript
const outerInit = await RSet.create({
    seed: 'outer',
    contentType: RSet.typeId,   // elements are themselves RSets
    initialElements: [],
    hashAlgorithm: 'sha256',
});
```

## Tests

This module's own tests cover the reference helpers:

- **Reference helper tests** (`test/refs_tests.ts`): payload creation/recognition, format validation (strict and non-strict), metadata construction, and DAG query utilities.

The concrete types that exercise the full MVT machinery are tested in [**std_types**](../std_types):

- **Simple set tests** (`test/simple_set_tests.ts`): creation with initial elements, add/delete, redundancy policies, barrier add/delete, concurrent add-delete resolution, and payload validation.
- **Nested set tests** (`test/nested_set_tests.ts`): nested `RSet`-within-`RSet` scenarios, concurrent operations across nesting levels, and fork detection through the causal DAG.
- **Authorship tests** (`test/authorship_tests.ts`): sign/verify round-trips, tampered payloads, and type guards.
- **RCap tests** (`test/rcap_tests.ts`): all capability operations, barrier semantics, `managedBy` delegation, and transitive revocation.
- **Permissioned set tests** (`test/permissioned_set_tests.ts`): RCap-gated `RSet` integration — authorization, peeling, ref-advance, and `extractForeignDeps`.

End-to-end sync tests for permissioned types live in [**replica**](../replica) (`test/replica_permissioned_sync_tests.ts`).

To run:

```
npm run test
```

from `modules/mvt`, or from the workspace root:

```
npm run test --workspace=modules/mvt
```

## Building

To build, run the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```

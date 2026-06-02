# Standard Types

A collection of reusable [Monotone View Types](../mvt) for Hyper Hyper Space v3. These are ready-made, general-purpose replicable data types that application developers can use directly or compose into larger structures.

## Available types

### `RSet` — Replicable Set

A set data type with full MVT support. See the [MVT module documentation](../mvt#concrete-type-rset) for a detailed description and usage examples.

Key features:

- **Simple sets**: add/delete operations over JSON literals, with configurable redundancy acceptance.
- **Nested object sets**: elements can themselves be `RObject`s, with operations transparently routed through the parent DAG via `NestedScopedDag`.
- **Barrier operations**: optional barrier add/delete for fine-grained concurrency control.
- **Version-scoped views**: `RSetView` computes set membership at any `(at, from)` version pair, correctly handling concurrent operations and barriers.
- **Permissioned mode**: when created with a `capabilityRef` and `capRequirements`, the set becomes RCap-gated — add/delete operations require signed payloads from identities holding the appropriate capabilities. See [Permissioned RSet](#permissioned-rset-references-and-compositional-authorization) and [RCap](#rcap--replicable-capability-system) below.

Source: [`src/types/rset.ts`](./src/types/rset.ts), [`src/types/rset/payload.ts`](./src/types/rset/payload.ts)

### `RCap` — Replicable Capability System

An identity-scoped capability system for controlling access to MVT operations. `RCap` manages a set of named capabilities and tracks which identities hold them, with full support for concurrent grant/revoke resolution through MVT barrier semantics.

Key features:

- **Named capabilities**: defined at creation time, with optional `managedBy` lists for internal delegation (e.g. an `admin` capability can control who may grant/revoke `write`).
- **Multiple irrevocable creators**: creators always hold all capabilities and cannot be revoked (this will be refined later).
- **Identity registry**: public keys are stored once via `add-identity` operations and looked up by `KeyId`, keeping signed payloads small.
- **Grant / revoke with barrier semantics**: `revoke` is a barrier operation, ensuring that concurrent uses of a revoked capability are correctly invalidated when observed from later versions.
- **Mutable capability set**: `create-cap` and `delete-cap` operations allow adding and removing capability names after creation. `delete-cap` is a barrier that voids grants tied to the deleted capability's origin.
- **Transitive authorization (admissibility)**: `hasCapability(X, cap)` answers whether an operation appended at the view's position that requires `X` to hold `cap` would be *admissible* when observed from the view's `from` frontier. Each grantor's authority is evaluated **at the version where its grant was made** — so revoking a grantor's authority does **not** retroactively void grants it already made (**use-before-revoke**), while a revoke that is *concurrent* with the use still voids it (**concurrent-void**, a use-anchored barrier). The recursive walk uses a **see-through validity predicate** so unauthorized or authority-voided grants *and* revokes are skipped instead of masking the last valid operation. Cycle detection bounds the recursion. When the view position is a multi-hash frontier it is treated as a single **collapsed use point**: the use-anchored barrier voids only if the revoke is concurrent with *every* element, while a revoke that is merely later on one branch defers to the grant-anchored check (**use-before-revoke**).

Source: [`src/types/rcap.ts`](./src/types/rcap.ts), [`src/types/rcap/payload.ts`](./src/types/rcap/payload.ts)

#### Bounded delta computation

`computeDelta(start, end)` reports what observably changed between two versions: identity additions, capability existence changes, and grant flips. Two strategies are selectable via `setDeltaStrategy`:

- **`full`** (default): re-checks every identity / capability / grant pair across the whole DAG. Its `revisionBound` is the empty version, meaning "nothing is excluded -- re-check everything".
- **`bounded`**: restricts the work to the entries a new (forkB) barrier can affect, and returns a non-empty `revisionBound`. Requires `end` to extend `start` (throws otherwise).

The bounded strategy walks back from `end` only as far as the **meet** (greatest lower bound) of the fork points (`findForkPosition(start, end).common`), computed with `dag.computeMeet`. The meet -- not `common`, not its antichain -- is the correct floor:

- A grant flip is only detected if its own entry is walked, so the walk must reach every shared entry that can be concurrent with a forkB barrier.
- Stopping at `common` / `commonFrontier` is too shallow when `end` merges an old branch whose barrier voids a grant on the shared trunk below `common` (a transitive flip on a node still in shared history).
- Stopping at the antichain of `common` is too shallow when there are concurrent fork siblings: their meet is their shared parent, strictly below them.

`revisionBound` is the floor below which the type guarantees no `(start, end)` change can reach. A composing consumer (e.g. a permissioned `RSet`) uses it to bound how far back it must re-evaluate references.

**Known limitations / future work:**

- **Native DAG meet** (performance): `computeMeet` folds `O(k)` indexed `findForkPosition` calls; a native N-ary meet at the index level would compute the greatest lower bound directly.

### Permissioned RSet: references and compositional authorization

A permissioned `RSet` does not embed permissions in its own DAG. At creation it stores a **`capabilityRef`**: the id of an `RCap` object whose DAG holds grants, revokes, and identities. The set only records, in its own history, **which version of that `RCap`** it is using when it validates or re-checks operations.

**Ref-advance** is the operation that updates that pointer. Each ref-advance is an entry in the **RSet** DAG with payload `ref-advance`, a `refId` (the `RCap` id), and `refVersion` (a set of hashes: a frontier or version of the `RCap` DAG). Ref-advances are **monotonic**: later ref-advances subsume earlier ones for the same reference, so only the cover of ref-advances in the past of a position matters. See also [Reference helpers](../mvt#reference-helpers-refsts) in the MVT module.

For each DAG entry E in a permissioned `RSet`, view-time authorization queries the referenced `RCap` compositionally:

```text
RCap.getView(rcapAt, rcapFrom)
```

- **`rcapAt`**: RCap version at the entry's position in the RSet DAG, computed by `resolveRefVersionAtPosition(entryPosition, rsetView.from)`. Ref-advances in the entry's past are included. Ref-advances are also **barrier** operations in the RSet: if E is **concurrent** to a barrier ref-advance that moves the set to a newer `RCap` version, E must be checked against that newer version, not an older one the peer could still see by forking before the ref-advance. This is BFT protection in the observer.
- **`rcapFrom`**: RCap version from ref-advances already in the **history of the RSet view's frontier** — `resolveRefVersionAtPosition(rsetView.from, rsetView.from)`. Using the same position for both arguments skips RSet-side concurrent barrier widening (that widening is handled on `rcapAt` per entry). This version is passed as the **`from`** argument to `RCap.getView`, so barriers **inside the RCap** (e.g. a concurrent revoke on another branch of the RCap) can revise authorization at `rcapAt`.

Insertion-time validation (`validatePayload` / `checkPayloadAuth`) still uses unrevised views (`getView(at, at)` on both objects). View-time membership re-checks use the compositional pair above.

See **PSET10** (sequential revoke does not void past adds), **PSET17–18** (concurrent RSet ref-advance), and **PSET20** (sequential RSet ref-advance + concurrent RCap branch).

#### Bounded delta computation

`computeDelta(start, end)` reports element membership changes (`added` / `removed`) and, for permissioned sets, per-entry authorization flips (`validityChanges`). As with `RCap`, two strategies are selectable via `setDeltaStrategy`:

- **`full`** (default): scans the whole RSet DAG, comparing `hasByHash` (and `checkEntryAuthorization`) at `start` vs `end` for every element. Its `revisionBound` is the empty version.
- **`bounded`**: walks back from `end` only to a computed **floor**, collecting candidate entries there, and returns that floor as `revisionBound`. Requires `end` to extend `start` (throws otherwise).

For a **plain** set, the floor is the **meet** of the fork points (`dag.computeMeet` over `findForkPosition(start, end).common`) — identical to the `RCap` pattern. Only entries above the meet can change membership.

For a **permissioned** set, an entry's membership can also flip below the RSet meet, because the `end`-view observes the referenced `RCap` from a later `from` (`rcapFrom`), pulling in `RCap` barriers (e.g. a revoke concurrent with a grant) that were not visible from `start`. The floor is therefore **woven** with the `RCap` delta:

1. `rcap_at_meet = resolveRefVersionAtPosition(rset_meet, rset_meet)` and `rcap_at_end = resolveRefVersionAtPosition(end, end)`.
2. `rcap_bound = rcap.computeDelta(rcap_at_meet, rcap_at_end)` (bounded) `.getRevisionBound()` — the floor below which no `RCap` change can reach.
3. Descend to the **lowest unstable ref-advance(s)**. Call a ref-advance *stable* iff its referenced `RCap` version is at or below `rcap_bound` (a `findForkPosition` check on the `RCap` DAG with empty `forkA`), *unstable* otherwise. Starting from the ref-advance cover at `rset_meet`, skip stable ref-advances and descend through unstable ones via their preds; a branch settles when no unstable ref-advance sits below it. The create op is an implicit stable ref-advance to `version(refId)` (the `RCap` root, always at or below `rcap_bound`), so a branch that bottoms out there settles too. The floor is the set of lowest unstable ref-advances — excluded from the walk, since ref-advances carry no element or authorization to re-check. Below the floor the referenced `RCap` version is bounded by `rcap_bound`, so authorization is identical observed from `start` and `end`. If no ref-advance is unstable, the floor is `rset_meet`.

`revisionBound` is the floor below which no `(start, end)` change — membership or authorization — can reach; a sync peer can trust shared state below it without re-checking.

**Known limitations / future work:**

- **Monotonic ref-advances**: the floor descent assumes ref-advances are monotonic — a ref-advance never moves the referenced `RCap` version backward. This lets the descent stop at the first stable ref-advance on each branch (everything below is then stable too), so the concurrent-revoke case lands at the lowest unstable ref-advance rather than degrading to a full scan (see **DELTA06** and **DELTA10**). Enforcing monotonicity in `RSet` validation is tracked separately.
- **Native DAG meet** (performance): shared with `RCap` — `computeMeet` folds `O(k)` `findForkPosition` calls.

## Authorship helpers

Generic building-block utilities for types that need payload-level signing and verification, analogous to how `refs.ts` in the MVT module provides helpers for inter-object references.

Public keys are stored once by the type (e.g. in a creation payload or identity registry) and looked up by `KeyId` during verification — they are not embedded in every payload.

Provides: `signPayload`, `verifyPayloadSignature`, `extractAuthor`, `isAuthoredPayload`, `computeKeyId`, `serializePublicKeyToBase64`, `deserializePublicKeyFromB64`.

Source: [`src/authorship.ts`](./src/authorship.ts)

## Testing

Six test suites exercise the module:

- **Simple set tests** (`test/simple_set_tests.ts`): creation with initial elements, add/delete, redundancy policies, barrier add/delete, concurrent add-delete resolution, and payload validation.
- **Nested set tests** (`test/nested_set_tests.ts`): nested `RSet`-within-`RSet` scenarios, including creation of inner sets, adding/deleting elements in inner sets, concurrent operations across nesting levels, and fork detection through the causal DAG.
- **Authorship tests** (`test/authorship_tests.ts`): sign/verify round-trips, tampered payloads, missing keys, and `extractAuthor`/`isAuthoredPayload` type guards.
- **RCap tests** (`test/rcap_tests.ts`): all RCap operations (create, grant, revoke, create-cap, delete-cap, add-identity), barrier semantics for revoke and delete-cap, `capOrigin` voiding, creator irrevocability, `managedBy` delegation, and transitive revocation.
- **Permissioned set tests** (`test/permissioned_set_tests.ts`): RCap-gated `RSet` integration covering authorized/unauthorized add and delete, signed convenience methods, ref-advance authorization, predicate-aware peeling for void entries, compositional `RCap.getView(at, from)` for per-entry authorization (PSET20), transitive revocation through `managedBy` chains, and `extractForeignDeps`.
- **Permissioned sync integration tests** (`replica/test/replica_permissioned_sync_tests.ts`): end-to-end tests through the full Replica + mesh stack — see the [replica module](../replica) for details.

To run the unit tests:

```
npm run test
```

from `modules/std_types`, or from the workspace root:

```
npm run test --workspace=modules/std_types
```

## Building

From the workspace root:

```
npm install
npm run build
```

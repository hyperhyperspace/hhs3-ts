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
- **Permissioned mode**: when created with a `capabilityRef` and `capRequirements`, the set becomes RCap-gated — add/delete operations require signed payloads from identities holding the appropriate capabilities. See [RCap](#rcap--replicable-capabilities) below.

Source: [`src/types/rset.ts`](./src/types/rset.ts), [`src/types/rset/payload.ts`](./src/types/rset/payload.ts)

### `RCap` — Replicable Capability System

An identity-scoped capability system for controlling access to MVT operations. `RCap` manages a set of named capabilities and tracks which identities hold them, with full support for concurrent grant/revoke resolution through MVT barrier semantics.

Key features:

- **Named capabilities**: defined at creation time, with optional `managedBy` lists for internal delegation (e.g. an `admin` capability can control who may grant/revoke `write`).
- **Multiple irrevocable creators**: creators always hold all capabilities and cannot be revoked.
- **Identity registry**: public keys are stored once via `add-identity` operations and looked up by `KeyId`, keeping signed payloads small.
- **Grant / revoke with barrier semantics**: `revoke` is a barrier operation, ensuring that concurrent uses of a revoked capability are correctly invalidated when observed from later versions.
- **Mutable capability set**: `create-cap` and `delete-cap` operations allow adding and removing capability names after creation. `delete-cap` is a barrier that voids grants tied to the deleted capability's origin.
- **Transitive authorization**: `hasCapability` recursively checks the full chain of grantors, with cycle detection, ensuring that revoking a grantor's authority invalidates all downstream grants.

Source: [`src/types/rcap.ts`](./src/types/rcap.ts), [`src/types/rcap/payload.ts`](./src/types/rcap/payload.ts)

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
- **Permissioned set tests** (`test/permissioned_set_tests.ts`): RCap-gated `RSet` integration covering authorized/unauthorized add and delete, signed convenience methods, ref-advance authorization, iterative peeling for void entries, transitive revocation through `managedBy` chains, and `extractForeignDeps`.
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

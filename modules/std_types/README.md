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

Source: [`src/types/rset.ts`](./src/types/rset.ts)

## Testing

Two test suites exercise the `RSet` implementation:

- **Simple set tests** (`test/simple_set_tests.ts`): creation with initial elements, add/delete, redundancy policies, barrier add/delete, concurrent add-delete resolution, and payload validation.
- **Nested set tests** (`test/nested_set_tests.ts`): nested `RSet`-within-`RSet` scenarios, including creation of inner sets, adding/deleting elements in inner sets, concurrent operations across nesting levels, and fork detection through the causal DAG.

To run:

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

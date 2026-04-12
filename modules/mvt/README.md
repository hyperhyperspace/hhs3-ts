# MVT — Monotone-View Types

A type system for coordination-free replicable objects. MVTs are a formalism in which observations are monotonic but explicitly version-scoped, allowing historical views to be refined as additional information becomes available. They generalize CRDTs by cleanly separating the write path (validated, version-stamped payloads) from the read path (version-scoped views), enabling coordination-free approximations for applications in any domain.

This module defines the core interfaces, a DAG-based nesting mechanism, and a concrete `RSet` type that exercises both.

## Core interface: `RObject`

An `RObject` (Replicable Object) encapsulates a DAG-backed history log. It provides methods for both writing and reading state:

```typescript
type RObject = {
    getId(): B64Hash;
    getType(): string;

    // writing
    validatePayload(payload: Payload, at: Version): Promise<boolean>;
    applyPayload(payload: Payload, at: Version): Promise<B64Hash>;

    // reading (version-scoped)
    getView(at?: Version, from?: Version): Promise<View>;

    subscribe(callback: (event: Event) => void): void;
    unsubscribe(callback: (event: Event) => void): void;
}
```

- `Version` is a DAG position — a set of entry hashes representing a point in the causal history.
- `Payload` is a JSON literal, the unit of replication.
- `View` is a read-only snapshot of the object's state at a given version, when observed from another version.
- `Event` signals that the object's state has changed.

## Secondary interfaces

### ObjectMap

A narrow interface for looking up and instantiating replicable objects, decoupled from any particular orchestration layer (like `Replica`):

```typescript
type ObjectMap = {
    getObject(id: B64Hash): Promise<RObject>;
    addObject(init: RObjectInit): Promise<B64Hash>;
};
```

### BasicProvider

The resource bundle passed to `RObject` factories and instances at construction time:

```typescript
type BasicProvider = {
    getObjectMap(): ObjectMap;
    getConfig(): RObjectConfig;
    getRegistry(): RObjectTypeRegistry<any>;
    getCrypto(): BasicCrypto;
};
```

### RObjectFactory

Defines how to compute IDs, validate creation payloads, execute creation, and load existing objects for a given type:

```typescript
type RObjectFactory<P extends BasicProvider = BasicProvider> = {
    computeRootObjectId: (createPayload: Payload, provider: P) => Promise<B64Hash>;
    validateCreationPayload: (createPayload: Payload, provider: P) => Promise<boolean>;
    executeCreationPayload: (createPayload: Payload, provider: P) => Promise<B64Hash>;
    loadObject: (id: B64Hash, provider: P) => Promise<RObject>;
}
```

### RObjectTypeRegistry

A registry that maps type names to their factories, enabling polymorphic object instantiation:

```typescript
type RObjectTypeRegistry<P extends BasicProvider = BasicProvider> = {
    lookup(typeName: string): Promise<RObjectFactory<P>>;
}
```

A concrete `TypeRegistryMap` implementation backed by a `Map` is included.

## DAG nesting

MVTs support composing objects inside other objects' DAGs through a scoping mechanism. The module provides:

- `**ScopedDag**` — an object's logical history surface, exposing `append`, `loadEntry`, `getFrontier`, and filtered cover queries. Root objects get a `RootScopedDag` backed by a full DAG; nested objects get a `NestedScopedDag` that wraps/unwraps payloads and metadata transparently.
- `**CausalDag**` — read-only access to the broader causal structure (fork position finding), used by objects that need to reason about concurrent branches.
- `**DagScope**` — the interface a parent object implements to define how a nested object's payloads and metadata are wrapped into the parent DAG and unwrapped on read.
- `**DagCapability**` — a provider extension that supplies `getScopedDag()` and `getCausalDag()` to objects that need DAG access.

This design lets objects nest arbitrarily without knowing whether they are root-level or embedded inside another object's history.

## Concrete type: `RSet`

`RSet` (Replicable Set) is a fully featured MVT implementation that demonstrates the type system. It implements the `RObject` interface and supports:

- **Simple sets**: a set of JSON literals, with add/delete operations and configurable redundancy acceptance.
- **Nested object sets**: when a `contentType` is specified, each element is a nested `RObject` whose creation payload is stored as the add operation's content. Updates to nested elements are transparently routed through the parent DAG via `NestedScopedDag`.
- **Barrier operations**: optional add/delete barriers for fine-grained concurrency control.
- **Version-scoped views**: `RSetView` computes set membership at any version, correctly handling concurrent adds, deletes, and barriers by querying the causal DAG.

```typescript
const init = await RSet.create({
    seed: 'my-set',
    initialElements: ['a', 'b'],
});

const id = await objectMap.addObject(init);
const set = await objectMap.getObject(id) as RSet<string>;

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
});
```

## Tests

The test suite covers two areas:

- **Simple set tests** (`test/simple_set_tests.ts`): creation with initial elements, add/delete, redundancy policies, barrier add/delete, concurrent add-delete resolution, and payload validation with self-validation enabled.
- **Nested set tests** (`test/nested_set_tests.ts`): nested `RSet`-within-`RSet` scenarios, including creation of inner sets, adding/deleting elements in inner sets, concurrent operations across nesting levels, and fork detection through the causal DAG.

Tests use an in-memory DAG backend and a standalone `ObjectMap` implementation (no `Replica` dependency), validating that the MVT layer is fully self-contained.

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


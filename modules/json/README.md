# JSON

A JSON module for content-addressed data structures. In HHS v3, all DAG entry payloads, metadata, and replicated state are represented as JSON literals. This module provides the tools to work with them deterministically — a requirement for content addressing, where the same logical value must always produce the same hash.

## Content addressing

Content addressing means identifying data by its cryptographic hash rather than by its location. For this to work, serialization must be **canonical**: two structurally equal values must always serialize to the exact same byte string, regardless of insertion order, runtime, or platform.

The `toStringNormalized` function produces such a canonical string by sorting object keys lexicographically and applying consistent escaping rules. This normalized form is what gets hashed by the `crypto` module to produce `B64Hash` identifiers throughout the DAG and replica layers.

## Core types

```typescript
type Literal = boolean | string | number | Array<Literal> | LiteralMap;
type LiteralMap = { [key: string]: Literal };
```

`Literal` is a recursive type covering the JSON value space. All DAG payloads and metadata are expressed as `Literal` values.

## Normalization

```typescript
toStringNormalized(literal: Literal): string;
```

Produces a canonical string representation with sorted keys and consistent escaping. Two structurally equal literals always produce the same output.

```typescript
eq(a: Literal, b: Literal): boolean;
strongEq(a?: Literal, b?: Literal): boolean;
```

Structural equality via normalized form. `strongEq` handles `undefined` values.

## Deep equality

```typescript
equals(a: Literal, b: Literal): boolean;
hasKey(m: LiteralMap, key: string): boolean;
```

Direct structural comparison without serialization, and key existence check for literal maps.

## Sets

JSON-encoded sets, represented as objects with empty-string values (`{ "element": "" }`). Used throughout the DAG for positions (`Set<B64Hash>`) and metadata.

```typescript
type Set = { [key: string]: '' };

toSet(elements?: Iterable<string>): Set;
fromSet(s?: Set): Iterable<string>;
getSet(s: Set): Set<string>;
addToSet(s: Set, element: string): void;
removeFromSet(s: Set, element: string): void;
setUnion(a: Set, b: Set): Set;
setElements(s: Set): string[];
setSize(s: Set): number;
isEmptySet(s: Set): boolean;
```

## Format validation

A declarative schema system for validating `Literal` values at runtime:

```typescript
checkFormat(format: Format, literal: Literal): boolean;
```

Supported format types include `string`, `boolean`, bounded integers and floats (`int8`, `int16`, `int32`, `bounded_int`, `float64`, `bounded_float`), arrays (unbounded, bounded, and fixed-length), objects with optional fields, unions, and constant values.

## Building

To build, run the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```

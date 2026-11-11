# JSON

A JSON module for content-addressed data structures. In HHS v3, all DAG entry payloads, metadata, and replicated state are represented as JSON literals. This module provides the tools to work with them deterministically — a requirement for content addressing, where the same logical value must always produce the same hash.

## Content addressing

Content addressing means identifying data by its cryptographic hash rather than by its location. For this to work, serialization must be **canonical**: two structurally equal values must always serialize to the exact same byte string, regardless of insertion order, runtime, or platform.

The `toStringNormalized` function produces such a canonical string. It is the canonical form of [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785) (JSON Canonicalization Scheme): no whitespace, object keys sorted by UTF-16 code unit, arrays in index order, and strings and numbers as `JSON.stringify` writes them. This text is what gets hashed by the `crypto` module to produce `B64Hash` identifiers throughout the DAG and replica layers. It is also valid JSON, so `JSON.parse` reads it back to the same value and it can be stored as JSON text.

## Core types

```typescript
type Literal = boolean | string | number | Array<Literal> | LiteralMap;
type LiteralMap = { [key: string]: Literal };
```

`Literal` is a recursive type covering the JSON value space. All DAG payloads and metadata are expressed as `Literal` values.

A Literal is narrower than what JSON text or a JavaScript value can hold. At any depth it contains no `null`, no non-finite number, and no string or object key with a lone surrogate. A lone surrogate has no UTF-8 encoding: encoders write U+FFFD in its place, so two strings that differ only in their lone surrogates would produce the same hash.

A Literal also nests at most `MAX_LITERAL_DEPTH` levels, which is 128. A boolean, number or string has depth 0, and an array or object has one more than its deepest member. `JSON.parse` reads text nested millions of levels deep, while the code that then walks the value recurses and runs out of stack at a depth that differs between runtimes. The fixed limit makes every runtime accept the same values.

```typescript
const MAX_LITERAL_DEPTH = 128;

isLiteral(value: unknown): value is Literal;
literalProblem(value: unknown, maxDepth?: number): string | undefined;
isWellFormedString(s: string): boolean;
```

`isLiteral` is the check to run on any value that comes from outside (parsed JSON text, a message from a peer) before it is hashed or stored. `literalProblem` returns `undefined` for a Literal, and otherwise names the first thing found that a Literal cannot contain (`'null'`, `'a string that is not well-formed Unicode'`, `'more than 128 levels of nesting'`, ...), for use in an error message. It counts levels as it goes, so a value nested far too deep is named and never overflows the stack. `maxDepth` is for a caller that checks a value wrapping a Literal: `sync` allows a message one level more than a payload, and `rpack` allows a release file five more. `isWellFormedString` tells whether a string has no lone surrogate. The hashing functions do not run these checks; `dag` runs them when it creates an entry, and `sync` when it decodes a message.

## Normalization

```typescript
toStringNormalized(literal: Literal): string;
```

Produces the RFC 8785 canonical text of a literal. Two structurally equal literals always produce the same output, and any implementation of the RFC produces it too. It serializes without validating: it throws on a non-finite number or a value that is not JSON, but it does not check strings or depth, which is `literalProblem`'s job.

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

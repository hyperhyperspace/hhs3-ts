# DAG

A module for using an append-only DAG as a hash-linked history log. The DAG supports indexing to provide fast analysis of forks / merges and minimal cover finding.

The module uses two storage interfaces: one for the DAG entries, and another for storing the index. Three indexing algorithms are supported: flat (naive, used as a testing baseline), a topological index (a simple, well known algorithm), and a new multi-level indexing algorithm (faster, scales logarithmically over long branches by using progressively smaller projections of the DAG for the search).

Here's the DAG interface:

```typescript
 type Dag = {
    append(payload: json.Literal, meta: json.Literal, after?: Position): Promise<Hash>;

    computeEntryHash(payload: json.Literal, after?: Position): Promise<Hash>;

    loadEntry(h: Hash): Promise<Entry|undefined>;
    loadHeader(h: Hash): Promise<Header|undefined>;

    getFrontier(): Promise<Position>;

    // latest position where history hasn't forked yet
    findForkPosition(first: Position, second: Position): Promise<ForkPosition>;
    findMinimalCover(p: Position): Promise<Position>;

    // The following two are used for finding entries with specific properties

    // This one is for reading a value at a specific version, by finding the last changes on that value
    findCoverWithFilter(from: Position, meta: EntryMetaFilter): Promise<Position>;

    // This is useful for finding barrier ops that should be applied to concurrent changes
    findConcurrentCoverWithFilter(from: Position, concurrentTo: Position, meta: EntryMetaFilter): Promise<Position>;
};
```


## Building

To build, please write the following commands at the workspace level (top directory in this repo):

```
npm install
npm run build
```

## Usage

In-memory storage for the DAG and the indexing strategies is provided with this package.

DAG entries are composed by a payload, a metadata section (not included in the hash) and a header (computed automatically, includes the predecessor entries).

To crate DAGs, do as follows:

```typescript
// DAG with flat indexing
const store1 = new dag.store.MemDagStorage();
const index1 = dag.idx.flat.createFlatIndex(new dag.idx.flat.mem.MemFlatIndexStore());
const dag1 = dag.create(store1, index1); 

// DAG with topological indexing
const store2 = new dag.store.MemDagStorage();
const index2 = dag.idx.topo.createDagTopoIndex(store2, new dag.idx.topo.mem.MemTopoIndexStore());
const dag2 = dag.create(store2, index2); 

// DAG with multi-level indexing
const store3 = new dag.store.MemDagStorage();
const index3 = dag.idx.level.createDagLevelIndex(new dag.idx.level.mem.MemLevelIndexStore({levelFactor: 8}));
const dag3 = dag.create(store, index);  //Using a groping factor of 8 at each level
```

And then, to use:

```typescript
const a = await dag.append({'a': 1}, {});
const b1 = await dag.append({'b1': 1}, {}, position(a));
const b2 = await dag.append({'b2': 1}, {}, position(a));
const c1 = await dag.append({'c1': 1}, {}, position(b1));

const A = position(b2);
const B = position(c1);
const fp = await dag.findForkPosition(A, B);

console.log("common:        ", pp(fp.common));
console.log("commonFrontier:", pp(fp.commonFrontier));
console.log("forkA:         ", pp(fp.forkA));
console.log("forkB:         ", pp(fp.forkB));
```

Fork positions have 4 fields:

- __forkA__: all the entries only in __A__'s history, that have a direct predecessor in the intersection of __A__ and __B__'s histories.

- __forkB__: all the entries only in __B__'s history, that have a direct predecessor in the intersection of __A__ and __B__'s histories.

- __common__: all the entries in the intersection of __A__ and __B__'s histories with a direct successor in __forkA__ or __forkB__.

- __commonFrontier__: a minimal covering of the intersection of __A__ and __B__'s histories.

## Performance

Performance was analyzed by creating a synthetic set of branching DAGs of different sizes. We're showing average wall clock time, measeured in milliseconds.

### Fork Analysis

|DAG entries|  Topological | Multi-level  | Speedup |
|----------:|-------------:|-------------:|--------:|
| 10,000    | 30.3         | 6.8          | 4.4X    |
| 20,000    | 74.1         | 9.5          | 7.8X    |
| 50,000    | 193.2        | 9.6          | 20.1X   |
| 100,000   | 324.2        | 9.6          | 33.7X   |


## Minimal Cover Finding

|DAG entries|  Topological | Multi-level  | Speedup |
|----------:|-------------:|-------------:|--------:|
| 10,000    | 18.4         | 0.6          | 24.5X   |
| 20,000    | 25.8         | 0.5          | 55.2X   |
| 50,000    | 81.0         | 0.6          | 131.0X  |
| 100,000   | 439.8        | 0.9          | 470.2X  |

To run the benchmark, first build the workspace and then do

```
npm run bench
```

in `modules/dag`.

## Testing

We do deterministic testing over families of pseudo-randomly generated DAGs of different sizes. To run the test suite, first build the workspace and then do:

```
npm run test
```

To re-run specific tests, you can pass keywords that act as filters on which tests will be run, like


```
npm run test FORK_LEVEL_11
```

or

```
npm run test cover topo small
```

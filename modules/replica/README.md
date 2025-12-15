# Replica

This is HHS v3 **`replica`** module. It provides:

 - Local storage for application state
 - Secure state synchronization accross application instances (BFT)
 - Resolution of concurrent updates, following app-defined rules
 
## Data model

### Introduction

The architectural pattern this **`replica`** module follows is:

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

### Monotone View Types (MVT)
 
We'll define **Monotone View Types**, a family of coordination-free, replicable data types that aim to prevent _accidental conflicts_ and help us address _essential conflicts_ through monotonic transformation. Application developers are not expected to use them directly. Instead, we'll provide tooling that will derive the appropriate replicable data types from specs (in the form of schema definitions, constraints, foreign-key relationships, etc.)

Monotone View Types will be operation-based. A _version_ will be defined as the set of operations that have been applied so far. Operations themselves will be partially ordered by a happens-before relationship, like it's customary. Any set of operations that is downwards closed with respect to the happens-before relationship will be considered a valid version.

While most operations are expected to be commutative, _non-commutative operations will be allowed_, but they will be marked explicitly. We'll call them **barrier ops**. 

Finally, _state will only be inspected using views_, defined like this:

```
let view = obj.getView(at: Version, from: Version);

view.getValue(...);
```

To get a view, we must specify not only the version we want to inspect, but also _from what_ (later) version we want to inspect it. The contents of a view may then be adjusted by updates arriving later on (in the form of non-commutative **barrier ops**). This is naturally captured by the _from_ parameter.

We'll define `concurrent(to: Version, from: Version)` as the operations in `from` that are concurrent (i.e. not comparable under _happens-before_) with _all_ the operations in `to`.

Finally, we'll define `getView(at: Version, from: Version)` as the version that contains the union of all the operations in the set `at` and the **barrier operations** in `concurrent(at, from)`. Intuitively, to update the version `at` with anything relevant happening up to `from`, we need to add any barrier ops in `from` that happened concurrently with `at`.

The view mechanism ensures that **Monotone View Types** are coordination-free, and can be safely replicated, even if the underlying type is not (because of the barriers). However, the price we pay is having "non final" views.

To be of practical application, we'll require that whenever a new operation is applied at version `v`, we can derive a boundary that limits how far in the past other views may be affected:

```
let bound: Version = obj.getScopeBound(op: Operation);
```

This will ensure that for any version `w` before `bound`, 

```
obj.getView(w, v) == obj.getView(w, v ∪ {op}) 
```

Therefore, when applying a new update, the set of versions that need to be revised is  well specified.

Finally, the operational model with barriers + scoped views generalizes well when different levels of coordination are used. The outcome of a coordinated action can be encoded as a barrier op that rules out any concurrent modifications authored by peers that did not participate in the coordination scheme. The coordination protocol itself can be executed either operationally, modifying the state, or by a different channel.

#### Implementation

We provide an implementation of Monotone View Types over hash-linked DAGs. The **`dag`** module [[local]](../dag) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/dag) provides the fundamental algorithms (see `findForkPosition`, `findMinimalCover`, `findCoverWithFilter` and `findConcurrentCoverWithFilter`) to support MVTs that work by finding filtered minimal covers over the DAG.

As an example, a Monotone View Replicated Set was implemented here [[local]](src/types/rset.ts) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/replica/src/types/rset.ts). It supports both plain and barrier additions and deletions, showcasing how non-monotonic behavior can be transformed through the scoped view mechanism.

#### Nesting

Nesting at the DAG level is supported for MVTs. Oereations for the inner instance are wrapped and inserted into the outer instance's DAG. The Monotone View Replicated Set was extended to support nesting (sets of sets of aribtrary depth).

### Composability: SOaD Architecture

Application state will normally be modeled using a collection of Monotonic View Types. We've defined an architectural pattern, **State-Observation-as-Data** (SOaD), to maximize composability and reuse and to simplify reasoning, ensuring monotonicity by construction.

As an example, imagine a system composed by a `Cap` data type, that defines the set of user capabilities, and a series of types `A`, `B`, `C` with operations that are conditional to the right capabilities being present in `Cap`. We'll model concurrency in this system by enriching the state of each of `A`, `B` and `C` with a reference to the last known version of `Cap`. A _reference update_ operation to move this reference forward will also be added to each of them. The procedure to update `Cap` will now need logic to move this reference forward in all the types that _observe_ it, or alternatively this can by done spontaneously whenever any peer that replicates `A`, `B` or `C`discovers the new version of `Cap`.

Since references to foreign versions can only be moved forward, it's easy to see that the _reference update_ operations will be well defined. If two branches of `A`, `B` or `C` have received different versions of `Cap`, they can be reconciled by taking the union of both versions as the branches are merged.

The _reference update_ operations will need to be **barrier ops**, since we do not want to allow a replica that just refuses to sync `Cap` to keep using an old set of capabilities. By making the reference updates barriers, we ensure that when the versions where these untimely capability uses happened are observerd from later on (after the reference to 
`Cap` has been merged), they will be re-evaluated using the right capabilities. This is a consequence of the scoped views in MVTs.

This procedure can be generalized to any number of types and state dependencies, as long as the dependency graph is acyclic. In its purest form, the system can be designed with the reference update operations as the _only_ non-commutative barrier ops.

It also fosters re-use of types, since the observed type is unaware of where and how it is being observed. Only the observers are modified, by being enriched with state references. Furthermore, this provides an interesting application interoperability mechanism, where an app can depend on parts of the state of another. This is an interesting data-centric alterantive to integration via APIs or other execution-driven schemes.

It's interesting to notice how in traditional systems observation is _implicit_ and managed by the runtime, using metadata that's hidden from the application (locks, snapshots, isolation levels), while in this MVT-based concurrency resolution pattern it is _explicit_ and _stored as data_.

## Development Status

 - Monotone View Type support: **Completed**
 - SOaD Architecture: **In progress**
 - Porting Synchronizer from v2: **Pending**

## Testing

A test suite for the Monotone View Replicable Set data type [[local]](src/types/rset.ts) [[github]](https://github.com/hyperhyperspace/hhs3-ts/tree/main/modules/replica/src/types/rset.ts) is provided. It tests both barrier add / delete operations, and set nesting.

To run the test suite, first build the workspace by running at the top level:

```
npm install
npm run build
```

And then within the **`modules/replica`** folder:

```
npm run test
``` 
 

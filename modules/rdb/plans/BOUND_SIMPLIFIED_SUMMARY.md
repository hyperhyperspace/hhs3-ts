# The bounded-delta bound

## Why this is necessary

A consumer that has already applied one version of an object, and wants to catch up to a later version, does not replay every entry. It asks for a bounded delta. That delta walks back from the later version and stops at a bound. The promise is that every entry in the bound's history has the same effect in both versions, so skipping it cannot hide a change.

That promise is false for the bound the code computes today. There is a small history, with no observes and no second object, where a row goes from absent to present and the bounded delta does not report it. The full replay does. Both replicas compute the same bounded delta, so they agree with each other and disagree with a direct read. The stored data is not corrupt. The catch-up is short by one change.

The rest of this note defines the words, shows that history, states the three conditions a bound needs in order to keep the promise, and sets out the choice between a bound computed from the shape of the history and a tighter bound that looks at which operations actually changed.

## Terms

An **entry** is one node in the causal history. Its **predecessors** are the entries it was appended after. Following predecessors never loops.

A **position** is a set of entries, called its **heads**. The **history** of a position, written \(H(P)\), is those heads together with every entry reached by following predecessors. A position is **at or below** another when its history is contained in the other's. An entry is **strictly after** a position when every head of the position is a proper ancestor of the entry. An entry is **concurrent** with a position when it is outside that history and is not strictly after the position.

A **version** is a position used as the horizon of a read or a delta. A delta is computed from an earlier version, **start**, to a later version, **end**. End **extends** start: every entry in the history of start is in the history of end. The **new entries** are the ones in \(H(\mathrm{end})\) and not in \(H(\mathrm{start})\).

The **fork** of start and end records where those two histories differ. Because end extends start, one side of the fork is empty. **`fork.common`** is the set of entries that lie in both histories and have a direct successor in only one of them. In this delta those successors are new entries. The **shared frontier** is the smallest set of heads with the same history as the overlap of start and end. When start is already such a set of heads, the shared frontier is start. The **meet** is the greatest lower bound of the entries in `fork.common`: the frontier of the intersection of their histories. One entry meets to itself. The meet is at or below the shared frontier. They are equal only when `fork.common` is already that frontier.

A **verdict** says whether an entry is live or void at a version. Row state is derived from the verdicts of the entries that mention the row. A **barrier** is an entry a verdict can read from another branch: a delete, an observe, or a schema deploy. A verdict does not read a non-barrier entry across branches.

An **observe** is an entry that publishes a version of another object. Along causal predecessors the published version only moves forward. The objects one object observes themselves form an acyclic graph, so a question about an observed object is well-founded. An observe is **stable** when the version it publishes is at or below the bound of the observed object, computed between the version of that object visible at the meet and the version visible at end. Otherwise it is **unstable**.

A **delta** reports the verdict changes and the row changes between start and end. The **full delta** walks every entry in the history of end. The **bounded delta** walks back from end and stops at a **bound**. The history of the bound, including the bound's own entries, is not walked. The entries that are walked are the **reprocessed interval**: everything in the history of end that is not in the history of the bound. The bound is **sound** when every skipped entry has the same verdict at start and at end, and every row whose operations are all skipped has the same state at both versions. A sound bound may sit lower than necessary. The **greatest** sound bound is the one whose history contains every other sound history, and whose interval is therefore the smallest.

## The meet is not a sound bound

The bounded path takes the fork of start and end and uses the meet of `fork.common` as the bound. Observes can pull that bound lower. The history below has none, so the bound is the meet. It is not low enough.

One object, three tables. `perm` holds a row `p` with label `ok`. `targets` holds a row `x`. Deleting a target is allowed only while some row in `perm` has label `ok`. `items` holds rows with a foreign key to `targets`. The entries:

```text
              x            insert the target
             / \
            E   T          E inserts an item pointing at x
            |              T deletes x
           s1              any later entry on E's branch
            |
            N              N deletes p
```

`T` is concurrent with `E` and with `s1`. `N` is after `s1` and concurrent with `T`. Start is `{s1, T}`. End is `{N, T}`.

At start, `T` is live, so `x` is absent, so the foreign key fails, so `E` is void. At end, `N` removes the only `ok` row in `perm`, so `T`'s condition fails, so `T` is void, so `x` is present, so `E` is live. The full delta reports both `T` and `E`.

The only new entry is `N`. Its predecessor `s1` is in both histories and the successor `N` is new, so `fork.common` is `{s1}`. The meet of that one entry is `{s1}`. The shared frontier is the larger cut `{s1, T}`. The meet is the lower of the two, and it is the bound the code uses.

The walk starts at `{N, T}` and stops at `s1`. It visits `N` and `T`. `E` is reachable only through `s1`, so it is not visited. The bounded delta reports `T` and misses `E`.

`E` is inside the history of the meet. `T` is not. `T` is a barrier, and it is concurrent with `E`, so `E`'s verdict reads `T`. `N` is a new barrier concurrent with `T`, so `T` changes, and the change reaches `E`. `N` itself is strictly after `s1`, so `N` is not concurrent with `E` or with anything else inside the meet. The change crosses through `T`, which the meet leaves outside its history and the walk happens to visit, without visiting the entry `T` affects.

The same shape occurs with observes. An observe can sit in the history of start, outside the history of the meet, and concurrent with a row below the meet. The row's foreign cut widens to the version that observe publishes. A later observe can put a foreign delete into the horizon. The row flips. The observe itself can stay live the whole time. The local history is enough to show the meet failing. The observe history shows that "did this barrier's own verdict flip?" is the wrong question. That is taken up below.

## Three conditions

Write \(U\) for the history of the bound, including the bound. The reprocessed interval is \(H(\mathrm{end}) \setminus U\). The bound is sound when three conditions hold.

1. **Inside the meet.** \(U\) is contained in the history of the meet. In particular it is contained in the history of start, so it holds no new entry.
2. **Barriers sit above it.** Every barrier in \(H(\mathrm{end}) \setminus U\) is strictly after every head of the bound. No barrier outside \(U\) is concurrent with an entry inside \(U\).
3. **Observes inside it are stable.** Every observe in \(U\) publishes a version at or below the bound of the object it observes, and that foreign bound is itself sound in these three senses.

An entry `E` inside \(U\) then has the same verdict at start and at end. Its own past is inside \(U\), because a history is downward-closed. By the second condition, every barrier it can read concurrently is also inside \(U\). By the first, that same set of barriers is already visible at start. The foreign cuts it forms are folded from observes inside \(U\), so by the third condition those cuts sit at or below a foreign bound that does not move between the two horizons. The bound's own entries are in \(U\), which is why the walk does not need to visit them. A row whose operations all lie in \(U\) has the same state for the same reason. Any barrier that mentions the row is either inside \(U\) or inside the interval, and one inside the interval is walked.

The histories that satisfy the three conditions are closed under union. There is a greatest one. On a chain with no unstable observe it is the meet, and the interval is the new entries only. Every time the procedure below moves the bound, some sound bound was already required to exclude the entry it removes.

## Two ways to choose the interval

Both ways start from the meet, produce a bound at or below it, and then run an ordinary delta on the interval above that bound. They differ in how far down the bound moves.

### From the shape of the history

Use the three conditions and nothing about whether a verdict actually flipped. A barrier outside the bound counts as soon as it is concurrent with the bound.

Compute the meet. For each observed object, compute this same bound between the foreign version visible at the meet and the foreign version visible at end. That foreign bound is the \(b\) in the definition of stability. Descend through observes in the history of the meet. Monotonicity says that everything below a stable observe is stable, so the descent stops at the first stable observe on each branch. For each lowest unstable observe, replace the bound by the greatest lower bound of its current position and that observe's predecessors. The observe itself stays above the bound, so a change in the observe's own verdict is walked.

Then repeat. Walk the interval above the current bound. If a barrier in that interval is not strictly after every head, replace the bound by the greatest lower bound of its current position and that barrier's predecessors. Each such step removes exactly the entries concurrent with that barrier or above it. Stop when every barrier still in the interval is strictly after every head. Run the delta on that interval.

A barrier that already passed the test still passes after the bound moves down, so each later pass only looks at the entries the last move uncovered. The bound only descends, and the history is finite, so this ends. The resulting interval is the smallest one this rule can produce.

This is still an analysis of causal shape, plus the list of which entries are barriers. It does not read a verdict. The price is an old branch. A delete on a side branch that merged far below the meet, and that start already contains, is a barrier concurrent with the meet. The rule pulls the bound down to that delete's predecessors even when the new entries never touch it. On a history with old unmerged branches the interval can be the whole history of end, and the bounded delta costs the same as the full one. On a chain it costs the same as today's meet.

### From the operations that actually changed

Use the same three conditions, with one substitution. The second condition is required only for barriers that can have changed, not for every barrier. An unchanged barrier outside the bound really is unchanged, so an entry inside the bound may read it at both versions and see the same thing.

The entries that can have changed are built as follows. The seeds are the new entries, and the observes whose published version is above the foreign bound \(b\). Close that set upward along successors. Then, whenever a barrier is in the set, add every barrier concurrent with it. Only barriers in the finished set pull the bound down, by the same greatest-lower-bound step as above. A barrier outside the set stays outside the argument.

When the new entries contain no barrier, and every observe in the history of the meet is stable, the set adds no barrier from the rest of start. The bound stays at the meet. An old side branch is not reprocessed. That is the common case of a catch-up made entirely of ordinary inserts, and it is the case where this version is cheaper than the shape-only rule.

The saving stops when a new entry is itself a barrier. A new delete, observe, or schema deploy is strictly after the meet, and it is concurrent with a barrier on a branch that never merged into that meet. The closure takes the old barrier because of that concurrency, without asking whether the two entries read the same row. The bound then falls to the same place the shape-only rule would have chosen.

Looking only at the old barrier's own verdict is not enough. In the observe history sketched above, the observe in the history of start stays live at both versions, and the row below the meet still flips, because the observe widened the row's foreign cut and a new observe changed the foreign horizon. The closure includes that observe because its published version is above \(b\), not because the observe became void.

A further cut is possible. A delete affects entries that read its table. An observe affects entries that read its binding. Restricting the closure to those entries would leave an unrelated old branch in place even when a new barrier is concurrent with it. Doing so needs the schema as it stood at each entry. The closure above does not.

## What to build

Build the shape-only bound first. It makes the promise true, it leaves chains where they are, and it does not depend on a second argument about which operations changed. The closure in the previous section is the tightening to add once that bound is in use and an old side branch shows up as the cost. Ship the closure only with the seeds and the concurrency step as stated there. A test that merely asks whether an old barrier flipped will accept the meet on the observe history and miss the row.

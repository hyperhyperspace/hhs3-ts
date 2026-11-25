# Query subscriptions: a monitor exists only while something holds it

- **Status:** implemented 2026-11-25, in the working tree (not committed)
- **Written:** 2026-11-25, against `a5df3ea` (clean tree)
- **Modules:** rdb_adapter (engine, tests, README), rdb_react (tests, README), rdb_projection (tests, README)

## Summary

`QueryEngine.monitor(query)` starts work the moment it is called, and only `subscribe` has a matching release. `useQuery` calls it during render, and React may throw a render away. Measured on React 18.3.1, production build, no Strict Mode:

- Typing five characters into a search box whose list is slow to render (the text goes through `useDeferredValue`) leaves 4 monitors that nobody holds. One insert then runs 4 queries, for the rest of the session.
- A navigation that is abandoned before its page finishes loading leaves 1.
- A page with a `.col()` query that mounts and leaves before its first result leaves the upstream monitor behind, although it subscribed and cleaned up correctly. A monitor's run looks its upstreams up with `engine.open()`, which creates and registers a missing one without holding it.
- A second component showing a query set that is already on screen stays on "loading" until the next write to one of its tables.
- In development, Strict Mode leaves one query set behind per mount and runs one extra query.

All five come from one defect: a monitor can be in the engine while nothing holds it. This plan removes that state. It is the standard pattern for a shared, reference-counted subscription: creating one does nothing, the first subscriber starts the work, and the last one leaving stops it.

## Background

- **Query**: a description of one read from one table. Building one reads nothing. Two queries that describe the same read are the same query (same fingerprint).
- **Monitor**: the engine's object that keeps one query live. It runs the query, keeps the last result, and runs again when a commit touches its table. At most one per query.
- **Snapshot**: what a monitor shows, `{ data, events, loading, error }`. The same object until the result changes.
- **Handle**: what `monitor(query)` returns (`QuerySubscription`): `getSnapshot`, `subscribe`, `whenReady`.
- **Upstream**: in `messages where author_id IN profiles.col('id')`, `profiles` is the upstream of `messages`. The messages monitor cannot run before the profiles monitor has a result.
- **Query set**: `{ messages, profiles }`, several queries delivered as one snapshot.
- **Count**: how many are using a monitor (`refs`). `hold` adds one, `release` removes one. The holders are a subscribed listener, a monitor holding its upstreams, and a one-shot read while it waits.

## The rule

A monitor exists only while its count is above zero.

```
monitor(query)                 returns a handle. The engine is not touched.

handle.getSnapshot()           the running monitor's snapshot, or LOADING.
                               It only looks; it never creates a monitor.

handle.subscribe(listener)     m = hold(query); add the listener
                               cleanup: remove the listener; release(m)

handle.whenReady()             m = hold(query); wait for its first result;
                               release(m); return its snapshot

hold(query)                    m = the monitor running for this query
                               if none: create m; m.upstreams = hold(each
                                        upstream query); start its first run
                               m.count += 1

release(m)                     m.count -= 1
                               at 0: when the current synchronous code ends,
                                     if still 0, remove m and release its
                                     upstreams

a run of m                     wait for the first result of each of m.upstreams
                               (the ones it holds; it looks nothing up)
                               stop if m was removed; query; publish
```

- **LOADING** is one constant object, used for a query with no monitor and for a monitor before its first result. React compares snapshots by identity, so `getSnapshot` returns the same object just before and just after `subscribe`.
- **"When the current synchronous code ends"** is a microtask, not a timer. React runs the cleanups and the subscribes of one screen update in one stretch of synchronous code (checked on 18.3.1 for a Strict Mode mount, one component replacing another, and a component changing its query). So a cleanup followed at once by a subscribe keeps the monitor.
- **A query set** holds its members while it has a listener. Its snapshot is built from its members' snapshots. Before anyone subscribes it is built from whichever members are already running, and a member that is not running counts as LOADING. It carries events only for member deliveries made while it is subscribed.
- **A removed monitor never comes back.** A later `hold` of the same query creates a new one, which runs its query afresh.

## What changes for an app

| Situation | Today | After |
|---|---|---|
| A render React throws away | leaves a monitor or a set | leaves nothing |
| Strict Mode mount | one set left, one extra query | nothing left, no extra query |
| One component leaves, another arrives with the same query | same monitor | same monitor, by the rule |
| Leave before the first result of a `.col()` query | upstream left behind | nothing left |
| Second component, query set already on screen | loading until the next write | rows on its first render |
| Same handle subscribed again later | shows the old rows | runs the query again |
| `await whenReady()`, then `subscribe` | keeps the monitor | the read's monitor is gone; `subscribe` starts a new one |
| A query no other component shows | starts during render | starts when React subscribes, after the screen update |

The last row is the cost, accepted in conversation: the first screen update of a query nobody else shows is always "loading". Today a slow deferred render can skip that frame, but only when the query happens to finish before the render does.

## Changes, in landing order

1. **`modules/rdb_adapter/src/query_monitor.ts`**
   - `QueryEngine.monitor()` builds a handle and registers nothing. The single-query handle keeps the query, not a `Monitor`.
   - `open()` goes. `find(query)` looks a monitor up by fingerprint and never creates. `hold(query)` finds or creates: a new monitor is registered, holds its upstreams and starts.
   - `release(monitor)` at zero lists the monitor as idle and queues one microtask sweep. The sweep removes every listed monitor still at zero: it leaves the map, is marked removed, has its first-result promise resolved so that no run waits on it for ever, and releases its upstreams. An upstream this leaves at zero is removed in the same sweep.
   - `Monitor`: `upstreams` is set at creation, in the order of `query.dependencies()`, and kept for its whole life. `run()` and `incomingKeys()` read it and call nothing on the engine. `start()` and `run()` return after each `await` if the monitor was removed. The first snapshot is `LOADING`. `subscription()`, `retain()` and `release()` go.
   - `flush()` skips a removed monitor, and `settle()` returns after the run of one.
   - `QuerySet`: the constructor only stores the queries. One `refresh()` rebuilds the snapshot from the members and keeps the same object when no member's snapshot changed. `getSnapshot()` calls it while nobody is subscribed, and `publish()` is `refresh()` plus the listeners. The first listener holds the members and registers the set with `keepSet`; the last cleanup calls `dropSet` and releases them. `whenReady()` holds the members while it waits.

2. **`modules/rdb_adapter/src/query_builder.ts`**: `results()` becomes `whenReady()` on a handle, which now holds for the read. The comments on `QuerySubscription` say what each function does to the engine.

3. **`modules/rdb_react/src/use_query.ts`**: no change in behaviour. A comment says the `useMemo` is an optimization only, because any two handles for one query are interchangeable.

4. **Docs.** The full explanation goes in one place, rdb_adapter's README, *Subscriptions and snapshot identity*: the terms, the rule, the three handle functions, who holds, when a monitor leaves, what a handle shows before anyone subscribes, and query sets. Also there: the sample comment on `engine.monitor(messages)` and the Strict Mode sentence in *Query sets and `.col()`*. rdb_react's *How it behaves on render* is rewritten around the table above and links to that section. rdb_projection's *Reactive queries* sample shows `subscribe`.

## Tests

**Existing tests that change, all for one reason.** They call `whenReady()` and then read `getSnapshot()` or subscribe. A handle nobody subscribed to is no longer live after the read, so they subscribe first and start recording after the first result: `[ADPTQ12]` to `[ADPTQ15]`, `[ADPTQ17]`, `[RDBPROJ28]` to `[RDBPROJ30]`. What they assert stays the same. `[ADPTQ18]` is replaced by the Strict Mode test below.

**New, rdb_adapter** (`query_monitor_tests.ts`, from `[ADPTQ19]`):
- A handle nobody subscribes to shows loading, and later writes run no query. For a single query and for a set.
- A `.col()` query is subscribed and dropped before its first result, with queries that take 10 ms: dropped at once, and dropped while the upstream's query is running. Nothing is left, writes to either table run no query, and a query subscribed afterwards still loads.
- Subscribe, drop and subscribe again in one go, as Strict Mode does: as many queries as one subscribe, and the only delivery is the first result.
- One subscription is dropped and another taken for the same query in one go: the second handle shows the first's snapshot object and no query runs. The same when a query changes but keeps its upstream: the upstream is not queried again.
- Before subscribing, a handle shows what a running monitor shows. A set over members already loaded shows their rows, not loading, and still does after subscribing.
- With nothing running, `getSnapshot()` returns the same object before and right after `subscribe`. For a single query and for a set.
- A handle subscribed again after its monitor left, with a write in between, shows the new row.
- `whenReady()` on a set nobody subscribed to returns the members' rows and leaves nothing.

**New, rdb_react** (`run_tests.ts`, from `[REACT04]`):
- A render React throws away: a screen calls `useQuery` and a child suspends on first mount, then the tree is unmounted. No monitor and no set is left, and a write runs no query. For a single query and for a set.
- Strict Mode: mounting a `.col()` set runs as many queries as without Strict Mode, and after unmount the engine holds nothing.
- A second component showing a set already on screen never renders loading.
- One component leaves and another arrives with the same query in one update: the arriving one never renders loading, and no query runs.
- A component mounts and leaves from an effect before its first result, with slow queries: nothing is left.
- A mount renders twice (loading, then rows), not three times.

The tests that check "nothing is left" read the engine's private `monitors` and `sets` through a cast. No API is added for them.

Each new test is first run against today's code. The ones about a shared snapshot object for a single query pass there and guard the new code; the rest must fail.

## Verification

1. Tests first: add the new tests, run them on today's code, and note which fail.
2. Implement step 1 and 2, then `npm run build --workspace=modules/rdb_adapter` from the repo root (rdb_react and rdb_projection load the adapter from `dist`).
3. `npm test` in `modules/rdb_adapter`, `modules/rdb_react` and `modules/rdb_projection`. Typecheck each module's tests with a temporary tsconfig that includes `test/**`, since a type error in a test dies silently at load.
4. Root `npm run build`, to see that nothing downstream of the adapter breaks.
5. Run the review's two timing scenarios again, on React's production build and without `act()`: the search box with a deferred value over a slow list, with keystrokes sent as DOM events 20 ms apart, and the abandoned navigation. Both must leave no monitor. They stay probes, not tests, because they depend on real timers.

## Stop and report

Stop, without patching around it, if any of these turns up:
- an existing test changes outcome, other than the ones listed above;
- React runs a cleanup and its replacing subscribe with a microtask in between;
- a case needs a timer, or needs a monitor to exist with a count of zero beyond the end of the current synchronous code;
- two parts of the rule cannot both hold.

## As implemented

- **On today's code**, before the engine changed: the reordered tests `[ADPTQ12]` to `[ADPTQ17]` still passed. `[ADPTQ18]` to `[ADPTQ22]`, `[ADPTQ24]`, `[ADPTQ25]`, `[REACT04]`, `[REACT05]`, `[REACT06]` and `[REACT08]` failed, each on the assertion that names its defect. `[ADPTQ23]` (the same object before and after `subscribe`), `[REACT07]` (one component leaves as another arrives) and `[REACT09]` (a mount renders twice) passed, and guard the new code.
- **Existing tests that changed outcome:** `[RDBPROJ28]` and `[RDBPROJ30]`, as listed. `[RDBPROJ29]` passed unchanged, because it polls, and was reordered with the other two.
- **A removed monitor wakes what waits on it.** Removing a monitor resolves its first-result promise. Without that, a pass that was running a dependent again while its upstream's first query was in flight waited for ever, and no query set heard of a first result afterwards. `[ADPTQ20]` covers it: with that line off, its third case fails.
- **Listeners are kept per `subscribe` call**, not per function, in monitors and in sets. The same function subscribed twice is called until both cleanups run. Not in the plan; it came with moving the listeners out of the handle.
- **A handle computes its fingerprint once**, when it is made, since `getSnapshot()` looks the monitor up by it on every call.
- **The timing scenarios** leave nothing on React's production build: the search box (four abandoned renders), the abandoned navigation, and the page that leaves before its first result.

## Not in this plan

- A `.col()` dependent runs its first query twice at mount, with the same keys. It did before and still will.
- Keeping the previous rows on screen while a changed query loads.
- Which events a snapshot carries. Unchanged.
- No commit. The user commits.

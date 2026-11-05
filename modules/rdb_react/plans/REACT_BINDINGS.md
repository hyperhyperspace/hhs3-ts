# React bindings

How a React UI stays on the rows of a projected database. v2 bound components to live objects. v3's screen reads a projection, and the binding has to follow the query that is actually rendered.

## Problem

In v2 the React package (`@hyper-hyper-space/react`, a sibling of core) treated a `Space` as the shared thing a screen lived in, and a mutable object as the unit of observation.

`PeerComponent` put the peer's `Resources` in context. `useSpace` resolved an entry point and started sync for the life of the mount. `useObjectState` took an object that was already bound to the store, added a `MutationObserver`, and on each accepted event stored a new wrapper around the same object so React would re-render. The wrapper was only an identity change: the object is mutated in place. `filterMutations` dropped events whose path was not the field on screen. `debounceFreq` collapsed bursts. Components then read the live object during render.

That was sound because the notification and the readable state were the same mutation. `MutableReference` assigns `_value` and then emits. By the time `setState` runs, the next render's read is already there.

v3 splits those two. An `RObject.subscribe` callback means the scoped DAG has advanced and `getView()` can see it. The projection is a second state, later. `RdbProjection` hears each member `RTableGroup.subscribe`, waits `debounceMs` (default 50), then runs `syncDatabase`. `onResult` runs after that cycle writes the target. `subscribeOpEvents` carries ingestion failures and concurrency void/reinstate flips, not ordinary row changes. `ChangeSignalSource` fires the other way, when a local edit is in the outbox, before ingest.

The UI does not render `RObject` views. It renders the projected tables, the way a v2 screen rendered the objects inside a space. The projection is the space: one local database for one `RDb`, already kept current by the host. The binding's job is to keep a component on the query it shows.

Three commits can change what that query returns, and they are not one stream.

- **Local write.** The app inserts, updates, or deletes through the projection database. The capture trigger writes `rdb_outbox` in that same statement, and the row is already visible to a `SELECT`. This wake drives the UI. It must not wait for `schedule()`, for ingest, or for the op to land in the replica. `ChangeSignalSource` is this wake today: it fires when the outbox grows, and it ignores `apply()`, so a checkpoint write is not mistaken for a local edit.
- **Projected apply.** `apply()` writes the row actions and the concurrency op-events in one transaction, with capture gated off, so those writes never enter the outbox and never fire `ChangeSignalSource`. A listener that only watches the outbox stays stale for every remote change and for the project-back of a local edit that ingested cleanly.
- **Failure backchannel.** `commitIngest` reverts the rejected rows, updates sync status, and appends the ingestion `OpEvent`s in one transaction, again with capture gated off. After that commit the optimistic row is already gone. Nothing in the outbox says so. Concurrency void and reinstate ride inside `apply()`, in the same transaction as the row change. The event is the reason (`table`, `rowId`, `localId`, and for an ingestion failure the attempted `op`).

A local insert therefore shows up twice when both results differ. First when the app's transaction commits. Later when project-back commits the canonical row, or when the settle transaction reverts it and records the failure. The component has to apply the second read. Holding the optimistic row past the event is how the screen drifts from the table.

`filterMutations` does not carry over. The v3 callback is a raw frontier, with no field path, and a `WHERE` cannot be evaluated on it. The predicate is the query the component is rendering.

The outbox is also the wrong place to evaluate that predicate. An insert records the full new row. An update records only the columns that changed, one outbox row per column. A delete records `local_id` and a null image. A projected delete carries a `rowId` and no before-image. A row that just left the `WHERE` is invisible in all of those. Deciding relevance from the change record misses departures.

## Solution landscape

### Bind the component to the RObject

Port `useObjectState` onto `subscribe` plus `getView()`. Await `subscribe`, read a snapshot, and on later callbacks read again. This preserves the v2 invariant for object-shaped state (`RSet`, a capability, a single row through its view): the callback means the view is ready.

It is the wrong binding for a screen of projected tables. `RTableView.query` scans the DAG. The projection exists so the UI does not. `RTable.subscribe` also delivers the table's scoped frontier, while `getView()` reads the group frontier so a concurrent ref-advance barrier still counts. A hook that passes the callback's version into `getView` misses those revisions.

### Subscribe to the RObject and read SQL

The natural mistake. The callback fires when the projection's trigger fires, which is before `syncDatabase` writes. The `SELECT` sees the previous cycle. Debouncing makes it worse: several DAG advances become one SQL update, and a component that reads on the first callback can store stale rows and not read again until the next event. `IngestResult` carries no version, so there is nothing to wait on of the form "synced through this frontier."

### Watch a table, and match a WHERE against the change

The unit of observation is a projected table (`notes_note`, the group-qualified name) plus a simple `WHERE`, no joins and no `EXISTS`. Wake on a local outbox row for that table, a projected row action for that table, and an op-event for that table. Re-read `SELECT * FROM table WHERE …` when the change looks relevant.

This is the right grain, and the wrong test. Relevance needs a before-image the outbox and the row actions do not keep. An update that moves a row out of the `WHERE` looks like a current row that simply does not match. The safe version of this option re-reads on every wake for the table and lets the query do the filtering. At that point the `WHERE` is the rendered query, and matching it against the change record is optional work that the capture format does not support.

### Monitor the rendered query on the projector

The query that is on screen is the observation. `RdbProjection` is already the object that hears the local outbox, the committed `apply()`, and the op-events from `commitIngest`. The binding is an addendum there, and the React hook only subscribes.

```typescript
projection.monitor(
    "SELECT * FROM notes_note WHERE folder_id = ?",
    [folderId],
    (snapshot) => { /* rows, plus op-events since the last delivery */ },
)
```

The first call runs the query immediately. Later calls run that same statement. Each mounted list is one small statement. No joins and no `EXISTS` in the first cut.

Wake coarsely. Any local outbox growth, any committed apply, any new op-event marks the monitor dirty. A short debounce, on the order of a frame, collapses a multi-statement transaction and a burst of keystrokes into one execution. That timer is not the projector's ingest debounce. `schedule()` waits and then ingests. If the monitor rides `onResult`, a local insert stays off screen until the replica accepts it. The local wake re-reads the tables as they are. `apply()` and the settle transaction already commit the projected rows and the failure events together, so those wakes re-read at the end of that cycle, not when `RTableGroup.subscribe` fires.

Then compare the new rows to the last snapshot delivered. If the rows are equal and there is no new op-event for this query, do not call the listener. The compare is the small result already in hand. It is cheaper than a React commit, and it drops the useless wake: a write outside the `WHERE`, a project-back that did not change values, chatter on another table. It does not need a before-image of the write. Skip the compare and every such wake re-renders. Keep it and the screen follows the query.

Compare against the last delivery, not against the original rows. A rejected insert delivers the optimistic snapshot first. The settle re-read returns the pre-edit rows plus the failure. Those rows differ from the last delivery, so the listener runs and the optimistic row leaves the screen. If the second `SELECT` matches the first, the compare swallows it.

Op-events still have to be delivered when the rows come back unchanged only if the monitor cannot tell the event belongs to another table. Failures are rare. For a single-table statement the table name is the `FROM` identifier, and events for other tables can be dropped. An ingestion failure is matched against the attempted values in `op` when the reverted row is no longer in the result. A void or reinstate is matched against the previous result by `rowId` or `localId`. The snapshot gives the component both the rows and the events that concern them. The rows show that something disappeared. The event says whether the replica rejected the edit or a concurrent op was voided.

A screen with a handful of these queries can re-run all of them on a coarse wake and let the result compare drop the ones that did not change. The queries are cheap. The renders are not. Scoping the wake to the table named in the statement is a later sharpening, once many monitors share one projection.

The React hook is the v2 `useObjectState` reduced to a subscription. It calls `monitor`, stores the snapshot, and re-subscribes when the statement or its parameters change. A parameter change re-runs immediately. The hook does not debounce on its own, and it does not start sync. The host already opened the projection, the way `useObjectState` assumed the object was already bound to the store.

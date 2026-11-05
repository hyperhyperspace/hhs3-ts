# Critique of the Reactive Query Engine & UI Bindings Plan, v4

A review of [REACT_BINDINGS_PLAN.md](./REACT_BINDINGS_PLAN.md) after the multi-process channel landed. The split is right: the query engine sits on the target, the host streams `apply` and `settle`, a writer paints from its own commit, and the shared database is the state. Five rules in that channel will drop a write, apply it twice, or compare strings in a different order than the index.

## Verdict

Keep the engine in `rdb_adapter`, with `projection.selectFrom` delegating to it. A client target can host monitors without constructing `RdbProjection`, and the adapter already depends on `hhs3_rdb`, so `likeMatch` belongs there.

| Item | Where in the plan |
| --- | --- |
| 1. Dedup each outbox id, not the whole event | §3.2, §3.3 |
| 2. Another process's write arrives on the host's peek | §3.2, §3.3 |
| 3. `ingestExternalCommit` does not write rows | §3.3 |
| 4. `reconnect` refreshes rows and leaves the event cursor | §3.3, §6.2 |
| 5. The watch push is not a `HostStatus`, and a failed send ends it | §3.3, layer 5 |
| 6. Text order is SQLite's UTF-8 binary order | §5.1 |

---

## 1. Dedup Each Outbox Id, Not the Whole Event

§3.3 drops a `local` event when `event.maxOutboxId <= lastNotifiedOutboxId`. Outbox ids are one sequence for every connection.

Process B writes id 10 and sets its cursor to 10. Process C writes id 12 and sets its cursor to 12. The host's pre-drain peek broadcasts `{ changes: [10, 12], maxOutboxId: 12 }`. C's check `12 <= 12` throws the event away, and with it B's row, which C never saw. The same thing happens the other way the next time B writes.

Put the outbox id on each local `TargetRowChange`. Apply the changes whose id is above the cursor, and advance the cursor to the highest id applied. A client that only watches still receives the batch. A client that wrote the newest id still receives the older rows from everyone else. The same rule covers a second copy of an IndexedDB `local` post.

---

## 2. Another Process's Write Arrives on the Host's Peek

`noteLocalCommit` inside `drainChanges` runs when the host ingests, after the projection's debounce, and it runs before the delete, so the payload survives. The writing process does not wait for that. It already painted from its own wrapper. Every other process does.

An empty peek after those rows are gone is not a catch-up. The IPC payload is the record of someone else's local write. A client WAL watch that finds an empty outbox has missed them, and it must not treat that empty read as having caught up.

---

## 3. `ingestExternalCommit` Does Not Write Rows

§3.3 wires `watchCommits` to `ingestExternalCommit`. The host has already committed the rows into the shared file. The client method fans the event into `ReactiveChangeTarget` listeners so monitors can filter and re-query. Writing the `TargetRowChange`s again inserts them twice.

---

## 4. `reconnect` Refreshes Rows and Leaves the Event Cursor

§3.3 marks every monitor dirty on connect, so the screen reads the database as it is. §6.2 starts `lastSeenEventId` at the log's high-water mark and only attaches events above it. Events from the gap stay in the log. They do not attach later when a row scrolls into the page.

A screen that needs the reason for a change it missed calls `opEvents({ afterId: lastSeenEventId })`. Resetting the cursor on reconnect delivers that gap as if it were new, including voids for rows that later enter a page.

---

## 5. The Watch Push Is Not a `HostStatus`

`modules/rhost_client/src/protocol.ts` types a push as `{ id, event: HostStatus }`, and `decodeServerMessage` casts `event` to that. `watchCommits` needs its own push body. The channel stays read-only: it still carries nothing that signs. The client writes through the database, and the host ingests.

`serveClient` swallows a failed `send`:

```ts
try {
    connection.send(encodeMessage(message));
} catch {
    // the other end is gone
}
```

A missed `apply` then leaves the client watching a socket that has gone quiet. A failed send ends that watch. The client reconnects, and §4's re-query runs.

---

## 6. Text Order Is SQLite's UTF-8 Binary Order

§5.1 says projected text is ordered by "UTF-16/code-point" comparison so a native index can serve `ORDER BY`. SQLite's binary collation compares UTF-8 bytes, which follows code-point order. JavaScript's `<` compares UTF-16 code units. For characters outside the basic multilingual plane those orders differ, and a far-edge ignore then drops a row the index would return.

`compareSortTuples` compares text the way that `ORDER BY` does. `NULL` stays smaller than every non-null value, and `bigint` / `decimal` stay lexical, as §5.1 already says.

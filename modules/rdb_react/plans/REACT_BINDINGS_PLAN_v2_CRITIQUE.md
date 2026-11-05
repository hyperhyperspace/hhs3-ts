# Critique of the Reactive Query Engine & UI Bindings Plan, v2

A review of [REACT_BINDINGS_PLAN_v2.md](./REACT_BINDINGS_PLAN_v2.md). One host cursor, the WAL watch, and a fast track at the start of `drainChanges` are the right shape. Five rules in that path drop a committed row, delay the writer's own paint, or turn an ingest batch into a storm.

## Verdict

Keep the single stream from the host, `notifyExternalCommit` as a fan-out that does not write rows, and the registration-ack `reconnect` that leaves `lastSeenEventId` in place.

| Item | Where in the plan |
| --- | --- |
| 1. Do not move `lastOutboxId` past a missing id | §2, §3.2 |
| 2. Bring back the SQLite commit shim | §2, §3.2 |
| 3. Leave the client WAL watcher on | §3.3 |
| 4. Coalesce watcher callbacks | §3.2 |
| 5. A failed `send` ends the watch | §3.3, `rhost_node` |

A second `{ source: 'local' }` is safe. The monitor re-queries, and the snapshot comparison drops the delivery when the rows did not change.

---

## 1. Do Not Move `lastOutboxId` Past a Missing Id

§3.2 reads `rdb_outbox WHERE id > lastOutboxId` and sets `lastOutboxId` to the last row returned. Outbox ids are assigned when the insert runs. A transaction can commit after a higher id has already committed, and `AUTOINCREMENT` does not reuse the earlier id.

1. Process B inserts and is given id 10, and has not committed.
2. Process C inserts, is given id 11, and commits. `fireIfOutboxGrew` sees 11 and sets `lastOutboxId` to 11.
3. B commits. The next read is `id > 11` and does not see 10.

`drainChanges` still ingests 10, because that query is `ORDER BY id` with no cursor. The replica gets the row. No observer got `{ source: 'local' }` for it. An accepted write shows up later on `apply`. A rejected one is reverted by `settle` without the screen ever having shown it, and the failure is not attached, because the row was never in `cachedRowIds`.

Emit every committed outbox row above the cursor that is actually visible, and move `lastOutboxId` only through the contiguous prefix. If 11 is visible and 10 is not, emit 11 and leave the cursor below 10. The next wake emits 10, and it may emit 11 again.

---

## 2. Bring Back the SQLite Commit Shim

§2 drops the handle wrapper so a plain `better-sqlite3` connection wakes the host through the WAL file. IndexedDB and memory already have the other hook. `MemoryTarget.record` fires before it returns. `IdbTarget.onOutboxCommitted` fires from the writing transaction's `oncomplete`. SQLite has no commit hook, so the wrapper is that hook.

Call `fireIfOutboxGrew` once, after the outermost transaction commits. Stay quiet on rollback. `apply` may use the same handle. Its writes do not grow `rdb_outbox`, so the poke finds nothing.

The WAL watch stays. The shim runs only on a handle that goes through it, and only in that process. Another process, a raw connection, and a missed shim still show up when a watcher or the `drainChanges` fast track reads the outbox. The shim uses the cursor rule in §1: after its own commit it emits the rows that commit made visible, including an earlier id that a higher one had already passed.

---

## 3. Leave the Client WAL Watcher On

§3.3 disables the client watcher while `watchCommits` is connected, so the writing process paints only when the host reads `rdb_outbox` and the socket delivers the event. A committed row in the local file stays off the screen while the host is inside a synchronous ingest, or while the watch is down.

Leave the watcher on. The writing process paints from its own file. The host stream carries `apply`, `settle`, and any local row this watcher did not see. A duplicate `local` from the host is the second event §1 already allows.

---

## 4. Coalesce Watcher Callbacks

`watchFile` is at-least-once and does not coalesce. `apply` and `commitIngest` append to the WAL with capture gated off, so they do not insert outbox rows. The host is blocked for the whole synchronous `apply`, and its callbacks arrive afterward as a burst. Every other process is not blocked. Each WAL frame during that batch runs their callback while the host is still writing.

Keep one pending check per process. The first callback schedules a single `fireIfOutboxGrew`. Further callbacks return until that check has run. If more frames arrived while it ran, one more check follows. Emit `{ source: 'local' }` and schedule `syncDatabase` only when a new outbox id is visible. An ingest batch then costs one empty outbox read, and it does not become a `local` event or another ingest.

---

## 5. A Failed `send` Ends the Watch

§3.3 ends the watch when `connection.send()` throws. The Node connection does not throw when the buffer is full:

```ts
send: (line) => { if (!socket.destroyed) socket.write(line); },
```

`socket.write` returns false and keeps the socket open. The client is still watching, and a missed `apply` is not followed by `reconnect`. Treat a false return, or a send that throws, as a dead watch. Close it. The client reconnects, and the registration ack re-queries.

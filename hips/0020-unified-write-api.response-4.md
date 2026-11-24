# Response to the fourth critique of HIP-20

A reply to [the fourth critique](0020-unified-write-api.critique-4.md) of [HIP-20](0020-unified-write-api.md). The HIP was revised in place on 2026-11-19. Paths are relative to `modules/`. The critique was written from reading the code, and each of its claims was checked in the working tree, which has not changed since `6110da1`. E1 is a probe of this response.

## What the revision takes

| Critique | In the revised HIP |
|---|---|
| §2. SQLite's event log serializes the failed op again | Taken, with another fix: ingestion refuses a value that isn't a json literal before rdb sees it (*Changes* 1, "A failed rejection record no longer blocks ingestion"). See below. |
| §3.1. A stored identity reused under another signer | Taken: *Authorship* (`autoAuthor` off), *Changes* 2, *Tests*. |
| §3.2. An already-applied revert marks a row `ingestion_failure` | Taken: that mark only reconciles (*Changes* 1). |
| §3.3. Telling a local row that is still there from one that is gone | Taken: a new method of the change source (*Changes* 1). |
| §4. The IndexedDB writer can't use `target.database` | Taken: one transaction over the target's stores, sharing the facade's write code (*Changes* 4, *Checks*). |
| §4. The writer record in the meta store | Taken: it goes in the capture config store (*Changes* 4). |
| §5. When and where authorship modes are written | Taken another way: a mode per group in the writer record, and the group in each table's meta. See below. |
| §6. rhost's config validation | Taken: *Changes* 7, *Tests*. |
| §7. Defaults on SQLite | Taken, the second option: the shape records which columns have a default (*Changes* 2, 3). |
| §7. Bookkeeping before `openWriter` | Taken: *Changes* 3. |
| §7. The author column in `prefetchKeys`, and text in it | Taken: *Changes* 1, "The author column is captured". |
| §7. Splitting updates under `updateMerge: false` | Taken, as a wording fix: split per column before merging (*Changes* 1). |
| §7. Two translate-time rejects of one row collide | Taken, and widened: they collide across passes too (E1). *Changes* 1, "Each failure is logged". See below. |
| §7. `openWriter` on a read-only query source, and `useWriter` | Taken: *Changes* 2, 9, *Tests*. |
| §7. `writerKeyId()` with no writer key recorded | Taken: it throws `no-writer-key` (*Checks*, *Tests*). |
| §7. `undefined` in `RowValues` | Taken: not given (*Write semantics*, *Tests*). |
| §7. `{ pk }` in the author column | Taken: refused with `author` (*Key columns*, *Authorship*, *Tests*). |
| §7. Memory's author column | Taken: memory keeps it with the row's author (*Changes* 1). |
| §7. Citation drift | Not taken, see below. Two of the HIP's citations were off by one line, and are fixed. |

## §7: failure events collapse, in every pass (E1)

The critique lists this among its smaller points, for two rejects in one pass. A scratch probe shows it reaches further:

| # | Scenario | Result |
|---|---|---|
| E1 | Memory target, the suite's `ledger` table, whose `ref` is readonly. A row inserted in rdb and projected. Two local updates of `ref`, to `B` and then to `C`, each followed by an ingest pass and a projection | Both passes reject the update, and each ingest result reports it. The op event log holds one failure event: the second has the same hash as the first. |

When ingestion refuses a write before rdb sees it, the failed op it records carries no values (`rdb_adapter/src/ingest.ts:457-458`). A failure's hash is the hash of that op (`rdb_adapter/src/ingest_orchestrator.ts:168-173`, `:395`), and all three targets keep one event per op hash and direction. So once a row has one such failure in the log, its later ones never reach it, whatever they wrote and in whichever pass. Two rdb rejections of equal ops collapse the same way. The log is what the UI receives, so the app is told about the first failure only.

The revision has a failure's hash cover the ids of the captured changes it came from. The outbox keeps a change until the settle, so a crash replay reads the same ids and logs once, and every new write gets its own event. This also settles the critique's question in §2 about a fallback hash.

## §2: refuse the value before rdb sees it

The critique would catch the hash error, leave `op` out of the event, and hash the op's text instead. The revision moves the check ahead of rdb. Ingestion refuses a captured value that isn't a json literal (`json.literalProblem`, `json/src/normalize.ts:63`), as it refuses a readonly column. The failure it records then carries a failed op with no values, which hashes on every target and serializes in SQLite's event log (`rdb_adapter_sqlite/src/sqlite_target.ts:470`). The catch-all stays as a safety net, and stores the event without its op. Its hash covers the captured change ids like every failure's, so no hash of the op's text is needed.

## §5: authorship modes per group

The critique offers writing the modes after every sync, or keeping them in the writer record or the schema actions. The revision keeps one mode per group in the writer record, and has each table's meta record its group:

- **`autoAuthor` is set per group.** The projection knows every member group when it reconfigures, which runs before a new group's tables exist (`rdb_projection/src/projection.ts:151-154`).
- **A table's group never changes.** The target writes it once, when it creates the table for that group. Caching it with the rest of the meta is safe, and `writeMeta` keeps it as part of `TableMeta` (`rdb_adapter_sqlite/src/sqlite_target.ts:563-572`).
- **The writer already reads the writer record inside each transaction.** A new mode is seen at the next write, and nothing depends on `PRAGMA schema_version`, which a change of rows doesn't bump.
- **A table that a schema change adds after the projection opened has its group's mode at once**, with no write by the projection.

## §7: the citation corrections

Not taken. The source has not changed since `6110da1`, and each corrected number points at a different line:

| The critique's correction | What that line holds | The HIP's citation |
|---|---|---|
| `ingest.ts:305-331`, the reversal of key columns | `:305` returns a dangling FK | `:316-342`, from `const keyRef` |
| `ingest_orchestrator.ts:455-461`, `isWriteOnceFor` | the skip of an already-deleted row | `:446-452` |
| `ingest_orchestrator.ts:411-415`, the planner rejects | the same lines, with the comment and loop header above | `:413-415` |
| `project.ts:89-90`, the direction mapping | `:89` is the loop header | `:90-91` |
| `sqlite_target.ts:213`, `toLogical` | a comment | `:215` |
| `sqlite_target.ts:508-516`, the capture triggers | `:508` is blank, and `:516` is the trigger's first line | `:505-518`, fixed from `:504-517` |

The critique's own citations are off in the same way in several places, though the mechanisms behind them are as it describes. For example, the event log's serialization is `sqlite_target.ts:470` (cited as `:487`), `writeMeta` is `:563-572` (cited as `:575-583`), and the membership handler is `projection.ts:526-535` (cited as `:455-465`).

## Decisions taken in discussion, 2026-11-19

- **Each failure gets its own event.** A failure's hash covers the ids of its captured changes.
- **A value that isn't a json literal is refused before rdb sees it**, with the catch-all kept as a safety net.
- **Authorship modes are recorded per group**, and each table's meta records its group.
- **The rest of the critique is taken as written**, apart from the citation corrections.

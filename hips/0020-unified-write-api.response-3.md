# Response to the third critique of HIP-20

A reply to [the third critique](0020-unified-write-api.critique-3.md) of [HIP-20](0020-unified-write-api.md). The HIP was revised in place on 2026-11-18. Paths are relative to `modules/`. The labels W22 to W27, Q1, S2 and T1 are the critique's. J1 is a new check of this response.

Between the critique and this revision, rdb gained null writes (`6110da1`). An insert or an update can set a nullable column to `NULL`, an explicit `NULL` wins over the column's default, and a value wins over a concurrent `NULL` (`rdb/README.md`, *Nulls and defaults*). The same change made the three targets capture, ingest and project a `NULL` alike. It settles three of the critique's items in the code, and turns several of the HIP's rules around.

## What the revision takes

| Critique | In the revised HIP |
|---|---|
| §2. An old target ingests every insert as anonymous with `autoAuthor` off | No change. Projections built before the revision are rebuilt, as after any target change while the project is unreleased. |
| §3. A native insert with a `null` ends differently by target | Settled by the null writes. Ingestion keeps an insert's `NULL` only where the column has a default, and leaves it out elsewhere, on every target (`rdb_adapter/src/ingest.ts:355-370`). The HIP's fix "Updates to `NULL` are rejected the same way on every target" is gone. |
| §4. An IndexedDB `put` that leaves out a column | Settled by the null writes for business columns. The facade reads a missing property as `NULL`, so the `put` clears the column, or rdb refuses it and the row is reverted (`rdb_adapter_idb/src/idb_facade.ts:251-267`). The author column is kept instead, see below. |
| §5. An explicit `NULL` for a column with a default, on SQLite | Settled by the null writes: the `NULL` wins over the default, in rdb and on every device. |
| §6. An immediate transaction on SQLite | Taken: *Changes* 3. |
| §7. The test plan | Taken where the cases still apply. W22 to W24 now end as the null writes have them, and that change's suites cover them natively (`rdb_adapter_test/src/ingestion_suite.ts:970-1004` on all three targets, `rdb_adapter_idb/test/idb_target_tests.ts:234`). The writer suite gains the writer's versions, and the race with a host commit (*Tests*, "A race with the host"). |
| §8. `no-writer-key` at the write | Taken: *Checks*, *Tests*. |
| §8. A SQLite update with `{ pk }` reads the row first | Taken further: every SQLite update reads the row first (*Changes* 3), see below. |
| §8. The shape holds FK target tables that no check uses | Taken, the second option: they come out of the shape. *Considered and not taken* says why the writer doesn't refuse a dangling FK. |
| §8. The claim about concurrent edits | Taken, and extended to clears (*Write semantics*, "Unchanged columns"). |
| §8. A native insert that names another key | Taken: rejected under both settings (*Authorship*, *Changes* 1). |
| §8. The nullability backfill in *Changes* 4 | Taken: removed. |
| §10.1. A native change of the id column on SQLite | Taken into *Changes* 1, with a test. |
| §10.2. Memory query rows leave out empty columns | Taken into *Changes* 1, with a test. Memory keeps a `NULL` as a missing key, so this now also decides what a `null` written on memory reads back as. |

## What null writes change in the HIP

Five of the HIP's rules assumed that rdb had no `NULL`:

- **An insert's `null`** was "not given". It is now `NULL`, which differs from leaving the column out only where the column has a default. The writer's inserts reach rdb through ingestion and get its rule, so the op carries the `NULL` only there.
- **An update to `null`** was refused, with `null-update`. It is now a clear. `not-null` replaces the reason code, for a `null` in a column that isn't nullable, in an insert or an update.
- **The author column** was the one column where `null` was a value. `null` now means `NULL` everywhere, and in the author column that is an anonymous insert. The author table stays as it was.
- **The unchanged rule** matters more. A write of an unchanged value wins over a concurrent clear, so a form that wrote every field would undo another device's clear of a field it didn't change. The rule and the native comparison by value stay, with that reason added.
- **IndexedDB writes** go through the facade, which stores a `null` as a missing key (`rdb_adapter_idb/README.md:97`).

Two fixes of *Changes* 1 were done by that change and leave the HIP: updates to `NULL`, and column drops on SQLite with capture on.

## Native defaults, without `ON CONFLICT REPLACE`

The critique's §5 proposed a revert after an insert that left out a column with a default. Before the null writes landed, the discussion looked at making the default native in each target instead, and briefly at `NOT NULL ON CONFLICT REPLACE` on SQLite, which turns an explicit `NULL` into the default. That was dropped: an explicit `NULL` where a column doesn't allow one should fail, as SQL has it. With null writes, a `NULL` where the column allows one is kept, also where the column has a default.

What stays is the default each target already applies natively:

- **SQLite** applies its `DEFAULT` to a column the statement leaves out, and the capture trigger records it.
- **IndexedDB and memory** fill in the default of a column a local insert leaves out, in the row and in the captured change, and a `null` doesn't count as left out. Project-back's fill of a new row already follows that rule (`rdb_adapter_idb/src/idb_target.ts:501-503`, `rdb_adapter/src/memory_target.ts:839-841`). Local inserts still lack it. The delta lists only written columns (`rdb/src/rtable/delta.ts:6-7`), so project-back never brings the default to the device that wrote the insert, and the null-writes case that leaves a column out checks it in rdb and on a second device only (`rdb_adapter_test/src/ingestion_suite.ts:970-1004`).

## §4: a `put` keeps the stored author

The critique proposed that a column missing from a `put` is an update to `null`, the author column included, which the planner rejects and reverts. For business columns that is now what happens, as a clear. For the author column the revision has the facade keep the stored author when a `put` leaves it out or gives it `null`, and capture nothing for it. A `put` that gives another key's id is still a change of the author, rejected and reverted.

- **No op can change a row's author.** The row hash is derived from the uuid and the author (`rdb/src/rtable/hash.ts:10-15`), an update's author is that update's signer, and a null write names only schema columns (`rdb/src/rtable/validate_ops.ts:45-48`, `:84-88`). A missing author can't ask for anything rdb could do.
- **The end state is the same either way.** A rejected change of the author is reverted to rdb's author. The rejection only adds a failure event, and a window in which the row shows no author.
- **It would fire for every form.** A record built from a form has no author column, so every such `put` would log a failure.
- **Only `put` needs it.** It is the one native write that replaces a whole record. SQL's `UPDATE` and memory's `localUpdate` change only the columns they name.
- **The facade already owns such fields.** It sets `table`, `id` and the id column itself on every write (`rdb_adapter_idb/src/idb_facade.ts:502-504`).

## §8: every SQLite update reads the row first

The critique adds `{ pk }` to the cases in which a SQLite update reads the row first. The list would then have five entries: a `null`, the id column, the author column, a `{ pk }`, and no columns. The `null` entry is gone with null writes, since `not-null` depends only on the schema. The revision drops the list and has every update read the row, as IndexedDB already does. The read is one lookup by primary key, inside an immediate transaction the call already holds. `update` then resolves from the read, and `delete` keeps resolving from the statement's count of changed rows.

## §8: FK target tables out of the shape

The critique offers two ways out: the writer refuses a local id the referenced table never had, or the target tables leave the shape. The revision takes the second. Deciding "never had" exactly needs the referenced table's rows, its sync table and the outbox of changes not yet ingested. Ingestion already rejects such a reference at planning and logs an op event (W26), and the HIP leaves FK validity to ingestion, with readonly columns, `ALLOW` rules and voiding.

## J1: a json value with a `null` inside it

With null writes, the HIP's first fix in *Changes* 1, "A failed rejection record no longer blocks ingestion", lost the case it was written for. The null-writes plan names another case in its place, which it didn't run (`plans/null-writes.md`, *Found while reading* 3). A scratch probe ran it:

| # | Scenario | Result |
|---|---|---|
| J1 | Memory target, a table with a nullable json column. Two local inserts: one without the column, then one whose json value is `{ a: null }`. Two ingest passes | Each pass throws `Cannot create an entry: its payload contains null.` The outbox still holds both changes, so the first insert doesn't land either. |

Appending the op throws, the orchestrator records that as a failure, and recording it hashes the same payload and throws again (`rdb_adapter/src/ingest_orchestrator.ts:427-436`, `:168-173`, `dag/src/dag.ts:65-69`). The fix stays, with this case as its reason. The writer refuses such a value before it writes: `wrong-type` now covers a value that isn't a json literal (`json.literalProblem`, `json/src/normalize.ts:63`). The probe is not in the repo.

## Decisions taken in discussion, 2026-11-17 and 2026-11-18

- **No `ON CONFLICT REPLACE`, and no `NOT NULL` for columns with a default** (2026-11-17). An explicit `NULL` where it isn't allowed fails.
- **Defaults are native** (2026-11-17): SQLite's `DEFAULT`, and the fill of the IndexedDB facade and of memory's local inserts.
- **The HIP follows rdb's null writes** (2026-11-18), as above.
- **An IndexedDB `put` keeps the stored author** (2026-11-18), as above.
- **Anonymous groups** (2026-11-18). The writer refuses a key in the author column, as the HIP already had. A native insert that names one, the writer key's included, goes through and is rejected at ingestion, with its row reverted and an op event logged. `autoAuthor` is ignored there rather than refused: the projection records the group's tables with the mode `anonymous`, because one projection-wide default covers groups of both kinds, and a group's identity provider is fixed when it is created (`rdb/src/rtable_group/payload.ts:123`). `RdbProjection` refuses `autoAuthor: false` with `authorColumn: false` only for a group with an identity provider.

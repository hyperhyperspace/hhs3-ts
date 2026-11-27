# Implementing HIP-20: the unified write API

- **Status:** Phases C, D and E done. C landed in `22589f1`, and D is outside the repo.
- **Written:** 2026-11-25, against `527bb38` with HIP-20's citation pass of the same day, and revised the same day for decision 11 and on 2026-11-26 for decisions 12 to 16. Line numbers are from that tree.
- **Design:** [HIP-20](../hips/0020-unified-write-api.md). Its *Changes* lists the work by module, and says in which order what is left lands.
- **Earlier plan:** the plan approved on 2026-11-19, in a Claude Code session, was never committed. Its phases were 0 (a baseline), A (the HIP text), B (*Changes* 1), C (the draft writer), D (the trial app) and E (the rest). A and B are done, and HIP-29 came between B and C. This plan replaces it for C to E, keeps their names, and keeps its decisions (*Decisions*).
- **Modules:** rdb_adapter, rdb_adapter_sqlite, rdb_adapter_idb, rdb_adapter_test, rdb_adapter_test_gen (the mock context's hash suite, for the BLAKE3 case), rdb_projection, rhost, rhost_client_node, rhost_client_browser, rdb_react, and for decision 15 rhost_client, crypto and mvt. Outside the repo: the trial app, `/Users/santi/Hyper/mb-chat-exploration`, made again, since the copy set up on 2026-11-19 is gone.

## Summary

HIP-20 adds `RowWriter`: `insert`, `update` and `delete` on a projection target, in the names and value forms queries return, with one meaning on SQLite, IndexedDB and memory. Part of it is in the code: *Changes* 1, and what HIP-29 needed, the writer record and `autoAuthor`. Decision 11 changes the shape of those two. What is left is that change and the writer itself.

It goes in three phases, each green and test first, with a stop after each of the first two:
- **C, the draft writer.** First the authorship setting and the writer record in their new shape, then the types and the checks, a writer on each target, `rowHash`, `localId` and `tableNames` on the query source, `openWriter` on the projection, the rhost and client changes, and `useWriter`. The writer suite tests it on all three targets.
- **D, the trial app.** A copy of memoblocks' chat app writes only through the writer. After it the interface is fixed, on your go-ahead.
- **E, the rest.** The ingestion suites write through the writers, then the remaining tests and the docs.

## Background

Each term uses only the ones above it.
- **Target.** Where a projection copies a replica's groups: SQLite, IndexedDB or memory. Each projected row has a *local id*, an integer specific to the device, and a *row hash*, rdb's id for the row.
- **Native writes.** How apps write today: SQL on SQLite, the IndexedDB facade (`target.database`), and memory's `localInsert`, `localUpdate` and `localDelete`.
- **Capture, ingestion and project-back.** Capture records each native write in the target's *outbox*. Ingestion turns each recorded change into an rdb entry. Project-back writes rdb's rows back to the target. A change rdb refuses is reverted and logged as an *op event*.
- **The writer.** `RowWriter`, which this plan builds. It checks a write against the table, then makes it as a native write in one target transaction, so capture records it like any other.
- **Table meta.** What a target keeps about each projected table: column types, defaults, FK and key columns, the author column, and, since HIP-29, the table's group and its name in the schema. From C1 it also says whether that group has an identity provider, the table of keys rdb checks the group's signatures against.
- **The authorship setting.** How an insert's author is chosen in a group with an identity provider. HIP-29 landed it as `autoAuthor`, on or off, a default that a per-group override could change. From C1 it is `author`, `auto` or `manual`, one for the whole projection. A group without an identity provider never signs.
- **The writer record.** What a projection records in its target: the writer key's id and the replica's hash suite, and, as HIP-29 landed it, each group's member name and authorship mode (`on`, `off` or `anonymous`), written again whenever the groups change. From C1 it holds the `author` setting in their place, and is written when the projection opens.
- **Query source.** The read surface of HIP-5: `selectFrom` and `monitor`. `QueryEngine` serves one over a target, and `RdbProjection` is one.
- **The trial app.** A copy of memoblocks' chat app, with its own schema release and catalog, whose writes all go through the writer.

## What has landed

- ***Changes* 1:** in `4db216b`, with the writer suite's fixture table (`typedTables`, `rdb_adapter_test/src/group_fixture.ts:121`). Its native insert over an existing local id landed with HIP-29 in `3bae793`.
- **From HIP-29** (`3bae793`, `efa3d7e`):
  - the writer record: its type (`rdb_adapter/src/types.ts:692-713`), and its store on each target (`rdb_adapter/src/memory_target.ts:478`, `:482`; `rdb_adapter_sqlite/src/sqlite_target.ts:1286`, `:1292`; `rdb_adapter_idb/src/idb_target.ts:615`, `:622`);
  - each table's group id and name in the schema, in memory's `TableState` (`memory_target.ts:88-89`), SQLite's `TableMeta` (`sqlite_target.ts:70-81`) and IndexedDB's `TableMetaRecord` (`rdb_adapter_idb/src/idb_schema.ts:41-55`), read by `tableGroup(table)` on each (`memory_target.ts:487`, `sqlite_target.ts:1304`, `idb_target.ts:631`);
  - `autoAuthor`: in `AdapterConfig` (`types.ts:96`), in `RdbProjection`'s options with its per-group override (`rdb_projection/src/scope.ts:127`) and its refusal of a signed group without an author column (`[RDBPROJ21]`), and in rhost's projection config (`rhost/src/config.ts:36`, `[RHOST28]`);
  - ingestion under `autoAuthor` off, and capture filling in an empty author column;
  - the projection recording the writer record at open and on each reconfigure, interning its writer key as it does (`rdb_projection/src/projection.ts:568-587`, `:574`);
  - IndexedDB's `patch`, a partial update (`rdb_adapter_idb/src/idb_facade.ts:428`);
  - C1 changes the record, `autoAuthor`, capture's fill and the recording, and adds to the table meta (decision 11).
- **Since:** op events reach the query snapshots that show their rows (`a5df3ea`); a query starts when something subscribes to it (`49c79dd`); opening a writer starts nothing and holds nothing (`527bb38`).
- **Tests of the landed parts:** `[<label>-IN25]` to `[<label>-IN89]` in the shared ingestion suites, `[ADPTS-SQL18]` to `[ADPTS-SQL28]`, `[ADPTI-07]` to `[ADPTI-11]`, `[ADPTM01]` to `[ADPTM04]`, `[ADPTI21]` to `[ADPTI24]`, `[RDBPROJ15]` to `[RDBPROJ27]` and `[RHOST28]`.

## Decisions

Carried over:
1. **The trial app is a new project**, `/Users/santi/Hyper/mb-chat-exploration`, built from mb-chat's code, with its own packs and a throwaway key, and complete enough to exercise every rule HIP-20 names (2026-11-19). Its design is detail 14.
2. **Open questions 2 (typed rows) and 4 (a new row's hash before ingestion) stay deferred** (2026-11-19).
3. **Bindings get table names from the projection** (2026-11-19), now HIP-20's *Table names*.
4. **Settlement parity at the source.** A target settles a local write as rdb does, and the cases where it can't are named exceptions, handled one at a time (2026-11-21).
5. **The writer compares only when its update is given `compare`** (2026-11-21, in HIP-29's plan).
6. **The writer record and `autoAuthor` land ahead of Phase C, with HIP-29.** `openWriter`, `writerKeyId()` and the writer's own refusals stay in Phase C (2026-11-22, in HIP-29's plan). Decision 11 changes their shape.
7. **Opening a writer starts nothing and holds nothing** (2026-11-25, `527bb38`).
8. **Citations in HIP-20.** A plain `path:line` points at the code as it stands. A sentence about code that *Changes* 1 or HIP-29 replaced cites `6110da1:path:line`, the tree before *Changes* 1 (2026-11-19). Code that *Changes* 1 added and HIP-29 removed exists in neither tree, so it cites `4db216b`, where it was added (one case, 2026-11-25).
9. **Nothing is committed or pushed by me.** You commit.
10. **Nothing is measured until it's built** (2026-11-25). HIP-29's two open questions, how long local writes hold remote changes back and what its records cost on IndexedDB, are measured neither in the trial nor anywhere in this plan. On 2026-11-23 you dropped the IndexedDB benchmark "for now". Its record added "to be measured in HIP-20's trial app", which you never asked for, and HIP-29 no longer says it.
11. **One authorship setting, and a writer record without per-group entries** (2026-11-25). In your words: "I don't think we need that option per-group. manual should mean: if the user set an id, it is used, if not, it is left as anonymous. auto: we auto-sign with the default identity. if the group has no id provider, we never sign." Then you asked why the database needs the member names, and approved dropping them with the rest of the per-group entries. HIP-20 now says:
    - `autoAuthor` becomes `author`, `'auto'` (the default) or `'manual'`, one for the whole projection. A `configOverride` can't change it for one group.
    - Under `manual`, an insert that leaves the author out is anonymous, as one that gives `null` is, through the writer and natively alike. HIP-20 had the writer refuse it. The only id an insert can name is the writer key's, the one key the device signs with.
    - The writer record holds the writer key's id, the hash suite and the setting, and the projection writes it when it opens. The member names and modes go.
    - Each table's meta records whether its group has an identity provider, set when the table is created. Capture's fill and the writer read it there.
    - `tableNames` reads the tables' meta. An unknown member gives `{}`, as a member whose tables aren't projected yet does, where HIP-20 had it throw. The writer no longer refuses a table whose group isn't projected: an RDb never loses a member group.
    - It lands first in Phase C, as C1, since the writer reads the record and the meta.
12. **A client's `registerKey` throws when the target has no writer record** (2026-11-26), with an error that names the missing record. A target is left without one when its projection's open stops before recording it, for example on a config the projection refuses, which it checks before it writes the record (`rdb_projection/src/projection.ts:543-553`). An IndexedDB target then has its stores but no record, since the host opens the target first and opening it creates every store (`rhost/src/host.ts:818`, `:908`; `rhost_browser/src/platform.ts:157-161`; `rdb_adapter_idb/src/idb_schema.ts:156`). A SQLite target creates its tables at their first use, so there the key table is missing too, and `registerKey` already fails. Without the record the client can't know the replica's hash suite, and a guess, such as today's SHA-256, could intern the key under a hash rdb doesn't use, the bug HIP-20's *Changes* 8 fixes. HIP-20 now says so in *What the projection records in the target*, *Changes* 8 and *Tests*, and lists SHA-256 under *Considered and not taken*.
13. **`anonymous` and `schemaName` are required, on the `create-table` action and in the table meta** (2026-11-26). They were to be optional, a missing one read as a group with an identity provider and as the projected name, only so that the tests that build the action by hand needn't change. A builder that left one out would then pass the type checker and get a wrong answer. You kept the names. The tests give both fields (*The authorship change (C1)*). A store whose tables were created before C1 has no `anonymous` in its table meta and isn't upgraded: it is projected again.
14. **No "sending…" marker in the trial** (2026-11-26). In your words: "we said that ingestion was irrelevant to the app, so writing "sending..." until ingested should not be a feature." A projected row reads the same before and after ingestion, so the trial shows nothing about it. Detail 14 had the marker look for a row hash.
15. **A key lookup and a key check** (2026-11-26), from the trial's member invite, which needed a key's id before writing and could get it only from `registerKey`, which stored even a malformed key. In your words: "don't we have a function, analogous to registerKey, that tells us the local id for a key? we should, and it shuould be able to detect that a key is not in the projection. this lookup could be paired with a validateKey that checks whether it is well formed". You approved writing it into HIP-20. HIP-20 now says:
    - the rhost clients gain `lookupKey(publicKey)`: the key's id, or `undefined` when the projection doesn't have it, without storing it;
    - mvt gains `validateKey(publicKey)`, over a new `publicKeyProblem(key)` of each signing suite in crypto: an `ed25519` key must be a point of the curve and not one of its 8 small-order points, an `ml-dsa-65` key is checked by its length, and the hybrid by both halves;
    - every `registerKey` checks the key, and `RdbProjection`'s also checks that the hash it is given is the key's. The writer's `{ pk }` check becomes `validateKey`.
16. **Ed25519 verification is strict** (2026-11-26), a separate change, outside HIP-20. Found while answering decision 15: crypto's `ed25519.verify` used noble's default ZIP-215 rules, under which a fixed signature verifies against a small-order public key for every message, and rdb checks only that an identity row's key id is its key's hash. You approved fixing it now. It verifies with `zip215: false`, in `crypto/src/signing.ts`, with the test `[SIGN_04]` and a paragraph in crypto's README.

## The authorship change (C1)

Decision 11, by module. It reworks code HIP-29 landed, so most of its cases are HIP-29's, rewritten.
- **rdb_adapter.**
  - `AdapterConfig.autoAuthor` (`types.ts:91-96`) becomes `author?: 'auto' | 'manual'`. Ingestion reads it where it read `autoAuthor` (`ingest_chain.ts:373`), and the planner's `autoAuthorOff` (`ingest.ts:104-106`) becomes `manual`.
  - `WriterRecord` (`types.ts:692-701`) becomes `{ writerKeyId?, hashSuite, author }`, and `AuthorshipMode` goes.
  - The `create-table` action (`types.ts:514-515`) gains `anonymous: boolean`, true for a group without an identity provider, and its `schemaName` becomes required (decision 13). `createTableAction` (`schema_actions.ts:167-182`) sets `anonymous` from a flag that `initialSchemaActions` and `schemaDeltaActions` (`:189`, `:214`) take, and `project.ts` passes the group's (`:149`, `:160`). The comment above the action that lets tests leave `schemaName` out goes (`types.ts:511-513`).
  - Memory keeps the flag in `TableState` (`memory_target.ts:55-90`), with `schemaName` required there too. `createTable` (`:757`) takes both from the action, without the fallback to the projected name (`:787`), and `tableGroup` (`:487`) returns both. `fillAuthor` (`:379-384`) fills only where `anonymous` is false, and only when the record says `auto`.
- **rdb_adapter_sqlite.** `rdb_writer_record` (`sqlite_target.ts:475-478`) gets an `author` column in place of `groups`, and `recordWriter` and `readWriterRecord` (`:1286-1300`) follow. `TableMeta` (`:70-81`) and `rdb_table_meta` gain the flag, with `schemaName` required in both. `createTable` takes both from the action, without the fallback to the projected name (`:876`), `writeMeta` (`:686-695`) keeps them, and `tableGroup` (`:1304`) returns them. The insert trigger that fills in the author (`:594-613`) is installed only where `anonymous` is false, and compares the record's `author` with `'auto'` where it read its group's mode.
- **rdb_adapter_idb.** `WriterRecordRecord` (`idb_schema.ts:107-112`) follows the record. `TableMetaRecord` (`:41-55`) gains the flag, with `schemaName` required. `createTable` takes both from the action, without the fallback to the projected name (`idb_target.ts:294`), and `tableGroup` (`:631`) returns them. The facade's fill (`idb_facade.ts:600-616`) reads the flag from the meta record, and `author` from the writer record.
- **rdb_adapter_test.** `recordWriter` and `projectAs` (`ingestion_helpers.ts:54-77`) record the setting, not a group's mode.
- **rdb_projection.** The option becomes `author` (`projection.ts:66`). The scope sets it on every member after the override, as it sets `tableNames` and `tablePrefix` (`scope.ts:41-43`, `:120-127`). The check (`projection.ts:543-551`) refuses, under `manual`, a group with an identity provider and `authorColumn: false`. `recordWriter` (`:568-587`) writes the three values, and runs once at open, not in each reconfigure (`:553`).
- **rhost.** `ProjectionConfig` takes `author` (`config.ts:36`), validated as `'auto'` or `'manual'` (`:201`, `:214-216`), and the host passes it on (`host.ts:826`, `:916`).
- **Docs.** The rdb_adapter, rdb_projection and rhost READMEs describe `autoAuthor` since HIP-29, so they change with the code and describe `author`. The rest of *Changes* 10 waits for Phase E.
- **Cases rewritten for the new shape:** `[<label>-IN40]`, `[<label>-IN42]` and `[<label>-IN43]` in the shared ingestion suite (capture's fill, a record without a writer key, and ingestion under `manual`); `[ADPTI22]` (the planner under `manual`); `[ADPTI-10]` on IndexedDB (a record written by hand); `[RDBPROJ20]` (the record at open); `[RDBPROJ21]` (the option, without its per-group override); `[RHOST28]` (the config field).
- **Actions the tests build by hand** give both fields, and no outcome changes. There are 34: 19 in `rdb_adapter_sqlite/test/sqlite_target_tests.ts`, 4 in `rdb_react/test/run_tests.ts`, 3 in `rdb_adapter_test/src/projection_tests.ts`, 2 each in `rdb_adapter/test/query_monitor_tests.ts` and `rdb_adapter_idb/test/idb_index_tests.ts`, and 1 each in `rdb_adapter/test/memory_local_tests.ts`, `rdb_adapter/test/query_eval_tests.ts`, `rdb_adapter_idb/test/idb_facade_error_tests.ts` and `rdb_adapter_idb/test/idb_target_tests.ts`.
- **New cases:** the table meta records whether the group has an identity provider, on all three targets, also for a table a schema change adds later; a reconfigure that adds a group leaves the record as it was; a `configOverride` that returns `author` changes nothing; rhost refuses an `author` other than `'auto'` or `'manual'`. They take the next free numbers (*Phases*).

## Implementation details

Proposed. Each one fixes where code goes, or goes beyond HIP-20's wording, and then says so.

1. **Types** (`rdb_adapter/src/types.ts`): `KeyValue`, `RowValues`, `RowWriter`, `WritableTarget`, `isWritableTarget`, `keyValue`, `WriteRefusal` and `WriteRefusedError`, as HIP-20's *RowWriter* has them, next to the writer record. The comment at `:180-182`, which HIP-20 cites for "the outbox doesn't record which changes an app wrote together", still says bundling is dependency-driven, which HIP-29 ended. It is fixed here.
2. **The checks** go in a new `rdb_adapter/src/writer.ts`: `checkWrite(shape, write, row?)`, one pure function over a `TableShape` with the fields HIP-20's *Changes* 2 lists. Each target builds the shape inside the write's transaction, from its meta and the writer record, and calls it. It throws a `WriteRefusedError`, or returns the write as the native path is to make it: the row's own id and author set aside, the unchanged columns dropped under `compare`, and each `{ pk }` checked and given its key hash, computed with the recorded suite. One function on all three targets, so they can't drift.
3. **Table names.** A target interface, `TableNameIndex`, with `tableNames(member)`. It lists the tables whose meta names a group, and keeps each one whose projected name is the member's prefix, then `_` and its name in the schema, keyed by that name. The prefix is the member name made safe as an SQL identifier by the scope's `sanitizePrefix` (`rdb_projection/src/scope.ts:48-51`), which moves to rdb_adapter's `names.ts` so that the scope and the targets share it. A member no table matches gives `{}` (decision 11). Each target implements it next to `tableGroup`. `QueryEngine`'s source and `RdbProjection` answer through it, and so does a client that opens the target.
4. **`rowHash` and `localId`** on the query source go through the target's `RowIdentityIndex` (`types.ts:774-781`), which the three targets implement (`memory_target.ts:653`, `:662`; `sqlite_target.ts:1736`, `:1744`; `idb_target.ts:991`, `:998`).
5. **The query source's `openWriter`.** `QuerySourceImpl` (`rdb_adapter/src/query_builder.ts:178-191`) has it only when its engine's target is a `WritableTarget`, so `isWritableTarget(source)` answers. `QueryEngine` (`rdb_adapter/src/query_monitor.ts:38-51`) hands it the target.
6. **Memory** (`MemoryTarget.openWriter`).
   - `TableState` (`memory_target.ts:54-90`) gains the nullable columns, from each column's `ColumnDef` at create-table and at add-column. HIP-20 names this for IndexedDB only, but memory's checks need it too.
   - A write runs synchronously: the checks, then `localInsert`, `localUpdate` or `localDelete` (`:288`, `:336`, `:364`), so capture records it as a native write. A `{ pk }` is interned with `internKey` (`:876`) once the checks pass and the row is found.
7. **SQLite** (`SqliteTarget.openWriter`), as HIP-20's *Changes* 3 has it:
   - opening runs `ensureBookkeeping` (`sqlite_target.ts:453`), and refuses a target without capture;
   - each call is one immediate transaction. It reads `freshMeta` (`:703`), the writer record, `rdb_capture_config.enabled`, the table's meta (`:715`) and its `PRAGMA table_info`, cached per schema version, and the row of an update or a delete. Then it checks, interns keys (`internKey`, `:1057`) and runs the statement, with values through `toParam` (`:117`);
   - `noteLocalCommit()` (`:1628`) follows each call that wrote;
   - the statement builder of `sqliteLocal` (`rdb_adapter_sqlite/test/sqlite_target_tests.ts:132`) moves into `src`.
8. **IndexedDB** (`IdbTarget.openWriter`), as HIP-20's *Changes* 4 has it:
   - each call is one native read-write transaction over every store, opened with `env.withReadWrite`. It must include the key counter (`COUNTERS`, read by `internKey`, `idb_target.ts:464`), which a facade transaction leaves out (`idb_facade.ts:943-946`);
   - the facade's write steps are methods of `FacadeObjectStore` chained on a `FacadeRequest`: `issueWrite` (`:509`), `patchOne` (`:440`), `deleteOne` (`:523`) and `queueOutbox` (`:631`). They become functions over a native transaction and a meta record, which the facade and the writer both call. An update takes `patch`'s step, which is HIP-20's read-merge-write;
   - `TableMetaRecord` (`idb_schema.ts:41-55`) gains the nullable columns;
   - a table missing from `tableNameSet` (`idb_target.ts:91`) is read from the meta store in the transaction before the writer refuses it;
   - a call resolves when its transaction completes, and announces its local commit as the facade does (`onOutboxCommitted`).
9. **The writer suite.** `createWriterSuite(label, factory)` in a new `rdb_adapter_test/src/writer_suite.ts`. It takes the ingestion suites' factory (`IngestionHarness`, `ingestion_suite.ts:63`), opens the writer on its target, and writes to `typedTables`. It runs on memory in `rdb_adapter_test/test/run_tests.ts`, and on SQLite and IndexedDB next to the other shared suites (`rdb_adapter_sqlite/test/run_tests.ts:13-23`, `rdb_adapter_idb/test/run_tests.ts:15-27`). Its cases are named `[<label>-WR01]` and on, as the ingestion suite's are `[<label>-IN01]` and on.
10. **rdb_projection.** `openWriter()` opens with or without a writer key. Without one, each write to a group with an identity provider refuses with `no-writer-key`, and a write to an anonymous group is captured, as HIP-20's *Checks* and HIP-29 have it. `rowHash`, `localId` and `tableNames` go through the target, as the query engine's do (`rdb_projection/src/projection.ts:440-455`).
11. **rhost.** `openProjection` drops `registerWriterKey` (`rhost/src/projection.ts:16`, `:32-37`; passed at `rhost/src/host.ts:825`, `:915`). The projection already interns its writer key (`rdb_projection/src/projection.ts:574`).
12. **The rhost clients.** `registerKey` hashes with the suite the writer record names (`createBasicCrypto().hash(name)`), not with SHA-256 (`rhost_client_node/src/client.ts:31`, `:261-262`; `rhost_client_browser/src/client.ts:28`, `:321-324`). Without a record it throws, naming the missing record (decision 12). It already fails before the host's first start, when the key table doesn't exist either (`rhost_client_node/README.md:34`, `rhost_client_browser/README.md:28`).
13. **rdb_react.** `useWriter()` in a new `rdb_react/src/use_writer.ts`, exported from `index.ts`, with a memo keyed by the source, as `useQuery`'s is (`use_query.ts:19-26`). Its tests' memory targets (`rdb_react/test/run_tests.ts`) record a writer record with a writer key where their tables' groups have an identity provider, since a writer refuses a write there without one (`no-writer-key`).
14. **The trial app**, as decided on 2026-11-19, with what changed since:
    - **The project.** mb-chat's sources (`/Users/santi/Hyper/mb-chat`, which is not a git repo and loads hhs3 from its built `dist`), without `node_modules`. The package is `@memoblocks/chat-exploration`, outside the workspace list. `APP` (`src/app/host.ts:20`) becomes `'mb-chat-exploration'`, so its browser stores stay apart from mb-chat's.
    - **Its packs**, made with rdb_tools' `rpack` and `rkeys`, in its own registry and keystore:
      - schema `mbx:chat` 1.0.0: mb:chat plus `channels.archived boolean DEFAULT false`, and a `reports` table with `messageId` (a readonly FK), `reason` (a nullable string) and `resolved boolean DEFAULT false`. Anyone may insert a report, and moderators may update or delete one;
      - catalog `chatx` 1.0.0, importing `mb:user` 1.0.0 unchanged, with its release file copied from `mb-registry`;
      - both signed by a throwaway key, `exploration`, in `mb-chat-exploration/.keys/keys.json`.
    - **`author: 'manual'`** for the whole projection, in the host's config. A signed insert names `writerKeyId()` as its author, and an anonymous report leaves the author out.
    - **The port.** `src/app/writes.ts` goes away. The app writes through `host.projection.openWriter()`, and components through `useWriter()`. Table names come from `tableNames('chat')` and `tableNames('user')`. A new member's key is a `{ pk }`. An edit updates the fields it changes, where today it `put`s a spread row (`writes.ts:64-68`). The "sending…" marker (`src/chat_server.tsx:96`) goes (decision 14).
    - **New UI:** a channel-edit form that saves every field with `compare`, where emptying the topic clears it; an anonymous "report" action on messages; a moderators' list of reports with a "resolved" toggle.
    - **A Node end-to-end test** (`test/binding.test.ts`), with a host set up as `rhost_client_node/test/run_tests.ts` sets one up: rhost_node in a temporary folder, the trial's catalog, `author: 'manual'`. It runs every action through the binding, then checks rdb's rows, the op event log (no failures but the expected ones) and the projected rows.

## Phases

Each phase keeps the tree green and is test first: every new case fails before its change. New case numbers continue from:
- the shared ingestion suites, IN90, and the new writer suite, WR01;
- SQLite, ADPTS-SQL29; IndexedDB, ADPTI-12; memory, ADPTM05; the planner, ADPTI25;
- rdb_projection, RDBPROJ31; rhost, RHOST29; the clients, RHOST_CLIENT_NODE04 and RHOST_CLIENT_BROWSER02; rdb_react, REACT10.

### P0: baseline

- Build everything, run every suite listed under *Verification*, and record the counts.
- Map each case of HIP-20's *Tests* to the case that covers it now, if one does. *Changes* 1 and HIP-29 brought many, such as the native author cases (`[<label>-IN30]` to `[<label>-IN33]`). The others are this plan's to write, in the phase their change lands in.

### Phase C: the draft writer

In this order, each module rebuilt before its dependents:
- **C1. The authorship change** (decision 11, *The authorship change (C1)*), in rdb_adapter, rdb_adapter_test, the SQLite and IndexedDB adapters, rdb_projection and rhost, with the cases it lists. The writer doesn't exist yet, so they write natively and through the projection.
- **C2. rdb_adapter** (details 1 to 6). The writer suite starts here, on memory, with HIP-20's cases for values, the write semantics, key values, authorship under both settings, refused writes, a voided delete, opening a writer, writers that cannot replicate, row identity and table names.
- **C3. SQLite** (detail 7). The suite runs on SQLite, with the observer targets and the race with the host.
- **C4. IndexedDB** (detail 8). The suite runs on IndexedDB, with the observer targets, with and without a commit listener.
- **C5. rdb_projection** (detail 10), with HIP-20's projection cases that need the writer: an insert through `projection.openWriter()`; a writer in another process that sees a new `author` at its next write.
- **C6. rhost** (detail 11). Its suite passes as C1 left it.
- **C7. The clients** (detail 12), with two cases: on a BLAKE3 replica, `registerKey` returns the id of the key hash rdb computes, and on a target with no writer record it throws.
- **C8. rdb_react** (detail 13): `useWriter()` throws when the source can't write, and an insert through it re-renders a `useQuery` over that table.

**Stop C.** I report each step's cases, the suite counts against P0, anything the guardrail caught, and the HIP edits made. I continue only on your go-ahead.

### Phase D: the trial app

Detail 14, in `/Users/santi/Hyper/mb-chat-exploration`: the project and its packs, the port, the new UI and the Node end-to-end test.

**Stop D.** I report what the trial showed: how the interface reads in real code, where it got in the way, and the changes it suggests. A change to the interface goes into HIP-20 first, on your go-ahead, and Phase E starts after it.

### Phase E: the rest

- **The key checks** (*Changes* 11, decision 15), test first, with HIP-20's cases under *Key values*, *crypto and mvt*, *rdb_projection* and *rhost clients*: `publicKeyProblem` on each signing suite, `validateKey` in mvt, the writer's `{ pk }` check through it, the checking `registerKey`s, and `lookupKey` on `RhostClient` and its three implementations. The trial's member invite then compares through `lookupKey`, and writes the key only as a `{ pk }`.
- **The ingestion suites through the writers** (*Changes* 5). `IngestionHarness` (`ingestion_suite.ts:63`) replaces `LocalMutator` (`:49`) with the target's writer. The cases about the native paths keep a native mutator, since those paths stay and these cases are their only coverage: the ones HIP-20's *Tests* lists under the existing ingestion suite, and every case that uses `insertAt` or `replaceAt`. P0's map names them.
- **The remaining cases** of HIP-20's *Tests*, from P0's map.
- **The docs** (*Changes* 10), with HIP-20 on the HIPs line of each README they change.
- **HIP-20's reviews.** Each finding of its critiques is checked to be in the HIP or in *Considered and not taken*, and the result goes in the report. Folding the reviews in and removing their files is yours to ask for, as with HIP-29. The header stays `draft` until you commit.
- **The final report.**

## Expected test flips

None by surprise. C1 rewrites the cases it lists, since decision 11 changed what they test, and adds the two required fields to the actions the tests build by hand (decision 13), which changes no outcome. Apart from those, the writer is new, and no existing case should change outcome. In Phase E, a case that moves from `LocalMutator` to the writer and then changes outcome is reported, not adjusted.

## Process

- **No commits or pushes.** No edits to `/Users/santi/Hyper/mb-chat`, `mb-user`, `mb-registry` or `~/.rdb/keys.json`: they are only read.
- **Stop and report on a conceptual problem**, without deciding it alone. Any of these counts:
  - a rule of HIP-20 that can't be implemented as written, or two that can't both hold;
  - an existing case changing outcome;
  - a case HIP-20 doesn't cover that needs a rule;
  - a public API, stored format or schema change HIP-20 doesn't name, including one the trial suggests;
  - a behavior change in a module HIP-20 doesn't list.
- **Fixed in place and listed in the report:** bugs in new code, expected-text changes in tests, citation drift in HIP-20.
- **Reported, not worked around:** blockers outside the design, such as a failing baseline, a stale build, or a step that needs a browser.
- **At each stop,** I report and continue only on your go-ahead.

## Risks

- **Reworking landed capture** (C1). Capture's author fill changes on all three targets. HIP-29's fill and ingestion cases, rewritten for the setting, guard it.
- **The IndexedDB refactor** (detail 8) moves the facade's write steps out of `FacadeObjectStore` without changing what the facade does. The facade's own tests and the ingestion suites on IndexedDB guard it.
- **One transaction per call.** A binding that writes many rows, such as an import, pays a target transaction for each, which is slow on IndexedDB. Batches are HIP-20's open question 1.
- **Writes refused up front.** Code ported from native writes gets a `WriteRefusedError` where it used to get a revert and an op event, for example for a missing required column. The trial shows how that reads in an app.

## Verification

- **Per module.** From `modules/<m>`: `node --import ../../register.mjs ./test/run_tests.ts '<filter>'`. rdb_react, the rhost modules and rdb_files use `TS_NODE_PROJECT=./tsconfig.test.json`.
- **Type errors in tests** fail silently at load. After each step, a temporary tsconfig that includes `test/**` typechecks them, and is then deleted.
- **Rebuild order:** rdb_adapter, rdb_adapter_test, rdb_adapter_sqlite, rdb_adapter_idb, rdb_projection, rhost, rhost_node and rhost_browser, the two clients, rdb_react. The SQLite and IndexedDB suites load rdb_adapter_test's `dist`.
- **At each stop:**
  - the suites of rdb_adapter, rdb_adapter_test, rdb_adapter_sqlite, rdb_adapter_idb, rdb_projection, rdb_react, rhost, rhost_node, rhost_browser, the two clients, rdb_tools, rdb_repl, rdb_files_node and rdb_files_web;
  - `test:parity:fast` in rdb_adapter, and at stop C also `test:parity:full`, and `test:projection:full` in both adapters.
- **The trial app:** `npm run typecheck`, `npm run build` and its Node end-to-end test. The final report says how to try it in a browser (`npm run dev`).

## Found while reading

- **HIP-29 pointed at "HIP-20's Phase C"**, which HIP-20 never defined: the phases were the earlier plan's. It now points at HIP-20's *Changes* 2 to 4.
- **HIP-20 didn't place the trial in its landing order.** The trial's paragraph sat before the list, as if it came first. *Changes* now says that what is left lands in three steps: the draft writer, the trial, then the rest on the fixed interface. The numbered list is by module.
- **HIP-20 spoke of HIP-29's parts as still to land** ("lands with HIP-29", in seven places). They now say they landed, and *Changes* 7 no longer says that rhost's config "today accepts a fixed list of fields". Two sentences of its *Design* said "today" of what HIP-29 and *Changes* 1 changed, ingestion's bundling and a client's meta that never refreshes. They now say it in the past.
- **HIP-20's *Changes* 6 said `RdbProjection.openWriter()` "refuses without a writer"**, against its own *Checks* and HIP-29's decision that a writer without a key takes writes to anonymous groups. It now agrees with them (detail 10).
- **HIP-29's open questions assigned their measurements to HIP-20's trial app.** They now say nothing is measured for now (decision 10).
- **HIP-20's citations had drifted** with HIP-29. Of 89, 56 changed:
  - 47 to current lines, 2 of them in `ingest_chain.ts`, where HIP-29 moved the ingestion pass;
  - 9 newly pinned to `6110da1`;
  - 1 pinned to `4db216b` (decision 8).

  Every cited line now shows what its sentence names.
- **The writer suite's fixture table exists:** `typedTables` (`group_fixture.ts:121`), added in `4db216b`. HIP-20 now says so.
- **rhost's `registerWriterKey` is redundant** since HIP-29: the projection interns its writer key when it records the writer record (detail 11).
- **Memory keeps no nullability**, which the writer's checks need (detail 6).
- **`tableGroup` is on all three targets but in no shared interface** (detail 3).
- **mb-chat's "sending…" marker no longer shows**, since HIP-29 fills in the author at capture (detail 14).
- **A stale code comment:** `types.ts:180-182` still describes dependency-driven bundling (detail 1).
- **Nothing in the target read the writer record's member names.** They were there for `tableNames`, which isn't built yet (decision 11).
- **`autoAuthor` could differ per group only through `RdbProjection`'s `configOverride`**, which no app used, and rhost's config set only the default. On 2026-11-16 you had asked for one switch (decision 11).
- **Which cases keep a native mutator** (Phase E). The rule above names the cases HIP-20 lists and those that use `insertAt` or `replaceAt`. Twenty-five keep one: those, the eight whose writes the writer refuses up front (Stop C), IN24, and the cases named for a native path that would pass through the writer while no longer testing it (IN31, IN40, IN41, IN43, IN90: capture's fill and the native author and update rules). Three cases that test the settle's records, IN44, IN47 and IN48, projected without recording a writer, which the writer needs, so they set the target up as `RdbProjection` does (`projectAs`). No case changed outcome through the writer.
- **A replica's hash suite governs key ids only.** rdb hashes its entries, row ids and catalog payloads with SHA-256 whatever the suite (`rdb/src/rschema/rschema.ts:63`, `rdb/src/rtable_group/group.ts:132`, `rdb/src/rtable/hash.ts:14`), and checks identity rows with the replica's suite (`rdb/src/rtable_group/validate_ops.ts:412`). The writer suite's BLAKE3 case models that: BLAKE3 key ids over SHA-256 entries.

# Critique of HIP-20: Unified write API for projection targets

A review of [HIP-20](0020-unified-write-api.md), checked against the working tree on 2026-11-05. The interface belongs in rdb_adapter, with one implementation per target and a shared suite, and a prebuilt batch is the right shape. The HIP rests on one premise: a writer "makes exactly the target-side writes an app makes today", so nothing downstream changes. Those writes do not mean the same thing on the three targets, and on two of them a `null` stops the projection. The writer has to say what a write means. As drafted, it inherits the answer from each target.

Paths are relative to `modules/`. W1 to W14, O1 and O2 are the scenarios in §1.

## Verdict

**Is it necessary?** Yes. The HIP argues it from syntax: three ways to write. The stronger reason is in §1. The three native paths give one logical write three meanings, so a binding that hid the syntax would still behave differently on each target. The risk is timing. No binding exists yet, so the interface would be fixed before anything has used it (§3).

**Is this the right way to implement it?** The layering is right and the contract is missing. These are the changes, numbered by the section that argues each:

| Item | Where in the HIP |
|---|---|
| 4. `null` is accepted, and rdb cannot store it | `RowValues`; *Tests*, "Values" |
| 5. Defaults and required columns differ by target | *Checks*; `TableShape` in *Changes* 1 |
| 6. An update's columns and its failures differ by target | *Design*, "`update` writes only the columns it is given"; *Changes* 3 |
| 7. A batch is atomic in the target only | *Batches*; open question 1 |
| 8. The author column is wrong for anonymous groups | *Author*; *Why it is sound* 3 |
| 9. The checks use table meta that observers never refresh | *Checks*; *Where apps get a writer* |
| 10. The identity helpers do not belong on the writer | `RowWriter`; open question 4 |
| 11. The test plan cannot see any of this | *Tests* |

Several of the findings are defects in code that exists today, with or without this HIP. §14 lists them.

## 1. How the claims were checked

I read the code the HIP cites and the three write and capture paths:

- `rdb_adapter/src/ingest.ts`, `ingest_orchestrator.ts`, `memory_target.ts`, `row_actions.ts`
- `rdb_adapter_sqlite/src/sqlite_target.ts`
- `rdb_adapter_idb/src/idb_facade.ts`, `idb_target.ts`
- the three `LocalMutator` implementations
- `rdb_projection/src/projection.ts` and the rhost clients

Then I ran a scratch probe against the built modules. It makes each write the way the test mutators do, which is how the HIP says the writer will:

- **memory:** `localInsert`, `localUpdate`, `localDelete`.
- **SQLite:** SQL on the projected table, values through `toParam`, `null` bound as `NULL`.
- **IndexedDB:** `target.database`, with `add` for an insert, and `get` then `put({ ...existing, ...values })` for an update.

After each write it reads the outbox with `drainChanges`, runs `ingestChanges` and `projectGroup`, and reads the row back from the target with `executeQuery` and from rdb.

It uses two schemas. One is the suite's `ledger`: `ref` readonly, `memo` nullable with `maxLength` 8, `amount` decimal. The other is an `items` table: `title string`, `done boolean DEFAULT false`, `meta json NULL`, `conf json NULL READONLY`, `note string NULL`, `qty integer NULL`.

**Where the targets disagree:**

| # | Write | memory | SQLite | IndexedDB |
|---|---|---|---|---|
| W1 | Insert `{ title }`, omitting `done` | reads `done: null`, also after project-back; the op omits `done` | reads `false`; the op carries `done: false` | as memory |
| W2 | Insert with `note: null` | the ingest pass throws, and so does every later pass | accepted; the `null` is dropped at capture | as memory |
| W3 | Update `note` from `'x'` to `null` | as W2 | captured as an update of no columns and dropped: local `null`, rdb `'x'`, no op-event | as W2 |
| W4 | Update `{ title }` on a row holding json objects in `meta` and `conf` | captures `title` | captures `title` | captures `title`, `meta` and `conf`; rejected for the readonly `conf`; the title edit is reverted |
| W5 | Update `{ memo: 'old' }`, the value the row already has | one op | one op | nothing captured |
| W6 | Update `{ memo: 'new', ref: 'R-X' }`, where `ref` is readonly | the whole call is rejected | `memo` lands, `ref` is rejected | the whole call is rejected |
| W7 | Update `{ amount: '5.00', memo: 'waytoolong' }` | the whole call is rejected | `amount` lands, `memo` is rejected | the whole call is rejected |
| W8 | Insert omitting the required `title` | accepted; rejected at ingestion; the row is removed | throws `NOT NULL constraint failed` at the write | as memory |
| W9 | Delete of an id the table does not have | no error; captures a delete that ingestion rejects | no error; nothing captured | no error; nothing captured |

After W2 or W3 on memory or IndexedDB, I made a valid insert and added a row to rdb from outside, then ran a sync cycle (`syncGroup`). The cycle threw again. The valid insert was not ingested, and the outside row did not reach the projection.

**Where the targets agree:**

- **W10.** Two updates to two rows, written in one transaction, the second one invalid. The first lands in rdb. The second is rejected and reverted. The same two updates as one `group.bundle` are rejected together.
- **W11.** An insert with the author column filled in, in a group with no identity provider. The op is anonymous. After project-back the writing device still shows the author it filled in. rdb and a second device show none.
- **W12.** `rowHashForLocalId` right after a local insert is `undefined`. It is defined after ingestion.
- **W13.** A write on a target that was not provisioned for capture (memory and SQLite). The row stays in the projection after a project pass and never reaches rdb.
- **W14.** Values other than `null` round-trip on all three: boolean, json, integer, float, bigint, decimal, bytes.

**Observer instances.** A client process or tab opens a second target on the store the host writes:

- **O1, SQLite.** After the host adds a column, a long-lived second `SqliteTarget` returns rows without it, and a fresh one returns it. After the host drops a column, the long-lived one's query throws `no such column`.
- **O2, IndexedDB.** A second `IdbTarget`, opened before the host creates a table, can query the table. Its `database.transaction([table])` throws `NotFoundError`.

The probe is not in the repo. Each scenario is a few lines against public API, and §11 proposes keeping them as tests.

## 2. Claims that hold up

- **Every code location the HIP cites is where it says.**
- **Reads are portable and writes are not.** The three `LocalMutator`s are three different programs.
- **Values round-trip, for every value except `null`** (W14). *Why it is sound* 2 holds that far.
- **The build callback is forced.** A better-sqlite3 transaction is a synchronous function, and an IndexedDB transaction commits when no request is pending. *Why it is sound* 4 follows.
- **FK-linked inserts written consecutively become one entry** (`groupEntries`, `rdb_adapter/src/ingest.ts:512`), as *Batches* says.
- **`rowHash` is undefined until ingestion** (W12), as the identity test's wording allows.
- **The five alternatives under *Considered and not taken* are rightly not taken.**
- ***Why it is sound* 1 is true as far as it goes.** The writer's writes would be the mutators' writes. The trouble is what those writes do (§4 to §6).

## 3. Is it necessary?

The *Why* lists five consequences. They do not weigh the same:

| Consequence | Assessment |
|---|---|
| A binding cannot write | True, and the only one that needs a cross-target interface. The binding is planned, not written: the memoblocks workspace has no modules yet, and in this repo nothing outside tests and README examples writes through a projection. |
| SQLite writes use a different form than reads | True. A conversion helper in rdb_adapter_sqlite would fix it alone. |
| The prompt local wake on SQLite is opt-in | True, with little effect. The WAL watcher and the drain rule already announce the write (HIP-5). |
| In process, there is no write handle | True, and the most concrete of the five. `RdbProjection` keeps its target private, so an in-process app opens a second connection, or a second `IdbTarget`, only to write. |
| System columns look writable | True. §8 covers where the proposed fix goes wrong. |

*The interface already exists, in the tests.* It does, for the suite's schema. `baseTables` and `fkTables` have string columns and one decimal (`rdb_adapter_test/src/group_fixture.ts:26`, `:52`). The three mutators have never sent a boolean, a json value, a `null`, or a row that leans on a default through capture and ingestion. Their existence shows that a portable mutator is wanted. It does not show that the three paths agree, and §1 shows they do not.

That disagreement is the case for the HIP. A smaller change would cover the two concrete needs: expose the projection's target, and ship a promise-based helper for the IndexedDB facade. It would leave every row of the first table in §1 as it is. The interface is worth having because it is the one place where a write can be given a single meaning, with a suite that holds every target to it.

Because no binding exists, the interface should be tried before it lands. The editor app's schema (the pack in the working tree, not committed yet) has a boolean, nullable strings and a readonly FK, which exercises most of §1. One binding for it, written on the draft interface, would settle open questions 2 to 5 with evidence.

## 4. `null` is accepted, and rdb cannot store it

`RowValues` is `{ [column: string]: json.Literal | null }`. The type check passes a `null` for any column, because `null` is what a read returns for an absent value. The test plan lists `null` among the values that round-trip.

rdb has no `null`. `json.Literal` excludes it (`json/src/literal.ts:2`), an update carries `json.Literal` values (`rdb/src/rtable/payload.ts:51`), and validation rejects anything else (`rdb/src/rschema/validate.ts:99`). A nullable column is one that a row may leave unwritten. No op clears a column once it is written.

So an insert can treat `null` as "not given", and an update to `null` has no rdb op to become. The editor schema has `bio string NULL`. Clearing it is the first thing a profile form does.

What happens today (W2, W3):

- **SQLite.** `toLogical` turns `NULL` into `undefined` (`rdb_adapter_sqlite/src/sqlite_target.ts:216`) and `drainChanges` skips it (`:1157`). For an insert that is right. For an update, the planner receives a change with no columns and drops it as "only system cols touched" (`rdb_adapter/src/ingest.ts:430`). The local row keeps `NULL`, rdb keeps the old value, and nothing is logged or reverted.
- **Memory and IndexedDB.** The `null` reaches the planner and the op, and rdb rejects the op. Recording the rejection hashes the op (`syntheticOpHash`, `rdb_adapter/src/ingest_orchestrator.ts:165`, called at `:392`), and the hash throws on the `null`. The throw leaves the pass before `commitIngest` (`:591`), so the outbox keeps the change and the next pass throws on it again. `syncDatabase` ingests before it projects (`:616`, `:631`), so the projection stops receiving remote changes as well.

The second case with the suite's fixture:

```ts
const { group, admin } = await createGroup();
const target = new MemoryTarget({ captureChanges: true });
await projectGroup(group, target);
target.localInsert('ledger', { ref: 'R-9', amount: '1.00', memo: null as unknown as string });
await ingestChanges(group, target, { writer: admin });   // TypeError, on this pass and on every later one
```

The cast is the point. The mutators' `RowValues` has no `null` (`rdb_adapter_test/src/projection_reader.ts:12`), which is why the suite never met this. HIP-20's type lets it in.

**Change.** State the rule, and enforce it in the writer before anything is written:

- `insert`: a column given as `null` is treated as not given.
- `update`: a `null` is refused, with an error that says rdb cannot clear a column.

Clearing a column needs an rdb op, a last-writer-wins rule for it, and a projection of "unset" as `NULL` (`rdb_adapter/src/row_actions.ts:63` skips an unset value today). That is its own HIP. Until it exists, the writer should not offer it.

## 5. Defaults and required columns differ by target

**Defaults (W1).** On SQLite the DDL default fills the row, and the insert trigger captures the whole `NEW` row (`sqlite_target.ts:519`). The row reads `false` at once and the op carries `done: false`.

On memory and IndexedDB the row is stored as given (`rdb_adapter/src/memory_target.ts:262`, `rdb_adapter_idb/src/idb_facade.ts:489`). Defaults are filled only when `apply` creates a row (`memory_target.ts:827`, `rdb_adapter_idb/src/idb_target.ts:496`). The project-back of a local insert updates a row that already exists, with a delta that carries only written values. So the row reads `null` before and after project-back, while rdb and every other device read `false`.

The ops differ too. One row has `done` written. The other has no value, and falls back to the schema's default whenever it is read (`rdb/src/rtable/view.ts:473`). The same binding call leaves different state in the replica on Node and in a browser.

**Required columns (W8).** SQLite declares `NOT NULL` (`sqlite_target.ts:133`) and throws at the write. Memory and IndexedDB accept the row, show it with a `null`, and remove it when ingestion rejects it.

`TableShape` has column types, FK columns, key-ref columns, the id column and the author column. It has neither defaults nor nullability, so `checkWrite` cannot make the targets agree.

**Change.**

- An insert writes the default for every column it was not given, in the row and in the captured change. SQLite's trigger cannot tell a given value from a defaulted one, so this is the only rule all three targets can share. Memory and IndexedDB already keep `defaults` in their table meta.
- An insert that leaves out a column with no default that is not nullable throws at the write. `TableShape` needs `nullable` for that. SQLite can read it from `PRAGMA table_info`. Memory and IndexedDB need it added to their table meta, and an existing IndexedDB projection needs it filled in when the projection opens.

## 6. An update's columns and its failures differ by target

The HIP says "`update` writes only the columns it is given", and that on IndexedDB this "is a read-merge-write inside the same transaction, as `idbLocal` does now". Two things then depend on the target.

**Which columns become writes (W4, W5).**

- SQLite captures every column named in the statement. `AFTER UPDATE OF <col>` fires whether or not the value changes (`sqlite_target.ts:531`). The comment above it gives the aim as capturing "ONLY the columns it actually changed" (`:497`).
- Memory captures what `localUpdate` is given.
- IndexedDB captures the columns that the facade's diff reports, and the diff compares with `!==` (`idb_facade.ts:254`). The old record and the new one are separate structured clones, so a json object or array never equals itself. Every such column is captured on every update.

On IndexedDB, an update of `title` therefore writes `meta` again. It is a real write, and it can win last-writer-wins over a concurrent edit of `meta`. I checked the entry on a table with no readonly column: its values are `{ title, meta }`. When one of the json columns is readonly, the whole update is rejected and the title edit is reverted (W4).

In the other direction, an update that writes the value a row already has produces an op on SQLite and memory, and nothing on IndexedDB (W5).

**What fails together (W6, W7).** SQLite captures one outbox row per column. The planner merges them and keeps each one as a fallback. A readonly column is rejected alone, and when rdb rejects the merged op, the columns are resubmitted one by one.

Memory and IndexedDB capture one change with all the columns. It is one part. The planner rejects it at the first readonly column (`ingest.ts:336`), and a one-part op has no fallback.

`[IN12]` pins the first behavior: "a multi-field update lands its valid field". It writes the two fields as two `update` calls, which is the only form that behaves the same on all three targets. A single `writer.update(table, id, { memo, ref })` is the other form.

**Change.** Pick one rule for each, and hold every target to it.

- **Columns.** I would capture only the columns whose value differs from the row's, compared by value. A form that saves every field must not overwrite concurrent edits to the fields the user left alone, and this is what the SQLite comment intends. It needs three edits:
  - a by-value comparison in the facade (`json.equals`);
  - the same comparison in `localUpdate`;
  - `WHEN NEW.<col> IS NOT OLD.<col>` on the SQLite update triggers. I checked that this fires for a change to `NULL` and stays quiet for an equal value.
- **Failures.** There are two options, and the choice should follow from §7.
  - Keep `[IN12]`'s rule, and have the planner split a multi-column change into one part per column, so memory and IndexedDB match SQLite.
  - Make one call succeed or fail as a whole. On SQLite this needs the outbox to mark the rows of one call, which is the mark §7 needs.

## 7. A batch is atomic in the target only

The HIP says so: "A batch is atomic in the target, not in the replica", and a rejected op "reverts its own row, and the rest of the batch stands". W10 is that sentence observed. Two updates go into one transaction. The first lands in rdb, and the second is rejected and reverted, on every target.

A batch can come apart in the replica in two ways:

1. **At ingestion.** One op is rejected and the others land. Peers receive the others.
2. **Later.** Each op is its own entry, so a concurrent barrier can void one and leave the rest.

rdb already has the unit that prevents both. `RTableGroup.bundle` writes several ops as one entry that "validates and applies as a unit", and a bundle is voided as a unit too (`rdb/src/rtable_group/group.ts:1061`). The same two updates as one bundle are rejected together (W10). Ingestion puts more than one op in an entry only for FK-linked inserts.

This weighs more than its place as open question 1 suggests:

- **The HIP's own motive is a binding that holds "the writes that satisfy its rules".** A rule over two rows needs both writes or neither.
- **The name will be read as a transaction.** The method is `batch`, and its second bullet is "All or nothing".
- **Deferring it changes the contract later.** A batch that used to apply in part would start to fail as a whole, under bindings written against the first behavior.
- **The writer is the one place where the app states the unit.** Raw SQL gives ingestion no boundary to recover, which is why capture never kept one (`rdb_adapter/src/types.ts:146`).

**What it takes.**

1. **Outbox rows carry a batch id.** On memory and IndexedDB the writer sets a field. On SQLite the triggers fill a column from a one-row value that the writer sets at the start of its transaction and clears at the end, the way `applying` works.
2. **The planner puts the consecutive ops of one batch and one group in one entry**, with no per-op fallback. A batch that spans groups becomes one entry per group, because a bundle belongs to one group. The contract has to say that.
3. **When rdb rejects the entry, the orchestrator records a failure for each op and reverts each row.**
4. **Replay after a crash.** An entry with an insert is recognized as today: its own insert already exists, so the entry was applied. An entry without one has no such marker. A delete that is no longer live can be removed and the rest resubmitted. A repeated update is a duplicate write of the same values, as it is now.

**Change.** Do this in HIP-20 for the ops of one group. If it is deferred, do two things now: carry the batch id in the outbox from the first version, and word the contract so that a batch may be rejected as a whole.

## 8. The author column is wrong for anonymous groups

The HIP fills the author column on insert, and argues in *Why it is sound* 3 that project-back then writes the same value. That holds when the op is signed.

A group with no identity provider admits only anonymous ops (`ingest_orchestrator.ts:339`). The row has no author. `tableRowActions` sets none on the action (`row_actions.ts:87`), and each target's upsert leaves the column alone when the action has none (`memory_target.ts:800`, `sqlite_target.ts:1007`, `idb_target.ts:475`). The projection still has an author column for those tables. In W11 the writing device shows the author it filled in, and nothing ever corrects it. rdb and every other device show none.

The parameter has a second problem. `openWriter({ author })` takes the id from the caller, and only one id is right: the projection's writer key. A client passes `(await client.me()).id`, and the writer cannot tell if it was passed something else.

**Change.**

- **Make project-back the authority for the column.** An upsert whose action has no author writes `NULL`. Every `RowChange` carries the row's author (`rdb/src/rtable/delta.ts:52`), so a missing one means the row is anonymous. This also repairs a raw write that set the column. Not projecting an author column at all for a group with no identity provider would be cleaner still.
- **Drop the parameter.** When a projection opens with a writer, it records that key's id in the target, and `openWriter()` reads it. A client process gets the same value while the host is stopped.

## 9. The checks use table meta that observers never refresh

`checkWrite` runs on "the meta it already keeps", and a client process or tab gets its writer from a target it opened itself. That target learns the schema once.

- **SQLite (O1).** `metaCache` (`sqlite_target.ts:225`) is filled on first use and changed only by this instance's own schema actions. `notifyExternalCommit` (`:1299`) hands the event to the listeners and does nothing else. After the host adds a column, the client's rows lack it. After the host drops one, the client's query throws.
- **IndexedDB (O2).** `tableNameSet` (`idb_target.ts:90`) is filled at open (`:144`) and by this instance's own `apply` (`:224`), and the facade refuses a table it does not list (`idb_facade.ts:964`). A client that opened before the host created a table can query it and cannot write to it.

This is a gap from HIP-5, and reads already show it. The writer would add two failures to it: a valid write to a new column or table refused as unknown, and a value converted with the type of a column that was dropped and added again under the same name.

**Change.**

- A target drops its cached meta for the tables an `apply` commit lists in `schemaTables`, and drops all of it on `reconnect`. The host already sends both.
- On SQLite the cache can also be checked without the stream: `PRAGMA schema_version` changes whenever the schema does.
- Open question 5, `openProjection(dir)`, is the natural place to wire the commit stream into the target. Forwarding it is the caller's job today.

## 10. The identity helpers do not belong on the writer

- **`registerKey(publicKey)` is already `RhostClient.registerKey`** (`rhost_client/src/types.ts:119`), with `RdbProjection.registerKey` under it. It needs the replica's hash suite to compute the key hash. The in-process client takes the suite from the replica (`rhost/src/client.ts:88`). The two client packages hard-code SHA-256 (`rhost_client_node/src/client.ts:31`, `rhost_client_browser/src/client.ts:28`). A target has neither, so a writer opened from a target would be a third copy with the same constant.
- **`rowHash` and `localId` are reads.** Code that only renders needs `localId` to resolve a link it was sent, and it has no writer.
- **`rowHash` is `undefined` until the row is ingested** (W12). That is after `insert` resolves, and it does not happen at all while the host is stopped. The HIP's reason for the helper is a row reference in an invite or a link. That flow is `insert`, wait for ingestion, then `rowHash`, so it depends on open question 4.

**Change.** Leave `registerKey` on the client, and pass the id or the client to the binding. Put `rowHash` and `localId` beside the read side. Decide open question 4 together with them.

## 11. The test plan cannot see any of this

The "Values" test reads back what it wrote, from the target. W1 to W8 all pass it, because each of them reads back locally as written. The differences appear in the captured change, in the ingest result, in the row after project-back, and in rdb.

*Why it is sound* 1 leans on the ingestion suite: the writer makes the mutators' writes, "and ingestion is already tested against those". The suite's tables have string columns and one decimal. That covers no boolean, no json, no default and no `null`.

**Change.**

- **Every writer case asserts four things:** the captured change, the ingest result, the row after project-back, and rdb's row.
- **The fixture gains a table** with one column of each type, a default, a required column, a readonly column and a readonly json column.
- **W1 to W14, O1 and O2 become cases.** They can go in first as tests of today's behavior, as `[OCUT]` did for HIP-7, and be flipped by the change that fixes each.
- **A few cases keep writing through raw SQL and the raw facade** when `IngestionHarness` moves to the writer. Those paths stay supported, and the suite is their only coverage.

## 12. Smaller points

- **Unknown ids.** "An `update` or `delete` of an id the table doesn't have" is listed with the meta checks. It is a check on row state, and it races with `apply`: a remote delete can land between the render and the click. No native path throws on such a delete (W9), and `delete` should be a no-op there. Memory's `localDelete` should also stop recording a change for a row it does not have (`memory_target.ts:280`).
- **A writer that cannot replicate.** On a target without capture, or a projection without a writer key, a write stays in the projection and never reaches rdb (W13). `openWriter` and `RdbProjection.writer` should refuse.
- **`ProjectionProvider` takes `source` and `writer` separately.** Nothing ties them to one target. Writes through a writer on another target would never show in the queries.
- **The summary's sentence stops being true.** "A writer makes exactly the target-side writes an app makes today" no longer holds once §4 to §6 are in. The writer normalizes, then writes.
- **Projected names.** The example binding builds `${group}_${table}` by hand. The projection sanitizes the group name and lets the host rename it (`rdb_projection/src/scope.ts:45`, `:72`), so the rule is repeated, and can be wrong, in every binding. This applies to reads as much as to writes.

## 13. Recommendations

1. **Fix the paths that exist first** (§14). The writer is built on them, and apps that write natively hit them now.
2. **Add a *Write semantics* section to the HIP** with the rules of §4 to §6, and have `checkWrite` and each target's writer enforce them:

   | Case | Rule on every target |
   |---|---|
   | Insert, column given as `null` | treated as not given |
   | Insert, column not given, has a default | the default is written, in the row and in the op |
   | Insert, column not given, required | throws at the write |
   | Update to `null` | refused |
   | Update, column whose value does not differ | not written |
   | Update, one column fails | one rule (§6), following from item 3 |
   | Delete of an unknown id | no-op |

3. **Decide replica atomicity for a batch in this HIP** (§7).
4. **Make project-back the authority for the author column, and drop `author` from `openWriter`** (§8).
5. **Refresh table meta in observer targets** (§9).
6. **Move `registerKey`, `rowHash` and `localId` off `RowWriter`**, and settle open question 4 with them (§10).
7. **Rewrite the test plan to assert through ingestion and project-back** (§11).
8. **Write one binding on the draft interface before it lands** (§3).

## 14. Outside this HIP

These are defects on paths that exist today, found while checking. None depends on HIP-20.

1. **An exception while recording a rejection aborts the ingest pass, and the pass then fails on every retry** (§4). It is reachable now through the IndexedDB facade, with `store.put({ ...row, bio: null })`. A failure tied to one change should become that change's rejection, with its row reverted, and the pass should always reach `commitIngest`.
2. **SQLite drops an update to `NULL` without a trace** (§4). The capture should hand it to the planner, which rejects it and reverts the row.
3. **The IndexedDB facade's diff compares by identity** (§6).
4. **A local insert on memory or IndexedDB never shows a default** (§5).
5. **Observer targets keep stale table meta** (§9).
6. **A `drop-column` migration cannot be projected into a capture-provisioned `SqliteTarget`.**
   - **Cause.** `dropColumn` runs `ALTER TABLE … DROP COLUMN` while the capture triggers still name the column (`sqlite_target.ts:791`), and SQLite refuses. The triggers are reinstalled only afterwards (`:810`).
   - **Check.** I ran `schema.updateSchema([{ rule: 'drop-column', table: 'ledger', column: 'memo' }])`, `group.deploy` and `projectGroup`. A target without capture is fine. One with capture throws `error in trigger rdb_cap_ins_ledger after drop column: no such column: NEW.memo`.
   - **Coverage.** `[ADPTS-SQL02]`, `[ADPTS-SQL07]`, `[ADPTS-IDX03]` and `[ADPTS-IDX04]` all use a target without capture. rhost_node always opens one with it (`rhost_node/src/platform.ts:265`), so a release that drops a plain column would stall the projection of every Node host.
   - **Fix.** Drop the table's capture triggers before the `ALTER`.

# Second critique of HIP-20: Unified write API for projection targets

A review of [HIP-20](0020-unified-write-api.md) as revised on 2026-11-16, and of [the response](0020-unified-write-api.response.md) to [the first critique](0020-unified-write-api.critique.md). Checked against the working tree on 2026-11-17. The revision takes the first critique in full, and the response is right on the four points where it differs. What is left is in the parts the revision added: the `autoAuthor` setting, and the way two of the write rules meet.

Paths are relative to `modules/`. W15 to W21, V1, A1, M1, B1, K1, K2, S1 and O3 are the scenarios in §1. W1 to W14, O1 and O2 are the first critique's.

## Verdict

**Does the revision resolve the first critique?** Yes. Each finding is in the HIP, or has a line under *Considered and not taken*. All six defects of today's code are in *Changes* 1.

**Is the response right where it differs?** Yes, on all four points: no batches, the author column kept in anonymous groups, open question 4 left open, and `false` for a missing row. §2 gives the check behind each. One of its three reasons for keeping the author column does not count, and the other two are enough.

**Is anything still wrong?** Yes. These are the items, numbered by the section that argues each:

| Item | Where in the HIP |
|---|---|
| 3. `autoAuthor` off reads a column that capture does not record | *Authorship*; *What stays*; *Changes* 2 to 4 |
| 4. The author rules differ silently between the two settings | *Authorship*; *Write semantics*, first row |
| 5. `null` for a column that has no value throws | *Write semantics*; *Changes* 1, "Changes are compared by value" |
| 6. A voided delete is reported as `void`, not `reinstate` | *Write semantics*, "Missing rows" |
| 7. The meta refresh depends on a stream that some targets do not receive | *Checks*; *Why it is sound* 6 |
| 8. The test plan misses the cases above | *Tests* |

Item 3 blocks `autoAuthor` off as written. Items 4 and 5 are rules I would change. Items 6 and 7 are corrections. §11 lists two more defects of today's native paths that the fixes in *Changes* 1 do not reach.

## 1. How the claims were checked

I read the revised HIP, the response, and the code they cite, and then the paths the new parts depend on:

- `rdb_adapter/src/ingest.ts`, `ingest_orchestrator.ts`, `project.ts`, `names.ts`
- the capture code of each target: the SQLite triggers, the IndexedDB facade, and `MemoryTarget`'s local methods
- `rdb/src/rtable_group/validate_ops.ts` and `validate.ts`, for authorship and bundles
- `crypto/src/identity.ts`, for the key format
- the commit stream of the SQLite and IndexedDB targets

Then I ran three scratch probes against the working tree. They write the way the test mutators do: SQL on SQLite, `target.database` on IndexedDB, and the local methods on memory. After a write they read the outbox with `drainChanges`, run `ingestChanges` and `projectGroup`, and read the row from the target and from rdb. Most scenarios use the suite's group, which has an identity provider and the tables `ledger` (`ref`, `memo` nullable, `amount`) and `tags` (`code`).

**The author column:**

| # | Scenario | Result |
|---|---|---|
| W15 | An insert that sets `author_key_id` to another key's id, on each of the three targets | The row holds the id. The captured change has `ref` and `amount` only, on all three targets. |
| W16 | A native update of `author_key_id` on an ingested row: `UPDATE` on SQLite, `put` on IndexedDB | Nothing is captured. The row keeps the written author through a sync cycle, with no rejection and no op event. On SQLite, a later update of the row in rdb puts the real author back. |
| W20 | SQLite: an insert that sets the author column, then a delete of that row, both before ingestion | Two changes are captured and the row is gone from the target. Ingestion submits both ops. |
| A1 | A table with no rules, in a group with an identity provider. An unsigned insert, then an update and a delete of the row, each tried signed by the admin and unsigned | The insert is accepted. The four later ops are all rejected, by `ALLOW … IF rowAuthor = $author`. |

**`null`, missing rows and reused ids:**

| # | Scenario | Result |
|---|---|---|
| W19 | An update to `null` of `memo`, on a row that never set `memo` | SQLite captures an update of no columns, which is dropped. Memory and IndexedDB capture `{ memo: null }`, and the ingest pass throws. |
| V1 | A table whose delete rule needs a row of a second table. A row is deleted, and the delete is projected. Then a concurrent delete of the row the rule needs arrives, and is projected. On SQLite and memory | The row leaves the target, then returns under the same local id. One op event is logged: origin `concurrency`, direction `void`, kind `delete`. |
| M1 | In rdb, an update and a delete of a row that was deleted | Both are rejected: the row "is not live". |
| W17 | SQLite: `INSERT OR REPLACE` with the id of a live row. (a) The row came from rdb. (b) The row was inserted on this device and ingested | Captured as an insert. (a) is dropped with a warning. (b) is dropped with none. In both, the target shows the new values and a `NULL` author, rdb keeps the old values, and no op event is logged. |
| W18 | IndexedDB: a `put` with the id of a deleted row | Captured as an insert and dropped with a warning. The row stays in the target, rdb has no such row, and no op event is logged. |
| W21 | An insert whose key column holds an id the key table does not have, on SQLite and memory | Stored on both. Ingestion rejects it ("unknown key id"), removes the row and logs a failure op event. |
| S1 | SQLite: the changed-row count of statements on a table with capture triggers | 1 for an insert, for an update of two columns, for an update to the value the row has, and for a delete. 0 for an update or a delete of a missing id. |

**Keys and bundles:**

| # | Scenario | Result |
|---|---|---|
| K1 | A key hash handed to the public key deserializer | No error. The result has a suite name of garbage and a key of 0 bytes. |
| K2 | A group whose hash suite is BLAKE3. Two identity-provider rows for one public key: one with the key hash computed with SHA-256, one with BLAKE3 | The first is rejected: "provider keyId does not match public key". The second is accepted. |
| B1 | A bundle that inserts a row and then updates it | Rejected: the bundle "writes rowId … more than once". |

For K2, I replaced the mock context's `getHashSuite`. Its DAG still hashes entries with SHA-256, which the check under test does not use.

**Observer targets:**

| # | Scenario | Result |
|---|---|---|
| O3 | Three IndexedDB targets on one database. The second and third are opened before the first projects a group. The third has a commit listener | The third receives an `apply` commit that lists the new tables. On both the second and the third, `database.transaction(['tags'])` still throws `NotFoundError`, as in O2. |

I also re-ran the probe the response cites for a reused id on SQLite. It gives what the response says: the insert is dropped with a warning, the row stays in the target, rdb has no such row, and no op event is logged.

The probes are not in the repo. Each scenario is a few lines against public API, and §8 lists the ones worth keeping as tests.

## 2. Claims that hold up

**The revision.** I checked each row of the response's table against the HIP's text. Every row is where the table says.

**The response, where it differs:**

- **No batches.** With no `batch`, there is no contract that applies in part and would later start to fail as a whole. The constraints it found are in the code: a bundle writes each row once (B1), `ALLOW` rules read the state before the bundle (`rdb/src/rtable_group/validate_ops.ts:554-555`), no op in a bundle touches a foreign group (`:507-508`), and a bundle has one signer (`rdb/src/rtable_group/validate.ts:158-172`). Its choice for failures by column follows.
- **The author column stays in anonymous groups.** The conclusion holds. Of its three reasons, "dropping it costs every existing projection a column" does not count while the project is unreleased. A projection is derived data and can be rebuilt. The other two are enough: project-back keeps the column `NULL`, and a column that exists in some groups only is one more thing for a binding to check.
- **Open question 4 stays open.** The option it records is real: ingestion reuses a reserved identity for a local row on replay (`rdb_adapter/src/ingest.ts:219-236`).
- **`false` for a missing row.** rdb accepts neither op on a deleted row (M1). When the delete is voided, the row returns under the same local id (V1). §6 corrects the name of the event.

**New claims of the revision:**

- **Every code location is where the HIP and the response say, except two** (§9).
- **The public key deserializer checks nothing** (K1), so `{ pk }` and the writer's own checks are needed.
- **A SHA-256 key hash fails on a BLAKE3 replica** (K2), so the hash suite has to reach the writer and the clients.
- **An unsigned op is accepted in a group with an identity provider, and under the default rules the row is then permanent** (A1; `validate_ops.ts:364-366`).
- **A reused deleted id is dropped without a trace today**, on SQLite (the response's probe) and on IndexedDB (W18).
- **The writer's own inserts cannot reuse an id.** SQLite's id column is `AUTOINCREMENT` (`rdb_adapter_sqlite/src/sqlite_target.ts:691`), and memory and IndexedDB allocate from a counter.
- **SQLite's count of changed rows leaves out the capture triggers' writes** (S1), so `update` and `delete` can resolve from it. An update to the value a row already has counts 1, which matches "`true` also when an update changed no column".
- **A dangling reference or an unknown key id means the same on every target.** The foreign keys SQLite declares are not enforced (`sqlite_target.ts:410`), so such a write is stored on all three targets and rejected at ingestion (W21).
- **"That column is reverted, and the others land" holds.** A revert writes the whole row again from rdb, after the other columns were submitted (`rdb_adapter/src/ingest_orchestrator.ts:581-586`).
- **Writing a default into the op changes nothing later.** No schema rule changes a column's default (`rdb/src/rschema/payload.ts:365-371`), so a row with the default written and a row that falls back to it read the same from then on.

## 3. `autoAuthor` off reads a column that capture does not record

The HIP says that with `autoAuthor` off "an insert's author column decides its signer", for the writer and for native writes. It also says capture is unchanged: *What stays* lists "capture, the outbox, settle, op events and the commit stream", and *Changes* gives the new behavior to ingestion only.

No target puts the author column in a captured insert (W15):

- **SQLite.** The insert trigger records the columns of `meta.columnTypes`, which are the business columns (`sqlite_target.ts:508`, `:519`).
- **IndexedDB.** `businessValues` copies the same set (`rdb_adapter_idb/src/idb_facade.ts:237-243`, `:499`).
- **Memory.** `localInsert` takes the author as a separate argument and records only `values` (`rdb_adapter/src/memory_target.ts:262-270`).

So ingestion has no author to read. The skip of system columns at `rdb_adapter/src/ingest.ts:333` acts on a column that no target sends.

Reading the row at ingestion instead does not work:

- **The row can be gone.** An insert followed by a delete is submitted as two ops (`ingest.ts:20-22`), and by then the target has no row (W20).
- **The row can have changed.** A native update of the author column is captured on no target (W16). The insert would be signed as the column says at ingestion, not as it said when the insert was made.

**Change.** Record the author column in the captured insert, on all three targets and under both settings. Ingestion ignores it when `autoAuthor` is on.

- **SQLite.** The insert trigger's image gains the column. Existing targets need their triggers installed again. Today that happens only on a schema action for the table (`sqlite_target.ts:749`, `:783`, `:810`, `:917`). The same holds for the `WHEN NEW.<col> IS NOT OLD.<col>` guard of *Changes* 1, so the HIP should say that a projection reinstalls its triggers at open, or that existing projections are rebuilt.
- **IndexedDB and memory.** `businessValues` and `localInsert`'s record gain the column.
- **Ingestion.** It takes the signer from the captured value. A crash replay then reads the same author from the outbox, signs the same way and derives the same row hash.
- **The HIP's text.** *What stays* no longer lists capture as unchanged, and *Changes* 3 and 4 name the capture change.

## 4. The author rules differ silently between the two settings

A binding is a package for one schema. `autoAuthor` is an option of the projection, which in rhost comes from the host's configuration. The binding gets a `QuerySource` and a `RowWriter`, and neither tells it the setting.

In a group with an identity provider, these are the HIP's rules for an insert, and the ones I propose:

| The insert gives, for the author column | `autoAuthor` on, in the HIP | `autoAuthor` off, in the HIP | Proposed |
|---|---|---|---|
| nothing | signed with the writer key | anonymous | on: signed. off: refused |
| the writer key's id | refused | signed | signed, under both |
| `null` | signed with the writer key | anonymous | on: refused. off: anonymous |
| another key's id | refused | refused | refused |

Two of the HIP's cells give the binding the opposite of what it asked for, with no error:

- **`null` under `on`.** *Write semantics* treats an insert's `null` as "not given", so the writer fills in the writer key. A binding that asks for an anonymous row gets one signed with the host key. `on` is the default. The signed entry stays in the DAG.
- **Nothing under `off`.** A binding written for the default leaves the column out. The row is anonymous, and under the default rules nobody can update or delete it (A1).

A third cell is a refusal of the value the writer itself writes: the writer key's id under `on`. With it refused, no value a binding can give means "signed" under both settings.

The `null` cell also breaks the HIP's own property. A query returns `null` in the author column of an anonymous row. Under `on`, writing that value back gives a row that reads back with the writer key's id. *Design* names one exception to "a value written is the value read back", the `{ pk }` key value. This would be a second one.

With the proposed column, a binding that states the author gets what it stated or a `WriteRefusedError` with reason `author`. Only "nothing" depends on the setting, and then the outcomes are a signed row or a refusal.

Three notes on the proposal:

- **Anonymous groups are unchanged.** There the column is `NULL` under both settings, and `null` or nothing is accepted.
- **Native writes keep the HIP's rule.** Raw SQL cannot tell a column left out from one set to `NULL`, so with `autoAuthor` off a native insert without an author is anonymous. The setting is still needed for that. The writer is stricter than the native path, because the writer can tell the two apart.
- **The first row of *Write semantics* gets an exception.** For the author column, `null` is a value, not "not given".

**Change.**

- Replace the three bullets of *Authorship* that say what the writer throws on with the proposed column.
- Give the binding the writer key's id. The writer already reads it inside each write. A method on `RowWriter` that returns it would let a binding state "signed" without a client or a projection in hand.

## 5. `null` for a column that has no value throws

Two rows of *Write semantics* apply to one write:

- "Update to `null`: throws."
- "Update, a column whose value does not change: not written."

When a row never set a nullable column, an update of that column to `null` changes nothing. The HIP does not say which rule comes first, and its *Checks* list `null-update` without a condition. Read that way, the write throws.

The HIP's reason for the second rule is "a form that saves every field". Such a form sends `null` for each nullable field the user left empty, and it would throw on every save until every field has a value.

The native paths go the other way, and after *Changes* 1 they disagree with each other:

- **SQLite.** Today the update is captured with no columns and dropped (W19). With the `WHEN NEW.<col> IS NOT OLD.<col>` guard nothing is captured, because `NULL IS NOT NULL` is false. Both treat the write as no change.
- **Memory and IndexedDB.** Today `{ memo: null }` is captured and the ingest pass throws (W19). *Changes* 1 compares with `json.equals`, which finds `null` and a missing value different, because their types differ (`json/src/literal.ts:8-17`). The change is still captured, then rejected and reverted, with a failure op event for a write that changed nothing.

**Change.**

- **The writer.** Apply the unchanged rule first. A `null` for a column the row has no value for is not written. `null-update` is thrown only when the row has a value. The writer reads the row for this, as it does to detect a missing row. On SQLite that is a read the count of changed rows does not give, needed only when an update carries a `null`.
- **The native comparison.** In the facade's diff and in `localUpdate`, a `null` and a missing value are equal.

A related case, for the trial binding to decide. The HIP says that tables, columns and values are the ones a query returns. A row from a query has `id` and `author_key_id`, so `update(table, row.id, { ...row, name })` is refused twice, with `id-column` and `author`, although neither value changes. Either the unchanged rule covers these two columns as well, or the README says that a row is not to be spread into an update.

## 6. A voided delete is reported as `void`, not `reinstate`

*Write semantics* says that when a delete is voided later, "the row comes back under the same local id, and the app learns of it through a `reinstate` op event (`rdb_adapter/src/project.ts:72-82`)".

The first half holds (V1). The event is the other one. The op whose verdict flips is the delete, and it flips to void, which the code reports as `void` (`project.ts:81`). `reinstate` is for an op that was void and is live again (`:82`, and `[ADPTV02]`). In V1 the one event has direction `void` and kind `delete`.

An app that waits for `reinstate` to learn that a row is back does not hear of it.

**Change.** Say "a `void` op event whose kind is `delete`". The app also sees the row return in its queries, without the event.

## 7. The meta refresh depends on a stream that some targets do not receive

*Why it is sound* 6 says "a client cannot write with stale meta", because "cached meta is dropped by the commits that change it". That holds for a target that receives those commits, after they arrive.

- **IndexedDB.** A target opens the channel that carries commits from other instances only while it has a commit listener (`rdb_adapter_idb/src/idb_target.ts:697-701`). A target with one receives the `apply` commit with its tables (O3). A target that only writes has none, and receives nothing.
- **SQLite.** A target hands an external commit to its listeners and does nothing else (`sqlite_target.ts:1299`). In a client process the commits come from the host's socket, and wiring them into the target is the caller's job (open question 5). The schema version check the HIP adds does not need the stream.
- **Both.** A write made after the host changed the schema, and before the commit arrives, uses the old meta.

Most of what the writer needs can be read where the write happens:

- **IndexedDB.** The facade already reads the table's meta record inside each write transaction (`rdb_adapter_idb/src/idb_facade.ts:474`). The only cached part is `tableNameSet`, the list that refuses an unknown table (O2, O3).
- **SQLite.** In the HIP's design, the writer reads `rdb_table_meta` inside its transaction, for the authorship mode.

**Change.**

- **IndexedDB.** When a table is not in `tableNameSet`, read the meta store again before refusing it.
- **SQLite.** Compare the schema version inside the write's transaction, where the writer record is read.
- **The HIP's text.** Keep the commit-driven drop for reads, and have *Why it is sound* 6 rest on the reads inside the transaction.

## 8. The test plan

*Tests* covers what the revision set out to cover. These cases are missing, and each pins one of the points above:

- **`autoAuthor` off.**
  - An insert followed by a delete before ingestion is signed as its author column said (W20's shape).
  - A crash replay of an insert keeps its signer and its row hash.
- **The author table of §4.** One case per cell, in a group with an identity provider and in an anonymous one.
- **`null` for a column with no value.** An update resolves `true`, writes nothing and captures nothing, on all three targets. The same update made natively captures nothing (W19).
- **A voided delete.** The row returns under the same local id, and the op event is `void` with kind `delete` (V1).
- **An observer target with no commit listener** writes to a table the host created after it opened (O3).
- **Key values.** The HIP states two rules that have no case: `{ pk }` in a json column is stored as json, and a `{ pk }` whose base64 is not canonical throws `invalid-key`.
- **The native cases of §11.** A native update of the author column, and `INSERT OR REPLACE` over a live row (W16, W17).

## 9. Smaller points

- **A native update to `null` takes two paths.** *Changes* 1 has SQLite hand it to the planner, which rejects it. On memory and IndexedDB the `null` still goes to rdb, and the rejection is then recorded through the path that used to throw. The planner should reject it for all three targets. A captured update needs a way to carry it: its `values` are `json.Literal` (`rdb_adapter/src/types.ts:140-144`), which has no `null`.
- **A key id that the key table does not have.** It is a fact of the target, and ingestion is certain to reject it (W21). The writer could refuse it with `invalid-key`.
- **`id-column`.** The id column's name is configurable (`rdb_adapter/src/names.ts:17-19`). *Checks* should say "the table's id column", not `id`.
- **An update that changes nothing.** The HIP says it resolves `true`. It should also say whether it announces a commit event. SQLite's `noteLocalCommit` announces only when the outbox grew (`sqlite_target.ts:1357-1365`), so "no event" is the rule all targets can share.
- **Refused writes and interned keys.** A refused write "writes nothing", and a write to a missing row "writes nothing". A `{ pk }` in such a write should not be interned, so the checks and the row read come before the interning.
- ***Changes*.**
  - `RdbProjection` is used as a `QuerySource`, so it needs `rowHash` and `localId` too. *Changes* 6 lists only `openWriter`.
  - `ProjectionProvider` already takes only `source` (`rdb_react/src/context.ts:7`). *Changes* 9 adds `useWriter` and nothing else.
  - `registerWriterKey` is passed at `rhost/src/host.ts:823` and `:912`.
- **Docs.** *Changes* 10 misses three READMEs:
  - rhost: `autoAuthor` in the configuration, and `host.projection.openWriter()`.
  - rhost_client_browser: it says `registerKey` computes the key id with `keyIdFromPublicKey`, with no hash suite.
  - rdb_adapter_test: it lists `LocalMutator`, which goes away, and not the writer suite.
- **Citations.**
  - `rhost/src/host.ts:486` is now `:501`.
  - `rdb_adapter/src/ingest.ts:241` is the author of a reserved identity. The signer is chosen at `rdb_adapter/src/ingest_orchestrator.ts:339-342` and passed at `:440`.
- **Wording.** In the *Authorship* table, "the writer key's id signs the insert with that key" reads better as "an insert whose author column holds the writer key's id is signed with that key".

## 10. Recommendations

1. **Record the author column in captured changes** (§3, §11.1): in inserts, so that `autoAuthor` off has its input, and in updates, so that a native change of the column is rejected and reverted.
2. **Use one table for the author column** (§4), and give the binding the writer key's id.
3. **Put "unchanged" before "`null`"** (§5), in the writer and in the native comparison.
4. **Name the event of a voided delete correctly** (§6).
5. **Read the writer's meta where the write happens** (§7).
6. **Add the cases of §8.**
7. **Word the reused-id fix as "revert to rdb's row"** (§11.2).
8. **Have the trial binding cover these.** The binding that *Changes* asks for before the interface is fixed should include a form that saves every field of a row it read, and an anonymous insert. Those are the two places where §4 and §5 show.

## 11. Outside this HIP

Two defects of today's native paths, found while checking. Neither is among the fixes of *Changes* 1.

1. **A native update of the author column is never captured** (W16).
   - **What happens.** The SQLite triggers and the IndexedDB diff cover business columns only. The row shows the written author, with no rejection and no op event, until the row next changes in rdb.
   - **Why it matters here.** *Authorship* says project-back "corrects a native write that set the column". For an insert it does, because the insert is projected back. For an update nothing is projected back.
   - **Fix.** Capture the author column in updates. The planner rejects the change and the row is reverted, as for an update to `NULL`.
2. **`INSERT OR REPLACE` over a live row diverges without a trace** (W17).
   - **What happens.** SQLite replaces the row and the insert trigger fires. The target shows the new values and a `NULL` author, rdb keeps the old row, and no op event is logged.
   - **A row that came from rdb.** Its identity was not minted on this device, so the insert is dropped as a stale replay, with a warning in the ingest result (`ingest.ts:228-236`).
   - **A row inserted on this device.** Its identity is reusable, so the insert is taken for a crash replay. rdb answers that the row exists, and ingestion skips it as already applied (`ingest_orchestrator.ts:443-449`). There is no warning.
   - **Why it matters here.** The HIP's fix for a reused id says that when the target still has the row, "ingestion reverts it, which removes it". That is right for a deleted row. Applied to a live row it would delete a row that rdb has.
   - **Fix.** State the fix as a revert to rdb's row: remove the local row when rdb has none, and write rdb's row when it has one. For a row inserted on this device, mark the row for that revert when the insert is skipped as already applied. A real replay then writes the same values again, and a replaced row gets its values and its author back.

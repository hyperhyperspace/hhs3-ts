# HIP-20: Unified write API for projection targets

- **Type:** design
- **Status:** draft
- **Modules:** rdb_adapter, rdb_adapter_sqlite, rdb_adapter_idb, rdb_adapter_test, rdb_projection, rdb_react, rhost, rhost_client_node, rhost_client_browser
- **Requires:** [29](0029-checkpoint-consistency.md), which carries the native insert over an existing local id of *Changes* 1
- **Superseded by:** —
- **Implemented in:** —

## Summary

Apps read projected tables the same way on every target, through `QuerySource` (HIP-5). Writes are different on each target, and so is their effect: on today's paths, a default, a rewritten value or a multi-column update leaves a different result in the replica on SQLite than on IndexedDB or memory. This HIP adds `RowWriter` to rdb_adapter, with `insert`, `update` and `delete` in the table names, column names and value forms that queries return. It gives each write one meaning on every target, and a shared suite checks that meaning through ingestion and project-back. Key columns also accept a public key, which the writer interns using the replica's hash suite. For a group with an identity provider, a per-group setting, `autoAuthor`, selects between two modes. Under `on`, the default and today's behavior, every op is signed with the projection's writer key. Under `off`, the app names an insert's author: the writer key's id, or `null` for an anonymous insert. The writer offers no multi-row atomicity: ingestion turns its writes into ops as it does today. The native write paths stay, and the defects found in them are fixed first.

## Why

### Background: how a local write reaches the replica

- **Groups, entries and bundles.** rdb keeps each table group as a DAG of signed entries. An entry carries one row op (an insert, an update or a delete), or a *bundle*: several ops of one group that validate and apply together, all or nothing. An entry can later be *voided*, when a concurrent change makes it invalid. A voided bundle is dropped whole.
- **Projection and target.** A projection copies a replica's groups into a relational *target*: SQLite, IndexedDB or memory. Its tables are named `<group>_<table>`. Each projected row has a *local id*, an integer that is specific to the device, and a *row hash*, rdb's id for the row, which is the same everywhere.
- **Capture, ingestion and project-back.** When an app writes to a target, *capture* records the change in the target's *outbox*. *Ingestion* drains the outbox, turns the changes into rdb ops, signs them and appends them. *Project-back* then writes each row back to the target as rdb has it. A change rdb rejects is reverted in the target and logged as an *op event*, which the UI receives together with its query results.
- **Column values and `NULL`.** An insert carries the columns it sets, and an update only the columns it changes. Each column reads the latest write to it. Between concurrent writes the one in the entry with the larger hash wins, except that a value always wins over a concurrent `NULL`. An insert or an update can set a nullable column to `NULL`. As in SQL, an explicit `NULL` wins over the column's default, and a column an insert leaves out gets the default (`rdb/README.md`, *Nulls and defaults*).
- **The writer key.** Ingestion signs with one identity per group, the projection's *writer key*. In rhost, that's the host's key.
- **Identity providers.** A group can bind an identity-provider table, whose rows pair a key hash with a public key, and rdb verifies signatures through it. A group without one is *anonymous*: rdb accepts only unsigned ops there. A group with one accepts unsigned ops too, as anonymous ops.
- **Keys.** A *key hash* is the hash of a serialized public key, computed with the replica's hash suite, for example SHA-256 or BLAKE3. The target keeps a key table, `rdb_keys`, that gives each key hash a local integer id. Key columns hold those ids: `<col>_key_id`, an identity-provider table's `key_id`, and the author column `author_key_id`. Ingestion turns them back into key hashes, and it needs the public key as well to write an identity-provider row (`rdb_adapter/src/ingest.ts:384-410`).

### Reads are portable, writes are not

HIP-5 gave every target one read surface: `QuerySource` (`rdb_adapter/src/query_builder.ts:15`), with the fluent `TableQuery` and `monitor()`. A component written against it runs on `SqliteTarget` in Node, `IdbTarget` in a browser, and `MemoryTarget` in tests. Writes have no such surface:

| Target | How an app writes | Values | Prompt `local` wake |
|---|---|---|---|
| `SqliteTarget` | SQL on a better-sqlite3 handle to the projection file | stored form: booleans as `0`/`1`, json as canonical text | only through `wrapDatabase(db, target)` or `noteLocalCommit()`; otherwise the WAL watcher |
| `IdbTarget` | `target.database.transaction(...).objectStore(table).add/put/delete`, with IndexedDB's request and callback protocol | logical | on the transaction's `oncomplete` |
| `MemoryTarget` | `localInsert`, `localUpdate`, `localDelete`, synchronous | logical | on each call |

That has these consequences:

- **A binding cannot write.** memoblocks plans one TypeScript package per schema, with the schema's row types, queries, and the writes that satisfy its rules (`send(channel, body)`, `rename(channel, name)`). Its queries already work on every target. Its writes would need three implementations, chosen by target type. The SQLite one would also need the column types, to convert values.
- **SQLite writes use a different form than SQLite reads.** An app inserting a boolean writes `1`, and a json value as json text. Reads convert back (`toLogical`, `rdb_adapter_sqlite/src/sqlite_target.ts:227`).
- **In process, there is no write handle.** `host.projection` (`rhost/src/host.ts:501`) is an `RdbProjection`, which is a `QuerySource`. Its target is a private field (`rdb_projection/src/projection.ts:140`), so an in-process app opens a second connection only to write.
- **System columns look writable.** No target captures `author_key_id` (`6110da1:rdb_adapter_sqlite/src/sqlite_target.ts:505-518`, `6110da1:rdb_adapter_idb/src/idb_facade.ts:243-249`, `6110da1:rdb_adapter/src/memory_target.ts:264-275`), and ingestion signs every op with the writer key (`rdb_adapter/src/ingest_orchestrator.ts:367-372`, `:472`). An app that leaves `author_key_id` empty in an insert shows an authorless row until project-back. One that writes another value shows the wrong author until then. A native update of the column is not captured at all, so the row shows the written author until it next changes in rdb. An IndexedDB `put` replaces the whole record, so a record built from a form, which has no author column, stores the row without its author.

### One write, three meanings

The same logical write ends differently depending on the target it goes to:

- **Defaults.** An insert that leaves out a column with a default reads the default at once on SQLite, and its op carries the value, because the capture trigger records the whole stored row (`sqlite_target.ts:556-559`). On memory and IndexedDB the row is stored as given (`6110da1:rdb_adapter/src/memory_target.ts:264-270`, `6110da1:rdb_adapter_idb/src/idb_facade.ts:497-509`). It reads `NULL`, also after project-back, which writes only the columns an op wrote (`rdb/src/rtable/delta.ts:6-7`). Its op leaves the column unwritten, so rdb and every other device read the default.
- **Required columns.** SQLite refuses an insert without a required column, and a `NULL` in a column that isn't nullable, at the statement. Memory and IndexedDB accept both and show the change, which ingestion undoes when rdb refuses the op.
- **Which columns an update writes.** The IndexedDB facade compares old and new values by identity (`6110da1:rdb_adapter_idb/src/idb_facade.ts:264`). Two structured clones of a json value are never identical, so an update of one column writes every json column again, and can overwrite a concurrent edit of one. An update that writes the value a row already has produces an op on SQLite and memory, and outside json columns nothing on IndexedDB. Such an op also undoes a clear made concurrently on another device, since a value wins over a concurrent `NULL`.
- **What fails together.** SQLite captures one change per updated column, so a readonly column is rejected alone and the other columns land. Memory and IndexedDB capture one change, and one bad column rejects all of it.
- **A missing row.** Memory captures a delete of one, which ingestion then rejects, and `localUpdate` of one throws. SQLite captures nothing for either. On IndexedDB a delete captures nothing, and a `put` of a missing id inserts the row again under that id.
- **The author column in an anonymous group.** An insert whose author column the app filled keeps showing that author on the writing device after project-back. rdb and every other device show none.

A portable writer that only hid the syntax would carry these differences into every binding. The writer is the one place where a write can be given a single meaning, and a shared suite can hold every target to it.

### The interface already exists, in the tests

The ingestion conformance suite needed a portable write and defined one. `LocalMutator` (`rdb_adapter_test/src/ingestion_suite.ts:146`) is "the app-side write surface…: direct mutations of the projected tables that the capture machinery observes." It has three implementations: raw SQL converting values with the types in `rdb_table_meta`, `target.database`, and the memory methods. The rdb_react tests call `MemoryTarget.localInsert` directly (`rdb_react/test/run_tests.ts:93`).

The suite's tables hold strings, one decimal, and, for null writes, nullable strings, a boolean, a foreign key and one default (`rdb_adapter_test/src/group_fixture.ts:26-45`, `:96-119`). No json value has gone through those mutators, capture and ingestion, and the case that leaves a column out to its default reads it in rdb and on a second device, not on the device that wrote it (`6110da1:rdb_adapter_test/src/ingestion_suite.ts:970-1004`). That is how the differences above went unnoticed. This HIP moves the contract into rdb_adapter, gives it one meaning, and tests that meaning end to end.

## Design

### `RowWriter`

```ts
// @hyper-hyper-space/hhs3_rdb_adapter
export type KeyValue = { pk: string };                              // a serialized public key, in base64
export type RowValues = { [column: string]: json.Literal | KeyValue | null | undefined };

export interface RowWriter {
    insert(table: string, values: RowValues): Promise<number>;       // the new row's local id
    update(table: string, id: number, values: RowValues, options?: { compare?: boolean }): Promise<boolean>;  // false: no such row
    delete(table: string, id: number): Promise<boolean>;                     // false: no such row
    writerKeyId(): Promise<number>;                                          // the writer key's id in the key table
}

export interface WritableTarget {
    openWriter(): RowWriter;
}

export function isWritableTarget(t: object): t is WritableTarget;
export function keyValue(pk: PublicKey): KeyValue;

export type WriteRefusal =
    | 'unknown-table' | 'unknown-column' | 'id-column' | 'wrong-type' | 'invalid-key'
    | 'missing-required' | 'not-null' | 'author' | 'no-capture' | 'no-writer-key';

export class WriteRefusedError extends Error {
    readonly reason: WriteRefusal;
}
```

Tables, columns and values are the ones a query returns:

- **Tables** are the projected names, which a binding gets from `tableNames` (*Table names*).
- **Columns** are the projected columns:
  - `<col>_id` holds the local id of the referenced row;
  - `<col>_key_id` holds a key id (see *Key columns*);
  - `<col>_row_hash` holds a cross-group reference to a group that isn't co-projected;
  - `key_id` is the key column of an identity-provider table;
  - `author_key_id` is the author column (see *Authorship*).
- **Values** are logical: booleans, numbers, strings and json values, and `null` for `NULL`. Each target converts them to its stored form with the column types it already keeps for reads.

A value written is the value read back, with one exception: a key column given a public key reads back as that key's id.

Each call is one target transaction, and announces one `local` commit event when it wrote something. A refused write, a write to a missing row and an update that changes nothing announce none. `update` writes only the columns it is given. On IndexedDB that is a read-merge-write inside the transaction.

### Write semantics

The writer applies these rules on every target before it writes:

| Write | Rule |
|---|---|
| Insert, a column not given, with a default | the default is written, in the row and in the op |
| Insert, a column not given, required | throws |
| Insert or update, `null` for a nullable column | `NULL`, also where the column has a default |
| Insert or update, `null` for a column that isn't nullable | throws |
| Update, a column given its current value | written, and not written with `compare` |
| Update, the id column or the author column at the row's own value | not written |
| Update that ingestion rejects | the whole update is reverted |
| Update or delete of an id the table doesn't have | does nothing, and resolves `false` |

The author column follows *Authorship*, where `null` asks for an anonymous insert. With `compare`, the unchanged rule applies first, and a column it sets aside is not checked further. A column given `undefined` counts as not given.

- **`null`.** It means `NULL`, in an insert and in an update, as it does in SQL and in rdb (*Background*). An insert's `null` asks for `NULL` where the column would otherwise get its default, and an update's `null` clears the column. A `null` for a column that isn't nullable is refused, since rdb would refuse the op. A clear never wins a race: a value written concurrently on another device wins over it, and every device ends with the value. Ingestion writes an insert's `NULL` into the op only where the column has a default. Elsewhere it reads the same as a column left out, and the op leaves the column out (`rdb_adapter/README.md`, *NULL values*).
- **Defaults.** A column an insert leaves out gets its default, in the row and in the op, and a `null` doesn't count as left out. That is SQL's rule, which SQLite applies natively and rdb follows. SQLite's capture records the stored row, so it cannot tell a value the app gave from one the default filled in, and its op carries the default. Writing the default into the op is therefore the only rule all three targets can share. It also makes the row read the same on every device.
- **Required columns.** A column is required when it has no default and is not nullable.
- **Unchanged columns.** An update writes every column it is given, as a native `UPDATE` does under HIP-29. A write of an unchanged value is still a write: it undoes a clear made concurrently on another device, since a value wins over a concurrent `NULL`, and it competes with a concurrent write of another value. So `update` takes a `compare` option. With it, the writer writes only the columns whose value differs from the row in the target, compared by value, json included, with `null` equal to `NULL`. A form that saves every field uses it. An edit or a clear made on another device that hasn't reached this one yet is then kept. One that reached it after the form read the row is overwritten with the form's old value. A binding that wants to keep those too sends only the fields the user changed.
- **A row spread into an update.** Code that reads a row from a query and writes it back, `update(table, row.id, { ...row, name })`, passes every column the query returned, the id column and the author column included. Those two hold the row's own values, and the writer sets them aside, with `compare` or without. Without it, the other columns are written with the values they have, and ingestion sets aside the readonly ones among them (HIP-29). With it, only `name` is written. A different id or author is still refused.
- **Failures of an update.** An update lands or is rejected as a whole: it is one captured change and one op, and a refusal reverts the row (HIP-29). Landing its columns one by one could leave a row that nobody wrote.
- **Missing rows.** A remote delete can land between a render and a click. An update or delete of a row the table doesn't have writes nothing, captures nothing, announces no commit event, and resolves `false`, so the app can tell the user that the row is gone. `true` means the row was there and the write is stored, also when an update changed no column. It doesn't mean rdb accepted the write. Recording the write would gain nothing, because rdb accepts an update or a delete only of a row that is live at the op's parent frontier (`rdb/src/rtable_group/validate_ops.ts:429-433`), and an op ingested after the delete has that delete in its past. The other orders already have a path:
  - **The delete reaches the replica after the write.** Ingestion rejects the update or delete, reverts the row and logs an op event, as it does today.
  - **The delete is voided later.** The row comes back under the same local id, and the app sees it return in its queries. The op event log records the flip as a `void` event of kind `delete`, since the op whose verdict flipped is the delete (`rdb_adapter/src/project.ts:90-91`). The write that resolved `false` is not replayed, so the user makes it again.

### Key columns

A key column takes one of two values, or `null` when it is nullable:

- **a number:** a key id from the target's key table. An id the key table doesn't have throws, since ingestion is certain to reject it.
- **`{ pk }`:** a public key, as the base64 of the serialized key. The writer computes its key hash with the replica's hash suite and interns it in the key table, in the same transaction as the write. Then it stores the id.

The column's type decides what a value means. `{ pk }` is a key only in a key column: in a json column it is a json object like any other, and a number there is a number. The author column takes a key id or `null`, not a `{ pk }` (*Authorship*).

The writer interns a key only after the write's checks pass and its row is found. A refused write, and a write to a missing row, leave the key table as it was.

```ts
w.insert('chat_members', { member_key_id: 7 });                       // a key id
w.insert('chat_members', { member_key_id: { pk: invite.publicKey } }); // interned by the writer
w.insert('chat_members', { member_key_id: '7' });                     // throws
```

A bare string throws, as any value of the wrong JSON type does, so a string is never mistaken for a public key. That matters because the deserializer checks nothing (`crypto/src/identity.ts:28-35`). It reads a 4-byte length and slices whatever follows, so a key hash passed by mistake parses into a garbage suite name and an empty key. The writer therefore also checks the key itself. Its suite must be a registered signing suite, its length must be that suite's public key size (`crypto/src/signing.ts:13`), and its base64 must be the canonical encoding. The serialized key names its own signing suite (`identity.ts:9-26`), so `{ pk }` needs no suite field. `keyValue(pk)` builds the value from a `PublicKey`.

Interning a public key also lets an app write a new identity-provider row with only a public key in hand. Ingestion needs the public key for that row, not just its hash.

### What the projection records in the target

When a projection opens, it records three things in the target:

- the writer key, interned in the key table, when it opens with one;
- the name of the replica's hash suite;
- each projected group's member name (*Table names*) and authorship mode: `anonymous` for a group without an identity provider, and otherwise `on` or `off`, from `autoAuthor` (see *Authorship*).

A projection opened without a writer key records the rest, so `tableNames` answers on a read-only projection too, and a writer there refuses every write to a group with an identity provider with `no-writer-key`. A write to an anonymous group, recorded as `anonymous`, needs no key: HIP-29's ingestion takes an anonymous group's changes without one. It records them again whenever its groups change. The writer reads them inside each write's transaction, so a writer in a client process always sees the current values, also while the host is stopped.

HIP-29's capture reads the writer key and the modes to fill in the author column, so this record lands with HIP-29, ahead of the rest of the writer. So does `autoAuthor` (*Authorship*): the setting, `RdbProjection`'s option and its check, rhost's config field, and ingestion's handling of `autoAuthor` off. The writer's own refusals stay with the writer.

The mode is recorded per group, since `autoAuthor` is set per group. To find a table's mode, the writer reads the table's group from its meta. Each target records the group id in a table's meta when it creates the table for that group, with the table's name in the schema, and neither ever changes. So a table that a schema change adds after the projection opened has a mode at once, and the projection needs to write the modes only when its groups change, even before a new group's tables exist. A table whose group has no recorded mode is refused as unknown, since ingestion would drop its writes (`rdb_adapter/src/ingest.ts:581`).

The hash suite belongs to the replica, and the projection already reads it there. The target holds a copy because a writer is opened from a target and can't reach the host. For the same reason, the rhost clients read the hash suite from the target instead of assuming SHA-256 (`rhost_client_node/src/client.ts:31`, `rhost_client_browser/src/client.ts:28`). On a BLAKE3 replica, today's clients intern keys under a hash rdb doesn't use. An identity-provider row written with such a hash fails rdb's check that its key hash matches its public key (`rdb/src/rtable_group/validate_ops.ts:406`, `:531`), and a key column ends up pointing at a hash that no key has.

### Authorship

A per-group setting, `autoAuthor`, decides what an insert gets when it doesn't name its author. It is on by default, which is today's behavior. `RdbProjection` takes a default for all its groups, and the per-group config override can set it for one group.

In a group with an identity provider, an insert through the writer gets:

| The insert gives, for the author column | `autoAuthor` on (default) | `autoAuthor` off |
|---|---|---|
| nothing | signed with the writer key | refused |
| the writer key's id | signed with the writer key | signed with the writer key |
| `null` | refused | anonymous |
| another key's id | refused | refused |

A refusal is a `WriteRefusedError` with reason `author`. A `{ pk }` in the author column is refused too: the column accepts one key, the writer key, whose id `writerKeyId()` gives. So a binding that names the author gets what it named, or a refusal, under either setting. Only an insert that leaves the column out depends on the setting, and then it gets a signed row or a refusal. `writerKeyId()` gives a binding the value that means "signed". A signed row reads back with that id in its author column, and an anonymous one with `NULL`.

In an anonymous group, every op is anonymous, and `autoAuthor` is ignored. The projection records the group's authorship mode as `anonymous`, whatever the setting says, so one projection-wide default can cover groups of both kinds. A group's identity provider is fixed when the group is created (`rdb/src/rtable_group/payload.ts:123`), so a group never changes kind. Through the writer, `null` or nothing is accepted, the column reads `NULL`, and a key id is refused, the writer key's included.

In an update, the author column is refused unless it holds the row's own author (*Write semantics*). A row's author is the author of its insert, and no op can change it, to `NULL` or to another key. The row hash is derived from the row's uuid and its author (`rdb/src/rtable/hash.ts:10-15`), and the author an update carries is only the signer of that update.

Native writes follow the same setting, applied by ingestion, which reads the author column from the captured insert (*Changes* 1). In a group with an identity provider, an insert naming the writer key's id is signed under both settings, and one naming another key is rejected under both, with its row reverted and an op event logged. The native paths treat a column left out and one set to `NULL` alike, since raw SQL can't tell the two apart: with `autoAuthor` on the insert is signed with the writer key, and with it off it is anonymous. So the native path can't refuse an insert that leaves the column out, as the writer does. Under HIP-29, capture fills in the writer key's id there, from the writer record, so the row shows its author at once. A native update of the author column is rejected and reverted under both settings. An IndexedDB `put` over an existing record that leaves the column out, or gives it `null`, keeps the stored author and captures nothing for it. A `put` replaces the whole record, and a record built from a form has no author column. Since no op can change the author, keeping it loses nothing. In an anonymous group, a native insert that names a key, the writer key's included, is rejected, with its row reverted and an op event logged, since no op there can carry an author (`rdb/src/rtable_group/validate_ops.ts:368`). One that leaves the column out or sets it to `NULL` is anonymous.

Project-back is the authority for the author column. It writes the author of the row's insert, or `NULL` for an anonymous row. That corrects a native write that set the column, and the writing device then shows what rdb and every other device show.

`autoAuthor` off needs the author column, because an insert's author is read from it. `RdbProjection` refuses a group with an identity provider whose config sets `autoAuthor: false` together with `authorColumn: false`. It checks at open and on each reconfigure, before it writes anything to the target, and throws an error that names the group. An anonymous group with the same config opens, since `autoAuthor` is ignored there.

**Updates and deletes are always signed with the writer key** in a group with an identity provider. Unlike an insert, they have no column that could carry a signer. Under rdb's default rules, this makes an anonymous row in such a group permanent. By default, only a row's author may update or delete it (`rdb/src/rschema/payload.ts:322-325`), and a comparison with a missing author is false (`rdb/src/rtable_group/predicates.ts:198`). Unless the schema's rules allow it, no one can change or delete such a row. Signing an update or delete anonymously, or as another identity, needs a signer per change in the outbox (open question 3).

With `autoAuthor` off, ingestion makes two changes:

- **The signer comes from the captured insert.** Each insert is signed as the author column of its captured change says, and its row hash is derived from its uuid and that signer, or from the uuid alone when it is anonymous. Ingestion reads the author from the outbox, not from the row, because by then the row may be deleted or its author column changed. After a crash, the next cycle finds the entry by the hash recorded in the outbox (HIP-29), with its signer and row hash.
- **No identity is stored before the append.** Under HIP-29, the end of a pass reads an insert's identity from the entry it finds by its recorded hash. Today the row hash is derived from the writer key (`rdb_adapter/src/ingest.ts:270`). With signers that differ per insert, it is derived from the captured signer. An insert on a local id that already has a live identity replaced the row, for example through an `INSERT OR REPLACE` that names another author. It is refused and reverted to rdb's row with a failure event (*Changes* 1, which lands with HIP-29). Signed as captured, it would get a new row hash and reach rdb as a second row with the same uuid.

### Atomic writes

The writer offers no multi-row atomicity. Each call is one target transaction, and ingestion turns each captured change into its own entry (HIP-29):

- **FK-linked inserts.** Each insert is its own entry. One that references a row inserted earlier is appended after that row's entry, so its reference finds the row.
- **Updates of one row.** An update of several columns is one change and one op. If rdb rejects it, the whole update is reverted.

Today ingestion also bundles consecutive FK-linked inserts and merges consecutive updates of one row. HIP-29 removes both: each change becomes one entry, and its outbox row records what became of it.

So a binding's two writes can come apart in the replica. One may be accepted and the other rejected, or one may be voided later. The outbox doesn't record which changes an app wrote together (`rdb_adapter/src/types.ts:162-164`), so ingestion has nothing to keep them together by.

An app that needs atomic writes goes to rdb directly, with `RTableGroup.bundle` or rdb_lang's `BUNDLE ON <group> (…)` (`rdb_lang/README.md:200`). That path has three limits:

- **In process only.** It works only in the process that holds the replica. A client process or a tab has no atomic path.
- **rdb's names and identities.** It takes rdb's table and column names, row hashes and key hashes, not the projection's names, local ids and key ids.
- **No optimistic rows.** Its rows reach the target only through project-back.

Open question 1 sketches a batch that becomes one bundle.

### Checks

Inside each write's transaction, before it writes, the writer checks the write. It throws a `WriteRefusedError`, writing nothing, when the write has:

- an unknown table or column, counting a table whose group has no recorded authorship mode as unknown (`unknown-table`, `unknown-column`);
- the table's id column in an insert, or in an update with a value other than the row's id (`id-column`);
- an author value that *Authorship* refuses (`author`);
- a value whose JSON type isn't the one reads return for that column type, or that isn't a json literal, such as a json value with a `null` inside it (`wrong-type`);
- a key value that isn't a valid key, or a key id the key table doesn't have (`invalid-key`);
- an insert without a required column (`missing-required`), or a `null` for a column that isn't nullable (`not-null`).

Any other error a write throws comes from the target's storage, such as a full disk or a closed database.

The same transaction reads whether the row of an update or delete is there, and resolves `false` without writing when it isn't (*Write semantics*). An update's checks that depend on the row's values, for the id column and the author column, run after that read. Keys are interned last.

Everything that depends on the schema view or on history stays with ingestion: readonly columns, FK validity, `ALLOW` rules and voiding. A write that breaks one of those is reverted at ingestion and logged as an op event, which reaches the UI through `useQuery`'s `events` and the rhost clients' `events`.

The checks read the table meta that targets keep for reads: column types, which columns have a default, nullability, FK columns, key columns, the author column and the table's group. A target opened by a client process caches this meta and today never refreshes it. After the host adds a column, the client's SQLite target returns rows without it. After the host drops one, the client's queries throw. An IndexedDB target opened before the host created a table can query that table but cannot write to it. So targets follow the host (*Changes* 1, "Table meta follows the host"). SQLite drops its cached meta for the tables an `apply` commit lists, all of it on `reconnect`, and compares its schema version at each read and write. IndexedDB reads its table names and indexes again before it delivers a commit that changes tables, or a reconnect.

That keeps reads current once the commit arrives, but a target that only writes may never receive it. An IndexedDB target opens the channel that carries other instances' commits only while it has a commit listener (`rdb_adapter_idb/src/idb_target.ts:739-743`). In a client process, a SQLite target gets the host's commits only if the caller forwards them (open question 5). And a write made before the commit arrives would use the old meta. So the writer doesn't rely on the stream, and reads what it checks against inside the write's transaction:

- **IndexedDB.** The writer's transaction spans the target's stores (*Changes* 4), so it reads the table's meta record there, as the facade does inside each write (`rdb_adapter_idb/src/idb_facade.ts:508`). What the target caches is what the facade answers synchronously, the table names and the indexes, and the capture switch. So when a table isn't among the cached names, the writer reads the meta store before refusing the table.
- **SQLite.** The writer compares `PRAGMA schema_version` inside its transaction, where it reads the writer record, and reloads the meta when the version changed.

A writer refuses to open on a target that isn't set up for capture (`no-capture`), since its writes would never reach rdb. For the same reason, each write checks inside its transaction that capture is still on, and refuses with `no-capture` when `setCaptureEnabled(false)` has switched it off. It reads the switch its target's capture obeys: `rdb_capture_config.enabled` on SQLite, which the capture triggers read, and the target's own switch on IndexedDB and memory. Each write to a group with an identity provider also refuses with `no-writer-key` when no projection has recorded a writer key in the target. It reads the group's recorded mode to tell: a write to a group recorded as `anonymous` needs no key, since ingestion signs nothing there and takes its changes without one (HIP-29). That check can't run at open. `openWriter` is synchronous, IndexedDB reads the writer record only inside a transaction, and a copy cached when the target opens would stay wrong for a tab that opened before the projection first recorded it. `writerKeyId()` throws `no-writer-key` in the same case.

### Row identity

`rowHash(table, id)` and `localId(table, rowHash)` move to `QuerySource`, next to the reads. Code that only renders needs `localId` to resolve a link it was sent, and it has no writer. A new row's hash is unknown until the row is ingested, and stays unknown while the host is stopped (open question 4).

`registerKey(publicKey)` stays where it already is, on the rhost clients and on `RdbProjection`. Code calls it to get a key's id, for example to compare with an author column.

### Table names

A projected table's name is its group's member name, as a prefix, and the table's name in the schema: `chat_messages` for the table `messages` of the member `chat`. A binding can't build it by hand. The member name gets a suffix when two catalogs define groups of the same name (`rdb/src/rdb/instantiate.ts:227-264`), and one binding serves groups of any name.

`QuerySource` gains `tableNames(group)`. It resolves a member name to the projected names of its group's tables, keyed by their names in the schema:

```ts
const t = await db.tableNames('chat');   // { messages: 'chat_messages', channels: 'chat_channels', … }
```

It answers from the target, so it gives the same names in process, in a client process and in a tab, also while the host is stopped. The projection records each group's member name in the target with its authorship mode (*What the projection records in the target*). Each target records a table's group and its name in the schema in the table's meta, when it creates the table. An unknown member name throws, and a group whose tables are not projected yet gives an empty map.

### Where apps get a writer

- **In process:** `projection.openWriter()`. With rhost, that's `host.projection.openWriter()`.
- **In a client process or tab:** `openWriter()` on the target the app already queries (HIP-5), or on the query source built on that target.
- **In React:** `useWriter()` opens the writer from the `ProjectionProvider`'s source, so reads and writes always use the same target. It throws when the source cannot write.

A binding takes a `QuerySource` and a `RowWriter`, and needs nothing else from the platform:

```ts
export async function chat(db: QuerySource, w: RowWriter, { group }: { group: string }) {
    const t = await db.tableNames(group);
    return {
        messages: (channelId: number) => db.selectFrom(t.messages)
            .where('channel_id = :c', { c: channelId }).orderBy('sent_at', 'desc').limit(50),
        send: (channelId: number, body: string) =>
            w.insert(t.messages, { channel_id: channelId, body, sent_at: new Date().toISOString() }),
        invite: (publicKey: string) =>
            w.insert(t.members, { member_key_id: { pk: publicKey } }),
    };
}
```

### What stays

- **The native write paths:** SQL on SQLite, `target.database` on IndexedDB, and the memory methods. The writer is built on them. An app that needs something only one target offers, such as a set-based `UPDATE … WHERE` in SQLite, can still issue it. These paths get the fixes listed under *Changes* 1, and `autoAuthor` applies to them at ingestion.
- **Everything downstream of the target:** the outbox, settle, op events and the commit stream. Capture changes only to record the author column and to skip unchanged values, and ingestion only as *Authorship* and the fixes describe (*Changes* 1). HIP-29 then has capture record every column a write names and fill in the author.

## Why it is sound

1. **One meaning per write.** The writer applies *Write semantics* before writing, so every target receives the same normalized write. The suite asserts four results on all three targets: the captured change, the ingest result, the row after project-back, and rdb's row. A difference between targets then fails a test.
2. **Values round-trip.** On SQLite the writer converts with `toParam` (`sqlite_target.ts:111`), the inverse of `toLogical`, using the column types from `rdb_table_meta`. IndexedDB and memory store logical values unchanged. A column an insert leaves out reads back as its default or as `NULL`, a `null` as `NULL`, and a `{ pk }` as the key's id.
3. **Key hashes are rdb's.** The writer hashes with the suite of the replica the projection reads. That is the suite rdb uses when it checks an identity-provider row.
4. **The author column agrees on every device.** With `autoAuthor` on, the writer fills in the id of the key ingestion signs with. With it off, ingestion signs as the captured column says. In both cases the column ends up holding the author of the row's insert, or `NULL`. Project-back writes it on the devices that receive the row. On the writing device, under HIP-29, capture fills it in for a native insert that left it empty. A native update of the column is reverted. An IndexedDB `put` that leaves the column out keeps the stored author.
5. **No new replica claims.** Each call becomes the ops ingestion makes today, and the writer promises no atomicity.
6. **A client cannot write with stale meta.** The writer reads the meta it checks against, and the projection's recorded values, inside each write's transaction. It doesn't depend on receiving the commits that change them, which keep reads current.

## Changes

Before the interface is fixed, one binding is written against the draft: the writes of memoblocks' chat app, ported in a copy of the app that has its own schema release and catalog. The schema already has nullable strings and readonly FKs. The copy adds a boolean column, a channel-edit form that saves every field of a row it read with a field the user empties, and an anonymous report on a message, under `autoAuthor` off: the places where the rules for unchanged columns, for `null` and for the author column show. Its tables are named through `tableNames`.

In landing order:

1. **Fixes to today's paths.** None of them depends on the writer, and apps that write natively hit them now.
   - **A failed rejection record no longer blocks ingestion.** A json value with a `null` inside it can't be hashed. Appending its op throws, and so does recording the failure, which hashes the same payload (`6110da1:rdb_adapter/src/ingest_orchestrator.ts:168-173`, `dag/src/dag.ts:65-69`). The pass throws with the outbox untouched, and so does every later one. Local writes stop reaching rdb, and remote changes stop reaching the projection, because a sync cycle ingests before it projects. Instead, ingestion refuses a captured value that isn't a json literal (`json.literalProblem`) before it sends anything to rdb, as it refuses a readonly column. The failed op it records then carries no values, so it can be hashed, and SQLite's event log, which serializes the op again (`rdb_adapter_sqlite/src/sqlite_target.ts:508`), can store it. If recording a failure still throws, the failure stays that change's rejection, its row is reverted, and its event is stored without the op, under the ids of its captured changes (next fix). The ingest pass always reaches `commitIngest`.
   - **Each failure is logged.** All three targets keep one op event per op hash and direction (`rdb_adapter/src/memory_target.ts:196`, `rdb_adapter_idb/src/idb_schema.ts:164`, `rdb_adapter_sqlite/src/sqlite_target.ts:470`), and a failure's hash is computed from its failed op. When ingestion refuses a write before sending it to rdb, the failed op it records has no values (`rdb_adapter/src/ingest.ts:491-492`), so its hash names only the table and the row. Once one such failure is logged for a row, every later one on that row is dropped from the log, which is what the UI receives, though the ingest result still reports it. Two rdb rejections of the same op collapse the same way. A failure's hash also covers the ids of the captured changes it came from, and, for a write refused before rdb sees it, the column it refused. A multi-column update from memory or IndexedDB is one captured change, which the planner splits per column, so two columns refused from it would otherwise share a hash. After a crash, the same ids give the same hash, so each failure is logged once, and every new write gets its own event.
   - **Changes are compared by value.** The IndexedDB facade's diff uses `json.eq`, and so does `localUpdate`. The SQLite update triggers fire only `WHEN NEW.<col> IS NOT OLD.<col>`. On all three, `null` equals `NULL`, so an update to `NULL` of a column that is `NULL` captures nothing. Today a captured unchanged value becomes a write, which wins over a clear made concurrently on another device. HIP-29 moves the comparison out of capture: native capture records every column a write names, and the writer compares when its update is given `compare`.
   - **The author column is captured.** The SQLite capture triggers, the IndexedDB facade's `businessValues` and diff, and memory's `localInsert` and `localUpdate` record the author column, which none of them does today. Memory keeps it where it keeps a row's author, apart from the row's values. A captured insert's value is the input of `autoAuthor` off. Ingestion resolves it through the key table, as it resolves other key columns (`rdb_adapter/src/ingest_orchestrator.ts:100`), and rejects a value that isn't a key id of that table, such as text, which SQLite's integer affinity lets into the column. An insert naming another key is rejected under both settings, and in an anonymous group an insert naming any key is rejected. A captured update of the column is rejected by the planner, and the row reverted, under both settings. An IndexedDB `put` over an existing record that leaves the column out, or gives it `null`, stores the old author and captures nothing for it, the way the facade already sets `table`, `id` and the id column on every write (`rdb_adapter_idb/src/idb_facade.ts:532-534`).
   - **Local inserts show their defaults.** The IndexedDB facade's `add`, its `put` of a new record, and memory's `localInsert` fill in the default of each column the record leaves out, in the row and in the captured change, as SQLite's `DEFAULT` does. A `null` doesn't count as left out, as project-back's fill of a new row already decides (`rdb_adapter_idb/src/idb_target.ts:536-538`, `rdb_adapter/src/memory_target.ts:904-906`).
   - **A missing row's update or delete records nothing on memory.** `localUpdate` and `localDelete` do nothing for a row the table doesn't have, and return `false`. Today `localUpdate` throws and `localDelete` captures a delete that ingestion rejects.
   - **Memory refuses a value for the id column.** `localInsert` and `localUpdate` copy every value into the row, the id column included. A query row then shows that value in place of the row's local id (`6110da1:rdb_adapter/src/memory_target.ts:361-367`), while ingestion ignores the column (`rdb_adapter/src/names.ts:219`). Memory allocates ids itself, and an update can't change one, as on SQLite. So `localInsert` refuses a value for the id column, and `localUpdate` refuses one other than the row's own id, which it sets aside, as SQLite lets `SET id = id` pass. A query row shows only the table's columns, so no stored value can stand in for the id.
   - **A native insert over an existing local id is reverted to rdb's row.** Two such inserts are captured and then lost today:
     - **The id of a deleted row.** A SQL `INSERT` or an IndexedDB `put` with it is dropped by ingestion as a stale crash replay, with only a warning in the ingest result (`rdb_adapter/src/ingest.ts:251-268`, `:584-589`, `ingest_orchestrator.ts:442-444`). The row stays in the target, with no rdb row and no op event.
     - **The id of a live row.** SQLite's `INSERT OR REPLACE` replaces the row and fires the insert trigger. The target shows the new values and a `NULL` author, while rdb keeps the old row. A row that came from rdb is dropped as a stale replay, with a warning. A row inserted on this device is taken for a crash replay, and ingestion skips it as already applied, with no warning (`ingest_orchestrator.ts:475-482`).

     Instead, ingestion refuses the insert and reverts the row to rdb's row: it removes the local row when rdb has none, and writes rdb's row when it has one. Each is logged as a failure op event, and the mapping keeps its status.

     Both cases land with [HIP-29](0029-checkpoint-consistency.md) (its *Reverts*). Today ingestion can't tell such an insert from a crash replay, since both reach rdb as an insert whose row exists. Under HIP-29 there are no replays: what a crashed pass appended is known from the entry hashes recorded in the outbox, and an insert on a local id that already has an identity is an ordinary failure. No change source method is needed to tell a stale replay from a live insert: the revert of an insert whose local row is already gone writes nothing. SQLite's `INSERT OR REPLACE` over a row inserted on this device is reverted to rdb's row and logged as a failure op event, like one over a row that came from rdb.
   - **SQLite columns refuse values that rdb would settle differently.** Two native writes are accepted today and diverge from rdb:
     - **Booleans.** A boolean column takes any value, and capture reads every value but `0` as true (`6110da1:rdb_adapter_sqlite/src/sqlite_target.ts:216-220`). A row written with `5` syncs as `true` but keeps `5` until project-back writes `1`, so `WHERE flag = 1` misses it.
     - **Json.** A json column takes any text, and capture parses it when it drains the outbox (`6110da1:rdb_adapter_sqlite/src/sqlite_target.ts:218`).
       - **Text that isn't json** makes the drain throw, so this pass and every later one throw with the outbox untouched. Local writes stop reaching rdb, and remote changes stop reaching the projection.
       - **The text `null`** parses to `null`, which reaches rdb as SQL `NULL`, while SQLite holds the four characters, so `WHERE <col> IS NULL` misses the row.

     The SQLite target creates boolean columns with `CHECK (<col> IN (0, 1))`, and json columns with `CHECK (json_valid(<col>) AND json_type(<col>) != 'null')`. SQLite then refuses such a write at the statement, as it refuses a `NULL` in a `NOT NULL` column. SQL `NULL` passes, since both checks are then unknown.

     The drain also refuses a change whose value it can't read, rather than throwing, so the pass reaches `commitIngest`, as with a value that isn't a json literal.
     - **The marker.** The captured change names such a column in an optional `unreadable` field, with the reason, and leaves it out of its values.
     - **The refusal.** The planner refuses that column before any other check and records the failure. An update's other columns still land. HIP-29 refuses the whole update instead.
     - **Dropped tables.** A change whose table is no longer in the target is read the same way.

     Json text in another form than projection's canonical text, such as `{ "b": 1, "a": 2 }`, is accepted, since it holds the same value. HIP-29's settle rewrites it in canonical form, so that a query binding the value finds the row, as it finds rows from rdb. HIP-29, which stops projection from writing a pass's own rows a second time, relies on this parity.
   - **A native change of the id column fails on SQLite.** Nothing captures it today. The row sits under the new id with no rdb identity, and a later native update of it is rejected but not reverted, since ingestion finds no rdb row for that id. When the row next changes in rdb, project-back inserts it again under the old id, and the target holds it twice.
     - **The trigger.** A trigger installed with the capture triggers fails a statement that changes the id. It is a `BEFORE UPDATE` trigger on the table, since one written `UPDATE OF <id>` misses `SET rowid = …`, and `SET id = id` still passes.
     - **Capture paused.** It fires whether or not capture is on: with capture paused, an id change would still break the row's mapping.
     - **The adapter's own writes.** Ingestion and project-back never change an id, and the trigger lets their writes through. IndexedDB and memory have no such statement: a `put` with another id writes another record, and memory's `localUpdate` takes the id apart from the values.
   - **Updates fail column by column on every target.** The planner splits a multi-column update from memory or IndexedDB into one change per column before it merges updates, so `updateMerge` treats them as it treats SQLite's per-column changes (`rdb_adapter/src/ingest.ts:157-209`). HIP-29 replaces this: it removes `updateMerge`, captures an update of a row as one change on every target, and lands or refuses each update whole.
   - **Project-back corrects the author column.** It writes `NULL` to the author column when the row action has no author.
   - **Memory queries return `null` for empty columns.** A row from a memory query has no key for a column the row has no value in, the author column included (`6110da1:rdb_adapter/src/memory_target.ts:361-367`), where SQLite and IndexedDB return `null`. Memory keeps `NULL` as a missing key, so a `null` written there reads back as `undefined`, where the app on SQLite or IndexedDB sees `null`. Memory returns `null` for each column of the table the row has no value in.
   - **Table meta follows the host, for reads.** A target that another handle or tab changes now reads the new meta.
     - **SQLite.** It drops its cached meta for the tables an `apply` commit names (`schemaTables`), all of it on `reconnect`, and all of it when its own `apply` rolls back. Today a rollback leaves the cache edits of the failed `apply` behind. It also compares `PRAGMA schema_version` at the start of each read and write that uses column types, inside the transaction of the reads that follow.
     - **IndexedDB.** It reads its table names and indexes again before it delivers a commit that changes tables, or a reconnect, and when a listener arms its channel. Refreshes and deliveries run in order, so no delivery overtakes another.
     - **Two limits remain on IndexedDB.** A tab without a commit listener isn't told about new tables until it reconnects, and an index reconcile is never announced.
   - **A table that a schema change adds is projected with its group's prefix.**
     - **Today.** A projection builds each group's table names from the tables the group has when it opens or reconfigures (`rdb_projection/src/scope.ts:87-94`). The adapter falls back to the bare name for a table that list doesn't hold (`6110da1:rdb_adapter/src/names.ts:41-43`).
     - **The effect.** A table a later schema change adds is projected as `extra` instead of `shop_extra`. A foreign key into it from another group projects as a row hash, as if that group weren't co-projected, and ingestion writes the table under its bare name.
     - **The fix.** `AdapterConfig` gains `tablePrefix`, which the projection sets to each group's prefix. A table that no rename lists is named `<prefix>_<table>`, and the reverse mapping strips the prefix. The cross-group resolver falls back the same way.
2. **rdb_adapter:**
   - **Types.** `KeyValue`, `RowValues`, `RowWriter`, `WritableTarget`, `isWritableTarget`, `keyValue`, `WriteRefusal` and `WriteRefusedError` in `types.ts`, and `autoAuthor` in `AdapterConfig`.
   - **`QuerySource`.** It gains `rowHash`, `localId` and `tableNames`. `QueryEngine` implements them through its target, and opens a writer when its target can. The query source has `openWriter` only then, so `isWritableTarget` is false for one over a target that can't write (`rdb_adapter/src/query_builder.ts:168-181`).
   - **Checks.** A shared `checkWrite` over a `TableShape`. The shape holds column types, which columns have a default, nullability, FK columns, key columns, the id column, the author column, the table's group and its authorship mode.
   - **The writer record.** A target interface for it: the projection writes the writer key id when it has one, the hash suite and each group's member name and authorship mode, and the writer reads them inside its transaction. Each target's table meta gains the table's group id and its name in the schema, set when the table is created: the group from the `apply` that creates it, and the name from the `create-table` action, which now carries it (`rdb_adapter/src/types.ts:420`). The field is optional: an action without it, as tests write them, names a table whose projected name is its name in the schema. The record lands with HIP-29, in each target.
   - **Ingestion with `autoAuthor` off**, which lands with HIP-29, with `autoAuthor` in `AdapterConfig`:
     - each insert's signer comes from the author column of its captured change;
     - row hashes are derived per signer;
     - a stored identity is reused only when the captured signer derives its row hash;
     - op events name the signer.
   - **Memory.** `MemoryTarget.openWriter`.
3. **rdb_adapter_sqlite:** `SqliteTarget.openWriter`.
   - `openWriter` sets up the target's bookkeeping first, which is where the target detects capture (`sqlite_target.ts:494`).
   - It builds statements from `rdb_table_meta`, which moves `sqliteLocal` out of the tests.
   - Values go through `toParam`. A `null` is bound as `NULL`, and an `INSERT` names the column with it, so the table's `DEFAULT` doesn't apply.
   - It runs each call in one immediate transaction, `db.transaction(…).immediate()`, and calls `noteLocalCommit()` after it. A call reads before it writes. In a deferred transaction that write fails once another connection, such as the host's, has committed in between, in WAL mode with `SQLITE_BUSY_SNAPSHOT`, which the busy timeout doesn't retry. The target's own transactions that read and then write are immediate for the same reason (`sqlite_target.ts:407-413`).
   - Every update reads the row first, as on IndexedDB. The read tells whether the row is there, the id column and the author column are compared with it, and a `{ pk }` is interned only once the row is found. `delete` resolves from the statement's count of changed rows, which doesn't count the rows its capture triggers write.
   - Before each write it compares `PRAGMA schema_version`, and reloads the meta when the version changed.
   - Nullability, and whether a column has a default, come from `PRAGMA table_info`. The writer leaves the default itself to the table's `DEFAULT`.
   - The writer record is a one-row table. `rdb_table_meta` gains the table's group id and its name in the schema as fields of `TableMeta`, so that `writeMeta`, which rewrites the whole row when a column is added or dropped, keeps it (`sqlite_target.ts:619-628`).
4. **rdb_adapter_idb:** `IdbTarget.openWriter`.
   - Each call is one IndexedDB read-write transaction over the target's stores, with a read-merge-write for each update, which moves `idbLocal` out of the tests. It isn't a `target.database` transaction: the facade refuses a table outside its cached table list before any read (`rdb_adapter_idb/src/idb_facade.ts:1014-1018`), and it doesn't expose the key counter, the meta store or the writer record.
   - The writer shares the facade's write code, so the facade's rules hold, such as a `null` stored as a missing key (`rdb_adapter_idb/README.md:97`).
   - An update or delete reads the row first and stops when it is missing. An update never `put`s a missing row, which would insert it again. A call resolves when its transaction completes, not when its read returns.
   - A table missing from `tableNameSet` is looked up in the meta store before the writer refuses it.
   - Nullability is added to the table meta, and so are the table's group id and its name in the schema.
   - The writer record is a record in the capture config store (`rdb_adapter_idb/src/idb_schema.ts:166`). In the meta store it would be taken for a table, since opening the target lists every meta record as one (`rdb_adapter_idb/src/idb_target.ts:400-405`).
5. **rdb_adapter_test:**
   - A new `createWriterSuite(label, factory)`, which the sqlite and idb packages run as they run the others.
   - A fixture table for it (see *Tests*).
   - `IngestionHarness` drops `LocalMutator` and writes through the target's writer, except in the cases listed under *Tests*. `setCaptureEnabled` stays on the harness.
6. **rdb_projection:**
   - The projection records the writer record at open and on each reconfigure, with the member name and mode of every member group. A reconfigure runs before the first sync (`rdb_projection/src/projection.ts:151-154`), when a new group's tables don't exist yet, which is one reason the mode is kept per group.
   - It takes an `autoAuthor` option, a default for every member that a per-group `configOverride` can change. It records an anonymous group's mode as `anonymous`, whatever the option says, and refuses a group with an identity provider, `autoAuthor: false` and `authorColumn: false`. This lands with HIP-29, as does the recording of the writer record.
   - `RdbProjection.openWriter()`, which refuses without a writer.
   - `RdbProjection` gains `rowHash`, `localId` and `tableNames`, since it is used as a `QuerySource`.
7. **rhost:** `openProjection` drops `registerWriterKey` (passed at `rhost/src/host.ts:823` and `:912`), since the projection now always interns its writer key. `autoAuthor` joins the projection config. `ProjectionConfig` and its validation, which today accepts a fixed list of fields, take it (`rhost/src/config.ts:33`, `:195`), and the host passes it to `openProjection`. This part lands with HIP-29.
8. **rhost_client_node, rhost_client_browser:** `registerKey` hashes with the suite recorded in the target.
9. **rdb_react:** `useWriter()` opens the writer from the `ProjectionProvider`'s `source`, which is already the provider's only prop (`rdb_react/src/context.ts:7`). It opens one writer per source, as `useQuery` keeps one subscription per source and query (`rdb_react/src/use_query.ts:19-22`). The tests write through the writer instead of `localInsert`.
10. **Docs:**
    - **rdb_adapter README.** A new *Writing* section, next to *Reactive queries*, holds the full explanation: the write semantics, key values, authorship, and the absence of atomicity. For what a `NULL` becomes it links to *NULL values* and to rdb's *Nulls and defaults*.
    - **sqlite and idb READMEs.** They link to that section as the portable path, and keep their native write sections, which now say that `autoAuthor` applies to native writes. The IndexedDB one also says that a `put` keeps the stored author.
    - **rdb_projection README.** It documents `autoAuthor` and links to the section.
    - **rdb_react README.** It covers `useWriter`.
    - **rhost_client_node README.** Its example writes through the writer.
    - **rhost README.** `autoAuthor` in the host's configuration, and `host.projection.openWriter()`.
    - **rhost_client_browser README.** `registerKey` hashes with the suite recorded in the target, not with `keyIdFromPublicKey`'s default.
    - **rdb_adapter_test README.** It lists the writer suite instead of `LocalMutator`.

## Tests

`createWriterSuite`, run on memory, SQLite and IndexedDB. Every case asserts four things: the captured change, the ingest result, the row after project-back, and rdb's row. The suite's fixture gains a table with one column of each type, a nullable column with a default, a column with a default that isn't nullable, a required column, a readonly column and a readonly json column.

- **Values.** Booleans, json values, integers, floats, bigints, decimals and bytes written through the writer read back as written, on the writing device and in rdb.
- **Write semantics.** One case per row of the rules table:
  - an omitted column with a default is written with the default, in the row and the op;
  - a column given `undefined` is treated as omitted;
  - an omitted required column throws `missing-required`;
  - an insert's `null` reads `NULL` on the writing device, in rdb and on a second device, also in a column with a default. The captured change carries the `null`, and the op carries it only for the column with a default;
  - an update to `null` of a column that has a value clears it, and a second device reads `NULL`. With `compare`, one of a column that is `NULL` resolves `true`, writes nothing and captures nothing;
  - a `null` for a column that isn't nullable throws `not-null`, in an insert and in an update, also where the column has a default;
  - an update that repeats a row's value writes and captures it, and with `compare` it writes nothing and fires no commit event;
  - a form saves every field of a row it read, with `compare`, while another device clears a field the form didn't change: after both sync, the field is `NULL` on both devices;
  - an update with a row from a query spread into it lands, and with `compare` writes only the changed column. One with a different id or author is refused;
  - an update of one json column doesn't write the others;
  - an update with a readonly column at another value is refused whole;
  - an update or delete of a missing id writes nothing, captures nothing, fires no commit event and resolves `false`. One of a row the table has resolves `true`, also for an update that changes no column, and for an update given no columns.
- **Key values.**
  - `{ pk }` interns the key and reads back as its id. Interning the same key twice gives the same id.
  - The ingested op carries the key hash rdb computes, on a SHA-256 replica and on a BLAKE3 one.
  - An identity-provider row written with `{ pk }` is accepted by rdb.
  - A bare string throws. So does a `{ pk }` holding a key hash, a key of an unknown suite, or a key of the wrong length.
  - A `{ pk }` whose base64 isn't canonical, and a key id the key table doesn't have, throw `invalid-key`.
  - A `{ pk }` in a json column is stored as json.
  - A `null` in a nullable key column reads `NULL`.
  - A `{ pk }` in the author column throws `author`, also for the writer key.
  - A refused write with a `{ pk }`, and a write with one to a missing row, leave the key table unchanged.
- **Authorship.** One case per cell of the author table, under both settings, in a group with an identity provider and in an anonymous group. Each asserts the signer in rdb, and the author column on the writing device and on a second one.
- **Authorship, `autoAuthor` off.**
  - An insert followed by a delete, both before ingestion, is signed as its author column said.
  - After a crash, an insert already appended keeps its signer and its row hash.
  - An `INSERT OR REPLACE` over a row inserted on this device, naming another author, is reverted to rdb's row and logged, and rdb gets no second row.
- **Native writes of the author column.** In a group with an identity provider, under both settings, an insert with another key's id is rejected and reverted, and logged as an op event, and an update of the column is rejected and reverted. An insert that leaves the column out, or sets it to `NULL`, is signed under `on` and anonymous under `off`. An IndexedDB `put` over an existing record that leaves the column out, and one that gives it `null`, keep the stored author and capture nothing for it. In an anonymous group, a native insert that names a key, the writer key's included, is rejected and reverted, and logged as an op event.
- **Refused writes.** An unknown table or column, the id column in an insert, a value of the wrong JSON type, a json value with a `null` inside it, and another author in an update each throw a `WriteRefusedError` with its reason. Nothing is written and no commit event fires.
- **A voided delete.** A delete made void by a concurrent change: the row returns under the same local id, and the op event is `void` with kind `delete`.
- **Observer targets.** A second target, opened the way a client process opens one, sees each of these through its queries and its writer: a column the host adds, a column the host drops, and a table the host creates after the second target opened. On IndexedDB, the case runs once more with a second target that has no commit listener.
- **Writers that cannot replicate.** `openWriter` throws on a target without capture, and a query source over a target that can't write has no `openWriter`. A write to a group with an identity provider, on a target where no projection has recorded a writer key, throws `no-writer-key`, also on an IndexedDB target opened before the projection first recorded one, and so does `writerKeyId()`. A write to an anonymous group there is captured, and ingested without a key. A write after `setCaptureEnabled(false)` throws `no-capture`. None of them writes anything.
- **Row identity.** `rowHash` is undefined right after an insert, and `rowHash` and `localId` round-trip after ingestion.
- **Table names.** `tableNames` gives each group's projected names, through the projection and through a second target opened the way a client process opens one, also for a table a schema change adds later, and also on a projection opened without a writer key. An unknown member name throws.
- **A race with the host.** On a SQLite file, writer calls succeed while another process commits to the same file in a loop.

Elsewhere:

- **Today's behavior first.** The cases of *One write, three meanings* go in as tests of today's behavior. The change that fixes each one flips its test.
- **The existing ingestion suite** runs on the writers. A few of its cases keep writing through raw SQL and the raw IndexedDB facade, since those paths stay supported and the suite is their only coverage. It gains these cases:
  - A raw-SQL insert, and an IndexedDB `put`, that reuses a deleted row's local id is removed from the target and logged as a failure op event.
  - An `INSERT OR REPLACE` over a live row gets rdb's values and author back and logs a failure op event, for a row that came from rdb and for one inserted on this device.
  - A replayed insert of a deleted row whose local row is gone was dropped and logged nothing. HIP-29 removes replays, and this case with them.
  - A native update that sets a column to the value it has, `NULL` included, captures nothing, on all three targets. HIP-29 reverses this: the update records every column it names.
  - A native insert on memory and IndexedDB that leaves out a column with a default reads the default at once, and still after it syncs. One that gives the column `null` reads `NULL`.
  - A json value with a `null` inside it, written natively, is rejected and reverted with a failure event, and the same pass ingests the other changes.
  - Two rejected updates of a readonly column of one row, in separate passes, log two failure events. After a crash, each failure is logged once.
  - A native update that sets two readonly columns of one row logs two failure events. Under HIP-29 the update is refused whole, with one failure event.
  - Project-back writes `NULL` to the author column of a row rdb has no author for: after a refused native change of the column, and after a later remote update of the row.
- **Target-specific cases.**
  - **SQLite.**
    - A native change of the id column fails at the statement, also through `rowid` and with capture paused. `SET id = id` passes and captures nothing, and ingestion and project-back run as before.
    - A native write fails at the statement when it puts a value other than `0` or `1` into a boolean column, or text that isn't json, or the json text `null`, into a json column. SQL `NULL` and json text in another form sync.
    - The checks and the id guard survive:
      - an added column;
      - a rebuild, for a `NOT NULL` tighten or a foreign-key column drop;
      - a plain column drop.
    - With SQLite's checks switched off (`PRAGMA ignore_check_constraints`), a value the drain can't read is refused with a failure op event, and the same pass ingests the other changes.
    - A second handle on the same file follows the first one's schema changes. A new json column drains as json, and added and dropped columns and new tables show in its queries.
    - A rolled-back `apply` leaves no meta behind.
  - **IndexedDB.** A listening target can open a table and an index that another target created, as soon as that apply is delivered. A reconnect refreshes a target with no listener.
  - **Memory.**
    - A query returns `null` for each column a row has no value in, as SQLite and IndexedDB do.
    - `localInsert` refuses a value for the id column, and `localUpdate` refuses one other than the row's own id. A query row always shows the row's local id.
  - **The planner.** A column the drain couldn't read is refused before any other check, and an update's other columns are still planned. Under HIP-29 the whole update is refused.
- **rdb_projection.**
  - A table that a schema change adds after the projection opened is projected with its group's prefix. A foreign key into it from another group is co-projected as an id, and a local insert into it is ingested.
  - An insert through `projection.openWriter()` is ingested with the writer key, and project-back leaves its author column unchanged.
  - With `autoAuthor` off, an anonymous insert has no author in rdb.
  - A group with an identity provider, `autoAuthor: false` and `authorColumn: false` makes the projection throw at open, and on a reconfigure that adds it, and the target is left unchanged.
  - A projection whose default is `autoAuthor: false` opens over an anonymous group that has no author column, and records the group's mode as `anonymous`.
  - A table that a schema change adds after the projection opened is written with its group's authorship mode.
  - After the projection reopens with another `autoAuthor`, a writer in another process sees the new mode at its next write.
- **rhost.** A host config that sets `autoAuthor` loads, and the projection gets it.
- **rhost clients.** On a BLAKE3 replica, `registerKey` returns the id of the key hash rdb computes.
- **rdb_react.** `useWriter()` throws when the provider's source cannot write. An insert through it re-renders a `useQuery` over that table.

## Considered and not taken

- **SQL as the portable write language.** Parse `INSERT`, `UPDATE` and `DELETE`, then run them on IndexedDB and memory. That means a SQL dialect to define, parse and keep in step across three targets, for statements that bindings build programmatically anyway. Reads chose a builder over SQL in HIP-5 for the same reason.
- **Batches that are atomic only in the target.** An earlier draft had `batch`, written in one target transaction and then ingested change by change. One op could be rejected while the rest landed, or one could be voided later. The name and "all or nothing" would read as a transaction the replica does not keep. An atomic batch needs the outbox to record it, and that is open question 1.
- **Callback transactions with reads and `await`s inside** (`transaction(async (tx) => …)`). IndexedDB ends the transaction at the first unrelated `await`, and better-sqlite3 transactions are synchronous.
- **Writing to the replica directly, as the general path**, with `RTable` ops instead of the projection. That loses the optimistic row and the single ingest path, and it needs the replica in process, which tabs and client processes don't have. It stays the path for atomic writes in process.
- **A bare string as a public key in key columns.** JavaScript code passes strings from forms, URLs and decoded payloads. With a bare string, a mistake would reach a parser that accepts any bytes. `{ pk }` turns it into a type error.
- **A suite field next to the public key.** The serialized key already names its signing suite, and a second field would need a rule for when the two disagree. The hash suite isn't a property of the key. It belongs to the replica, and the projection records it.
- **The hash suite only in the host's config, passed to the projection.** The projection already gets it from its replica, which in rhost is the host's. A writer is opened from a target and can't reach the host, so the target is the one place every writer can read it.
- **`openWriter({ author })`.** Only one id was ever right, the writer key's, and the writer could not tell a wrong one. The recorded key id and `autoAuthor` replace it.
- **`autoAuthor` per writer.** Ingestion cannot tell which writer made a change, so the setting would have to travel with each change in the outbox. It is set per group, and applies the same way to native writes.
- **Refusing `autoAuthor` for an anonymous group.** The setting has nothing to decide there, and a group's identity provider never changes. But `autoAuthor` is usually one default for the whole projection, so refusing it would force an override on every anonymous group.
- **Authorship modes kept per table**, written after every sync. Every table a schema change creates, and every reconfigure, would need a write, and SQLite's cached meta, which reloads only when the schema version changes, would miss a change of mode. The mode belongs to a group, so it is recorded per group, and each table records its group.
- **A fallback hash, from the failed op's text, for a failure whose op can't be hashed.** Ingestion refuses such a value before rdb sees it, so the failed op it records can always be hashed. A failure's hash also covers the ids of its captured changes, which is what keeps distinct failures apart.
- **Not projecting the author column for anonymous groups.** It would be cleaner, but project-back already keeps the column `NULL` on every device. It also makes the column's presence depend on the group, which a binding over several groups would have to check.
- **Reading an insert's author from the row at ingestion**, instead of capturing it. By then the row may be deleted or its author column changed, and a pass made again after a crash could sign differently from the first attempt.
- **Telling bindings not to spread a row into an update.** A form that saves every field of a row it read does exactly that. The writer sets aside the id and author columns it would trip on, and ingestion the readonly ones (HIP-29).
- **An insert's `null` as "not given"**, the rule of an earlier draft, from when rdb had no `NULL`. A form could send `null` for an empty field and get the default. But SQL, the native paths and rdb all read an explicit `NULL` as `NULL`, also where the column has a default, so the writer would be the one path where `null` meant something else.
- **A `NULL` replaced by the column's default on SQLite**, with `NOT NULL ON CONFLICT REPLACE`. It overrides SQL's rule that an explicit `NULL` wins over the default, which rdb follows, and it would turn an update to `NULL` into a reset to the default. A `NULL` where the column doesn't allow one fails, as SQL has it.
- **An IndexedDB facade that merges every `put`**, keeping the columns a record leaves out. A `put` would no longer replace a record, as IndexedDB defines and the IndexedDB README documents (`rdb_adapter_idb/README.md:95`, `:97`). Only the author column is kept, because no op can change it. HIP-29 adds `patch` beside `put` instead, a partial update that writes only the fields it is given.
- **The writer refusing an FK to a row that never existed.** Deciding that exactly needs the referenced table's rows, its sync table and the pending outbox. Ingestion already rejects such a reference with an op event, so FK validity stays with it, and the shape keeps the FK columns without their target tables.
- **Per-target adapters in each binding.** Every binding would carry the three implementations the tests carry today.
- **Removing the native write paths.** SQL on SQLite is useful for set-based updates and for tools such as the REPL. The IndexedDB-shaped API is what the writer is built on.

## Open questions

1. **Atomic batches, in a later HIP.** A batch of writes could be ingested as one rdb bundle, accepted or rejected as one, and voided as one. What it would take:
   - **The outbox records the batch.** Every captured change gets a batch id, `null` by default. On memory and IndexedDB, the writer sets it. On SQLite, the capture triggers read it from a one-row value that the writer sets at the start of its transaction and clears at the end, the way the `applying` flag works (`sqlite_target.ts:517-521`). Native writes leave it `null` and behave as today.
   - **One batch, one bundle.** The planner makes the changes of one batch into one bundle, with no fallback to single ops. It first collapses each row's changes into one op, because a bundle writes a row once and needs a row it updates or deletes to exist before it (`validate_ops.ts:536`, `:552`). An insert then an update becomes one insert. An insert then a delete becomes nothing. An update then a delete becomes a delete.
   - **Rejection and voiding.** When rdb rejects the bundle, every op gets a failure and every row is reverted. A bundle that is voided later reverts as a whole too.
   - **One group per batch.** A bundle is an entry in one group's DAG, and no op in it can touch a foreign group (`validate_ops.ts:509-510`). A batch over two groups cannot be one entry, because the second group has to observe the first between them. The writer would refuse such a batch, so that "a batch is all or nothing" holds without exceptions.
   - **Rules see the state before the batch.** Inside a bundle, an FK can point at a row inserted earlier in the same bundle, but `ALLOW` rules read only the state before the bundle (`validate_ops.ts:556-557`). A batch that grants a permission and then uses it is rejected whole, so it takes two batches.
   - **One signer.** A bundle is signed once, so with `autoAuthor` off a batch's inserts must share an author.
   - **Ids during the batch.** The batch is built by a synchronous callback, for the same reasons as above. Its inserts can still return real local ids while the callback runs, so no placeholder type is needed:
     - on SQLite, the callback runs inside `db.transaction`;
     - on IndexedDB, the writer reads the table meta records that allocate ids at the start of the transaction, and allocates from them (`idb_target.ts:443-453`);
     - memory is synchronous.
   - **Crashes.** The end of a crashed pass finds a bundle that was already appended as HIP-29 finds any entry: by the hash recorded in its changes' outbox rows.
2. **Typed rows (deferred).** `RowWriter` and `TableQuery` could be generic over a row type, giving bindings compile-time checks. That probably belongs in its own HIP, together with generating row types from schemas.
3. **Several identities, and anonymous updates and deletes, in a later HIP.** Each captured change would carry its signer, a key id or none, set by the writer. On SQLite, that would use a one-row value like the batch id's. The host would hold several keys in its config and keystore, the target would list the ones it can sign with, and ingestion would sign each op with its change's key. A change whose key the host no longer holds would be rejected, rather than blocking the outbox. The whole design shares the target as its trust boundary: anyone who can write the target can sign as any key the host holds, as they can sign as the host key today.
4. **A new row's hash before ingestion (deferred).** Code that puts a new row into an invite or a link needs the row's hash, which is unknown until the row is ingested. A function could wait for the row's ingestion and return its hash, or reject when the insert is refused or voided. When it can't wait, for example while the host is stopped, it would mint the row's uuid and save it in the insert's outbox row, and ingestion would use that uuid instead of minting one. Under HIP-29, a pass that builds the entry again after a crash then reuses it too. Capture fills in the author at insert (HIP-29), so the row hash, derived from the uuid and the author, is known as soon as the uuid is.
5. **Client helpers.** rhost_client_node and rhost_client_browser could open the target, its query source and its writer together, for example `openProjection(dir)`. That would also be the natural place to forward the host's commit stream into the target, which the meta refresh relies on and which callers wire by hand today.
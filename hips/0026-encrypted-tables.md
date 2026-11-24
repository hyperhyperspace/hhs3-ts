# HIP-26: Encrypted tables

- **Type:** design
- **Status:** draft
- **Modules:** rdb, rdb_lang, rdb_adapter, rdb_projection, crypto
- **Requires:** 28
- **Superseded by:** —
- **Implemented in:** —

## Summary

Every replica of an rdb database can read every row it holds, so a database is private only as far as its hosts choose whom to sync with. This HIP adds encrypted tables. A column marked `ENCRYPTED` is stored as ciphertext that only the table's *audience* can read: a set of identities that share keys, as [HIP-28](0028-audience-keys.md) defines it. The audience can be the same for every row, or depend on a column of the row, so that one table holds many private conversations. The rows of each audience live in its own history, its *part*, synced only among its members and the database's relays ([HIP-27](0027-relays.md)). The audience's keys decide who reads what: a removed member cannot read what is written after it left, and a member added later reads the past or only what follows, as the audience declares. Rules never read encrypted values, so a host that cannot decrypt reaches the same verdicts as a member. Encrypted FILES and padding are left for later.

## Why

### Background: what a replica reads today

- **Objects and entries.** A database is made of objects: the database itself, its catalog, its schemas, its table groups and its FILES. Each object is a DAG of signed entries. Sync copies entries between replicas, and every replica checks every entry it receives.
- **Table groups.** The tables of a group share one history: one position is a snapshot of all of them, and a write to several of them is one entry. A group pins a version of its schema. It observes other groups at versions it moves forward with *ref-advance* entries, and its rules read those groups at the observed version.
- **Columns.** A column can be `PUB`, which lets rules read it, and `READONLY`, which fixes its value at insert. Rules such as `ALLOW insert IF EXISTS (user.caps WHERE ...)` read only `PUB` columns.
- **Admission and use.** Each entry is checked twice. *Admission* checks it at its own position, against the entries in its past and nothing else, so every replica reaches the same answer and it never changes. An entry that fails admission is refused. *Use* re-checks an admitted entry against the concurrent entries visible from a later position: a write whose permission was revoked concurrently becomes *void*. A void entry is kept, but does not count.
- **Identities.** An identity is a signing key. A group's *identity provider* is a table that maps key ids to public keys.
- **Hosts.** A host is one database on one device. Its app's *allow list*, read from a column of the database, says which peers it gives entries to.

So every replica that holds a group stores its rows in plaintext, checks them, and projects them into a local SQL database.

### Background: audience keys

[HIP-28](0028-audience-keys.md) gives a set of identities a shared key. The terms this HIP takes from it:

- **Audience.** A set of identities read from a table, declared in a schema: `AUDIENCE readers AS (user.caps.grantee WHERE user.caps.label = 'reader')`. An audience can take a *key*, a value such as a conversation's id, as in `participants.who WHERE participants.conv = $key`. Each key value is then its own audience.
- **Home group.** The table group whose schema declares the audience.
- **Part.** The object that belongs to one audience: one for an audience without a key, one per key value otherwise. It has its own DAG and holds the audience's key history. It observes its home group as a group observes a group it binds.
- **Epoch.** A span of a part's history during which one secret is in use. It begins with a *rotation* entry, which delivers the secret to each member of the audience, encrypted to a key the member publishes. The *holders* of an epoch are those it was delivered to.
- **The epoch rule.** Admission refuses an entry encrypted under an epoch that someone outside the audience holds. So the first writer after a removal rotates first, in the same entry.
- **History.** Under `HISTORY SHARED` (the default) a newcomer is given every earlier epoch. Under `HISTORY JOINED` it is given only the epochs that begin after its add.

### What this rules out

- **Whoever holds a database reads all of it.** mb-chat's next step is an always-on host on a server, so that joining and syncing don't need a member online. That host would read every message. So does anyone with a backup copy of a replica or a lost device, and so would any future replica kept by a cloud provider.
- **No private subsets inside a database.** Every member that holds a group reads every row of it. mb-chat's feature list plans direct messages as "probably one small database per conversation": a deploy, identity rows and rules for every conversation, with nothing tying them to the server the conversation belongs to. Private channels have the same problem.
- **A removal only stops serving.** A removed member can still read what is written after its removal, if a copy reaches it. HIP-28's keys close that, for whatever is encrypted under them.

### Prior art

HIP-28 surveys the systems that encrypt shared data for a group: p2panda, Keyhive, DCGKA, MLS, Keybase teams, Jazz and Matrix. Two of their lessons show in tables. A newcomer can read the past only if old keys are kept, so there is no forward secrecy. And a write concurrent with a removal stays readable to the removed member.

## Design

### Declaring an encrypted table

An audience without a key gives a table one part:

```sql
CREATE SCHEMA notes AS (
  AUDIENCE readers AS (user.caps.grantee WHERE user.caps.label = 'reader'),

  TABLE notes (
    title  string ENCRYPTED,
    body   string ENCRYPTED,
    pinned boolean
  ) ENCRYPTED FOR readers
    ALLOW insert IF EXISTS (user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author)
);
```

An audience with a key gives one part per value of a column. Direct messages in mb-chat:

```sql
  TABLE participants (
    conv string PUB READONLY,
    who  identity PUB READONLY
  ) ALLOW insert IF EXISTS (user.caps WHERE user.caps.label = 'member' AND user.caps.grantee = $author)
    ALLOW delete IF who = $author
                 OR EXISTS (participants AS p WHERE p.conv = $row.conv AND p.who = $author),

  AUDIENCE conversation AS (participants.who WHERE participants.conv = $key) HISTORY SHARED,

  TABLE dm_messages (
    conv   string PUB READONLY,
    body   string(4000) ENCRYPTED,
    sentAt integer READONLY ENCRYPTED
  ) ENCRYPTED FOR conversation(conv)
    ALLOW insert IF EXISTS (participants WHERE participants.conv = $row.conv AND participants.who = $author),

  TABLE dm_reactions (
    conv      string PUB READONLY,
    messageId string PUB READONLY REFERENCES dm_messages,
    emoji     string(64) READONLY ENCRYPTED
  ) ENCRYPTED FOR conversation(conv)
```

The rules:

- **An `AUDIENCE`** is declared as HIP-28 says.
- **An `ENCRYPTED` column** cannot be `PUB`, a foreign key, or a column of an identity provider: rules, foreign keys and lookups read those, and every replica must be able to. It can be `READONLY` and `NULL`. Its type and constraints are checked by the members who decrypt it; see *Reading*.
- **A table `ENCRYPTED FOR`** an audience with a key names its key column. That column is `PUB READONLY`, and admission checks that its value is the key of the part the row is written in. A table with `ENCRYPTED` columns must be `ENCRYPTED FOR` an audience.
- **Tables for the same audience share its parts.** A message and its reactions in one conversation are in one part. They can be written in one entry, and one can reference the other.
- **References.** A table in a part may reference rows of the same part, and of the home group and the groups it observes. No table outside a part may reference rows inside it.
- **Plaintext columns** are allowed in an encrypted table (`pinned` above). Every holder of the part sees them, and rules read them when they are `PUB`.

### Rows in parts

The rows of an encrypted table are written in the parts of its audience, beside the audience's key history.

- **History is per part.** Each part has its own frontier, verdicts and last-writer-wins. A view of an encrypted table is one position per part, as a view of a database is one position per group.
- **A part reads its home group** at the version it observes: the table definitions, the tables its rules read, and its own audience.
- **Rules as today.** Row operations in a part are checked by their table's rules, at admission and at use, like the rows of a group checked against a group it observes. A participant whose message is concurrent with their removal sees that message voided once the part observes the removal, as today.
- **Writes are atomic within a part only.** Starting a conversation (its `participants` rows, in the home group) and posting its first message (in its part) are two writes.

### Values

Each encrypted value is encrypted with a key derived from its epoch's secret and a random salt carried in the entry. The encryption binds the part, table, row, column, author and epoch as associated data, so a ciphertext copied to another place fails to decrypt. The entry names its epoch in plaintext. A delete carries no values and needs no epoch.

An entry that carries encrypted values is under HIP-28's epoch rule. A writer whose past contains a removal rotates first, in the same entry.

### History

What HIP-28's two histories mean for rows:

- **`HISTORY SHARED`** (the default). A newcomer reads every value ever written in the part: superseded values and deleted rows included.
- **`HISTORY JOINED`**. Rows from before a newcomer's add show with their encrypted values unreadable.
- **Adding someone back.** Under `SHARED`, a member removed and added back reads what was written while it was away. Under `JOINED`, it reads nothing from the gap.

### Reading

- **Members decrypt.** A member decrypts each value with the epochs it holds, then checks it against its column's type and constraints. The projection shows the decrypted values.
- **Unreadable values.** A value the member cannot decrypt projects as `NULL`, and the row is marked unreadable. That covers rows from before its add under `JOINED`, an epoch not delivered yet, and a malformed value. When a delivery arrives, the delta reports the rows as updated and the values project.
- **A malformed value never voids anything.** Replicas that cannot decrypt cannot see it, so it cannot enter a verdict. Every member decrypts the same ciphertext under the same secret, so every member sees the same malformed value and projects it the same way.
- **Last-writer-wins picks entries, not values.** The winning write for an encrypted column is chosen as today, by position and hash. A member that cannot read the winner shows `NULL`. It does not fall back to an older value.

### Sync

Parts sync as HIP-28 says: a topic per part, served to the members of its audience. A relay (HIP-27) holds parts without being in their audience. It checks their entries and stores the values it cannot read.

The home group syncs as today. Its audience rows are plaintext: who is in which conversation, and when they joined or left, is visible to everyone who holds the home group.

### Outcomes

What members read in a conversation. HIP-28's *Outcomes* cover the keys behind each row, and the cases that are about keys alone: concurrent removals, an add that turns out void, and concurrent rotations.

| # | What happens | Result |
| :--- | :--- | :--- |
| 1 | Alice and Bob have a conversation. An always-on host is a relay. | Alice and Bob read it. The relay stores, checks and serves its entries, and cannot read the values. |
| 2 | Alice adds Carol, under `SHARED`. | Once Carol is delivered the current epoch, she reads everything since the conversation began. |
| 3 | Alice adds Carol, under `JOINED`. | Carol reads from the next write on. Earlier rows show with their values unreadable. |
| 4 | Alice removes Bob. | Bob reads nothing written by a member who had seen his removal. |
| 5 | Carol is offline and keeps writing while Alice removes Bob. | Carol writes under the old epoch, and Bob can read those writes. They stay live, since Carol may still write. |
| 6 | Bob writes concurrently with his removal. | As today, his writes are void once the part observes the removal. |
| 7 | A member writes a value that does not decrypt, or decrypts to the wrong type. | Every member sees the same bad value. It projects as `NULL` and the row is marked. Verdicts are the same at members and relays. |
| 8 | Rows reach a member before the delivery of their epoch. | The values project as `NULL` and the rows are marked. When the delivery arrives, the delta reports them as updated. |
| 9 | A member restores its replica from scratch, or starts a second device with its key. | It syncs the part with its deliveries and reads everything its identity may read. |

### What stays as today

- Tables without `ENCRYPTED`, their rules and their verdicts.
- FILES. Encrypting them is later work.
- A group without an identity provider cannot declare an audience, and so no encrypted table.
- The local projection holds plaintext. Encrypting a replica at rest is a separate question.

## Why it is sound

**Verdicts do not depend on keys.** Everything that decides whether an entry counts reads plaintext: authors and signatures, `PUB` columns, the audience at a fixed position, epoch ids, and who each delivery is for. No rule, foreign key or lookup reads an encrypted value. A relay that cannot decrypt and a member that can reach the same verdicts, so they agree on which rows are live and on every delta. They differ only in whether they can show a live value.

**What a departed member reads.** HIP-28 bounds it: a member outside the audience reads a value only if the value's writer saw it in the audience, or, under `SHARED`, if it is added back later and given the history.

**Limits.**

- **No forward secrecy**, and post-compromise security only from rotations, as HIP-28 says.
- **Metadata.** Holders of a part see who writes, when, and how much, and the plaintext columns. Holders of the home group see the audience's rows.

## Changes

In the order they land, after HIP-28:

1. **crypto.** Value encryption with associated data.
2. **rdb, schemas.** The `ENCRYPTED` column flag and `ENCRYPTED FOR` on tables, with the checks listed under *Declaring an encrypted table*. Incarnations cover them as they cover any column or table.
3. **rdb, parts.** Row operations in a part under their tables' rules, and the check of the key column at admission.
4. **rdb, writing and reading.** Encrypting values on write, rotating first when the epoch rule needs it. Decrypting on read with the replica's keystore. A view of an encrypted table across its parts, and deltas when a delivery makes values readable.
5. **rdb_lang.** The syntax, `SELECT` across the parts a replica holds, and the dump.
6. **rdb_adapter, rdb_projection.** The projection across parts, `NULL` for unreadable values, and the unreadable mark.
7. **Docs.** One section of the rdb README, *Encrypted tables*, that explains all of it and links to *Audiences and keys*. rdb_lang and rdb_adapter link to it.

## Tests

- **Each row of *Outcomes***, run with two or three members and a host outside the audience.
- **Hosts with and without keys agree.** The same entries, given to a replica with keys and to one without: the same live rows and the same deltas, apart from the values.
- **The epoch rule on rows.** A row written under an epoch a removed member holds is refused.
- **Associated data.** A ciphertext copied to another row, column or part fails to decrypt.
- **Declarations.** An `ENCRYPTED` column that is `PUB` or a foreign key, a rule that reads an encrypted column, a reference into a part from outside it, and a key column that is not `PUB READONLY` are refused.

## Considered and not taken

- **Encrypted rows in the home group's history.** No new object type, and writes stay atomic with the group's other tables. But every holder of the group would store every conversation's ciphertext and see its activity, and the keys would share a history with unrelated writes.
- **One database per conversation.** mb-chat's current plan. Every conversation needs its own deploy, identity rows and rules, and nothing ties it to the server's members.
- **An audience written inline on each table** (`ENCRYPTED FOR (participants.who WHERE ...)`). It cannot say that two tables share parts. It could come back as shorthand for an audience that only one table uses.
- **Keys of its own.** The key layer could live in this HIP, for tables alone. Blind relays and the membership sidecar need the same keys with no table, so it is HIP-28.

## Open questions

- **The unreadable mark.** One flag per row, one per encrypted column, or a side table in the projection.
- **Encrypted FILES.**
- **Padding.** Values are encrypted at their own length. Padding them to fixed sizes hides lengths, at a cost in space.

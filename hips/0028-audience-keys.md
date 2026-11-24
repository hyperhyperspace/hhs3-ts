# HIP-28: Audience keys

- **Type:** design
- **Status:** draft
- **Modules:** rdb, rdb_lang, crypto, sync, rhost, rpack
- **Requires:** —
- **Superseded by:** —
- **Implemented in:** —

## Summary

Nothing in a database is secret from a host that holds it. Three proposals need a secret that only a set of identities shares, and that changes when the set does: encrypted tables ([HIP-26](0026-encrypted-tables.md)), blind relays ([HIP-27](0027-relays.md)), and the second level of the membership sidecar ([HIP-24](0024-membership-sidecar.md)). This HIP adds that secret once, for all three. An *audience* is a set of identities read from a table, written like the condition of an `EXISTS`. Each audience has an object of its own, a *part*, that holds its key history: a chain of *epochs*, each with a secret sent to every member encrypted to a key the member publishes. The first writer after a removal starts a new epoch, so whoever left cannot read what is written later. A member added later is given the past (`HISTORY SHARED`, the default) or only what follows the add (`HISTORY JOINED`). Every check reads plaintext only, so a host without the keys reaches the same verdicts as a member. What is encrypted under the epochs is left to the HIPs that build on this one. Forward secrecy and keys per device are left for later.

## Why

### Background

- **Objects and entries.** A database is made of objects: the database itself, its catalog, its schemas, its table groups and its FILES. Each object is a DAG of signed entries. Sync copies entries between replicas, and every replica checks every entry it receives.
- **Table groups.** The tables of a group share one history: one position is a snapshot of all of them, and a write to several of them is one entry. A group pins a version of its schema. It observes other groups at versions it moves forward with *ref-advance* entries, and its rules read those groups at the observed version.
- **Columns.** A column can be `PUB`, which lets rules read it, and `READONLY`, which fixes its value at insert. Rules such as `ALLOW insert IF EXISTS (user.caps WHERE ...)` read only `PUB` columns.
- **Admission and use.** Each entry is checked twice. *Admission* checks it at its own position, against the entries in its past and nothing else, so every replica reaches the same answer and it never changes. An entry that fails admission is refused. *Use* re-checks an admitted entry against the concurrent entries visible from a later position: a write whose permission was revoked concurrently becomes *void*. A void entry is kept, but does not count.
- **Identities.** An identity is a signing key. A group's *identity provider* is a table that maps key ids to public keys.
- **Hosts.** A host is one database on one device. Its app's *allow list*, read from a column of the database, says which peers it gives entries to. Hosts find each other by an object's id, its *topic*, which they announce to a tracker. Connections between hosts are encrypted.

So a database is private only as far as its hosts choose whom to sync with. Whoever holds a copy reads all of it.

### What needs a shared secret

- **A removal only stops serving.** Peers stop giving entries to a removed member. Everything it already had stays readable to it, and nothing can change that. But what is written after the removal is readable too, if a copy reaches it, for example through a host that has not yet seen the removal.
- **Encrypted tables** (HIP-26) need a key that only a table's readers hold, and one per conversation where a table holds many.
- **Blind relays** (HIP-27) hold a private database as ciphertext while its members are away. They need a key that every member holds and the relay does not, and the keys of the conversations inside the database.
- **The membership sidecar** (HIP-24) needs, at its second level, "a secret replaced at every removal and delivered to the remaining members", and nothing in the system does that today.

All three ask for the same thing. If each made its own keys, a removal would have to be acted on three times, by three rules that could disagree about who is out. One key layer gives one answer.

### Prior art

| System | What it does | What this HIP takes |
| :--- | :--- | :--- |
| [p2panda](https://github.com/p2panda/p2panda) (`p2panda-auth`, `p2panda-encryption`, `p2panda-spaces`) | A permissions CRDT (`Pull < Read < Write < Manage`) decides who gets the keys of each *space*. Its [Data Encryption scheme](https://github.com/p2panda/p2panda/blob/main/p2panda-encryption/README.md) keeps one secret per epoch, replaces it when a member is removed, and gives a newcomer every earlier secret. Its Message Encryption scheme ratchets a key per sender (DCGKA): forward secrecy, and no history for newcomers. Keys go to each member separately. Only causal delivery is needed. | Membership read from permission data, a new secret per removal, one key history per audience, delivery to each member, and the split between data and messages. |
| [Keyhive](https://github.com/inkandswitch/keyhive) (Ink & Switch) | Capabilities with `Relay < Read < Edit < Admin`. A tree of keys ([BeeKEM](https://eprint.iacr.org/2026/1434)) that needs only causal delivery, with post-compromise security and, by choice, no forward secrecy. Each encrypted block carries its ancestors' keys. | Giving up forward secrecy for data that newcomers must read. |
| [DCGKA](https://eprint.iacr.org/2020/1281) (Weidner, Kleppmann, Hugenroth, Beresford, CCS 2021) | Group key agreement with no central server, from causal delivery and two-party channels. | Causal order is enough. |
| [MLS](https://www.rfc-editor.org/rfc/rfc9420) and its [decentralised drafts](https://datatracker.ietf.org/doc/draft-kohbrok-mls-dmls/) | A tree of keys advanced in epochs that need a total order. The drafts tolerate forks at a cost in forward secrecy, or, like [Marmot](https://github.com/marmot-protocol/marmot) on Nostr, apply one of two competing commits and drop the other. | Why not MLS: in a DAG every merge is a fork. |
| [Keybase teams](https://book.keybase.io/docs/teams/crypto) | A key per generation, sent to each remaining member at a removal. Each new key encrypts the previous one, and rotation can wait for the next writer. | The chain of epochs, and rotation by the next writer. |
| [Jazz](https://jazz.tools/docs/react/reference/encryption) | A group read key, replaced at a removal and revealed to the remaining members. | The same model, used in production. |
| Matrix | Keys per sender, shared over two-party channels. A room's `history_visibility` (`world_readable`, `shared`, `invited`, `joined`) says what a newcomer reads. Keys drifting apart from membership across servers is its chronic failure ("unable to decrypt"). | `HISTORY SHARED` and `HISTORY JOINED`, and keys that follow membership by a deterministic rule. |

Two lessons hold across all of them:

- **History or forward secrecy, not both.** Forward secrecy means old keys are deleted, so a newcomer, a new device or a restored replica cannot read the past. Every design above that lets newcomers read the past gives up forward secrecy and keeps post-compromise security: whoever steals a key, or is removed, is shut out by the next new key.
- **Writes concurrent with a removal stay readable to the removed member.** A writer who has not seen the removal uses the old key. No design without coordination avoids it.

## Design

### Terms

- **Audience.** A set of identities that share keys: the values of an identity column over the rows a condition selects, such as `user.caps.grantee WHERE user.caps.label = 'reader'`. An audience can take a *key*, a value such as a conversation's id, as in `participants.who WHERE participants.conv = $key`. Each key value is then its own audience.
- **Home group.** The table group whose schema declares the audience.
- **Part.** The object that belongs to one audience: one part for an audience without a key, one per key value otherwise. A part has its own DAG. It holds the audience's key history, and whatever else is written for that audience: HIP-26 puts the rows of encrypted tables there.
- **Epoch.** A span of a part's history during which one secret is in use. An epoch begins with a *rotation* entry. Its id is the hash of its secret, so whoever receives the secret can check it.
- **KEM key.** A public key for encryption (KEM: key encapsulation mechanism). Each identity publishes one, separate from the signing key that identifies it.
- **Delivery.** An epoch's secret, encrypted to one member's KEM key.
- **Holders of an epoch.** The identity that started it, and every identity it was delivered to. Under `HISTORY SHARED`, the holders of a later epoch that carries it also hold it.

### Declaring an audience

A schema declares its audiences by name. Without a key, an audience has one part:

```sql
  AUDIENCE readers AS (user.caps.grantee WHERE user.caps.label = 'reader'),
```

With a key, it has one part per key value. The participants of each conversation in a chat:

```sql
  TABLE participants (
    conv string PUB READONLY,
    who  identity PUB READONLY
  ) ALLOW insert IF EXISTS (user.caps WHERE user.caps.label = 'member' AND user.caps.grantee = $author)
    ALLOW delete IF who = $author
                 OR EXISTS (participants AS p WHERE p.conv = $row.conv AND p.who = $author),

  AUDIENCE conversation AS (participants.who WHERE participants.conv = $key) HISTORY SHARED,
```

The rules:

- **An `AUDIENCE`** reads tables the way an `EXISTS` does: `PUB` columns, equalities combined with `AND` and `OR`, in the home group or the groups it binds. The column it selects holds identities. `$key` stands for the key value. `HISTORY` is `SHARED` (the default) or `JOINED`; see *History*.
- **The home group must have an identity provider.**
- **An audience needs nothing encrypted for it.** It has its part and its keys as soon as it is declared. HIP-26's `ENCRYPTED FOR` puts a table's rows in an audience's parts. HIP-27 and HIP-24 use an audience for its keys alone.

### The database audience

A catalog may name one audience without a key as the *database audience*: the database's members. For an app whose allow list is `user.caps.grantee where label = 'member'`, it reads the same column.

Two designs use it. HIP-27 seals a blind relay's log under its epochs and derives the relay's key from them. HIP-24's second level would take the sidecar's secret from it (see *What builds on this*).

How the catalog names it, and whether the allow list should then be read from it so that the members are written once, are under *Open questions*.

### Parts

- **Computed, not served.** A part's id is computed from the database, the home group, the audience's name and the key value, the way a database's groups are computed from its releases. Every replica that sees a key value derives the same part, with the same first entry.
- **Which replicas hold a part.** A replica holds the parts whose audience contains its own key, at the version of the home group it holds. When a row that sync brings in puts a replica's key in an audience, the replica creates that part and starts syncing it. A relay (HIP-27) holds parts without being in their audience.
- **A part observes its home group** the way a group observes a group it binds: at a version it moves forward with ref-advances. Through that version it reads its own audience, and whatever else the HIPs that use the part read there. Any member of the audience at either version may write a part's ref-advance. A host writes one as soon as its home group holds a change to the audience, so that the keys follow the membership promptly.

### Keys

**KEM keys.** Each identity publishes a KEM public key, signed by its own signing key. The signature matters because identity rows are often written by someone else: in mb-chat a manager writes a newcomer's identity row, and whoever could set a member's KEM key would read everything delivered to it. A member replaces its KEM key when it wants, and senders use the latest one in their past. The suite is selectable, as signing suites are: X25519, ML-KEM-768, or the hybrid of the two, all of which the crypto module provides. The hybrid is the default, because this ciphertext is stored for good, on hosts the members do not control. An identity without a KEM key can be in an audience, but receives nothing until it publishes one.

**Rotation.** A rotation entry begins an epoch. It is written by a member of the audience at the part's position, and carries:

- the epoch's id, the hash of its secret;
- a delivery to every member of the audience at that position that has a KEM key, and to no one else;
- under `HISTORY SHARED`, the secrets of the epochs it follows (the latest epochs in its past), encrypted under its own secret. Whoever holds an epoch can then decrypt every earlier one.

A member may rotate at any time. The epoch rule below says when a writer must.

**Delivery entry.** A delivery entry gives an existing epoch to a member who lacks it: a newcomer, a member that missed a concurrent rotation, or a member whose removal turned out void. Its author must be a holder of the epoch, and its recipient a member of the audience at its position. Under `HISTORY JOINED`, an epoch can be delivered only to a member who was in the audience at the epoch's rotation.

**Request entry.** A member that lacks an epoch, or whose delivery does not decrypt to the epoch's id, writes a request naming it. Any holder can answer with a delivery entry. Several may answer, and the extra deliveries do no harm.

**Using an epoch.** Whatever is encrypted under an epoch names the epoch in plaintext, by its id. What gets encrypted, and how, belongs to the HIP that does it: the values of encrypted columns (HIP-26), the bundles of a blind relay (HIP-27).

### The epoch rule

Admission checks every entry of a part that carries something encrypted under an epoch, from plaintext alone. Let A be the audience at the entry's position, read from the home group's version the part observes there.

1. The entry's epoch is in its past.
2. Every holder of that epoch, as of the entry's past, is in A.
3. Under `HISTORY JOINED`, every member of A that has a KEM key is a holder of that epoch.

Rule 2 makes a removal take effect. A writer whose past contains a removal cannot write under a key the removed member holds, so it rotates first, in the same entry. Rotation therefore happens at the first write that needs it, by whoever writes. A removal by someone outside the audience, such as a server manager who is not in a conversation, works the same way.

Rule 3 makes `HISTORY JOINED` rotate at adds as well, so that a newcomer can read everything written after its add.

Ciphertext that is not an entry of the part, such as a blind relay's bundle, cannot be checked at admission. Its writer applies the same rule to what its own replica holds (HIP-27).

### History

- **`HISTORY SHARED`** (the default). A newcomer is delivered the current epoch, and through the chain every earlier one. It can read everything ever written under the audience's keys.
- **`HISTORY JOINED`**. A newcomer is delivered only epochs that begin after its add, and rule 3 puts every later write under such an epoch. What was written before its add stays unreadable to it.
- **Adding someone back.** Under `SHARED`, a member removed and added back reads what was written while it was away. Under `JOINED`, it keeps the epochs it held before and reads nothing from the gap.

Matrix's other two values are not needed. `world_readable` is data that is not encrypted. `invited` and `joined` are one value here, because an add is the membership. An app that wants invite-then-accept writes both as rows, and its audience reads the accepting row.

### Sync

- **A topic per part.** Each part is an object with its own topic.
- **Serving.** A host serves a part to the members of its audience, as its own replica reads them, and to the relays (HIP-27). The serve and admit rules of [HIP-23](0023-join-bootstrap.md) apply to each part, with the audience in place of the app's allow list.
- **The home group syncs as today.** Its audience rows are plaintext: who is in which audience, and when they joined or left, is visible to everyone who holds the home group.

### Outcomes

Alice, Bob, Carol and Dave are members of an audience. Eve and another identity are managers who can add to it.

| # | What happens | Result |
| :--- | :--- | :--- |
| 1 | Alice adds Carol, under `SHARED`. | The next member to write, or any member answering Carol's request, delivers the current epoch. Carol holds every epoch since the part began. |
| 2 | Alice adds Carol, under `JOINED`. | The next writer rotates to an epoch for Alice, Bob and Carol. Carol holds the epochs from there on, and none from before. |
| 3 | Alice removes Bob. | Hosts advance the part to the removal. The first writer after that rotates. Bob holds no epoch started by a member who had seen his removal. |
| 4 | Carol is offline and keeps writing while Alice removes Bob. | Carol writes under the old epoch, and Bob can read those writes. |
| 5 | Alice and Dave remove Bob concurrently. | Each writer who saw a removal rotates without Bob. Today the two removals void each other ([HIP-22](0022-concurrent-removals.md)), so Bob is still in the audience. He requests the epochs he lacks and is delivered them. If HIP-22 lands, Bob stays removed. |
| 6 | Eve, a manager, adds Frank while another manager concurrently revokes Eve's right to add. | Members who saw the add before the revocation delivered Frank the keys. He reads what was written by members who had not seen the revocation. The first writer who sees it finds Frank holding the epoch but outside the audience, and rotates. |
| 7 | Two members rotate concurrently. | Both epochs stand. A later writer may use either, as long as all its holders are in the audience. Under `SHARED` the next rotation carries both. |
| 8 | A member receives a delivery that does not decrypt to the epoch's id. | It writes a request, and any holder answers with a delivery entry. |
| 9 | A member restores its replica from scratch, or starts a second device with its key. | It syncs the part with its deliveries, and holds every epoch its identity was delivered. |

### What builds on this

- **Encrypted tables** ([HIP-26](0026-encrypted-tables.md)). The values of `ENCRYPTED` columns are encrypted under the epochs of the table's audience, and the table's rows live in the audience's parts.
- **Blind relays** ([HIP-27](0027-relays.md)). A blind relay holds a private database as a log of sealed bundles. Each bundle is sealed under the current epoch of one audience, and the key that lets members write to the log is derived from the database audience's epochs.
- **The sidecar's second level** ([HIP-24](0024-membership-sidecar.md)). HIP-24 leaves level 2 without a mechanism: it "needs a way to deliver each new secret to every remaining member, and a place to keep it that is not the genesis". The database audience is one:
  - its secret is replaced by the first writer after each removal;
  - each new secret is delivered to every remaining member;
  - it is kept in the audience's part, not in the genesis.

  A sidecar entry would compute its tags from the epoch that is current when it is written, and name that epoch by its id. A past member keeps the epochs it held, and so "keeps what it could read when it left", as level 2 says. The sidecar itself can stay a standalone object that anyone syncs. Only its secrets come from the part, which syncs among members.

## Why it is sound

**Verdicts do not depend on keys.** Everything that decides whether an entry of a part counts reads plaintext: authors and signatures, `PUB` columns, the audience at a fixed position, epoch ids, and who each delivery is for. A host that cannot decrypt and a member that can reach the same verdicts.

**The epoch rule is the same everywhere.** It reads the audience at the entry's own position: the entries in its past, and nothing else. Every replica gets the same answer, and it never changes, so every replica refuses the same entries.

**Who can learn a secret.** An epoch's secret leaves the member who started it only through the rotation's deliveries, through later delivery entries, and, under `SHARED`, inside later epochs. Admission refuses each of these unless every recipient is in the audience at the entry's own position. So an honest replica never accepts a hand-off to someone outside the audience as the writer saw it. A dishonest member can always pass on what it can read, outside the system. No cryptography prevents that.

**What a departed member reads.** By rule 2, no entry at whose position X is outside the audience is encrypted under an epoch X holds. So X reads something only if its writer saw X in the audience, or, under `SHARED`, if X is added back later and given the history. The bound is the same whether X left by a removal or by an add that turned out void.

**Why at admission, not at use.** Suppose the epoch rule were checked at use, from later positions. An add that later turns out void would leave its newcomer holding, from outside the audience, every epoch it had received. Every write under those epochs, by every member, would then become void: one bad add would cancel everything written for the audience meanwhile. At admission, the rule asks only what the writer could know.

**Nothing gets stuck.** A member of the audience that lacks an epoch writes a request, and any holder can answer. Nothing waits on one particular member. A writer can always write: when no epoch in its past passes rules 2 and 3, it rotates.

**Limits.**

- **No forward secrecy.** Whoever steals a member's KEM secret reads every delivery to it, and so every epoch it ever held. A new KEM key protects only later deliveries.
- **Post-compromise security comes from rotations.** They happen at removals, at adds under `JOINED`, and whenever a member chooses to rotate.
- **Metadata.** Holders of a part see the recipients of each rotation and delivery. Holders of the home group see the audience's rows.
- **Bad deliveries.** A dishonest member can send some members a wrong secret. The epoch id lets them detect it and request again, but not prove who did it.

## Changes

In the order they land:

1. **crypto.** Sealing to a KEM key (KEM, key derivation and AEAD, in the manner of HPKE, RFC 9180). The suites exist.
2. **rdb, schemas.** `AUDIENCE` declarations, with the checks listed under *Declaring an audience*, and the catalog's database audience.
3. **rdb, identities.** A KEM key per identity, signed by its owner, read without liveness, as signing keys are.
4. **rdb, parts.** A new object type, provisionally `RPart`: its computed first entry, its observation of the home group, rotation, delivery and request entries, and the epoch rule at admission.
5. **rdb, RDb.** Computing a replica's parts from the audience's rows in the home group, and keeping them present and syncing. `catalogStatus` lists them.
6. **rdb_lang.** The `AUDIENCE` syntax, parts in the REPL (`LOG` per part), and the dump.
7. **sync, rhost.** A topic per part, serving a part to its audience, and writing ref-advances and answering requests without being asked.
8. **rpack.** Checking audiences in releases.
9. **Docs.** One section of the rdb README, *Audiences and keys*, that explains all of it. rdb_lang and rhost link to it.

## Tests

- **Each row of *Outcomes***, run with two or three members.
- **Hosts with and without keys agree.** The same entries, given to a replica that holds the epochs and to one that does not: the same entries admitted, and the same verdicts.
- **The epoch rule refuses**:
  - an entry under an epoch a removed member holds;
  - a delivery to someone outside the audience;
  - a rotation by someone outside the audience;
  - a delivery by someone who does not hold the epoch;
  - under `JOINED`, a delivery of an epoch that began before the recipient's add.
- **KEM keys.** A KEM key not signed by its identity is refused.
- **Declarations.** An audience that reads a column that is not `PUB`, or selects a column that does not hold identities, is refused. So is one in a group without an identity provider.
- **An audience with nothing encrypted for it** has its part, rotates and delivers.
- **Suites.** A round trip with each KEM suite, including the hybrid.

## Considered and not taken

- **A key layer per feature.** Encrypted tables, blind relays and the sidecar could each make their own keys. A removal would then be acted on three times, by rules that could disagree about who is out. And if the layer lived inside encrypted tables alone, blind relays and the sidecar would need an audience that no table uses.
- **Forward secrecy now (a conversation mode).** A ratchet per sender (DCGKA, p2panda's Message scheme, Signal) deletes each key after use. In rdb that means a newcomer, a new device or a restored replica cannot read the past from sync. Sync stops being the backup, and plaintext has to be kept and backed up some other way. It also needs a sending chain per device. It can be added later as a second way to make keys, over the same audiences, parts and deliveries.
- **MLS.** Its epochs need a total order. In a DAG every merge is a fork, which the decentralised drafts handle by keeping old secrets or dropping commits.
- **A tree of keys (TreeKEM, BeeKEM).** It rotates in O(log n) instead of O(n). Delivery to each member is simpler, and enough for conversations and team databases. A tree can replace it later for large audiences without changing the rest.
- **Two-party ratchets for deliveries (DCGKA's channels).** They give forward secrecy to the deliveries, but need state per device.
- **The epoch rule at use.** See *Why it is sound*.
- **Rotation by the remover, at the removal.** The remover may not be in the audience: a server's manager need not be in a direct message. And the removal may be written on a host that never syncs the part. Rotation by the next writer works for every removal.
- **Devices in the data model.** Only ratchets need them. One KEM key per identity works for everything here, and mb-chat already moves a key between devices.

## Open questions

- **Where KEM keys live.** A column of the identity provider that only the identity may write, or a table of its own. And whether a sender may still use a key it sees replaced.
- **The database audience.** How the catalog names it. And whether the app's allow list should be read from it, so that the members are written once and hosts and keys cannot disagree about them.
- **Part topics.** A topic computed from the part's id lets a tracker see which conversations are active. A topic derived from a secret hides that, but a newcomer then needs the secret before it can find peers.
- **History once the add is settled.** Deliver the past only once an add is final ([HIP-15](0015-certified-checkpoints.md)), and make new writes readable at once.
- **Where `HISTORY` is set.** In the schema, as here, or on the audience's rows, so that a conversation can change it as a Matrix room can.
- **Changing an audience.** A deploy that changes an audience's condition changes who is in it, and parts follow at the home group version they observe. A change to the key or to the audience's table would change which parts exist. Should those be refused, as a FILES definition never changes?
- **The sidecar.** Whether HIP-24's sidecar stays a standalone object that takes its secrets from the database audience, or becomes that audience's part. A part observes its home group, and the sidecar has to be held by replicas that hold nothing else of the database. And how a newcomer that holds only the sidecar gets its first delivery.

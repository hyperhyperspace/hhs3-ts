# HIP-27: Relays: always-on hosts for databases

- **Type:** design
- **Status:** draft
- **Modules:** rhost, sync, crypto, relay (new), relay_client (new)
- **Requires:** 26, 28
- **Superseded by:** —
- **Implemented in:** —

## Summary

A database syncs only while two of its hosts are online at the same time. Members who are rarely online together wait for each other, and a newcomer added by someone who then goes offline waits for whoever comes next. This HIP proposes *relays*: always-on hosts that hold a database while its members are away, usually run by a third party that is trusted to stay up and not with the data. There are two kinds. A **relay** is an ordinary host that is not a member. It holds every object, checks every entry, and reads whatever is not encrypted. It suits a public database, and a private one that hides only its encrypted tables ([HIP-26](0026-encrypted-tables.md)). A **blind relay** holds a private database as an opaque log. Members upload their entries in encrypted, padded bundles, and the relay learns neither the content, nor who wrote it, nor which objects or conversations changed, nor how the history branches. Members can tell when it hides bundles or shows them different logs. Bundles are sealed under audience keys ([HIP-28](0028-audience-keys.md)), so a removed member cannot read what is written later. The HIP also says how relays bound their storage, how bundles are sized and timed, what each kind can learn, and what is left for later: compaction, and newcomers who find only a blind relay online.

## Why

### Background

- **Host.** One database on one device, with its own key. A host syncs its database with the hosts of the other members.
- **Objects and entries.** A database is a set of objects: its table groups, its catalog, its schemas. Each object is a DAG of signed entries. An entry is named by the hash of its contents, and it names the entries it follows, its *predecessors*. A replica adds an entry only after its predecessors, and checks it by its hash and by the rules of its object. So an entry is as trustworthy from a stranger as from a member.
- **Sync.** Hosts sync each object on its own *topic*, named by the object's id, which they announce to a tracker to find each other. A host tells its peers its *frontier*, the entries nothing follows yet. A peer that lacks some fetches their headers (hashes and predecessors), then their contents.
- **The allow list.** The app's config says which peers its hosts sync with: `everyone`, or a column of the database that holds keys, such as `user.caps.grantee where label = 'member'`. Each host reads the list from its own replica, and *serves*, that is gives entries to, only the peers it lists. In this HIP a *member* is a key on the list, and a *removal* is an entry that takes a key off it, such as deleting its row from `user.caps`.
- **Bootstrapping** ([HIP-23](0023-join-bootstrap.md)). A host that has just joined takes entries from any peer until it first finds its own key on its list.
- **The membership sidecar** ([HIP-24](0024-membership-sidecar.md), a draft). A second object beside a database that records the members only as tags computed from a secret the members hold, so that it can be synced with anyone.
- **Audience keys** ([HIP-28](0028-audience-keys.md), a draft). The terms this HIP takes from it:
  - An *audience* is a set of identities read from a table, such as `user.caps.grantee WHERE user.caps.label = 'member'`. An audience can have one *part*, an object of its own that holds its key history, or one part per value of a column: one per conversation, say.
  - An *epoch* is a span of a part's history under one secret. It begins with a *rotation* entry, which carries a *delivery* of the secret to each member of the audience, encrypted to the *KEM key* that member publishes. A KEM key lets anyone seal a secret that only its owner can open.
  - A *delivery entry* gives an existing epoch to a member who lacks it, and a *request entry* asks for one. The *holders* of an epoch are the identity that started it and those it was delivered to.
  - Under `HISTORY SHARED` each rotation carries the earlier secrets, so a newcomer reads the past.
  - The *epoch rule*: nothing is written under an epoch that someone outside the audience holds. So the first writer after a removal rotates first.
  - The *database audience* is the audience a catalog names for the database's members.
- **Encrypted tables** ([HIP-26](0026-encrypted-tables.md), a draft). A table can be encrypted for an audience. Its rows live in the audience's parts, with their encrypted values readable only to holders of the part's epochs. Rules never read those values.

### What is missing

Sync needs two hosts online at the same time. Without something that is always online:

- **Members who are rarely online together.** Alice writes in the morning and Bob in the evening. Nothing Alice writes reaches Bob until both are online at once.
- **A list that is behind.** Bob has been offline. Carol is added and joins. Bob comes back with only Carol online. His list does not name her, so he refuses her, and nobody else is online to bring his list up to date. This is HIP-24's first case.
- **Nobody online knows the newcomer.** The admin adds Dave and goes offline before any other member syncs. Nobody online has Dave on their list. This is HIP-24's third case.
- **A removed member learns nothing.** Its peers stop serving it, so it cannot tell its removal from a quiet network. This is HIP-24's fourth case.

A member can already run a host on a server that never sleeps. That host is a member and reads everything, which is fine when a member runs it. The operator in mind here is a third party that runs relays for many accounts: trusted to keep them up, not trusted with their contents. How much such an operator may see differs by database:

- A public database has nothing to hide. But anyone can write to it, so the relay has to protect its storage without refusing honest entries.
- A database that keeps its secrets in encrypted tables may be fine with a host that reads the rest: mb-chat's always-on host, which should not read messages.
- A private database may want the operator to learn nothing at all, including who writes, when, and in which conversation.

### Prior art

*Relay* is the usual name for a host that carries data for others: Nostr and Evolu have relays, and Keyhive has a `Relay` capability, below `Read`, for a peer that syncs what it cannot read. Holepunch calls the blind kind a *blind peer*.

A survey made for this HIP in November 2026 looked at always-on services for local-first and end-to-end encrypted systems. None hides the content, the writer, the shape of the history and the timing of writes all at once. HIP-28's prior art covers key agreement. This table covers the services.

| System | What the service sees | How it checks writers | If the service lies |
| :--- | :--- | :--- | :--- |
| Beelay (Keyhive, Automerge) | Commit hashes and parent links, so the shape of the history. Contents are encrypted. | Each request is signed with the device's key. | Not addressed. |
| Holepunch blind peer | Which logs it holds, their lengths, block sizes and timing (inferred from the protocol). Block contents only when the app encrypts them. | It does not: anyone can ask it to store a log. Storage is one byte budget, cleared lowest priority first. | Not addressed. |
| SPORC | Who shares a document, who wrote each operation, sizes and timing. | Pseudonyms in the clear. | Each client chains the hashes of the history it has seen and signs the latest chain value into each operation. A mismatch proves the server showed two histories. |
| Frientegrity | Who wrote each operation, by pseudonym. | Pseudonyms in the clear. | The server signs the root of each object's history. Two signed roots for one version are a proof. |
| Depot | Everything: no encryption. | Signatures. | Each update carries a hash of the history its writer had seen. Forks are joined, and a proof of misbehaviour cuts the writer off. |
| SUNDR | Everything. | Signatures. | *Fork consistency*: a server can hide one user's writes from another only by keeping their views apart for good, and they find out as soon as they compare. |

Four results shape this HIP:

- **Syncing the history shows its shape.** Beelay keeps the commit graph outside the encryption so that its server can sync it. To hide the shape, the relay must not see entries or their links at all.
- **Chunking by a rolling hash leaks.** The sequence of chunk lengths fingerprints the content. The keyed chunkers of Borg, Restic, Tarsnap and others give up their keys to known-plaintext attacks (Truong et al., 2025). Where data is cut must not depend on the data.
- **Padding to a few sizes is cheap.** Rounding a length up to the next power of two leaks only about log log M bits of it, where M is the largest length. Padmé leaks the same order with under 12% overhead (Nikitin et al., 2019).
- **Lying can be detected, not prevented**, and only once members compare what they saw (SUNDR).

For a public database, Kleppmann (2022) shows that in a DAG of hash-named entries any number of misbehaving writers cannot stop correct replicas from converging, as long as correct replicas can reach each other. A relay needs limits on resources, not a defence against fake identities.

The survey checked its main claims against their sources three times each, independently. The claims about anonymous credentials (Signal's private groups, Privacy Pass), Plutus and Nostr relays were read in their primary sources but not checked again. The references are at the end.

## Design

### Two kinds of relay

A relay keeps one database for an *account*: whoever set it up with the provider and pays for it, usually the database's admin.

| | Relay | Blind relay |
| :--- | :--- | :--- |
| What it is | An ordinary host that is not a member | A log of sealed bundles |
| How hosts reach it | Sync, as with any host | Its address, with requests signed by a key only members can derive |
| What it reads | Everything that is not encrypted | Nothing |
| Who checks entries | The relay, and every host | Members, when they open a bundle |
| A newcomer with nobody else online | Is served by it | Cannot start from it |
| Suits | A public database. A private one that hides only its encrypted tables. | A private database |

*What a relay learns*, below, compares them in full.

### Relays

A relay is a host with its own key. It syncs like any other host, holds every object of the database, and checks every entry.

- **How it is let in.** On a public database, whose allow list is `everyone`, it needs nothing. On a private one, an allow entry names the relays, such as `user.caps.grantee where label = 'relay'`. Hosts serve a listed relay every object of the database.
- **Parts.** A relay is in no audience. It holds every part whose key value appears in the audience's table, and hosts serve it those parts as they serve the audience's members. It checks each entry's signature, its rules and the epoch rule, and stores the encrypted values it cannot read. Every check reads plaintext only (HIP-26, HIP-28), so a relay reaches the same verdicts as a member.
- **What it does for the others.** It serves whoever its own list names: a member whose list is behind, and a newcomer, whose addition it holds.

What a relay adds to a host is limits on what it stores:

- **A budget per database**, set by the account. When the database reaches it, the relay stops taking new entries for it and says so in its status. It keeps serving what it has. It does not evict: it is often the only copy online, and a host cannot serve an entry whose predecessors it has dropped.
- **No limit per writer.** Refusing an entry refuses every later entry that names it as a predecessor, whoever wrote those. A relay that refused a prolific writer would end up refusing the honest members who build on its entries.
- **Throttling is fine.** The relay may slow down how fast it takes entries from one connection or one address. The entries still arrive, later or through another peer.
- **Rules every host must share belong to the database.** The largest entry, the most predecessors, how often a key may write, a proof of work: a limit that only the relay enforces makes it refuse entries the members accept, and then everything after them.

The relay's limits decide what it holds and when, never whether an entry is valid.

For comparison: the blind peer bounds storage with one byte budget and evicts by priority and inactivity. Nostr relays publish caps on message size and on subscriptions, and may ask for a minimum proof of work per event or an admission fee. Eviction suits a cache of data that lives elsewhere. A relay is often the only copy online.

### Blind relays

#### Overview

A blind relay holds an append-only *log* of *bundles*. A bundle is a set of entries of one audience, sealed under that audience's current epoch and padded to one of a few sizes. The relay numbers bundles as they arrive and signs each new end of the log. It cannot open a bundle, and cannot tell which bundles belong to the same audience. To write to or read from the log, a host signs its request with a *relay key*, which only the database's members can derive. When a member is removed, the first host to upload after that starts a new epoch and gives the relay a new relay key.

The app's config names the blind relay: its address and the provider's public key.

Members still sync with each other directly, as today. The log is one more path, used mostly when no other member is online.

#### Keys

The log uses HIP-28's keys, and needs the database to have a database audience under `HISTORY SHARED`.

- **What seals what.** Entries of the database's ordinary objects (groups, catalog, schemas) travel in bundles sealed under the database audience's current epoch. Entries of a part travel in bundles sealed under that part's current epoch. A bundle never mixes audiences.
- **Relay keys** are signing key pairs derived from the database audience's epochs. Every member derives the same one, so the relay cannot tell members apart.
- **Hints.** Next to each bundle the relay stores a *hint*: a hash of the epoch's secret and the bundle's random nonce. A member tests each hint against the epochs it holds to find the bundles it can open. The nonces differ, so the relay cannot tell which bundles share an epoch or an audience.
- **Every member reads every bundle**, including those it cannot open, so that what it fetches does not tell the relay which audiences it belongs to. It drops the bundles it cannot open and keeps their hints. When it later receives a delivery, it tests the kept hints again and fetches the bundles that now match.

#### Rotation

A bundle is not an entry of a part, so admission cannot check it. Its writer applies the epoch rule to what its own replica holds: a host never seals a bundle under an epoch that a key outside the audience holds. So the first host to upload after a removal rotates first. That is usually the remover's own host, flushing the removal. It also works when the remover is not a member, or its host stopped before flushing.

A rotation entry, and a delivery for a member that lacks the current epoch, travel in a bundle sealed under an epoch their recipients already hold, usually the previous one. A member that was offline during the rotation can then open its delivery from the log.

What a removed member can still read is what HIP-28 says. It can read everything written before its removal. It can also read what members write under an epoch it holds before they see the removal, until they do.

A rotation costs one delivery per remaining member: about 1.2 KB with the hybrid KEM that HIP-28 makes the default, or about 110 bytes with X25519 alone. Removing one member out of fifty writes about 60 KB with the hybrid.

#### Newcomers

A newcomer gets its first delivery from a member, by sync, the way it gets the rest of the database: its host bootstraps (HIP-23), and a member whose list has the newcomer serves it. It cannot get it from the log, because it holds no epoch to open a bundle with. So a newcomer needs a member online once. That is the main limit on what a blind relay can do by itself. A new device of an existing identity is in the same position. *A welcome lane*, under *Considered and not taken*, would lift this limit at a cost.

A member added later to a part's audience, such as a new participant in a conversation, gets that part's delivery from any member who holds it: by sync, or from the log in a bundle sealed under the database audience. Every member can then see that the delivery was made, which tells them no more than the audience's rows in the home group already do (HIP-28). Then the newcomer to the part tests its kept hints, as above.

#### The relay key

- **Registering.** The account creates the log with the first relay public key. The log's id is derived from the database audience's first secret, so the relay cannot tie the log to the database's topics on the trackers.
- **Requests.** A request names the log, the operation, a hash of its body, the relay it is meant for and the time, and is signed with a relay key. The relay refuses a request whose time is more than five minutes off its clock. This is Beelay's request envelope, with a key that the group shares in place of the device's. Plutus, an earlier encrypted file system, used a bearer token whose hash the server kept. A signature also ties each request to its body.
- **Numbered keys.** The relay numbers the relay keys in the order they were registered. Only the latest, the *current* key, writes.
- **Labels.** The relay labels each bundle with the number of a relay key, which the uploader gives. A bundle sealed under an epoch of the database audience is labelled with the key derived from that epoch. A bundle of any other part is labelled with the current key. A key reads the bundles labelled with its number or an earlier one, wherever they sit in the log. For the other positions it gets only the bundle's hash, so that the chain (below) can still be checked. So after a rotation, an old key can still read the rotation and the deliveries sealed under the old epoch, and nothing else written since.
- **Re-keying.** Only a host that rotates the database audience re-keys the relay. It sends the new public key, signed with the current one, before it uploads anything under the new epoch. Then it uploads the rotation, sealed under the previous epoch, and then the rest. The relay takes one new key per key, the first. When HIP-28's concurrent epochs both stand, the relay stays on the one it has. A host that does not hold that epoch gets a delivery, from the log or from a member.
- **Catching up through the log.** A member that was offline across a rotation finds that its key no longer writes. It reads the bundles labelled with its key, finds its delivery, and moves on to the new key. A removed member finds that its key no longer writes, and that the log grows with bundles it may not read. That tells it it was removed.
- **A member that blocks the log.** Any member can re-key the relay to a key only it knows. The other members can then no longer write, and the account resets the current key through the provider. A shared key cannot stop a member from harming availability: a member could just as well fill the log's budget. It never lets a member read what it should not, because the epochs change whatever the relay does. Keys per member would let the relay cut one member off, at the cost of showing it who writes (see *Considered and not taken*).

#### Writing

A host *flushes* to the log on each whole minute of its clock if it has something to send, and at once when it stops.

- It uploads the entries it wrote since its last flush.
- It uploads entries it received from other members if the log still lacks them two flushes after they arrived. An entry then reaches the log even when its author went offline first. Two hosts may upload the same entry, and readers keep one copy.
- It does not upload what the log already holds. It knows that from reading the log and from its own uploads.

A bundle holds, sealed:

- the entries, each with the id of its object;
- the writer's identity, and its signature over the bundle;
- the last signed end of the log the writer had seen (see *Catching a relay that lies*).

It is compressed, sealed under a key derived from the epoch's secret and the nonce, and padded.

#### Reading

A host reads the log when it starts and while it runs, from the last position it read. It opens the bundles it can, checks each writer's signature, and gives the entries to its replica as sync does. Each entry is checked by its hash and its object's rules, and waits for its predecessors. A bundle that does not open although its hint matched, or that holds an entry that fails, is skipped and reported in the host's status.

An entry from the log is trusted no more than an entry from a stranger, so validation does not change.

#### Sizes and timing

The relay sees each bundle's size and when it arrives. Two choices keep that small.

- **Sizes.** A bundle is padded to a power of two, from 4 KiB to 1 MiB. A flush larger than 1 MiB is split into 1 MiB bundles. The relay sees one of nine sizes, and how many full bundles a large flush made. Padding wastes less than half of each bundle. Most flushes are small, so most bundles are 4 KiB.
- **Timing.** A host uploads only on whole minutes. The relay learns in which minutes some member wrote: not the moment, not how many writes, not who, not in which conversation.

What this costs: a member that writes a little every minute for eight hours uploads 480 bundles of 4 KiB, about 2 MiB, whatever the edits amount to, as long as they stay under 4 KiB a minute per audience. A flush that touches three conversations makes three bundles. Without compaction, all of it stays in the log.

What still leaks:

- the total size of the log and the number of bundles;
- the minutes in which someone wrote;
- when hosts connect, and from which network addresses.

To hide when hosts are online, each host would have to upload on every minute, written or not (cover traffic). That costs 4 KiB a minute per host online, about 5.6 MiB a day for a host that never stops, kept for good without compaction. This HIP does not do it.

#### Catching a relay that lies

A blind relay can drop a bundle, show an earlier end of the log, or show different members different logs. It cannot forge or change an entry, since entries are signed and checked as they arrive. The design detects the rest:

- **A chained log.** Each position has a chain value: the hash of the previous chain value and of the bundle at that position. With each answer the relay signs the end of the log, its position and chain value, with the provider's key. Every member fetches every bundle, so every member can check the whole chain.
- **Each bundle carries its writer's view.** The last signed end its writer had seen is sealed inside it. A reader checks it against its own chain at that position. A mismatch means two signed chain values for one position: a proof anyone can check.
- **Hosts compare when they meet.** Two hosts that sync directly exchange the latest signed ends they hold, in a new sync message. The one that is behind reads up to the other's end and checks that the chain agrees.
- **No going back.** A host remembers the furthest signed end it has seen. A shorter log later is a proof.
- **Own writes.** A host checks that each bundle it uploaded appears where the relay's answer to the upload said it would.

A host that holds a proof stops using the relay and shows the proof in its status. The account can move the database to another relay, and any member then uploads its whole replica as a new log. Entries check themselves, so joining what two logs held is a plain union.

Two things stay undetectable, as SUNDR shows:

- bundles from a member that goes offline before anyone reads them, until that member comes back;
- two groups of members kept apart for good, as long as no one in one group syncs directly with anyone in the other.

An independent witness that reads the log every few seconds and publishes the signed ends it sees would bound the second. This HIP leaves it out.

A relay of the first kind can also withhold entries. Hosts notice the same way they would with any peer: by syncing with someone else.

#### Compaction

The log only grows. Compacting it can come in steps.

- **None, at first.** A newcomer reads the whole log. That costs about as much as syncing the whole database from a member, plus padding, duplicates, and the parts it cannot open.
- **Optimistic compaction, later.** A member packs an old part of the log that it can open into full bundles, without duplicates, and appends them as a replacement, with the chain value at the end of what they replace. The relay serves the replacement to readers that start before that point. It keeps the original bundles for a retention period, such as 30 days, and then deletes them. A member that holds the entries of the originals checks the replacement when it first reads it. If something is missing it objects, and the relay keeps the originals. It is optimistic because readers trust the member who compacted, as a private database trusts its members to behave.
- **Dropping entries for good**, that is, keeping a checkpoint of an object's state in place of the entries that led to it, needs to know that no entry can still arrive that depends on them. That is finality, the subject of [HIP-15](0015-certified-checkpoints.md).

### What a relay learns

| | Blind relay | Relay, private database with encrypted tables | Relay, public database |
| :--- | :--- | :--- | :--- |
| Content | No | The plaintext tables. Not the encrypted values. | Yes |
| Who wrote an entry | No: members share the relay key. It sees network addresses. | Yes | Yes |
| Which objects or parts changed | No | Yes | Yes |
| Shape of the history | No | Yes | Yes |
| Number and size of entries | Only bundle sizes, one of nine | Yes | Yes |
| When | The minutes in which someone wrote, and when hosts connect | Yes | Yes |
| Members | No keys. When the database audience rotates, and roughly how many members there are, from the size of the bundle that carries a rotation | Yes: the audiences' rows, and who each delivery is for | Yes |
| The database's id | No | Yes | Yes |

### With HIP-23 and HIP-24

Either kind of relay settles HIP-24's cases for a database that has one, without the sidecar:

| HIP-24's case | With a relay | With a blind relay |
| :--- | :--- | :--- |
| A list that is behind | Bob syncs with the relay, which his list names, receives Carol's addition, and admits her. | Bob reads the log before he syncs with anyone, finds Carol's addition there, and admits her. |
| Nobody online knows the newcomer | The relay holds the admin's addition of Dave, and serves Dave. | The addition reached the log at the admin's last flush. The next member online reads it, and serves Dave. Dave still needs that member online. |
| A removed member learns nothing | Unchanged: the relay stops serving it, silently. | Its key stops writing, and it sees bundles it may not read. It can tell its removal from a quiet network. |

HIP-24's second case, a second device with a key the catalog seeds, is not helped by either: the device goes by a list that names only its own key.

HIP-28 gives the sidecar's second level its secret, from the database audience. If the sidecar is built, either kind of relay can hold it for anyone to fetch, since it shows no keys.

### Beyond rdb

A relay of the first kind is a host, and works for anything hosts sync. A blind relay's log carries entries of any objects, but its keys come from audiences, which only rdb defines. Any other kind of database built on these modules would need a member list with epochs of its own.

## Why it is sound

- **A relay reaches the same verdicts as a member.** Every check reads plaintext only (HIP-26, HIP-28). It differs from a member only in the values it cannot show.
- **A relay's limits never decide whether an entry is valid.** And a database converges on its other hosts whoever writes and whatever a relay withholds (Kleppmann, 2022).
- **A blind relay holds no secret.** Epoch secrets travel only as HIP-28 deliveries, or inside later epochs. Relay keys and bundle keys are derived from epoch secrets, and a hint, a hash of a secret and a nonce, reveals neither.
- **A removed member cannot read past its removal**, beyond what HIP-28 allows: what was written before, and what members wrote under an epoch it holds before they saw the removal. Its relay key stops at the rotation, and the bundles labelled with its key after that carry only deliveries it cannot open.
- **Nothing from a blind relay is trusted.** Each entry from the log is checked like an entry from a stranger. A relay, or a member, that puts garbage in the log costs readers time and the account storage, nothing more.
- **Lying is detected** within SUNDR's limits, stated above.

## Changes

Draft level, in the order they would land, after HIP-28. Relays of encrypted tables also need HIP-26.

1. `rhost`: the relay allow entry. Hosts on it are served every object and every part.
2. `relay` (new): the service. For a relay, an rhost host with a budget and throttling. For a blind relay: logs, numbered keys and labels, re-keying, signed log ends, and budgets.
3. `crypto`: padding to a power of two, and bundle hints.
4. `relay_client` (new): bundles (seal, pad, hint, open), the flush and read loops, kept hints, and the chain and proof checks.
5. `sync`: a message that carries the latest signed log end, sent to served peers.
6. `rhost`: a `relay` entry in the app config for a blind relay, the flush and read loops, the relay's state in `status()`, and re-keying when the host rotates the database audience.
7. Docs: a *Relays* section in the rhost README, and the new message in the sync spec.

## Tests

Relays:

- **A relay and members agree.** The same entries, given to a member and to a relay: the same live rows and the same deltas, apart from the encrypted values.
- **A conversation with a relay.** Alice and Bob have a conversation in an encrypted table. The relay stores, checks and serves its entries, and cannot read the values.
- **A newcomer with only a relay online.** The admin adds Dave and stops. Dave joins and is served by the relay.
- **A relay at its budget** stops taking entries, keeps serving what it holds, and never refuses an entry because of its writer.

Blind relays:

- **Members never online together.** Alice writes and stops. Bob starts later, with only the blind relay online, and has her entries.
- **A list that is behind.** Bob reads the log on start and admits Carol, whom his list did not name.
- **A removal.** Alice removes Xavier, and her next flush rotates the database audience and re-keys the relay. Xavier's key reads nothing written after that. Bob, offline during the removal, catches up through the bundles labelled with his old key and goes on with the new one.
- **Parts stay apart.** Alice and Bob have a conversation that Carol is not in. Carol fetches its bundles, cannot open them, and the relay's records do not show which bundles they are. Carol is added to the conversation, receives its delivery, and opens the bundles she kept hints for.
- **A member that blocks the log.** Xavier re-keys the relay to a key only he knows. Alice's re-keying is refused and her host reports it. Xavier still reads nothing new.
- **A lying relay.** A blind relay that drops a bundle, serves a shorter log, or shows Bob and Carol different logs is caught: by the reader's chain, by a remembered end, or when Bob and Carol meet.
- **Sizes.** Every bundle has one of the nine sizes. A 3 MiB flush becomes three 1 MiB bundles.

## Considered and not taken

- **Syncing over blinded headers.** A blind relay would speak today's sync protocol, with each entry hash replaced by a keyed hash so that it cannot confirm a guessed entry. This reuses sync, fetches per object, and lets the relay drop duplicates. But the relay sees the shape of each object's history, the number and size of entries, and which objects are active: what Beelay's server sees. Not taken, since the shape and the timing were to be hidden.
- **Cutting bundles with a rolling hash.** Chunk lengths fingerprint the content, keyed or not (Truong et al., 2025). Levels taken from entry hashes, as in Automerge's sedimentree, may still help members agree on what to compact, as long as they stay inside the bundles.
- **Padmé padding.** It wastes at most 12% where powers of two waste up to half, but it gives many more distinct sizes. Worth it if storage costs more than what sizes reveal.
- **One log per audience.** The relay would see which bundles belong to which conversation, and how active each is.
- **Fetching only the bundles a member can open.** Less to download, but the relay would learn which audiences each member belongs to.
- **A key per member at a blind relay.** The relay could cut one member off at once and give each member a budget. It would also learn who wrote each bundle, as the SPORC and Frientegrity servers do.
- **Anonymous credentials.** Signal's private groups let a server check that a request comes from one of the members of a list it holds only encrypted, without learning which. Privacy Pass tokens let a server count requests without linking them to who made them. Either would allow per-member budgets and cutting off one member without showing authorship. Both need an issuer at the relay and new cryptography. A later step, if the shared key falls short.
- **Manager keys registered with a blind relay.** Only managers could re-key it, so a member that is not a manager could not block the log. The relay would learn the managers' keys and when each re-keys.
- **Cover traffic.** See *Sizes and timing*: 4 KiB a minute per host online.
- **A welcome lane.** A part of the log that anyone with the database id can read, holding deliveries sealed directly to newcomers' KEM keys. A newcomer could then start from a blind relay with no member online. But the log could be found from the database id, the relay would learn when members join, and a newcomer would have to try to open every delivery in the lane. Not taken for now.
- **The key in the invitation.** Anyone who sees the invitation could read the database.
- **Limits per writer on a relay.** Refusing a writer's entries refuses everything that builds on them.

## Open questions

- **Relay lists.** One relay list per database or per audience, and whether a relay holds every part or only those it is asked to. Whether a relay may write a part's ref-advance.
- **Parts on a public database.** A relay sees who writes in which part. Keeping the parts in a blind log beside the relay would hide that.
- **Blocking.** Is a reset by the account an acceptable answer to a member that blocks a blind relay's log?
- **The numbers.** A minute between flushes, sizes from 4 KiB to 1 MiB, two flushes before uploading another member's entries. To be measured on real apps.
- **Reading every bundle.** In a database with many private conversations, each member downloads all their ciphertext. How much that costs, and whether some leak is worth trading for it.
- **One log per database.** An account with several databases shows a blind relay how many it has. One log per account would hide that, at the price of every host reading the bundles of every database in the account.
- **Network addresses.** A blind relay sees them. Hiding them needs a proxy or a mix network, outside this HIP.
- **Compaction.** Who may compact, how long originals are kept, and how a member objects.

## References

- Ink & Switch, *Keyhive lab notebook 05* (2025): https://www.inkandswitch.com/keyhive/notebook/05/
- Automerge, *Sedimentree* design notes: https://github.com/automerge/beelay/blob/main/docs/sedimentree.md
- Holepunch, *Blind peer* reference: https://docs.pears.com/p2p/reference/helpers/blind-peer/
- *Secure Untrusted Data Repository (SUNDR)*, OSDI 2004: https://www.cs.princeton.edu/courses/archive/fall15/cos518/papers/sundr.pdf
- *SPORC: Group Collaboration using Untrusted Cloud Resources*, OSDI 2010: https://www.cs.princeton.edu/courses/archive/fall15/cos518/papers/sporc.pdf
- *Depot: Cloud Storage with Minimal Trust*, OSDI 2010: https://www.cs.utexas.edu/depot/depot-osdi10.pdf
- *Social Networking with Frientegrity*, USENIX Security 2012: https://www.cs.princeton.edu/~mfreed/docs/frientegrity-sec12.pdf
- M. Kleppmann, *Making CRDTs Byzantine Fault Tolerant*, PaPoC 2022: https://martin.kleppmann.com/papers/bft-crdt-papoc22.pdf
- Truong, Merz, Scarlata, Günther, Paterson, *Breaking and Fixing Content-Defined Chunking*, 2025: https://eprint.iacr.org/2025/558
- Nikitin et al., *Reducing Metadata Leakage from Encrypted Files and Communication with PURBs*, PoPETs 2019: https://www.petsymposium.org/popets/2019/popets-2019-0056.pdf
- Chase, Perrin, Zaverucha, *The Signal Private Group System*, CCS 2020: https://eprint.iacr.org/2019/1416
- RFC 9576, *The Privacy Pass Architecture*: https://www.rfc-editor.org/rfc/rfc9576.html
- *Plutus: Scalable Secure File Sharing on Untrusted Storage*, FAST 2003: https://static.usenix.org/event/fast03/tech/full_papers/kallahalla/kallahalla.pdf
- Nostr NIP-11 (relay information) and NIP-13 (proof of work): https://github.com/nostr-protocol/nips/blob/master/11.md, https://github.com/nostr-protocol/nips/blob/master/13.md

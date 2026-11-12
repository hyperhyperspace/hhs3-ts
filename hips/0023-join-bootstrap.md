# HIP-23: Bootstrapping a joined host under an allow list

- **Type:** design
- **Status:** implemented
- **Modules:** rhost, rhost_node, rhost_browser, rhost_client, rdb_tools, sync, mesh
- **Requires:** —
- **Superseded by:** —
- **Implemented in:** `e90176a`

## Summary

An app's `allow` list says which peers its hosts sync with, and it is read from the database itself. A host that has just joined a database has none of that data yet: the tables the list reads don't exist until sync creates them, and once they do they hold only the rows written when the database was created. Today such a host fails to start, and if it started it would refuse every member those first rows don't name. This HIP separates two questions the list answers together today: which peers a host admits, and which peers it gives entries to. A host gives entries only to peers on its list, and checks each time it is about to send. A host that joined a database is *bootstrapping* until the first time it finds its own key on its list: it admits any peer and takes entries from it. It then records where it found its key, and never bootstraps again. And `start` checks an allow entry only once the table it reads exists. A newcomer can then be brought in by any member that knows it. This is a stopgap: a member whose list is merely out of date behaves as it does today, and [HIP-24](0024-membership-sidecar.md) explores a fuller answer.

## Why

### Background: how a host syncs today

- **Host.** One database on one device, with its own key, replica and network settings. rhost runs the hosts of an app.
- **Create and join.** `create` makes a new database, with the host's key as its creator. `join` takes the id of an existing database, fetches its first entry (the *genesis*) from a peer, and installs the catalog release the app ships. Neither starts the host.
- **Host record.** What a device keeps about each host, beside its replica: the database id, the host's key and network settings, and whether the host created its database or joined it. It is written once, when the host is made. On Node it is the file `host.json` in the host's folder. In the browser it is a row in IndexedDB.
- **Entries.** Every object of a database (the database itself, its catalog, its schemas, its table groups) is a DAG of signed entries. Sync copies entries between replicas. A replica checks each entry it receives against the rules of its object, as of the entries that precede it, and refuses one that fails. An entry that passes is stored, and the rules then decide whether it counts, given the entries concurrent with it.
- **Table groups.** A database's tables live in table groups, such as `user` and `doc`. A replica does not download a group's creation. It computes it from the genesis and the catalog, and it does so when sync starts. A new group holds the rows the catalog seeds for it: in the editor example and in the chat catalog, an identity and a first capability for the database's admin.
- **Swarm.** For each object, the set of connected peers that sync it. A swarm *admits* a peer when it takes it in.
- **Serving.** Inside a swarm, each peer announces its *frontier*, the hashes of its newest entries. A peer that sees hashes it lacks asks for them, and the peer that has them answers. Answering is *serving*. Today a swarm serves every peer it admitted.
- **`allow`.** A list in the app's config. Each entry is `everyone` or `group.table.column [where <condition>]`. A host admits a peer when the peer's key appears in one of the columns, read from the host's own replica at the moment the peer connects or is discovered. The same check applies to every swarm of the database.

### What happens today

Run on 2026-11-11 against the working tree, with rhost's in-memory test platform and the editor example catalog. The app's list is `user.identities.keyId`.

1. The admin creates a database and starts its host.
2. The admin adds Bob's key to `user.identities`.
3. Bob's host joins. This works: the admin's list has Bob, so the admin serves him the genesis.
4. Bob's host starts, and fails with `BIND_UNKNOWN_NAME: Group 'user' is not loaded`.

Bob's host never runs. The same failure was first seen in mb-chat in the browser, with the list `user.caps.grantee where label = 'member'`.

### The cause

**Start reads the list before sync creates its tables.** Starting a host runs each allow query once, so that a misspelled entry fails early ([`rhost/src/host.ts`](../modules/rhost/src/host.ts), `runStart`). On a joined host the `user` group does not exist yet, since sync creates it, and sync starts after the check. A host that created its database never hits this, because creating deploys the groups.

**Past that error, the list has nobody on it.** Skipping the check is not enough. When sync creates Bob's `user` group, its tables hold the catalog's seed rows: the admin, and nobody else. Bob's host would admit the admin's hosts and refuse every other member, including the ones that would gladly serve him. With only the start check changed, Bob syncs when the admin is online. When the admin is offline and another member, Carol, is online, Bob's host starts, connects to no one, and stays at the seed rows (row J2 below).

The list is data that arrives by sync, and sync is gated by the list.

**The check runs once.** A peer is checked when it connects. A member removed while its host is connected keeps receiving everything, including what is written after its removal, until the connection drops (row J4 below).

## Design

### Two questions

Today the list answers one question, once: may this peer join the swarm. A peer in the swarm gets everything. This HIP asks two:

- **Admit:** may this peer be in my swarm?
- **Serve:** may I give this peer entries?

They differ in what a wrong answer costs. Serving a peer that should not be served leaks data. Taking entries from a peer that is not on the list leaks nothing, and the entries are checked like any others.

### The serve rule

> A host gives entries only to a peer whose key is on its allow list, and it checks each time it is about to send.

- **Requests.** A peer that is not served gets no answer to a request for the genesis, for headers or for payloads.
- **Long answers.** One answer can take several batches, sent a tick apart: an honest peer asks for up to 1024 headers at a time, and they go out 128 per tick. The check is made again before each batch. It is also made before an answer that waited its turn behind another one, and before the payloads that follow a header answer without a request of their own. A no drops everything queued for that peer (J16).
- **Announcements.** The host does not announce its frontier to a peer it does not serve. Without the frontier, the peer has nothing to ask for, so it does not wait on unanswered requests. An announcement that was withheld does not count as made: when the peer next announces, the host answers with its own frontier as if it had never sent it.

The host still listens to that peer: it reads its announcements, asks it for entries, and applies what it gets.

The rule holds whether or not the host is bootstrapping. In an established host it makes a removal take effect at the next batch, with the peer still connected.

### Bootstrapping

> A host that joined its database is bootstrapping until the first time it finds its own key on its copy of the allow list. A bootstrapping host admits any peer. A host bootstraps once.

- **It starts by joining, and by nothing else.** A host that created its database never bootstraps: the database starts there. Nothing that arrives over the network puts a host into this state.
- **It ends when the host finds its key.** A host joins a database because someone added its key. While its copy of the list does not show that, the copy is older than the host's own admission, and it cannot be used to tell members from strangers. Once the key is there, the host is where any member is that has been offline since it was added.
- **It ends at the first check after the key arrives.** The host reads its list before it answers either question, admit or serve. So a stranger that connects just after the key arrived is refused, even if nothing was served in between.
- **It includes the time before the groups exist.** Then the list cannot be read at all, which counts as not finding the key.
- **It does not come back.** A host that is later removed, or whose list stops naming it for any other reason, goes by its list.
- **Peers admitted while bootstrapping stay** until they disconnect. They are the peers feeding the host, and its list may not name them all yet. Under the serve rule they get nothing they are not entitled to.
- **A list with `everyone`** has no check at all, today and with this HIP.

### The stored position

A host has to know, at every start, whether it is still bootstrapping. The host record already says whether the host joined. This HIP adds where it found its key:

- **What is written.** When a bootstrapping host first finds its key, it writes into its record the group whose column listed the key, and that group's frontier at that moment. The frontier is never empty: a group that lists a key has entries. A record with an empty one is refused when it is read.
- **What a start reads.** A joined host is bootstrapping unless its record names such a position and its replica holds those entries.
- **Why a position and not a flag.** The record and the replica are separate stores, and one can be restored without the other. If a replica is put back from an older copy, a flag still says the host is done, while its list may no longer name it or anyone it can reach. A position the replica does not hold means the host is bootstrapping again.
- **The record follows the replica.** The entries are in the replica before the list can show the key, so the record can be behind the replica and never ahead of it. A start reads the list once before it admits anyone. A host that holds its admission but not the record is therefore done at once.
- **A write that fails** is reported as the host's last error. The host goes by its list for that run, and the next start writes the record.
- **A write never makes a host.** If the host was stopped or removed while its key was being found, nothing is written, and a store refuses to update a host it does not have.

A running host's status says whether it is bootstrapping, and whoever watches the status is told when that ends. An app can then show a host that is waiting to be let in, and stop showing it.

### False starts

| What happens | Result |
| :--- | :--- |
| `join` finds no peer to fetch the genesis from | No host is left behind: a host is kept only once it is complete, as today. |
| `start` finds no peer | Nothing is written. The host is bootstrapping at its next start (J12). |
| A peer delivers part of the list's group, without the host's key, and leaves | The host is still bootstrapping. The entries stay in the replica. |
| The host finds its key, and the record cannot be written | The next start reads the list, finds the key and writes the record (J13). |
| The key never arrives: the host was never added, or its list matches nobody | The host bootstraps for good. It admits any peer and serves none. Its status says so. |

The second and fourth rows were run. The others follow from the rule.

### A removed host, and one that is added again

A removed host goes by its list, like any established host. That list still names the members it knew. If the host is added again, those members accept it again, and it catches up. Nothing is reset (J15).

It does need a restart, or a peer that leaves: a swarm looks for peers only then, so a host that was refused does not try again on its own.

What it cannot do is reach a member it never knew. That is the case of a member whose list is out of date, which this HIP leaves as it is. Removing the host and joining again works today and makes it a newcomer, at the cost of a full download.

### Checking the list at start

`start` still checks every allow entry, in two steps:

1. The group must be a group of the database as this replica has it, or a group of the release the app ships. Both sets of names are known before any group exists. A misspelled group fails the start, on a joined host too.
2. If the group exists on this replica, the query runs once, as today. If it does not exist yet, the entry is skipped.

The shipped release is there for a group added after the database was created. A new host has only the genesis, which names the groups of the release the database was created with. The upgrade that added a group arrives by sync, so a list that names that group would otherwise fail the start that sync depends on (J17).

A skipped entry with a misspelled table or column is caught at the next start, when the group exists. Until then no key is on the list, so the host keeps bootstrapping: it takes entries and serves no one (row J7).

### A join, step by step

Carol is a member. The admin added Bob and is now offline.

1. Bob's host joins. Carol's list has Bob, so she serves him the genesis.
2. Bob's host starts. Its record names no position, so it is bootstrapping. It finds Carol on the database's topic and admits her.
3. Sync creates Bob's `user` group with the seed rows. Bob is not on it, so he is still bootstrapping.
4. Carol serves Bob: her list has him. Bob does not serve Carol yet: his list has only the admin.
5. Bob receives the `user` group's entries. His list now has himself and Carol. He records the position, and is no longer bootstrapping. He serves Carol from here on.
6. Bob announces his frontier to Carol at his next new entry, or in answer to her next announcement. From then on the two sync both ways.

### Outcomes

Run on 2026-11-11 against the working tree and against a copy of it with the changes of *Changes* applied. The control columns come from the same copy with one rule switched off or back: "start check only" has bootstrapping switched off, and "no serve rule" has the serve rule switched off.

| # | Scenario | Today | Start check only | This HIP |
| :--- | :--- | :--- | :--- | :--- |
| J1 | The admin adds Bob. Bob joins and starts with the admin online. | Bob's start fails | Bob syncs, and his page reaches the admin | the same |
| J2 | The admin adds Carol and Bob. Carol joins and syncs. The admin goes offline. Bob joins and starts. | Bob's start fails | Bob starts, has no peers, and stays at the seed rows | Bob syncs from Carol, and his page reaches Carol |

| # | Scenario | No serve rule | This HIP |
| :--- | :--- | :--- | :--- |
| J3 | Dave was a member and synced. The admin removes him. Dave's host accepts and asks every peer, and restarts, so the admin now refuses it. Bob joins. The admin writes two more pages. | Bob admits Dave while bootstrapping. Dave receives both pages through Bob. | Bob admits Dave while bootstrapping. Dave receives neither page. |
| J4 | Dave is a member and his host is connected to the admin's. The admin removes him and writes a page. | Dave receives the removal and the page. This is also what the working tree does. | Dave receives neither. |

| # | Scenario | This HIP |
| :--- | :--- | :--- |
| J5 | Bob is a member and goes offline. The admin adds Carol, who joins and syncs, and goes offline. Bob comes back with only Carol online. | Bob's record names a position his replica holds, so he is not bootstrapping. Carol is not on his list, so he refuses her. Neither syncs until the admin is back. As today. |
| J6 | The admin adds Carol, who syncs, and goes offline. A second device with the admin's own key joins. | Sync opens the swarms of the database, the catalog and the two schemas before it creates the groups. The device is bootstrapping then, and admits Carol to those four. When the swarms of `user` and `doc` open, the seed rows already name its key, so it has stopped bootstrapping and refuses her there. At the end of the run it held Carol in the first four swarms and no peer in the two groups', and it had nothing beyond the seed rows. |
| J7 | A joined host whose list misspells a group; another whose list misspells a column. | The group: the first start fails with `Unknown group 'usr'`. The column: the first start succeeds and the host takes entries and serves no one; the second start fails with `unknown column 'nope'`. On a host that created its database both fail at the first start, as today. |

The next table separates this HIP's rule from the one it first proposed, under which a host bootstrapped whenever its own key was not on its list. J8 to J10 were run on the copy when it had that rule, and again on the revised copy. J11 was run on the revised copy with the earlier rule switched back on. In all four, the list is `user.caps.grantee where label = 'member'`.

| # | Scenario | Bootstrapping whenever the key is not listed | This HIP |
| :--- | :--- | :--- | :--- |
| J8 | Bob is an established member. The admin removes him, and Bob receives the removal from a member whose app has no allow list. The members go offline. Eve was never added and has only the database id. She starts a host. Bob restarts. | Bob admits Eve, before and after his restart. She receives nothing. | Bob refuses Eve, before and after his restart. |
| J9 | As J8. Mallory was a manager. She was removed before Bob, while offline. On her out-of-date replica she grants herself `member`, and connects to Bob. | Bob admits her and takes the entry. The rules void it. She receives nothing. | Bob refuses her. The entry does not arrive. |
| J10 | As J9, and Mallory first removes the admin's `manager` capability on her replica. | Bob admits her and takes both entries. On his replica the two removals cancel each other, her grant counts, and he serves her the page written after her removal. | Bob refuses her. Neither entry arrives. |
| J11 | A host that created its database is restarted with an allow entry whose `where` matches nobody. Eve starts a host. | The host admits Eve, and gives her nothing. | The host refuses Eve, as the working tree does. |

The last table is this HIP's rule only.

| # | Scenario | This HIP |
| :--- | :--- | :--- |
| J12 | Bob joins, and starts twice with no peer online. | He is bootstrapping both times, and his record names no position. |
| J13 | The admin is back. Bob syncs, and the first write of his record fails. Bob restarts with no peer online. | For that run Bob goes by his list, and his status carries the error. At the restart the record is written and the error is gone. |
| J14 | Bob's record is changed to name an entry his replica does not hold. Bob restarts. | The start reads the list, finds his key, and writes the position his replica does hold. |
| J15 | Bob is removed, receives the removal, and restarts. The admin adds him again and writes a page. Bob restarts. | While removed, Bob has no peers. After he is added again he does not catch up on his own. After the restart he connects to the admin and catches up. |

A host that created its database was never bootstrapping in these runs, and its record names no position.

Two more rows each have a control of their own, from the same copy.

| # | Scenario | Control | This HIP |
| :--- | :--- | :--- | :--- |
| J16 | The `doc` group holds 2,600 pages. Dave is a member, and his host starts downloading it. The admin removes him when 260 of its headers have been sent. | *Checking only when a request arrives:* 768 more headers go to Dave over the next 0.6 s, the rest of one answer, and no payloads. | Nothing more is sent to Dave. |
| J17 | A database is created at release 1.0.0 and upgraded to 1.1.0, which adds a group `notes`. Bob's app ships 1.1.0, and its list names `notes`. Bob joins. | *Checking the name against the database's groups only:* Bob's start fails with `Unknown group 'notes'`, as it does for a misspelled name. | Bob's start succeeds, sync brings the upgrade, and a second start succeeds. A list that names `nots` still fails the start. |

In J16 Dave ends with none of the pages either way: the answer under way held headers only, and his next requests were refused.

The mesh, sync, replica and rhost suites pass on the revised copy (88, 25, 40 and 13 tests), once the swarms that the sync tests build by hand have the new method. rdb's `[RDB…]` tests passed on the copy before the revisions (44 tests), which did not touch rdb. The Node and browser host stores were not changed in the copy. The rest of rdb's suite and the other modules were not run on it.

As implemented, the tests listed under *Tests* cover these rows in the tree. The runs for J1 to J9, J11 to J15 and J17 were repeated against it on 2026-11-12, with the same results. J16 was not repeated there, since its count of what a host sends was taken on the copy; the fourth case of `[SYNC_25]` covers it.

### What stays as today

- **A member whose list is out of date** (J5). It trusts its list, and refuses members added while it was away. It catches up when a peer both sides know is online.
- **A second device with a seeded key** (J6). The catalog's seed rows already name the key, so the new device stops bootstrapping as soon as its groups exist, before it has received anything.
- **Nobody online knows the newcomer.** If the only member that knows Bob was added is offline, no one serves him. The fact exists on one replica only.
- **Peers are found once.** A swarm looks for peers when it starts and when a peer leaves. A host that learns of a new member later does not go looking for it. In J5, once the admin is back, Bob learns of Carol, but the two reach each other only through the admin until a swarm of one of them looks for peers again. In J15 it is why Bob needs a restart.

## Why it is sound

**Nothing is given to a peer that is not on the list.** The serve rule holds in every state, for every kind of answer, for each batch of an answer, and for announcements. What a peer that is not served can see is that the host is on the topic, and the hashes in the host's own requests: the entries it asks for, which that peer announced, and the host's frontier, sent as the points where the answer should stop. It sees no entry.

**What is taken from a peer that is not on the list is checked like everything else.** An entry that fails the rules of its object, as of the entries before it, is refused when it arrives. `join` already fetches the genesis this way, with no check on who serves it.

**Only joining opens a host.** A host admits strangers because the person at that device joined a database, and for no other reason. The three ways a host can come to be missing from its own list all leave it closed:

- *It was removed.* It goes by its list (J8).
- *Its list matches nobody.* It admits no one, as today (J11).
- *It was never added.* It joined, so it bootstraps, and keeps bootstrapping. No member serves it, since no member's list has it. It serves only the keys on whatever list it holds, as any host does.

**The list is a second line, and bootstrapping takes it down, so the state is kept narrow.** An entry from a former member can pass the check on arrival, when it was written on a replica that predates the removal. Whether it counts is then up to the rules. J10 is a case where the rules let it count: two managers who remove each other both stay. On an established host the list keeps a former member from delivering such entries at all. A host with a full replica should therefore never be made to admit strangers by something that arrives from outside, which the earlier rule allowed and this one does not. A newcomer still admits them, for the length of its join, with a replica that holds nothing yet.

**Bootstrapping ends at the right time for the peers that matter.** The host stops admitting strangers when it finds its own key. By then it holds the entries that precede its own admission, which name the members that existed before it. Members whose admission it has not received yet are refused as new peers, but the peers it already has keep feeding it.

**A peer that is not served is not left waiting.** It never receives the host's frontier, so it never asks the host for anything. When it becomes served, the host announces to it at its next new entry or in answer to the peer's next announcement, and the peer asks then.

**The cost.** While a host bootstraps, a stranger that knows the database id and can reach the host can take one of its swarm's slots and deliver nothing. A swarm holds eight peers at most. Enough strangers could keep a new host from reaching a member.

## Changes

1. **mesh, [`swarm.ts`](../modules/mesh/src/swarm.ts).** `PeerAuthorizer` gains an optional `serves(keyId)`. `Swarm` gains `wouldServe(keyId)`, which answers `true` at once when the authorizer has no `serves`, so a swarm without one behaves as today.
2. **sync, [`session.ts`](../modules/sync/src/session.ts).**
   - The session asks the peer's swarm before it answers an `init-request`, a `header-request` or a `payload-request`, and before it sends a `new-frontier`. A `cancel-request` is always handled.
   - Announcements to one peer are sent in the order they were made.
   - It gives its provider a function that asks the same question for a peer.
   - When it withholds a `new-frontier`, it tells its synchronizer to forget what was last sent to that peer.
   - Any no, to a request or to an announcement, also drops what its provider has queued for that peer.
3. **sync, [`provider.ts`](../modules/sync/src/provider.ts).** `createDagProvider` takes an optional `mayServe(peerKey)`.
   - It is asked before every header batch and every payload burst, the first of an answer included. A no calls `cancelPeer`.
   - Every answer starts in `startNext`, which asks before the first batch. That covers the payloads that `autoPayload` queues after the last header batch, and a second request that waited behind the first. An answer that starts as its request arrives is asked about twice in a row, once by the session and once here; the list is cached, so the second costs a frontier read.
   - While `startNext` is asking, the peer's slot in `activeResp` stays taken, so a request that arrives then queues behind it.
   - A tick of a stream does nothing while the previous tick of that stream is still asking or sending, so two batches never overlap.
   - A stream that ends gives up the peer's slot only if the slot is still its own, since `cancelPeer` now runs while the peer stays connected.
4. **sync, [`synchronizer.ts`](../modules/sync/src/synchronizer.ts).** `DagSynchronizer` gains `forgetSentFrontier(peerKey)`, which clears the peer's entry in `lastSentFrontier`. The synchronizer records a frontier as sent when it hands it to the send path, and that record is what holds back an answer to the peer's next announcement for 15 s.
5. **rhost, [`sync.ts`](../modules/rhost/src/sync.ts).**
   - `createAllowAuthorizer` takes options: `frontier`, a function that gives the frontier of the group an allow entry reads, and `bootstrap`, the host's key with a `found` callback, given only to a host that is bootstrapping.
   - The listed keys are cached and read again when the frontier of a group the list reads has changed. Both `serves` and `authorize` go through that read first.
   - `serves` is today's check. `authorize` answers yes if `bootstrap` is still pending after the read, and is today's check otherwise.
   - The first time the listed keys include the host's key, the authorizer reads that group's frontier again, after the query, and calls `found` once with the group and the frontier (the new type `ListedAt`). If the frontier cannot be read or is empty, `bootstrap` stays pending and the next read tries again.
   - `validateAllowSources` takes `frontier`, and skips an entry whose group is not on the replica yet.
6. **rhost, [`config.ts`](../modules/rhost/src/config.ts).** `HostRecord` gains an optional `listedAt`: a `group` name and a `frontier`, a list of hashes. `parseHostRecord` accepts it, and refuses a `frontier` that is empty or holds an empty string.
7. **rhost, [`platform.ts`](../modules/rhost/src/platform.ts).** `HostStore` gains `update(name, record)`, which replaces the record of an existing host. It throws when the host does not exist, and never creates one.
8. **rhost, [`host.ts`](../modules/rhost/src/host.ts).**
   - `runStart` builds `frontier` from the group's name. A name among the database's member groups gives that group's frontier, or nothing while the group is not on the replica. A name that is only among the group definitions of the shipped release, read from the installed catalog, gives nothing. Any other name throws `Unknown group`.
   - A host is bootstrapping when its app has an allow list, its record has `created: false`, and its record's `listedAt` is missing or names an entry the group's DAG does not hold.
   - A bootstrapping host passes `bootstrap`, and reads the list once before sync starts.
   - `found` does nothing once the host has stopped or closed. Otherwise it reads the stored record, adds `listedAt`, writes it with `update`, and notifies the status listeners. A failed write becomes the host's last error.
   - `Host.record` becomes a getter over the current record. `Host` gains `bootstrapping`, and `status()` reports it.
9. **rhost_client, [`types.ts`](../modules/rhost_client/src/types.ts).** `HostStatus` gains an optional `bootstrapping`. The `rhost status` command in [`rdb_tools/bin/rhost.ts`](../modules/rdb_tools/bin/rhost.ts) shows it on its `peers` line.
10. **The host stores.** Each `update` fails for a host that is not there.
    - rhost_node, [`platform.ts`](../modules/rhost_node/src/platform.ts): `update` rewrites `host.json` through a temporary file and a rename, inside the host's existing folder.
    - rhost_browser, [`hosts.ts`](../modules/rhost_browser/src/hosts.ts): `update` replaces the record in the host's row, if the row is there and published.
    - The in-memory store of rhost's tests, in `rhost/test/fixtures.ts`.
11. **Tests with hand-built swarms** gain `wouldServe`: two in `sync/test/sync_test.ts`, one each in `rdb/test/rdb_sync_tests.ts` and `replica/test/replica_sync_tests.ts`.
12. **Docs.**
    - [`sync/SPECS.md`](../modules/sync/SPECS.md): §1, a peer may withhold serving from a peer, and stop an answer part way; §3.1, a peer announces its frontier to the peers it serves.
    - [`sync/README.md`](../modules/sync/README.md): a session serves only the peers its swarm serves.
    - [`mesh/README.md`](../modules/mesh/README.md): `PeerAuthorizer`.
    - [`rhost/README.md`](../modules/rhost/README.md): `allow` (whom a host serves and whom it admits, bootstrapping, the start check), the `start` bullet, `listedAt` in `host.json`, and `bootstrapping` in the status.

The REPL's `\sync` builds its authorizer without options. It gets the serve rule and never bootstraps.

In the copy used for *Outcomes*, items 1 to 9 add about 280 lines.

## Tests

- **rhost:**
  - `[RHOST14]`: J1. A joined host under an allow list starts, syncs, and its own write reaches the admin.
  - `[RHOST15]`: J2. With the admin offline, a newcomer syncs from another member, and that member gets the newcomer's write.
  - `[RHOST16]`: J3. A peer that is not on the list is admitted by a bootstrapping host and receives nothing.
  - `[RHOST17]`: J4. A member removed while connected receives nothing written afterwards.
  - `[RHOST18]`: J5. An established member refuses a peer its list does not name. This pins the limit, so that a later change to it is deliberate.
  - `[RHOST19]`: J7. A misspelled group fails a joined host's first start; a misspelled column fails its second.
  - `[RHOST20]`: J8. A removed host that has received its removal refuses a peer its list does not name, before and after a restart.
  - `[RHOST21]`: J11. A host that created its database, with an allow entry that matches nobody, refuses every peer.
  - `[RHOST22]`: J12 and J13. A joined host is bootstrapping through starts with no peer, and its status says so. It records its position when it finds its key, and a status listener hears that bootstrapping ended. A write that fails is reported, and the next start writes the record.
  - `[RHOST23]`: J14. A record that names an entry the replica does not hold makes the host bootstrap again, and the record is rewritten.
  - `[RHOST24]`: J15. A host that is removed and added again catches up after a restart.
  - `[RHOST25]`: J17. A list that names a group added by a later release passes a joined host's first start, and a misspelled group still fails it.
  - `[RHOST26]`: a host removed while its key is being found leaves no record behind.
  - The authorizer's unit test:
    - While `bootstrap` is pending, `authorize` accepts any key and `serves` accepts none.
    - Once the key is on the list, a call to `authorize` alone ends bootstrapping and refuses a key that is not listed.
    - `found` is called once, with the group and a frontier that is not empty.
    - After that both follow the list, also once the host's key has left it.
  - The config test: `parseHostRecord` accepts `listedAt`, and refuses one with an empty `frontier`.
- **rhost_node:** `[RHOST_NODE07]`: `update` rewrites `host.json`, and the record reads back with its other fields unchanged. On a host that was removed it throws and leaves no folder.
- **rhost_browser:** `[RHOST_BROWSER03]`: the same, for the IndexedDB store and its row.
- **sync:** `[SYNC_25]`, a session whose swarm does not serve a peer:
  - It answers none of the peer's requests and sends it no frontier. It still applies what that peer serves.
  - It serves the peer once the swarm's answer changes.
  - When the answer changes within the 15 s in which a repeated frontier is held back, and the peer announces again with no new entry on this side, the peer gets this side's frontier.
  - When the answer turns to no part way through an answer of several batches, no further batch is sent (J16).
  - A request that was waiting behind that answer is not answered either, and neither are the payloads that would have followed a header answer.
- **mesh:** `[AUTH_Z_08]`: `wouldServe` is `true` without an authorizer and without `serves`, and follows `serves` otherwise.

J9 and J10 are not tests: under this HIP they end as J8 does, and what the earlier rule did in J10 depends on a rule that [HIP-16](0016-seniority-order.md) proposes to change.

## Considered and not taken

- **Only changing the start check.** J2: the newcomer then depends on a key the catalog seeded being online.
- **Bootstrapping whenever the host's key is not on its list.** This HIP first proposed it, since it needs nothing stored. It reads three other conditions as a join: a removed host, a list that matches nobody, and a host that was never added. A removed host then admits strangers for good, with a full replica behind it (J8 to J11).
- **A stored flag instead of a position.** It can disagree with a replica that was restored from an older copy, and then the host is closed on a list that may not name it.
- **Keeping the state in the replica.** A replica's storage holds DAGs and nothing else.
- **Ending the state when a removed host is added again**, by something that arrives. An arriving entry that opens a host is what J8 to J10 rule out. A person at the device can remove the host and join again.
- **Taking entries from any peer at all times**, not only while bootstrapping. It would also fix J5 and J6. An established host knows its members, though, and admitting strangers costs it slots, and takes down the second line described above, for no gain in the common case.
- **Dropping the peers the list does not name when bootstrapping ends.** A host can find its own key before it has received the admission of the peer it is syncing from. Dropping that peer would cut its only source, and peers are not looked for again.
- **Checking only when a request arrives, and dropping what is queued for a peer when one of its requests is refused.** It stops nothing while the peer sends no further request for that object, which is the case while it waits on the answer under way. In J16 the drop never ran, and the 768 headers went out as they do without it. Checking before each batch needs no trigger.
- **Grading peers in the swarm** (full, source only, refused), with the grade asked again when the list changes. It gives the same result with more machinery in the mesh. Asking on each request needs none.
- **Answering a peer that is not served with an empty reply**, and announcing to everyone. Header requests have an empty reply that the asker remembers until the next announcement. Payload requests have none, so the asker would wait out its 30 s timeout each time.
- **A refusal message.** A host does not tell a peer that it is not serving it. Refusals stay indistinguishable from silence.
- **Carrying the serve check through rdb's runtime config and a session option.** The authorizer already reaches every swarm, and the session already holds its swarms.
- **A key to trust carried in the invitation.** It works for a newcomer, adds something to hand over outside the app, and does nothing for J5.

## Open questions

- **A position carried in the invitation.** The member that adds a newcomer could send, with the database id, the frontier of the list's group. The newcomer would bootstrap until its replica holds it. That is exact, it does not depend on the list naming the host, and it would let a second device with a seeded key in (J6). It changes what every app hands over. The record's field would hold it.
- **Entries from former members, through a newcomer.** A newcomer admits any peer while it joins, and afterwards serves what it took to members. Whether a former member can get entries like J10's to the members that way was not established: in one run with a newcomer, the two entries had not reached the newcomer or the admin when the run ended, and the reason was not traced. The answer belongs to the rules, whichever way it goes.
- **Slots while bootstrapping.** Whether to cap the peers a bootstrapping host admits without their being on its list, and to replace the ones that deliver nothing.
- **A host that bootstraps for good.** A host that was never added, or whose list has a misspelled column, keeps admitting strangers. Whether it should stop after a time, and how an app should show it.
- **A removed member is told nothing.** With the serve rule it never receives the entry that removes it, and while it stays connected it is not refused either: its peers just go quiet (J4). Today it sees its removal if it is connected at the time. Should a host drop a peer it has stopped serving, so that the peer reconnects and is refused, which an app can already report?
- **Looking for peers again** when the list gains a key. It would shorten J5's tail and spare J15 its restart, and is independent of this HIP.
- **An app that changes what its list reads.** If a release moves `allow` to a table that starts empty, every established host goes by a list that names nobody. The earlier rule would have had them bootstrap. Whether such a change needs a migration step, or the record should be tied to the list it was found on.
- **`host.json` is also edited by hand.** On Node the record is a file an operator may edit, and the host now rewrites it once. Whether the position belongs in a separate file beside it.
- **What one request may ask for.** The provider takes the number of headers to send, and the list of payloads, from the request, with no limit of its own. With the check before each batch this no longer decides what a removed peer receives. It still decides how much work one request from a served peer can cause.
- **A peer that becomes served while connected** learns what it missed in a group at that group's next new entry on either side, or when it reconnects. Nothing tells the host that its list now names a peer it had been withholding from.
- **A misspelled column on a joined host** (J7) is caught at the second start. It could be reported as the host's last error as soon as the group arrives.

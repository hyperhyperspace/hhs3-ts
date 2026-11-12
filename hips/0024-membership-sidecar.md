# HIP-24: An opaque membership sidecar

- **Type:** design
- **Status:** draft
- **Modules:** rdb, std_types, rhost, mesh
- **Requires:** 23
- **Superseded by:** —
- **Implemented in:** —

## Summary

A host decides which peers to sync with from a member list that is itself synced data, kept inside the database it guards. A host whose copy of the list is behind therefore refuses the very peers that could bring it up to date, and a removed member cannot tell removal from a network fault. This HIP explores keeping membership in a second object beside the database, the *sidecar*, in a form that reveals nothing to someone outside it and so can be given to anyone. Every host syncs the sidecar with any peer, both ways, and reads the member list from it. Everything else stays behind the list, with no exception for hosts that are catching up. The idea is not worked out: this HIP states what the sidecar would have to be, what it would reveal and to whom, and the questions that decide whether it is worth building.

## Why

### Background

- **Host.** One database on one device, with its own key. An app's hosts sync a database with the hosts of its other members.
- **Entries.** Each object of a database is a DAG of signed entries. A replica checks every entry it receives, by its hash and by the rules of its object, so an entry is as trustworthy from a stranger as from a member.
- **The allow list.** A list in the app's config, read from a column of the database, such as `user.caps.grantee where label = 'member'`. A host reads it from its own replica.
- **Admit and serve** ([HIP-23](0023-join-bootstrap.md)). A host *admits* a peer when it lets it into the group of peers that sync an object. It *serves* a peer when it gives it entries. A host serves only peers on its list.
- **Bootstrapping** (HIP-23). A host that has joined a database admits any peer and takes entries from it, until the first time it finds its own key on its list. It does this once: an established host goes by its list, even if the list stops naming it.
- **Topic.** Hosts find each other by an object's id, which they announce to a tracker.

### What HIP-23 leaves open

HIP-23 lets a newcomer in. Each case below was run on 2026-11-11 against a copy of the tree with HIP-23 applied, except the third, which follows from how it works.

- **A member whose list is behind.** Bob is a member and goes offline. Carol is added and joins. Bob comes back with only Carol online. Bob is an established member, so he goes by his list. Carol is not on it, so he refuses her. Carol would serve him.
- **A second device with a key the catalog seeds.** The admin's key is on the list from the first entry. A new device with that key finds it at once, and from then on goes by a list that names only itself.
- **Nobody online knows the newcomer.** The admin adds Bob and goes offline before any other member syncs. No one online has Bob on their list, and Bob has nothing to show them.
- **A removed member learns nothing.** Its peers stop serving it. It never receives the entry that removes it, and it is not refused while it stays connected. An app can only guess from the silence.

### The root

The first two cases are one problem. A rule that decides whom to take entries from, computed from a list that only those entries can update, can freeze the list. HIP-23 avoids the freeze for a host that has just joined. It cannot do it for an established host: one that takes entries from anyone, always, gives strangers a seat in every swarm, and lets a former member deliver entries the list would have kept out.

The third and fourth cases are another. The list is confidential, so a host cannot hand it to a peer it does not recognise, and a peer it does not recognise is exactly who needs it.

Both go away if membership can be given to anyone.

## Design

This section is a sketch. *Open questions* lists what it does not settle.

### The idea

Beside each database sits a second replicated object, the sidecar. It records who is a member, and who may add or remove members, in a form that shows no keys.

- **Open sync.** A host syncs the sidecar with every peer on its topic, in both directions, with no check.
- **The list reads it.** "Is this key a member?" is answered from the sidecar.
- **Everything else is closed.** Every other object of the database keeps the plain rule: admit and serve only keys the list has. No host takes entries from a stranger for anything but the sidecar, so HIP-23's bootstrapping state is no longer needed. Its serve rule stays.

A host's copy of the list can still be behind. It is never stuck there, because any peer at all can bring it up to date.

### What the sidecar has to be

1. **Standalone.** A replica that holds nothing else of the database must be able to hold it and sync it. A table group cannot play this part: creating one takes the database's genesis, which carries the database's name, its params and its creators. The sidecar's id is computed from the database id, so that someone who was given the database id can find it.
2. **Self-validating.** Whether an entry is allowed has to be decidable from the sidecar alone. So it carries its own chain of authority: its first entry names the founding key, every entry is signed, and an entry counts when its signer held the right to make it. This is what `user.caps` does for `manager` and `inviter` today, moved into an object that needs no other.
3. **Blinded.** No entry shows a key. One way: a secret that members hold, delivered with the genesis. A key appears in the sidecar only as a tag computed from the secret and the key. The public keys needed to verify signatures are stored encrypted under the same secret. Someone with the secret can test a key against the sidecar and validate its entries. Someone without it sees tags and ciphertext.
4. **In step with the database's own tables.** The database's write rules read `user.caps`. Sync reads the sidecar. A grant or a removal has to reach both.

The secret cannot be the database id: hosts announce object ids to trackers.

RCap, the capability object in std_types, is the closest existing type: a standalone object that records which identities hold which capabilities, validates grants and revokes by who holds the managing capability, and resolves concurrent grants and revokes. A sidecar could be an RCap whose grantees are tags.

### How the open cases resolve

- **A newcomer.** Bob is given the database id. A member whose sidecar has Bob serves him the genesis, and with it the secret. Bob syncs the sidecar from that member, finds the member on it, and the two sync as members. There is no bootstrapping state.
- **A member whose list is behind, and a second device.** Bob and Carol sync the sidecar first, openly. Bob now finds Carol on it and admits her.
- **Nobody online knows the newcomer.** Bob gets the sidecar from the admin before the admin goes offline: he needs only the database id for that, not membership. Later he meets Carol, whose sidecar lacks his admission. Open sync gives it to her. She validates it, finds Bob, and serves him.
- **A removed member.** It still syncs the sidecar and still holds the secret, so it finds its own removal. An app can say "you were removed" and stop guessing.

### How opaque it can be

| Level | How | Who can test a key or follow the list |
| :--- | :--- | :--- |
| 1 | One secret for the life of the database | Anyone who ever held it: members and past members. |
| 2 | A secret replaced at every removal and delivered to the remaining members | Members. A past member keeps what it could read when it left. |

Level 2 needs a way to deliver each new secret to every remaining member, and a place to keep it that is not the genesis. Nothing in the system does that today.

At either level, anyone who can fetch the sidecar sees how many entries it has and when they arrived, which is the pace of joins and removals without the names. Anyone who knows the sidecar's topic sees which hosts answer for it.

### What changes for sync

- One object per database whose swarm has no check.
- The list is read from the sidecar.
- A host looks for peers again when its sidecar gains a member. Today a swarm looks for peers when it starts and when a peer leaves, so a host that learns of a member later does not go looking for it.

## Considered and not taken

- **Taking membership entries from anyone, one way.** A host would take the entries of the `user` group from any peer, and serve them only to peers on its list. It needs no cryptography and fixes the first two open cases. It does not fix the other two: the entries are plaintext, so a host still cannot give them to a peer it does not recognise.
- **Asking a member.** Over an authenticated connection, a member could tell a peer "as of my copy, you are a member" or "you are not". It needs no cryptography either. It needs a member online, the answer has to be trusted, and it turns a refusal into a statement. Refusals stay silent.
- **A plaintext public member list.** It resolves every case and publishes who is in each database.
- **A key to trust carried in the invitation.** It helps a newcomer and no one else.

## Open questions

- **Is level 1 enough?** A past member could test any key it knows against the list, and watch joins and removals, for as long as the database lives. If that is not acceptable, the sidecar depends on level 2, which is new cryptography.
- **Membership becomes a fact of the database.** Today `allow` is the app's choice, and two apps on one database can disagree. A sidecar has one answer. What declares it: the catalog? And what becomes of an `allow` entry that names some other column?
- **One source of truth or two.** Either the sidecar is where a member is added and the tables mirror it, or the host that writes a grant writes both. Two objects cannot be written in one step, so the second form needs a repair for a write that reached only one. And a key that is in the sidecar but holds no capability in the tables can read the database and write nothing: is that a role we want, or a state to prevent?
- **Entries from removed members.** Anyone can deliver an entry to the sidecar, at any time. A removed manager can deliver one it signed after its removal, naming only earlier entries as its predecessors. Whether such an entry counts is decided by the sidecar's rules, as it is for RCap and for tables today. What changes is reach: today such an entry travels only through a member that still syncs with its author. The rule for removals has to be judged with that door open.
- **Holders without the secret.** A newcomer carrying its own admission cannot validate what it carries. Should a host without the secret accept anything beyond what it was handed? If it does, anyone can fill sidecars with entries that only members can tell are worthless.
- **Answering for the topic.** With the sidecar, a host confirms to anyone who knows the sidecar's topic that it holds it. Today a host confirms nothing to a peer that is not on its list. The topic can be derived from the database id in one direction only, so that only someone who has the id can ask.
- **Where the secret lives.** In the genesis it is fixed for good and reaches every host that is ever served the database. Level 2 needs it elsewhere.
- **RCap or a new type.** Whether RCap's rules, with tags as grantees and sealed signer keys, are the rules the sidecar needs.
- **The shared topics.** A database's catalog and schemas are shared with every database of that catalog, and an app ships them. Today the list gates their swarms like any other. Whether it should is a separate question.

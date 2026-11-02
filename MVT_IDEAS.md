# MVT ideas — data types where monotone view types add value

A survey of data types that could be built as [monotone view types](modules/mvt/README.md), and of which ones need what MVTs add over CRDTs. Each candidate is a sketch: the invariant that concurrency breaks, the rule an MVT would apply, what it reuses from this repo, and what is still open. None of it is a spec. [`REPLICATED_SPREADSHEETS.md`](REPLICATED_SPREADSHEETS.md) is the worked example that prompted it.

## What MVTs add over CRDTs

A CRDT keeps replicas convergent by making every op commute. That removes accidental conflicts. MVTs also address essential ones ([MVT README](modules/mvt/README.md#introduction)), with four mechanisms this repo already has:

1. **Verdicts on invariants.** Bailis et al. call an invariant *I-confluent* when merging two states that each satisfy it yields a state that satisfies it. One owner per asset, a balance that stays non-negative, an acyclic tree, a unique name: none of these is I-confluent, so no set of commuting ops keeps them. An MVT admits an op where it is written and gives it a verdict at use. An op that breaks the invariant against the concurrent ops visible from `from` is void. Verdicts that depend on each other are solved together to their well-founded values, and a cycle that nothing outside it decides is void ([`VOID_SEMANTICS.md`](modules/rdb/VOID_SEMANTICS.md) §4).
2. **Authorization that changes under use.** A revoke concurrent with a use voids it ([RCap](modules/std_types/README.md#rcap--replicable-capability-system)).
3. **Coordination outcomes as barriers.** The outcome of a coordinated action is a barrier op that voids the concurrent ops of peers that did not take part ([MVT README](modules/mvt/README.md#monotone-view-types)).
4. **Versioned references.** An object records which version of another object it read, through ref-advances, and re-checks against the concurrent ones ([SOaD](modules/mvt/README.md#composability-soad-architecture)).

A type is a good MVT candidate when it needs at least one of these. A type that needs none is a CRDT on the DAG. It may still be worth building, because applications need it, but it does not test what MVTs are for.

## Candidates at a glance

| Type | Mechanisms | What it gives | Cost | Needs first |
| --- | --- | --- | --- | --- |
| [RTree](#rtree--hierarchies-with-move) | verdicts (acyclic, one parent), authorization by position | folders and outlines with move, inherited permissions | high | subtree-scoped capabilities |
| [RAsset](#rasset--ownership-and-transfer) | verdicts (one owner), coordination barrier | transfers, tickets, unique names, bookings | medium | [settlement](#cross-cutting-settlement) |
| [RApproval](#rapproval--sign-off-pinned-to-a-version) | versioned references, authorization | sign-off on an exact version, thresholds | low | foreign deps on the target |
| [RLedger](#rledger--balances-with-a-floor) | verdicts (floor) | balances, budgets, quotas, inventory | medium to high | settlement, escrow |
| [Seals](#seals) | coordination barrier | closing auctions, polls, periods; freezes | low | — |
| [RList](#plumbing-rlist) | none | ordered collections with move | medium | — |

## Plumbing: RList

An ordered list with move is CRDT-shaped, and the repo has no ordering primitive besides sorting by a column. The design is in [`REPLICATED_SPREADSHEETS.md`](REPLICATED_SPREADSHEETS.md#order-slots-placements-and-a-sibling-order): a grow-only list of slots, a placement register per id, and a sibling order that does not interleave concurrent runs (Fugue). Task lists, boards, outlines, playlists, and the spreadsheet grid need it. Nothing in it is a barrier, so its view ignores `from` and its delta bound is the meet. Build it when an application needs ordering.

## RTree — hierarchies with move

### Problem

Folders, outlines, org charts, and category trees all move subtrees. Two invariants break under concurrent moves: a node has one parent, and the tree has no cycles. Moving A into B while another replica moves B into A leaves two valid states whose merge is a cycle.

`RFileMap` keys files by path and has only add and remove, so renaming a folder removes and re-adds every file below it. An edit concurrent with the rename removes the old content at the old path and adds the new content there, so the merge has two files: the old content at the new path and the edited one at the old path. That is the duplication Kleppmann describes for a move implemented as delete plus insert. A tree keyed by node id makes a rename one op, and the edit lands on the renamed file.

### Prior art

Kleppmann et al. give a replicated tree with move: every move carries a Lamport timestamp, replicas apply moves in timestamp order, undoing and redoing when one arrives out of order, and a move that would create a cycle is skipped. It converges, but which of two cycle-forming moves survives is decided by the clock.

### Sketch

- **Ops.** `create(node, parent)`, `move(node, parent)`, and delete as a move to a trash node, as in Kleppmann et al.
- **Admission** at `(at, at)`: the node and the new parent exist, the new parent is neither the node nor one of its descendants, and the author is authorized.
- **Parent.** The parent of `n` at `(at, from)` comes from the cover of live moves of `n` at `at`. A void move is skipped, and the move below it counts.
- **Moves are barriers.** A move `m` of `n` to `p` is void at `(at, from)` when, in the tree read at `m`'s own position with the live concurrent moves visible from `from`, `p` is `n` or one of its descendants.

Two concurrent cycle-forming moves make each other void: `live(mA) = ¬live(mB)` and the reverse. That is an even negation cycle, which the verdict engine leaves undecided and then voids, so both folders stay where they were. No clock picks a winner. A longer cycle (A into B, B into C, C into A) voids every move on it, although dropping any one would do. That is conservative and symmetric, and the alternative is a tie-break among the moves.

Two concurrent moves of the *same* node give it two parents. Either both are void, by treating a concurrent move of the same node as a barrier against it, which leaves the node where it was and reports the conflict, or a deterministic winner is shown and the other move is reported. The first is symmetric and is the same rule as a double spend in [RAsset](#rasset--ownership-and-transfer): a node's parent is an asset, and a move is a transfer of it.

### Permissions by position

A capability scoped to a node covers its subtree: an editor of `/projects/x` may edit anything below it. An op on node `x` is authorized at `(pos, from)` when its author holds the capability on some ancestor of `x`, read at `pos` with the live concurrent moves visible from `from`.

A move of folder `F` under a restricted folder `R`, concurrent with an edit inside `F` by someone without access to `R`, voids the edit. Moving a folder into a restricted area acts as a revoke for everyone who loses access, and that is RCap's concurrent-void rule. A concurrent move can never authorize an op, because admission already required the author to hold the capability at the op's own position.

A mover's authority also depends on position, and a move can change the position that authorizes another move. Verdicts can then form negation cycles across moves and edits. The engine in mvt (`VerdictEvaluation`) already solves those; what is new is the ancestor query it has to run at `(pos, from)`.

### Open questions

- RCap capabilities are named, not scoped. Rdb has resource-scoped capabilities ([`CAPABILITIES.md`](modules/rdb/CAPABILITIES.md), `[PERM11]`), but not ancestry. Subtree scopes belong either in RCap or in RTree's own grant ops.
- Trashing a folder while someone edits inside it: the edit either goes to the trash with the folder, or keeps the folder, as an edit keeps a row in the spreadsheet design.
- Ancestor queries at `(pos, from)` over a DAG with verdicts are the expensive part. They need caching across the reads of one view, as RSet does for ref-advance verdicts.
- Child order, if needed, is an RList per folder.

### Cost and reuse

High. It reuses barriers, see-through covers, the verdict engine, RCap, and `walkRevisionBound`; the bound drops below concurrent moves of `start` whose verdict differs between `start` and `end`, as RCap's does for revokes. Of all the candidates it is the strongest demonstration of the thesis: broadly useful, awkward for CRDTs, and it needs both verdicts and authorization at use.

## RAsset — ownership and transfer

### Problem

An asset has one owner, and the owner transfers it. Tickets, seats, licenses, domain names, user handles, and tokens all work this way. An owner who transfers one asset to B on one branch and to C on another has spent it twice. The two states are valid alone, and their merge has two owners.

### Prior art

Blockchains order every transfer by consensus. Guerraoui et al. show that asset transfer needs less: when each account has a single owner, its consensus number is 1, and Byzantine reliable broadcast suffices. That still needs a quorum of nodes to acknowledge a transfer before anyone accepts it, so it is not available under partition. CRDTs cannot express the invariant.

### Sketch

- **Ops.** `mint(asset, owner)`, authorized by an issuer capability, and `transfer(asset, to)`, signed by the current owner.
- **Admission** at `(at, at)`: the author owns the asset at `at`.
- **Owner.** The owner at `(at, from)` follows the cover of live transfers of the asset at `at`, seeing through void ones.
- **Transfers are barriers.** A transfer is void when its author does not own the asset at its own position, or when a live transfer of the same asset is concurrent with it and visible from `from`.

Any two concurrent transfers of one asset come from a double spend: each needs its author to own the asset at its position, so their histories split at some owner who transferred on both branches. The two transfers void each other, an even negation cycle, and the verdict engine voids both. Transfers that built on a void transfer lose their owner and are void too, which is a positive dependency the engine already handles. The asset stays with the owner before the fork.

Within the ledger, the double spender ends up with the asset. Anyone who gave something off the ledger in exchange for an unsettled transfer loses it, which is why RAsset is not usable without [settlement](#cross-cutting-settlement). Two conflicting transfers signed by one owner are also a proof of equivocation; the type can expose them, and an RCap revoke can act on them.

An exchange that moves asset X from A to B and asset Y from B to A in one entry is voided as a unit, so neither side loses on the ledger. An exchange across two objects needs each to reference the other, and is open.

### Variants

- **Unique claims.** A name is an asset nobody owns until it is claimed. Two concurrent claims of one name void each other, and nobody gets it. Rdb cannot express this today: uniqueness is a negative condition ("no other live row has this name"), and rdb's restriction grammar stays positive ([`CAPABILITIES.md`](modules/rdb/CAPABILITIES.md#beyond-rcap-parity-subject-row-attenuation)).
- **Bookings.** A slot is an asset owned by the venue, and a booking transfers it. Voiding both of two concurrent bookings leaves the room empty, so a booking type should report the conflict and let an arbiter's op, a barrier, decide.

### Cost and reuse

Medium for the type: barriers, the verdict engine, RCap for issuers, and authorship. Settlement is the larger piece.

## RApproval — sign-off pinned to a version

### Problem

Someone with authority attests to an exact version of another object: a code review, a document sign-off, a contract signature, a release or schema-migration approval, a compliance attestation. An edit after the approval must not inherit it, and a revoke of the approver's authority concurrent with the approval must void it.

### Sketch

- An approval object holds approvals of one target object `T`. `approve(T, V)` names a version `V` of `T` and is signed by the approver. `withdraw` removes an approval.
- `V` is a foreign dependency of the approval (`extractForeignDeps`), so the sync layer holds the approval until `T` has `V`.
- Authorization is the permissioned-RSet pattern: the approver holds an `approve` capability on an RCap, checked at admission and again at use, so a concurrent revoke voids the approval.
- `isApproved(W, k)` holds when at least `k` live approvals name `W` itself. An edit concurrent with an approval produces a merged version the approval did not name, so it needs approving again. No special rule is needed.
- A looser variant approves `W` when `T.computeDelta(V, W)` changes nothing in a protected scope, for example when only comments changed. Each type's delta defines what its scopes can be.

### Uses beyond sign-off

A gate can read approvals: deploy only approved releases, or merge only reviewed changes. Rdb's deploy gates (`ALLOW DEPLOY IF EXISTS ...`) are the natural place to read them.

A threshold of signatures on a pinned version is also the certificate that [settlement](#cross-cutting-settlement) needs for assets, with one extra rule for witnesses: never sign two conflicting transfers. RApproval is the building block for that.

### Cost and reuse

Low. It is a permissioned RSet whose elements are `(T, V, approver)`, a foreign dependency on `T`, and a query. The new parts are declaring `T`'s version as a foreign dependency and the delta-scoped variant.

## RLedger — balances with a floor

### Problem

Accounts with balances that must not go negative: payments, budgets, quotas, rate limits, inventory. Two debits that each fit the balance can overdraw it together.

### Prior art

O'Neil's escrow method and Balegas et al.'s bounded counter split the available amount into rights held by each replica. A replica debits within its own rights without coordination, and moving rights between replicas is the only coordinated step. Indigo (Balegas et al.) generalizes this to reserve rights for any invariant.

### Sketch

- **Ops.** Credits (a mint, or a transfer from another account) and debits signed by the account's owner.
- **Admission** at `(at, at)`: the balance covers the debit.
- **Debits are barriers.** A debit's verdict reads the balance at its own position, less the live concurrent debits visible from `from`.

A group of concurrent debits that overdraws together needs a rule, and there is no single symmetric answer:

1. **Void the group.** Every debit whose verdict depends on the others is undecided, and the engine voids them all. This is conservative: nobody in the race is paid.
2. **Order the group.** Sort the concurrent debits deterministically, by entry hash, and accept them greedily until the balance runs out. This is stratified, so there are no cycles, and it keeps the most payments. For an account with one owner, only the owner authors debits, so the order only decides which of the owner's own payments go through. For a pool with many spenders, the order is a tie-break that a spender can [grind](#cross-cutting-tie-breaks).
3. **Escrow.** Give each spender rights to part of the pool. A debit within the spender's own rights never conflicts, and rights move with transfers, which are an [RAsset](#rasset--ownership-and-transfer) problem. For shared pools this is the only rule that never voids an honest debit.

A credit that turns out void (a transfer in whose source double-spent) voids the debits that relied on it: a positive dependency. The delta reports these flips (`validityChanges`), and an application that already acted on a voided payment has to compensate. MVTs make that visible; they cannot prevent it.

### Cost and reuse

Medium to high. A debit's verdict reads an aggregate over concurrent debits, not one cover, so it costs more than RSet's or RCap's per-key reads, and the delta bound drops below every concurrent debit of `start` whose verdict changes.

## Seals

### Problem

Some actions end a period of writing: closing an auction or a poll, closing an accounting period, freezing a release. Writes that the closer did not see must not count.

### Sketch

- `seal(scope)` by an authorized author is a barrier. A write concurrent with a live seal and visible from `from` is void. Admission rejects writes after the seal. The sealed content is the seal's history.
- Two concurrent seals: a write is void when it is concurrent with either, so the sealed content is the intersection of their histories. That is deterministic and symmetric.
- A reopen is an op after the seal. A reopen concurrent with a seal needs a rule; reopen-wins and seal-wins are both defensible.
- Accounting closes may prefer carry-over to voiding: a write concurrent with the close of one period belongs to the next.

For a permissioned object, a seal mostly exists already. A `delete-cap` of the object's write capability turns it off, so nobody holds it. It is a barrier, and a re-create reopens it ([RCap](modules/std_types/README.md#rcap--replicable-capability-system)). Two limits: `delete-cap` is creator-only, and it seals every object that uses that capability. A seal held by a non-creator, or scoped to one object, needs its own op.

### Composition: a poll

A vote is a per-voter register; the latest vote wins, and that part is a CRDT. Eligibility is an RCap capability, so a revoke concurrent with a vote voids it. Closing the poll is a seal. Every piece except the seal exists.

An auction is the same shape, with bids instead of votes and the winner read from the live bids at the seal. A bidder on the far side of a partition cannot know the auction closed; their bids become void when the branches merge. No coordination-free design avoids that.

## Smaller ideas

- **Derived objects.** An object computed from other objects at pinned versions: a report, an aggregate, a dataset, a build output. Its ref-advances record which inputs it read, so a reader can tell when an input has advanced and the result is stale. Cross-sheet formulas are one instance. This is mostly a pattern over existing references.
- **Schema-validated documents.** A JSON document whose edits are checked against a referenced schema version. Cross-field rules (exactly one of two fields, a field required when another is set) are not I-confluent. Rdb covers most of this for relational data.

## Cross-cutting: settlement

### Views are not final

A peer that has not been heard from can still author an op concurrent with old history. If that op is a barrier, or voids something through a verdict, old views change. The [revision bound](modules/mvt/README.md#delta-helpers-deltats-revision_boundts) does not help: it bounds what changed between two versions a replica already has, and says nothing about ops it has not received. ("Stable" in the delta-bound code means something else: a ref-advance whose published version is at or below its referenced object's bound.) RAsset and RLedger are not usable without some notion of a *settled* op, one that no later op can void.

### Honest writers: causal stability

Baquero et al. call an op causally stable once every replica has delivered it, so nothing concurrent with it can arrive later. In an open network the set of replicas is unknown, but a permissioned MVT knows who can author ops: RCap lists them. A version `v` is settled against honest writers when every principal who could author a conflicting op has an op whose history contains `v`, or has lost the capability by a revoke the reader has seen. A revoked author's ops are either in the revoke's history, so the reader has them, or concurrent with it and void, or after it and rejected. Creators cannot be revoked, so creators must acknowledge.

The set can be small. In RAsset, only owners can author transfers, and a conflicting transfer needs a double spend by an owner on the asset's chain, so a transfer is settled once the owners on its chain since the last settled transfer have acknowledged it. A writer who stays offline stalls settlement indefinitely.

### Byzantine writers: certificates

An owner can always equivocate on its own transfer: sign a second, conflicting one with the same parents. The best a coordination-free rule can do is void both, so against a Byzantine owner no amount of waiting settles a transfer. This is Guerraoui et al.'s result seen from the other side: asset transfer needs others to vouch for a transfer, though not consensus.

A certificate does that. A set of `n` witnesses, holders of a `witness` capability on an RCap, each sign at most one transfer of an asset from a given position. With fewer than `n/3` faulty witnesses, two quorums of more than `2n/3` share an honest witness, so two conflicting transfers cannot both be certified. The certificate is a barrier: an uncertified transfer concurrent with a certified one is void, and the certified one is not voided by it. That is the MVT README's coordination outcome encoded as a barrier op. Uncertified transfers stay available and provisional; certified ones are final. Changing the witness set while certificates are in flight is open.

## Cross-cutting: tie-breaks

An entry hash is under its author's control: changing a nonce or any payload field changes it, so an author can grind for a hash that wins. Rdb already breaks ties this way, in RTable's per-field values and in RSchema's slots when versions are equal.

A hash tie-break is sound when nobody gains by winning it: ordering two slots inserted at one place in an RList, choosing which of two placements a grid displays, ordering one owner's own debits. Where authors compete for the outcome (claims of one name, bids, debits from a shared pool, two moves of one node), use a rule that does not reward grinding: void every op in the conflict and report it, let an arbiter's barrier decide, or remove the competition with escrow.

Voiding both sides has a liveness cost. Two parties that retry the same claim concurrently can void each other indefinitely. An arbiter, or backoff before retrying, breaks the loop in practice.

## Where MVTs add little

- **Text and rich text, counters, registers, lists, grids.** CRDTs cover them. Build them on the DAG when an application needs them.
- **Locks and leases.** A lock's purpose is to prevent an action before it happens. Voiding the action afterwards does not help, so these need coordination. Its outcome can still be recorded as a barrier.
- **Membership and roles.** RCap covers them.

## Recommendation

1. **RApproval** first. It is cheap, it is useful on its own, and it is the certificate that settlement needs.
2. **Seals** next. For permissioned objects they are mostly `delete-cap` already; a scoped seal op is small.
3. **RTree** as the type that demonstrates MVTs best. It is the largest piece of work here, and subtree-scoped capabilities have to be designed first.
4. **RAsset** and **RLedger** after settlement is designed, since neither is usable without it.
5. **RList** when an application needs ordering, with RCalc on top of it when an application needs a sheet.

## References

- Bailis, Fekete, Franklin, Ghodsi, Hellerstein, Stoica. *Coordination Avoidance in Database Systems.* PVLDB 8(3), 2014. [10.14778/2735508.2735509](https://doi.org/10.14778/2735508.2735509)
- Kleppmann, Mulligan, Gomes, Beresford. *A Highly-Available Move Operation for Replicated Trees.* IEEE TPDS 33(7), 2022. [10.1109/TPDS.2021.3118603](https://doi.org/10.1109/TPDS.2021.3118603)
- Guerraoui, Kuznetsov, Monti, Pavlovič, Seredinschi. *The Consensus Number of a Cryptocurrency.* PODC 2019. [10.1145/3293611.3331589](https://doi.org/10.1145/3293611.3331589)
- O'Neil. *The Escrow Transactional Method.* ACM TODS 11(4), 1986. [10.1145/7239.7265](https://doi.org/10.1145/7239.7265)
- Balegas, Serra, Duarte, Ferreira, Shapiro, Rodrigues, Preguiça. *Extending Eventually Consistent Cloud Databases for Enforcing Numeric Invariants.* SRDS 2015. [10.1109/SRDS.2015.32](https://doi.org/10.1109/SRDS.2015.32)
- Balegas, Duarte, Ferreira, Rodrigues, Preguiça, Najafzadeh, Shapiro. *Putting Consistency Back into Eventual Consistency.* EuroSys 2015. [10.1145/2741948.2741972](https://doi.org/10.1145/2741948.2741972)
- Baquero, Almeida, Shoker. *Making Operation-Based CRDTs Operation-Based.* DAIS 2014. [10.1007/978-3-662-43352-2_11](https://doi.org/10.1007/978-3-662-43352-2_11)
- Kleppmann, Howard. *Byzantine Eventual Consistency and the Fundamental Limits of Peer-to-Peer Databases.* 2020. [arXiv:2012.00472](https://arxiv.org/abs/2012.00472)
- Weidner, Kleppmann. *The Art of the Fugue: Minimizing Interleaving in Collaborative Text Editing.* IEEE TPDS 36(11), 2025. [10.1109/TPDS.2025.3611880](https://doi.org/10.1109/TPDS.2025.3611880)

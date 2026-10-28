# Critique of the Phase 5 plan

This is a review of [`PHASE_5_PLAN.md`](./PHASE_5_PLAN.md). Section numbers cite that plan. The check was a reading of the walk, the permissioned-set view and its admission, the RCap holding rule, and the bounded-delta floor. Nothing was implemented or run.

A position is a set of entries. $\downarrow P$ is the causal past of $P$, including $P$. An entry is **after** $P$ when it lies outside $\downarrow P$ and above every head of $P$. It is **concurrent** with $P$ when it lies outside $\downarrow P$ and is not after $P$. A version is **at or below** another when the first is in the past of the second. **Strictly dominates** means "at or above, and not the other way around."

---

## 1. What a permissioned set is deciding

A permissioned set does not store grants. It stores a pointer, the **capability ref**, at an [`RCap`](../std_types/src/types/rcap/view.ts) object whose own history holds identities, grants, and revokes. A **ref-advance** is an entry in the set that publishes a new RCap version for that pointer. Advances are monotonic: the published version is at or above the version each parent already observes. They are barriers, so a read can see an advance that is concurrent with the entry it is judging. Writers publish the minimal cover, and admission rejects a version that is not an antichain.

A read is a pair $(at, from)$. $at$ is the cut whose membership is being asked. $from$ is the horizon of evidence the reader is willing to use. For an element entry $E$ the set answers two questions in the RCap history:

- **refAt**, the version $E$ is judged at. It is folded from the ref-advances in $\downarrow E$ and the ref-advance barriers concurrent with $E$ and visible from $from$.
- **refFrom**, the evidence horizon inside RCap. It is folded from the ref-advances in $\downarrow from$ only. Concurrent advances do not widen it.

RCap then asks whether $E$'s author holds the capability at $(\mathit{refAt}, \mathit{refFrom})$. The holding rule has two parts. The **cover** at $\mathit{refAt}$ is the latest grant or revoke in that past. **Kill 2** is a live revoke of the same capability that is concurrent with $\mathit{refAt}$ and lies in $\downarrow\mathit{refFrom}$. Either one failing means the author does not hold it. When the two arguments are the same version, Kill 2 is empty: nothing in a version's own past is concurrent with it. A creator holds every capability, and that answer is returned before any revoke is consulted (`holdingIn` in `rcap/view.ts`).

`canonicalVersion` drops a head that some other head in the same fold is strictly above. Two advances that both land in one fold, one strictly above the other, already behave as if the lower one were absent. That is ordinary canonicalization. It is not see-through.

Rdb splits the same problem into two layers, and the plan keeps the split. Merging them is what would make two benign advances void each other.

**Layer 1** judges one advance. The base is the version that advance publishes, folded with the advances in its own causal past. A concurrent advance widens the base only when its published version strictly dominates the base and the widener is itself live. An advance that does not strictly dominate stops the walk. Monotonicity puts every version below it at or below its own version, so nothing further down can strictly dominate the base either. Liveness is asked only of candidates that do strictly dominate. Two advances that publish incomparable versions never consult each other. The walk already has this shape in `RTableGroupImpl.resolveObserveGateRefAt` (`group.ts`, the method that starts just after the observe-gate comment, lines 699–734). A predicate that returns true keeps the entry and stops; a predicate that returns false skips it and continues into its predecessors (`findConcurrentCoverUsing` in `dag/src/idx/concurrent_cover.ts`).

**Layer 2** is the fold a reader uses for an element. A void advance contributes no version and does not hide a live advance below it, whether or not its version dominated anything. $\mathit{refFrom}$ stays the geometric fold, with no liveness filter. Filtering decides which versions take part in $at$. It does not decide how far the evidence horizon reaches. A void advance in $\downarrow from$ still imports the revokes in its published version. That is the conservative direction: hiding those revokes would help the principal who was revoked.

The plan's proposal is that split, applied to the set:

- Lift the Layer 1 walk into `mvt/src/refs.ts` and call it from rdb unchanged.
- An advance is live when its author would pass today's admission rule at the Layer 1 cut, read from the geometric $\mathit{refFrom}$.
- Admission of the advance itself moves from the parent cut to $(\mathit{newRefVersion}, \mathit{newRefVersion})$.
- Element authorization see-throughs on the Layer 1 verdict. The public reference version, and the inputs of the delta floor, stay geometric.

That proposal matches the code it cites, and it is the same rule rdb already runs for a gated observe. The holes are in the argument that the delta floor needs nothing, and in two places an implementer will build the wrong fixture.

---

## 2. Verdict

Build the lift, the verdict, and the admission move as the plan describes them. Ship the floor repair in the same change. The plan's reason for one change still holds: a replica that admits the new way and folds the old way disagrees on which heads a set observes. The new fold is also what makes the floor wrong, so a replica that folds the new way and walks the old floor disagrees on which membership changes a delta reports.

What already holds:

- The walk to lift is the one in `resolveObserveGateRefAt`, including the uncanonicalized base, the predicate `widening(z) === undefined || isLive(z)`, and `canonicalVersion` on the result. `findForkPosition` is a function of the causal past, so dominance against a base that still contains a dominated ancestor agrees with dominance against its minimal cover. Canonicalizing the base inside the lift would be a behavior change dressed up as a move.
- Admission really does resolve the parent cut. `validateRefAdvancePayload` takes a view at $(at, at)$, resolves the capability ref there, and calls `hasCapability` (`validate.ts`, the block after the creator short-circuit). `validateObserveGate` already evaluates the gate at $(\mathit{newRefVersion}, \mathit{newRefVersion})$. At that pair Kill 2 is empty, so admission fails only for a revoke in the past of the imported cut. A revoke concurrent with the imported cut is caught when the advance is read. That is rdb's split, and it is the right one here.
- `hasCapability` returns a boolean. It mints one evaluation, and a cycle inside that evaluation collapses there. The set's verdict calls it as a top-level read. The advance does not join an RCap component, and it does not need a `VerdictEvaluation` of its own.
- `[PSET17]`, `[PSET20]`, `[PSET24]`, and `[PSET25]` stay. In those tests the advances are made by the creator, `refAdvanceCreators` defaults to true, and a creator is live before any revoke is read. See-through has nothing to remove. `[PSET25]` is Kill 2 at the entry's $\mathit{refAt}$ from a geometric $\mathit{refFrom}$; its comment should say that, as the plan asks.
- The public `resolveRefVersion` stays geometric, which is what `[PSET24]` asserts. The causal see-through fixture in §5 is the one that shows a void advance the canonical union does not already drop, and the version test on the floor does reach it. That half of §4 is right.

---

## 3. The floor misses a flip this phase creates

### 3.1 What the floor promises today

A bounded delta walks backward from `end` and stops at a floor. An entry in the floor is not visited, and neither are its predecessors (`walkEntriesBackwardsToBound` in `mvt/src/delta.ts`). The full delta walks all of $\downarrow\mathit{end}$. Both strategies then call `has` on the start view and the end view for every element mentioned by an entry they actually visited (`RSetDeltaAccumulator.ingest`). An element whose only entry sits below the floor is invisible to the bounded delta.

For a permissioned set the floor comes from `computeObserverRevisionBound`. It resolves the geometric RCap version at the set's fork meet and at `end`, asks RCap for the delta between those two versions, and projects that delta's revision bound $b$ back into the set (`projectForeignBound` in `mvt/src/refs.ts`). RCap's own bound is the meet of `fork.common` on the RCap history. `common` is the shared entries that have a successor on only one side (`dag_defs.ts`), so every RCap entry that is new between the two versions attaches at one of them, and $b$ is at or below every attachment. A new RCap entry is therefore after every version at or below $b$. It is never concurrent with such a version.

`projectForeignBound` starts at the ref-advance cover of the set meet and walks toward the past. An advance is **stable** when its published version is at or below $b$, and unstable otherwise. A stable advance stops the descent. The floor is the lowest unstable advance on each branch. Below a stable advance, monotonicity puts every earlier published version at or below that advance's version, hence at or below $b$. An advance whose liveness is a function of its own published version cannot flip while it is stable: a new revoke that would change the answer is after that version, and Kill 2 does not see it.

The causal fixture in §5 is this case. $Y$ publishes $v_{adv}$, the later import puts the revoke into $\mathit{refFrom}$, and the revoke is concurrent with $v_{adv}$. Then $v_{adv}$ is above $b$, $Y$ is unstable, and the descent continues through $Y$ to the live advance below it. The element sitting above $Y$ is visited. Full and bounded agree. That argument can stay.

### 3.2 The plan's residual case, and the element it does not consider

§4 then considers an advance whose verdict reads its widened cut. A live concurrent advance $C$ publishes $v_C$ above $b$, while $Y$'s own version $v_Y$ is at or below $b$. A new revoke, after $v_Y$ and concurrent with $v_C$, voids $Y$ even though the version test calls $Y$ stable.

The plan says this is not new. Its proof takes an element $E$ causally after $Y$, assumes $E$ is skipped, and concludes that $C$ is concurrent with $E$. That conclusion is right for an element after $Y$: $C$ after $E$ would be after $Y$, and then $C$ would not be concurrent with $Y$. So $C$ is in $E$'s fold. $v_C$ strictly dominates $v_Y$, the canonical union already drops $v_Y$, and see-through of $Y$ changes nothing about $E$. Elements after $Y$ do not flip because of this phase.

The element that flips is concurrent with $Y$, and $C$ is after that element. The proof never looks at it. Its fold contains $Y$ and does not contain $C$, so the canonical union keeps $v_Y$.

There is a second constraint the proof does not mention, and it decides the direction of the flip. Admission of an add consults the causal version at the parent, with $from = at$. A grant that exists only on an advance concurrent with the add is invisible there, and the add is rejected. See-through of that concurrent advance falls back to the causal version that was enough to admit the element, so the element stays. The removals in the plan's own fixtures are the other pattern: the void advance is in the add's causal past, it is the version the add was admitted against, and the version underneath does not authorize the author. The causal fixture is that pattern, and §3.1 already has the floor visiting it. The mixed cut is the same pattern read directly with `has`.

The flip the floor misses runs the other way, and it can be admitted. A concurrent advance publishes a version strictly above the causal one. The canonical union replaces the causal version with it, and the higher version is where the element's author has lost the capability. While the advance is live the element is absent. When the advance is void, see-through restores the causal version and the element is present.

### 3.3 The history

RCap, with the attacker a non-creator who holds the ref-advance capability, and $C$ and $Z$ made by the creator:

```text
L     Bob holds write. The attacker holds the ref-advance capability.
└─ rb  revoke Bob's write
   └─ vY
      ├─ vC
      └─ r   revoke the attacker's ref-advance capability, concurrent with vC
```

$r$ is after $v_Y$. At $v_Y$ itself the attacker still holds the capability, so $Y$ is admitted. Bob does not hold write at $v_Y$, because $rb$ is in its past. Bob does hold write at $L$: $rb$ is after $L$, so it is neither in the cover at $L$ nor concurrent with $L$.

The set. $M$ is an ordinary merge, with no ref of its own, so the ref-advance cover at $M$ is $\{C, Y\}$.

```text
A  creator publishes L
├── E  Bob adds the element, admitted against L
│   └── C  creator publishes vC ─┐
└── Y  attacker publishes vY ────┴─ M  start
                                      └── Z  creator publishes {vC, r}; end
```

$C$ is after $E$ and concurrent with $Y$. $v_C$ strictly dominates $v_Y$, which strictly dominates $L$. $Z$ is after $Y$, so $Z$ is not a widening candidate for $Y$. $Z$ is how $r$ gets into $\mathit{refFrom}$.

Read $E$ from $M$, before $Z$ exists. The causal fold at $E$ is $L$. The concurrent fold from $M$ contains $Y$ and does not contain $C$, because $C$ is after $E$. The canonical union is $v_Y$. Bob's write is revoked there. $E$ is absent. This is the start view.

Read $E$ from $Z$. $Y$'s Layer 1 base is $v_Y$. $C$ is concurrent with $Y$, live, and strictly above, so the widened cut is $v_C$. $\mathit{refFrom}$ is $\{v_C, r\}$. $r$ is concurrent with $v_C$ and revokes the attacker, so $Y$ is void. See-through falls from $Y$ to $A$, and the version is $L$. Bob holds write at $L$, read from $\{v_C, r\}$, because $rb$ is after $L$. $E$ is present.

Today $Y$ is not void. Both views resolve $E$ at $v_Y$, and $E$ is absent in both. The two delta strategies agree that nothing changed. After this phase the full walk visits $E$ and reports it added. That report is the new behavior.

The bounded walk does not visit $E$. The RCap delta from the version observed at $M$ to the version $Z$ publishes runs from $v_C$ to $\{v_C, r\}$. The new entry is $r$, attached at $v_Y$, so $b$ is $v_Y$. $v_C$ is above $b$ and $v_Y$ is at or below it. The ref-advance cover at $M$ is $\{C, Y\}$. $Y$ is stable, and the descent stops. $C$ is unstable, and the advance below it publishes $L$, which is at or below $b$, so the floor on that branch is $C$. The walk from $Z$ enters $C$ and goes no further. $E$ is a predecessor of $C$, and $E$ is the only entry that carries the element. The bounded delta reports nothing.

$Y$ being stable is why the other branch does not save the walk. The floor does not contain $Y$, so $Y$ itself is visited, but $Y$'s predecessors lead to $A$, not to $E$.

### 3.4 The repairs named in §4 miss this history

§4 offers two repairs if parity fails: treat an advance or an element as unstable when its concurrent fold contains an unstable advance, or stop at the meet when such a fold exists.

$E$'s concurrent fold contains $Y$, and $Y$'s published version is at or below $b$. The unstable advance $C$ is after $E$, so it is not in that fold. A test that looks for an unstable advance inside the element's own fold does not fire.

Stopping at the meet stops at $M$. $E$ is in the shared past below $M$. The walk still does not enter it. The meet is the frontier of what both versions have already seen. $E$'s membership changes because the horizon moved, not because $E$ itself is new.

Marking $Y$ unstable, on the grounds that its widened cut is $v_C$ and $v_C$ is above $b$, puts $Y$ on the floor or descends $Y$'s branch to $A$. $E$ is not on $Y$'s branch. The floor on $C$'s branch remains $C$, still above $E$.

The guarantee the floor has to grow is: visit every element whose ref fold contains an advance whose Layer 1 cut is above $b$. $E$'s fold contains $Y$, and $Y$'s Layer 1 cut is $v_C$. The published-version test cannot see that. Whatever the repair is, this history is the one it has to walk through $E$, and a disagreement on it blocks the phase. Recording it against a follow-on leaves bounded deltas silent on a membership change the full delta reports.

The generator sentence in §5 does not build the history. A non-creator advance concurrent with a higher live advance, plus a revoke concurrent with the higher version, is the widening. The element has to be concurrent with the lower advance, causally before the higher one, admitted against the lower causal version, and absent at that lower version because the higher concurrent version revoked its author. The existing generators revoke `write` and advance as the creator (`rset_generate.ts`, `syncRefAdvance`). A revoke of `write` does not void a ref-advance. The new capability has to be the one named in `refAdvance`.

---

## 4. The advance that imports the revoke is the creator's

Admission at $(\mathit{newRefVersion}, \mathit{newRefVersion})$ sees every revoke in the past of the imported cut. The causal fixture says a later advance publishes $\{v_{adv}, r\}$, where $r$ revokes the attacker's ref-advance capability. Read as the attacker's op, that advance is rejected: $r$ is in the past, and the attacker is a non-creator. The fixture cannot be appended under `selfValidate`.

The creator can append it. `refAdvanceCreators` defaults to true (`RSetImpl.refAdvanceCreators`), and the creator short-circuit returns before the capability loop. With the flag off, a creator still passes `hasCapability` whenever the capability list is non-empty, for the same reason: `holdingIn` answers the creator before it reads a revoke. The later import in §5, the admin advance in the mixed-cut fixture, and $Z$ in §3 are all this op. The plan says the attacker is a non-creator for the two flip fixtures. It should name the author of the import as well. An implementer who lets the attacker publish the import will see admission fail and conclude the scenario is impossible.

The same fixture has to keep the fall-through live. $L$'s version sits below the fork of $r$, which the delta paragraph in §4 already requires and the test bullets do not say. If the fork is below $L$ and $L$'s author is the attacker, Kill 2 voids $L$ too, and the element falls through somewhere else. Placing $L$ with the creator, or below the fork, keeps the fall-through the test describes.

---

## 5. The cycle check has to run before the memo

The verdict is a promise per advance, stored on the view. Two `has` calls on one view share it, which is what the concurrent-read test is for. Storing the promise before the first await is the right way to close the gap where both callers miss an empty map: an async call runs until its first await and returns its promise in the same turn, and the caller can store that promise before it awaits.

The chain check has to happen before the map is consulted. The promise is already in the map by the time a recursive call looks. Returning it waits on the caller that is waiting for the recursive call. That is the hang the plan wants the chain to prevent, reached by the order of the two checks. The chain is the closure of that call. A set stored on the view is the in-progress flag the plan correctly rejects: two reads computing different advances would see each other's entries and throw.

Strict dominance makes the throw unreachable. The check is there so a real cycle fails instead of waiting. `hasCapability`'s throw stays uncaught, as the plan says. There is nothing at the set layer that would make an undecided RCap holding decided.

---

## 6. Two sentences that will be built the wrong way

**"Admission of adds and deletes stays geometric."** The capability check does. `checkPayloadAuth` resolves the capability ref on a view at $(at, at)$ and does not learn about the verdict. The existence check does not stay geometric. `validateAddPayload` and `validateDeletePayload` call `hasByHash`, and `hasByHash` authorizes through `rcapViewForEntry`. Once that fold see-throughs, an add's "element already exists" and a delete's "element does not exist" see through a void advance in the causal past. That is the right outcome: the element they accept or reject is the element a reader at that cut will see. The sentence as written says the opposite, and an implementer can spend the change trying to keep a second, geometric `has` for validation.

**"[PSET17] and [PSET24] are that shape."** The shape in that paragraph is two concurrent advances, the later one strictly above the earlier, with the canonical union dropping the earlier heads. In `[PSET17]` the second advance is causally after the first and concurrent with the add. Both versions land in the add's fold, and the later one dominates, so the union drops the earlier heads anyway. The conclusion stands: those tests stay, and a passing membership assertion there is not coverage of see-through. The label is the `[OCUT01]` shape, which the plan separately, and correctly, says not to add.

---

## 7. Edits in the plan before implementation

In §4, keep the argument that an advance whose verdict reads only its published version is stable exactly when that version is at or below $b$, and keep the causal fixture as the demonstration. Replace the residual paragraph. The element concurrent with the stable advance and causally before the widener flips from absent to present, the history in §3 can be admitted, and full and bounded disagree. The two repairs named there do not visit it.

In §5, name the creator as the author of every advance that publishes the revoke of the attacker. Pin $L$ below the fork of $r$. Describe the parity history as the one in §3, not only as a pair of advances and a revoke. A disagreement on it blocks.

In the exit, drop the sentence that a residual disagreement does not block. The floor repair ships with the verdict.

In §4's list of geometric callers, say that the capability check for an add or a delete stays geometric, and that `hasByHash` see-throughs on every view, including the existence check during admission.

In §2, state the order: the chain contains the advance, or the stored promise is returned. The chain belongs to the call.

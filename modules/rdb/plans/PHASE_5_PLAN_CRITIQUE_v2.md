# Critique of the Phase 5 plan, v2

This reviews [`PHASE_5_PLAN.md`](./PHASE_5_PLAN.md) and [`PHASE_5_PLAN_CRITIQUE.md`](./PHASE_5_PLAN_CRITIQUE.md), called "the first critique" below. Every history here was traced by hand against the code it cites. None was built or run. Notation is the first critique's.

---

## 1. Settled

These points of the first critique hold against the code. The plan should take them as written.

- **The widened-verdict history is real.** Call it H2 (first critique §3.3). Admission of $Y$ passes at the parent cut and at $v_Y$ (`rset/validate.ts` 165–173). The concurrent search for $E$ from $M$ keeps $Y$ and passes over $C$, because $C$ is after $E$ (`concurrent_cover.ts` 57–93). RCap's bound is the meet of `fork.common`, here $v_Y$ (`rcap/delta.ts` 187). The RSet floor is the projection itself (`rset/delta.ts` 219). The descent marks $Y$ stable and floors $C$'s branch at $C$ (`refs.ts` 253–275). $E$ is reachable only through $C$. After this phase the full delta reports $E$ added and the bounded delta reports nothing. Today both report nothing.
- **The plan's two candidate repairs do not reach H2.** The projection starts at the ref-advance cover of the meet and falls back to the meet (`refs.ts` 253, 276), so it is always at or below the meet. "Stop at the meet" is a shallower floor than today's. A test on the element's own concurrent fold does not fire, because $C$ is not in that fold.
- **Fixture authorship.** Under the new admission anchor, an advance that publishes $r$ cannot be the attacker's: $r$ is in the past of $(\mathit{newRefVersion}, \mathit{newRefVersion})$. The import in the causal fixture is the creator's. $L$ is the creator's too, or Kill 2 voids it along with $Y$.
- **Chain before memo.** A recursive call that reads the memo first finds its caller's pending promise and waits on itself. The chain check runs first. The check catches a cycle on one chain only; a cycle split across two concurrent top-level reads would still wait. Strict dominance rules out both. Do not claim the check covers the second.
- **The existence checks see through.** `validateAddPayload`, `validateDeletePayload`, and `validateUpdatePayload` (`validate.ts` 209–215, which the first critique omits) call `hasByHash`, which picks up see-through. The capability check in `checkPayloadAuth` goes through the public `resolveRefVersion` and stays geometric.
- **The `[PSET17]` / `[PSET24]` label.** In both tests the first advance is in the add's causal past and the second is causally after it and concurrent with the add. They are not two concurrent advances. The conclusion stands.

---

## 2. The floor is unsound today

The first critique frames the floor problem as a flip this phase creates. The plan says the concurrent-fold gap predates this phase. The plan is right about that, and the gap is wider than either document says. It shows up with only creator advances, and the simplest case needs no projection below the meet.

### 2.1 H1: an unstable advance above the meet

RCap, every op by the creator:

```text
L     Bob holds write
├─ vW
└─ r   revoke Bob's write, concurrent with vW
```

The set:

```text
A  creator publishes L
├── E   Bob adds the element, admitted against L
│   └── s1  any op
│       └── Z  creator publishes {r}
└── W   creator publishes vW
```

`start` is $\{s1, W\}$ and `end` is $\{Z, W\}$. End extends start.

- Start view. $E$'s causal fold is $L$. $W$ is concurrent with $E$ and visible, so $\mathit{refAt}$ is $v_W$. $\mathit{refFrom}$ is $v_W$. Bob holds write. $E$ is present.
- End view. $\mathit{refAt}$ is still $v_W$; $Z$ is after $E$. $\mathit{refFrom}$ is $\{v_W, r\}$. $r$ is concurrent with $v_W$, so Kill 2 fires. $E$ is absent.
- Bounded. `fork.common` is $\{s1\}$, so the set meet is $s1$. $\mathit{refAtMeet}$ is $L$, $\mathit{refAtEnd}$ is $\{v_W, r\}$, and $b$ is $L$. The ref-advance cover at $s1$ is stable, so the projection returns $s1$. The walk enters $Z$, $W$, and $A$ and stops at $s1$. $E$ is only below $s1$. The full delta reports $E$ removed; the bounded delta reports nothing.

The projection only classifies advances in the past of the meet. $W$ is in the past of `start` and not of the meet. It is never classified, and an element below the meet that is concurrent with it is skipped. The comment in `mvt/src/delta.ts` (77–80) that the meet is safe for any object does not hold for an observer whose reads widen with concurrent barriers.

### 2.2 H1b: the plan's residual shape, built

RCap, every op by the creator: $p$ after $L$, $v_X$ after $p$, $v_F$ after $L$ and concurrent with $p$, and $r$ after $p$, revoking Bob's write, concurrent with $v_X$.

The set: $A$ publishes $L$. Bob's $E$ follows $A$. $F$ follows $E$ and publishes $v_F$. $X$ follows $A$ and publishes $v_X$. $M$ is an ordinary op merging $F$ and $X$ (`start`). $Z$ follows $M$ and publishes $\{v_X, v_F, r\}$ (`end`).

$b$ is $p$. $F$ and $X$ are both unstable, and $A$ below each is stable, so the floor is $\{F, X\}$. $E$ is reachable only through $F$. At start, $\mathit{refAt}$ for $E$ is $v_X$ ($X$ is concurrent with $E$) and Bob holds write. At end, $r$ is in $\mathit{refFrom}$ and concurrent with $v_X$, and $E$ is absent. The bounded delta misses it.

### 2.3 What follows

- Old fold and old floor already disagree on H1 and H1b. The first critique's reason to ship the floor repair inside this phase is that a replica folding the new way and walking the old floor would disagree. A bounded delta is a local computation, and the disagreement exists today without Phase 5. The floor repair is a standalone bug fix. It lands first, with H1 and H1b as parity tests that fail before it.
- H2 still blocks Phase 5. A repair that only classifies every advance whose version is above $b$ fixes H1 and H1b and does not see $Y$.
- Rdb uses the same projection (`combineObserverRevisionBounds`, `rtable_group/delta.ts` 192), and its foreign-table reads widen with concurrent observation barriers (`resolveForeignTableViewIn`). H1 likely has an rdb analog. Gated observes already run the Layer 1 widening, so H2 may too. This has not been traced; the floor change has to check it.

---

## 3. The guarantee the floor needs

The first critique states it as: visit every element whose ref fold contains an advance whose Layer 1 cut is above $b$. A Layer 1 cut is computed with liveness, and liveness is what flips, so the cut at one view does not say whether the verdict is stable.

Sketch, not built. $Y$ and $C$ are concurrent, with $v_C$ strictly above $v_Y$, both at or below $b$. $D$ is after $Y$ and concurrent with $C$, with $v_D$ strictly above $v_C$ and above $b$. A new revoke concurrent with $v_D$ voids $C$'s non-creator author, so $C$ is live at start and void at end. $Y$'s cut is $v_C$ at start and $v_Y$ at end. Both are at or below $b$. A revoke of $Y$'s author after $v_Y$ and below $v_C$ makes $Y$ void at start and live at end.

State it geometrically and recursively. An advance is **verdict-stable** when its version is at or below $b$ and every concurrent advance that strictly dominates it, visible from `end`, is verdict-stable. Strict dominance makes the recursion terminate. An element whose folds consult only verdict-stable advances cannot flip.

---

## 4. A repair to evaluate

Let $U$ be every ref-advance in $\downarrow\mathit{end}$ whose version is not at or below $b$. Close $U$: add every ref-advance barrier concurrent with a member, seen from `end`, until nothing changes. Dropping the dominance filter is conservative. The floor is the meet of the set's fork meet and every member of $U$.

Why it is enough, sketched. An entry in the common past of every member is neither after nor concurrent with any member. Its causal and concurrent folds contain no member. Every advance in those folds has no widening candidate in the closure, or it would have been added. Every holding the entry reads is therefore at a cut at or below $b$, and no new RCap entry is concurrent with such a cut.

- H1: $U = \{Z, W\}$. The meet with $s1$ is $A$. $E$ is walked.
- H1b: $U = \{F, X, Z\}$. The meet is $A$.
- H2: $U = \{C, Z\}$, and the closure adds $Y$. The meet is $A$.
- A chain of advances, which is all `rset_generate.ts` produces (`syncRefAdvance` always appends at the set frontier): the closure adds nothing, and the floor is the lowest unstable advance, today's floor.

Cost: classification descends from the ref-advance cover at `end` instead of the meet, plus one concurrent-cover search per closure member. `projectForeignBound` needs `end` and the concurrent search, so its signature changes.

---

## 5. Edits

**A new change, before Phase 5: the floor.**

- H1 and H1b as permissioned-set parity tests. Both fail today.
- The repair in §4, in `projectForeignBound` / `computeObserverRevisionBound`, stated against the guarantee in §3.
- Trace the rdb analog through `combineObserverRevisionBounds`.
- Fix the `mvt/src/delta.ts` comment.

**`PHASE_5_PLAN.md`.**

- §4: replace "Why the delta floor needs no change" and the residual paragraph with a pointer to the floor change and the guarantee in §3.
- §4, the geometric list: the capability check of an add or delete stays geometric. The existence checks of add, delete, and update see through.
- §2: the chain check runs before the memo lookup. The chain belongs to the call.
- §5: the creator authors every advance that publishes a revoke of the attacker, and $L$. Add H2 to delta parity, blocking. The generator needs a non-creator ref-advancer holding a cap named in `refAdvance`, and advances appended below the frontier.
- "`[PSET17]` and `[PSET24]` are that shape": the second advance is causally after the first and concurrent with the add.
- Exit: drop "a disagreement on the residual shape alone does not block."
- Order: the floor change first.

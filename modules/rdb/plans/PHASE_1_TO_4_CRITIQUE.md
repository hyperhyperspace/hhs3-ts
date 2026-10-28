# Critique: Phases 1-4 of the ANY implementation

**Scope:** the uncommitted implementation of Phases 1-4 of
[`DESIGN_AND_PLAN_FOR_ANY.md`](./DESIGN_AND_PLAN_FOR_ANY.md), checked against
that document and the implementation plan (`any_switch_implementation`). The
review is a code reading only: nothing was built or run.

Notation, as in the design document:

- $\downarrow P$ is the causal past of a position $P$, including $P$ itself.
- "After $P$" means outside $\downarrow P$ and above every head of $P$.
- "Concurrent with $P$" means outside $\downarrow P$ and not after $P$.

---

## 1. Summary

Most of the work matches the plan.

- **The DAG primitive is correct.** `dag/src/idx/concurrent_cover.ts` handles
  the empty position, dominated heads, and heads outside $\downarrow from$, and
  the three indexes call it.
- **Admission and canonicalization are in place.** All four admission sites run
  `validateRefVersionShape` before monotonicity, all five writers canonicalize
  before signing, and every computed version the plan lists is canonicalized.
- **RCap and RSet follow the plan's pseudocode.** `holdingIn`, `slotTruth`,
  `definition()` and `hasByHash` implement what the plan specifies.

The findings, most serious first:

| # | Finding | Kind |
| :--- | :--- | :--- |
| 1 | `NestedScopedDag.findConcurrentCoverWithFilter` drops out-of-scope heads | unplanned semantic change, breaks the design's invariants |
| 2 | Kill 1 is "in the cover", so a barrier in $\downarrow at$ masked on its own branch loses its effect | gap in the plan, shows up in RCap definitions and RSet |
| 3 | A voided revoke and a surviving origin combine to resurrect a revoked grant | consequence of 2, plus `opVerdict`'s `managedBy` read |
| 4 | `[CAP08]` flipped, and the documents still say only `[CAP29]` flips | documentation |
| 5 | Planned tests are missing | test coverage |
| 6 | Some returned versions are still not canonical | residual |
| 7 | Nits | cleanup |

---

## 2. Finding 1: nested scopes drop out-of-scope heads

### 2.1 What the code does

The plan does not mention this change, and no test covers it directly.

```text
// modules/mvt/src/dag/dag_nesting.ts, NestedScopedDag
async findConcurrentCoverWithFilter(from, concurrentTo, meta, predicate) {
    inScope = { h in concurrentTo : entry(h) matches scope.baseFilter() }
    heads   = inScope.size > 0 ? inScope : concurrentTo
    return parent.findConcurrentCoverWithFilter(from, heads, scopeFilter ∧ meta, predicate)
}
```

The intent, from its comment, is sound: an entry of a sibling scope should not
make an in-scope barrier concurrent with a cut that the barrier is otherwise
after. Under ANY, any head the barrier has not seen makes it concurrent, so an
unrelated sibling op at the outer frontier would make every nested barrier
apply there.

### 2.2 Why it is wrong

Filtering the *heads* is not the same as restricting the *position* to the
scope. The heads say nothing about the in-scope history that lies below an
out-of-scope head. Three properties the design relies on fail.

Nested entries are appended at the parent's frontier
(`NestedScopedDag.append`, `after = this.dag.getFrontier()`). Likewise, the
parent appends at a frontier that includes nested entries. So the case below is
the ordinary one, not a contrived one.

**(a) Not a function of the causal past.** Let $n_1$ be an in-scope entry and
$s_1$ a parent or sibling entry appended after it, so $n_1 < s_1$. Let $r$ be an
in-scope barrier appended at $\{n_1\}$, concurrently with $s_1$, and visible
from `from`.

| `concurrentTo` | Heads used | $r$ |
| :--- | :--- | :--- |
| $\{s_1\}$ | $\{s_1\}$ (nothing in scope, so raw) | concurrent, **applies** |
| $\{n_1, s_1\}$ | $\{n_1\}$ | after, **does not apply** |

Both positions have the same causal past. The randomized test `[META_09]`
would fail on `NestedScopedDag`, but it only runs on the raw indexes.

**(b) Not monotone.** The same table read the other way: adding the head $n_1$
to $\{s_1\}$ makes $r$ stop applying. Knowing more removes a barrier. This is
the "rescue" that §3.1 property 3 of the design rules out.

**(c) In-scope history behind an out-of-scope head is ignored.** Let $n_1$ and
$n_2$ be concurrent in-scope entries, and $s_1 > n_2$ an out-of-scope entry.
For `concurrentTo` $= \{n_1, s_1\}$ the heads become $\{n_1\}$, so $n_2$ is
forgotten. A barrier $r$ after $n_1$ that has not seen $n_2$ counts as after the
position, although the position's in-scope past contains $n_2$. That is the
leniency of the old union-of-futures rule, reintroduced inside nested scopes. In
the observe and RCap setting it is the same shape as the Mixed-Cut Attack
(`[OCUT02]`).

Since the RSet rewrite (Phase 4) now runs kill 2 against `this.at`, not
`version(add)`, every nested RSet membership read goes through this code. So do
nested RCaps, if any.

### 2.3 Suggested fix

Superseded by section 1 of [`PHASE_1_TO_4_FOLLOW_UP_PLAN.md`](./PHASE_1_TO_4_FOLLOW_UP_PLAN.md): a scoped concurrent cover delegates to the root DAG, and the scope filter only selects matches. The projection below is not the fix.

Project the position onto the scope, instead of filtering its heads. The
projection is the maximal in-scope entries of $\downarrow P$, which
`NestedScopedDag.findMinimalCover` already computes
(`this.dag.findCoverWithFilter(p, this.scope.baseFilter())`):

```ts
async findConcurrentCoverWithFilter(from: Position, concurrentTo: Position, meta: EntryMetaFilter, predicate?: EntryPredicate): Promise<Position> {
    const projected = concurrentTo.size === 0 ? concurrentTo : await this.findMinimalCover(concurrentTo);
    const heads = projected.size > 0 ? projected : concurrentTo;
    return this.dag.findConcurrentCoverWithFilter(
        from, heads, joinFilters(this.scope.baseFilter(), this.scope.wrapFilter(meta)), predicate,
    );
}
```

Why this is right:

- **It depends only on the causal past.** The projection's past is
  $\downarrow(\text{scope} \cap \downarrow P)$, which depends only on
  $\downarrow P$.
- **It is monotone.** If $\downarrow P \subseteq \downarrow Q$, the projection
  of $P$ lies below the projection of $Q$. Barriers that reach $P$ therefore
  still reach $Q$.
- **It keeps the comment's intent.** Sibling entries contribute only the
  in-scope history below them, never themselves.
- **"In the past" is unchanged for in-scope entries.** An in-scope $x \in
  \downarrow P$ lies below some maximal in-scope entry of $\downarrow P$. The
  query only matches in-scope entries, so excluding $\downarrow \text{proj}(P)$
  is the same as excluding $\downarrow P$.

**The empty projection.** When $\downarrow P$ holds no in-scope entry, the scope
has not started at $P$. The sketch above keeps today's fallback to the raw
heads, which preserves whatever behavior the nested tests rely on for views at
the scope's start (for example `version(nestedSet.getId())` in the nested-set
tests). This leaves one discontinuity: going from an empty projection to a
non-empty one can still drop a barrier. The fully consistent alternative is to
treat an empty projection as the empty position, so nothing is concurrent with
it. Choose after finding which nested test motivated the current change. Both
are strictly better than filtering heads.

**Cost.** One extra `findCoverWithFilter` per call. It is skipped for the empty
position, and it could be skipped when every head is already in scope and they
form an antichain.

### 2.4 Tests to add

Superseded by section 1 of [`PHASE_1_TO_4_FOLLOW_UP_PLAN.md`](./PHASE_1_TO_4_FOLLOW_UP_PLAN.md). The tests below describe the projection, which is not the fix.

- A `[DNS_*]` test in `mvt/test/dag_nesting_tests.ts` with the table in §2.2(a).
  The concurrent cover at $\{s_1\}$ and at $\{n_1, s_1\}$ must agree.
- A nested version of `[META_09]`: random `concurrentTo` over a DAG mixing two
  scopes and parent entries. The nested call on $S$ must equal the call on the
  raw minimal cover of $S$.
- The §2.2(c) shape as an RSet test: a nested barrier delete after $n_1$ that
  has not seen $n_2$ removes an add when read at $\{n_1, s_1\}$.
- An RSet test pinning the intended sibling isolation: a sibling-scope op at the
  outer frontier does not make a nested barrier delete apply.

---

## 3. Finding 2: kill 1 misses barriers masked on their own branch

### 3.1 The gap

The plan implements kill 1 as "a live barrier op is in the see-through cover at
`at`". The old code was anchored on the positive op: a positive op survived
unless a barrier visible from `from` was concurrent with it. The design argues
that dropping the positive-anchored search loses nothing (§3.5, "Dropping B1
loses nothing"). The argument is sound for a barrier *outside* $\downarrow at$.
For a barrier *inside* $\downarrow at$ it assumes the barrier is in the cover.
That holds only when nothing on the barrier's own branch masks it.

| Object | Can a barrier in $\downarrow at$ be masked? | Effect |
| :--- | :--- | :--- |
| RCap holding (grants and revokes) | No. A later revoke is itself a live revoke in the cover. A later grant is a legitimate re-grant. | none |
| RCap definition (`create-cap`, `delete-cap`) | Yes, by a re-create after the delete | the concurrent old create stays an origin |
| RSet membership | Yes, by a re-add and then a *non-barrier* delete | the concurrent add survives |

**RCap definition.** `create-cap` $c_1$ is on branch X. On branch Y, a
`delete-cap` $d$ concurrent with $c_1$ is followed by a re-create $c_2 > d$. At
any `at` containing $c_1$ and $c_2$:

- The cover is $\{c_1, c_2\}$, and $d$ is masked by $c_2$.
- Kill 2 does not fire, because $d \in \downarrow at$.
- So `origins` $= \{c_1, c_2\}$, where the old code had $\{c_2\}$.

The capability exists either way. But grants recorded against $c_1$ now hold,
although $d$ is a barrier concurrent with $c_1$. That contradicts the
`std_types` README ("`delete-cap` is a barrier that voids grants tied to the
deleted capability's origin"). The plan noted the `capOrigins` change for
`validateGrant` as a deliberate rule change, but not this consequence.

**RSet.** A set with `supportBarrierDelete` can mix barrier and non-barrier
deletes (`deleteByHash` and `deleteWithBarrierByHash`). Add $a$ is on one
branch. On the other, barrier delete $d$ is followed by re-add $a_2$ and then
non-barrier delete $nd$. At $\{a, nd\}$:

- The cover is $\{a, nd\}$, and $d$ is masked.
- Kill 1 finds no barrier delete in the cover.
- Kill 2 does not fire, because $d \in \downarrow at$.
- `hasByHash` returns **true**. The old code returned **false**, because $d$ is
  concurrent with $a$.

A re-add on its own is harmless, since the element is present through $a_2$
under either rule. It takes the non-barrier delete on top to expose $a$.

### 3.2 Suggested fix

Kill 1 should mean "a live barrier in $\downarrow at$ concurrent with the
positive op", not "a live barrier in the cover". The two agree except when the
barrier is masked. Kill 2 stays use-anchored, against `at` from `from`.

Restore the positive-anchored check, restricted to $\downarrow at$ (that is,
with `from = at`):

```text
RCap readDefinition, for each positive candidate c in the cover:
    drop c if findConcurrentCoverWithFilter(at, version(c), {caps: [capName], barrier: ['t']}) is non-empty

RSet hasByHash, for each add a in the cover (barrier-delete mode):
    drop a if findConcurrentCoverWithFilter(at, version(a), {barrier: ['t'], elmts: [h]}, predicate) holds a delete
```

Then:

- The "delete-cap in the cover" and "barrier delete in the cover" shortcuts
  become special cases and can stay as fast paths.
- The walks run under `at`, not `from`, so they cost less than the old ones.
- RCap holdings need no change (see the table in §3.1).
- The behavior the plan *intended* is unchanged: `[CAP28]`, `[CAP29]`,
  `[CAP37]`, `[CAP38]`, `[SET02d]` and `[SET02e]` keep their outcomes.

Alternatively, if masking is accepted as the intended semantics, state it in
the design document and the README, and pin both shapes with tests.

### 3.3 Tests to add

- RCap: the $c_1 \parallel d < c_2$ shape. A grant with origins $\{c_1\}$ does
  not hold at the merge, and `currentCapCreationVersion` is $\{c_2\}$.
- RSet: the $a \parallel d < a_2 < nd$ shape. The element is absent at
  $\{a, nd\}$.

---

## 4. Finding 3: a revoked grant can be resurrected

This is an edge case, but it affects security. It is the RCap shape of §3.1
combined with how `opVerdict` reads `managedBy`.

Setup:

- `create-cap` $c_1$ for `write`, then a grant $g$ to alice with origins
  $\{c_1\}$, then a revoke $r$ of that grant by a *non-creator* manager. All on
  branch X.
- On branch Y, concurrently, the creator deletes `write` ($d \parallel c_1$),
  then re-creates it ($c_2 > d$).
- Read at $\{r, c_2\}$ from the same position.

What happens:

1. At the merge, `origins` $= \{c_1, c_2\}$ (§3.1), so $g$'s origin survives.
2. `opVerdict(r)` builds `new RCapViewImpl(target, version(r), this.from)`.
   At `version(r)` from the merge, $d$ is concurrent, so `readDefinition`
   returns `ABSENT` and `getManagedBy` returns `[]`.
3. With no managers, $r$ is void as `unauthorized`.
4. The cover for alice's slot sees through $r$ to $g$, which is live. **Alice
   holds `write`.**

The old code returned false. Its `getManagedBy` ignored delete barriers, so $r$
was authorized and live.

The two halves disagree: the merge treats $c_1$ as a live definition, while
$r$'s own verdict treats the same definition as deleted.

**Suggested fix.** The fix in §3.2 is enough on its own: $c_1$ drops out of the
origins, $g$ is void, and the result is false again. Even with it, consider
reading the *managed* capability's `managedBy` at the op's own position with
`from = version(hash)`:

```ts
const managers = (await new RCapViewImpl(this.target, version(hash), version(hash)).getManagedBy(p.capName))
    .filter((mgr) => mgr !== 'creator');
```

The holding check on the *managing* capability (`opView.holdingIn(..., mgr,
hash)`) must keep `this.from`, so that a concurrent revoke of the author's
authority still voids the op. Only the lookup of who manages `p.capName` changes.
Whether a revoke is authorized should not depend on a concurrent deletion of the
capability it revokes: revoking a deleted capability is harmless, and voiding the
revoke is what opens this hole.

**Test.** Add the setup above as an RCap test. Alice does not hold `write` at the
merge.

---

## 5. Finding 4: `[CAP08]` flipped, and the documents don't say so

`[CAP08]` used to be "Concurrent createCap and unrelated delete does not void
initial origin". It is now "A delete concurrent with the view position removes
the capability there", and its assertion flipped to false.

The setup: `create-cap deploy` and `delete-cap write` are concurrent, off the
same frontier. The view is at `version(createDeploy)`, from the latest frontier.

The view position is a single head, so ANY is not the cause. The flip comes from
Phase 3's switch of the definition read to the use point: the old check asked
whether the delete was concurrent with the *genesis origin*, and it was not. The
new kill 2 asks whether it is concurrent with *`at`*, and it is. That follows the
modernization ("a concurrent negative op wins"), and it matches the RSet case
"barrier delete concurrent with `at`, causally after the add: gone". So the new
outcome is defensible. But the documents disagree with it:

- `DESIGN_AND_PLAN_FOR_ANY.md` §1 ("Only `[CAP29]` fails"), the §4 table
  ("Existing tests that flip"), and §4 reason 5 all say `[CAP29]` is the only
  std_types flip.
- The §3.5 expected-outcomes table has no row for this case.
- `MODERNIZATION_v2.md` has a corrected status note, but its body still says
  `[CAP28]` "flips to true" and that `[CAP29]` stays (around lines 66-74 and
  264-269). The plan asked for the definitional paragraph and the expectations
  to be updated, not only a status note.

**Suggested fix.** Add a §3.5 row: "`delete-cap` concurrent with `at` and
causally after the origin → capability absent (`[CAP08]`)". Update §1 and §4 to
list `[CAP08]` as a Phase 3 flip. Update the body of `MODERNIZATION_v2.md` (the
`[CAP28]` paragraph, the §5 test list, and the "future is the union" sentence)
to the ideal rule.

---

## 6. Finding 5: missing planned tests

| Planned test | Phase | Status |
| :--- | :--- | :--- |
| Two reads whose foreign `from` differ only by a dominated ancestor share a verdict frame | 2 | missing |
| A foreign table view's `getVersion()` and `getFromVersion()` are antichains | 2 | missing |
| An undecided grant above a live revoke (the hidden path in `slotTruth`) | 3 | missing. `[CAP39]` covers only a live revoke *beside* an undecided grant. |
| Schema param barrier after one head of an observer `at` | 1 | missing (optional in the plan) |
| `getManagedBy` agrees across replicas for two concurrent creates | 3 | `[CAP38]` is single-replica. Acceptable, since the rule is a deterministic hash order. |

Also:

- **`[META_08]`.** `aboveBoth` repeats the `aboveOne` call with the same
  arguments, so it asserts nothing new. Test the "after both heads" case on its
  own: with `from = {m}` and `concurrentTo = {a, b}` the result must be empty.
- **`[META_09]`.** It runs one seed with 40 nodes, and only with
  `from = frontier`. Add a second seed and a few random `from` positions. The
  `from`-side walk is where a head outside $\downarrow from$ matters.
- **The `slotTruth` hidden path.** The branch where an undecided grant
  dominates a live revoke (`hidden = true`) currently has no coverage. The
  undecided-above-live-revoke fixture from `MODERNIZATION_v2.md` is the test
  that would exercise it.

---

## 7. Finding 6: returned versions that are still not canonical

The plan's canonicalization list (§3.4(b) of the design) is fully done. The
design's reason for canonicalizing ("callers that compare or display versions
see the dominated ancestor") also applies to these sites, which return
`resolveRefVersionAtPosition` raw:

| Site | Used by |
| :--- | :--- |
| `RTableGroupImpl.resolveObservedForeignVersion` (`group.ts`, around line 756) | the group view's `resolveRefVersion` |
| `rblob_store/view.ts`, line 23 | the blob store view's `resolveRefVersion` |
| `rfile_map/view.ts`, line 38 | the file map view's `resolveRefVersion` |
| `rfiles/access.ts`, line 174 | the bound-group version used by RFiles access checks |

**Suggested fix.** Canonicalize each against the referenced group's causal DAG,
as `resolveForeignTableViewIn` now does. Alternatively, record in the design
document that view-level `resolveRefVersion` may return a non-antichain, and say
why.

---

## 8. Finding 7: nits

- `version` is still imported in `std_types/src/types/rset/view.ts`, but no
  longer used.
- `[CAP18]` is still named "Candidate-based delete barriers only void concurrent
  capability origins", which describes the removed per-candidate rule. If §3.2 is
  adopted, the name becomes accurate again, restricted to $\downarrow at$.
- `diagnoseObserve` canonicalizes `refAt`, which `resolveObserveGateRefAt`
  already returns canonical. This costs nothing for a single head; drop one of
  the two.
- `[OCUT13]` sits before `[OCUT12]` in `observed_cut_tests.ts`.
- `RCapViewImpl.definitions` caches the promise, so a rejected read stays cached
  for the life of the view. Views are short-lived, so this is fine. Note it if
  views ever become long-lived.
- `ABSENT.origins` is one shared `Set`. All current readers copy it or only read
  it. A comment, or `Object.freeze` on a wrapper, would keep a future caller from
  mutating it.

---

## 9. Suggested order of work

1. **Finding 1** (nested projection), with its tests. It decides replicated
   semantics for every nested RSet read, and it is the one place where the
   design's invariants do not hold.
2. **Finding 2** (positive-anchored kill 1 within $\downarrow at$). This also
   closes **finding 3**. Optionally, add the `opVerdict` `managedBy` change.
3. **Findings 4 and 6** (documents and canonical returns).
4. **Findings 5 and 7** (tests and cleanup).

Findings 1 and 2 change verdicts. Like Phases 1-4, they need to ship to all
replicas together.

# The observe floor misses a row

A bounded group delta stops too early. A row below the floor can change its
verdict between `start` and `end`, because an observe the floor never
inspects has changed the foreign cut the row is judged against. The full
delta reports the flip. The bounded delta does not. Replicated state is
fine: a direct read at either horizon is correct, and every replica computes
the same short delta. The bug is the early stop.

The random parity sweep does not build this shape, so a green sweep is not
evidence against it. History A is built and checked by
[`test/observe_floor_proof.ts`](./test/observe_floor_proof.ts). History B is
not in that script.

---

## What the floor decides

`computeDelta` on a group walks backward from `end` and stops at a floor
(`walkEntriesBackwardsToBound`). Two channels only see entries that walk
reached:

- `computeOpVerdictFlips` compares `isEntryVoided` at `start` and at `end`.
- The per-table accumulator records a row id only when a walked entry
  mentions it, then compares `deltaRowState` at the two horizons.

An entry below the floor contributes neither an op-verdict flip nor a row
change, whatever those two reads would have said.

The floor comes from `combineObserverRevisionBounds`, which projects each
observed object's own bound through `projectForeignBound`. That projection
starts at the ref-advance cover of the set meet and walks toward the past.
An observe is stable when the version printed on it is at or below the
foreign bound. A stable observe stops the descent. The result is never
deeper than the meet, and an observe that is concurrent with the meet is
never classified at all.

A row's verdict does not use that printed version alone. `diagnoseEntry`
resolves the foreign table at the row's own position, from the horizon, with
void observes filtered out (`resolveSchemaForeignTableViewIn`,
`filterVoided` true). An observe concurrent with the row and visible from
`end` widens that cut. A later observe can put a foreign delete into the
horizon. The delete is concurrent with the widened cut, and the row's
`exists` check fails. None of that requires the widening observe to sit
below the meet, which is the only place the floor looks.

The comment on `RTableDeltaAccumulator` says at-use flips sit above this
floor and that a causal-past target can no longer revise an old row. The
history below is a concurrent observe, not a causal-past target.

---

## History A, the one to build first

No gate. One foreign group, one observer, an `exists` restriction. This is
the smaller bug: the observe that widens the row stays live. Nothing is
voided except the row.

Foreign group `G`, table `caps` with `concurrentDeletes: true`:

```text
g    insert the grant row
├─ vW   filler; the grant is still present
└─ r    delete the grant, concurrent with vW
```

Observer `O`, ungated binding `users` → `G`. Table `items`, insert
restricted by `exists` on `users.caps` where `label = grant`. The same
shape as `itemLaterImport` in `test/observed_cut_tests.ts`.

```text
A    observe {g}
└─ E    insert the item, admitted against A
   └─ s1   any local op that is not an observe
W    observe {vW}, concurrent with E
```

`start` is `{s1, W}`. `end` is `{Z, W}`, where `Z` is after `s1` and
observes `{r}`. `end` extends `start`. `Z`'s parent is `s1`, whose causal
observe is `{g}`, and `r` is after `g`, so the observe is monotonic.
`{vW}` is not a parent of `Z` and is not required.

At `start` the item's foreign cut is `vW`: `W` is concurrent with `E` and
visible, and `vW` is above `g`. The grant is present. `r` is not in the
horizon. The insert is live.

At `end` the cut is still `vW`, because `Z` is after `E`. The horizon now
includes `r`. `r` is concurrent with `vW` and deletes the grant. The insert
is void.

The set meet of `(start, end)` is `s1`. The foreign version at `s1` is `g`.
The foreign version at `end` adds `vW` and `r`, so the foreign bound is `g`.
The only observe in `s1`'s past publishes `g` and is stable, and the
projection returns `s1`. `W` is a head of `start`, not an ancestor of `s1`,
so it is never classified. The walk enters `Z` and `W` and stops at `s1`.
`E` is reachable only through `s1`.

The full delta walks `E` and reports the insert flipping from live to void.
The bounded delta has no `opVerdictChanges` entry for `E`, and no row
change, because the insert is the only entry that carries that row id.

---

## History B, one observe voids another

Rdb already gates observes, so this does not wait on the permissioned-set
work. Build it after History A. A failure of A already proves the floor.
B proves that filtering void observes makes a second row flip the floor
cannot see.

`G` is a Users group. `O` binds it with `canObserve` requiring a live
manager cap, as in `makeGatedApp`. Alice is a manager at `L` and is revoked
on a branch concurrent with a strictly later version `vC`.

```text
L     Alice holds manager. The witness row the item needs is present.
└─ vY  the witness has been deleted
   ├─ vC
   └─ ry  revoke Alice, concurrent with vC
```

```text
A    the creator observes {L}
├─ E    Alice's item, admitted against A
│  └─ C    the creator observes {vC}
└─ Y    Alice observes {vY}
   the parents of M are C and Y; M is not itself an observe
   └─ Z    the creator observes {vC, ry}
```

`start` is `M`. `end` is `Z`.

Alice's observe is admitted at `vY`: the revoke is after `vY`, so it is not
in that cut. The creator's observe of `{vC, ry}` is admitted because a
creator passes the gate. Alice cannot publish that cut.

While `Y` is live, the item's foreign cut is `vY`, which dominates `L`, and
the witness is gone, so the insert is void. `C` is after `E`, so it is not
in the item's fold. At `end`, `C` widens `Y` to `vC`, `ry` is concurrent
with `vC` and visible from `Z`, and `Y` is void. See-through falls back to
`A`, the cut is `L`, and the witness is present. The insert is live.

`vY` is at or below the foreign bound and `vC` is above it, so the
projection treats `Y` as stable and stops `C`'s branch at `C`. The walk
visits `Y` and can report `Y`'s own observe flip. It does not visit `E`.
The full delta reports the insert flipping from void to live. The bounded
delta does not.

---

## The proof script

[`test/observe_floor_proof.ts`](./test/observe_floor_proof.ts) builds
History A and nothing else. It is not registered in `test/run_tests.ts`, so
`npm test` does not run it. From `modules/rdb`:

```bash
node --import ../../register.mjs ./test/observe_floor_proof.ts
```

The script reads the item at `(start, start)` and at `(end, end)`. It
requires the item to be live and then absent, and `isEntryVoided` to agree.
If that read does not flip, the script throws `diagnosis wrong` and does
not look at the deltas.

On today's code the read does flip. The script then runs `computeDelta`
twice. Exit 0 means the bug reproduced: the full delta has one
`opVerdictChanges` entry for the insert (`voidBefore` false, `voidAfter`
true), and the bounded delta has none. A throw after the read, where both
deltas report the flip, means the floor reached the insert. A throw where
neither delta reports it means the full walk missed the insert too.

History B is still only the section above. A repair that classifies every
observe whose printed version is above the foreign bound can make History A
go red while leaving History B broken, because `Y`'s printed version is at
or below that bound.

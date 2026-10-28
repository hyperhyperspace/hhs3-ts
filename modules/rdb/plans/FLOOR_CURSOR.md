# The delta floor and the cursor cache

**Status:** proposal. It restates the floor of [`RDB_OBSERVE_FIX_PLAN.md`](./RDB_OBSERVE_FIX_PLAN.md) as three conditions, and adds a cache for a consumer whose end is the current head. The batch computation in that plan stays the definition. A missing cache is recomputed from the meet.

---

## 1. The floor

`computeDelta(start, end)` requires that `end` extend `start`. Write \(H(P)\) for the causal history of a position, including itself. The meet is the greatest lower bound of `fork.common`: the frontier of the intersection of the histories of the shared entries that have a direct successor on only one side. Every new entry is strictly after that meet.

A barrier is an entry whose tag participates in a cross-branch read: a delete, an observe, or a schema deploy. An observe \(X\) of an object \(o\) publishes a version \(v_o(X)\). It is stable when \(v_o(X) \le b_o\), where \(b_o\) is this same floor on \(o\) between the foreign version visible at the meet and the foreign version visible at `end`. Observes are monotonic, and bindings form a DAG, so the recursion is well-founded. An object with no observes has no \(b_o\).

Let \(U\) be the past of the floor, including the floor. The walk is \(H(\mathit{end}) \setminus U\). \(U\) is sound when:

1. **Meet.** \(U \subseteq H(\mathit{meet})\). It holds no new entry.
2. **Barriers.** Every barrier in \(H(\mathit{end}) \setminus U\) is strictly after every entry of \(U\).
3. **Observes.** Every observe in \(U\) is stable.

The greatest such \(U\) is the floor. These sets are closed under union.

The meet is what keeps new entries in the walk. The cut `end` itself satisfies (2) and (3) vacuously, and walking nothing misses every new entry. A chain \(E \to s1 \to N\) with no barrier has meet \(\{s1\}\) and floor \(\{s1\}\).

A barrier outside the meet can still reach an entry inside it. In the local history the meet is \(\{s1\}\), the delete \(T\) is concurrent with \(E\) below that meet and with the new delete \(N\), and \(E\) flips. Condition (2) pulls the floor to \(\mathrm{preds}(T) = \{x\}\).

An observe the floor kept would not be walked, so an unstable observe has to sit strictly after the floor. The floor moves to its predecessors. On \(A \to E \to s1\) with meet \(\{s1\}\), an unstable \(A\) sets the floor to \(\mathrm{preds}(A)\), and both \(A\) and \(E\) are walked. Observes outside the meet are barriers, and condition (2) handles them.

## 2. Computing it

Start at the meet and lower. \(\mathrm{glb}\) is the frontier of the intersection of the two histories. An empty floor walks everything.

```text
floor := meet

for each observed object o:
    b_o := floor of o between the foreign version at the meet and at end
    for each lowest unstable observe X of o in H(meet):
        floor := glb(floor, preds(X))

repeat:
    exposed := entries newly outside the floor
    for each barrier Z in the walked region:
        if preds(Z) is not at or above floor:
            floor := glb(floor, preds(Z))
until the floor stays
```

The lowest unstable observes are the ref-advance cover under the meet, descending through unstable publishes and stopping at a stable one. A barrier that is already strictly after the floor stays after every lower floor, so a later pass only looks at barriers the last lowering exposed.

In History B the unstable observe \(C\) lowers the floor to \(\mathrm{preds}(C)\), which exposes the barrier \(Y\), which lowers it again to \(\mathrm{preds}(Y)\).

## 3. The cursor cache

A cursor \(c\) is the start a consumer has already applied. For the projector that is the checkpoint. The cache is the pair \((c, G)\) with \(G = \mathrm{floor}(c, F)\) and \(F\) the head. It lives with the checkpoint. Two consumers have two pairs. A call whose end is not the head ignores the cache and runs §2.

While \(c\) stays fixed and the head grows from \(F\) to \(F' = H(F) \cup \{n\}\), the floor only moves toward the origin, or stays:

- The meet moves toward the origin or stays. The only new one-sided successor is \(n\), so `common` gains parents of \(n\) that lie in \(H(c)\), and the glb of a larger set is closer to the origin.
- \(b_o\) moves toward the origin or stays. The foreign interval widens on the end, and it widens on the start when the meet drops.
- Conditions (2) and (3) only get stricter. The new greatest \(U\) is a subset of the old one.

Seed the pair when the cursor equals the head. There is nothing new, and \(G = c\). On the next entry:

```text
G := glb(G, meet(c, F'))
if n is a barrier and preds(n) is not at or above G:
    G := glb(G, preds(n))
recompute each b_o that widened
for each observe in H(G) that is now unstable:
    G := glb(G, preds(X))
run the barrier scan on the entries that just left H(G)
```

The new entry is not always the barrier that moves the floor. With cursor \(\{s1, T\}\) and seed \(G = \{s1, T\}\), appending \(N\) drops the meet to \(\{s1\}\) and exposes the old barrier \(T\). The scan then lowers \(G\) to \(\{x\}\).

When the cursor advances to the head, drop the pair and seed \(G\) at the new cursor. The floor is allowed to move toward the frontier on that step. After \(E \to s1 \to N\) with floor \(\{s1\}\), a cursor that catches up to \(N\) seeds the next floor at \(N\). Keeping \(\{s1\}\) walks the prefix again. Keeping a floor from before a new concurrent barrier is wrong in the other direction: the true floor can now sit closer to the origin than the stored one, and stopping at the stored cut misses the flip.

The empty position is a sound floor and a useless seed. \(\mathrm{glb}\) with it stays empty, so every later head walks its whole past. The creation op \(r\) is a valid seed only for the cursor that is still \(r\), and only as the equal-horizon value \(G = \mathrm{floor}(r, r)\). A later cursor seeded at \(r\) never rises off it.

Each \(b_o\) is a floor on the observed object for the pair (foreign version at the local meet, foreign version at the local head). It is not that object's sync cursor. While the local cursor is fixed, that pair only widens, so the foreign value only moves toward the origin and the same lowering applies across whatever foreign entries entered the interval. One local observe can add more than one of them. When the local cursor advances, those foreign values are dropped with the local pair.

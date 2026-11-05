# HIP-16: Seniority order for negation cores

- **Type:** design
- **Status:** draft
- **Modules:** mvt, rdb, std_types
- **Requires:** —
- **Superseded by:** —
- **Implemented in:** —

## Summary

The engine collapses a residual negation core, the entries the well-founded model leaves undefined in cycles that nothing outside them decides, to void. This HIP proposes picking a canonical model for each core by a total seniority order instead: make the most senior contestant live, propagate, and repeat, which gives a unique extension for even cycles and an imposed answer for odd ones, as a value-based argumentation framework does. The key is the authorizing capability's insert position, fixed at grant time, so a later attacker cannot grind the result. The components, the fixpoint and the read rules stay as they are.

## Design

What §4 collapses to void is exactly the residual negation core: entries the
well-founded model leaves undefined, in cycles that nothing outside them
decides. Collapsing them is the conservative choice (every cap on a revoke ring
survives). A more opinionated resolution would instead pick a canonical model
for each core by a **total value order**: set the most-senior contestant live,
propagate (its live deletes void the junior caps it targets), repeat. With a
*total* order this yields a unique extension for even cycles and a
deterministically *imposed* answer for odd cycles (which have no stable model).
This is exactly a **value-/preference-based argumentation framework**: a total
preference order guarantees a unique extension even in the presence of odd
cycles. It would replace step 2 of the solve; the components, the fixpoint and
the read rules stay as they are.

### The value (seniority) order

The seniority key must be the **authorizing cap's insert position** (fixed at
grant time), not the revoke op's position — otherwise an attacker who controls
how they sequence revokes could grind the result. "Ancestor wins; concurrent
breaks by entry hash" is the canonical linear extension of the causal order.

Two caveats for a general engine:

- **Cross-DAG totality.** A single DAG's `findForkPosition` totally-orders
  positions *within* that DAG. Components stay inside one group today (§4), but
  a type whose cycles can span DAGs would need an explicit inter-DAG tiebreak
  (e.g. dag-id then hash), and uniqueness for such cycles would rest entirely on
  it.
- **Non-grindability is the type's obligation.** A generic engine can guarantee
  the order is total and deterministic; it cannot guarantee it is non-grindable.
  That property comes from feeding it the cap-insert position + rowId, both fixed
  at grant time and neither controlled by the later attacker.

The order would be a `value` oracle passed to the mvt engine next to `diagnose`:
the mechanism stays type-agnostic, and the *policy* (what the value of a node
is) stays the type's job.

## Open questions

- **An order across a binding.** An rdb core that holds an observe already spans two objects: the importer's authority is a row of the observer, and the deleter's is a capability in the bound group. Ranking the bound group's authority by where the observer first imported it can be ground, since an author can back-date an import of their own grant. A rule fixed at creation cannot: authority a group holds itself outranks authority it reads from a bound group, so an entry cannot use its view of a binding to block an update of that binding ([HIP-14](0014-observer-frame-ref.md) §7, item 3; `mvt/WELL_FOUNDED_VERDICTS.md` §6.5).
- **A member tried live must satisfy its own rule.** What to do when no member does is a question for the order in general; odd cycles raise it too.

## History

Extracted on 2026-11-04 by [HIP-1](0001-hips.md) from `modules/rdb/VOID_SEMANTICS.md` §6, "Future direction (not implemented)", readable at commit `f45f1e3`. `mvt/WELL_FOUNDED_VERDICTS.md` §3.5 and §6.5 discuss the same order among the rules considered.

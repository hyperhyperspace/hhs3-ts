# HIP-19: Replicated spreadsheets (prior art toward RCalc)

- **Type:** informational
- **Status:** active
- **Modules:** mvt
- **Requires:** —
- **Superseded by:** —
- **Implemented in:** —

## Summary

A survey of the spreadsheet-CRDT literature toward an `RCalc` monotone view type: which pairs of concurrent operations on a grid must both survive, which fact each operation writes (membership, order, value, range), how each existing design represents the grid, and what transfers to an MVT with stable row and column identities, an order per axis, a value per cell and ranges that follow those identities.

A survey of the spreadsheet-CRDT literature for an `RCalc` monotone view type, in the same family as [`RSet`](../modules/std_types/README.md#rset--replicable-set), [`RCap`](../modules/std_types/README.md#rcap--replicable-capability-system), and [`Rdb`](../modules/rdb/README.md), and what it implies for that type.

`RSet`, `RCap`, and `Rdb` are histories on an append-only DAG. A view is read at `(at, from)`: the rules are evaluated at the op's own position, now seeing every concurrent barrier visible from the frontier. Admission checks an op where it is inserted. Use re-checks it whenever a later frontier can void it. There is no wall clock and no central sequencer. `RCalc` would be that machine applied to a grid: stable row and column identities, an order on each axis, a value per cell, and ranges that follow those identities.

The literature below is about what a grid must do when two edits are concurrent. Most of it assumes either a central server (Google Sheets, Notion) or a library of Yjs types. The part that transfers is the **conflict table**: which pairs of operations must both survive, and which fact (membership, order, value, range) each operation writes.

## Sources

| Source | What it contributes |
| --- | --- |
| Yanakieva, Bird, Bieniusa. *A Study of Semantics for CRDT-based Collaborative Spreadsheets.* PaPoC 2023, pp. 37–43. [10.1145/3578358.3591324](https://doi.org/10.1145/3578358.3591324) | Which spreadsheet operations fail to commute, and why a Yjs nested array or nested map does not give update-wins for "edit a cell whose row was deleted". |
| Bieniusa. *Local-first Software.* BOB Konf 2025. [slides](https://bobkonf.de/2025/slides/bieniusa.pdf), slide 61 | The same pairs drawn as a matrix: non-commutative, commutative, or implementation-dependent. |
| Pfeil, Scandurra, Haas. *AegisSheet: A Compositional CRDT for Collaborative Spreadsheets.* PaPoC 2026, pp. 40–46. [10.1145/3806077.3806695](https://doi.org/10.1145/3806077.3806695) (CC-BY) | Empirical Google Sheets and Notion outcomes for every pair of `EditCell` / `Insert` / `Remove` / `Move`, graded desirable / suboptimal / destructive, plus the semantics their CRDT implements. Nicolás Hatcher (IronCalc) showed this table in his Local-First Conf 2026 talk, *Local first collaborative Spreadsheets. Are CRDTs useful?* The `tabular-rdt` README in [Bismuth `Documentation/Tabular`](https://github.com/stg-tud/Bismuth/blob/main/Documentation/Tabular/README.md) describes the same design. |
| Weidner. *Designing Data Structures for Collaborative Apps* (2022). [mattweidner.com](https://mattweidner.com/2022/02/10/collaborative-data-design.html) | The compositional grid: rows and columns are opaque ids in movable lists, where a move sets a per-row position register, and cells are a map keyed by `(row, column)`. |
| Weidner. *CRDT Survey, Part 2: Semantic Techniques* (2023). [mattweidner.com](https://mattweidner.com/2023/09/26/crdt-survey-2.html) | The same grid. A deleted row or column cascade-forgets its cells. Under update-wins collections it cites Yanakieva et al. for the user who edits cells of a column another user is deleting, and probably wants the column back. |
| Kleppmann. *Moving elements in list CRDTs.* PaPoC 2020. [10.1145/3380787.3393677](https://doi.org/10.1145/3380787.3393677) | A move implemented as delete-then-insert duplicates the element under concurrent moves. Position and identity have to be separate: a move writes a per-element position register. |
| Weidner, Kleppmann. *The Art of the Fugue: Minimizing Interleaving in Collaborative Text Editing.* IEEE TPDS 36(11), 2025. [10.1109/TPDS.2025.3611880](https://doi.org/10.1109/TPDS.2025.3611880), [arXiv:2305.00583](https://arxiv.org/abs/2305.00583) | Most list CRDTs can interleave two runs inserted concurrently at one place. Fugue and FugueMax avoid it. In a grid, the runs are two blocks of rows pasted at the same spot. |
| Sypytkowski. *Conflict-free Replicated Spread Sheets* (2023). [bartoszsypytkowski.com/crdt-tables](https://www.bartoszsypytkowski.com/crdt-tables/) | A practical last-writer-wins-per-cell table with fractional indexes, versioned by a hybrid logical clock with a client-id tie-break. A row delete clears only the cells whose versions it dominates, so a newer concurrent edit keeps the row. |
| Coblenz, Yim, et al. *Kale: A Transformation-Safe Spreadsheet System.* 2026. [arXiv:2608.26345](https://arxiv.org/abs/2608.26345) | Single-user. Formula references break when a range is a rectangle of coordinates. Kale stores absolute references as row ids it assigns, resolved at parse time, and restricts references to a cell, a whole column, or a whole row. No replication. |

Kale does not bear on merge policy. It bears on what a formula is allowed to name, which is the same stable-id question the CRDT papers answer for structure.

## What conflicts

Bieniusa's slide 61 draws Yanakieva et al.'s classification of operation pairs, for their Yjs models where a cell is addressed by index. The slide is a triangle; it is mirrored here. **C** is commutative, **N** non-commutative under the stated condition, **I** implementation-dependent.

|  | Insert col | Delete col | Insert row | Delete row | Edit cell |
| --- | --- | --- | --- | --- | --- |
| **Insert col** | N: same position | C | I | I | C |
| **Delete col** | C | N: same column | I | I | N: same column |
| **Insert row** | I | I | N: same position | I | C |
| **Delete row** | I | I | I | N: same row | N: same row |
| **Edit cell** | C | N: same column | C | N: same row | N: same cell |

The slide labels delete column against delete column "if same cell"; it reads as "same column". The implementation-dependent pairs are the structural ops across axes, plus insert row against delete row: their outcome depends on how the grid is stored. Once rows and columns have identities and cells are keyed by the pair, every pair that touches different rows, columns, or cells commutes. What remains are the pairs that share a row, a column, a cell, or an insertion point, and those are application choices. They are the pairs AegisSheet measured.

### How to read the AegisSheet tables

Two clients edit offline. Client A does the column operation, client B the row operation, then they sync. Operations are on the same row or column, except insert against remove or move: the insert is immediately to the right of the row or column the other client removed or moved. A dash is a pair the paper left blank (`EditCell` × `Insert` and the swap). Grades are the paper's: desirable (both intents kept), suboptimal (text kept, something else lost or surprising), destructive (text discarded, or a concurrent creation/edit/move discarded).

Data gathered by the authors on 17 December 2025.

#### Google Sheets

| B \ A | EditCell | InsertR/C | RemoveR/C | MoveR/C |
| --- | --- | --- | --- | --- |
| **EditCell** | suboptimal: last write wins | — | destructive: remove wins | desirable: move and edit |
| **InsertR/C** | — | desirable: both exist | desirable: insert and remove | suboptimal: insert at the pre-move position |
| **RemoveR/C** | destructive: remove wins | desirable: insert and remove | desirable: remove | desirable: move wins |
| **MoveR/C** | destructive: move wins, edit discarded | suboptimal: insert at the pre-move position | desirable: move wins | suboptimal: last move wins; the earlier move leaves an empty row or column |

#### Notion (table feature)

| B \ A | EditCell | InsertR/C | RemoveR/C | MoveR/C |
| --- | --- | --- | --- | --- |
| **EditCell** | suboptimal: last write wins | — | destructive: remove wins | desirable: move and edit |
| **InsertR/C** | — | destructive: first insert wins, including edits inside the second | suboptimal: remove is discarded and the row is cleared; the insert is applied | destructive: insert lands at the pre-move position and the move is dropped |
| **RemoveR/C** | destructive: remove wins | destructive: remove wins, insert discarded | desirable: remove | destructive: remove wins |
| **MoveR/C** | desirable: move and edit | destructive: move wins, insert discarded | suboptimal: move, but cells cleared | suboptimal: last move wins |

#### AegisSheet

| B \ A | EditCell | InsertR/C | RemoveR/C | MoveR/C |
| --- | --- | --- | --- | --- |
| **EditCell** | desirable: keep both values as a conflict | — | desirable: discard the remove, keep the edit | desirable: move and edit, at the new position |
| **InsertR/C** | — | desirable: both exist | desirable: insert and remove | desirable: insert stays at the old position, and the moved row or column moves |
| **RemoveR/C** | desirable: discard the remove, keep the edit | desirable: insert and remove | desirable: remove | desirable: move wins |
| **MoveR/C** | desirable: move and edit, at the new position | desirable: insert at the old position, and the move | desirable: move wins | suboptimal: last move wins |

Their rule, in one line: a structural delete discards text only when nothing concurrent has created, edited, or moved that row or column. A cell edit or a move is evidence the row or column is still relevant, so the delete loses. Two concurrent edits of one cell stay visible until a user picks one. Two concurrent moves of one row or column keep the later move.

### Undo is a second table

Both products and AegisSheet then undo A's operation after the merge. Undo is a new operation, not a rewrite of history, which is the only form an append-only DAG can offer. The failure mode is an undo that also undoes the other client's compatible edit (Notion: undoing a column move also undoes a concurrent insert to its right; Sheets: undoing one of two concurrent moves duplicates or clears the row).

AegisSheet's undo inverts the local op by id: undoing a delete re-inserts that id if it is still absent; undoing a cell edit removes the value that edit wrote and restores the values it replaced, including a multi-value set; undoing a move puts the id back, which is trivial because the id never changed. A concurrent remote edit of the same cell becomes a conflict with the restored value. An undo whose id is already gone is a no-op.

## How each design represents the grid

Three representations show up, and they predict the tables.

**Index arrays.** Yanakieva et al.'s naïve model is a `Y.Array` of `Y.Array`s. Concurrent insert and delete shift indexes, so an edit lands in the wrong cell and a deleted column reappears as the column that was inserted beside it. This is the failure mode of treating `A1` as the identity of a cell.

**Nested maps of ids.** Their nested-map model, and Weidner's grid, give each row and each column an id. Cells are a map keyed by the pair. Insert and delete of different axes commute. A row delete that is a map-key delete also drops concurrent edits of that row (delete-wins). Getting update-wins out of stock Yjs types is what they report as awkward: the edit has to be able to recreate the key the delete removed.

**Id list plus cell map, with position split from content.** AegisSheet's `ReplicatedUniqueList` orders element ids in a list CRDT and stores the row or column id in a side map. A move is a new position for the same id, not a delete plus an insert. The merge keeps, per id, the winning position and the winning value independently, so a move concurrent with an edit keeps both. Kleppmann's list-with-move is the same split, and Weidner's grid keeps rows in such a list; without it, two concurrent moves of one row surface the row twice. Sypytkowski uses fractional indexes for the order and a per-cell timestamp for the value, and refuses to drop a row while any cell in it has a newer timestamp than the delete. That is update-wins implemented with clocks.

Kale makes the same identity choice for a different reason. An absolute reference is a row id assigned by the sheet, resolved when the formula is parsed. Inserting, deleting, or permuting rows does not retarget it. A relative reference is an offset from the formula's own row and is recomputed. A coordinate rectangle such as `B2:C3` is not expressible: under a structural edit it can mean "these cells" or "this geometric window", and the edit cannot know which.

## What transfers to an MVT

### Presence: an edit is a keep

`RSet` already has the slot shape a row id needs. An element is one hash. An add is a positive op, a delete is a negative op. With `supportBarrierDelete`, a delete in the cover at `at`, or concurrent with `at` and visible from `from`, removes the element: that is Google Sheets' remove-wins, and it is `killedByConcurrentDelete` in `Rdb`. Without the barrier flag, a delete removes the element only when it dominates every add. A concurrent add stays. AegisSheet's "edit restores the row" is that second rule, with one extra positive op: an edit of any cell in the row is itself a keep of the row id, the way their earlier `KeepRemoveList` treated a keep-dot. A remove that does not dominate every keep leaves the row present, cells and all. An edit keeps its column the same way.

A row remove never voids an edit. It decides presence, and presence decides only what the view shows. The edit's value stays in its own slot, and comes back if the row does.

### Why not a schema on Rdb

`Rdb` can hold the facts; it cannot compute the view. The encoding that fails is a cell row with a foreign key to its sheet row. A delete of the sheet row voids a concurrent cell write when the table sets `concurrentDeletes`, and leaves it live but dangling when it doesn't; neither is update-wins. Making the edit keep the row through that key ("the row is live because the cell is live, and the cell is live because the row is live") is a positive cycle, and a positive cycle is void ([`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md) §3).

The encoding that works is the one [`CAPABILITIES.md`](../modules/rdb/CAPABILITIES.md) uses for grants: presence is "some live witness exists". Every edit or move inserts a fresh keep row for its sheet row, and a remove deletes the keeps it has seen. A concurrent keep survives because the remove never named it. Cells never point at the sheet row, so no cycle forms. A multi-value cell fits the same way: each edit inserts a value row and deletes the value rows it saw.

What `Rdb` has no way to express is the order of an axis under concurrent inserts and moves, the row and column numbers derived from it, ranges defined by that order, and formula evaluation over all of it. Those, and the cost of a row per keep and per value at sheet scale, are the reasons `RCalc` is a type beside `RSet` rather than a schema on `Rdb`.

### Cell values: multi-value

Cell contents are a second slot, keyed by `(rowId, colId)`, and they must not share the row slot's delete. AegisSheet wraps each value in a multi-value register: every concurrent edit is visible, and a later edit that observes them overwrites the set. Google Sheets, Notion, Yanakieva et al., and Sypytkowski use last-writer-wins.

A tie-break is available. `Rdb` already resolves concurrent writes to one slot by the larger entry hash (RTable's per-field values, and RSchema's slots after comparing versions). An entry-hash order is arbitrary but convergent, and unlike a timestamp it claims nothing about which write came later. The reason not to use it for cells is what it does to the user: it discards one of two edits without telling anyone, which is the suboptimal grade in every measured product. The multi-value cover matches the way a view already reads a cover: every live edit that no later edit dominates is part of the cell, and resolving the conflict is a new op that names the chosen value. `RSet` membership is the boolean special case of the same cover (one element, add or delete). A cell is the register case.

### Order: slots, placements, and a sibling order

The order of an axis is two facts.

- **Slots.** A grow-only list of positions. An insert allocates a slot next to an existing slot and places a fresh id in it. Slots are never removed, so an insert next to a row that is concurrently removed still resolves, which is AegisSheet's "insert and remove".
- **Placement.** A per-id register naming the slot the id occupies: Kleppmann's move. An insert writes it once, and a move writes it again. A move writes the placement and never the value slot, so a move concurrent with an edit keeps both, at the new position. Sheets' destructive cell — move wins, edit discarded — is what happens when a move is a delete plus an insert of a fresh id: the edit is an op on the old id.

An insert names its neighbor *slot*, never its neighbor *element*. "Move A after B" concurrent with "move B after A" is a cycle when positions are relative to elements, and two independent writes when they are relative to slots.

Numbering the surviving ids needs a total order on slots, and two slots allocated concurrently at the same place have no causal order. Something deterministic has to break that tie, and the entry hash is the one this stack already uses. A tie-break here loses nothing: both rows survive, and only their relative order is arbitrary. What the order algorithm must avoid is interleaving. Two users pasting a block of rows at the same place should get one block after the other, not alternating rows. Fugue is a list CRDT built for that; fractional indexes, as in Sypytkowski's table, can interleave.

Operations that only shift indexes — the commutative cells of the Yanakieva matrix — are not operations. They are what the view does when it numbers the surviving ids. Notion's "first insert wins, and edits inside the second insert are lost" is an index collision treated as one slot. `RSet` already refuses that: two adds of different elements are different hashes.

### Concurrent moves

Two concurrent moves of one id leave two placements in the cover, and neither dominates. This is the one pair AegisSheet still grades suboptimal, and the one pair a causal cover cannot decide by "later". A grid cannot show one row in two places; that is the duplication Kleppmann describes. So the placement register is multi-value with one displayed winner: the view shows the row at the placement whose entry hash wins, and reports the other as a conflict, which a later move that dominates both resolves. The displayed position is still a tie-break, but the losing move stays visible instead of being discarded, which is what separates this from AegisSheet's last move wins.

### Ranges

Ranges are names for spans of ids, not of coordinates. AegisSheet anchors markers on the row-id list and the column-id list, and a marker's removal behavior says whether deleting its element moves the marker to the successor, the predecessor, or drops the range. Deleting a border element shrinks a range, deleting an interior one keeps its endpoints, and a range whose endpoints cross is removed. Kale's absolute reference is the marker that follows the id, and Kale's relative reference is an offset evaluated at read time.

An id-anchored rectangle keeps the ambiguity Kale avoids by forbidding rectangles. It means "the window between these anchors", read over the current order, and that has consequences:

- A row moved into the window joins the range, and one moved out leaves it.
- A move of one anchor past the other crosses the endpoints. AegisSheet drops the range; normalizing it to the new span is the alternative.
- A row inserted next to a border anchor is inside or outside according to the anchor's side, so a marker needs an insertion behavior as well as a removal behavior.

### Formulas

A formula in `RCalc` should store ids. Shifting `B2` to `B3` because a row was inserted above it is a view computation over the id order, the same way an `RSet` view computes membership instead of storing the set. Recomputing the formula graph is a separate evaluator over a view; it is not a second CRDT. The evaluator needs a rule for an operand that holds a conflict: an error value, or a conflict that propagates to every dependent cell.

A formula that reads another sheet is a state reference, which MVT already has. The reading sheet records the other sheet's version with ref-advances, as a permissioned `RSet` does for its `RCap` ([SOaD](../modules/mvt/README.md#composability-soad-architecture)), and the other sheet stays unaware of who reads it. Those ref-advances are barriers, as every reference update in SOaD is.

### Authorization

Authorization is the permissioned-`RSet` pattern, not a new one. Structural ops and cell edits can require capabilities on a referenced `RCap`, re-checked at use against the live ref-advance cover. A revoke concurrent with an edit voids the edit the way a revoke voids an add. A void edit is skipped by the cover's see-through predicate, so it keeps nothing: a remove concurrent with it wins. That is independent of the grid rules: one is authority, the other is presence.

### What the desirable column buys

With the choices above — removes that are not barriers plus keeps, multi-value cells, multi-value placements — no grid op in `RCalc` is a barrier. Presence, placement, and cell value are each a cover read at `at`, and nothing concurrent with `at` can change them. An unpermissioned sheet's view ignores `from`, an op admitted at its position holds in every later view, and the bounded delta's revision bound is the meet, as for a plain `RSet`. The only barriers are ref-advances: to the authorizing `RCap` if the sheet is permissioned, and to the other sheets its formulas read. That is the purest form of SOaD in the [MVT README](../modules/mvt/README.md#composability-soad-architecture), where reference updates are the only barrier ops.

A barrier remove, Sheets' and Notion's remove-wins, is the one choice that brings `from` into the grid itself.

### Deltas

`computeDelta(start, end)` compares two views and reports membership of row and column ids, order changes, and cell-value changes, including a conflict set appearing or being resolved. For an unpermissioned sheet the bound is the meet. A permissioned sheet declares its `RCap` as a horizon reference and takes the bound of a permissioned `RSet` with no barrier adds or deletes: the bound drops below unstable ref-advances, and below ref-advances of `start` whose verdict differs between `start` and `end` (rules 1 and 3 of [RSet's delta computation](../modules/std_types/README.md#delta-computation-1)). Rule 2 has nothing to act on.

## A sketch of the facts

What the tables force `RCalc` to store, as slots on one DAG:

| Fact | Key | Positive ops | Negative ops | Concurrent rule the tables call desirable |
| --- | --- | --- | --- | --- |
| Presence | axis id | insert, and any cell edit or move of that id (a keep) | remove | present while the cover holds a live keep. A remove wins only when it dominates every keep; a concurrent edit or move discards it. |
| Slots | slot id | insert (allocates a slot) | — | grow-only. Concurrent slots at one place are ordered by a non-interleaving list algorithm, with the entry hash as the last tie-break. |
| Placement | axis id | insert, move | — | every placement no later move dominates is in the register. The view shows one, chosen by entry hash, and reports the rest as a conflict until a later move dominates them. |
| Cell value | `(rowId, colId)` | edit | clear (an edit to empty) | every edit that no later edit dominates is in the cell. A user resolves by editing again. |
| Range | range id | anchors: two row ids, two column ids | drop | anchors are ids, and the span is the window between them in the current order. A border delete shrinks it, an interior delete keeps its endpoints, crossed endpoints drop or normalize it, and an insert at a border follows the anchor's side. |

Admission reads `(at, at)`. An edit, a move, and a remove require the id present at `at`. An insert requires its neighbor slot to exist, whether or not the id in it is present. Restoring an absent id is its own op, a keep admitted on an absent id; an edit of an absent id is rejected.

Undo is an ordinary op that inverts one of these by id: a remove's inverse is a keep, an edit's inverse restores the value set it replaced, and a move's inverse writes the previous placement, whose slot still exists because slots are never removed. It is admitted and re-checked like any other op. It does not peel history.

## Reading at scale

`RSet.hasByHash` runs one cover query per element. A sheet view needs every visible cell at once, so it needs a read that produces all covers in one walk, and entries that carry many cell writes (a paste, a fill) indexed under every key they touch. For a sheet of real size, the likely shape is a projection kept current by `computeDelta`, as [rdb_projection](../modules/rdb_projection) does for `Rdb`.

## Open choices

These are the pairs the papers grade differently, and the gaps they leave, stated as choices for the type rather than as a default implementation:

- **Same-cell edits.** Multi-value cover, as above, or an entry-hash tie-break. The tie-break converges but discards an edit silently, the suboptimal grade in every measured product.
- **Concurrent moves of one row or column.** A multi-value placement with a displayed winner, as above, or a tie-break that discards the losing move. AegisSheet keeps the later move and grades the result suboptimal.
- **Whether a remove is a barrier.** A barrier remove reproduces Sheets and Notion on edit-versus-delete, which AegisSheet grades destructive, and it is the only choice that makes the grid read `from`. A non-barrier remove plus keeps reproduces the desirable column and matches `RSet` without `supportBarrierDelete`.
- **Whether a clear is a keep.** A clear is an edit to empty. If every edit is a keep, a concurrent clear of one cell stops the removal of its row and column.
- **Sibling order.** Fugue or FugueMax, RGA, or fractional keys, judged by whether concurrent block inserts interleave.
- **Range boundaries.** Drop or normalize a range whose endpoints cross, and which side of a border anchor a concurrent insert lands on.
- **Formulas as values or as structure.** The cell register can hold text, including formula text that names ids (Kale, Weidner). Two concurrent edits of one formula are a cell conflict, not a merged AST, unless a later design puts a text CRDT inside the cell. AegisSheet lists that as future work (Eg-walker, Peritext).
- **Conflicted operands.** A formula reading a cell that holds a conflict evaluates to an error value, or carries the conflict to every dependent cell.
- **Permission.** An unpermissioned sheet, like a plain `RSet`, or a `capabilityRef` plus per-op requirements, like a permissioned `RSet`. The grid rules above do not change either way.

## Summary

- **The grid is not where MVTs add anything.** With the desirable choices, `RCalc`'s grid has no barriers. It is an op-based CRDT on the DAG, and its view never reads `from`. Built on its own, it would be AegisSheet on the DAG.
- **MVTs add something between objects.** Edits can be gated by an `RCap`, so a concurrent revoke voids them. Formulas can read other sheets at pinned versions through ref-advances. A model built from another sheet's numbers at a known version, recomputed when that version advances, is new. It pays off where several parties share a model and trust each other only in part.
- **Most of the cost is not MVT work.** It is a formula engine at spreadsheet parity, a UI, and reads at sheet scale. IronCalc, an open-source engine in Rust with WebAssembly bindings, could evaluate formulas over a view.
- **The reusable part is the ordered list with move:** grow-only slots, a placement register per id, and a sibling order that does not interleave. Task lists, boards, and outlines need the same thing, and neither std_types nor `Rdb` has an ordering primitive beyond sorting by a column. Given an `RList`, a grid is two lists, presence with keeps, and a map of multi-value cells.
- **The semantics can be tried before any new type.** An `Rdb` app can use keep rows for presence, value rows for multi-value cells, and a fractional-key column for order. Moves and interleaving would be approximate, but it would show whether the permission and versioned-reference angle is wanted.

Build `RList` when an application needs ordering. Build `RCalc` when a concrete user needs permissioned or versioned sheets, with this survey as its design.

## History

This HIP was `REPLICATED_SPREADSHEETS.md`, moved here by [HIP-1](0001-hips.md) on 2026-11-04. Text above that names a file by its old name refers to it as it was. The files it was built from can be read at commit `4ec4661` (`git show 4ec4661:<path>`).

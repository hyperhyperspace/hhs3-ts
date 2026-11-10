# HIP-21: `OR` in `EXISTS ... WHERE`

- **Type:** design
- **Status:** draft
- **Modules:** rdb, rdb_lang, mvt, rpack, rdb_tools, rdb_repl_web, rdb_runtime, rhost
- **Requires:** —
- **Superseded by:** —
- **Implemented in:** —

## Summary

An `EXISTS` atom in a rule asks whether some row of a table matches a filter. Today the filter can only be a conjunction of equalities, so a rule with alternatives has to be written as an `OR` of whole `EXISTS` atoms, one per combination of alternatives, and that grows exponentially. This HIP lets the filter combine equalities with `AND` and `OR`, and reads it as written, at one index lookup per equality, without expanding it. The new read gives the verdicts the expanded `OR` of atoms would give. In C-SQL the atom is delimited as in SQL, `EXISTS (t WHERE …)`, its filter also takes `IN` lists, and a bare column name must belong to exactly one of the two tables in scope. A filter that is a conjunction keeps its payload, its hash and its verdicts. The comparison stays equality, and the verdict engine does not change.

## Why

### What is rejected

An `exists` atom is stored as `{ p: 'exists'; table: string; where: { [field: string]: WhereValue } }` ([`rschema/payload.ts`](../modules/rdb/src/rschema/payload.ts)): `where` maps columns of the target table to the values they must equal. `lowerExistsWhere` ([`compile/query.ts`](../modules/rdb_lang/src/compile/query.ts)) accepts only `=` comparisons joined by `AND`. Lowering these rules on a gated table `t`, with `caps (label, grantee)`, in today's grammar:

| Rule | Result |
| :--- | :--- |
| `EXISTS caps WHERE caps.label = 'writer' OR caps.label = 'manager'` | `EXISTS WHERE only supports equality predicates` |
| `EXISTS caps WHERE caps.grantee = $author AND (caps.label = 'writer' OR caps.label = 'manager')` | the same error |
| `EXISTS caps WHERE … OR EXISTS caps WHERE …` | the same error: the first `WHERE` takes the rest of the condition |
| `(EXISTS caps WHERE …) OR (EXISTS caps WHERE …)` | compiles |
| `EXISTS caps WHERE caps.label = 'writer' AND caps.label = 'manager'` | compiles to `where: { label: 'manager' }` |

### What the workaround costs

An existential distributes over `OR`, so the author can distribute by hand and write one parenthesized `EXISTS` per combination. A rule that combines $k$ two-way choices becomes $2^k$ atoms of $k$ equalities each: $k \cdot 2^k$ index lookups for a rule that names $2k$ values. An `or` group holds at most 64 members (`MAX_EXPR_ARGS` in [`rschema/validate.ts`](../modules/rdb/src/rschema/validate.ts)), so seven choices do not fit in one group. A compiler that distributed for the author would meet the same limit, and dumps and failure messages would show the distributed rule instead of the one that was written.

### Defects in the same place

- **The limit is not documented.** The grammar in the [rdb_lang README](../modules/rdb_lang/README.md) (*Expressions*) reads `"EXISTS" table [ "AS" alias ] "WHERE" condition`, as if any condition were accepted.
- **The `WHERE` takes the rest of the condition.** In `EXISTS caps WHERE caps.label = 'admin' AND caps.grantee = $author AND body = 'x'`, on a gated table with a `body` column, `body` is lowered as a column of `caps`, which has none. An `EXISTS` followed by `OR` without parentheses fails with the message about equality, when what is missing is the parentheses.
- **A bare name in the filter binds to the `EXISTS` table without looking.** `EXISTS caps WHERE caps.label = 'admin' AND owner = $author`, where `t` and `caps` both have `owner`, reads `caps.owner` with no error. The README's rule is that an unqualified name must refer to exactly one table in scope. The compiler applies it to the gated table's columns and not to the filter's.
- **A column named twice is overwritten.** `lowerExistsWhere` merges an `AND` with `Object.assign`, so the last equality on a column silently replaces the others (the last row of the table).

## Design

### Background: how an `exists` atom is read today

This section covers what the rest of the HIP builds on. The full accounts are in [`VOID_SEMANTICS.md`](../modules/rdb/VOID_SEMANTICS.md) and [`WELL_FOUNDED_VERDICTS.md`](../modules/mvt/WELL_FOUNDED_VERDICTS.md).

**Rules, atoms and terms.** A table's rules (`ALLOW insert IF …`) are predicates over an op. An `exists` atom is true when some live row of its target table matches its `where`. A value in `where` is a literal or a term:
- **`$author`** is the op's author.
- **`$param.<name>`** is a schema parameter.
- **`$row.<col>`** is a column of the row the op writes, the **subject row**. C-SQL writes it as `docs.col`.

Only **pub** columns can be searched. A column is declared `PUB`, and every write records the values it gives such columns in an index.

**Verdicts.** Every entry (an insert, an update, a delete, an import, …) ends up **live**, when it takes effect, or **void**, when it stays in the log with no effect. An entry's rule reads other entries' verdicts. An insert may need a cap row to be live, and a delete makes its row not live.

**Undecided values.** While entries that read each other are being solved together, a verdict can be **undecided**, and rules are evaluated in Kleene's three-valued logic. `and` is false when any input is false, `or` is true when any input is true, and otherwise the result is undecided.

**Supports.** An undecided result carries its **support**: the undecided entries it rests on. The engine computes the well-founded model of the rules. Entries left undecided are linked into a graph by their supports. A set of them that depend only on each other, a **core**, is a real conflict, and all its members are made void, with the reason `undecided-cycle`. So supports matter: two rules that agree in value but rest on different entries can put different entries in a core, and give different verdicts.

**What a read must be.** Every read is:
- **exact:** with no undecided input, it gives the ordinary answer;
- **sound:** a decided answer holds however the undecided inputs turn out;
- **monotone:** deciding more inputs never changes a decided answer.

A read is also no more precise than its rule says. A read that decides more cases changes which entries end up in cores, so precision is part of the protocol, and every replica reads the same way (`WELL_FOUNDED_VERDICTS.md` §5.4).

**Candidates.** The target table is not scanned. For a column and a value, the index gives the rows that have a write carrying that value: the value's **carriers**. One field of the atom picks the candidates, and every other field must be carried by them too. Then each candidate is read:
- **its column values.** A value is undecided when an undecided write could change it.
- **its liveness.**

The atom is the `or` over the candidates, and the read stops at the first one that surely matches.

**Defaults.** A row that never wrote a column holds the column's default, and it left nothing in the index for that column. So a field whose value is the column's default can neither pick nor narrow the candidates. When every field of an atom is at its default, every inserted row of the table is a candidate.

**The subject row.** Write `S` for the subject row's liveness. It can be undecided, for instance when the op is an update and a concurrent delete of the same row is undecided. An atom with a `$row` term also needs the subject's values, and those can be undecided too.

**Imports and the touch rule.** A group can read the tables of a group it is bound to (`auth.caps`), at the versions of that group it has imported. Each import, an **observe**, has a verdict of its own, and it can be undecided while it is solved together with the reader.

Write `D` for the version that the live imports give, and call an undecided import **pending**. A pending import can carry a schema change (a deploy) that alters what a read depends on: whether the table exists, or a column's default or pub status. These parts of a schema are its **slots**: one existence slot per table, and one slot per column.

A pending import **touches** a read when it writes a slot the read depends on. A touched read is undecided, with the touching imports as its support. An untouched read is decided as at `D` ([`rtable/touch.ts`](../modules/rdb/src/rtable/touch.ts)). While imports are pending, candidates are gathered at `D` plus every pending version, which holds the candidates of every outcome.

**Today's order.** Today's atom fixes its value at the first of these steps that applies:

1. **A term with no value is false.** That is `$author` for an anonymous op, an absent parameter, or a subject column with no value.
2. **A subject row that is not live is false.**
3. **An undecided subject value gives `and(S, undecided)`**, on that value's support.
4. **A touched slot of the table or of a searched column gives undecided**, on the touching imports, inside `and(S, …)` when the atom reads the subject.
5. **Otherwise the atom is the read over the candidates**, inside `and(S, …)` when it reads the subject.

### Payload

```ts
export type WhereLeaf = { [field: string]: WhereValue };          // every field equals its value
export type Where = WhereLeaf | { $and: Where[] } | { $or: Where[] };

// in Predicate
| { p: 'exists'; table: string; where: Where }
```

A **leaf** is today's map. On a gated table `docs`, the rule

```sql
EXISTS (grants WHERE grantee = $author
                 AND label IN ('writer', 'manager')
                 AND grants.resource IN (docs.resource, '*'))
```

is:

```ts
{ p: 'exists', table: 'grants', where: { $and: [
    { grantee: '$author' },
    { $or: [{ label: 'writer' }, { label: 'manager' }] },
    { $or: [{ resource: '$row.resource' }, { resource: '*' }] },
] } }
```

`grants.resource` is qualified because `docs` has a `resource` column too (*C-SQL*, "One rule for names").

A field name matches `^[a-zA-Z_][a-zA-Z0-9_]*$`, so a `$` key can never be a field and a group cannot be mistaken for a leaf. `validatePredicate` accepts:

- **A leaf**, as today: one or more fields, each with a valid name and a valid where-value.
- **A group**: an object whose only key is `$and` or `$or`, holding 2 to 64 members (`MAX_EXPR_ARGS`). A one-member group is not canonical, as for predicates. Members may be equal, as in predicate groups.
- **At most one leaf in an `$and`.** The equalities of one `AND` share a leaf. Without this rule `{ $and: [{ a: 1 }, { b: 2 }] }` and `{ a: 1, b: 2 }` would be two payloads for one text, and `formatPredicate` could not render both so that each parses back to itself.
- **Limits.** An atom holds at most 64 equalities in all (`MAX_WHERE_FIELDS`), counting each value of an `IN` list as one. Group nesting counts toward `MAX_EXPR_DEPTH` (16) together with the predicate's own.

The protocol is unreleased, so a schema can use a group as soon as this lands, and no replica has to read both forms. After release, raising the limit of 64 would be a protocol change, since older replicas would reject the new payloads.

### The read rule

This replaces the `exists` rule of `VOID_SEMANTICS.md` §4.

**What it must equal.** A **way** of satisfying the tree picks one member of every `$or` on its path, and so is a conjunction of leaves: `(a OR b) AND c` has two ways, `a AND c` and `b AND c`. The **distributed form** of a tree is the `OR` of one of today's atoms per way. It is what an author writes today by distributing by hand. It can be exponentially large, and the read never builds it. It is the yardstick: the new read has the distributed form's value in every state, on a support that is no larger.

The read has four parts: the terms are resolved, the touched leaves are found, the tree is read on the target table, and the reads are combined with the subject row.

#### Terms

`$author`, `$param.<name>` and `$row.<col>` are resolved before anything is read, in this order:

1. **A leaf with a term that has no value is false**: `$author` for an anonymous op, an absent parameter, a subject column with no value.
2. **The tree simplifies.** An `$and` with a false member is false, and an `$or` loses its false members. If nothing is left, the atom is false. After this step every leaf lies on some way of satisfying the tree.
3. **A leaf that names an undecided subject value is set aside.** The reads below take it as false, and `X` (*Combining the reads*) counts it.

For one leaf this is today's order: a term with no value makes the atom false even when another subject value is undecided.

#### Touched leaves

While imports of the target's group are pending, a leaf that is not set aside is **touched** when a pending import writes the table's existence slot or the slot of a column the leaf names. `rowAuthor` has no slot of its own. The reads take a touched leaf as false, so they read only columns whose slots are untouched, and `T0` and `T$` count it.

A set-aside leaf is not checked, since today's atom reads the subject's values before it checks slots. For one leaf this is today's check.

#### Reading the tree

- **A field is carried by a row** when an insert or update of the row carries its value, or when the value is the column's default. The writes counted are those at or below the version that candidates are gathered at: the view's own version, or `D` plus every pending version while imports are pending. `rowAuthor = k` is carried exactly by the rows `k` inserted. A field that is not a pub column of the table, in the schema the view reads, is carried by no row.
- **Candidates** are the tree evaluated over sets of rows. A field is its carriers, found through the index. A leaf intersects its fields, `$and` intersects its members, and `$or` unites them.
- **A field at its default** stands for every inserted row without listing them. Write `ALL` for that set. An intersection ignores `ALL`, and a union with an `ALL` member is `ALL`. So `ALL` stays a symbol through both, and every inserted row is listed only when the root's set is `ALL`. In `{ $and: [{ grantee: '$author' }, { $or: [{ label: 'peer' }, { label: 'manager' }] }] }` with `'peer'` the default, the `$or` is `ALL` and the candidates are the carriers of `grantee`.
- **A row is evaluated in two passes.**
  - **First,** the tree is evaluated on "carried" alone. A leaf or group that is false there is false on the row, with nothing read.
  - **Second,** what is left is evaluated with Kleene logic. A field is the column read compared with its value, using the default when no live write gives the column a value. It is undecided when the read is, and `rowAuthor` needs no read.

  The row's value is the `and` of the tree and the row's liveness, values first, so a row that its values rule out never has its liveness read. The reads made on a row are therefore among those of the distributed atoms that the row is a candidate of.
- **The read** is the `or` over the candidates. It stops at the first that surely matches.
- **A missing target.** When the target cannot be looked up, every read is false, and so are `T0` and `T$`; `X` is still combined by the table. The target cannot be looked up when its group is not bound, or when the table does not exist. Today's atom reads the subject's values before it looks the target up. Inside a view of a bound group, a table absent at `D` is handled as today: its existence slot is checked first.

For one leaf this is today's rule: one field picks the candidates, every other field must be carried too, and a field at its default neither picks nor narrows.

#### Combining the reads

The subject row and the touched leaves make some ways of satisfying the tree depend on more than the target's rows. The rule therefore sorts the ways by what they use, and gives each class the value today's atom would give its members (*Today's order*):

| A way that uses | Today's atom for it | Its arm |
| :--- | :--- | :--- |
| a set-aside leaf | false when `S` is false, else `and(S, undecided)` on the subject values | `X` |
| a touched leaf and a `$row` leaf, and no set-aside leaf | false when `S` is false, else `and(S, undecided)` on the touching imports | `T$` |
| a touched leaf, and no `$row` or set-aside leaf | undecided on the touching imports | `T0` |
| a `$row` leaf, and no touched or set-aside leaf | `and(S, read)` | `R$` |
| only plain leaves | the read | `R0` |

A **plain** leaf names no `$row` term and is neither set aside nor touched. Every way falls in exactly one class.

**The reads.** They take set-aside and touched leaves as false:
- **`R0`** is the read with every `$row` leaf false too.
- **`R$`** is the read that counts only matches through a `$row` leaf. A `$row` leaf matches through itself, an `$or` when a member does, and an `$and` when one member does and its other members match.
- **`R`** is the read of the whole tree. In value it is `or(R0, R$)`, and it is used where `S` is known to be live, because it can stop earlier.

**The other arms** come from the tree's structure, in one pass each, and read nothing:
- **`X`** is undecided when set-aside leaves are left after step 2, on the supports of their subject values, and false otherwise.
- **`T0`.** Simplify the tree again with every `$row` and set-aside leaf false. `T0` is undecided when touched leaves are left, on the imports that touch them, and false otherwise.
- **`T$`.** Simplify the tree again with every set-aside leaf false. In what is left, a touched leaf counts when it names a `$row` term itself, or when an `$and` above it has another member that holds a `$row` leaf. `T$` is undecided when a touched leaf counts, on the imports that touch the leaves that count, and false otherwise.

Each arm is the class of its row in the table above, without listing ways: after a simplification every leaf left lies on some way.

| The subject row is | The atom is |
| :--- | :--- |
| not read (no `$row` leaf) | `or(R, T0)` |
| not live | `or(R0, T0)` |
| live | `or(R, X, T$, T0)` |
| undecided | `or(R0, T0, and(S, or(R$, X, T$)))` |

The four rows are the last one with `S` simplified where it is known. `R` stands in for `or(R0, R$)` where `S` is live, since it has the same value on a support that is no larger.

**One leaf.** With no `$row` term, the atom is `R`, or `T0` when the leaf is touched. With one, `R0` and `T0` are false, `R$` is `R`, and at most one of `R`, `X` and `T$` is not false. The rows then give false, `or(R, X, T$)` and `and(S, or(R, X, T$))`, which is today's atom.

**Cost.**
- **Index lookups:** one per distinct field and value, at most 64, plus set operations on their results.
- **Per candidate:** one pass over the tree on "carried", and one read per column the second pass needs. When `S` is undecided and the tree has a `$row` term, `R0` and `R$` are two more passes over the same candidates.
- **The other arms:** `X`, `T0` and `T$` are passes over the tree with no reads.
- **While imports are pending:** the slot check is one query per leaf.

**Defaults.** Inside a leaf or an `$and`, a field at its default leaves the other members to narrow the candidates. Under an `$or` nothing narrows them: a row at the default satisfies that member by itself, so the group's set is `ALL`. When the root's set is `ALL`, the read goes through every inserted row until one surely matches. Today that happens only when every field of the atom is at its default.

### C-SQL

The `EXISTS` is delimited as in SQL, and its filter takes `AND`, `OR`, parentheses and `IN`:

```ebnf
primary = ... | "EXISTS" "(" table [ "AS" alias ] "WHERE" filter ")" ;
filter  = conj { "OR" conj } ;
conj    = term { "AND" term } ;
term    = "(" filter ")"
        | column "=" value | value "=" column
        | column "IN" "(" value { "," value } ")" ;
value   = literal | variable | column ;   (* a column of the gated table *)
```

Braces mean zero or more, as in the README's grammar, so a filter can be one equality. The rules of the README's examples become:

```sql
ALLOW insert IF EXISTS (users.caps WHERE label = 'writer' AND grantee = $author)
ALLOW delete IF grantee = $author OR EXISTS (caps AS c WHERE c.label = 'manager' AND c.grantee = $author)
ALLOW insert IF length(body) <= :max_len
            AND EXISTS (auth.caps WHERE label = :write_cap AND grantee = $author)
```

- **The parentheses are where SQL puts them.** Only the one that matches `EXISTS (` ends the `EXISTS`, and every parenthesis inside it groups. A writer who puts each comparison in parentheses cannot end the `EXISTS` early by doing so.
- **No text changes meaning.** `EXISTS (` is a parse error today, and every text in today's form is a parse error in the new grammar.
- **Today's form is removed.** `EXISTS t WHERE …` is a parse error whose message shows the new form. Kept beside the new one, it would keep the trap: its `WHERE` takes the rest of the condition.
- **`IN` is contextual.** It is recognized as an operator after an operand, as `ESCAPE` is after a `LIKE` pattern, and stays an ordinary name elsewhere.

**One rule for names.** A bare column name in a filter must belong to exactly one of the two tables, the `EXISTS` table or the gated table. Otherwise it is an error that asks for a qualifier. This is the README's rule for unqualified names, applied to the filter as it already is to the gated table's columns.

- **A local target.** A name in both tables is ambiguous. A name only in the gated table is not a filter on the target. Both are errors where the rule is compiled.
- **A target in a bound group.** Its columns are not known when a schema is compiled. A bare name that the gated table has is an error, and any other bare name is the target's.
- **A self-referential `EXISTS`.** The alias stays required. Every name is in both tables, so every name is qualified.
- **A gate.** A gate has no gated table, so no bare name is ambiguous.

The payload needs no such rule. A key of `where` is always a column of the atom's table, and a column of the gated row is always a `$row.<col>` term. The renderer qualifies every column.

**Lowering.** `lowerExistsWhere` returns a `Where`:

- An equality is a one-field leaf.
- An `AND` lowers its members and merges the leaves among them into one, at the position of the first. Groups stay members. If only the leaf is left, the result is the leaf.
- An `OR` is an `$or` of its lowered members.
- `c IN (v1, …, vn)` is the `$or` of the leaves `{ c: v1 }` to `{ c: vn }`.
  - **Grouping.** It is a group of its own, also under an `OR`. Under an `AND` it is a group beside the leaf.
  - **One value.** A list of one value is the equality.
  - **Repeats.** A value listed twice gives two equal members, which cost nothing extra when the rule runs, since the read makes one index lookup per distinct value.
  - **Values.** A list holds every value `=` takes, as in SQL: a literal, `$author`, a parameter, or a column of the gated row. Each value is resolved on its own, so `grantee IN ($author, 'public')` for an anonymous op keeps only its `'public'` arm, as `grantee = $author OR grantee = 'public'` does.
- A parenthesized group keeps its nesting, as in predicates, except that a conjunction of equalities is always one leaf.

**Errors.**

- **Anything in a filter other than `=`, `IN`, `AND`, `OR` and parentheses:** `EXISTS WHERE supports only = and IN, combined with AND / OR`. An `EXISTS` inside a filter is a syntax error.
- **A comparison with no column of the `EXISTS` table:** today's `EXISTS WHERE filter must reference the EXISTS table`.
- **A column named twice in one `AND`:** an error that names the column and suggests the rewrite. A leaf maps each column to one value, so `a = v1 AND a = v2` has no leaf. It means `v1 = v2 AND a = v1`, and `v1 = v2` does not involve the target row, so it can go outside the `EXISTS`: `t.x = $author AND EXISTS (caps WHERE a = $author)`.
- **A bare name that breaks the rule for names:** an error that asks for a qualifier, or that says the column is not the target's.
- **Today's form:** a parse error that shows the new one.

**Rendering.** `formatExists` ([`rschema/format_predicate.ts`](../modules/rdb/src/rschema/format_predicate.ts)) renders `EXISTS (t [AS a] WHERE …)`.
- **Parentheses.** It puts them where the filter needs them: around a group inside a group of the same kind, and around an `$or` inside an `$and`.
- **`IN`.** An `$or` whose members are all one-field leaves on one column renders as `IN`, with equal members repeated. So `a = 1 OR a = 2` and `a IN (1, 2)` are two texts for one payload, and the renderer writes the second.
- **Groups.** `formatGroupMember` no longer wraps an `exists` member in parentheses, since the atom brings its own.

Every valid payload renders to a text that parses and lowers back to it.

## Why it is sound

**A conjunction is unchanged.** A `where` with no group is one leaf. The read of one leaf is today's, and *Combining the reads* reduces to today's atom, in value and in support, with or without pending imports. Its payload is the same, so its hash is too.

**The read of the tree is exact, sound and monotone.** It is built from column reads and row liveness, which already are (`[VREAD06]` checks every read rule over every state), combined with Kleene connectives. The one other step is "carried". A column resolves to a value that some write at or below the view carries, or to the default. So a field no write carries is false however the undecided entries turn out, and so is a leaf or group that is false on "carried". Which writes exist does not depend on any verdict, so deciding more entries cannot change it.

**It has the value of the distributed form.** The table in *Combining the reads* gives the value today's atom has for every way in each class. Kleene's `and` distributes over `or`, so the `or` of those values is `or(R0, T0, and(S, or(R$, X, T$)))`, which is the table's last row. Today's atoms are exact, sound and monotone, the touch rule included, so the atom is too.

**Its support is no larger.** A model of the rules shows this. It is a program that enumerates, for small trees, every way of marking each leaf (plain, `$row`, set aside, with no value, touched) and every combination of the inputs' values. For trees of up to four leaves that is 4,612,221 states. In each state it computes the rule and the distributed form.
- **The rule against the distributed form.** The value agrees in every state, and the support is contained in the distributed form's in every state.
- **One leaf.** It is today's atom exactly.
- **The arms.** Computed in one pass, they equal their definitions over ways for every marking.

*Tests* keeps the model. The supports can differ. In `(a OR b) AND c` with `a` true and `b`, `c` undecided, the tree rests on `c`, and the distributed form `(a AND c) OR (b AND c)` rests on `b` and `c`.

Both forms have the same well-founded model, since the fixpoint reads only values. Supports enter where the graph of undecided entries is built, so the forms can differ only in which undecided entries end up in a core together. A payload's verdicts therefore depend on its form, and no implementation may rewrite a stored tree into its distributed form, or back.

**Reads stay narrow.** A row is read only if it is a candidate, and on a candidate only the leaves the row's writes can satisfy are read. A row that no write could make match is never read, so its verdict never joins the reader's group of entries solved together. Today's tests rely on this. For example, an atom with one clean witness and another candidate caught in a ring of revokes still holds (`[PERM19]`). Reading every field of a candidate would read columns of arms the row cannot match. A column read that meets an undecided write is undecided even when no outcome would make it match (`[VREAD01]`).

**Why the subject row is read per leaf.** The subject's liveness reads the deletes of the subject row that are concurrent with the op. That is a negative read of concurrent entries, the kind cycles are made of (`WELL_FOUNDED_VERDICTS.md` §2.2). An earlier draft of this HIP read it for the whole atom, which made every arm depend on it.

Take the example rule above as an update rule on `docs`, and an update by the holder of a `'*'` grant, concurrent with a live delete of its doc. As two `EXISTS` atoms the update is live, since the `'*'` atom does not read the doc. Under that draft it was void: the draft read the doc's liveness in every arm, and the doc is deleted. Under the table above it is live. For two concurrent deletes of the doc, the draft's rule also formed a cycle. [HIP-22](0022-concurrent-removals.md) proposes a rule that removes that cycle.

**Why slots are checked per leaf.** Earlier drafts checked the slot of every column the tree names, once, for the whole read. That makes the read undecided whenever a pending import writes a column of any arm. An example:
- **The groups.** Group `K` has a table `caps (grantee, role)`, with `role` defaulting to `'member'`. Group `G` is bound to it as `auth`. `G` has an `admins` table whose delete rule is `EXISTS (auth.caps WHERE grantee = $author OR (grantee = $author AND role = 'admin'))`, and `G` lets a member import `K` only if they are in `admins`. The second arm adds nothing to the first; it stands for any arm that names a column the first does not.
- **The race.** Bob deletes alice's `admins` row. Concurrently, alice imports a version of `K` whose deploy drops `role` and adds it again with the same default. That writes `role`'s slot and nothing else.
- **As two atoms.** The first atom names only `grantee`, which the import does not touch, so it is decided. Bob's delete is live, and alice's import is void, since she is no longer an admin. That is today's outcome for this race (`[OBSFRAME14]`).
- **Under one check for the tree.** The delete's read is undecided on the import, and the import is undecided on the delete. The two form a core, and both are void, so bob fails to remove alice.
- **Under per-leaf checks.** The first arm decides, as with two atoms.

In the model the single check never decides wrongly. In 443,424 of its 4,612,221 states it is undecided where the distributed form is decided. `WELL_FOUNDED_VERDICTS.md` §5.3 already counts a cost of this kind: an import racing a delete can void both, which can hold up a removal. A single check per tree would give the importer more ways to do that.

**The touch rule carries over.** The reads use only untouched columns, so their defaults and pub status are those of `D`. Candidates are gathered at `D` plus every pending version, which holds every outcome's candidates, since the set evaluation only grows when its fields' sets grow. "Carried" is judged at that same version on every candidate. Judged at `D`, a field would be false without a read when only a pending version carries its value, and the atom could then be decided false in an outcome where that import is live and the atom is true. Each candidate's column reads and liveness are touched or not on their own, as today.

**Order does not change the result.** The connectives stop only on a deciding value, so a value and its support do not depend on the order of members, fields or candidates (`WELL_FOUNDED_VERDICTS.md` §4.1). Which reads are made does depend on it, as today, since the read stops at the first candidate that surely matches. The first pass on "carried" bounds them.

## Changes

1. **Payload and validation (rdb).**
   - [`rschema/payload.ts`](../modules/rdb/src/rschema/payload.ts): `WhereLeaf`, `Where`, and `whereEqualities(where)`, which yields every field and value of a tree.
   - [`rschema/validate.ts`](../modules/rdb/src/rschema/validate.ts): `validatePredicate` checks the shape, the canonical form and the limits. `predicateReferencesAuthor`, `collectRowFieldRefs`, `collectParamRefs`, `checkPredicateColumns` and `validateSchemaTables` read fields through `whereEqualities`.
2. **The other readers of `where` fields.**
   - **The list.** The pub-column check and the drop-column guard in [`rschema/validate_ops.ts`](../modules/rdb/src/rschema/validate_ops.ts), `checkExistsAtomAgainstTable` in [`rcatalog/validate_ops.ts`](../modules/rdb/src/rcatalog/validate_ops.ts), `existsAtomReason` in [`rfiles/access.ts`](../modules/rdb/src/rfiles/access.ts), and the FILES check in rdb_lang's [`compile/catalog.ts`](../modules/rdb_lang/src/compile/catalog.ts).
   - **`rowAuthor`.** The pub-column check in `validate_ops.ts` also accepts it, as `validateSchemaTables` does. Today a create accepts `EXISTS docs WHERE rowAuthor = $author` and `set-restrictions` rejects it.
   - **What the compiler catches.** Only `existsTruth` and the drop-column guard stop compiling when `where` becomes a union. The others keep compiling and must be found by *Tests*.
3. **The read (rdb).**
   - [`rtable/view.ts`](../modules/rdb/src/rtable/view.ts):
     - `existsMatching` takes the resolved tree with each leaf marked plain, `$row` or set aside. It checks each leaf's slots, with one `slotsTouched` query per leaf. It takes the touched leaves as false, and returns the reads that the subject row's state needs, along with the touched leaves' supports.
     - `candidateRowIds` evaluates the tree over sets, with one `carriers` call per distinct field and value, and `ALL` kept as a symbol.
     - `candidateMatch` takes the version candidates are gathered at, evaluates the tree on "carried", then reads what is left.
     - `findRowIds` keeps its map argument and is the one-leaf case.
   - [`rtable_group/predicates.ts`](../modules/rdb/src/rtable_group/predicates.ts):
     - `existsTruth` resolves terms over the tree, makes the leaves with no value false and sets aside the leaves with an undecided subject value.
     - It computes `X`, `T0` and `T$` in functions of their own that take a tree with marked leaves, and combines the arms by the table.
     - When there is no view of the target, it combines `X` alone. Today's line returns false for the atom there. Kept ahead of the table, it would make a one-leaf atom with an undecided subject value false, where today it is undecided.
   - **`truthView`**, the fallback for views that are not `RTableViewImpl`, evaluates the tree over the sets that `findRowIds` returns per leaf. A leaf whose call throws is the empty set, so the two paths agree.
4. **Rendering (rdb).** `formatExists` and `formatGroupMember` in [`rschema/format_predicate.ts`](../modules/rdb/src/rschema/format_predicate.ts).
5. **C-SQL (rdb_lang).**
   - [`syntax/parser.ts`](../modules/rdb_lang/src/syntax/parser.ts): `parseExistsRest` parses the delimited form and `IN`, and today's form becomes an error that shows the new one.
   - [`compile/query.ts`](../modules/rdb_lang/src/compile/query.ts): `lowerExistsWhere` gets the lowering and errors of *C-SQL*. `resolveUnqualifiedColumn`, which applies the rule for names to the gated table's columns today, replaces `resolveExistsFilterColumn` on the filter side.
6. **Docs.**
   - **rdb:** the `exists` rule in `VOID_SEMANTICS.md` §4, and the quote of `existsMatching` in [`rdb/CAPABILITIES.md`](../modules/rdb/CAPABILITIES.md).
   - **mvt:** the outcomes and tests in `WELL_FOUNDED_VERDICTS.md` §5.2 and §6.6, and the `EXISTS` rules quoted in `mvt/paper`.
   - **The rdb_lang README:**
     - the grammar under *Expressions*, which can now say what a filter accepts;
     - the line on self-referential `EXISTS`;
     - a note on what a default under `OR` costs;
     - the note that `EXISTS ... WHERE` takes the rest of the condition, which goes.
   - **Every README:** the examples in the rdb, rdb_lang and rpack READMEs, and the **HIPs** line of the rdb and rdb_lang READMEs.
7. **Sources.**
   - **What is rewritten.** Outside `hips/`, in files git does not ignore, 147 atoms are written in today's form, 11 of them split across lines. By module: rdb_lang 62, rdb 33, rpack 26, `packs/editor` 8, mvt 8 (in `paper/`, a golden snapshot included), rdb_tools 7, and one each in rdb_repl_web, rdb_runtime and rhost.
   - **What is not.** The `.released/` folder of `packs/editor` holds 16 more, which the release of item 8 writes again. Its generated `build/` folder holds 8, which git ignores.
   - **How.** Today's parser keeps a span on each `exists` node, and the node ends where its `WHERE` ends, so a script can insert the two parentheses. Tests that expect rendered text change with them.
8. **Released text.** A rule is stored as a predicate, so no payload depends on the grammar. rpack keeps text in two places, though:
   - **The sources of a work folder**, which hold the rules as written.
   - **A released folder**, whose `.released/` holds "the signed inputs and update.sql, as released" ([`rpack/src/project.ts`](../modules/rpack/src/project.ts)). `sameUpdate` ([`rpack/src/commands.ts`](../modules/rpack/src/commands.ts)) compares a re-rendered update with the released one as strings.

   After the change a released source in today's form does not parse, and a re-rendered update differs in every `EXISTS`. A release file holds payloads and no text, so it is not affected. With the protocol unreleased, the repo's packs are released again with `rpack release --force` once their sources are rewritten. The only one is `packs/editor`. rpack's code is not changed.

Not touched: the engine in mvt, [`rtable/touch.ts`](../modules/rdb/src/rtable/touch.ts), the atoms `users/users.ts` builds, and the `exists` payloads in the tests, each of which is a leaf.

## Tests

- **Today's behavior first.** Before the tree lands, as tests of the code as it is:
  - An update by a `'*'` holder concurrent with a live delete of its doc is live under the example rule written as two `EXISTS` atoms.
  - It is void under `docs.resource = 'r1' AND (EXISTS … resource = '*')`, a rule that reads the doc in every arm.

  They are written in today's grammar and rewritten with the other sources. When the tree lands, the first gets a twin written as one `EXISTS`, with the same expected result (`[PERM25]`).
- **`[PERM23]`**: an `OR` rule with a clean candidate through one arm, beside a candidate in a ring of revokes through the other, holds, as `[PERM19]` does for one leaf.
- **`[PERM24]`**: the ring of `[PERM17]`, where three admins revoke each other and one also revokes an outsider, under an `OR` rule. The outsider's cap is not a candidate of the ring members' rule, is not read, and stays out of the ring's core.
- **`[PERM26]`**: the example rule as a delete rule, with two `'*'` holders deleting one doc concurrently. Both deletes are live, as one `EXISTS` and as two.
- **`[OBSFRAME22]`**: the race of *Why slots are checked per leaf*, with the rule written as one `EXISTS`. The delete is live and the import void, as with two atoms.
- **`[VREAD06]`** gains tree reads among the read rules it checks exact, sound and monotone over every state: an `$or` of two leaves, and an `$and` of a leaf and an `$or`.
- **`[VREAD07]`**, new: a tree with a `$row` term in one arm only, evaluated through `evaluatePredicateTruth` in every state, against the model's distributed form, on a support contained in it. Its fixture has a second row with its own verdicts, so that the subject row is not the only witness.
- **The model of the rules**, in `rdb/test/verdict_reads_tests.ts` beside `[VREAD06]`.
  - **What it is.** The program described under *Why it is sound*, with values and supports built by mvt's `allTruth` and `anyTruth`. It is also `[VREAD07]`'s oracle.
  - **How it runs.** A subset runs in the suite, and the rest behind a variable, as `TOUCHR_SCALE` does in `rdb/test/touch_rule_tests.ts`.

  It goes in with the first tests.
- **The arm functions**, against their definitions: for every marking of small trees, `X`, `T0` and `T$` as the code computes them equal the arms built by listing every way. They read nothing, so no states are needed. This is what catches an arm that is undecided where it should be, on a support that is too large. The pending-import tests reach the arms through a single configuration.
- **Terms with no value**:
  - an anonymous op, an absent parameter and a subject column with no value, each in a leaf under an `$and` (the atom false) and under an `$or` (the other arm decides);
  - a set-aside leaf and a leaf with no value in one `$and`: false, not undecided. For one leaf that is an undecided subject value beside a subject column with no value.
- **A missing target**: one leaf with an undecided subject value and a target in a group that is not bound is undecided, as today.
- **Defaults**:
  - under an `$or` with a field at its default, a row that never wrote the column is a witness;
  - a leaf that pairs a default field with another field lists no rows beyond the other field's;
  - an `$or` with a default member inside an `$and` lists no rows beyond those of the `$and`'s other members.
- **The touch rule**:
  - `[TOUCHR01]` and `[TOUCHR02]` check that a decided read holds in every outcome of the pending imports. They include tree reads with touched leaves.
  - `[TOUCHR03]` pins broken variants of the rule on configurations where they decide wrongly. It gains "carried judged at `D`", pinned on a tree with an arm whose value only a pending version carries.
- **One test per reader of `where`**, with the offending term inside an `$or`:
  - a `$row` column that is not readonly, and one that does not exist;
  - an undeclared parameter;
  - `$author` in a group with no identity provider;
  - a field that is not pub, and a value of the wrong type, for a local target, a bound group and a FILES;
  - `drop-column` of a column that only a group names, as a field and as a `$row` term.
- **Validation** (`rschema_tests.ts`): each of these is rejected:
  - a one-member group;
  - two leaves in an `$and`;
  - a group with a second key;
  - an unknown `$` key;
  - a 65th equality;
  - nesting past the depth limit.
- **Rendering** (`format_predicate_tests.ts`):
  - the delimited form, with the parentheses the filter needs;
  - `IN` for an `$or` of one-field leaves on one column, equal members included;
  - no parentheses around an `exists` inside a group.
- **C-SQL** (`parser_tests.ts`, `roundtrip_tests.ts`):
  - **Lowering:** each shape above, `IN` included.
  - **Today's form:** a parse error that shows the new one.
  - **Names:** each of these is an error:
    - a bare name that both tables have (`EXISTS (caps WHERE caps.label = 'admin' AND owner = $author)`, with `owner` in both);
    - a bare name only the gated table has;
    - a bare name the gated table has, with a target in a bound group;
    - a bare name in a self-referential `EXISTS`.
  - **`IN`:** one value, a repeated value, a value of each kind, an `IN` under an `OR`, and each rendered back as `IN`.
  - **A column named twice in one `AND`:** the error, with its suggested rewrite.
  - **A nested `EXISTS`** in a filter: a syntax error.
  - **Round trip:** rendering, parsing and lowering give back the same payload.
- **Released text**: `packs/editor` released again, after which `rpack build` reports that it makes the update.sql it was released with.

## Considered and not taken

- **Distributing in the compiler.** No engine change, but the result is exponential, bounded by 64 members per `or`, and it is what dumps and failure messages would then show. It is also a different rule, since its supports are larger (*Why it is sound*).
- **A tagged tree** (`{ w: 'eq' | 'and' | 'or' }`). Uniform, but every existing `where` would change shape and hash, and the map already is the conjunction.
- **The predicate's own tags inside `where`** (`{ p: 'and', args }`). A table can have columns named `p` and `args`, so a group and a leaf could not be told apart.
- **`EXISTS t WHERE (…)`**, with the `WHERE` ending at its closing parenthesis. It reads well, but a parenthesis that groups can end the `EXISTS`:
  - **In new text.** `EXISTS caps WHERE (caps.label = 'admin') OR (grantee = $author)` would end at the first `)`, and read `grantee` as the gated table's whenever it has one, with no error.
  - **In text written today.** `EXISTS caps WHERE (caps.label = 'admin') AND (owner = $author)` compiles today with both comparisons inside the `EXISTS`, and would change meaning instead of failing.
- **Today's form with a rule of precedence.**
  - **With the rule for names,** it is safe, since anything foreign inside the `WHERE` becomes an error. But a reader needs the precedence and both schemas to see what the `EXISTS` covers.
  - **Ending the `WHERE` at an `OR`** is worse: an arm meant for the `EXISTS` is read as a comparison on the gated row, which nothing can flag.
- **Keeping today's form beside the new one.** It keeps the trap.
- **A parser that keeps reading today's form for released text.** That means two grammars, for packs that can be released again while the protocol is unreleased.
- **`IN` with a subquery.** It is not an equality on the target's index.
- **`IN` with literals only, or with constants only** (literals and parameters). A list of constants is easier to scan. But `IN` stands for an `OR` of equalities, and a narrower list would differ from that `OR` only in what the parser allows. The common case also needs a correlation beside a literal: a grant for this resource, or a wildcard. And the renderer could write `IN` only for some groups.
- **A value on the left and columns on the right**, as in `$author IN (c.grantee, c.delegate)`. SQL allows it, and it lowers to equalities too. It needs a second rule in the renderer, for an `$or` of one-field leaves with one value on different columns. It can come later, changing only the parser, the renderer, and the rendered text of such groups.
- **An error for a value listed twice in an `IN`.** `a = 1 OR a = 1` is a valid payload, since groups may have equal members, and it renders as `a IN (1, 1)`. With the error, that payload would have no text that parses back to it. Rejecting equal members in the payload instead would make `where` groups differ from predicate groups, for no gain at run time.
- **Several leaves in an `$and` when they share a column**, so that `a = v1 AND a = v2` has a payload. One text would then have several payloads, unless a further rule decided which leaf holds which field. The rewrite in *C-SQL*'s errors says the same thing.
- **A limit of its own for trees.** One limit of 64 equalities is plenty for permission rules, and it is the same constant as today's.
- **A warning for a default under `OR`**, in the compiler, when the root's set is `ALL`. It would catch a likely mistake: `grantee = $author AND role = 'admin' OR role = 'member'` with `'member'` the default reads as "or any member cap exists". It is left to rpack, which can warn later without any change to the protocol. The compiler could judge only literal values on local targets: `$author`, parameters and correlations can resolve to the default when the rule runs, and a bound group's defaults change when it migrates. Its compile path also produces no warnings today.
- **Rejecting a default under `OR` at validation.** `$author`, a parameter or a subject value can resolve to the default when the rule runs, so validation cannot see every case, and the read must handle it anyway.
- **Evaluating the whole tree on every candidate**, with no first pass on "carried". Simpler, but it reads columns of rows that cannot match through that arm, which makes more reads undecided and cores larger.
- **The subject row read for the whole atom**, as an earlier draft had it. It changes verdicts (*Why it is sound*, "Why the subject row is read per leaf").
- **One slot check for the whole read**, as the earlier drafts had it. It is undecided where the distributed form decides, and it changes verdicts (*Why it is sound*, "Why slots are checked per leaf").
- **`or(R0, and(S, R$), and(S, X))` for an undecided subject row**, as an earlier draft had it. It has the same values, but when `R$` is true it also rests on the subject values' entries, which cannot change the result.
- **`or(R0, and(S, R))` for an undecided subject row**, with the whole read in place of `R$`. It has the same values, but the support also holds the subject's entries when no `$row` leaf can match.
- **The subject's liveness as a conjunct of each `$row` leaf, per candidate.** The obvious implementation, and it changes the support of atoms that exist today. Today the read stops at the first candidate that surely matches, so `and(S, true)` rests on the subject alone. With `S` inside the leaf, no candidate is surely true while `S` is undecided, and the read collects the other candidates' supports.
- **Undecided subject values kept at the level of the atom.** That is undecided in states where the distributed form is true, through an arm that names no such value.
- **Reading one column once for an `$or` of one-field leaves on it**, so that `label IN ('writer', 'manager')` is decided when an undecided write sits between the two values.
  - **Nothing is lost without it,** since the distributed form is no better.
  - **It is a read rule of its own.** It needs a column read that returns the values the column can resolve to. While the protocol is unreleased it can come in its own HIP, and `IN`, the usual way to write such a group, is where it would apply.
- **Other comparisons** (`!=`, `<`, `LIKE`) on the target's columns. Each needs a candidate rule of its own and an index to back it, since a write is indexed by the exact value it carries.
- **`NOT EXISTS`.** A negative read. It needs a rule for an insert concurrent with the op that reads it.

// Payloads for RSchema operations, and their format validators.
//
// An RSchema is a standalone RObject in its own DAG: the spec for one table
// group. It evolves independently of any group instance; multiple RTableGroup
// instances may share one RSchema. A schema-update op carries ONLY migration
// rules (the slot writes); deploying it is a separate act (each group's
// barrier ref-advance to the new RSchema version).
//
// Spec authority: the create payload names the schema's creators (keyIds plus
// public keys); every schema-update must be signed by one of them. This gates
// spec evolution. Deploy authority is separate: it belongs to each observing
// group, gated by the group's own canDeploy predicate (fixed at group
// creation; see ../rtable_group/payload.ts).
//
// Sub-types shared across RSchema payloads (TableDef, MigrationRule, etc.),
// their json.Format validators, and related model defaults. Semantic
// validation beyond format structure is in validate.ts.

import { json } from "@hyper-hyper-space/hhs3_json";
import { KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { createPayloadTypeFormat } from "@hyper-hyper-space/hhs3_mvt";

// Size limits (payload formats)

export const MAX_NAME_LENGTH = 256;
export const MAX_QUALIFIED_NAME_LENGTH = 2 * MAX_NAME_LENGTH + 1;
export const MAX_SEED_LENGTH = 1024;
export const MAX_HASH_ALGORITHM_LENGTH = 256;
export const MAX_KEY_ID_LENGTH = 256;
export const MAX_SIGNATURE_LENGTH = 8192;
export const MAX_PUBLIC_KEY_LENGTH = 8192;
export const MAX_TABLES = 1024;
export const MAX_COLUMNS = 1024;
export const MAX_FKS = 256;
export const MAX_RESTRICTIONS = 64;
export const MAX_MIGRATION_RULES = 1024;

export const MAX_NOTE_LENGTH = 4096;
export const MAX_CREATORS = 64;
export const MAX_HASH_LENGTH = 128;

// Column types

export type ColumnType = 'string' | 'integer' | 'float' | 'boolean' | 'json';

export const COLUMN_TYPES: ColumnType[] = ['string', 'integer', 'float', 'boolean', 'json'];

export type ColumnDef = {
    type: ColumnType;
    nullable?: boolean;        // default false; nullable columns may be absent from a row
    default?: json.Literal;    // used when inserts omit the column
    pub?: boolean;             // default false; meta-exported (insert + update), filterable
    readonly?: boolean;        // default false; fixed at insert: updates rejected.
                               // Independent of pub; permission-witness columns
                               // (caps labels) should be pub + readonly.
};

export const columnDefFormat: json.Format = {
    type: [json.Type.Union, COLUMN_TYPES.map((t) => [json.Type.Constant, t] as json.Format)],
    nullable: [json.Type.Option, json.Type.Boolean],
    default: [json.Type.Option, json.Type.Something],
    pub: [json.Type.Option, json.Type.Boolean],
    readonly: [json.Type.Option, json.Type.Boolean],
};

// Deletes are permanent (a rowId is a write-once identity; no re-insertion).
// concurrentDeletes governs the delete's reach across concurrency: when true
// (the default) a delete is a barrier that also hides the row at view
// positions concurrent with it (and voids concurrent restriction uses
// witnessing the row); when false the delete acts only causally.

export const DEFAULT_CONCURRENT_DELETES = true;

// Foreign keys: column -> 'table' | 'group.table', AT-USE semantics.
//
// ENFORCEMENT NOTE. Checking the FK when the dependent row is written is the
// easy half; the hard half is the referenced row being deleted later, since
// the referenced row is completely oblivious to who points at it (there is
// no reverse index in the data, and coordination-free there cannot be one:
// the deleter may not have even received the ops that reference the row).
// So we never PREVENT the delete. Referential integrity is folded into AT-USE
// op-voiding, on the same view-time path used to recheck restrictions after
// they pass hard validation (see computeEntryVoided in ../rtable_group/group.ts
// and evaluateRowOpFKReach in ../rtable_group/predicates.ts):
//
//   - a write op whose own FK column points at a target that is not live at
//     the OP's own position (observed from the view's `from`) is VOID — a
//     voided insert never lives; a voided FK-update contributes no value, so
//     LWW reverts to the prior write. The FK SET itself is read from the
//     schema at the op's position, so a causally-later add-fk / drop-fk does
//     not revise an old write.
//   - this is anchored at the op, not the view horizon, so a target deleted
//     CONCURRENTLY with the write voids it at the merge (merge stability),
//     while a causally-LATER delete is inert (use-before-revoke): the
//     dependent becomes live-but-dangling rather than cascade-hidden. A
//     reference cycle resolves to DENY (the group's least-fixpoint void guard).
//
// Insert-time-only checking is unsound on its own (a delete concurrent with
// the dependent insert leaves a dangling reference both authors validly
// produced); at-use op-voiding resolves that case deterministically at the
// merge. To bound the one-time adoption, add-fk carries a DEPLOY PREREQUISITE
// (validate_ops.ts validateDeploy): a deploy whose new FK would strand an
// existing live row is hard-rejected.
//
// Cost lands on reads: voiding resolves the referenced row id at the op
// position (positional cover queries on the shared DAG for local FKs; the
// ref-advance-resolved foreign version for 'group.table' FKs), memoized per
// (entry, from) in the group's void cache.

export type FKs = { [column: string]: string };

export const fksFormat: json.Format =
    [json.Type.BoundedMap, [json.Type.BoundedString, MAX_NAME_LENGTH], [json.Type.BoundedString, MAX_QUALIFIED_NAME_LENGTH], MAX_FKS];

// Restrictions: at-use predicates gating row ops.

export type IdTerm = '$author';

export const ID_TERMS: IdTerm[] = ['$author'];

// A `$row.<column>` term references a readonly column of the subject row (the
// row being inserted / updated / deleted). Restricted to readonly columns so
// the value is fixed at insert (merge stability) and reading it never re-enters
// the current op's own value resolution. Row-context only (no subject row in
// 'object' context). Validated structurally (shape) in validate.ts and
// semantically (existing readonly column) in the table-aware checks.
export const ROW_FIELD_PREFIX = '$row.';

export type RowFieldTerm = string;   // matches /^\$row\.[a-zA-Z_][a-zA-Z0-9_]*$/

// Returns the column name of a `$row.<col>` term, or undefined if `s` is not a
// well-formed row-field term.
export function parseRowFieldTerm(s: string): string | undefined {
    if (!s.startsWith(ROW_FIELD_PREFIX)) return undefined;
    const column = s.substring(ROW_FIELD_PREFIX.length);
    return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(column) ? column : undefined;
}

// A `where` value is a literal, an identity term ($author), or a
// subject-row field term ($row.<col>).
export type WhereValue = json.Literal | IdTerm | RowFieldTerm;

export type PredicateContext = 'row' | 'object';

// Operand of a value expression (cmp / str atoms). `col` resolves to the
// subject row's readonly column value ($row.<col>); arithmetic is integer-only,
// `len` takes a string and yields an integer.
export type Operand =
    | { lit: json.Literal }
    | { col: string }
    | { fn: 'add' | 'sub' | 'mul'; args: [Operand, Operand] }
    | { fn: 'len'; args: [Operand] };

export const CMP_OPS = ['eq', 'ne', 'lt', 'le', 'gt', 'ge'] as const;
export type CmpOp = typeof CMP_OPS[number];

export const STR_OPS = ['prefix', 'suffix', 'contains'] as const;
export type StrOp = typeof STR_OPS[number];

export const ARITH_FNS = ['add', 'sub', 'mul'] as const;

export type Predicate =
    | { p: 'true' }
    | { p: 'false' }
    | { p: 'exists'; table: string; where: { [field: string]: WhereValue } }
    | { p: 'cmp'; cmp: CmpOp; left: Operand; right: Operand }
    | { p: 'str'; str: StrOp; value: Operand; sub: Operand }
    | { p: 'and'; args: Predicate[] }
    | { p: 'or'; args: Predicate[] };

export type OpTag = 'insert' | 'update' | 'delete' | 'all';

export const OP_TAGS: OpTag[] = ['insert', 'update', 'delete', 'all'];

export type Restriction = {
    on: OpTag;
    rule: Predicate;
};

// json.Format cannot express recursive structures; predicates are checked with
// validatePredicate in validate.ts. Format fields holding predicates use
// json.Type.Something as a placeholder.
export const restrictionFormat: json.Format = {
    on: [json.Type.Union, OP_TAGS.map((t) => [json.Type.Constant, t] as json.Format)],
    rule: json.Type.Something,
};

export function defaultRestrictionRule(op: 'insert' | 'update' | 'delete'): Predicate {
    return op === 'insert'
        ? { p: 'true' }
        : { p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: '$author' } };
}

// Identity provider: a table whose rows map a keyId to a publicKey, used to
// verify op signatures (authentication). The designation is STRUCTURAL: it is
// carried on the TableDef itself (written only by add-table; no set-* rule
// changes it). Both columns must be string-typed, pub AND readonly, and
// distinct (validated in validate.ts). A provider row is self-certifying:
// keyId == keyIdFromPublicKey(publicKey) is enforced at insert.

export type IdProvider = {
    keyIdColumn: string;
    publicKeyColumn: string;
};

export const idProviderFormat: json.Format = {
    keyIdColumn: [json.Type.BoundedString, MAX_NAME_LENGTH],
    publicKeyColumn: [json.Type.BoundedString, MAX_NAME_LENGTH],
};

// Returns [groupName, tableName]; groupName is undefined for local references.
export function splitTableRef(ref: string): [string | undefined, string] {
    const idx = ref.indexOf('.');
    if (idx < 0) return [undefined, ref];
    return [ref.substring(0, idx), ref.substring(idx + 1)];
}

// Table definitions

export type TableDef = {
    name: string;
    columns: { [column: string]: ColumnDef };
    concurrentDeletes?: boolean;
    fks?: FKs;
    restrictions?: Restriction[];
    idProvider?: IdProvider;       // this table maps keyId -> publicKey (structural)
};

export const tableDefFormat: json.Format = {
    name: [json.Type.BoundedString, MAX_NAME_LENGTH],
    columns: [json.Type.BoundedMap, [json.Type.BoundedString, MAX_NAME_LENGTH], columnDefFormat, MAX_COLUMNS],
    concurrentDeletes: [json.Type.Option, json.Type.Boolean],
    fks: [json.Type.Option, fksFormat],
    restrictions: [json.Type.Option, [json.Type.BoundedArray, restrictionFormat, MAX_RESTRICTIONS]],
    idProvider: [json.Type.Option, idProviderFormat],
};

// Migration rules: the slot writes of schema evolution (rules-only schema-updates).

export type MigrationRule =
    | { rule: 'add-table'; def: TableDef }
    | { rule: 'drop-table'; table: string }
    | { rule: 'add-column'; table: string; column: string; def: ColumnDef }
    | { rule: 'drop-column'; table: string; column: string }
    | { rule: 'set-concurrent-deletes'; table: string; value: boolean }
    | { rule: 'set-fks'; table: string; fks: FKs }
    | { rule: 'set-restrictions'; table: string; restrictions: Restriction[] };

export const migrationRuleFormat: json.Format = [json.Type.Union, [
    {
        rule: [json.Type.Constant, 'add-table'],
        def: tableDefFormat,
    },
    {
        rule: [json.Type.Constant, 'drop-table'],
        table: [json.Type.BoundedString, MAX_NAME_LENGTH],
    },
    {
        rule: [json.Type.Constant, 'add-column'],
        table: [json.Type.BoundedString, MAX_NAME_LENGTH],
        column: [json.Type.BoundedString, MAX_NAME_LENGTH],
        def: columnDefFormat,
    },
    {
        rule: [json.Type.Constant, 'drop-column'],
        table: [json.Type.BoundedString, MAX_NAME_LENGTH],
        column: [json.Type.BoundedString, MAX_NAME_LENGTH],
    },
    {
        rule: [json.Type.Constant, 'set-concurrent-deletes'],
        table: [json.Type.BoundedString, MAX_NAME_LENGTH],
        value: json.Type.Boolean,
    },
    {
        rule: [json.Type.Constant, 'set-fks'],
        table: [json.Type.BoundedString, MAX_NAME_LENGTH],
        fks: fksFormat,
    },
    {
        rule: [json.Type.Constant, 'set-restrictions'],
        table: [json.Type.BoundedString, MAX_NAME_LENGTH],
        restrictions: [json.Type.BoundedArray, restrictionFormat, MAX_RESTRICTIONS],
    },
]];

// RSchema operation payloads

export type RSchemaPayload = CreateRSchemaPayload | SchemaUpdatePayload;

export type SchemaCreator = {
    keyId: KeyId;
    publicKey: string;
};

export const schemaCreatorFormat: json.Format = {
    keyId: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    publicKey: [json.Type.BoundedString, MAX_PUBLIC_KEY_LENGTH],
};

export const RSCHEMA_TYPE_ID = 'hhs/rschema_v1';

export type CreateRSchemaPayload = {
    action: 'create';
    type: string;
    name: string;
    creators: SchemaCreator[];             // may sign schema-updates; at least one
    tables: TableDef[];
    hashAlgorithm?: string;
};

export const createRSchemaFormat: json.Format = {
    action: [json.Type.Constant, 'create'],
    type: createPayloadTypeFormat(RSCHEMA_TYPE_ID),
    name: [json.Type.BoundedString, MAX_NAME_LENGTH],
    creators: [json.Type.BoundedArray, schemaCreatorFormat, MAX_CREATORS],
    tables: [json.Type.BoundedArray, tableDefFormat, MAX_TABLES],
    hashAlgorithm: [json.Type.Option, [json.Type.BoundedString, MAX_HASH_ALGORITHM_LENGTH]],
};

// Rules-only: the effective schema is derived by the per-slot LWW resolution
// and never serialized. Must be signed by one of the schema's creators.

export type SchemaUpdatePayload = {
    action: 'schema-update';
    migration: MigrationRule[];            // the slot writes; at least one
    note?: string;
    author: KeyId;
    signature: string;
};

export const schemaUpdateFormat: json.Format = {
    action: [json.Type.Constant, 'schema-update'],
    migration: [json.Type.BoundedArray, migrationRuleFormat, MAX_MIGRATION_RULES],
    note: [json.Type.Option, [json.Type.BoundedString, MAX_NOTE_LENGTH]],
    author: [json.Type.BoundedString, MAX_KEY_ID_LENGTH],
    signature: [json.Type.BoundedString, MAX_SIGNATURE_LENGTH],
};

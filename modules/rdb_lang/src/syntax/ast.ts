import type { json } from "@hyper-hyper-space/hhs3_json";

import type { TextSpan } from "../diagnostics.js";

export type AstScript = {
    kind: 'script';
    statements: AstStatement[];
    span: TextSpan;
};

export type AstStatement =
    | CreateDatabaseStatement
    | CreateSchemaStatement
    | CreateTableGroupStatement
    | AddMemberStatement
    | AlterSchemaStatement
    | UpdateSchemaStatement
    | UpdateRefStatement
    | InsertStatement
    | UpdateStatement
    | DeleteStatement
    | BundleStatement
    | SetViewStatement
    | SelectStatement
    | LogStatement;

export type NameRef = {
    kind: 'name';
    text: string;
    parts: string[];
    span: TextSpan;
};

export type HashRef = {
    kind: 'hash';
    prefix: string;
    span: TextSpan;
};

export type NameOrHashRef = NameRef | HashRef;

export type VersionMember = HashRef | NameRef;

export type VersionExpr =
    | { kind: 'latest'; span: TextSpan }
    | { kind: 'hash'; hash: HashRef; span: TextSpan }
    | { kind: 'set'; members: VersionMember[]; span: TextSpan };

// The author of an authored statement, written with a trailing `BY` clause.
// `$name` / `#prefix` name an unlocked identity to sign as; `NOBODY` forces an
// explicitly unauthored op even when the session has a default author. Absence
// of the clause (an undefined `author` field) falls back to the default author.
export type AuthorExpr =
    | { kind: 'nobody'; span: TextSpan }
    | { kind: 'variable'; name: string; span: TextSpan }
    | { kind: 'hash'; prefix: string; span: TextSpan };

export type ValueExpr =
    | { kind: 'literal'; value: json.Literal | null; span: TextSpan }
    | { kind: 'variable'; name: string; field?: string; span: TextSpan }
    | { kind: 'hash'; prefix: string; span: TextSpan }
    | { kind: 'call'; name: string; args: ValueExpr[]; span: TextSpan };

export type TableRef = {
    group?: NameOrHashRef;
    table: string;
    span: TextSpan;
};

export type ColumnTypeName = 'string' | 'integer' | 'float' | 'boolean' | 'json' | 'bigint' | 'decimal' | 'bytes';

// Constraints parsed from the column declaration: parenthesized type params
// (STRING(n) / BYTES(n) -> maxLength; DECIMAL(p, s) -> precision, scale) and
// MIN / MAX modifiers (numeric-literal bounds). Bounds are the raw literal text
// here; bind canonically encodes them per the column type.
export type ColumnConstraintsExpr = {
    maxLength?: number;
    precision?: number;
    scale?: number;
    min?: ValueExpr;
    max?: ValueExpr;
};

export type ColumnDecl = {
    name: string;
    type: ColumnTypeName;
    nullable: boolean;
    defaultValue?: ValueExpr;
    pub: boolean;
    readonly: boolean;
    references?: string;
    constraints?: ColumnConstraintsExpr;
    span: TextSpan;
};

export type AllowOp = 'insert' | 'update' | 'delete' | 'all';

export type AllowRuleExpr = {
    op: AllowOp;
    predicate: PredicateExpr;
    span: TextSpan;
};

export type TableOption =
    | { kind: 'concurrent-deletes'; value: boolean; span: TextSpan }
    | { kind: 'identity-provider'; keyIdColumn: string; publicKeyColumn: string; span: TextSpan }
    | ({ kind: 'allow-rule' } & AllowRuleExpr);

export type TableDecl = {
    name: string;
    columns: ColumnDecl[];
    options: TableOption[];
    span: TextSpan;
};

export type CreateSchemaStatement = {
    kind: 'create-schema';
    name: string;
    creators: ValueExpr[];
    tables: TableDecl[];
    span: TextSpan;
};

export type CreateDatabaseStatement = {
    kind: 'create-database';
    name: string;
    seed?: string;
    creators: ValueExpr[];
    span: TextSpan;
};

export type InitialRow = {
    table: string;
    values: { column: string; value: ValueExpr }[];
    span: TextSpan;
};

export type CreateTableGroupStatement = {
    kind: 'create-tablegroup';
    name: string;
    seed?: string;
    schema: NameOrHashRef;
    schemaVersion?: VersionExpr;
    bindings: { name: string; group: NameOrHashRef; span: TextSpan }[];
    idProvider?: string;
    // deploy gate: `ALLOW UPDATE SCHEMA IF <predicate>`.
    canDeploy?: PredicateExpr;
    // per-binding observation gate: `ALLOW UPDATE REF <binding> IF <predicate>`.
    canObserve: { binding: string; predicate: PredicateExpr; span: TextSpan }[];
    initialRows: InitialRow[];
    span: TextSpan;
};

export type AddMemberStatement = {
    kind: 'add-member';
    member: 'schema' | 'tablegroup';
    target: NameOrHashRef;
    database: NameOrHashRef;
    note?: string;
    author?: AuthorExpr;
    at?: VersionExpr;
    span: TextSpan;
};

export type InsertStatement = {
    kind: 'insert';
    table: TableRef;
    columns: string[];
    values: ValueExpr[];
    author?: AuthorExpr;
    at?: VersionExpr;
    span: TextSpan;
};

export type UpdateStatement = {
    kind: 'update';
    table: TableRef;
    values: { column: string; value: ValueExpr }[];
    rowId: NameOrHashRef | ValueExpr;
    author?: AuthorExpr;
    at?: VersionExpr;
    span: TextSpan;
};

export type DeleteStatement = {
    kind: 'delete';
    table: TableRef;
    rowId: NameOrHashRef | ValueExpr;
    author?: AuthorExpr;
    at?: VersionExpr;
    span: TextSpan;
};

export type BundleWriteStatement = InsertStatement | UpdateStatement | DeleteStatement;

export type BundleStatement = {
    kind: 'bundle';
    group: NameOrHashRef;
    writes: BundleWriteStatement[];
    author?: AuthorExpr;
    at?: VersionExpr;
    span: TextSpan;
};

export type SetViewStatement = {
    kind: 'set-view';
    at: VersionExpr;
    from?: VersionExpr;
    span: TextSpan;
};

export type MigrationRuleExpr =
    | { kind: 'add-table'; table: TableDecl; span: TextSpan }
    | { kind: 'drop-table'; table: string; span: TextSpan }
    | { kind: 'add-column'; table: string; column: ColumnDecl; span: TextSpan }
    | { kind: 'drop-column'; table: string; column: string; span: TextSpan }
    | { kind: 'set-concurrent-deletes'; table: string; value: boolean; span: TextSpan }
    | { kind: 'set-fks'; table: string; fks: { [column: string]: string }; span: TextSpan }
    | { kind: 'set-allow-rules'; table: string; allowRules: AllowRuleExpr[]; span: TextSpan };

export type AlterSchemaStatement = {
    kind: 'alter-schema';
    schema: NameOrHashRef;
    rules: MigrationRuleExpr[];
    author?: AuthorExpr;
    at?: VersionExpr;
    span: TextSpan;
};

export type UpdateSchemaStatement = {
    kind: 'update-schema';
    schema: NameOrHashRef;
    version: VersionExpr;
    group: NameOrHashRef;
    author?: AuthorExpr;
    at?: VersionExpr;
    span: TextSpan;
};

export type UpdateRefStatement = {
    kind: 'update-ref';
    ref: NameOrHashRef;
    version: VersionExpr;
    group: NameOrHashRef;
    author?: AuthorExpr;
    at?: VersionExpr;
    span: TextSpan;
};

export type SelectStatement = {
    kind: 'select';
    projection: '*' | string[];
    table: TableRef;
    where?: PredicateExpr;
    orderBy: { column: string; dir?: 'asc' | 'desc'; span: TextSpan }[];
    limit?: number;
    offset?: number;
    at?: VersionExpr;
    from?: VersionExpr;
    span: TextSpan;
};

export type LogStatement = {
    kind: 'log';
    target: NameOrHashRef;
    at?: VersionExpr;
    from?: VersionExpr;
    limit?: number;
    offset?: number;
    explain?: boolean;
    span: TextSpan;
};

export type PredicateExpr =
    | { kind: 'true'; span: TextSpan }
    | { kind: 'false'; span: TextSpan }
    | { kind: 'comparison'; op: '=' | '!=' | '<' | '<=' | '>' | '>='; left: OperandExpr; right: OperandExpr; span: TextSpan }
    | { kind: 'like'; left: OperandExpr; pattern: ValueExpr; span: TextSpan }
    | { kind: 'exists'; table: string; alias?: string; where: PredicateExpr; span: TextSpan }
    | { kind: 'not'; arg: PredicateExpr; span: TextSpan }
    | { kind: 'and'; args: PredicateExpr[]; span: TextSpan }
    | { kind: 'or'; args: PredicateExpr[]; span: TextSpan };

export type OperandExpr =
    | { kind: 'column'; table?: string; name: string; span: TextSpan }
    | ValueExpr;

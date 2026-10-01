import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { json } from "@hyper-hyper-space/hhs3_json";
import type { CatalogUpdateResult, Row, RowQuery } from "@hyper-hyper-space/hhs3_rdb";

import type { CreatePlan } from "../compile/create.js";
import type { RenderVersionScope } from "../reverse/aliases.js";
import type { VersionExpr } from "../syntax/ast.js";

export type InsertLangResult = {
    kind: 'insert';
    entryHash: B64Hash;
    table: string;
    rowId: B64Hash;
    uuid: string;
};

export type UpdateLangResult = {
    kind: 'update';
    entryHash: B64Hash;
    table: string;
    rowId: B64Hash;
};

export type DeleteLangResult = {
    kind: 'delete';
    entryHash: B64Hash;
    table: string;
    rowId: B64Hash;
};

export type BundleLangResult = {
    kind: 'bundle';
    entryHash: B64Hash;
    group: string;
    writes: number;
};

export type SetViewLangResult = {
    kind: 'set-view';
    at: VersionExpr;
    from?: VersionExpr;
};

export type AlterCatalogLangResult = {
    kind: 'alter-catalog';
    catalog: B64Hash;
    version: string;
    release: B64Hash;
    // Present when the release introduced schemas: the implied declare entry.
    declare?: B64Hash;
};

// A catalog release deployed into a database (UPDATE CATALOG, and the
// planner run after CREATE DATABASE).
export type UpdateCatalogLangResult = {
    kind: 'update-catalog';
    catalog: B64Hash;
    database: B64Hash;
    release: B64Hash;
    update: CatalogUpdateResult;
};

export type UseDatabaseLangResult = {
    kind: 'use-database';
    database: B64Hash;
};

export type AlterSchemaLangResult = {
    kind: 'alter-schema';
    entryHash: B64Hash;
    schema: string;
    rules: number;
};

export type UpdateRefLangResult = {
    kind: 'update-ref';
    entryHash: B64Hash;
    group: string;
    ref: string;
};

export type SelectLangResult = {
    kind: 'select';
    table: string;
    query: RowQuery;
    rows: Row[];
    columns?: string[];   // schema column names; set only for SELECT *
    // Columns whose rdb type is `identity`, plus a provider table's keyIdColumn.
    // Used by the REPL formatter to render key hashes as $labels by type
    // rather than by value-match heuristics.
    identityColumns?: string[];
};

export type LogRow = {
    hash: string;
    fullHash: B64Hash;
    prev: string[];
    payload: json.Literal;
    void?: boolean;
    reason?: string;
};

export type LogRenderContext = {
    schemaRef?: B64Hash;
    schemaName?: string;
    groupRef?: B64Hash;
    groupName?: string;
    tableName?: string;
    databaseName?: string;
    catalogRef?: B64Hash;
    catalogName?: string;
    deployLabels?: { [versionKey: string]: string };
    versionScope?: RenderVersionScope;
};

export type LogLangResult = {
    kind: 'log';
    target: string;
    explain: boolean;
    renderContext: LogRenderContext;
    rows: LogRow[];
};

// PUT: the element written (the owner is set in a key section), whether its
// bytes had to be uploaded, and the entries appended.
export type PutFileLangResult = {
    kind: 'put-file';
    files: string;
    section: 'common' | 'key';
    owner?: string;
    path: string;
    fileHash: B64Hash;
    size: number;
    uploaded: boolean;
    entries: B64Hash[];
};

// GET: inline content (`text` or `b64`), or where it was written (`written`).
export type GetFileLangResult = {
    kind: 'get-file';
    files: string;
    section: 'common' | 'key';
    owner?: string;
    path: string;
    fileHash: B64Hash;
    size: number;
    text?: string;
    b64?: string;
    written?: string;
};

export type FileListRow = {
    section: 'common' | 'key';
    owner: string;            // '' in common
    path: string;
    size: number | null;      // null when no upload of it has arrived
    fileHash: B64Hash;
    complete: boolean;
};

export type ListFilesLangResult = {
    kind: 'list-files';
    files: string;
    columns: (keyof FileListRow)[];
    rows: FileListRow[];
};

export type CreatePlanResult = {
    kind: 'create-plan';
    plan: CreatePlan;
};

export type LangExecutionResult =
    | CreatePlanResult
    | AlterCatalogLangResult
    | UpdateCatalogLangResult
    | UseDatabaseLangResult
    | AlterSchemaLangResult
    | UpdateRefLangResult
    | InsertLangResult
    | UpdateLangResult
    | DeleteLangResult
    | BundleLangResult
    | SetViewLangResult
    | SelectLangResult
    | LogLangResult
    | PutFileLangResult
    | GetFileLangResult
    | ListFilesLangResult;

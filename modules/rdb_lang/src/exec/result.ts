import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { json } from "@hyper-hyper-space/hhs3_json";
import type { Row, RowQuery } from "@hyper-hyper-space/hhs3_rdb";

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

export type AddMemberLangResult = {
    kind: 'add-member';
    member: 'schema' | 'tablegroup';
    entryHash: B64Hash;
    database: B64Hash;
    memberId: B64Hash;
};

export type AlterSchemaLangResult = {
    kind: 'alter-schema';
    entryHash: B64Hash;
    schema: string;
    rules: number;
};

export type UpdateSchemaLangResult = {
    kind: 'update-schema';
    entryHash: B64Hash;
    group: string;
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
    versionScope?: RenderVersionScope;
};

export type LogLangResult = {
    kind: 'log';
    target: string;
    explain: boolean;
    renderContext: LogRenderContext;
    rows: LogRow[];
};

export type CreatePlanResult = {
    kind: 'create-plan';
    plan: CreatePlan;
};

export type LangExecutionResult =
    | CreatePlanResult
    | AddMemberLangResult
    | AlterSchemaLangResult
    | UpdateSchemaLangResult
    | UpdateRefLangResult
    | InsertLangResult
    | UpdateLangResult
    | DeleteLangResult
    | BundleLangResult
    | SetViewLangResult
    | SelectLangResult
    | LogLangResult;

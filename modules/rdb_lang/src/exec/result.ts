import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { json } from "@hyper-hyper-space/hhs3_json";
import type { Row, RowQuery } from "@hyper-hyper-space/hhs3_rdb";

import type { CreatePlan } from "../compile/create.js";
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

export type AlterSchemaLangResult = {
    kind: 'alter-schema';
    entryHash: B64Hash;
    schema: string;
    rules: number;
};

export type DeploySchemaLangResult = {
    kind: 'deploy-schema';
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
};

export type LogRow = {
    hash: string;
    fullHash: B64Hash;
    prev: string[];
    action?: string;
    type?: string;
    summary: string;
    payload: json.Literal;
};

export type LogLangResult = {
    kind: 'log';
    target: string;
    rows: LogRow[];
};

export type CreatePlanResult = {
    kind: 'create-plan';
    plan: CreatePlan;
};

export type LangExecutionResult =
    | CreatePlanResult
    | AlterSchemaLangResult
    | DeploySchemaLangResult
    | UpdateRefLangResult
    | InsertLangResult
    | UpdateLangResult
    | DeleteLangResult
    | BundleLangResult
    | SetViewLangResult
    | SelectLangResult
    | LogLangResult;

import { deriveRowId } from "@hyper-hyper-space/hhs3_rdb";
import { formatValidationFailure, ValidationRejectedError } from "@hyper-hyper-space/hhs3_mvt";

import { DiagnosticBag, err, ok, Result } from "../diagnostics.js";
import type { BoundExecutableStatement, BoundStatement } from "../bind/bind.js";
import { compileCreate } from "../compile/create.js";
import { executeLog } from "./history.js";
import type { LangExecutionResult, SelectLangResult } from "./result.js";

export async function execute(bound: BoundStatement): Promise<Result<LangExecutionResult>> {
    const diagnostics = new DiagnosticBag();
    try {
        if (bound.kind === 'create-database' || bound.kind === 'create-schema' || bound.kind === 'create-tablegroup') {
            return ok({ kind: 'create-plan', plan: await compileCreate(bound) });
        }
        return ok(await executeRuntime(bound));
    } catch (e) {
        if (e instanceof ValidationRejectedError) {
            diagnostics.add('VALIDATION_REJECTED', formatValidationFailure(e.why), bound.ast.span);
            return err(diagnostics.all());
        }
        diagnostics.add('EXECUTION_FAILED', e instanceof Error ? e.message : String(e), bound.ast.span);
        return err(diagnostics.all());
    }
}

async function executeRuntime(bound: BoundExecutableStatement): Promise<LangExecutionResult> {
    switch (bound.kind) {
        case 'add-member': {
            if (bound.database.db === undefined) throw new Error('ADD target database is not loaded');
            const entryHash = bound.member === 'schema'
                ? await bound.database.db.addSchema(bound.memberId, bound.note, bound.at)
                : await bound.database.db.addGroup(bound.memberId, bound.note, bound.at);
            return { kind: 'add-member', member: bound.member, entryHash, database: bound.database.id, memberId: bound.memberId };
        }
        case 'alter-schema': {
            if (bound.schema.schema === undefined) throw new Error('ALTER SCHEMA target is not loaded');
            const entryHash = await bound.schema.schema.updateSchema(bound.rules, bound.author, undefined, bound.at);
            return { kind: 'alter-schema', entryHash, schema: bound.schema.id, rules: bound.rules.length };
        }
        case 'update-schema': {
            if (bound.group.group === undefined) throw new Error('UPDATE SCHEMA target group is not loaded');
            const entryHash = await bound.group.group.deploy(bound.version, bound.author, bound.at);
            return { kind: 'update-schema', entryHash, group: bound.group.id };
        }
        case 'update-ref': {
            if (bound.group.group === undefined) throw new Error('UPDATE REF target group is not loaded');
            const entryHash = await bound.group.group.observe(bound.ref, bound.version, bound.author, bound.at);
            return { kind: 'update-ref', entryHash, group: bound.group.id, ref: bound.ref };
        }
        case 'insert': {
            const entryHash = await bound.table.table.insert(bound.uuid, bound.values, bound.author, bound.at);
            return {
                kind: 'insert',
                entryHash,
                table: `${bound.table.groupId}.${bound.table.tableName}`,
                rowId: deriveRowId(bound.uuid, bound.author?.keyId),
                uuid: bound.uuid,
            };
        }
        case 'update': {
            const entryHash = await bound.table.table.update(bound.rowId, bound.values, bound.author, bound.at);
            return { kind: 'update', entryHash, table: `${bound.table.groupId}.${bound.table.tableName}`, rowId: bound.rowId };
        }
        case 'delete': {
            const entryHash = await bound.table.table.delete(bound.rowId, bound.author, bound.at);
            return { kind: 'delete', entryHash, table: `${bound.table.groupId}.${bound.table.tableName}`, rowId: bound.rowId };
        }
        case 'bundle': {
            if (bound.group.group === undefined) throw new Error('BUNDLE target group is not loaded');
            const entryHash = await bound.group.group.bundle(bound.writes, bound.author, bound.at);
            return { kind: 'bundle', entryHash, group: bound.group.id, writes: bound.writes.length };
        }
        case 'set-view': {
            return bound.from === undefined
                ? { kind: 'set-view', at: bound.at }
                : { kind: 'set-view', at: bound.at, from: bound.from };
        }
        case 'select': {
            const view = await bound.table.table.getView(bound.at, bound.from);
            const result: SelectLangResult = {
                kind: 'select',
                table: `${bound.table.groupId}.${bound.table.tableName}`,
                query: bound.query,
                rows: await view.query(bound.query),
            };
            if (bound.ast.projection === '*') {
                result.columns = await view.getColumns();
            }
            return result;
        }
        case 'log':
            return executeLog(bound);
    }
}

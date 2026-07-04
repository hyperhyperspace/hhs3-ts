import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Entry } from "@hyper-hyper-space/hhs3_dag";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";
import type { CreateRDbPayload } from "@hyper-hyper-space/hhs3_rdb";
import { formatOpVoidDetail, isVoidCheckable } from "@hyper-hyper-space/hhs3_rdb";
import type { ResolvedLogTarget } from "../bind/context.js";
import type { BoundLog } from "../bind/bind.js";
import type { LogLangResult, LogRenderContext, LogRow } from "./result.js";

export async function executeLog(bound: BoundLog): Promise<LogLangResult> {
    const dag = await bound.target.object.getScopedDag();
    const allEntries: Entry[] = [];
    for await (const entry of dag.loadAllEntries()) allEntries.push(entry);

    const visible = filterAt(allEntries, bound.at);
    const offset = bound.ast.offset ?? 0;
    const limited = bound.ast.limit === undefined
        ? visible.slice(offset)
        : visible.slice(offset, offset + bound.ast.limit);

    const rows: LogRow[] = [];
    for (const entry of limited) {
        const row = toLogRow(entry);
        if ((bound.target.kind === 'group' || bound.target.kind === 'table')
            && isVoidCheckable(entry.payload)) {
            row.void = await bound.target.object.isEntryVoided(entry.hash, bound.from);
            if (bound.explain && row.void === true) {
                const detail = await bound.target.object.explainEntryVoided(entry.hash, bound.from);
                if (detail !== undefined) row.reason = formatOpVoidDetail(detail);
            }
        }
        rows.push(row);
    }

    return {
        kind: 'log',
        target: targetName(bound),
        explain: bound.explain,
        renderContext: await buildLogRenderContext(bound.target),
        rows,
    };
}

async function buildLogRenderContext(target: ResolvedLogTarget): Promise<LogRenderContext> {
    switch (target.kind) {
        case 'group': {
            const group = target.object;
            const groupId = group.getId();
            const groupName = group.getName();
            return {
                schemaRef: group.getSchemaRef(),
                groupRef: groupId,
                groupName,
                versionScope: { objectId: groupId, objectName: groupName },
            };
        }
        case 'schema': {
            const schema = target.object;
            const schemaName = schema.getName();
            return {
                schemaRef: schema.getId(),
                schemaName,
                versionScope: { objectId: schema.getId(), objectName: schemaName },
            };
        }
        case 'database': {
            const db = target.object;
            const dbId = db.getId();
            const genesis = await (await db.getScopedDag()).loadEntry(dbId);
            const databaseName = genesis === undefined
                ? 'database'
                : databaseNameFromPayload(genesis.payload);
            return {
                databaseName,
                versionScope: { objectId: dbId, objectName: databaseName },
            };
        }
        case 'table': {
            const table = target.object;
            const groupId = table.getGroupId();
            return {
                groupRef: groupId,
                versionScope: { objectId: groupId, objectName: groupId },
            };
        }
    }
}

function databaseNameFromPayload(payload: json.Literal): string {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return 'database';
    const create = payload as CreateRDbPayload;
    return create.name ?? create.seed ?? 'database';
}

function filterAt(entries: Entry[], at: Version): Entry[] {
    const byHash = new Map(entries.map((e) => [e.hash, e]));
    const reachable = new Set<B64Hash>();
    const visit = (hash: B64Hash) => {
        if (reachable.has(hash)) return;
        const entry = byHash.get(hash);
        if (entry === undefined) return;
        reachable.add(hash);
        for (const prev of json.fromSet(entry.header.prevEntryHashes) as B64Hash[]) visit(prev);
    };
    for (const hash of at) visit(hash);
    return entries.filter((entry) => reachable.has(entry.hash));
}

function toLogRow(entry: Entry): LogRow {
    return {
        hash: entry.hash,
        fullHash: entry.hash,
        prev: json.fromSet(entry.header.prevEntryHashes) as B64Hash[],
        payload: entry.payload,
    };
}

function targetName(bound: BoundLog): string {
    if (bound.target.kind === 'table') return `${bound.target.groupId}.${bound.target.tableName}`;
    return bound.target.id;
}

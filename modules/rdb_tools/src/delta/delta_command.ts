import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RSchemaDelta, RTableGroup, RTableGroupDelta, RTableGroupDeltaStrategy } from "@hyper-hyper-space/hhs3_rdb";

import { formatDelta } from "../format/delta.js";
import type { GroupDeltaPayload, SchemaDeltaPayload } from "./payload.js";
import type { WorkspaceSession } from "../session/session.js";
import { resolveVersionRef } from "../session/version.js";
import type { RootResolveContext } from "../workspace/root_index.js";

export type DeltaKind = 'schema' | 'group';

function rootCtx(session: WorkspaceSession): RootResolveContext {
    return { aliases: session.aliases };
}

function ref(text: string) {
    if (text.startsWith('#')) {
        return {
            kind: 'hash' as const,
            prefix: text.slice(1),
            span: { start: 0, end: text.length, line: 1, column: 1 },
        };
    }
    return {
        kind: 'name' as const,
        text,
        parts: text.split('.'),
        span: { start: 0, end: text.length, line: 1, column: 1 },
    };
}

function parseStrategy(text: string | undefined): RTableGroupDeltaStrategy | undefined {
    if (text === undefined) return undefined;
    if (text === 'bounded' || text === 'full') return text;
    throw new Error(`Unknown delta strategy '${text}' (expected bounded|full)`);
}

export async function runDeltaCommand(session: WorkspaceSession, args: string[]): Promise<string> {
    const [kind, name, startText, endText, strategyText] = args;
    if (kind !== 'schema' && kind !== 'group') {
        throw new Error('Usage: \\delta schema|group <name> <start> <end> [bounded|full]');
    }
    if (name === undefined || startText === undefined || endText === undefined) {
        throw new Error('Usage: \\delta schema|group <name> <start> <end> [bounded|full]');
    }

    const strategy = parseStrategy(strategyText);
    if (strategyText !== undefined && kind === 'schema') {
        throw new Error('Delta strategy applies to groups only (bounded|full)');
    }

    const ctx = rootCtx(session);

    if (kind === 'schema') {
        const resolved = await session.workspace.roots.resolveSchema(ref(name), ctx);
        if (resolved.schema === undefined) throw new Error('Schema is not loaded');
        const start = await resolveVersionRef(session, startText, resolved.id);
        const end = await resolveVersionRef(session, endText, resolved.id);
        const delta = await resolved.schema.computeDelta(start, end) as RSchemaDelta;
        const displayName = session.workspace.roots.get(resolved.id)?.name ?? name;
        return formatDelta(session, {
            kind: 'schema',
            name: displayName,
            objectId: resolved.id,
            delta,
        } satisfies SchemaDeltaPayload);
    }

    const resolved = await session.workspace.roots.resolveGroup(ref(name), ctx);
    if (resolved.group === undefined) throw new Error('Group is not loaded');
    const group = resolved.group as RTableGroup & { setDeltaStrategy(strategy: RTableGroupDeltaStrategy): void };
    if (strategy !== undefined) group.setDeltaStrategy(strategy);
    const start = await resolveVersionRef(session, startText, resolved.id);
    const end = await resolveVersionRef(session, endText, resolved.id);
    const delta = await group.computeDelta(start, end) as RTableGroupDelta;
    const tableIdToName = await buildTableIdMap(group);
    const displayName = session.workspace.roots.get(resolved.id)?.name ?? name;
    return formatDelta(session, {
        kind: 'group',
        name: displayName,
        objectId: resolved.id,
        delta,
        tableIdToName,
    } satisfies GroupDeltaPayload);
}

async function buildTableIdMap(group: RTableGroup): Promise<Map<B64Hash, string>> {
    const map = new Map<B64Hash, string>();
    const groupView = await group.getView();
    for (const tableName of groupView.getTableNames()) {
        const table = await group.getTable(tableName);
        map.set(table.getId(), tableName);
    }
    return map;
}

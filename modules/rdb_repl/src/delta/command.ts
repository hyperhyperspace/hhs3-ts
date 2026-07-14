import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RSchemaDelta, RTableGroup, RTableGroupDelta, RTableGroupDeltaStrategy } from "@hyper-hyper-space/hhs3_rdb";
import { resolveVersionRef, type RootResolveContext } from "@hyper-hyper-space/hhs3_rdb_runtime";
import type { ReplSession } from "../session.js";
import { formatDelta } from "../format/delta.js";
import type { GroupDeltaPayload, SchemaDeltaPayload } from "./payload.js";

export async function runDeltaCommand(session: ReplSession, args: string[]): Promise<string> {
    const [kind, name, startText, endText, strategyText] = args;
    if ((kind !== 'schema' && kind !== 'group') || name === undefined || startText === undefined || endText === undefined) {
        throw new Error('Usage: \\delta schema|group <name> <start> <end> [bounded|full]');
    }
    if (strategyText !== undefined && strategyText !== 'bounded' && strategyText !== 'full') {
        throw new Error(`Unknown delta strategy '${strategyText}' (expected bounded|full)`);
    }
    if (kind === 'schema' && strategyText !== undefined) {
        throw new Error('Delta strategy applies to groups only (bounded|full)');
    }
    const ctx: RootResolveContext = { aliases: session.aliases };
    if (kind === 'schema') {
        const resolved = await session.workspace.roots.resolveSchema(ref(name), ctx);
        if (resolved.schema === undefined) throw new Error('Schema is not loaded');
        return formatDelta(session, {
            kind,
            name: session.workspace.roots.get(resolved.id)?.name ?? name,
            objectId: resolved.id,
            delta: await resolved.schema.computeDelta(
                await resolveVersionRef(session, startText, resolved.id),
                await resolveVersionRef(session, endText, resolved.id),
            ) as RSchemaDelta,
        } satisfies SchemaDeltaPayload);
    }
    const resolved = await session.workspace.roots.resolveGroup(ref(name), ctx);
    if (resolved.group === undefined) throw new Error('Group is not loaded');
    const group = resolved.group as RTableGroup & { setDeltaStrategy(strategy: RTableGroupDeltaStrategy): void };
    if (strategyText !== undefined) group.setDeltaStrategy(strategyText);
    const delta = await group.computeDelta(
        await resolveVersionRef(session, startText, resolved.id),
        await resolveVersionRef(session, endText, resolved.id),
    ) as RTableGroupDelta;
    const tableIdToName = new Map<B64Hash, string>();
    const view = await group.getView();
    for (const tableName of view.getTableNames()) {
        tableIdToName.set((await group.getTable(tableName)).getId(), tableName);
    }
    return formatDelta(session, {
        kind,
        name: session.workspace.roots.get(resolved.id)?.name ?? name,
        objectId: resolved.id,
        delta,
        tableIdToName,
    } satisfies GroupDeltaPayload);
}

function ref(text: string) {
    const span = { start: 0, end: text.length, line: 1, column: 1 };
    return text.startsWith('#')
        ? { kind: 'hash' as const, prefix: text.slice(1), span }
        : { kind: 'name' as const, text, parts: text.split('.'), span };
}

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import { dumpGroupEntry, type LoggableObject } from "@hyper-hyper-space/hhs3_rdb_lang";
import type { ReplSession } from "../session.js";
import { createDumpRenderOptions } from "./alias_context.js";

type GroupObject = RTableGroup & LoggableObject;
type Resolved = { group: GroupObject; groupName: string; entryHash: B64Hash };

export async function runDumpOpCommand(session: ReplSession, args: string[]): Promise<string> {
    const { groupName, hashRef } = parseArgs(args);
    const resolved = groupName === undefined
        ? await findEntry(session, hashRef.slice(1))
        : await resolveEntry(session, groupName, hashRef);
    const rendered = await dumpGroupEntry(resolved.group, resolved.entryHash, {
        render: createDumpRenderOptions(session),
    });
    if (rendered === undefined) throw new Error(`Op '${hashRef}' is not in group`);
    return rendered;
}

function parseArgs(args: string[]): { groupName?: string; hashRef: string } {
    if (args.length === 1 && args[0].startsWith('#')) return { hashRef: args[0] };
    if (args.length >= 2 && args[1].startsWith('#')) return { groupName: args[0], hashRef: args[1] };
    throw new Error('Usage: \\dump op [group] #hash');
}

async function findEntry(session: ReplSession, prefix: string): Promise<Resolved> {
    const ordered = [
        ...(session.currentGroup === undefined ? [] : [session.currentGroup]),
        ...session.workspace.roots.list('group').map((root) => root.name ?? root.id),
    ];
    const matches: Resolved[] = [];
    const seen = new Set<B64Hash>();
    for (const name of ordered) {
        const resolved = await session.workspace.roots.resolveGroup(ref(name), { aliases: session.aliases });
        if (resolved.group === undefined || seen.has(resolved.id)) continue;
        const group = resolved.group as GroupObject;
        const entries: B64Hash[] = [];
        for await (const entry of (await group.getScopedDag()).loadAllEntries()) {
            if (entry.hash.startsWith(prefix)) entries.push(entry.hash);
        }
        if (entries.length > 1) throw new Error(`Ambiguous hash prefix '#${prefix}' in group '${name}'`);
        if (entries.length === 1) {
            seen.add(resolved.id);
            matches.push({ group, groupName: session.workspace.roots.get(resolved.id)?.name ?? name, entryHash: entries[0] });
        }
    }
    if (matches.length === 0) throw new Error(`Unknown hash prefix '#${prefix}'`);
    if (matches.length > 1) throw new Error(`Ambiguous hash prefix '#${prefix}' (matches in: ${matches.map((m) => m.groupName).join(', ')})`);
    return matches[0];
}

async function resolveEntry(session: ReplSession, groupName: string, hashRef: string): Promise<Resolved> {
    const resolved = await session.workspace.roots.resolveGroup(ref(groupName), { aliases: session.aliases });
    if (resolved.group === undefined) throw new Error('Group is not loaded');
    return {
        group: resolved.group as GroupObject,
        groupName: session.workspace.roots.get(resolved.id)?.name ?? groupName,
        entryHash: await session.workspace.roots.resolveHash(hashRefObj(hashRef), { kind: 'object', objectId: resolved.id }),
    };
}

function ref(text: string) {
    const span = { start: 0, end: text.length, line: 1, column: 1 };
    return text.startsWith('#')
        ? { kind: 'hash' as const, prefix: text.slice(1), span }
        : { kind: 'name' as const, text, parts: text.split('.'), span };
}

function hashRefObj(text: string) {
    return { kind: 'hash' as const, prefix: text.slice(1), span: { start: 0, end: text.length, line: 1, column: 1 } };
}

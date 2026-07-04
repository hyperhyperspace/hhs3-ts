import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { dumpGroupEntry, type LoggableObject } from "@hyper-hyper-space/hhs3_rdb_lang";
import type { RTableGroup } from "@hyper-hyper-space/hhs3_rdb";

import { createDumpRenderOptions } from "./alias_context.js";
import type { WorkspaceSession } from "../session/session.js";
import type { RootResolveContext } from "../workspace/root_index.js";

type GroupObject = RTableGroup & LoggableObject;

export type ResolvedGroupEntry = {
    group: GroupObject;
    groupName: string;
    entryHash: B64Hash;
};

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

function parseDumpOpArgs(args: string[]): { groupName?: string; hashRef: string } {
    if (args.length === 1 && args[0].startsWith('#')) {
        return { hashRef: args[0] };
    }
    if (args.length >= 2 && args[1].startsWith('#')) {
        return { groupName: args[0], hashRef: args[1] };
    }
    throw new Error('Usage: \\dump op [group] #hash');
}

async function entryMatchesInGroup(group: GroupObject, prefix: string): Promise<B64Hash[]> {
    const dag = await group.getScopedDag();
    const matches: B64Hash[] = [];
    for await (const entry of dag.loadAllEntries()) {
        if (entry.hash.startsWith(prefix)) matches.push(entry.hash);
    }
    return matches;
}

async function findGroupEntryByPrefix(
    session: WorkspaceSession,
    prefix: string,
): Promise<ResolvedGroupEntry> {
    const ordered: string[] = [];
    if (session.currentGroup !== undefined) ordered.push(session.currentGroup);
    for (const root of session.workspace.roots.list('group')) {
        if (root.id === session.currentGroup) continue;
        const name = root.name ?? root.id;
        if (!ordered.includes(name)) ordered.push(name);
    }

    const matches: ResolvedGroupEntry[] = [];
    const seenGroupIds = new Set<B64Hash>();
    for (const groupName of ordered) {
        const resolved = await session.workspace.roots.resolveGroup(ref(groupName), rootCtx(session));
        if (resolved.group === undefined || seenGroupIds.has(resolved.id)) continue;
        const group = resolved.group as GroupObject;
        const entryHashes = await entryMatchesInGroup(group, prefix);
        if (entryHashes.length > 1) {
            throw new Error(`Ambiguous hash prefix '#${prefix}' in group '${groupName}'`);
        }
        if (entryHashes.length === 1) {
            seenGroupIds.add(resolved.id);
            matches.push({
                group,
                groupName: session.workspace.roots.get(resolved.id)?.name ?? groupName,
                entryHash: entryHashes[0],
            });
        }
    }

    if (matches.length === 0) throw new Error(`Unknown hash prefix '#${prefix}'`);
    if (matches.length > 1) {
        const groups = matches.map((m) => m.groupName).join(', ');
        throw new Error(`Ambiguous hash prefix '#${prefix}' (matches in: ${groups})`);
    }
    return matches[0];
}

async function resolveExplicitGroupEntry(
    session: WorkspaceSession,
    groupName: string,
    hashRef: string,
): Promise<ResolvedGroupEntry> {
    const resolved = await session.workspace.roots.resolveGroup(ref(groupName), rootCtx(session));
    if (resolved.group === undefined) throw new Error('Group is not loaded');
    const entryHash = await session.workspace.roots.resolveHash(
        { kind: 'hash', prefix: hashRef.slice(1), span: { start: 0, end: hashRef.length, line: 1, column: 1 } },
        { kind: 'object', objectId: resolved.id },
    );
    return {
        group: resolved.group as GroupObject,
        groupName: session.workspace.roots.get(resolved.id)?.name ?? groupName,
        entryHash,
    };
}

export async function runDumpOpCommand(session: WorkspaceSession, args: string[]): Promise<string> {
    const { groupName, hashRef } = parseDumpOpArgs(args);
    const prefix = hashRef.slice(1);

    const resolved = groupName === undefined
        ? await findGroupEntryByPrefix(session, prefix)
        : await resolveExplicitGroupEntry(session, groupName, hashRef);

    const rendered = await dumpGroupEntry(resolved.group, resolved.entryHash, {
        render: createDumpRenderOptions(session),
    });
    if (rendered === undefined) throw new Error(`Op '${hashRef}' is not in group`);
    return rendered;
}

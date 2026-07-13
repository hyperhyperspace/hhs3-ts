import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import {
    AliasTable,
    isAliasScope,
    resolveAliasTarget,
    aliasLabel,
    rootNameForVersionHash,
    collectVersionOpHashes,
    type AliasScope,
    type AliasEntry,
    type AliasTarget,
} from "@hyper-hyper-space/hhs3_rdb_runtime";

import { formatDisplayString, formatSessionRows } from "../format/display.js";
import type { RootIndex } from "@hyper-hyper-space/hhs3_rdb_runtime";
import type { WorkspaceSession } from "./session.js";

export {
    AliasTable,
    isAliasScope,
    resolveAliasTarget,
    aliasLabel,
    rootNameForVersionHash,
    collectVersionOpHashes,
    type AliasScope,
    type AliasEntry,
    type AliasTarget,
};

export async function formatAliasResult(
    scope: AliasScope,
    name: string,
    hash: B64Hash,
    session: WorkspaceSession,
): Promise<string> {
    const label = await aliasLabel(scope, hash, session);
    const suffix = label === undefined ? '' : ` (${label})`;
    const rendered = formatDisplayString(session, hash, { role: 'hash' });
    return `${scope} ${name} => ${rendered}${suffix}`;
}

export async function formatAliasListing(session: WorkspaceSession, scope?: AliasScope): Promise<string> {
    const rows = await Promise.all(session.aliases.list(scope).map(async (entry) => ({
        scope: entry.scope,
        name: entry.name,
        hash: entry.hash,
        label: (await aliasLabel(entry.scope, entry.hash, session)) ?? '',
    })));
    return formatSessionRows(session, rows, undefined, { structuralColumns: new Set(['hash']) });
}

export type { RootIndex };

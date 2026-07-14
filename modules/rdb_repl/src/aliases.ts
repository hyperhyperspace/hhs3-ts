import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import {
    AliasTable,
    aliasLabel,
    collectVersionOpHashes,
    isAliasScope,
    resolveAliasTarget,
    rootNameForVersionHash,
    type AliasEntry,
    type AliasScope,
    type AliasTarget,
    type RootIndex,
} from "@hyper-hyper-space/hhs3_rdb_runtime";
import { formatDisplayString, formatSessionRows } from "./format/display.js";
import type { ReplSession } from "./session.js";

export {
    AliasTable,
    aliasLabel,
    collectVersionOpHashes,
    isAliasScope,
    resolveAliasTarget,
    rootNameForVersionHash,
    type AliasEntry,
    type AliasScope,
    type AliasTarget,
    type RootIndex,
};

export async function formatAliasResult(
    scope: AliasScope,
    name: string,
    hash: B64Hash,
    session: ReplSession,
): Promise<string> {
    const label = await aliasLabel(scope, hash, session);
    const suffix = label === undefined ? '' : ` (${label})`;
    return `${scope} ${name} => ${formatDisplayString(session, hash, { role: 'hash' })}${suffix}`;
}

export async function formatAliasListing(session: ReplSession, scope?: AliasScope): Promise<string> {
    const rows = await Promise.all(session.aliases.list(scope).map(async (entry) => ({
        scope: entry.scope,
        name: entry.name,
        hash: entry.hash,
        label: (await aliasLabel(entry.scope, entry.hash, session)) ?? '',
    })));
    return formatSessionRows(session, rows, undefined, { structuralColumns: new Set(['hash']) });
}

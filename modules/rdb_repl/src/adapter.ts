import type { CatalogUpdateResult } from "@hyper-hyper-space/hhs3_rdb";
import type { LangExecutionResult } from "@hyper-hyper-space/hhs3_rdb_lang";
import {
    createBindContext,
    executeText,
    keyPassphraseRequiredFromError,
    LanguageError,
    resolveFkRowId,
    resolveRowIdPrefix,
    type AuthInteractionContext,
    type ScriptRunResult as RuntimeScriptRunResult,
} from "@hyper-hyper-space/hhs3_rdb_runtime";
import { renderStatementMain } from "./format/table.js";
import type { ReplSession } from "./session.js";

export type StatementRunResult = {
    result: LangExecutionResult;
    notices?: string[];
    mainStreamed?: boolean;
    noticesStreamed?: boolean;
};
export type ScriptRunResult = { results: StatementRunResult[] };
export type RunLanguageTextOptions = AuthInteractionContext;

export {
    createBindContext,
    LanguageError,
    keyPassphraseRequiredFromError,
    resolveRowIdPrefix,
    resolveFkRowId,
};

export async function runLanguageText(
    session: ReplSession,
    text: string,
    options?: RunLanguageTextOptions,
): Promise<ScriptRunResult> {
    const runtimeOptions = options?.onProgress === undefined
        ? options
        : { ...options, onProgress: undefined };
    const run = await executeText(session, text, runtimeOptions);
    return mapRuntimeResults(session, run, options);
}

// What a catalog deploy did to the database's groups (CREATE DATABASE and
// UPDATE CATALOG).
function deployNotices(session: ReplSession, update: CatalogUpdateResult | undefined): string[] {
    if (update === undefined) return [];
    const name = (id: string) => session.workspace.roots.get(id)?.name ?? id.slice(0, 8);
    const notices: string[] = [];
    if (update.created.length > 0) notices.push(`created groups ${update.created.map(name).join(', ')}`);
    for (const deployed of update.deployed) notices.push(`deployed ${deployed.name}`);
    for (const failure of update.observeFailures) {
        notices.push(`ref update on ${name(failure.groupId)} (${failure.binding}) failed: ${failure.message}`);
    }
    return notices;
}

function mapRuntimeResults(
    session: ReplSession,
    run: RuntimeScriptRunResult,
    options?: RunLanguageTextOptions,
): ScriptRunResult {
    return {
        results: run.results.map((item) => {
            const notices = [
                ...deployNotices(session, item.deploy ?? (item.result.kind === 'update-catalog' ? item.result.update : undefined)),
                ...(item.refUpdates?.map((event) => event.message) ?? []),
            ];
            const mapped: StatementRunResult = {
                result: item.result,
                ...(notices.length > 0 ? { notices } : {}),
            };
            if (options?.onProgress !== undefined && session.outputMode !== 'json') {
                const main = renderStatementMain(session, mapped);
                if (main.length > 0) {
                    options.onProgress(main);
                    mapped.mainStreamed = true;
                }
                for (const notice of mapped.notices ?? []) {
                    options.onProgress(notice);
                    mapped.noticesStreamed = true;
                }
            }
            return mapped;
        }),
    };
}

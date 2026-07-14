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

function mapRuntimeResults(
    session: ReplSession,
    run: RuntimeScriptRunResult,
    options?: RunLanguageTextOptions,
): ScriptRunResult {
    return {
        results: run.results.map((item) => {
            const mapped: StatementRunResult = {
                result: item.result,
                notices: item.events?.map((event) => event.message),
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

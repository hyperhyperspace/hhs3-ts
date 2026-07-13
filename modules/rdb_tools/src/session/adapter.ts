import type { LangExecutionResult } from "@hyper-hyper-space/hhs3_rdb_lang";
import {
    createBindContext,
    executeText,
    keyPassphraseRequiredFromError,
    LanguageError,
    resolveFkRowId,
    resolveRowIdPrefix,
    type ScriptRunResult as RuntimeScriptRunResult,
} from "@hyper-hyper-space/hhs3_rdb_runtime";

import { renderStatementMain } from "../format/table.js";
import { toAuthInteractionContext } from "./auth_bridge.js";
import type { ReplAuthContext } from "./authz_suggest.js";
import type { WorkspaceSession } from "./session.js";

export type StatementRunResult = {
    result: LangExecutionResult;
    notices?: string[];
    mainStreamed?: boolean;
};

export type ScriptRunResult = {
    results: StatementRunResult[];
};

export type RunLanguageTextOptions = ReplAuthContext;

export {
    createBindContext,
    LanguageError,
    keyPassphraseRequiredFromError,
    resolveRowIdPrefix,
    resolveFkRowId,
};

export async function runLanguageText(
    session: WorkspaceSession,
    text: string,
    options?: RunLanguageTextOptions,
): Promise<ScriptRunResult> {
    const run = await executeText(session, text, toAuthInteractionContext(session, options));
    return mapRuntimeResults(session, run, options);
}

function mapRuntimeResults(
    session: WorkspaceSession,
    run: RuntimeScriptRunResult,
    options?: RunLanguageTextOptions,
): ScriptRunResult {
    const results: StatementRunResult[] = [];
    for (const item of run.results) {
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
        }
        results.push(mapped);
    }
    return { results };
}

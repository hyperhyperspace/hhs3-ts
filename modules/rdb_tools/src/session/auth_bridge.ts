import type { Interface } from "node:readline/promises";
import type { AuthInteractionContext } from "@hyper-hyper-space/hhs3_rdb_runtime";

import { confirmRefUpdateUnlock, confirmSignRetry, fulfillKeyPassphrase } from "../repl/passphrase.js";
import { canPromptForKeys } from "../repl/prompt_tty.js";
import type { ReplAuthContext } from "./authz_suggest.js";
import type { WorkspaceSession } from "./session.js";

export function toAuthInteractionContext(
    session: WorkspaceSession,
    options?: ReplAuthContext,
): AuthInteractionContext | undefined {
    const rl = options?.rl;
    if (!canPromptForKeys(session) && rl === undefined) {
        return options?.onProgress === undefined ? undefined : { onProgress: options.onProgress };
    }

    return {
        canPrompt: () => canPromptForKeys(session) || rl !== undefined,
        onProgress: options?.onProgress,
        confirmSignRetry: rl === undefined
            ? undefined
            : (authorLabel, op) => confirmSignRetry(rl, authorLabel, op),
        confirmRefUpdateUnlock: rl === undefined
            ? undefined
            : (observerGroup, authorLabel) => confirmRefUpdateUnlock(rl, observerGroup, authorLabel),
        unlockIdentity: rl === undefined
            ? undefined
            : async (label) => {
                await fulfillKeyPassphrase(session, { kind: 'unlock', label }, rl);
                return session.resolveIdentity(label);
            },
    };
}

export function authContextWithRl(session: WorkspaceSession, rl: Interface): AuthInteractionContext {
    return toAuthInteractionContext(session, { rl })!;
}

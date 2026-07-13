export {
    suggestAuthorsForFailure,
    suggestAuthorsForBindFailure,
    scanKeystore,
    evaluateObserveGateKey,
    labelForKeyId,
    formatAuthorHint,
    isBindAuthorRequiredFailure,
    isBindAuthorRetryStatement,
    hasExplicitByAst,
    isAuthRetryBound,
    hasExplicitBy,
    boundWithAuthor,
    resolveAuthorForBoundFailure,
    resolveAuthorsForAlterSchema,
    resolveAuthorsForAddMember,
    resolveAuthorForGate,
    resolveObserveAuthor,
    evaluateRowRestrictionKey,
    evaluateCanDeployKey,
    type AuthRetryBound,
    type AuthorCandidate,
    type AuthorResolution,
    type ResolveAuthorOptions,
} from "@hyper-hyper-space/hhs3_rdb_runtime";

import type { Interface } from "node:readline/promises";

export type ReplAuthContext = {
    rl?: Interface;
    onProgress?: (line: string) => void;
};

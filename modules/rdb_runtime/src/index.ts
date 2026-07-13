export { RdbRuntime, openMemWorkspace, type RdbRuntimeOptions } from "./runtime.js";

export type { DagBackend, DagEntry, Replica } from "@hyper-hyper-space/hhs3_replica";
export { MemDagBackend } from "@hyper-hyper-space/hhs3_replica";

export { registerRdbTypes } from "./workspace/register_types.js";
export {
    readRootPayloads,
    rehydrateRoots,
    payloadName,
    dagEntryFromRoot,
    type RehydratedRoot,
} from "./workspace/rehydrate.js";
export {
    RootIndex,
    kindFromType,
    type RootKind,
    type RootRecord,
    type RootResolveContext,
    type AliasLookup,
} from "./workspace/root_index.js";
export {
    RdbWorkspace,
    openMemWorkspace as openMemRdbWorkspace,
    type RdbWorkspaceOptions,
    type WorkspaceCloseable,
    type MemWorkspaceOptions,
} from "./workspace/workspace.js";

export {
    RdbSession,
    KeyPassphraseRequiredError,
    type RefAutoUpdateMode,
    type SessionView,
    type RdbSessionOptions,
} from "./session/session.js";
export {
    AliasTable,
    resolveAliasTarget,
    isAliasScope,
    aliasLabel,
    rootNameForVersionHash,
    collectVersionOpHashes,
    type AliasScope,
    type AliasEntry,
    type AliasTarget,
    type AliasSession,
} from "./session/aliases.js";
export {
    createBindContext,
    executeText,
    LanguageError,
    resolveRowIdPrefix,
    resolveFkRowId,
    keyPassphraseRequiredFromError,
    type StatementRunResult,
    type ScriptRunResult,
    type ExecuteTextOptions,
} from "./session/adapter.js";
export {
    resolveVersionRef,
    resolveVersionMember,
    assertHashInScopedDag,
    frontierForScope,
    hashScopeForVersionScope,
} from "./session/version.js";
export {
    propagateRefUpdates,
    extractRefUpdateTrigger,
    findObservers,
    formatRefAutoUpdateNotice,
    formatRefAutoUpdateFailure,
    formatRefAutoUpdateSkipped,
    RefAutoUpdateSkippedError,
    type RefUpdateEvent,
    type RefUpdateTrigger,
    type ObserverRef,
} from "./session/ref_auto_update.js";
export {
    suggestAuthorsForFailure,
    suggestAuthorsForBindFailure,
    scanKeystore,
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
    evaluateObserveGateKey,
    evaluateRowRestrictionKey,
    evaluateCanDeployKey,
    type AuthRetryBound,
    type AuthorCandidate,
    type AuthorResolution,
    type ResolveAuthorOptions,
} from "./session/authz_suggest.js";
export {
    tryAuthSignRetry,
    tryBindAuthorRetry,
    bindContextWithAuthor,
    type AuthSignRetryResult,
} from "./session/sign_retry.js";
export {
    AuthInteractionContext,
    KeyUnlockDeclinedError,
    canPromptForKeys,
} from "./session/prompts.js";
export { rootCtx, nameOrHashRef } from "./session/root_context.js";

export type { KeyVault, KeyRecord } from "./keys/key_vault.js";
export {
    encodePublicKey,
    decodePublicKey,
    encodeIdentitySecret,
    decodeIdentitySecret,
    bytesToBase64,
    base64ToBytes,
    type StoredPublicKey,
    type StoredIdentitySecret,
} from "./keys/identity.js";

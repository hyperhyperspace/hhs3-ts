import {
    RdbSession,
    type KeyVault,
    type RdbSessionOptions,
    type RdbWorkspace,
    type RefAutoUpdateMode,
    type SessionView,
} from "@hyper-hyper-space/hhs3_rdb_runtime";

import type { IssueReporter } from "@hyper-hyper-space/hhs3_mesh";

import type { ProjectionTargetFactory, ProjectSessionEntry } from "./projection/types.js";
import type { SyncMeshFactory, SyncSessionEntry } from "./sync/types.js";

export type OutputMode = 'table' | 'json' | 'vertical';
export type HashWidth = 'auto' | 'full' | number;

// Host-injected sink for asynchronous projection notices (reactive sync throws
// and ingest-rejection warnings). Kept as a plain string callback so the core
// repl never touches process/console/DOM: rdb_tools writes to stderr, the web
// client forwards to the transcript. Absent => reactive notices are dropped.
export type ProjectionErrorHandler = (message: string) => void;

export { type RefAutoUpdateMode, type SessionView };
export { KeyPassphraseRequiredError } from "@hyper-hyper-space/hhs3_rdb_runtime";

export type ReplSessionOptions = {
    workspace: RdbWorkspace;
    keyVault?: KeyVault;
    outputMode?: OutputMode;
    hashWidth?: HashWidth;
    hashLabels?: boolean;
    refAutoUpdate?: RefAutoUpdateMode;
    promptForKeys?: boolean;
    stopOnError?: boolean;
    createUuid?: () => string;
    projectionTargetFactory?: ProjectionTargetFactory;
    onProjectionError?: ProjectionErrorHandler;
    syncMeshFactory?: SyncMeshFactory;
    report?: IssueReporter;
};

export class ReplSession extends RdbSession {
    outputMode: OutputMode;
    hashWidth: HashWidth;
    hashLabels: boolean;
    promptForKeys: boolean;
    stopOnError: boolean;

    // Host-injected projection backend factory + the active \\project sessions,
    // keyed by a session-global incrementing id that is never reused after stop.
    projectionTargetFactory?: ProjectionTargetFactory;
    onProjectionError?: ProjectionErrorHandler;
    readonly projections = new Map<number, ProjectSessionEntry>();
    nextProjectId = 1;

    // Host-injected mesh factory + the active \\sync sessions, keyed by a
    // session-global incrementing id that is never reused after stop.
    syncMeshFactory?: SyncMeshFactory;
    readonly syncs = new Map<number, SyncSessionEntry>();
    nextSyncId = 1;
    nextFetchId = 1;

    // Host-injected structured issue sink, threaded into the sync mesh and the
    // RDb sync sessions. Absent => issues are dropped.
    report?: IssueReporter;

    constructor(options: ReplSessionOptions) {
        const sessionOptions: RdbSessionOptions = {
            workspace: options.workspace,
            keyVault: options.keyVault,
            refAutoUpdate: options.refAutoUpdate ?? 'off',
            createUuid: options.createUuid,
        };
        super(sessionOptions);
        this.outputMode = options.outputMode ?? 'table';
        this.hashWidth = options.hashWidth ?? 'auto';
        this.hashLabels = options.hashLabels ?? false;
        this.promptForKeys = options.promptForKeys ?? false;
        this.stopOnError = options.stopOnError ?? true;
        this.projectionTargetFactory = options.projectionTargetFactory;
        this.onProjectionError = options.onProjectionError;
        this.syncMeshFactory = options.syncMeshFactory;
        this.report = options.report;
    }

    get keystore(): KeyVault | undefined {
        return this.keyVault;
    }

    set keystore(store: KeyVault | undefined) {
        this.keyVault = store;
    }

    enableReplDefaults(): void {
        this.hashLabels = true;
        this.refAutoUpdate = 'auto';
    }

    enableScriptDefaults(): void {
        this.hashWidth = 'full';
        this.refAutoUpdate = 'off';
    }

    setOutputMode(mode: OutputMode): void {
        this.outputMode = mode;
    }

    setHashWidth(width: HashWidth): void {
        this.hashWidth = width;
    }

    setHashLabels(on: boolean): void {
        this.hashLabels = on;
    }

    setPromptForKeys(on: boolean): void {
        this.promptForKeys = on;
    }
}

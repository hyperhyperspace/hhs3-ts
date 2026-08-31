import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { BidirectionalTarget } from "@hyper-hyper-space/hhs3_rdb_adapter";
import type { RdbProjection } from "@hyper-hyper-space/hhs3_rdb_projection";
import {
    RdbSession,
    type KeyVault,
    type RdbSessionOptions,
    type RdbWorkspace,
    type RefAutoUpdateMode,
    type SessionView,
} from "@hyper-hyper-space/hhs3_rdb_runtime";

export type OutputMode = 'table' | 'json' | 'vertical';
export type HashWidth = 'auto' | 'full' | number;

// Host-injected factory for the relational projection backend. The core repl is
// browser-safe and engine-agnostic, so a host that wants `\projection` commands
// supplies the concrete BidirectionalTarget: rdb_tools opens a SQLite file,
// rdb_repl_web uses an in-memory target. Absent => projection commands report
// that no backend is configured.
export type ProjectionTargetFactory = (info: { databaseId: B64Hash; label?: string }) => Promise<BidirectionalTarget>;

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
};

export class ReplSession extends RdbSession {
    outputMode: OutputMode;
    hashWidth: HashWidth;
    hashLabels: boolean;
    promptForKeys: boolean;
    stopOnError: boolean;

    // Host-injected projection backend factory + the active projections, keyed
    // by database id (managed by the `\projection` meta commands).
    projectionTargetFactory?: ProjectionTargetFactory;
    onProjectionError?: ProjectionErrorHandler;
    readonly projections = new Map<B64Hash, RdbProjection>();

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

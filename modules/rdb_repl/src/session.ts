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
};

export class ReplSession extends RdbSession {
    outputMode: OutputMode;
    hashWidth: HashWidth;
    hashLabels: boolean;
    promptForKeys: boolean;
    stopOnError: boolean;

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

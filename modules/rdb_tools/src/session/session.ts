import {
    RdbSession,
    type RefAutoUpdateMode,
    type SessionView,
    type RdbSessionOptions,
} from "@hyper-hyper-space/hhs3_rdb_runtime";

import type { KeyStore } from "../keys/keystore.js";
import type { Workspace } from "../workspace/workspace.js";

export type OutputMode = 'table' | 'json' | 'vertical';
export type HashWidth = 'auto' | 'full' | number;

export { type RefAutoUpdateMode, type SessionView };
export { KeyPassphraseRequiredError } from "@hyper-hyper-space/hhs3_rdb_runtime";

export type WorkspaceSessionOptions = {
    workspace: Workspace;
    keystore?: KeyStore;
    outputMode?: OutputMode;
    hashWidth?: HashWidth;
    hashLabels?: boolean;
    refAutoUpdate?: RefAutoUpdateMode;
    promptForKeys?: boolean;
};

export class WorkspaceSession extends RdbSession {
    declare readonly workspace: Workspace;
    outputMode: OutputMode;
    hashWidth: HashWidth;
    hashLabels: boolean;
    promptForKeys: boolean;
    stopOnError = true;

    constructor(options: WorkspaceSessionOptions) {
        const sessionOptions: RdbSessionOptions = {
            workspace: options.workspace as unknown as RdbSessionOptions['workspace'],
            keyVault: options.keystore,
            refAutoUpdate: options.refAutoUpdate ?? parseRefAutoUpdateEnv() ?? 'off',
        };
        super(sessionOptions);
        (this as { workspace: Workspace }).workspace = options.workspace;
        this.outputMode = options.outputMode ?? 'table';
        this.hashWidth = options.hashWidth ?? parseHashWidthEnv() ?? 'auto';
        this.hashLabels = options.hashLabels ?? parseHashLabelsEnv() ?? false;
        this.promptForKeys = options.promptForKeys ?? parsePromptForKeysEnv() ?? false;
    }

    get keystore(): KeyStore | undefined {
        return this.keyVault as KeyStore | undefined;
    }

    set keystore(store: KeyStore | undefined) {
        this.keyVault = store;
    }

    enableReplDefaults(): void {
        if (parseHashLabelsEnv() === undefined) this.hashLabels = true;
        if (parseRefAutoUpdateEnv() === undefined) this.refAutoUpdate = 'auto';
    }

    enableScriptDefaults(): void {
        if (parseHashWidthEnv() === undefined) this.hashWidth = 'full';
        if (parseRefAutoUpdateEnv() === undefined) this.refAutoUpdate = 'off';
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

function parseHashWidthEnv(): HashWidth | undefined {
    const raw = process.env.RDB_HASH_WIDTH?.trim();
    if (raw === undefined || raw.length === 0) return undefined;
    if (raw === 'auto') return 'auto';
    if (raw === 'full') return 'full';
    const n = Number(raw);
    if (Number.isInteger(n) && n > 0) return n;
    throw new Error(`Invalid RDB_HASH_WIDTH '${raw}' (expected auto, full, or a positive integer)`);
}

function parseHashLabelsEnv(): boolean | undefined {
    const raw = process.env.RDB_HASH_LABELS?.trim().toLowerCase();
    if (raw === undefined || raw.length === 0) return undefined;
    if (raw === 'on' || raw === 'true' || raw === '1') return true;
    if (raw === 'off' || raw === 'false' || raw === '0') return false;
    throw new Error(`Invalid RDB_HASH_LABELS '${raw}' (expected on or off)`);
}

function parseRefAutoUpdateEnv(): RefAutoUpdateMode | undefined {
    const raw = process.env.RDB_REF_AUTO_UPDATE?.trim().toLowerCase();
    if (raw === undefined || raw.length === 0) return undefined;
    if (raw === 'auto' || raw === 'on' || raw === 'true' || raw === '1') return 'auto';
    if (raw === 'self') return 'self';
    if (raw === 'off' || raw === 'false' || raw === '0') return 'off';
    throw new Error(`Invalid RDB_REF_AUTO_UPDATE '${raw}' (expected auto, self, or off)`);
}

function parsePromptForKeysEnv(): boolean | undefined {
    const raw = process.env.RDB_PROMPT_KEYS?.trim().toLowerCase();
    if (raw === undefined || raw.length === 0) return undefined;
    if (raw === 'on' || raw === 'true' || raw === '1') return true;
    if (raw === 'off' || raw === 'false' || raw === '0') return false;
    throw new Error(`Invalid RDB_PROMPT_KEYS '${raw}' (expected on or off)`);
}

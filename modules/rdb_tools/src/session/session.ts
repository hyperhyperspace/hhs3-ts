import {
    type RefAutoUpdateMode,
    type SessionView,
} from "@hyper-hyper-space/hhs3_rdb_runtime";
import {
    ReplSession,
    type HashWidth,
    type OutputMode,
    type ReplSessionOptions,
} from "@hyper-hyper-space/hhs3_rdb_repl";

import type { KeyStore } from "../keys/keystore.js";
import type { Workspace } from "../workspace/workspace.js";

export { ReplSession, type OutputMode, type HashWidth };

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

export class WorkspaceSession extends ReplSession {
    declare readonly workspace: Workspace;

    constructor(options: WorkspaceSessionOptions) {
        const sessionOptions: ReplSessionOptions = {
            workspace: options.workspace as unknown as ReplSessionOptions['workspace'],
            keyVault: options.keystore,
            refAutoUpdate: options.refAutoUpdate ?? parseRefAutoUpdateEnv() ?? 'off',
            outputMode: options.outputMode,
            hashWidth: options.hashWidth ?? parseHashWidthEnv(),
            hashLabels: options.hashLabels ?? parseHashLabelsEnv(),
            promptForKeys: options.promptForKeys ?? parsePromptForKeysEnv(),
        };
        super(sessionOptions);
        (this as { workspace: Workspace }).workspace = options.workspace;
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

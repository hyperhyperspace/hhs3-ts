import type { DagBackend } from "@hyper-hyper-space/hhs3_replica";

import type { KeyVault } from "./keys/key_vault.js";
import { executeText, type ExecuteTextOptions, type ScriptRunResult } from "./session/adapter.js";
import type { RefAutoUpdateMode, RdbSession, RdbSessionOptions } from "./session/session.js";
import { RdbSession as RdbSessionClass } from "./session/session.js";
import { type RdbWorkspaceOptions, RdbWorkspace, openMemWorkspace } from "./workspace/workspace.js";

export type RdbRuntimeOptions = RdbWorkspaceOptions & {
    keyVault?: KeyVault;
    refAutoUpdate?: RefAutoUpdateMode;
    createUuid?: () => string;
};

export class RdbRuntime {
    readonly workspace: RdbWorkspace;
    readonly session: RdbSession;

    private constructor(workspace: RdbWorkspace, session: RdbSession) {
        this.workspace = workspace;
        this.session = session;
    }

    static async open(options: RdbRuntimeOptions): Promise<RdbRuntime> {
        const workspace = await RdbWorkspace.open(options);
        const session = new RdbSessionClass({
            workspace,
            keyVault: options.keyVault,
            refAutoUpdate: options.refAutoUpdate,
            createUuid: options.createUuid,
        });
        return new RdbRuntime(workspace, session);
    }

    static async openMemory(opts?: {
        backendLabel?: string;
        keyVault?: KeyVault;
        refAutoUpdate?: RefAutoUpdateMode;
        createUuid?: () => string;
    }): Promise<RdbRuntime> {
        const workspace = await openMemWorkspace({
            backendLabel: opts?.backendLabel,
        });
        const session = new RdbSessionClass({
            workspace,
            keyVault: opts?.keyVault,
            refAutoUpdate: opts?.refAutoUpdate,
            createUuid: opts?.createUuid,
        });
        return new RdbRuntime(workspace, session);
    }

    async execute(text: string, options?: ExecuteTextOptions): Promise<ScriptRunResult> {
        return executeText(this.session, text, options);
    }

    async close(): Promise<void> {
        await this.workspace.close();
    }
}

export { openMemWorkspace };

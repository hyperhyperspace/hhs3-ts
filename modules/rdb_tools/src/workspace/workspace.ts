import { HASH_SHA256, createBasicCrypto } from "@hyper-hyper-space/hhs3_crypto";
import { Payload, RObject } from "@hyper-hyper-space/hhs3_mvt";
import type { CreatePlan } from "@hyper-hyper-space/hhs3_rdb_lang";
import {
    RdbWorkspace,
    payloadName,
} from "@hyper-hyper-space/hhs3_rdb_runtime";

import { SqliteReplicaDagBackend } from "./backend.js";

export type WorkspaceOpenOptions = {
    path: string;
    backendLabel?: string;
};

export class Workspace {
    readonly path: string;
    readonly backendLabel: string;
    readonly replica: RdbWorkspace['replica'];
    readonly backend: SqliteReplicaDagBackend;
    readonly roots: RdbWorkspace['roots'];

    private readonly inner: RdbWorkspace;

    private constructor(path: string, inner: RdbWorkspace, backend: SqliteReplicaDagBackend) {
        this.path = path;
        this.inner = inner;
        this.backendLabel = inner.backendLabel;
        this.replica = inner.replica;
        this.backend = backend;
        this.roots = inner.roots;
    }

    static async open(options: WorkspaceOpenOptions): Promise<Workspace> {
        const crypto = createBasicCrypto();
        const hashSuite = crypto.hash(HASH_SHA256);
        const backend = await SqliteReplicaDagBackend.open({ path: options.path, hashSuite });
        const inner = await RdbWorkspace.open({ backend, backendLabel: options.backendLabel ?? 'default', hashSuite });
        return new Workspace(options.path, inner, backend);
    }

    async createRoot(plan: CreatePlan): Promise<RObject> {
        return this.inner.createRoot(plan);
    }

    async close(): Promise<void> {
        await this.inner.close();
    }
}

export { payloadName };

import { B64Hash, createBasicCrypto, HASH_SHA256, type BasicCrypto, type HashSuite } from "@hyper-hyper-space/hhs3_crypto";
import { Payload, RObject, type RObjectConfig } from "@hyper-hyper-space/hhs3_mvt";
import type { CreatePlan } from "@hyper-hyper-space/hhs3_rdb_lang";
import type { DagBackend, Replica } from "@hyper-hyper-space/hhs3_replica";
import { MemDagBackend, Replica as ReplicaClass } from "@hyper-hyper-space/hhs3_replica";

import { payloadName, rehydrateRoots } from "./rehydrate.js";
import { registerRdbTypes } from "./register_types.js";
import { RootIndex } from "./root_index.js";

export type WorkspaceCloseable = {
    close?(): void | Promise<void>;
};

export type RdbWorkspaceOptions = {
    backend: DagBackend;
    backendLabel?: string;
    crypto?: BasicCrypto;
    hashSuite?: HashSuite;
    replicaConfig?: RObjectConfig;
    rehydrate?: boolean;
    roots?: RootIndex;
};

export class RdbWorkspace {
    readonly backendLabel: string;
    readonly replica: Replica;
    readonly backend: DagBackend;
    readonly roots: RootIndex;

    private constructor(
        backendLabel: string,
        replica: Replica,
        backend: DagBackend,
        roots: RootIndex,
    ) {
        this.backendLabel = backendLabel;
        this.replica = replica;
        this.backend = backend;
        this.roots = roots;
    }

    static async open(options: RdbWorkspaceOptions): Promise<RdbWorkspace> {
        const crypto = options.crypto ?? createBasicCrypto();
        const hashSuite = options.hashSuite ?? crypto.hash(HASH_SHA256);
        const backendLabel = options.backendLabel ?? 'default';
        const replica = new ReplicaClass({
            crypto,
            hashSuite,
            config: options.replicaConfig ?? { selfValidate: true },
        });
        replica.attachBackend(backendLabel, options.backend);
        registerRdbTypes(replica);

        const roots = options.roots ?? new RootIndex();
        const workspace = new RdbWorkspace(backendLabel, replica, options.backend, roots);
        if (options.rehydrate !== false) {
            await rehydrateRoots(replica, options.backend, roots);
        }
        return workspace;
    }

    async createRoot(plan: CreatePlan): Promise<RObject> {
        const object = await this.replica.createObject(plan.payload as Payload, this.backendLabel);
        this.roots.registerObject(object.getId(), object, plan.name ?? payloadName(plan.payload as Payload));
        return object;
    }

    async close(): Promise<void> {
        await this.replica.destroy();
        const closeable = this.backend as WorkspaceCloseable;
        if (closeable.close !== undefined) {
            await closeable.close();
        }
    }
}

export type MemWorkspaceOptions = {
    backendLabel?: string;
    replicaConfig?: RObjectConfig;
    rehydrate?: boolean;
};

export async function openMemWorkspace(opts?: MemWorkspaceOptions): Promise<RdbWorkspace> {
    const crypto = createBasicCrypto();
    const hashSuite = crypto.hash(HASH_SHA256);
    const backend = new MemDagBackend(hashSuite);
    return RdbWorkspace.open({
        backend,
        crypto,
        hashSuite,
        backendLabel: opts?.backendLabel,
        replicaConfig: opts?.replicaConfig,
        rehydrate: opts?.rehydrate,
    });
}

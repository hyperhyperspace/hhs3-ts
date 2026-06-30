import { B64Hash, createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { Payload, RObject } from "@hyper-hyper-space/hhs3_mvt";
import { rDbFactory, rSchemaFactory, rTableGroupFactory } from "@hyper-hyper-space/hhs3_rdb";
import type { CreatePlan } from "@hyper-hyper-space/hhs3_rdb_lang";
import { Replica } from "@hyper-hyper-space/hhs3_replica";
import { rSetFactory } from "@hyper-hyper-space/hhs3_std_types";

import { SqliteReplicaDagBackend } from "./backend.js";
import { payloadName, rehydrateRoots } from "./rehydrate.js";
import { RootIndex } from "./root_index.js";

export type WorkspaceOpenOptions = {
    path: string;
    backendLabel?: string;
};

export class Workspace {
    readonly path: string;
    readonly backendLabel: string;
    readonly replica: Replica;
    readonly backend: SqliteReplicaDagBackend;
    readonly roots: RootIndex;

    private constructor(path: string, backendLabel: string, replica: Replica, backend: SqliteReplicaDagBackend, roots: RootIndex) {
        this.path = path;
        this.backendLabel = backendLabel;
        this.replica = replica;
        this.backend = backend;
        this.roots = roots;
    }

    static async open(options: WorkspaceOpenOptions): Promise<Workspace> {
        const crypto = createBasicCrypto();
        const hashSuite = crypto.hash(HASH_SHA256);
        const backendLabel = options.backendLabel ?? 'default';
        const backend = await SqliteReplicaDagBackend.open({ path: options.path, hashSuite });
        const replica = new Replica({ crypto, hashSuite, config: { selfValidate: true } });
        replica.attachBackend(backendLabel, backend);
        registerTypes(replica);

        const roots = new RootIndex();
        const workspace = new Workspace(options.path, backendLabel, replica, backend, roots);
        await rehydrateRoots(replica, backend, roots);
        return workspace;
    }

    async createRoot(plan: CreatePlan): Promise<RObject> {
        const object = await this.replica.createObject(plan.payload as Payload, this.backendLabel);
        this.roots.registerObject(object.getId(), object, plan.name ?? payloadName(plan.payload as Payload));
        return object;
    }

    async close(): Promise<void> {
        await this.replica.destroy();
        this.backend.close();
    }
}

function registerTypes(replica: Replica): void {
    replica.registerType('hhs/rdb_v1', rDbFactory);
    replica.registerType('hhs/rschema_v1', rSchemaFactory);
    replica.registerType('hhs/rtable_group_v1', rTableGroupFactory);
    replica.registerType('hhs/rset_v1', rSetFactory);
}

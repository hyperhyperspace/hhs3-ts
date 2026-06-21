import { promises as fs } from "node:fs";

import { B64Hash, createBasicCrypto, HASH_SHA256 } from "@hyper-hyper-space/hhs3_crypto";
import { Payload, RObject } from "@hyper-hyper-space/hhs3_mvt";
import { rDbFactory, rSchemaFactory, rTableGroupFactory } from "@hyper-hyper-space/hhs3_rdb";
import type { CreatePlan } from "@hyper-hyper-space/hhs3_rdb_lang";
import { Replica } from "@hyper-hyper-space/hhs3_replica";
import { rSetFactory } from "@hyper-hyper-space/hhs3_std_types";

import { SqliteReplicaDagBackend } from "./backend.js";
import { payloadName, rehydrateRoots } from "./rehydrate.js";
import { RootIndex } from "./root_index.js";

type WorkspaceMetadata = {
    names?: { [id: string]: string };
    aliases?: { [name: string]: B64Hash };
};

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
        await workspace.loadMetadata();
        return workspace;
    }

    async createRoot(plan: CreatePlan): Promise<RObject> {
        const object = await this.replica.createObject(plan.payload as Payload, this.backendLabel);
        this.roots.registerObject(object.getId(), object, plan.name ?? payloadName(plan.payload as Payload));
        await this.saveMetadata();
        return object;
    }

    async close(): Promise<void> {
        await this.replica.destroy();
        this.backend.close();
    }

    async setRootName(id: B64Hash, name: string): Promise<void> {
        this.roots.setName(id, name);
        await this.saveMetadata();
    }

    async setAlias(name: string, id: B64Hash): Promise<void> {
        this.roots.setAlias(name, id);
        await this.saveMetadata();
    }

    private async loadMetadata(): Promise<void> {
        let raw: string;
        try {
            raw = await fs.readFile(this.metadataPath(), 'utf8');
        } catch (e) {
            if ((e as NodeJS.ErrnoException).code === 'ENOENT') return;
            throw e;
        }

        const metadata = JSON.parse(raw) as WorkspaceMetadata;
        for (const [id, name] of Object.entries(metadata.names ?? {})) {
            if (this.roots.get(id as B64Hash) !== undefined) this.roots.setName(id as B64Hash, name);
        }
        for (const [alias, id] of Object.entries(metadata.aliases ?? {})) {
            if (this.roots.get(id) !== undefined) this.roots.setAlias(alias, id);
        }
    }

    private async saveMetadata(): Promise<void> {
        await fs.writeFile(this.metadataPath(), JSON.stringify(this.roots.exportNames(), undefined, 2) + '\n');
    }

    private metadataPath(): string {
        return `${this.path}.rdbtools.json`;
    }
}

function registerTypes(replica: Replica): void {
    replica.registerType('hhs/rdb_v1', rDbFactory);
    replica.registerType('hhs/rschema_v1', rSchemaFactory);
    replica.registerType('hhs/rtable_group_v1', rTableGroupFactory);
    replica.registerType('hhs/rset_v1', rSetFactory);
}

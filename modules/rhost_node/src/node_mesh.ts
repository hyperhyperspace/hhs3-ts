// RDb adapter over the Node mesh factory. The request is a MeshConfig; identity
// and the issue reporter travel beside it. `folderRoot`, when set, pins folder
// discovery entries that omit a root (including the default list) so a test
// does not write into the user's mesh folder.

import type { MeshConfig, ScopedMeshConfig } from "@hyper-hyper-space/hhs3_mesh";
import { createNodeMesh } from "@hyper-hyper-space/hhs3_mesh_node";
import type { MeshRuntime, SyncMeshFactory } from "@hyper-hyper-space/hhs3_rhost";

export type NodeSyncMeshFactoryOptions = {
    folderRoot?: string;
};

export function createNodeSyncMeshFactory(opts: NodeSyncMeshFactoryOptions = {}): SyncMeshFactory {
    return async (config, runtime: MeshRuntime) => {
        return createNodeMesh(pinFolder(config, opts.folderRoot), runtime);
    };
}

function pinFolder(config: ScopedMeshConfig, root: string | undefined): ScopedMeshConfig {
    if (root === undefined) return config;
    const discovery = (config.discovery ?? [{ type: 'tracker' as const }, { type: 'folder' as const }]).map(source =>
        source.type === 'folder' && source.root === undefined ? { ...source, root } : source,
    );
    return { ...config, discovery };
}

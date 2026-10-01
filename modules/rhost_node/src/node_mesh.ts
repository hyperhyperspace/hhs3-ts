// RDb adapter over the generic Node mesh factory. Maps RDB_SYNC_* env vars and
// the request (a command's flags, or a host's sync settings) onto
// createNodeMesh, which owns all mesh composition (transports, discovery,
// listen policy). Precedence: request > env > scope default.

import { createNodeMesh } from "@hyper-hyper-space/hhs3_mesh_node";
import type { BuiltSyncMesh, SyncMeshFactory } from "@hyper-hyper-space/hhs3_rhost";

export type NodeSyncMeshFactoryOptions = {
    folderRoot?: string;
    env?: NodeJS.ProcessEnv;
};

export function createNodeSyncMeshFactory(opts: NodeSyncMeshFactoryOptions = {}): SyncMeshFactory {
    return async (req): Promise<BuiltSyncMesh> => {
        const env = opts.env ?? process.env;
        return createNodeMesh(
            {
                scope: req.scope,
                identity: req.identity,
                trackerAddress: req.trackerAddress ?? empty(env.RDB_SYNC_TRACKER),
                trackerKeyId: req.trackerKeyId ?? empty(env.RDB_SYNC_TRACKER_KEY),
                listenAddress: req.listenAddress ?? empty(env.RDB_SYNC_LISTEN),
                report: req.report,
            },
            { folderRoot: opts.folderRoot },
        );
    };
}

function empty(value: string | undefined): string | undefined {
    if (value === undefined || value.trim() === '') return undefined;
    return value;
}

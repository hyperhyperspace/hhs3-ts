import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64, type RContext } from "@hyper-hyper-space/hhs3_mvt";
import type { RDb } from "@hyper-hyper-space/hhs3_rdb";
import type { BidirectionalTarget, IndexReconcileReport, IndexSpec } from "@hyper-hyper-space/hhs3_rdb_adapter";
import {
    RdbProjection, type FilesMountOpener, type FilesMountSpec, type FilesReconcileReport, type RdbProjectionOptions,
} from "@hyper-hyper-space/hhs3_rdb_projection";

export type OpenProjectionOptions = Omit<RdbProjectionOptions, 'writer'> & {
    db: RDb;
    ctx: RContext;
    target: BidirectionalTarget;
    writer: OwnIdentity;
    // Intern the writer's key in the projection's key table, so the app can
    // find its own key id there.
    registerWriterKey?: boolean;
    indexSpec?: IndexSpec;
    files?: { mounts: FilesMountSpec[]; open: FilesMountOpener };
};

export type OpenedProjection = {
    projection: RdbProjection;
    indexReport?: IndexReconcileReport;
    filesReport?: FilesReconcileReport;
};

// Opens a projection of `db` into `target` that ingests local edits as
// `writer`, then mounts its file folders. If registering the key, reconciling
// the index spec or mounting fails, the projection is stopped before the
// error is rethrown.
export async function openProjection(options: OpenProjectionOptions): Promise<OpenedProjection> {
    const { db, ctx, target, writer, registerWriterKey, indexSpec, files, ...rest } = options;
    const projection = await RdbProjection.open(db, ctx, target, { ...rest, writer });
    try {
        if (registerWriterKey === true) {
            await projection.registerKey(writer.keyId, serializePublicKeyToBase64(writer.publicKey));
        }
        const indexReport = indexSpec === undefined ? undefined : await projection.reconcileIndexes(indexSpec);
        const filesReport = files === undefined || files.mounts.length === 0
            ? undefined
            : await projection.reconcileFiles(files.mounts, files.open);
        return {
            projection,
            ...(indexReport !== undefined ? { indexReport } : {}),
            ...(filesReport !== undefined ? { filesReport } : {}),
        };
    } catch (err) {
        await projection.stop();
        throw err;
    }
}

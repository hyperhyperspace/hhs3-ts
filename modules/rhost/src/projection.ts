import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";
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
    indexSpec?: IndexSpec;
    files?: { mounts: FilesMountSpec[]; open: FilesMountOpener };
};

export type OpenedProjection = {
    projection: RdbProjection;
    indexReport?: IndexReconcileReport;
    filesReport?: FilesReconcileReport;
};

// Opens a projection of `db` into `target` that ingests local edits as
// `writer`, then mounts its file folders. The projection interns the writer's
// key in its key table when it records the writer record, at open, so the app
// finds its own key id there. If reconciling the index spec or mounting fails,
// the projection is stopped before the error is rethrown.
export async function openProjection(options: OpenProjectionOptions): Promise<OpenedProjection> {
    const { db, ctx, target, writer, indexSpec, files, ...rest } = options;
    const projection = await RdbProjection.open(db, ctx, target, { ...rest, writer });
    try {
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

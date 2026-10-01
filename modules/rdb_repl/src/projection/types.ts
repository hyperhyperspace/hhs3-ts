import type { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { BidirectionalTarget } from "@hyper-hyper-space/hhs3_rdb_adapter";
import type { FileDirectory, FilesMountSpec, RdbProjection } from "@hyper-hyper-space/hhs3_rdb_projection";

// Host-injected factory for the relational projection backend. The core repl is
// browser-safe and engine-agnostic, so a host that wants `\project` commands
// supplies the concrete BidirectionalTarget: rdb_tools opens a SQLite file,
// rdb_repl_web uses an in-memory target (`to :memory:` only). Absent =>
// projection commands report that no backend is configured.
export type ProjectionTargetFactory = (info: {
    databaseId: B64Hash;
    path: string;
}) => Promise<BidirectionalTarget>;

// Host-injected folder factory for `\project files`: the folder a FILES member
// is mounted at, with `path` as the user typed it (rdb_tools resolves it
// against the working directory). Absent => `\project files` is refused.
export type FilesDirectoryFactory = (info: {
    databaseId: B64Hash;
    name: string;
    path: string;
}) => Promise<FileDirectory>;

export type ProjectSessionEntry = {
    id: number;
    dbId: B64Hash;
    dbName: string;
    path: string;
    identityLabel: string;
    identityKeyId: KeyId;
    projection: RdbProjection;
    // The folders mounted with `\project files`, opened once each.
    files: { spec: FilesMountSpec; dir: FileDirectory }[];
};

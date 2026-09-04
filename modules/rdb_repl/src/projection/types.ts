import type { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { BidirectionalTarget } from "@hyper-hyper-space/hhs3_rdb_adapter";
import type { RdbProjection } from "@hyper-hyper-space/hhs3_rdb_projection";

// Host-injected factory for the relational projection backend. The core repl is
// browser-safe and engine-agnostic, so a host that wants `\project` commands
// supplies the concrete BidirectionalTarget: rdb_tools opens a SQLite file,
// rdb_repl_web uses an in-memory target (`to :memory:` only). Absent =>
// projection commands report that no backend is configured.
export type ProjectionTargetFactory = (info: {
    databaseId: B64Hash;
    path: string;
}) => Promise<BidirectionalTarget>;

export type ProjectSessionEntry = {
    id: number;
    dbId: B64Hash;
    dbName: string;
    path: string;
    identityLabel: string;
    identityKeyId: KeyId;
    projection: RdbProjection;
};

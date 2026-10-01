import type { KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { AllowSource, DatabaseSync, SyncScope } from "@hyper-hyper-space/hhs3_rhost";

export type SyncSessionEntry = {
    id: number;
    dbId: string;
    dbName: string;
    identityLabel: string;
    identityKeyId: KeyId;
    scope: SyncScope;
    sources: AllowSource[];
    sync: DatabaseSync;
};

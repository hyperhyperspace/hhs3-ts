// Public RDeployGate interfaces.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RObject, Version } from "@hyper-hyper-space/hhs3_mvt";

export interface RDeployGate extends RObject {
    getGroupId(): B64Hash;
    getSchemaId(): B64Hash;

    // Admits `version` of the schema and everything below it: appends the
    // missing mirrors, oldest first. Returns the appended entry hashes.
    admit(version: Version): Promise<B64Hash[]>;

    // Whether every entry of `version` has been admitted.
    isAdmitted(version: Version): Promise<boolean>;

    // The schema entries at the admitted frontier (empty before any admission).
    getAdmittedFrontier(): Promise<Version>;

    // The gate entry mirroring a schema entry, when it has been admitted.
    mirrorOf(schemaEntry: B64Hash): Promise<B64Hash | undefined>;
}

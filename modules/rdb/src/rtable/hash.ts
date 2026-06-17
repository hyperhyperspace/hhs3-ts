// Row id derivation.
//
// rowId = hash(uuid, ownerId), or hash(uuid) for anonymous rows.
//
// The id space is partitioned by owner: the image of the derivation is
// disjoint across different owners, so an ownership claim is verifiable
// from the id itself given (uuid, owner) in the create payload, and forgery
// reduces to signature checking. Primary-key uniqueness holds by collision
// resistance, not enforcement (there are no unique constraints in Rdb).
//
// v1 allows at most ONE owner per row; shared write access is expressed
// through restrictions (e.g. exists over a caps table), not co-ownership.
// Multi-owner rows may return post-v1 without changing the scheme's shape.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId, sha256, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";

export function deriveRowId(uuid: string, owner?: KeyId): B64Hash {
    const canonical: json.Literal = owner === undefined
        ? { uuid }
        : { uuid, owner };

    return sha256.hashToB64(stringToUint8Array(json.toStringNormalized(canonical)));
}

export function checkRowId(rowId: B64Hash, uuid: string, owner?: KeyId): boolean {
    return deriveRowId(uuid, owner) === rowId;
}

// Table id derivation: tables have no creation op (they exist by schema), so
// their object ids are derived deterministically from (groupId, tableName).
export function deriveTableId(groupId: B64Hash, table: string): B64Hash {
    const canonical: json.Literal = { group: groupId, table };
    return sha256.hashToB64(stringToUint8Array(json.toStringNormalized(canonical)));
}

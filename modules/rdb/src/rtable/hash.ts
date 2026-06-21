// Row id derivation.
//
// rowId = hash(uuid, authorId), or hash(uuid) for unauthored rows.
// Authorship is implicit in the insert op; there is no separate owner/recipient
// payload field, but the authored id space remains partitioned by author key.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId, sha256, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";

export function deriveRowId(uuid: string, author?: KeyId): B64Hash {
    const canonical: json.Literal = author === undefined
        ? { uuid }
        : { uuid, author };
    return sha256.hashToB64(stringToUint8Array(json.toStringNormalized(canonical)));
}

export function checkRowId(rowId: B64Hash, uuid: string, author?: KeyId): boolean {
    return deriveRowId(uuid, author) === rowId;
}

// Table id derivation: tables have no creation op (they exist by schema), so
// their object ids are derived deterministically from (groupId, tableName).
export function deriveTableId(groupId: B64Hash, table: string): B64Hash {
    const canonical: json.Literal = { group: groupId, table };
    return sha256.hashToB64(stringToUint8Array(json.toStringNormalized(canonical)));
}

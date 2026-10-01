import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { json } from "@hyper-hyper-space/hhs3_json";

import type { TableDef } from "../src/rschema/payload.js";
import type { InsertRowPayload } from "../src/rtable/payload.js";
import { IDENTITIES_TABLE, identityRow, usersSchemaTables } from "../src/users/users.js";

// A local identity provider for test groups whose authored ops must verify:
// add `identitiesTableDef()` to the schema, then spread
// `localIdentityProvider(...)` into the group create.
export function identitiesTableDef(): TableDef {
    return usersSchemaTables().find(t => t.name === IDENTITIES_TABLE)!;
}

export function identityRows(identities: OwnIdentity[]): InsertRowPayload[] {
    return identities.map(identity => identityRow('id-' + identity.keyId, identity));
}

export function localIdentityProvider(
    identities: OwnIdentity[], initialRows?: { [table: string]: json.Literal[] },
): { idProvider: string; initialRows: { [table: string]: json.Literal[] } } {
    return {
        idProvider: IDENTITIES_TABLE,
        initialRows: {
            ...initialRows,
            [IDENTITIES_TABLE]: [...(initialRows?.[IDENTITIES_TABLE] ?? []), ...identityRows(identities)],
        },
    };
}

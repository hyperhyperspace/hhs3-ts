// Mode 1: tracker (or any PeerDiscovery) + Users cap check at connect time.

import type { KeyId } from "@hyper-hyper-space/hhs3_crypto";
import type { PeerAuthorizer } from "@hyper-hyper-space/hhs3_mesh";

import type { RTableGroupImpl } from "../rtable_group/group.js";
import { findCapGrants, USERS_PEER_CAP } from "./users.js";

export function createUsersPeerAuthorizer(
    group: RTableGroupImpl,
    peerCapLabel: string = USERS_PEER_CAP,
): PeerAuthorizer {
    return {
        async authorize(keyId: KeyId): Promise<boolean> {
            return (await findCapGrants(group, keyId, peerCapLabel)).length > 0;
        },
    };
}

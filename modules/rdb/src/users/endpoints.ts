// Mode 2: explicit publish/withdraw of mesh listen addresses in the Users group.

import { B64Hash, KeyId, OwnIdentity, random, base64 } from "@hyper-hyper-space/hhs3_crypto";
import type { NetworkAddress, PeerInfo } from "@hyper-hyper-space/hhs3_mesh";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import { deriveRowId } from "../rtable/hash.js";
import type { RTableGroupImpl } from "../rtable_group/group.js";
import { CAPS_TABLE, ENDPOINTS_TABLE } from "./users.js";

function newEndpointUuid(): string {
    const bytes = random.getBytes(16);
    return 'ep-' + base64.fromArrayBuffer(bytes.slice().buffer);
}

async function endpointsViewAt(group: RTableGroupImpl, at?: Version) {
    at = at ?? await (await group.getScopedDag()).getFrontier();
    return { at, view: await (await group.getView(at, at)).getTableView(ENDPOINTS_TABLE) };
}

export async function findEndpoints(
    group: RTableGroupImpl, identity: KeyId, at?: Version,
): Promise<B64Hash[]> {
    const { view } = await endpointsViewAt(group, at);
    return view.findRowIds({ identity });
}

export async function publishEndpoints(
    group: RTableGroupImpl, author: OwnIdentity, addresses: NetworkAddress[], at?: Version,
): Promise<B64Hash | undefined> {
    const { at: resolvedAt, view } = await endpointsViewAt(group, at);
    const existing = await view.findRowIds({ identity: author.keyId });

    const writes: Array<{
        table: string;
        op:
            | { action: 'delete'; rowId: B64Hash }
            | { action: 'insert'; rowId: B64Hash; uuid: string; values: { address: string; identity: string } };
    }> = [];

    for (const rowId of existing) {
        writes.push({ table: ENDPOINTS_TABLE, op: { action: 'delete', rowId } });
    }
    for (const address of addresses) {
        const uuid = newEndpointUuid();
        writes.push({
            table: ENDPOINTS_TABLE,
            op: {
                action: 'insert',
                rowId: deriveRowId(uuid, author.keyId),
                uuid,
                values: { address, identity: author.keyId },
            },
        });
    }

    if (writes.length === 0) return undefined;

    return group.bundle(writes, author, resolvedAt);
}

export async function withdrawEndpoints(
    group: RTableGroupImpl, author: OwnIdentity, at?: Version,
): Promise<B64Hash | undefined> {
    return publishEndpoints(group, author, [], at);
}

export async function resolvePeerDirectory(
    group: RTableGroupImpl,
    peerCapLabel: string,
    opts?: { excludeKeyId?: KeyId; at?: Version },
): Promise<PeerInfo[]> {
    const at = opts?.at ?? await (await group.getScopedDag()).getFrontier();
    const view = await group.getView(at, at);
    const capsView = await view.getTableView(CAPS_TABLE);
    const endpointsView = await view.getTableView(ENDPOINTS_TABLE);

    const capRowIds = await capsView.findRowIds({ label: peerCapLabel });
    const holders = new Set<KeyId>();
    for (const rowId of capRowIds) {
        const row = await capsView.getRow(rowId);
        const grantee = row?.values['grantee'];
        if (typeof grantee === 'string') holders.add(grantee);
    }

    const result: PeerInfo[] = [];
    for (const keyId of holders) {
        if (opts?.excludeKeyId !== undefined && keyId === opts.excludeKeyId) continue;

        const endpointRowIds = await endpointsView.findRowIds({ identity: keyId });
        const addresses: NetworkAddress[] = [];
        for (const epId of endpointRowIds) {
            const ep = await endpointsView.getRow(epId);
            const addr = ep?.values['address'];
            if (typeof addr === 'string') addresses.push(addr as NetworkAddress);
        }
        if (addresses.length > 0) {
            result.push({ keyId, addresses });
        }
    }
    return result;
}

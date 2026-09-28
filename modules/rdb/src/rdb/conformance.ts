// catalogStatus: how this replica's database stands against its catalog.
//
// A read-only, per-replica report: presence, adoption and held releases are
// local, so two replicas can report different statuses for the same RDb.
//
//   released   the catalog's maximal releases
//   deployed   the RDb's maximal deployed releases (history: every one)
//   adopted    deployed releases within the adoption range
//   held       deployed releases outside it
//
// For each member: whether the group is present, its target version (the
// union of its versions across the deployed releases), its current deployed
// version, the version its gate admitted, and a state:
//   ok            current equals the target
//   intermediate  current is a version no release pins
//   behind        current is below the target
//   ahead         current is above the target
//   diverged      current and target are concurrent
//   missing       the group is not present on this replica

import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { refVersionAtOrAbove } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchema } from "../rschema/interfaces.js";
import type { RTableGroupImpl } from "../rtable_group/group.js";
import type { RDeployGateImpl } from "../rdeploy_gate/rdeploy_gate.js";
import { deployGateId } from "../rdeploy_gate/mirror.js";
import { versionKey } from "../rcatalog/resolve.js";
import type { UnresolvedReason } from "./resolve.js";
import { adoptedReleases, heldReleases } from "./adoption.js";
import type { RDbImpl } from "./rdb.js";

export type ReleaseInfo = {
    hash: B64Hash;
    version: string;
    note?: string;
};

export type MemberState = 'ok' | 'intermediate' | 'behind' | 'ahead' | 'diverged' | 'missing';

export type MemberStatus = {
    name: string;
    groupId: B64Hash;
    catalogGroupHash: B64Hash;
    schemaRef: B64Hash;
    present: boolean;
    target: B64Hash[];
    current?: B64Hash[];
    adopted?: B64Hash[];
    state: MemberState;
};

export type CatalogStatus = {
    rdb: B64Hash;
    catalog: B64Hash;
    unresolved?: UnresolvedReason;
    adoptionRange: string;
    released: ReleaseInfo[];
    deployed: ReleaseInfo[];
    history: ReleaseInfo[];
    adopted: ReleaseInfo[];
    held: ReleaseInfo[];
    members: MemberStatus[];
};

export async function catalogStatus(rdb: RDbImpl): Promise<CatalogStatus> {
    const ctx = rdb.getContext();
    const resolution = await rdb.resolve();
    const range = await rdb.getAdoptionRange();
    const catalog = await rdb.getCatalog();
    const index = catalog === undefined ? undefined : await catalog.getIndex();
    const catalogFrontier = catalog === undefined ? undefined : await (await catalog.getScopedDag()).getFrontier();

    const info = (hash: B64Hash): ReleaseInfo => {
        const state = index?.hasEntry(hash) === true && index.isRelease(hash) ? index.releaseState(hash) : undefined;
        const out: ReleaseInfo = { hash, version: state?.version ?? '?' };
        if (state?.note !== undefined) out.note = state.note;
        return out;
    };

    const status: CatalogStatus = {
        rdb: rdb.getId(),
        catalog: rdb.getCatalogRef(),
        adoptionRange: range,
        released: index === undefined || catalogFrontier === undefined ? [] : index.maximalReleasesAt(catalogFrontier).map(info),
        deployed: (resolution.deployed ?? []).map(info),
        history: resolution.history.map(info),
        adopted: adoptedReleases(resolution, range).map(info),
        held: heldReleases(resolution, range).map(info),
        members: [],
    };
    if (resolution.unresolved !== undefined) status.unresolved = resolution.unresolved;

    const membership = resolution.membership;
    if (membership === undefined || index === undefined || catalogFrontier === undefined) return status;

    const releases = index.releasesAt(catalogFrontier);
    for (const hash of membership.order) {
        const member = membership.byHash.get(hash)!;
        const target = [...member.target].sort();
        const entry: MemberStatus = {
            name: member.name,
            groupId: member.id,
            catalogGroupHash: hash,
            schemaRef: member.def.schemaRef,
            present: false,
            target,
            state: 'missing',
        };

        const gate = await ctx.getObject(deployGateId(member.id, member.def.schemaRef));
        if (gate !== undefined) entry.adopted = [...await (gate as RDeployGateImpl).getAdmittedFrontier()].sort();

        const groupObj = await ctx.getObject(member.id);
        const schemaObj = await ctx.getObject(member.def.schemaRef);
        if (groupObj !== undefined && schemaObj !== undefined) {
            const group = groupObj as RTableGroupImpl;
            const schema = schemaObj as RSchema;
            const frontier = await (await group.getScopedDag()).getFrontier();
            const current = [...await (await schema.getScopedDag()).findMinimalCover(await group.resolveSchemaVersion(frontier))].sort();
            entry.present = true;
            entry.current = current;

            const pinnedByRelease = releases.some((r) => {
                const g = index.releaseState(r).groups.get(hash);
                return g !== undefined && versionKey(g.version) === versionKey(current);
            });
            const causal = await schema.getCausalDag();
            const above = await refVersionAtOrAbove(causal, new Set(current), new Set(target));
            const below = await refVersionAtOrAbove(causal, new Set(target), new Set(current));
            if (above && below) entry.state = 'ok';
            else if (!pinnedByRelease) entry.state = 'intermediate';
            else if (below) entry.state = 'behind';
            else if (above) entry.state = 'ahead';
            else entry.state = 'diverged';
        }

        status.members.push(entry);
    }

    return status;
}

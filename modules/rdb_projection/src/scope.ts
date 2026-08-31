// Build the replica-wide projection SCOPE: resolve an RDb's member groups to
// the rdb_adapter GroupProjections that materialize them into ONE shared target.
//
// A replica already defines a set of table groups, so an RDb is the natural unit
// of projection: project every member group together and cross-group foreign
// keys resolve to serial ids (a sibling group's row is a real row in the same
// store) instead of opaque row_hash passthroughs. Two invariants make that safe:
//
//   - group-qualified names: every table is projected as `<groupName>_<table>`
//     (and its sync table likewise), so tables from different groups never
//     collide in the shared target;
//   - a cross-group resolver: for each group, a closure that maps a qualified FK
//     ref ('binding.table') to the referenced group's QUALIFIED target table -
//     but only when that group is CO-PROJECTED (a member the replica holds);
//     a ref into a non-projected group falls back to a row_hash passthrough.
//
// RDb membership is advisory, so a member the replica hasn't fetched yet is
// simply skipped and folded in later (the supervisor rebuilds the scope on a
// membership change).

import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";
import type { RDb, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import type { AdapterConfig, CrossGroupResolver, GroupProjection } from "@hyper-hyper-space/hhs3_rdb_adapter";

// Optional per-group config hook. It may set naming/fkBundling/writer, but NOT
// tableNames: the shared projection owns the group-qualified namespace so the
// cross-group resolver stays consistent (a tableNames override is ignored).
export type GroupConfigOverride = (info: { groupId: B64Hash; groupName: string; tableNames: string[] }) => AdapterConfig;

export type ScopeOptions = {
    // Default writer applied to every member that lacks one (enables ingestion).
    writer?: OwnIdentity;
    // Default FK-consecutive bundling applied to every member that does not set
    // it (see AdapterConfig.fkBundling); a per-group configOverride still wins.
    fkBundling?: boolean;
    configOverride?: GroupConfigOverride;
};

// Make a group name safe to use as a SQL identifier prefix.
function sanitizePrefix(name: string): string {
    const cleaned = name.replace(/[^A-Za-z0-9_]/g, '_');
    return /^[A-Za-z_]/.test(cleaned) ? cleaned : '_' + cleaned;
}

// Resolve the RTableGroup objects the replica currently holds for an RDb's
// member groups. Members not yet present are skipped (advisory membership).
export async function resolveMemberGroups(rdb: RDb, ctx: RContext): Promise<RTableGroup[]> {
    const ids = await rdb.getMemberGroups();
    const groups: RTableGroup[] = [];
    for (const id of ids) {
        const obj = await ctx.getObject(id);
        if (obj !== undefined) groups.push(obj as unknown as RTableGroup);
    }
    return groups;
}

// Build one GroupProjection per resolved group, sharing one cross-group registry
// (so a cross-group FK to a co-projected sibling resolves to serial ids).
export async function buildScope(groups: RTableGroup[], options: ScopeOptions = {}): Promise<GroupProjection[]> {
    // 1. Per-group qualified prefix (collision-checked) + current table list.
    const prefixes = new Map<B64Hash, string>();
    const usedPrefixes = new Map<string, B64Hash>();
    const tableList = new Map<B64Hash, string[]>();
    for (const group of groups) {
        const id = group.getId();
        const prefix = sanitizePrefix(group.getName());
        const clash = usedPrefixes.get(prefix);
        if (clash !== undefined && clash !== id) {
            throw new Error(
                `group name prefix '${prefix}' collides between groups '${clash}' and '${id}'; `
                + `group names must be distinct for a shared replica-wide projection`);
        }
        usedPrefixes.set(prefix, id);
        prefixes.set(id, prefix);
        const view = await group.getView();
        tableList.set(id, view.getTableNames());
    }

    // 2. Per-group tableNames map (rdbTable -> qualified target table).
    const tableNamesById = new Map<B64Hash, { [rdbTable: string]: string }>();
    for (const group of groups) {
        const id = group.getId();
        const prefix = prefixes.get(id)!;
        const map: { [rdbTable: string]: string } = {};
        for (const t of tableList.get(id)!) map[t] = `${prefix}_${t}`;
        tableNamesById.set(id, map);
    }

    // 3. A per-group cross-group resolver, closed over the shared registry.
    const resolverFor = (group: RTableGroup): CrossGroupResolver => {
        const bindings = group.getBindings();   // alias -> foreign group id
        return (ref: string): string | undefined => {
            const dot = ref.indexOf('.');
            if (dot < 0) return undefined;                 // not qualified: local
            const foreignId = bindings[ref.slice(0, dot)];
            if (foreignId === undefined) return undefined; // unknown binding alias
            const foreignTables = tableNamesById.get(foreignId);
            if (foreignTables === undefined) return undefined;   // not co-projected
            return foreignTables[ref.slice(dot + 1)];
        };
    };

    // 4. Assemble the GroupProjections.
    const members: GroupProjection[] = [];
    for (const group of groups) {
        const id = group.getId();
        const override = options.configOverride?.({ groupId: id, groupName: group.getName(), tableNames: tableList.get(id)! }) ?? {};
        const config: AdapterConfig = {
            ...override,
            tableNames: tableNamesById.get(id)!,       // owned by the scope (overrides any override)
            crossGroup: resolverFor(group),
        };
        if (config.writer === undefined && options.writer !== undefined) config.writer = options.writer;
        if (config.fkBundling === undefined && options.fkBundling !== undefined) config.fkBundling = options.fkBundling;
        members.push({ group, config });
    }
    return members;
}

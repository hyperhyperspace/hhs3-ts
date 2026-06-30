import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RTableGroup } from "@hyper-hyper-space/hhs3_rdb";

/**
 * Topo-sort member tablegroup ids so bound groups precede binders.
 * Only BIND edges where the bound id is also a member are considered.
 */
export async function sortMemberGroupsByBindings(
    memberGroupIds: B64Hash[],
    loadGroup: (id: B64Hash) => Promise<RTableGroup>,
): Promise<B64Hash[]> {
    const members = new Set(memberGroupIds);
    const deps = new Map<B64Hash, B64Hash[]>();
    for (const id of memberGroupIds) {
        const group = await loadGroup(id);
        const bound: B64Hash[] = [];
        for (const boundId of Object.values(group.getBindings())) {
            if (members.has(boundId)) bound.push(boundId);
        }
        deps.set(id, bound);
    }

    const inDegree = new Map<B64Hash, number>();
    const dependents = new Map<B64Hash, B64Hash[]>();
    for (const id of memberGroupIds) {
        inDegree.set(id, 0);
        dependents.set(id, []);
    }
    for (const [binder, boundIds] of deps) {
        for (const boundId of boundIds) {
            inDegree.set(binder, (inDegree.get(binder) ?? 0) + 1);
            dependents.get(boundId)!.push(binder);
        }
    }

    const queue = memberGroupIds.filter((id) => (inDegree.get(id) ?? 0) === 0);
    const sorted: B64Hash[] = [];
    while (queue.length > 0) {
        const id = queue.shift()!;
        sorted.push(id);
        for (const next of dependents.get(id) ?? []) {
            const deg = (inDegree.get(next) ?? 1) - 1;
            inDegree.set(next, deg);
            if (deg === 0) queue.push(next);
        }
    }

    if (sorted.length !== memberGroupIds.length) {
        const stuck = memberGroupIds.filter((id) => !sorted.includes(id));
        throw new Error(`Circular BIND dependency among tablegroups: ${stuck.join(', ')}`);
    }
    return sorted;
}

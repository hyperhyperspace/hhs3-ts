// The gate graph of a table group G. Its nodes are the groups G binds, by id,
// and it has an edge H → K whenever a canObserve gate on an alias bound to H
// names an alias bound to K ≠ H. It belongs to G alone: the binding DAG says
// that G reads H and K, never how G's gates relate them, so a gate on `a`
// reading `b.t` and a gate on `b` reading `a.t` form a cycle there.
//
// The graph must be a DAG, which creation checks. Observes of different
// bindings then never read each other's verdicts in a loop: every loop
// through an observe passes through G's own rows. And the graph orders the
// bindings to observe in: K before an H whose gate reads it.
//
// Edges compare bound ids, not alias names: several aliases can name one
// group, and an atom on any alias bound to H is a read of the binding being
// gated, not an edge.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";

import type { Predicate } from "../rschema/payload.js";
import { splitTableRef } from "../rschema/payload.js";
import { collectExistsAtoms } from "../rschema/validate.js";

export type GateGraph = Map<B64Hash, Set<B64Hash>>;

export function gateGraph(
    bindings: { [alias: string]: B64Hash },
    canObserve: { [alias: string]: Predicate },
): GateGraph {
    const graph: GateGraph = new Map();
    for (const id of Object.values(bindings)) graph.set(id, graph.get(id) ?? new Set());
    for (const [alias, gate] of Object.entries(canObserve)) {
        const gated = bindings[alias];
        if (gated === undefined) continue;   // an unbound gate key is rejected elsewhere
        for (const atom of collectExistsAtoms(gate)) {
            const [qualifier] = splitTableRef(atom.table);
            if (qualifier === undefined) continue;   // the observer's own table
            const read = bindings[qualifier];
            if (read === undefined || read === gated) continue;   // unbound (rejected elsewhere), or the gated group
            graph.get(gated)!.add(read);
        }
    }
    return graph;
}

// A cycle of the graph, as the ids along it, or undefined when it is a DAG.
export function gateGraphCycle(graph: GateGraph): B64Hash[] | undefined {
    const state = new Map<B64Hash, 'open' | 'done'>();
    const path: B64Hash[] = [];
    const visit = (node: B64Hash): B64Hash[] | undefined => {
        state.set(node, 'open');
        path.push(node);
        for (const next of [...(graph.get(node) ?? [])].sort()) {
            if (state.get(next) === 'open') return path.slice(path.indexOf(next));
            if (state.get(next) === undefined) {
                const cycle = visit(next);
                if (cycle !== undefined) return cycle;
            }
        }
        path.pop();
        state.set(node, 'done');
        return undefined;
    };
    for (const node of [...graph.keys()].sort()) {
        if (state.get(node) !== undefined) continue;
        const cycle = visit(node);
        if (cycle !== undefined) return cycle;
    }
    return undefined;
}

// The bound ids in the order to observe them: a group comes after every
// group its gate reads. Ties break by id, so every replica gets one order.
// Requires a DAG.
export function gateGraphOrder(graph: GateGraph): B64Hash[] {
    const order: B64Hash[] = [];
    const placed = new Set<B64Hash>();
    const place = (node: B64Hash): void => {
        if (placed.has(node)) return;
        placed.add(node);
        for (const read of [...(graph.get(node) ?? [])].sort()) place(read);
        order.push(node);
    };
    for (const node of [...graph.keys()].sort()) place(node);
    return order;
}

// The aliases bound to each id, for messages.
export function aliasesById(bindings: { [alias: string]: B64Hash }): Map<B64Hash, string[]> {
    const byId = new Map<B64Hash, string[]>();
    for (const [alias, id] of Object.entries(bindings)) byId.set(id, [...(byId.get(id) ?? []), alias].sort());
    return byId;
}

// Why the gates form a cycle, naming aliases, or undefined when they don't.
export function gateGraphCycleReason(
    bindings: { [alias: string]: B64Hash },
    canObserve: { [alias: string]: Predicate },
): string | undefined {
    const cycle = gateGraphCycle(gateGraph(bindings, canObserve));
    if (cycle === undefined) return undefined;
    const names = aliasesById(bindings);
    const steps = [...cycle, cycle[0]!].map((id) => names.get(id)!.join('/'));
    return `the ALLOW UPDATE REF gates read each other in a cycle: ${steps.join(' -> ')}`;
}

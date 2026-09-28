// Normative derivation of RDeployGate ids and mirror hashes.
//
// Deploy validity depends on these functions: a synced deploy carries the
// mirror hashes of its target version, and every replica must compute the
// same ones. They are versioned with the gate type (RDEPLOY_GATE_TYPE_ID) and
// locked by golden vectors; changing them changes which deploys validate.
//
//   gate id          the hash of the gate genesis entry {group, schema}
//   mirror(genesis)  the admit entry of the schema genesis, with the gate
//                    genesis as its only predecessor
//   mirror(h)        the admit entry of schema entry h, with the mirrors of
//                    h's predecessors as its predecessors
//
// Every hash is a function of the schema DAG alone, so it is identical on every
// replica whatever order entries were admitted in. Entries are hashed with
// SHA-256, like root object ids.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, sha256 } from "@hyper-hyper-space/hhs3_crypto";
import { dag, position } from "@hyper-hyper-space/hhs3_dag";

import { AdmitPayload, CreateDeployGatePayload, RDEPLOY_GATE_TYPE_ID } from "./payload.js";

export function deployGateCreatePayload(groupId: B64Hash, schemaId: B64Hash): CreateDeployGatePayload {
    return { action: 'create', type: RDEPLOY_GATE_TYPE_ID, group: groupId, schema: schemaId };
}

export function deployGateId(groupId: B64Hash, schemaId: B64Hash): B64Hash {
    return dag.createEntry(deployGateCreatePayload(groupId, schemaId), {}, position(), sha256).hash;
}

export function admitPayload(schemaEntry: B64Hash): AdmitPayload {
    return { action: 'admit', entry: schemaEntry };
}

// The hash of the admit entry mirroring `schemaEntry`, given the mirror hashes
// of its predecessors (the gate id for the schema genesis).
export function mirrorEntryHash(schemaEntry: B64Hash, mirroredPrevs: Iterable<B64Hash>): B64Hash {
    return dag.createEntry(admitPayload(schemaEntry), {}, position(...mirroredPrevs), sha256).hash;
}

// What mirroring needs from a schema DAG: its entries' predecessors.
export type SchemaHeaderSource = {
    loadEntry(h: B64Hash): Promise<{ header: { prevEntryHashes: json.Set } } | undefined>;
};

// The mirror hashes of every entry of `version` (sorted). `memo` maps schema
// entry -> mirror hash and may be shared across calls for the same gate: the
// schema DAG is append-only, so a mirror never changes.
export async function computeMirrorHashes(
    schemaDag: SchemaHeaderSource,
    gateId: B64Hash,
    targets: Iterable<B64Hash>,
    memo: Map<B64Hash, B64Hash> = new Map(),
): Promise<B64Hash[]> {
    const prevsOf = new Map<B64Hash, B64Hash[]>();

    const loadPrevs = async (h: B64Hash): Promise<B64Hash[]> => {
        const known = prevsOf.get(h);
        if (known !== undefined) return known;
        const entry = await schemaDag.loadEntry(h);
        if (entry === undefined) throw new Error(`schema entry '${h}' is not present`);
        const prevs = [...json.fromSet(entry.header.prevEntryHashes)];
        prevsOf.set(h, prevs);
        return prevs;
    };

    // iterative post-order walk: a mirror is computed once all of its
    // predecessors' mirrors are known
    for (const target of targets) {
        const stack: B64Hash[] = [target];
        while (stack.length > 0) {
            const h = stack[stack.length - 1];
            if (memo.has(h)) { stack.pop(); continue; }
            const prevs = await loadPrevs(h);
            const missing = prevs.filter((p) => !memo.has(p));
            if (missing.length > 0) {
                stack.push(...missing);
                continue;
            }
            stack.pop();
            memo.set(h, mirrorEntryHash(h, prevs.length === 0 ? [gateId] : prevs.map((p) => memo.get(p)!)));
        }
    }

    return [...new Set([...targets].map((h) => memo.get(h)!))].sort();
}

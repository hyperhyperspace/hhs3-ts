// Resolution of an RDb at a position.
//
// The RDb DAG has no barriers: its state is a pure function of the create and
// the update-catalog ops at or below `at`.
//
//   deployed   the maximal releases (in catalog order) among the create's
//              release and every update-catalog release. The full history of
//              deployed releases is kept too: the adoption policy admits each
//              deployed release within the app's range, even once a later
//              one supersedes it.
//   params     create ∪ updates. An update may only set params that are not
//              set in its causal past, so two ops can only disagree when they
//              are concurrent; the larger op hash wins.
//   members    computeMembership over the deployed releases' folded states,
//              with the resolved params and the RDb's creators.
//
// A missing catalog or release, or a create that does not match its catalog
// (a param missing or of the wrong type), resolves to a typed "unresolved"
// state with no members.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { Entry, Position } from "@hyper-hyper-space/hhs3_dag";

import type { CatalogIndex, ReleaseState } from "../rcatalog/resolve.js";
import { sortReleases } from "../rcatalog/resolve.js";
import type { CatalogParamDecl } from "../rcatalog/payload.js";
import { CreateRDbPayload, UpdateCatalogPayload, ParamValue } from "./payload.js";
import { computeMembership, Membership, MissingParamError, paramTypeFits } from "./instantiate.js";

export type RDbOps = {
    create: CreateRDbPayload;
    createHash: B64Hash;
    updates: { hash: B64Hash; payload: UpdateCatalogPayload }[];   // topological order
};

// The create and the update-catalog ops at or below `at`. `entries` must be
// the RDb's full entry list, in topological order.
export function collectOps(entries: Entry[], at: Position): RDbOps {
    const byHash = new Map<B64Hash, Entry>();
    for (const entry of entries) byHash.set(entry.hash, entry);

    const included = new Set<B64Hash>();
    const pending = [...at];
    while (pending.length > 0) {
        const hash = pending.pop()!;
        if (included.has(hash)) continue;
        const entry = byHash.get(hash);
        if (entry === undefined) throw new Error(`resolveRDb: entry '${hash}' not found`);
        included.add(hash);
        for (const prev of json.fromSet(entry.header.prevEntryHashes)) pending.push(prev);
    }

    let create: { hash: B64Hash; payload: CreateRDbPayload } | undefined;
    const updates: { hash: B64Hash; payload: UpdateCatalogPayload }[] = [];
    for (const entry of entries) {
        if (!included.has(entry.hash)) continue;
        const action = (entry.payload as json.LiteralMap)['action'];
        if (action === 'create') {
            create = { hash: entry.hash, payload: entry.payload as CreateRDbPayload };
        } else if (action === 'update-catalog') {
            updates.push({ hash: entry.hash, payload: entry.payload as UpdateCatalogPayload });
        } else {
            throw new Error(`resolveRDb: unknown action '${String(action)}'`);
        }
    }
    if (create === undefined) throw new Error("resolveRDb: create entry not at or below the requested position");
    return { create: create.payload, createHash: create.hash, updates };
}

// Every release named by the create or an update, in first-deployed order.
export function deployedHistory(ops: RDbOps): B64Hash[] {
    const seen = new Set<B64Hash>();
    const out: B64Hash[] = [];
    for (const release of [ops.create.release, ...ops.updates.map((u) => u.payload.release)]) {
        if (!seen.has(release)) { seen.add(release); out.push(release); }
    }
    return out;
}

export function resolveParams(ops: RDbOps): { [name: string]: ParamValue } {
    const params: { [name: string]: ParamValue } = { ...(ops.create.params ?? {}) };
    const winner = new Map<string, B64Hash>();
    for (const update of ops.updates) {
        for (const [name, value] of Object.entries(update.payload.params ?? {})) {
            if (Object.prototype.hasOwnProperty.call(ops.create.params ?? {}, name)) continue;
            const prior = winner.get(name);
            if (prior === undefined || update.hash > prior) {
                winner.set(name, update.hash);
                params[name] = value;
            }
        }
    }
    return params;
}

export type UnresolvedReason =
    | { kind: 'missing-catalog'; catalog: B64Hash }
    | { kind: 'missing-release'; release: B64Hash }
    | { kind: 'invalid-params'; message: string };

export type RDbResolution = {
    catalog: B64Hash;
    history: B64Hash[];                        // every deployed release, first-deployed order
    params: { [name: string]: ParamValue };
    // present when the catalog and every deployed release resolve:
    deployed?: B64Hash[];                      // maximal deployed releases, in catalog order
    releases?: Map<B64Hash, ReleaseState>;     // states of the history releases
    membership?: Membership;
    unresolved?: UnresolvedReason;
};

export function resolveRDb(input: {
    rdbId: B64Hash;
    ops: RDbOps;
    catalog: CatalogIndex | undefined;
}): RDbResolution {
    const { ops } = input;
    const history = deployedHistory(ops);
    const params = resolveParams(ops);
    const base = { catalog: ops.create.catalog, history, params };

    const index = input.catalog;
    if (index === undefined) return { ...base, unresolved: { kind: 'missing-catalog', catalog: ops.create.catalog } };

    const releases = new Map<B64Hash, ReleaseState>();
    for (const release of history) {
        if (!index.hasEntry(release) || !index.isRelease(release)) {
            return { ...base, unresolved: { kind: 'missing-release', release } };
        }
        releases.set(release, index.releaseState(release));
    }

    const maximal = history.filter((r) => !history.some((o) => o !== r && index.isReleaseBelow(r, o)));
    const deployedStates = sortReleases(maximal.map((r) => releases.get(r)!));
    const deployed = deployedStates.map((s) => s.hash);

    const declared = new Map<string, CatalogParamDecl>();
    for (const state of deployedStates) {
        for (const [name, decl] of state.params) declared.set(name, decl);
    }
    for (const [name, decl] of declared) {
        const value = params[name];
        if (value === undefined) {
            return { ...base, deployed, releases, unresolved: { kind: 'invalid-params', message: `catalog param ':${name}' has no value` } };
        }
        if (!paramTypeFits(decl, value)) {
            return { ...base, deployed, releases, unresolved: { kind: 'invalid-params', message: `catalog param ':${name}' does not have type '${decl.type}'` } };
        }
    }

    try {
        const membership = computeMembership({
            rdbId: input.rdbId,
            releases: deployedStates,
            params,
            creators: ops.create.creators ?? [],
            hashAlgorithm: ops.create.hashAlgorithm,
        });
        return { ...base, deployed, releases, membership };
    } catch (err) {
        if (err instanceof MissingParamError) {
            return { ...base, deployed, releases, unresolved: { kind: 'invalid-params', message: err.message } };
        }
        throw err;
    }
}

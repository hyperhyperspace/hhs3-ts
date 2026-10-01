// Resolution of an RCatalog's releases and their folded states.
//
// The catalog DAG has no barriers, so everything here is a pure function of
// the entries at or below a position. A release's state depends only on the
// release itself (its parents are fixed by its signed predecessors), so states
// are memoized per release hash and shared across index rebuilds.
//
// A release's parents are the maximal releases in the causal past of its
// predecessors (declares in between are skipped). Its state folds:
//   - the union of its parents' groups (a group whose version differs across
//     parents must be set by the release's `changes`; validate_ops.ts enforces
//     it, and resolution falls back to the union of the versions),
//   - the definitions it adds, at their pins,
//   - its `changes`.
// Group and FILES definitions and params accumulate over the causal past.
// FILES definitions are immutable, so parents never disagree on them.
//
// Catalog DAGs are small: the index loads every entry and computes causal
// pasts directly, with no meta indexing.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { Entry } from "@hyper-hyper-space/hhs3_dag";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import {
    CatalogGroupDef, CatalogFilesDef, CatalogParamDecl, CatalogReleaseBody, CreateRCatalogPayload,
    CatalogDeclarePayload, SchemaCreator, catalogGroupHash, catalogFilesHash,
} from "./payload.js";
import { compareSemver } from "./semver.js";

export type CatalogGroupState = {
    schema: B64Hash;
    version: B64Hash[];   // sorted
};

export type ReleaseState = {
    hash: B64Hash;
    version: string;
    parents: B64Hash[];                            // sorted; empty for the genesis
    ancestors: Set<B64Hash>;                       // releases strictly below
    groups: Map<B64Hash, CatalogGroupState>;       // folded: catalog group hash -> version
    defs: Map<B64Hash, CatalogGroupDef>;           // every definition in the causal past, own adds included
    addedIn: Map<B64Hash, B64Hash>;                // definition hash -> release that added it
    files: Map<B64Hash, CatalogFilesDef>;          // every FILES definition in the causal past, own adds included
    filesAddedIn: Map<B64Hash, B64Hash>;           // FILES definition hash -> release that added it
    params: Map<string, CatalogParamDecl>;         // every param declared in the causal past
    added: B64Hash[];                              // this release's own adds, in payload order
    changed: B64Hash[];                            // this release's own changes, sorted
    note?: string;
    author: KeyId;
};

export type DeclareInfo = {
    hash: B64Hash;
    schemas: B64Hash[];
    author: KeyId;
};

export function versionKey(v: Iterable<B64Hash>): string {
    return [...v].sort().join(',');
}

export function sortedVersion(v: Iterable<B64Hash>): B64Hash[] {
    return [...new Set(v)].sort();
}

// The result of folding a set of parent releases, before a release's own adds
// and changes are applied. `conflicts` are the groups whose version differs
// across the parents.
export type ParentFold = {
    groups: Map<B64Hash, CatalogGroupState>;
    conflicts: Map<B64Hash, CatalogGroupState[]>;
    defs: Map<B64Hash, CatalogGroupDef>;
    addedIn: Map<B64Hash, B64Hash>;
    files: Map<B64Hash, CatalogFilesDef>;
    filesAddedIn: Map<B64Hash, B64Hash>;
    params: Map<string, CatalogParamDecl>;
    ancestors: Set<B64Hash>;
};

export function emptyParentFold(): ParentFold {
    return {
        groups: new Map(), conflicts: new Map(), defs: new Map(), addedIn: new Map(),
        files: new Map(), filesAddedIn: new Map(), params: new Map(), ancestors: new Set(),
    };
}

export class CatalogIndex {

    private readonly byHash: Map<B64Hash, Entry> = new Map();
    private readonly order: B64Hash[] = [];
    private readonly genesisHash: B64Hash;
    private readonly genesis: CreateRCatalogPayload;

    constructor(entries: Entry[], private readonly memo: Map<B64Hash, ReleaseState>) {
        let genesisHash: B64Hash | undefined;
        for (const entry of entries) {
            this.byHash.set(entry.hash, entry);
            this.order.push(entry.hash);
            if ((entry.payload as json.LiteralMap)['action'] === 'create') genesisHash = entry.hash;
        }
        if (genesisHash === undefined) throw new Error("CatalogIndex: the catalog has no genesis entry");
        this.genesisHash = genesisHash;
        this.genesis = this.byHash.get(genesisHash)!.payload as CreateRCatalogPayload;
    }

    getGenesisHash(): B64Hash { return this.genesisHash; }
    getGenesis(): CreateRCatalogPayload { return this.genesis; }
    getCreators(): SchemaCreator[] { return this.genesis.creators; }

    hasEntry(hash: B64Hash): boolean { return this.byHash.has(hash); }

    private action(hash: B64Hash): string | undefined {
        const entry = this.byHash.get(hash);
        return entry === undefined ? undefined : (entry.payload as json.LiteralMap)['action'] as string;
    }

    isRelease(hash: B64Hash): boolean {
        const action = this.action(hash);
        return action === 'create' || action === 'release';
    }

    isDeclare(hash: B64Hash): boolean {
        return this.action(hash) === 'declare';
    }

    private prevs(hash: B64Hash): B64Hash[] {
        const entry = this.byHash.get(hash);
        if (entry === undefined) throw new Error(`CatalogIndex: entry '${hash}' not found`);
        return [...json.fromSet(entry.header.prevEntryHashes)];
    }

    // Every entry at or below `at` (the entries of `at` included).
    causalPast(at: Iterable<B64Hash>): Set<B64Hash> {
        const past = new Set<B64Hash>();
        const pending = [...at];
        while (pending.length > 0) {
            const hash = pending.pop()!;
            if (past.has(hash)) continue;
            if (!this.byHash.has(hash)) throw new Error(`CatalogIndex: entry '${hash}' not found`);
            past.add(hash);
            for (const prev of this.prevs(hash)) pending.push(prev);
        }
        return past;
    }

    // Releases at or below `at`, in the local topological order.
    releasesAt(at: Iterable<B64Hash>): B64Hash[] {
        const past = this.causalPast(at);
        return this.order.filter((h) => past.has(h) && this.isRelease(h));
    }

    // The maximal releases at or below `at` (sorted).
    maximalReleasesAt(at: Iterable<B64Hash>): B64Hash[] {
        const releases = this.releasesAt(at);
        const below = new Set<B64Hash>();
        for (const r of releases) {
            for (const a of this.releaseState(r).ancestors) below.add(a);
        }
        return releases.filter((r) => !below.has(r)).sort();
    }

    declaresAt(at: Iterable<B64Hash>): DeclareInfo[] {
        const past = this.causalPast(at);
        const out: DeclareInfo[] = [];
        for (const h of this.order) {
            if (!past.has(h) || !this.isDeclare(h)) continue;
            const payload = this.byHash.get(h)!.payload as CatalogDeclarePayload;
            out.push({ hash: h, schemas: [...json.fromSet(payload.schemas)].sort(), author: payload.author });
        }
        return out;
    }

    // Genesis pins plus every schema declared at or below `at`.
    referencedSchemasAt(at: Iterable<B64Hash>): Set<B64Hash> {
        const schemas = new Set<B64Hash>();
        for (const def of this.genesis.add ?? []) schemas.add(def.schemaRef);
        for (const declare of this.declaresAt(at)) {
            for (const s of declare.schemas) schemas.add(s);
        }
        return schemas;
    }

    findReleasesByVersion(version: string, at: Iterable<B64Hash>): B64Hash[] {
        return this.releasesAt(at).filter((r) => this.releaseState(r).version === version).sort();
    }

    // a strictly in b's causal past (both releases)
    isReleaseBelow(a: B64Hash, b: B64Hash): boolean {
        return this.releaseState(b).ancestors.has(a);
    }

    // Folds the states of `parents` (release hashes). Used both to build a
    // release's state and to validate a release before it is appended.
    foldParents(parents: B64Hash[]): ParentFold {
        const groups = new Map<B64Hash, CatalogGroupState>();
        const seen = new Map<B64Hash, CatalogGroupState[]>();
        const defs = new Map<B64Hash, CatalogGroupDef>();
        const addedIn = new Map<B64Hash, B64Hash>();
        const files = new Map<B64Hash, CatalogFilesDef>();
        const filesAddedIn = new Map<B64Hash, B64Hash>();
        const params = new Map<string, CatalogParamDecl>();
        const ancestors = new Set<B64Hash>();

        for (const parent of [...parents].sort()) {
            const state = this.releaseState(parent);
            ancestors.add(parent);
            for (const a of state.ancestors) ancestors.add(a);
            for (const [hash, def] of state.defs) defs.set(hash, def);
            for (const [hash, release] of state.addedIn) addedIn.set(hash, release);
            for (const [hash, def] of state.files) files.set(hash, def);
            for (const [hash, release] of state.filesAddedIn) filesAddedIn.set(hash, release);
            for (const [name, decl] of state.params) params.set(name, decl);
            for (const [hash, group] of state.groups) {
                const list = seen.get(hash) ?? [];
                if (!list.some((g) => versionKey(g.version) === versionKey(group.version))) list.push(group);
                seen.set(hash, list);
            }
        }

        const conflicts = new Map<B64Hash, CatalogGroupState[]>();
        for (const [hash, list] of seen) {
            if (list.length === 1) {
                groups.set(hash, list[0]);
            } else {
                conflicts.set(hash, list);
                const union = new Set<B64Hash>();
                for (const g of list) for (const h of g.version) union.add(h);
                groups.set(hash, { schema: list[0].schema, version: sortedVersion(union) });
            }
        }

        return { groups, conflicts, defs, addedIn, files, filesAddedIn, params, ancestors };
    }

    // The parents of an entry at `prevs`: the maximal releases below it.
    parentsOf(prevs: Iterable<B64Hash>): B64Hash[] {
        const list = [...prevs];
        if (list.length === 0) return [];
        return this.maximalReleasesAt(list);
    }

    releaseState(hash: B64Hash): ReleaseState {
        const cached = this.memo.get(hash);
        if (cached !== undefined) return cached;

        const entry = this.byHash.get(hash);
        if (entry === undefined) throw new Error(`CatalogIndex: release '${hash}' not found`);
        const payload = entry.payload as CatalogReleaseBody & { action: string };
        if (payload.action !== 'create' && payload.action !== 'release') {
            throw new Error(`CatalogIndex: entry '${hash}' is not a release`);
        }

        const parents = this.parentsOf(this.prevs(hash));
        const fold = this.foldParents(parents);
        const state = applyReleaseBody(hash, payload, parents, fold);
        this.memo.set(hash, state);
        return state;
    }
}

// Applies a release's own adds, changes and params on top of its parents'
// fold. Pure: shared by resolution and by validation (which applies a payload
// before it is appended).
export function applyReleaseBody(
    hash: B64Hash,
    body: CatalogReleaseBody,
    parents: B64Hash[],
    fold: ParentFold,
): ReleaseState {
    const groups = new Map(fold.groups);
    const defs = new Map(fold.defs);
    const addedIn = new Map(fold.addedIn);
    const params = new Map(fold.params);

    const added: B64Hash[] = [];
    for (const def of body.add ?? []) {
        const defHash = catalogGroupHash(def);
        added.push(defHash);
        defs.set(defHash, def);
        addedIn.set(defHash, hash);
        groups.set(defHash, { schema: def.schemaRef, version: sortedVersion(json.fromSet(def.schemaVersion)) });
    }

    const changed: B64Hash[] = [];
    for (const [defHash, change] of Object.entries(body.changes ?? {})) {
        changed.push(defHash);
        groups.set(defHash, { schema: change.schema, version: sortedVersion(json.fromSet(change.version)) });
    }
    changed.sort();

    const files = new Map(fold.files);
    const filesAddedIn = new Map(fold.filesAddedIn);
    for (const def of body.files ?? []) {
        const filesHash = catalogFilesHash(def);
        files.set(filesHash, def);
        filesAddedIn.set(filesHash, hash);
    }

    for (const decl of body.params ?? []) params.set(decl.name, decl);

    const state: ReleaseState = {
        hash,
        version: body.version,
        parents: [...parents].sort(),
        ancestors: fold.ancestors,
        groups,
        defs,
        addedIn,
        files,
        filesAddedIn,
        params,
        added,
        changed,
        author: body.author,
    };
    if (body.note !== undefined) state.note = body.note;
    return state;
}

export function versionOf(group: CatalogGroupState): Version {
    return new Set(group.version);
}

// Releases sorted by semver then hash (a stable display / resolution order).
export function sortReleases(states: ReleaseState[]): ReleaseState[] {
    return [...states].sort((a, b) => {
        const c = compareSemver(a.version, b.version);
        return c !== 0 ? c : (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0);
    });
}

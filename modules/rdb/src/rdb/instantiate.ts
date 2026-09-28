// Normative instantiation: how an RDb turns its catalog's group definitions
// into concrete RTableGroup create payloads.
//
// These functions decide group ids, so every replica must run them the same
// way; they are versioned with the RDb type and locked by golden vectors. The
// C-SQL compiler is not normative.
//
//   seed        deriveGroupSeed(rdbId, catalogGroupHash): each group is fresh
//               for its RDb (compute mode). instantiateGroup takes the seed
//               as an input, which keeps shared groups possible later.
//   rows        each row template becomes an unauthored insert with a
//               deterministic uuid (deriveGenesisRowUuid) and rowId =
//               deriveRowId(uuid); params are substituted, `publicKey(:p)`
//               takes an identity param's public key.
//   authority   a definition's ALLOW DEPLOY IF is copied as canDeploy. With no
//               declared rule and RDb creators present, canDeploy accepts the
//               creators ($author = keyId, one term per creator, sorted by key
//               id) and deployKeys embeds their keys so the predicate sees a
//               verified author. With neither, deploys stay open.
//   ids         rTableGroupFactory.computeRootObjectId of the payload; groups
//               are instantiated bottom-up over their bindings.

import { json } from "@hyper-hyper-space/hhs3_json";
import { B64Hash, base64, sha256, stringToUint8Array } from "@hyper-hyper-space/hhs3_crypto";
import { dag, position } from "@hyper-hyper-space/hhs3_dag";
import type { Version } from "@hyper-hyper-space/hhs3_mvt";

import type { Predicate, SchemaCreator } from "../rschema/payload.js";
import { columnValueMatchesType } from "../rschema/validate.js";
import { deriveRowId } from "../rtable/hash.js";
import type { InsertRowPayload } from "../rtable/payload.js";
import { CreateTableGroupPayload, RTABLE_GROUP_TYPE_ID } from "../rtable_group/payload.js";
import type { CatalogGroupDef, CatalogParamDecl } from "../rcatalog/payload.js";
import type { ReleaseState } from "../rcatalog/resolve.js";
import type { ParamValue } from "./payload.js";

function digest(value: json.Literal): string {
    return sha256.hashToB64(stringToUint8Array(json.toStringNormalized(value)));
}

export function deriveGroupSeed(rdbId: B64Hash, catalogGroupHash: B64Hash): string {
    return digest({ rdb: rdbId, group: catalogGroupHash });
}

export function deriveGenesisRowUuid(seed: string, table: string, index: number): string {
    return digest({ seed, table, index });
}

export function groupIdOf(payload: CreateTableGroupPayload): B64Hash {
    return dag.createEntry(payload as unknown as json.Literal, {}, position(), sha256).hash;
}

// The canDeploy default for a group whose definition declares none.
export function defaultDeployPredicate(creators: SchemaCreator[]): Predicate | undefined {
    const terms: Predicate[] = [...creators]
        .sort((a, b) => (a.keyId < b.keyId ? -1 : a.keyId > b.keyId ? 1 : 0))
        .map((c) => ({ p: 'cmp', cmp: 'eq', left: { lit: '$author' }, right: { lit: c.keyId } }));
    if (terms.length === 0) return undefined;
    return terms.length === 1 ? terms[0] : { p: 'or', args: terms };
}

export class MissingParamError extends Error {
    constructor(readonly param: string) {
        super(`catalog param ':${param}' has no value`);
        this.name = 'MissingParamError';
    }
}

function resolveParam(params: { [name: string]: ParamValue }, name: string, fn: 'publicKey' | undefined): json.Literal {
    const value = params[name];
    if (value === undefined) throw new MissingParamError(name);
    if ('identity' in value) return fn === 'publicKey' ? value.identity.publicKey : value.identity.keyId;
    if (fn === 'publicKey') throw new Error(`catalog param ':${name}' is not an identity`);
    return value.value;
}

export function instantiateGroup(def: CatalogGroupDef, opts: {
    seed: string;
    bindingIds: { [catalogGroupHash: string]: B64Hash };
    params: { [name: string]: ParamValue };
    creators: SchemaCreator[];
    hashAlgorithm?: string;
}): CreateTableGroupPayload {

    const payload: CreateTableGroupPayload = {
        action: 'create',
        type: RTABLE_GROUP_TYPE_ID,
        name: def.name,
        seed: opts.seed,
        schemaRef: def.schemaRef,
        schemaVersion: def.schemaVersion,
    };

    const rowTables = Object.keys(def.initialRows ?? {}).sort();
    if (rowTables.length > 0) {
        const initialRows: { [table: string]: json.Literal[] } = {};
        for (const table of rowTables) {
            initialRows[table] = def.initialRows![table].map((template, index) => {
                const values: json.LiteralMap = { ...template.values };
                for (const [column, ref] of Object.entries(template.params ?? {})) {
                    values[column] = resolveParam(opts.params, ref.param, ref.fn);
                }
                const uuid = deriveGenesisRowUuid(opts.seed, table, index);
                const insert: InsertRowPayload = { action: 'insert', rowId: deriveRowId(uuid), uuid, values };
                return insert as unknown as json.Literal;
            });
        }
        payload.initialRows = initialRows;
    }

    const aliases = Object.keys(def.bindings ?? {});
    if (aliases.length > 0) {
        const bindings: { [alias: string]: B64Hash } = {};
        for (const alias of aliases) {
            const target = def.bindings![alias];
            const id = opts.bindingIds[target];
            if (id === undefined) throw new Error(`binding '${alias}' of group '${def.name}' points at an uninstantiated group '${target}'`);
            bindings[alias] = id;
        }
        payload.bindings = bindings;
    }

    if (def.canDeploy !== undefined) {
        payload.canDeploy = def.canDeploy;
    } else {
        const predicate = defaultDeployPredicate(opts.creators);
        if (predicate !== undefined) {
            payload.canDeploy = predicate;
            payload.deployKeys = [...opts.creators]
                .sort((a, b) => (a.keyId < b.keyId ? -1 : a.keyId > b.keyId ? 1 : 0))
                .map((c) => ({ keyId: c.keyId, publicKey: c.publicKey }));
        }
    }

    if (def.canObserve !== undefined && Object.keys(def.canObserve).length > 0) payload.canObserve = def.canObserve;
    if (def.idProvider !== undefined) payload.idProvider = def.idProvider;
    if (opts.hashAlgorithm !== undefined) payload.hashAlgorithm = opts.hashAlgorithm;

    return payload;
}

// A computed member group of an RDb.
export type MemberGroup = {
    catalogGroupHash: B64Hash;
    name: string;              // the definition's name, tie-broken on clashes
    def: CatalogGroupDef;
    id: B64Hash;
    payload: CreateTableGroupPayload;
    target: Version;           // union of the group's versions across the deployed releases
};

export type Membership = {
    byHash: Map<B64Hash, MemberGroup>;
    byId: Map<B64Hash, MemberGroup>;
    order: B64Hash[];          // bottom-up over bindings (catalog group hashes)
    names: Map<string, B64Hash>;   // member name -> group id
};

// An identifier-safe prefix of a catalog group hash (hex of its first bytes).
function hashPrefix(hash: B64Hash, bytes: number): string {
    const raw = new Uint8Array(base64.toArrayBuffer(hash));
    return [...raw.slice(0, bytes)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

// Names: a clash can only come from concurrent deploys (or a merge release)
// joining definitions with the same name. The smallest catalog group hash
// keeps the name; the others become `name_<hashPrefix>`.
function assignNames(defs: Map<B64Hash, CatalogGroupDef>): Map<B64Hash, string> {
    const byName = new Map<string, B64Hash[]>();
    for (const [hash, def] of defs) {
        const list = byName.get(def.name) ?? [];
        list.push(hash);
        byName.set(def.name, list);
    }

    const taken = new Set(byName.keys());
    const names = new Map<B64Hash, string>();
    for (const [name, hashes] of [...byName.entries()].sort()) {
        const sorted = [...hashes].sort();
        names.set(sorted[0], name);
        for (const hash of sorted.slice(1)) {
            let bytes = 4;
            let candidate = `${name}_${hashPrefix(hash, bytes)}`;
            while (taken.has(candidate) && bytes < 32) {
                bytes += 2;
                candidate = `${name}_${hashPrefix(hash, bytes)}`;
            }
            taken.add(candidate);
            names.set(hash, candidate);
        }
    }
    return names;
}

// Bottom-up over bindings; ties broken by catalog group hash.
function instantiationOrder(defs: Map<B64Hash, CatalogGroupDef>): B64Hash[] {
    const order: B64Hash[] = [];
    const placed = new Set<B64Hash>();
    const remaining = [...defs.keys()].sort();
    while (remaining.length > 0) {
        const index = remaining.findIndex((hash) =>
            Object.values(defs.get(hash)!.bindings ?? {}).every((target) => placed.has(target)));
        if (index < 0) throw new Error("catalog group bindings do not form a DAG over the deployed definitions");
        const [hash] = remaining.splice(index, 1);
        order.push(hash);
        placed.add(hash);
    }
    return order;
}

export function paramTypeFits(decl: CatalogParamDecl, value: ParamValue): boolean {
    if (decl.type === 'identity') return 'identity' in value;
    return 'value' in value && columnValueMatchesType(value.value, decl.type);
}

// The member groups of an RDb whose deployed releases are `releases`.
// Membership is computed ∪ assigned; assigned is empty in v1.
export function computeMembership(input: {
    rdbId: B64Hash;
    releases: ReleaseState[];
    params: { [name: string]: ParamValue };
    creators: SchemaCreator[];
    hashAlgorithm?: string;
}): Membership {
    const defs = new Map<B64Hash, CatalogGroupDef>();
    const targets = new Map<B64Hash, Set<B64Hash>>();
    for (const release of input.releases) {
        for (const [hash, group] of release.groups) {
            const def = release.defs.get(hash);
            if (def === undefined) continue;
            defs.set(hash, def);
            const target = targets.get(hash) ?? new Set<B64Hash>();
            for (const h of group.version) target.add(h);
            targets.set(hash, target);
        }
    }

    const names = assignNames(defs);
    const order = instantiationOrder(defs);

    const bindingIds: { [hash: string]: B64Hash } = {};
    const byHash = new Map<B64Hash, MemberGroup>();
    const byId = new Map<B64Hash, MemberGroup>();
    const nameToId = new Map<string, B64Hash>();

    for (const hash of order) {
        const def = defs.get(hash)!;
        const payload = instantiateGroup(def, {
            seed: deriveGroupSeed(input.rdbId, hash),
            bindingIds,
            params: input.params,
            creators: input.creators,
            hashAlgorithm: input.hashAlgorithm,
        });
        const id = groupIdOf(payload);
        bindingIds[hash] = id;
        const member: MemberGroup = {
            catalogGroupHash: hash,
            name: names.get(hash)!,
            def,
            id,
            payload,
            target: new Set([...targets.get(hash)!].sort()),
        };
        byHash.set(hash, member);
        byId.set(id, member);
        nameToId.set(member.name, id);
    }

    return { byHash, byId, order, names: nameToId };
}

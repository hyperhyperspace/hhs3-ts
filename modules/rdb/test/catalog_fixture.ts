// Shared helpers for building catalog-based databases in tests.
//
// A fixture describes groups (each with its own schema unless it reuses
// another group's) and turns them into:
//   - offline payloads and ids (schemas, catalog genesis, RDb create, member
//     group ids), computable before any replica exists, so two-replica tests
//     can precompute their discovery topics;
//   - live objects on a context (mock or Replica): schemas + catalog, then the
//     RDb, deployed through the catalog planner (which creates the member
//     groups and their gates).

import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { createIdentity, SIGNING_ED25519, sha256 } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { dag, position } from "@hyper-hyper-space/hhs3_dag";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";
import { version } from "@hyper-hyper-space/hhs3_mvt";

import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import type { CreateRSchemaPayload, Predicate, TableDef } from "../src/rschema/payload.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { RCatalogImpl, rCatalogFactory } from "../src/rcatalog/rcatalog.js";
import { catalogGroupHash } from "../src/rcatalog/payload.js";
import type { CatalogFilesDef, CatalogGroupDef, CatalogParamDecl, CatalogRowTemplate, CreateRCatalogPayload } from "../src/rcatalog/payload.js";
import { CatalogIndex } from "../src/rcatalog/resolve.js";
import { RDeployGateImpl, rDeployGateFactory } from "../src/rdeploy_gate/rdeploy_gate.js";
import { RDbImpl, rDbFactory } from "../src/rdb/rdb.js";
import type { CreateRDbPayload, ParamValue } from "../src/rdb/payload.js";
import { resolveRDb } from "../src/rdb/resolve.js";
import type { MemberFiles, Membership } from "../src/rdb/instantiate.js";
import { instantiateGroup, deriveGroupSeed, groupIdOf } from "../src/rdb/instantiate.js";
import { deployCatalogRelease } from "../src/rdb/catalog_update.js";
import { rBlobStoreFactory } from "../src/rblob_store/rblob_store.js";
import { RBLOB_STORE_TYPE_ID } from "../src/rblob_store/payload.js";
import { rFileMapFactory } from "../src/rfile_map/rfile_map.js";
import { RFILE_MAP_TYPE_ID } from "../src/rfile_map/payload.js";

export function registerCatalogTypes(ctx: RContext): void {
    const registry = ctx.getRegistry();
    registry.register(RSchemaImpl.typeId, rSchemaFactory);
    registry.register(RCatalogImpl.typeId, rCatalogFactory);
    registry.register(RTableGroupImpl.typeId, rTableGroupFactory);
    registry.register(RDeployGateImpl.typeId, rDeployGateFactory);
    registry.register(RDbImpl.typeId, rDbFactory);
    registry.register(RBLOB_STORE_TYPE_ID, rBlobStoreFactory);
    registry.register(RFILE_MAP_TYPE_ID, rFileMapFactory);
}

export async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, sha256);
}

export function openTable(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

export function rootIdOf(payload: object): B64Hash {
    return dag.createEntry(payload as unknown as json.Literal, {}, position(), sha256).hash;
}

export type FixtureGroup = {
    name: string;
    tables?: TableDef[];                      // a fresh schema for this group
    schemaOf?: string;                        // or reuse the schema of another fixture group
    pin?: Version;                            // the pinned schema version (default: the schema genesis)
    bindings?: { [alias: string]: string };   // alias -> fixture group name
    idProvider?: string;
    canDeploy?: Predicate;
    canObserve?: { [alias: string]: Predicate };
    initialRows?: { [table: string]: CatalogRowTemplate[] };
};

export type FixtureFiles = {
    name: string;
    group: string;                            // the bound fixture group
    alias?: string;                           // defaults to the group name
    idProvider: string;
    canWrite: Predicate;
};

export type FixtureSpec = {
    dev: OwnIdentity;
    name?: string;
    version?: string;
    groups: FixtureGroup[];
    files?: FixtureFiles[];
    params?: CatalogParamDecl[];
    note?: string;
};

export type BuiltCatalog = {
    dev: OwnIdentity;
    name: string;
    schemaPayloads: Map<string, CreateRSchemaPayload>;   // by schema-owning group name
    schemaIds: Map<string, B64Hash>;                     // by group name
    defs: Map<string, CatalogGroupDef>;
    hashes: Map<string, B64Hash>;
    filesDefs: Map<string, CatalogFilesDef>;             // by FILES name
    catalogPayload: CreateRCatalogPayload;
    catalogId: B64Hash;
};

function defOf(group: FixtureGroup, schemaId: B64Hash, pin: Version, hashes: Map<string, B64Hash>): CatalogGroupDef {
    const def: CatalogGroupDef = {
        name: group.name,
        seedSource: 'rdb',
        schemaRef: schemaId,
        schemaVersion: json.toSet([...pin]),
    };
    if (group.bindings !== undefined) {
        const bindings: { [alias: string]: B64Hash } = {};
        for (const [alias, target] of Object.entries(group.bindings)) {
            const hash = hashes.get(target);
            if (hash === undefined) throw new Error(`fixture group '${group.name}' binds '${target}', which is not defined before it`);
            bindings[alias] = hash;
        }
        def.bindings = bindings;
    }
    if (group.idProvider !== undefined) def.idProvider = group.idProvider;
    if (group.canDeploy !== undefined) def.canDeploy = group.canDeploy;
    if (group.canObserve !== undefined) def.canObserve = group.canObserve;
    if (group.initialRows !== undefined) def.initialRows = group.initialRows;
    return def;
}

// Offline: the schema payloads and the catalog genesis (every group pinned
// at its schema's genesis).
export async function buildCatalog(spec: FixtureSpec): Promise<BuiltCatalog> {
    const name = spec.name ?? 'fixture';
    const creators = [{ keyId: spec.dev.keyId, publicKey: spec.dev.publicKey }];
    const schemaPayloads = new Map<string, CreateRSchemaPayload>();
    const schemaIds = new Map<string, B64Hash>();
    const defs = new Map<string, CatalogGroupDef>();
    const hashes = new Map<string, B64Hash>();

    for (const group of spec.groups) {
        let schemaId: B64Hash;
        if (group.schemaOf !== undefined) {
            const reused = schemaIds.get(group.schemaOf);
            if (reused === undefined) throw new Error(`fixture group '${group.name}' reuses the schema of unknown '${group.schemaOf}'`);
            schemaId = reused;
        } else {
            const payload = await RSchemaImpl.create({ name: `${name}:${group.name}`, creators, tables: group.tables ?? [] });
            schemaPayloads.set(group.name, payload);
            schemaId = rootIdOf(payload);
        }
        schemaIds.set(group.name, schemaId);
        const def = defOf(group, schemaId, group.pin ?? version(schemaId), hashes);
        defs.set(group.name, def);
        hashes.set(group.name, catalogGroupHash(def));
    }

    const filesDefs = new Map<string, CatalogFilesDef>();
    for (const files of spec.files ?? []) {
        const target = hashes.get(files.group);
        if (target === undefined) throw new Error(`fixture FILES '${files.name}' binds unknown group '${files.group}'`);
        filesDefs.set(files.name, {
            name: files.name,
            bindings: { [files.alias ?? files.group]: target },
            idProvider: files.idProvider,
            canWrite: files.canWrite,
        });
    }

    const catalogPayload = await RCatalogImpl.create({
        name,
        creators,
        author: spec.dev,
        version: spec.version ?? '1.0.0',
        add: [...defs.values()],
        files: [...filesDefs.values()],
        params: spec.params,
        note: spec.note,
    });

    return { dev: spec.dev, name, schemaPayloads, schemaIds, defs, hashes, filesDefs, catalogPayload, catalogId: rootIdOf(catalogPayload) };
}

export type BuiltDatabase = {
    rdbPayload: CreateRDbPayload;
    rdbId: B64Hash;
    membership: Membership;
    groupIds: Map<string, B64Hash>;
    files: Map<string, MemberFiles>;   // by FILES name
};

export async function buildDatabase(catalog: BuiltCatalog, opts: {
    seed: string;
    name?: string;
    creators?: OwnIdentity[];
    params?: { [name: string]: ParamValue };
}): Promise<BuiltDatabase> {
    const rdbPayload = await RDbImpl.create({
        seed: opts.seed,
        name: opts.name,
        creators: opts.creators?.map((c) => ({ keyId: c.keyId, publicKey: c.publicKey })),
        catalog: catalog.catalogId,
        release: catalog.catalogId,
        params: opts.params,
    });
    const rdbId = rootIdOf(rdbPayload);

    const index = new CatalogIndex([dag.createEntry(catalog.catalogPayload as unknown as json.Literal, {}, position(), sha256)], new Map());
    const resolution = resolveRDb({ rdbId, ops: { create: rdbPayload, createHash: rdbId, updates: [] }, catalog: index });
    if (resolution.membership === undefined) throw new Error(`fixture database does not resolve: ${JSON.stringify(resolution.unresolved)}`);

    const groupIds = new Map<string, B64Hash>();
    for (const [groupName, hash] of catalog.hashes) groupIds.set(groupName, resolution.membership.byHash.get(hash)!.id);
    const files = new Map<string, MemberFiles>();
    for (const member of resolution.membership.files.values()) files.set(member.name, member);
    return { rdbPayload, rdbId, membership: resolution.membership, groupIds, files };
}

// Offline: the id a later release's definition gets as a member of `db`.
export function offlineGroupId(db: BuiltDatabase, def: CatalogGroupDef, opts?: {
    bindingIds?: { [catalogGroupHash: string]: B64Hash };
    params?: { [name: string]: ParamValue };
}): B64Hash {
    const payload = instantiateGroup(def, {
        seed: deriveGroupSeed(db.rdbId, catalogGroupHash(def)),
        bindingIds: opts?.bindingIds ?? {},
        params: opts?.params ?? db.rdbPayload.params ?? {},
        creators: db.rdbPayload.creators ?? [],
    });
    return groupIdOf(payload);
}

export type LiveCatalog = {
    catalog: RCatalogImpl;
    schemas: Map<string, RSchemaImpl>;   // by group name
};

export async function materializeCatalog(ctx: RContext, built: BuiltCatalog): Promise<LiveCatalog> {
    const schemas = new Map<string, RSchemaImpl>();
    for (const [groupName, payload] of built.schemaPayloads) {
        schemas.set(groupName, (await ctx.createObject(payload)) as RSchemaImpl);
    }
    for (const [groupName, schemaId] of built.schemaIds) {
        if (!schemas.has(groupName)) schemas.set(groupName, (await ctx.getObject(schemaId)) as RSchemaImpl);
    }
    const catalog = (await ctx.createObject(built.catalogPayload)) as RCatalogImpl;
    return { catalog, schemas };
}

export type LiveDatabase = {
    rdb: RDbImpl;
    groups: Map<string, RTableGroupImpl>;   // by group name
};

// Creates the RDb and deploys its genesis release (creating the member
// groups and their gates).
export async function materializeDatabase(ctx: RContext, built: BuiltDatabase, author?: OwnIdentity): Promise<LiveDatabase> {
    const rdb = (await ctx.createObject(built.rdbPayload)) as RDbImpl;
    await deployCatalogRelease(rdb, { release: built.rdbPayload.release, author });
    const groups = new Map<string, RTableGroupImpl>();
    for (const [groupName, id] of built.groupIds) groups.set(groupName, (await ctx.getObject(id)) as RTableGroupImpl);
    return { rdb, groups };
}

// Everything at once, on one context.
export async function catalogDatabase(ctx: RContext, spec: FixtureSpec, db: {
    seed: string;
    name?: string;
    creators?: OwnIdentity[];
    params?: { [name: string]: ParamValue };
    author?: OwnIdentity;
}) {
    const built = await buildCatalog(spec);
    const builtDb = await buildDatabase(built, db);
    const live = await materializeCatalog(ctx, built);
    const liveDb = await materializeDatabase(ctx, builtDb, db.author);
    return { built, builtDb, ...live, ...liveDb };
}

// The discovery topics of a database: the RDb, its catalog, its schemas, its
// member groups and its FILES objects (plus any extra ids).
export function databaseTopics(catalog: BuiltCatalog, db: BuiltDatabase, extra: B64Hash[] = []): B64Hash[] {
    const files = [...db.files.values()].flatMap((f) => [f.storeId, f.mapId]);
    return [...new Set([db.rdbId, catalog.catalogId, ...catalog.schemaIds.values(), ...db.groupIds.values(), ...files, ...extra])];
}

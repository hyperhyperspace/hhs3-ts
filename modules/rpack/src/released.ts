// The released side of a catalog repository: every release file installed into
// one in-memory replica, the release DAG, and the merged state of a set of
// parents, which is where the next release starts.

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { version, type Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    compareSemver, filesSchemaReason,
    type CatalogFilesDef, type CatalogGroupDef, type CatalogGroupState, type CreateRCatalogPayload, type ParentFold,
    type RCatalogImpl, type RSchemaImpl, type RSchemaView, type SchemaCreator, type TableDef,
} from "@hyper-hyper-space/hhs3_rdb";
import type { Replica } from "@hyper-hyper-space/hhs3_replica";

import { releaseTag, type ReleaseFile } from "./format.js";
import { installRelease } from "./install.js";
import { filesModelOf, groupModelOf, type CatalogModel, type FilesModel, type GroupModel, type SchemaModel } from "./model.js";
import { memoryReplica } from "./verify.js";

export type ReleaseInfo = {
    hash: B64Hash;
    version: string;
    tag: string;
    // The release's name, as its file and build folder use it: editor-2.0.0-8b21d0aa.
    name: string;
    parents: B64Hash[];
    note?: string;
};

export class ReleaseSelectionError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ReleaseSelectionError';
    }
}

export type BaseSchema = {
    id: B64Hash;
    name: string;
    // The minimal cover of the parents' pins: where the release's update goes.
    at: B64Hash[];
    object: RSchemaImpl;
    view: RSchemaView;
    tables: Map<string, TableDef>;
    creators: SchemaCreator[];
    hashAlgorithm?: string;
};

export type BaseGroup = {
    hash: B64Hash;
    def: CatalogGroupDef;
    model: GroupModel;
    // The group's version in each parent that has it.
    pins: CatalogGroupState[];
};

export type BaseFiles = {
    hash: B64Hash;
    def: CatalogFilesDef;
    model: FilesModel;
    // Why the FILES no longer fits its group's schema here, which leaves it
    // read-only. Absent while it fits.
    readOnly?: string;
};

// The parents' merged state.
export type BaseState = {
    parents: ReleaseInfo[];
    catalogId: B64Hash;
    genesis: CreateRCatalogPayload;
    fold: ParentFold;
    schemas: Map<string, BaseSchema>;
    schemaById: Map<B64Hash, BaseSchema>;
    groups: Map<string, BaseGroup>;
    // Every FILES, read-only ones included.
    files: Map<string, BaseFiles>;
    // The catalog as target-catalog.sql describes it: without read-only FILES.
    model: CatalogModel;
    // Names that two different schemas, or two different definitions, share.
    clashes: string[];
};

export function sortReleases(list: ReleaseInfo[]): ReleaseInfo[] {
    return [...list].sort((a, b) => compareSemver(a.version, b.version) || (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0));
}

export function describeRelease(info: ReleaseInfo): string {
    return `${info.version} (${info.tag})`;
}

export class Released {
    private infos = new Map<B64Hash, ReleaseInfo>();

    private constructor(readonly ctx: Replica, private id: B64Hash | undefined, private name: string | undefined) {}

    get catalogId(): B64Hash | undefined {
        return this.id;
    }

    get catalogName(): string | undefined {
        return this.name;
    }

    // Installs every file; they must all be releases of one catalog, named
    // `name` when given.
    static async open(files: ReleaseFile[], name?: string): Promise<Released> {
        const ctx = memoryReplica();
        let catalogId: B64Hash | undefined;
        let catalogName: string | undefined;
        for (const file of files) {
            if (catalogId !== undefined && file.manifest.catalog !== catalogId) {
                throw new Error(`releases/ holds releases of two catalogs (${catalogName} #${catalogId.slice(0, 8)} and ${file.manifest.name} #${file.manifest.catalog.slice(0, 8)})`);
            }
            if (name !== undefined && file.manifest.name !== name) {
                throw new Error(`releases/ holds a release of catalog '${file.manifest.name}', not '${name}'`);
            }
            catalogId = file.manifest.catalog;
            catalogName = file.manifest.name;
            await installRelease(ctx, file);
        }
        const released = new Released(ctx, catalogId, catalogName);
        await released.refresh();
        return released;
    }

    async close(): Promise<void> {
        await this.ctx.destroy();
    }

    async catalog(): Promise<RCatalogImpl> {
        if (this.catalogId === undefined) throw new Error('there are no releases yet');
        return (await this.ctx.getObject(this.catalogId)) as unknown as RCatalogImpl;
    }

    // Rereads the release DAG, after a release was added to the replica (the
    // first one creates the catalog).
    async refresh(catalogId?: B64Hash): Promise<void> {
        if (catalogId !== undefined) this.id = catalogId;
        this.infos = new Map();
        if (this.id === undefined) return;
        const catalog = await this.catalog();
        const index = await catalog.getIndex();
        const name = index.getGenesis().name;
        this.name = name;
        for (const hash of index.releasesAt(await (await catalog.getScopedDag()).getFrontier())) {
            const state = index.releaseState(hash);
            const tag = releaseTag(hash);
            const info: ReleaseInfo = { hash, version: state.version, tag, name: `${name}-${state.version}-${tag}`, parents: state.parents };
            if (state.note !== undefined) info.note = state.note;
            this.infos.set(hash, info);
        }
    }

    releases(): ReleaseInfo[] {
        return sortReleases([...this.infos.values()]);
    }

    get(hash: B64Hash): ReleaseInfo | undefined {
        return this.infos.get(hash);
    }

    // A release by version, version-tag, file name or #hash.
    resolve(selector: string): ReleaseInfo {
        let text = selector.endsWith('.rpack') ? selector.slice(0, -'.rpack'.length) : selector;
        const slash = text.lastIndexOf('/');
        if (slash !== -1) text = text.slice(slash + 1);
        if (text.startsWith('#')) {
            const matches = this.releases().filter((r) => r.hash.startsWith(text.slice(1)));
            return this.single(selector, matches);
        }
        if (this.catalogName !== undefined && text.startsWith(`${this.catalogName}-`)) text = text.slice(this.catalogName.length + 1);
        const dash = text.indexOf('-');
        const wanted = dash === -1 ? text : text.slice(0, dash);
        const tag = dash === -1 ? '' : text.slice(dash + 1);
        return this.single(selector, this.releases().filter((r) => r.version === wanted && r.tag.startsWith(tag)));
    }

    private single(selector: string, matches: ReleaseInfo[]): ReleaseInfo {
        if (matches.length === 1) return matches[0];
        if (matches.length === 0) throw new ReleaseSelectionError(`no release matches '${selector}'`);
        throw new ReleaseSelectionError(`'${selector}' matches several releases: ${matches.map((r) => `${r.version}-${r.tag}`).join(', ')}; add the tag`);
    }

    // `a` strictly in `b`'s past.
    async isBelow(a: B64Hash, b: B64Hash): Promise<boolean> {
        return (await (await this.catalog()).getIndex()).isReleaseBelow(a, b);
    }

    // The releases with `hash` in their past, by version.
    async descendants(hash: B64Hash): Promise<ReleaseInfo[]> {
        const out: ReleaseInfo[] = [];
        for (const release of this.releases()) if (await this.isBelow(hash, release.hash)) out.push(release);
        return out;
    }

    private async ancestorsOf(hashes: B64Hash[]): Promise<Set<B64Hash>> {
        const out = new Set<B64Hash>();
        if (hashes.length === 0) return out;
        const index = await (await this.catalog()).getIndex();
        for (const hash of hashes) for (const a of index.releaseState(hash).ancestors) out.add(a);
        return out;
    }

    // The releases below `version` that no other one below it includes.
    async defaultParents(forVersion: string): Promise<ReleaseInfo[]> {
        const lower = this.releases().filter((r) => compareSemver(r.version, forVersion) < 0);
        if (lower.length === 0) return [];
        const below = await this.ancestorsOf(lower.map((r) => r.hash));
        return lower.filter((r) => !below.has(r.hash));
    }

    // The maximal ones among `hashes` (those not in another's past).
    async maximal(hashes: B64Hash[]): Promise<B64Hash[]> {
        const below = await this.ancestorsOf(hashes);
        return hashes.filter((h) => !below.has(h));
    }

    // The releases below `version` outside the past of `parents`.
    async outside(forVersion: string, parents: B64Hash[]): Promise<ReleaseInfo[]> {
        const past = await this.ancestorsOf(parents);
        for (const p of parents) past.add(p);
        return this.releases().filter((r) => compareSemver(r.version, forVersion) < 0 && !past.has(r.hash));
    }

    async base(parents: B64Hash[]): Promise<BaseState> {
        if (parents.length === 0) throw new Error('the first release has no base state');
        const catalog = await this.catalog();
        const index = await catalog.getIndex();
        const fold = index.foldParents(parents);
        const genesis = index.getGenesis();

        const pins = new Map<B64Hash, Set<B64Hash>>();
        for (const group of fold.groups.values()) {
            const set = pins.get(group.schema) ?? new Set<B64Hash>();
            for (const h of group.version) set.add(h);
            pins.set(group.schema, set);
        }
        for (const id of index.referencedSchemasAt(parents)) if (!pins.has(id)) pins.set(id, new Set([id]));

        const clashes: string[] = [];
        const schemas = new Map<string, BaseSchema>();
        const schemaById = new Map<B64Hash, BaseSchema>();
        for (const id of [...pins.keys()].sort()) {
            const object = (await this.ctx.getObject(id)) as unknown as RSchemaImpl;
            const cover = await (await object.getScopedDag()).findMinimalCover(version(...pins.get(id)!));
            const at: Version = version(...cover);
            const view = await object.getView(at, at);
            const tables = new Map<string, TableDef>();
            for (const name of view.getTableNames()) tables.set(name, view.getTable(name)!);
            const schema: BaseSchema = {
                id, name: object.getName(), at: [...cover].sort(), object, view, tables, creators: view.getCreators(),
            };
            const hashAlgorithm = object.hashAlgorithm();
            if (hashAlgorithm !== undefined) schema.hashAlgorithm = hashAlgorithm;
            if (schemas.has(schema.name)) clashes.push(`two schemas are named ${schema.name}`);
            else schemas.set(schema.name, schema);
            schemaById.set(id, schema);
        }

        const paramTypes = new Map<string, string>();
        for (const parent of parents) {
            for (const decl of index.releaseState(parent).params.values()) {
                const seen = paramTypes.get(decl.name);
                if (seen !== undefined && seen !== decl.type) clashes.push(`param :${decl.name} is ${seen} in one parent and ${decl.type} in another`);
                paramTypes.set(decl.name, decl.type);
            }
        }

        const groups = new Map<string, BaseGroup>();
        for (const [hash, def] of fold.defs) {
            if (groups.has(def.name)) { clashes.push(`two group definitions are named ${def.name}`); continue; }
            const model = groupModelOf(def, schemaById.get(def.schemaRef)!.name, (h) => fold.defs.get(h)!.name);
            const groupPins = parents.map((p) => index.releaseState(p).groups.get(hash)).filter((g): g is CatalogGroupState => g !== undefined);
            groups.set(def.name, { hash, def, model, pins: groupPins });
        }

        const files = new Map<string, BaseFiles>();
        for (const [hash, def] of fold.files) {
            if (files.has(def.name) || groups.has(def.name)) { clashes.push(`two definitions are named ${def.name}`); continue; }
            const entry: BaseFiles = { hash, def, model: filesModelOf(def, (h) => fold.defs.get(h)!.name) };
            const schema = schemaById.get(fold.defs.get(Object.values(def.bindings)[0])!.schemaRef)!;
            const readOnly = filesSchemaReason(def, schema.name, (t) => schema.tables.get(t));
            if (readOnly !== undefined) entry.readOnly = readOnly;
            files.set(def.name, entry);
        }

        const model: CatalogModel = {
            name: genesis.name,
            creators: genesis.creators,
            ...(genesis.seed !== undefined ? { seed: genesis.seed } : {}),
            ...(genesis.hashAlgorithm !== undefined ? { hashAlgorithm: genesis.hashAlgorithm } : {}),
            params: new Map(fold.params),
            schemas: new Map([...schemas].map(([name, s]) => [name, schemaModelOf(s)])),
            groups: new Map([...groups].map(([name, g]) => [name, g.model])),
            files: new Map([...files].filter(([, f]) => f.readOnly === undefined).map(([name, f]) => [name, f.model])),
        };

        const infos = parents.map((p) => this.infos.get(p)!);
        return { parents: sortReleases(infos), catalogId: this.catalogId!, genesis, fold, schemas, schemaById, groups, files, model, clashes };
    }
}

function schemaModelOf(schema: BaseSchema): SchemaModel {
    const model: SchemaModel = { name: schema.name, creators: schema.creators, tables: schema.tables };
    if (schema.hashAlgorithm !== undefined) model.hashAlgorithm = schema.hashAlgorithm;
    return model;
}

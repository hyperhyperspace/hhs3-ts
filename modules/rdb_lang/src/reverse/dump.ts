import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { Entry } from "@hyper-hyper-space/hhs3_dag";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { RefAdvancePayload } from "@hyper-hyper-space/hhs3_mvt";
import type { RCatalogImpl, RDbImpl, RDbResolution, RSchema, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import { versionKey } from "@hyper-hyper-space/hhs3_rdb";
import type { LoggableObject } from "../bind/context.js";
import { sortMemberGroupsByBindings } from "./planner.js";
import {
    isSchemaDeployPayload, renderIdent, renderOp, renderUseDatabase, type DumpRenderProfile, type RenderOptions,
} from "./render.js";

export type DumpOptions = {
    includeUnknown?: boolean;
    render?: RenderOptions;
};

export type DumpDatabaseMode = DumpRenderProfile;

export type DumpCatalogOptions = DumpOptions & {
    loadSchema: (id: B64Hash) => Promise<RSchema & LoggableObject>;
    // Dump the referenced schemas first (default true).
    includeSchemas?: boolean;
};

export type DumpDatabaseOptions = DumpOptions & {
    mode?: DumpDatabaseMode;
    loadSchema: (id: B64Hash) => Promise<RSchema & LoggableObject>;
    loadGroup: (id: B64Hash) => Promise<RTableGroup & LoggableObject>;
};

function renderStatement(payload: json.Literal, options?: RenderOptions): string {
    const sql = renderOp(payload, options);
    if (options?.aliasMode !== true || options.aliases === undefined) return sql;
    const defs = options.aliases.drainDefinitions();
    return defs.length > 0 ? `${defs.join('\n')}\n${sql}` : sql;
}

async function loadEntries(object: LoggableObject): Promise<Entry[]> {
    const entries: Entry[] = [];
    for await (const entry of (await object.getScopedDag()).loadAllEntries()) entries.push(entry);
    return entries;
}

function prevsOf(entry: Entry): B64Hash[] {
    return [...json.fromSet(entry.header.prevEntryHashes)] as B64Hash[];
}

export async function dumpObject(object: LoggableObject, options: DumpOptions = {}): Promise<string> {
    const statements: string[] = [];
    for (const entry of await loadEntries(object)) {
        const rendered = renderStatement(entry.payload, {
            at: entry.header.prevEntryHashes,
            ...options.render,
        });
        if (options.includeUnknown === false && rendered.startsWith('-- unknown payload')) continue;
        statements.push(rendered);
    }
    return statements.join('\n\n');
}

export async function dumpSchema(schema: RSchema & LoggableObject, options?: DumpOptions): Promise<string> {
    const name = schema.getName();
    return dumpObject(schema, {
        ...options,
        render: {
            ...options?.render,
            schemaRef: schema.getId(),
            schemaName: name,
            versionScope: { objectId: schema.getId(), objectName: name },
        },
    });
}

async function groupRenderOptions(group: RTableGroup, base?: RenderOptions): Promise<RenderOptions> {
    const groupId = group.getId();
    const groupName = group.getName();
    const schemaName = base?.resolveSchemaName?.(group.getSchemaRef()) ?? base?.schemaName ?? await loadedSchemaName(group);
    return {
        ...base,
        schemaRef: group.getSchemaRef(),
        ...(schemaName !== undefined ? { schemaName } : {}),
        groupRef: groupId,
        groupName,
        versionScope: { objectId: groupId, objectName: groupName },
    };
}

async function loadedSchemaName(group: RTableGroup): Promise<string | undefined> {
    const withSchema = group as RTableGroup & { getSchemaObject?: () => Promise<RSchema> };
    if (withSchema.getSchemaObject === undefined) return undefined;
    try {
        return (await withSchema.getSchemaObject()).getName();
    } catch {
        return undefined;
    }
}

export async function dumpGroup(group: RTableGroup & LoggableObject, options?: DumpOptions): Promise<string> {
    return dumpObject(group, { ...options, render: await groupRenderOptions(group, options?.render) });
}

export async function dumpGroupEntry(
    group: RTableGroup & LoggableObject,
    entryHash: B64Hash,
    options?: DumpOptions,
): Promise<string | undefined> {
    const entry = await (await group.getScopedDag()).loadEntry(entryHash);
    if (entry === undefined) return undefined;
    return renderStatement(entry.payload, { ...await groupRenderOptions(group, options?.render), at: entry.header.prevEntryHashes });
}

// The schemas a catalog references, in the order they entered it: the
// genesis pins, then each declare.
function referencedSchemaOrder(entries: Entry[], genesisHash: B64Hash): B64Hash[] {
    const out: B64Hash[] = [];
    const add = (id: B64Hash) => { if (!out.includes(id)) out.push(id); };
    for (const entry of entries) {
        const payload = entry.payload as json.LiteralMap;
        if (entry.hash === genesisHash) {
            for (const def of (payload['add'] ?? []) as { schemaRef: B64Hash }[]) add(def.schemaRef);
        } else if (payload['action'] === 'declare') {
            for (const id of json.fromSet(payload['schemas'] as json.Set)) add(id as B64Hash);
        }
    }
    return out;
}

// A catalog dump: the referenced schemas, CREATE CATALOG, then each release as
// ALTER CATALOG in topological order. Declares are implied by the releases
// that need them, so a release on a declare is rendered at the declare's own
// position. Replaying re-signs every entry, which takes the developer's keys.
export async function dumpCatalog(catalog: RCatalogImpl & LoggableObject, options: DumpCatalogOptions): Promise<string> {
    const index = await catalog.getIndex();
    const entries = await loadEntries(catalog);
    const byHash = new Map(entries.map((e) => [e.hash, e]));
    const genesisHash = index.getGenesisHash();
    const schemaIds = referencedSchemaOrder(entries, genesisHash);

    const sections: string[] = [];
    const schemaNames = new Map<B64Hash, string>();
    for (const id of schemaIds) {
        const schema = await options.loadSchema(id);
        schemaNames.set(id, schema.getName());
        if (options.includeSchemas !== false) sections.push(await dumpSchema(schema, { ...options, render: { ...options.render } }));
    }

    const catalogName = catalog.getName();
    const base: RenderOptions = {
        resolveSchemaName: (id) => schemaNames.get(id),
        ...options.render,
        catalogRef: catalog.getId(),
        catalogName,
        versionScope: { objectId: catalog.getId(), objectName: catalogName },
    };

    const expandDeclares = (prevs: B64Hash[]): B64Hash[] => {
        const out = new Set<B64Hash>();
        const pending = [...prevs];
        while (pending.length > 0) {
            const hash = pending.pop()!;
            if (index.isDeclare(hash)) pending.push(...prevsOf(byHash.get(hash)!));
            else out.add(hash);
        }
        return [...out].sort();
    };

    for (const entry of entries) {
        if (entry.hash === genesisHash) {
            sections.push(renderStatement(entry.payload, base));
        } else if (index.isRelease(entry.hash)) {
            const prevs = prevsOf(entry);
            sections.push(renderStatement(entry.payload, {
                ...base,
                at: json.toSet(expandDeclares(prevs)),
                catalogDefs: index.foldParents(index.parentsOf(prevs)).defs,
            }));
        }
    }
    return sections.filter((s) => s.length > 0).join('\n\n');
}

type GroupEntryKind = 'op' | 'deploy' | 'intermediate-deploy' | 'planner-observe';

// A database dump: its catalog, CREATE DATABASE, then (full profile) each
// member's history in rounds. The deploys of a release are made by the
// statement that deploys it (CREATE DATABASE or UPDATE CATALOG), so they are
// not rendered; round k holds the ops that depend on round k's deploys and
// follows its UPDATE CATALOG. The planner's own ref advances after a bound
// group's deploy are regenerated the same way and are skipped too. A deploy
// to a version no deployed release pins cannot be replayed and is rendered as
// a comment.
export async function dumpDatabase(db: RDbImpl & LoggableObject, options: DumpDatabaseOptions): Promise<string> {
    const profile: DumpRenderProfile = options.mode ?? 'full';
    const catalog = await db.getCatalog();
    if (catalog === undefined) throw new Error(`the catalog of database '${db.getName() ?? db.getId()}' is not loaded`);

    const sections: string[] = [];
    sections.push(await dumpCatalog(catalog as RCatalogImpl & LoggableObject, {
        loadSchema: options.loadSchema,
        render: { profile, ...options.render },
    }));

    const schemaNames = new Map<B64Hash, string>();
    for (const id of (await catalog.getIndex()).referencedSchemasAt(await (await catalog.getScopedDag()).getFrontier())) {
        schemaNames.set(id, (await options.loadSchema(id)).getName());
    }
    const groupIds = await sortMemberGroupsByBindings(await db.getMemberGroups(), options.loadGroup);
    const groups = new Map<B64Hash, RTableGroup & LoggableObject>();
    const groupNames = new Map<B64Hash, string>();
    for (const id of groupIds) {
        const group = await options.loadGroup(id);
        groups.set(id, group);
        groupNames.set(id, group.getName());
    }

    const databaseName = db.getName() ?? db.seed();
    const renderOpts: RenderOptions = {
        profile,
        databaseRef: db.getId(),
        databaseName,
        catalogRef: catalog.getId(),
        catalogName: catalog.getName(),
        resolveSchemaName: (id) => schemaNames.get(id),
        resolveGroupName: (id) => groupNames.get(id),
        ...options.render,
    };

    const dbEntries = await loadEntries(db);
    const genesis = dbEntries.find((e) => e.hash === db.getId());
    if (genesis === undefined) throw new Error('RDb genesis entry not found');
    const updates = dbEntries.filter((e) => e.hash !== db.getId());
    const dbScope = { objectId: db.getId(), objectName: databaseName };

    sections.push(renderStatement(genesis.payload, { ...renderOpts, versionScope: dbScope }));
    sections.push(renderUseDatabase(db.getId(), databaseName, renderOpts));

    const renderUpdate = (entry: Entry) => renderStatement(entry.payload, { ...renderOpts, versionScope: dbScope });
    if (profile !== 'full') {
        for (const entry of updates) sections.push(renderUpdate(entry));
        return sections.filter((s) => s.length > 0).join('\n\n');
    }

    // resolutions[k]: the database after its first k update-catalog ops.
    const resolutions: RDbResolution[] = [];
    for (let k = 0; k <= updates.length; k++) {
        resolutions.push(await db.resolve(version(genesis.hash, ...updates.slice(0, k).map((e) => e.hash))));
    }
    const targetKey = (k: number, groupId: B64Hash): string | undefined => {
        const member = resolutions[k].membership?.byId.get(groupId);
        return member === undefined ? undefined : versionKey(member.target);
    };

    const rounds: string[][] = updates.map(() => []);
    rounds.push([]);
    const roundOf = new Map<B64Hash, number>();
    const kindOf = new Map<B64Hash, GroupEntryKind>();
    const warnings: string[] = [];

    const memberNames = new Map([...(await db.getMemberGroupNames()).entries()].map(([name, id]) => [id, name]));
    for (const groupId of groupIds) {
        const group = groups.get(groupId)!;
        const groupRender: RenderOptions = {
            ...await groupRenderOptions(group, renderOpts),
            groupTarget: renderIdent(memberNames.get(groupId) ?? group.getName()),
        };
        // The group exists from the first deploy that makes it a member.
        const joined = resolutions.findIndex((r) => r.membership?.byId.has(groupId) === true);
        roundOf.set(groupId, Math.max(0, joined));
        for (const entry of await loadEntries(group)) {
            if (entry.hash === groupId) continue;
            const prevs = prevsOf(entry);
            let round = Math.max(0, ...prevs.map((p) => roundOf.get(p) ?? 0));
            let kind: GroupEntryKind = 'op';
            const payload = entry.payload as json.LiteralMap;

            if (isSchemaDeployPayload(entry.payload, group.getSchemaRef())) {
                const key = versionKey(json.fromSet((payload as unknown as RefAdvancePayload).refVersion));
                let matched: number | undefined;
                for (let k = round; k < resolutions.length && matched === undefined; k++) {
                    if (targetKey(k, groupId) === key) matched = k;
                }
                if (matched !== undefined) {
                    round = matched;
                    kind = 'deploy';
                } else {
                    kind = 'intermediate-deploy';
                    warnings.push(`-- warning: group '${group.getName()}' has a deploy (#${entry.hash}) to a version no deployed release pins; it is rendered as a comment and cannot be replayed`);
                }
            } else if (payload['action'] === 'ref-advance') {
                const refVersion = [...json.fromSet((payload as unknown as RefAdvancePayload).refVersion)] as B64Hash[];
                round = Math.max(round, ...refVersion.map((h) => roundOf.get(h) ?? 0));
                if (refVersion.some((h) => kindOf.get(h) === 'deploy')) kind = 'planner-observe';
            }

            roundOf.set(entry.hash, round);
            kindOf.set(entry.hash, kind);
            if (kind === 'op' || kind === 'intermediate-deploy') {
                rounds[round].push(renderStatement(entry.payload, { ...groupRender, at: entry.header.prevEntryHashes }));
            }
        }
    }

    sections.push(...warnings);
    sections.push(...rounds[0]);
    updates.forEach((entry, i) => {
        sections.push(renderUpdate(entry));
        sections.push(...rounds[i + 1]);
    });
    return sections.filter((s) => s.length > 0).join('\n\n');
}

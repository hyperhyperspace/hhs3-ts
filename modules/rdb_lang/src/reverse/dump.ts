import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { CreateRDbPayload, RDb, RSchema, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import type { LoggableObject } from "../bind/context.js";
import { sortMemberGroupsByBindings } from "./planner.js";
import { renderOp, type DumpRenderProfile, type RenderOptions } from "./render.js";

export type DumpOptions = {
    includeUnknown?: boolean;
    render?: RenderOptions;
};

export type DumpDatabaseMode = DumpRenderProfile;

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

export async function dumpObject(object: LoggableObject, options: DumpOptions = {}): Promise<string> {
    const statements: string[] = [];
    const dag = await object.getScopedDag();
    for await (const entry of dag.loadAllEntries()) {
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

export async function dumpGroup(group: RTableGroup & LoggableObject, options?: DumpOptions): Promise<string> {
    const groupId = group.getId();
    const groupName = group.getName();
    return dumpObject(group, {
        ...options,
        render: {
            ...options?.render,
            schemaRef: group.getSchemaRef(),
            groupRef: groupId,
            groupName,
            versionScope: { objectId: groupId, objectName: groupName },
        },
    });
}

export async function dumpDatabaseCreate(db: RDb & LoggableObject, options: DumpDatabaseOptions): Promise<string> {
    const dag = await db.getScopedDag();
    const entry = await dag.loadEntry(db.getId());
    if (entry === undefined) throw new Error('RDb genesis entry not found');
    const databaseName = databaseNameFromPayload(entry.payload);
    return renderStatement(entry.payload, {
        ...options.render,
        versionScope: { objectId: db.getId(), objectName: databaseName },
    });
}

export async function dumpDatabaseAddSchemas(db: RDb & LoggableObject, options: DumpDatabaseOptions): Promise<string> {
    return dumpRDbMembershipOps(db, 'add-schema', options);
}

export async function dumpDatabaseAddGroups(db: RDb & LoggableObject, options: DumpDatabaseOptions): Promise<string> {
    return dumpRDbMembershipOps(db, 'add-group', options);
}

async function dumpRDbMembershipOps(
    db: RDb & LoggableObject,
    action: 'add-schema' | 'add-group',
    options: DumpDatabaseOptions,
): Promise<string> {
    const statements: string[] = [];
    const dag = await db.getScopedDag();
    const genesis = await dag.loadEntry(db.getId());
    const databaseName = genesis === undefined ? 'database' : databaseNameFromPayload(genesis.payload);
    const versionScope = { objectId: db.getId(), objectName: databaseName };
    for await (const entry of dag.loadAllEntries()) {
        if (entry.hash === db.getId()) continue;
        const payload = entry.payload as json.LiteralMap;
        if (payload['action'] !== action) continue;
        statements.push(renderStatement(entry.payload, {
            at: entry.header.prevEntryHashes,
            versionScope,
            ...options.render,
        }));
    }
    return statements.join('\n\n');
}

export async function dumpGroupCreate(group: RTableGroup & LoggableObject, options?: DumpOptions): Promise<string> {
    const dag = await group.getScopedDag();
    const entry = await dag.loadEntry(group.getId());
    if (entry === undefined) throw new Error('Tablegroup genesis entry not found');
    const groupId = group.getId();
    const groupName = group.getName();
    return renderStatement(entry.payload, {
        ...options?.render,
        schemaRef: group.getSchemaRef(),
        groupRef: groupId,
        groupName,
        versionScope: { objectId: groupId, objectName: groupName },
        at: entry.header.prevEntryHashes,
    });
}

function databaseNameFromPayload(payload: json.Literal): string {
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return 'database';
    const create = payload as CreateRDbPayload;
    return create.name ?? create.seed;
}

export async function dumpDatabase(db: RDb & LoggableObject, options: DumpDatabaseOptions): Promise<string> {
    const profile: DumpRenderProfile = options.mode ?? 'full';
    const schemaIds = await db.getMemberSchemas();
    const groupIds = await sortMemberGroupsByBindings(await db.getMemberGroups(), options.loadGroup);

    const schemaNames = new Map<B64Hash, string>();
    for (const id of schemaIds) {
        const schema = await options.loadSchema(id);
        schemaNames.set(id, schema.getName());
    }
    const groupNames = new Map<B64Hash, string>();
    for (const id of groupIds) {
        const group = await options.loadGroup(id);
        groupNames.set(id, group.getName());
    }

    const dag = await db.getScopedDag();
    const genesis = await dag.loadEntry(db.getId());
    const databaseName = genesis === undefined ? 'database' : databaseNameFromPayload(genesis.payload);

    const renderOpts: RenderOptions = {
        profile,
        databaseName,
        resolveSchemaName: (id) => schemaNames.get(id),
        resolveGroupName: (id) => groupNames.get(id),
        ...options.render,
    };
    const dumpOpts: DumpDatabaseOptions = { ...options, render: renderOpts };

    const sections: string[] = [];
    sections.push(await dumpDatabaseCreate(db, dumpOpts));

    for (const schemaId of schemaIds) {
        const schema = await options.loadSchema(schemaId);
        const schemaName = schema.getName();
        sections.push(await dumpSchema(schema, {
            ...options,
            render: {
                ...renderOpts,
                schemaRef: schemaId,
                schemaName,
                versionScope: { objectId: schemaId, objectName: schemaName },
            },
        }));
    }

    const addSchemas = await dumpDatabaseAddSchemas(db, dumpOpts);
    if (addSchemas.length > 0) sections.push(addSchemas);

    for (const groupId of groupIds) {
        const group = await options.loadGroup(groupId);
        const schemaName = schemaNames.get(group.getSchemaRef());
        const groupName = group.getName();
        const groupRender: RenderOptions = {
            ...renderOpts,
            schemaRef: group.getSchemaRef(),
            schemaName,
            groupRef: groupId,
            groupName,
            versionScope: { objectId: groupId, objectName: groupName },
        };
        if (profile === 'full') {
            sections.push(await dumpGroup(group, { ...options, render: groupRender }));
        } else {
            sections.push(await dumpGroupCreate(group, { ...options, render: groupRender }));
        }
    }

    const addGroups = await dumpDatabaseAddGroups(db, dumpOpts);
    if (addGroups.length > 0) sections.push(addGroups);

    return sections.filter((s) => s.length > 0).join('\n\n');
}

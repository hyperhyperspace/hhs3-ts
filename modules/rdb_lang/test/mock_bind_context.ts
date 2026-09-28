import { B64Hash, createBasicCrypto, HASH_SHA256, KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RObject } from "@hyper-hyper-space/hhs3_mvt";
import type { RCatalogImpl, RDbImpl, RSchema, RTableGroup, RTableView } from "@hyper-hyper-space/hhs3_rdb";
import { splitTableRef } from "@hyper-hyper-space/hhs3_rdb";

import type {
    HashScope, LangBindContext, LangValue, ResolvedCatalogRef, ResolvedDatabaseRef, ResolvedGroupRef, ResolvedLogTarget,
    ResolvedSchemaRef, ResolvedTableRef, VersionScope,
} from "../src/bind/context.js";
import type { HashRef, NameOrHashRef, TableRef, VersionExpr } from "../src/syntax/ast.js";

type ScopedObject = RObject & { getScopedDag(): Promise<{ getFrontier(): Promise<Version>; loadAllEntries(): AsyncIterable<{ hash: B64Hash }> }> };

export type TestBindContext = LangBindContext & {
    registerSchema(name: string, schema: RSchema & ScopedObject): void;
    registerGroup(name: string, group: RTableGroup & ScopedObject): void;
    registerDatabase(name: string, db: RDbImpl & ScopedObject): void;
    registerCatalog(name: string, catalog: RCatalogImpl & ScopedObject): void;
    setCurrentDatabase(id: B64Hash | undefined): void;
    setCurrentGroup(id: B64Hash | undefined): void;
};

// Group names resolve like the runtime's: `db.group` exactly; a bare name in
// the current database when one is set, else uniquely across databases.
// Groups registered by name directly (tests that build groups by hand) are
// looked up first.
export function createTestBindContext(ctx: RContext, vars: { [name: string]: LangValue } = {}): TestBindContext {
    const crypto = createBasicCrypto();
    const hashSuite = crypto.hash(HASH_SHA256);
    const schemas = new Map<string, RSchema & ScopedObject>();
    const groups = new Map<string, RTableGroup & ScopedObject>();
    const dbs = new Map<string, RDbImpl & ScopedObject>();
    const catalogs = new Map<string, RCatalogImpl & ScopedObject>();
    let currentDatabase: B64Hash | undefined;
    let currentGroup: B64Hash | undefined;
    let nextUuid = 1;

    const rootIds = () => [
        ...[...schemas.values()].map((s) => s.getId()),
        ...[...groups.values()].map((g) => g.getId()),
        ...[...dbs.values()].map((d) => d.getId()),
        ...[...catalogs.values()].map((c) => c.getId()),
    ];

    const findByIdOrName = <T extends RObject>(map: Map<string, T>, ref: NameOrHashRef, what: string): T => {
        if (ref.kind === 'hash') {
            const matches = [...map.values()].filter((o) => o.getId().startsWith(ref.prefix));
            if (matches.length === 1) return matches[0];
            throw new Error(matches.length === 0 ? `Unknown ${what} '#${ref.prefix}'` : `Ambiguous ${what} '#${ref.prefix}'`);
        }
        const found = map.get(ref.text) ?? [...map.values()].find((o) => o.getId() === ref.text);
        if (found === undefined) throw new Error(`Unknown ${what} '${ref.text}'`);
        return found;
    };

    const loadGroup = async (id: B64Hash): Promise<RTableGroup & ScopedObject> => {
        const obj = await ctx.getObject(id);
        if (obj === undefined) throw new Error(`Group '${id}' is not loaded`);
        return obj as unknown as RTableGroup & ScopedObject;
    };

    const groupInDb = async (db: RDbImpl, name: string): Promise<B64Hash | undefined> =>
        (await db.getMemberGroupNames()).get(name);

    const resolveGroupRef = async (ref: NameOrHashRef): Promise<ResolvedGroupRef> => {
        if (ref.kind === 'hash') {
            const candidates = new Set(rootIds());
            for (const db of dbs.values()) for (const id of await db.getMemberGroups()) candidates.add(id);
            const matches = [...candidates].filter((id) => id.startsWith(ref.prefix));
            if (matches.length !== 1) throw new Error(matches.length === 0 ? `Unknown group '#${ref.prefix}'` : `Ambiguous group '#${ref.prefix}'`);
            return { id: matches[0], group: await loadGroup(matches[0]) };
        }
        if (ref.parts.length === 2) {
            const db = findByIdOrName(dbs, { kind: 'name', text: ref.parts[0], parts: [ref.parts[0]], span: ref.span }, 'database');
            const id = await groupInDb(db, ref.parts[1]);
            if (id === undefined) throw new Error(`Database '${ref.parts[0]}' has no group '${ref.parts[1]}'`);
            return { id, group: await loadGroup(id) };
        }
        const registered = groups.get(ref.text) ?? [...groups.values()].find((g) => g.getId() === ref.text);
        if (registered !== undefined) return { id: registered.getId(), group: registered };
        if ((await ctx.getObject(ref.text)) !== undefined) return { id: ref.text, group: await loadGroup(ref.text) };

        if (currentDatabase !== undefined) {
            const db = [...dbs.values()].find((d) => d.getId() === currentDatabase)!;
            const id = await groupInDb(db, ref.text);
            if (id === undefined) throw new Error(`Unknown group '${ref.text}' in the current database`);
            return { id, group: await loadGroup(id) };
        }
        const matches: B64Hash[] = [];
        for (const db of dbs.values()) {
            const id = await groupInDb(db, ref.text);
            if (id !== undefined) matches.push(id);
        }
        if (matches.length === 1) return { id: matches[0], group: await loadGroup(matches[0]) };
        throw new Error(matches.length === 0 ? `Unknown group '${ref.text}'` : `Ambiguous group '${ref.text}'`);
    };

    const scopeObject = (scope: VersionScope): ScopedObject | undefined => {
        if (scope.kind === 'schema') return scope.schema as ScopedObject | undefined;
        if (scope.kind === 'group') return scope.group as ScopedObject | undefined;
        if (scope.kind === 'table') return scope.table as ScopedObject | undefined;
        return scope.object as ScopedObject | undefined;
    };

    const entryHashes = async (object: ScopedObject | undefined): Promise<B64Hash[]> => {
        if (object === undefined) return rootIds();
        const hashes: B64Hash[] = [];
        for await (const entry of (await object.getScopedDag()).loadAllEntries()) hashes.push(entry.hash);
        return hashes;
    };

    const bindContext: TestBindContext = {
        registerSchema(name, schema) { schemas.set(name, schema); },
        registerGroup(name, group) { groups.set(name, group); },
        registerDatabase(name, db) { dbs.set(name, db); },
        registerCatalog(name, catalog) { catalogs.set(name, catalog); },
        setCurrentDatabase(id) { currentDatabase = id; },
        setCurrentGroup(id) { currentGroup = id; },

        async resolveSchema(ref: NameOrHashRef): Promise<ResolvedSchemaRef> {
            const schema = findByIdOrName(schemas, ref, 'schema');
            return { id: schema.getId(), schema };
        },

        resolveGroup: resolveGroupRef,

        async resolveDatabase(ref: NameOrHashRef): Promise<ResolvedDatabaseRef> {
            const db = findByIdOrName(dbs, ref, 'database');
            return { id: db.getId(), db };
        },

        async resolveCatalog(ref: NameOrHashRef): Promise<ResolvedCatalogRef> {
            const catalog = findByIdOrName(catalogs, ref, 'catalog');
            return { id: catalog.getId(), catalog };
        },

        async resolveDefaultDatabase(): Promise<ResolvedDatabaseRef | undefined> {
            if (currentDatabase === undefined) return undefined;
            const db = [...dbs.values()].find((d) => d.getId() === currentDatabase);
            return db === undefined ? undefined : { id: db.getId(), db };
        },

        async resolveDefaultGroup(): Promise<NameOrHashRef | undefined> {
            if (currentGroup === undefined) return undefined;
            return { kind: 'name', text: currentGroup, parts: [currentGroup], span: { start: 0, end: 0, line: 1, column: 1 } };
        },

        async resolveTable(ref: TableRef): Promise<ResolvedTableRef> {
            if (ref.group === undefined) throw new Error(`Table '${ref.table}' requires a group qualifier`);
            const groupRef: NameOrHashRef = ref.database !== undefined && ref.database.kind === 'name' && ref.group.kind === 'name'
                ? { kind: 'name', text: `${ref.database.text}.${ref.group.text}`, parts: [ref.database.text, ref.group.text], span: ref.span }
                : ref.group;
            const resolved = await resolveGroupRef(groupRef);
            const group = resolved.group!;
            const table = await group.getTable(ref.table);
            return { groupId: group.getId(), group, tableName: ref.table, table };
        },

        async resolveHash(ref: HashRef, scope: HashScope): Promise<B64Hash> {
            const candidates = scope.kind === 'object'
                ? await entryHashes(await ctx.getObject(scope.objectId) as ScopedObject | undefined)
                : rootIds();
            return resolveHashPrefix(ref.prefix, candidates);
        },

        async resolveRowId(ref, table, at, from) {
            const view = await table.table.getView(at, from ?? at);
            const tableName = `${table.groupId}.${table.tableName}`;
            return matchRowIdPrefix(ref.prefix, await view.liveRowIds(), tableName);
        },

        async resolveFkRowId(prefix, sourceTable, column, at, from) {
            const fromVersion = from ?? at;
            const groupView = await sourceTable.group.getView(at, fromVersion);
            const schemaView = groupView.getSchemaView();
            const targetRef = schemaView.getFKs(sourceTable.tableName)[column];
            if (targetRef === undefined) {
                throw new Error(`Column '${column}' is not a REFERENCES column`);
            }

            const [groupName, targetTable] = splitTableRef(targetRef);

            if (groupName === undefined) {
                const view = await groupView.getTableView(targetTable);
                const tableName = `${sourceTable.groupId}.${targetTable}`;
                return matchRowIdPrefix(prefix, await view.liveRowIds(), tableName);
            }

            const fkGroup = sourceTable.group as CrossGroupFkResolvable;
            const view = await fkGroup.resolveForeignTableView(groupName, targetTable, at, fromVersion);
            if (view === undefined) {
                throw new Error(`Unknown foreign table '${groupName}.${targetTable}' for FK column '${column}'`);
            }
            const tableName = `${sourceTable.groupId}.${groupName}.${targetTable}`;
            return matchRowIdPrefix(prefix, await view.liveRowIds(), tableName);
        },

        async resolveVersion(expr: VersionExpr | undefined, scope: VersionScope): Promise<Version> {
            const obj = scopeObject(scope);
            if (expr?.kind === 'set') {
                const candidates = await entryHashes(obj);
                return version(...expr.members.map((m) => m.kind === 'hash'
                    ? resolveHashPrefix(m.prefix, candidates)
                    : (() => { throw new Error(`Unknown version alias '${m.text}'`); })()));
            }
            if (expr?.kind === 'hash') return version(resolveHashPrefix(expr.hash.prefix, await entryHashes(obj)));
            return obj === undefined ? version() : await (await obj.getScopedDag()).getFrontier();
        },

        async resolveVariable(name: string): Promise<LangValue> {
            const v = vars[name];
            if (v === undefined) throw new Error(`Unknown variable '$${name}'`);
            return v;
        },

        async resolvePublicKey(labelOrPrefix: string): Promise<{ keyId: KeyId; publicKey: PublicKey }> {
            const normalized = labelOrPrefix.startsWith('#') ? labelOrPrefix.slice(1) : labelOrPrefix;

            const byName = vars[normalized];
            if (byName !== undefined) {
                const record = creatorRecordFrom(byName);
                if (record !== undefined) return record;
            }

            const records: { label: string; keyId: KeyId; publicKey: PublicKey }[] = [];
            const seenKeyIds = new Set<KeyId>();
            for (const [label, value] of Object.entries(vars)) {
                const record = creatorRecordFrom(value);
                if (record === undefined || seenKeyIds.has(record.keyId)) continue;
                seenKeyIds.add(record.keyId);
                records.push({ label, ...record });
            }

            const labelMatches = records.filter((record) => record.label === normalized);
            if (labelMatches.length === 1) return labelMatches[0];

            const keyMatches = records.filter((record) => record.keyId.startsWith(normalized));
            if (keyMatches.length === 1) return keyMatches[0];
            if (keyMatches.length === 0) throw new Error(`Unknown key '${labelOrPrefix}'`);
            throw new Error(`Ambiguous key prefix '${labelOrPrefix}'`);
        },

        async resolveLogTarget(ref: NameOrHashRef): Promise<ResolvedLogTarget> {
            if (ref.kind === 'name' && ref.parts.length === 2) {
                const readings: ResolvedLogTarget[] = [];
                try {
                    const group = (await resolveGroupRef({ kind: 'name', text: ref.parts[0], parts: [ref.parts[0]], span: ref.span })).group!;
                    const table = await group.getTable(ref.parts[1]);
                    readings.push({ kind: 'table', id: table.getId(), object: table as any, groupId: group.getId(), group: group as any, tableName: ref.parts[1] });
                } catch { /* not group.table */ }
                try {
                    const resolved = await resolveGroupRef(ref);
                    readings.push({ kind: 'group', id: resolved.id, object: resolved.group as any });
                } catch { /* not db.group */ }
                if (readings.length === 1) return readings[0];
                throw new Error(readings.length === 0 ? `Unknown LOG target '${ref.text}'` : `Ambiguous LOG target '${ref.text}'`);
            }
            const tryFind = <T extends RObject>(map: Map<string, T>): T | undefined => {
                try { return findByIdOrName(map, ref, 'object'); } catch { return undefined; }
            };
            const schema = tryFind(schemas);
            if (schema !== undefined) return { kind: 'schema', id: schema.getId(), object: schema as any };
            const catalog = tryFind(catalogs);
            if (catalog !== undefined) return { kind: 'catalog', id: catalog.getId(), object: catalog as any };
            const db = tryFind(dbs);
            if (db !== undefined) return { kind: 'database', id: db.getId(), object: db as any };
            const group = await resolveGroupRef(ref);
            return { kind: 'group', id: group.id, object: group.group as any };
        },

        async currentAuthor(): Promise<OwnIdentity | undefined> {
            const me = vars['me'];
            return typeof me === 'object' && me !== null && 'secretKey' in me ? me as OwnIdentity : undefined;
        },

        async resolveAuthor(ref): Promise<OwnIdentity> {
            const isIdentity = (v: LangValue | undefined): v is OwnIdentity =>
                typeof v === 'object' && v !== null && 'secretKey' in v;
            if (ref.kind === 'variable') {
                const v = vars[ref.name];
                if (isIdentity(v)) return v;
                throw new Error(`Unknown or locked identity '${ref.name}'`);
            }
            const matches = [...new Set(Object.values(vars).filter(isIdentity))].filter((v) => v.keyId.startsWith(ref.prefix));
            if (matches.length === 1) return matches[0];
            throw new Error(`Unknown or locked identity '#${ref.prefix}'`);
        },

        createUuid(): string {
            const uuid = `rdb-lang-test-${nextUuid}`;
            nextUuid += 1;
            return uuid;
        },

        createSeed(kind: 'rdb' | 'group', name?: string): string {
            return hashSuite.hashToB64(new TextEncoder().encode(`${kind}:${name ?? ''}`));
        },
    };

    return bindContext;
}

function creatorRecordFrom(value: LangValue): { keyId: KeyId; publicKey: PublicKey } | undefined {
    if (typeof value !== 'object' || value === null || ('kind' in value && value.kind === 'key-id')) {
        return undefined;
    }
    if (!('keyId' in value) || !('publicKey' in value)) return undefined;
    if (typeof value.keyId !== 'string' || value.publicKey === undefined) return undefined;
    return { keyId: value.keyId, publicKey: value.publicKey as PublicKey };
}

function resolveHashPrefix(prefix: string, hashes: B64Hash[]): B64Hash {
    const matches = [...new Set(hashes)].filter((h) => h.startsWith(prefix));
    if (matches.length === 1) return matches[0];
    if (matches.length === 0) throw new Error(`Unknown hash prefix '#${prefix}'`);
    throw new Error(`Ambiguous hash prefix '#${prefix}'`);
}

function matchRowIdPrefix(prefix: string, rowIds: B64Hash[], tableName: string): B64Hash {
    const matches = rowIds.filter((rowId) => rowId.startsWith(prefix));
    if (matches.length === 1) return matches[0];
    if (matches.length === 0) throw new Error(`Unknown rowId prefix '#${prefix}' in ${tableName}`);
    const examples = matches.slice(0, 5).map((rowId) => `#${rowId}`).join(', ');
    throw new Error(`Ambiguous rowId prefix '#${prefix}' in ${tableName}: ${examples}`);
}

type CrossGroupFkResolvable = RTableGroup & {
    resolveForeignTableView(
        groupName: string,
        table: string,
        at: Version,
        from: Version,
        filterVoided?: boolean,
    ): Promise<RTableView | undefined>;
};

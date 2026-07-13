import { B64Hash, createBasicCrypto, HASH_SHA256, KeyId, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RObject } from "@hyper-hyper-space/hhs3_mvt";
import type { RDb, RSchema, RTableGroup, RTableView } from "@hyper-hyper-space/hhs3_rdb";
import { splitTableRef } from "@hyper-hyper-space/hhs3_rdb";

import type {
    HashScope, LangBindContext, LangValue, ResolvedDatabaseRef, ResolvedGroupRef, ResolvedLogTarget,
    ResolvedSchemaRef, ResolvedTableRef, VersionScope,
} from "../src/bind/context.js";
import type { HashRef, NameOrHashRef, TableRef, VersionExpr } from "../src/syntax/ast.js";

type ScopedObject = RObject & { getScopedDag(): Promise<{ getFrontier(): Promise<Version> }> };

export type TestBindContext = LangBindContext & {
    registerSchema(name: string, schema: RSchema & ScopedObject): void;
    registerGroup(name: string, group: RTableGroup & ScopedObject): void;
    registerDatabase(name: string, db: RDb & ScopedObject): void;
};

export function createTestBindContext(_ctx: RContext, vars: { [name: string]: LangValue } = {}): TestBindContext {
    const crypto = createBasicCrypto();
    const hashSuite = crypto.hash(HASH_SHA256);
    const schemas = new Map<string, RSchema & ScopedObject>();
    const groups = new Map<string, RTableGroup & ScopedObject>();
    const dbs = new Map<string, RDb & ScopedObject>();
    let nextUuid = 1;

    const ids = () => [
        ...[...schemas.values()].map((s) => s.getId()),
        ...[...groups.values()].map((g) => g.getId()),
        ...[...dbs.values()].map((d) => d.getId()),
    ];

    const refText = (ref: NameOrHashRef) => ref.kind === 'name' ? ref.text : resolveHashPrefix(ref.prefix, ids());

    const bindContext: TestBindContext = {
        registerSchema(name, schema) { schemas.set(name, schema); },
        registerGroup(name, group) { groups.set(name, group); },
        registerDatabase(name, db) { dbs.set(name, db); },

        async resolveSchema(ref: NameOrHashRef): Promise<ResolvedSchemaRef> {
            const name = refText(ref);
            const schema = schemas.get(name) ?? [...schemas.values()].find((s) => s.getId() === name);
            if (schema === undefined) throw new Error(`Unknown schema '${name}'`);
            return { id: schema.getId(), schema };
        },

        async resolveGroup(ref: NameOrHashRef): Promise<ResolvedGroupRef> {
            const name = refText(ref);
            const group = groups.get(name) ?? [...groups.values()].find((g) => g.getId() === name);
            if (group === undefined) throw new Error(`Unknown group '${name}'`);
            return { id: group.getId(), group };
        },

        async resolveDatabase(ref: NameOrHashRef): Promise<ResolvedDatabaseRef> {
            const name = refText(ref);
            const db = dbs.get(name) ?? [...dbs.values()].find((d) => d.getId() === name);
            if (db === undefined) throw new Error(`Unknown database '${name}'`);
            return { id: db.getId(), db };
        },

        async resolveTable(ref: TableRef): Promise<ResolvedTableRef> {
            if (ref.group === undefined) throw new Error(`Table '${ref.table}' requires a group qualifier`);
            const group = (await this.resolveGroup(ref.group)).group;
            if (group === undefined) throw new Error(`Group '${ref.group.kind === 'name' ? ref.group.text : ref.group.prefix}' is not loaded`);
            const table = await group.getTable(ref.table);
            return { groupId: group.getId(), group, tableName: ref.table, table };
        },

        async resolveHash(ref: HashRef, _scope: HashScope): Promise<B64Hash> {
            return resolveHashPrefix(ref.prefix, ids());
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
            if (expr?.kind === 'set') {
                return version(...expr.members.map((m) => m.kind === 'hash'
                    ? resolveHashPrefix(m.prefix, ids())
                    : (() => { throw new Error(`Unknown version alias '${m.text}'`); })()));
            }
            if (expr?.kind === 'hash') return version(resolveHashPrefix(expr.hash.prefix, ids()));
            const obj = objectForVersion(scope);
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
            const name = refText(ref);
            if (name.includes('.')) {
                const parts = name.split('.');
                const group = groups.get(parts[0]);
                if (group === undefined) throw new Error(`Unknown group '${parts[0]}'`);
                const table = await group.getTable(parts[1]);
                return {
                    kind: 'table',
                    id: table.getId(),
                    object: table as any,
                    groupId: group.getId(),
                    group,
                    tableName: parts[1],
                };
            }
            const schema = schemas.get(name) ?? [...schemas.values()].find((s) => s.getId() === name);
            if (schema !== undefined) return { kind: 'schema', id: schema.getId(), object: schema };
            const group = groups.get(name) ?? [...groups.values()].find((g) => g.getId() === name);
            if (group !== undefined) return { kind: 'group', id: group.getId(), object: group };
            const db = dbs.get(name) ?? [...dbs.values()].find((d) => d.getId() === name);
            if (db !== undefined) return { kind: 'database', id: db.getId(), object: db };
            throw new Error(`Unknown LOG target '${name}'`);
        },

        async currentAuthor(): Promise<OwnIdentity | undefined> {
            const me = vars['me'];
            return typeof me === 'object' && me !== null && 'secretKey' in me ? me as OwnIdentity : undefined;
        },

        async resolveAuthor(ref): Promise<OwnIdentity> {
            const key = ref.kind === 'variable' ? ref.name : ref.prefix;
            const v = vars[key];
            if (typeof v === 'object' && v !== null && 'secretKey' in v) return v as OwnIdentity;
            throw new Error(`Unknown or locked identity '${key}'`);
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

function objectForVersion(scope: VersionScope): ScopedObject | undefined {
    if (scope.kind === 'schema') return scope.schema as ScopedObject | undefined;
    if (scope.kind === 'group') return scope.group as ScopedObject | undefined;
    if (scope.kind === 'table') return scope.table as ScopedObject | undefined;
    return scope.object as ScopedObject | undefined;
}

function resolveHashPrefix(prefix: string, hashes: B64Hash[]): B64Hash {
    const matches = hashes.filter((h) => h.startsWith(prefix));
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

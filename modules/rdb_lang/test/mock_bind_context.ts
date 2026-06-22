import { B64Hash, createBasicCrypto, HASH_SHA256, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import type { RContext, Version } from "@hyper-hyper-space/hhs3_mvt";
import type { RObject } from "@hyper-hyper-space/hhs3_mvt";
import type { RDb, RSchema, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";

import type {
    HashScope, LangBindContext, LangValue, ResolvedDatabaseRef, ResolvedGroupRef, ResolvedLogTarget,
    ResolvedSchemaRef, ResolvedTableRef, VersionScope,
} from "../src/bind/context.js";
import type { NameOrHashRef, TableRef, VersionExpr } from "../src/syntax/ast.js";

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

        async resolveHash(ref: NameOrHashRef, _scope: HashScope): Promise<B64Hash> {
            if (ref.kind === 'name') return ref.text;
            return resolveHashPrefix(ref.prefix, ids());
        },

        async resolveVersion(expr: VersionExpr | undefined, scope: VersionScope): Promise<Version> {
            if (expr?.kind === 'set') return version(...expr.hashes.map((h) => resolveHashPrefix(h.prefix, ids())));
            if (expr?.kind === 'hash') return version(resolveHashPrefix(expr.hash.prefix, ids()));
            const obj = objectForVersion(scope);
            return obj === undefined ? version() : await (await obj.getScopedDag()).getFrontier();
        },

        async resolveVariable(name: string): Promise<LangValue> {
            const v = vars[name];
            if (v === undefined) throw new Error(`Unknown variable '$${name}'`);
            return v;
        },

        async resolveLogTarget(ref: NameOrHashRef): Promise<ResolvedLogTarget> {
            const name = refText(ref);
            if (name.includes('.')) {
                const parts = name.split('.');
                const group = groups.get(parts[0]);
                if (group === undefined) throw new Error(`Unknown group '${parts[0]}'`);
                const table = await group.getTable(parts[1]);
                return { kind: 'table', id: table.getId(), object: table as any, groupId: group.getId(), tableName: parts[1] };
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

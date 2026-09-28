import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RObject } from "@hyper-hyper-space/hhs3_mvt";
import type { RCatalogImpl, RDbImpl, RSchema, RTable, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import {
    RCATALOG_TYPE_ID, RDB_TYPE_ID, RDEPLOY_GATE_TYPE_ID, RSCHEMA_TYPE_ID, RTABLE_GROUP_TYPE_ID,
} from "@hyper-hyper-space/hhs3_rdb";
import type {
    HashRef,
    HashScope,
    NameOrHashRef,
    ResolvedCatalogRef,
    ResolvedDatabaseRef,
    ResolvedGroupRef,
    ResolvedLogTarget,
    ResolvedSchemaRef,
    ResolvedTableRef,
    TableRef,
} from "@hyper-hyper-space/hhs3_rdb_lang";

import type { AliasScope } from "../session/aliases.js";

export type RootKind = 'database' | 'catalog' | 'schema' | 'group' | 'other';

export type RootRecord = {
    id: B64Hash;
    type: string;
    kind: RootKind;
    name?: string;
    object?: RObject;
};

export type AliasLookup = {
    get(scope: AliasScope, name: string): B64Hash | undefined;
};

export type RootResolveContext = {
    aliases?: AliasLookup;
    // Bare group names resolve only inside this database when set.
    currentDatabase?: B64Hash;
};

// RDeployGates are replica-local bookkeeping of their groups, never roots a
// user addresses.
function isHiddenType(type: string): boolean {
    return type === RDEPLOY_GATE_TYPE_ID;
}

export class RootIndex {
    private readonly roots = new Map<B64Hash, RootRecord>();

    upsert(record: RootRecord): void {
        if (isHiddenType(record.type)) return;
        const existing = this.roots.get(record.id);
        this.roots.set(record.id, { ...existing, ...record });
    }

    registerObject(id: B64Hash, object: RObject, name?: string): void {
        const existing = this.roots.get(id);
        this.upsert({
            id,
            type: object.getType(),
            kind: kindFromType(object.getType()),
            name: name ?? existing?.name,
            object,
        });
    }

    list(kind?: RootKind): RootRecord[] {
        const roots = [...this.roots.values()];
        return kind === undefined ? roots : roots.filter((root) => root.kind === kind);
    }

    get(id: B64Hash): RootRecord | undefined {
        return this.roots.get(id);
    }

    async resolveSchema(ref: NameOrHashRef, ctx: RootResolveContext = {}): Promise<ResolvedSchemaRef> {
        const root = await this.resolveRoot(ref, ['schema'], ctx);
        return { id: root.id, schema: root.object as RSchema | undefined };
    }

    async resolveCatalog(ref: NameOrHashRef, ctx: RootResolveContext = {}): Promise<ResolvedCatalogRef> {
        const root = await this.resolveRoot(ref, ['catalog'], ctx);
        return { id: root.id, catalog: root.object as RCatalogImpl | undefined };
    }

    async resolveDatabase(ref: NameOrHashRef, ctx: RootResolveContext = {}): Promise<ResolvedDatabaseRef> {
        const root = await this.resolveRoot(ref, ['database'], ctx);
        return { id: root.id, db: root.object as RDbImpl | undefined };
    }

    // `db.group` resolves exactly; a bare name resolves in the current
    // database when one is set (no fallback), otherwise when exactly one
    // database has a group by that name. Hashes, aliases and full ids work as
    // for every root.
    async resolveGroup(ref: NameOrHashRef, ctx: RootResolveContext = {}): Promise<ResolvedGroupRef> {
        const id = await this.resolveGroupId(ref, ctx);
        return { id, group: this.roots.get(id)?.object as RTableGroup | undefined };
    }

    private async resolveGroupId(ref: NameOrHashRef, ctx: RootResolveContext): Promise<B64Hash> {
        if (ref.kind === 'hash') return (await this.resolveRoot(ref, ['group'], ctx)).id;

        const aliased = ctx.aliases?.get('group', ref.text);
        if (aliased !== undefined) return aliased;

        if (ref.parts.length === 2) {
            const [dbName, groupName] = ref.parts as [string, string];
            const db = await this.resolveDatabase({ kind: 'name', text: dbName, parts: [dbName], span: ref.span }, ctx);
            const id = db.db === undefined ? undefined : (await db.db.getMemberGroupNames()).get(groupName);
            if (id === undefined) throw new Error(`Database '${dbName}' has no group '${groupName}'`);
            return id;
        }
        if (ref.parts.length !== 1) throw new Error(`Expected group or db.group, got '${ref.text}'`);

        const direct = this.roots.get(ref.text);
        if (direct !== undefined && direct.kind === 'group') return ref.text;

        const found = await this.groupsNamed(ref.text);
        if (ctx.currentDatabase !== undefined) {
            const here = found.find((f) => f.db.id === ctx.currentDatabase);
            if (here !== undefined) return here.groupId;
            const current = this.roots.get(ctx.currentDatabase);
            const where = current?.name ?? ctx.currentDatabase;
            const elsewhere = found.map((f) => `${f.db.name ?? `#${f.db.id.slice(0, 8)}`}.${ref.text}`);
            throw new Error(elsewhere.length > 0
                ? `Unknown group '${ref.text}' in database '${where}'; use ${elsewhere.join(' or ')}`
                : `Unknown group '${ref.text}' in database '${where}'`);
        }
        if (found.length === 1) return found[0].groupId;
        if (found.length === 0) throw new Error(`Unknown group '${ref.text}'`);
        const candidates = found.map((f) => `${f.db.name ?? `#${f.db.id.slice(0, 8)}`}.${ref.text}`);
        throw new Error(`Ambiguous group '${ref.text}': ${candidates.join(', ')} (use db.group or USE DATABASE)`);
    }

    private async groupsNamed(name: string): Promise<{ db: RootRecord; groupId: B64Hash }[]> {
        const out: { db: RootRecord; groupId: B64Hash }[] = [];
        for (const root of this.list('database')) {
            if (root.object === undefined) continue;
            const id = (await (root.object as RDbImpl).getMemberGroupNames()).get(name);
            if (id !== undefined) out.push({ db: root, groupId: id });
        }
        return out;
    }

    // The member name of a group in the database that has it (for display).
    async memberName(groupId: B64Hash): Promise<{ database: RootRecord; name: string } | undefined> {
        for (const root of this.list('database')) {
            if (root.object === undefined) continue;
            for (const [name, id] of await (root.object as RDbImpl).getMemberGroupNames()) {
                if (id === groupId) return { database: root, name };
            }
        }
        return undefined;
    }

    async resolveTable(ref: TableRef, ctx: RootResolveContext = {}): Promise<ResolvedTableRef> {
        if (ref.group === undefined) throw new Error(`Table '${ref.table}' requires a group qualifier`);
        let groupRef = ref.group;
        if (ref.database !== undefined) {
            if (ref.database.kind !== 'name' || ref.group.kind !== 'name') {
                throw new Error('db.group.table takes names, not hashes');
            }
            groupRef = {
                kind: 'name',
                text: `${ref.database.text}.${ref.group.text}`,
                parts: [ref.database.text, ref.group.text],
                span: ref.span,
            };
        }
        const group = await this.resolveGroup(groupRef, ctx);
        if (group.group === undefined) throw new Error(`Group '${refText(groupRef)}' is not loaded`);
        const table = await group.group.getTable(ref.table);
        return { groupId: group.id, group: group.group, tableName: ref.table, table };
    }

    // `a.b` is group.table or db.group; it resolves when exactly one reading
    // does.
    async resolveLogTarget(ref: NameOrHashRef, ctx: RootResolveContext = {}): Promise<ResolvedLogTarget> {
        if (ref.kind === 'name' && ref.parts.length >= 2) {
            const readings: ResolvedLogTarget[] = [];
            const errors: string[] = [];
            const tableRef: TableRef = ref.parts.length === 3
                ? {
                    database: { kind: 'name', text: ref.parts[0]!, parts: [ref.parts[0]!], span: ref.span },
                    group: { kind: 'name', text: ref.parts[1]!, parts: [ref.parts[1]!], span: ref.span },
                    table: ref.parts[2]!,
                    span: ref.span,
                }
                : {
                    group: { kind: 'name', text: ref.parts[0]!, parts: [ref.parts[0]!], span: ref.span },
                    table: ref.parts[1]!,
                    span: ref.span,
                };
            try {
                const table = await this.resolveTable(tableRef, ctx);
                if ((await table.group.getView()).getSchemaView().getTable(table.tableName) === undefined) {
                    throw new Error(`Unknown table '${table.tableName}'`);
                }
                readings.push({
                    kind: 'table',
                    id: table.table.getId(),
                    object: table.table as RTable & { getScopedDag(): ReturnType<RTable['getScopedDag']> },
                    groupId: table.groupId,
                    group: table.group as RTableGroup & { getScopedDag(): ReturnType<RTableGroup['getScopedDag']> },
                    tableName: table.tableName,
                });
            } catch (e) {
                errors.push(e instanceof Error ? e.message : String(e));
            }
            if (ref.parts.length === 2) {
                try {
                    const group = await this.resolveGroup(ref, ctx);
                    if (group.group === undefined) throw new Error(`Group '${ref.text}' is not loaded`);
                    readings.push({ kind: 'group', id: group.id, object: group.group as RTableGroup & ResolvedLogTarget['object'] });
                } catch (e) {
                    errors.push(e instanceof Error ? e.message : String(e));
                }
            }
            if (readings.length === 1) return readings[0]!;
            if (readings.length > 1) throw new Error(`Ambiguous LOG target '${ref.text}': it names both a group.table and a db.group`);
            throw new Error(errors.join('; '));
        }

        const root = await this.resolveLogRoot(ref, ctx);
        if (root.object === undefined) throw new Error(`Root '${root.id}' is not loaded`);
        if (root.kind === 'database') return { kind: 'database', id: root.id, object: root.object as RDbImpl & ResolvedLogTarget['object'] };
        if (root.kind === 'catalog') return { kind: 'catalog', id: root.id, object: root.object as RCatalogImpl & ResolvedLogTarget['object'] };
        if (root.kind === 'schema') return { kind: 'schema', id: root.id, object: root.object as RSchema & ResolvedLogTarget['object'] };
        return { kind: 'group', id: root.id, object: root.object as RTableGroup & ResolvedLogTarget['object'] };
    }

    // Databases, catalogs and schemas by name; otherwise a group by the group
    // rules.
    private async resolveLogRoot(ref: NameOrHashRef, ctx: RootResolveContext): Promise<RootRecord> {
        try {
            return await this.resolveRoot(ref, ['database', 'catalog', 'schema'], ctx);
        } catch (e) {
            if (ref.kind === 'hash') return this.resolveRoot(ref, ['database', 'catalog', 'schema', 'group'], ctx);
            try {
                const id = await this.resolveGroupId(ref, ctx);
                const root = this.roots.get(id);
                if (root === undefined) throw new Error(`Group '${ref.text}' is not loaded`);
                return root;
            } catch {
                throw e;
            }
        }
    }

    async resolveHash(ref: HashRef, scope: HashScope): Promise<B64Hash> {
        const candidates = await this.hashCandidates(scope);
        const matches = candidates.filter((hash) => hash.startsWith(ref.prefix));
        if (matches.length === 1) return matches[0]!;
        if (matches.length === 0) throw new Error(`Unknown hash prefix '#${ref.prefix}'`);
        throw new Error(`Ambiguous hash prefix '#${ref.prefix}'`);
    }

    private async resolveRoot(ref: NameOrHashRef, kinds: RootKind[], ctx: RootResolveContext): Promise<RootRecord> {
        const id = await this.resolveRootId(ref, kinds, ctx);
        const root = this.roots.get(id);
        if (root === undefined) throw new Error(`Unknown root '${refText(ref)}'`);
        if (!kinds.includes(root.kind)) {
            throw new Error(`Root '${refText(ref)}' is a ${root.kind}, expected ${kinds.join(' or ')}`);
        }
        return root;
    }

    private async resolveRootId(ref: NameOrHashRef, kinds: RootKind[], ctx: RootResolveContext): Promise<B64Hash> {
        if (ref.kind === 'name') {
            for (const kind of kinds) {
                const aliasScope = rootKindToAliasScope(kind);
                const aliased = ctx.aliases?.get(aliasScope, ref.text);
                if (aliased !== undefined) return aliased;
            }
            const matches = [...this.roots.values()].filter((root) => kinds.includes(root.kind) && root.name === ref.text);
            if (matches.length === 1) return matches[0]!.id;
            if (matches.length > 1) throw new Error(`Ambiguous root name '${ref.text}'`);
            if (this.roots.has(ref.text)) {
                const root = this.roots.get(ref.text)!;
                if (kinds.includes(root.kind)) return ref.text;
            }
            throw new Error(`Unknown ${kinds.join(' or ')} '${ref.text}'`);
        }
        const matches = [...this.roots.values()]
            .filter((root) => kinds.includes(root.kind) && root.id.startsWith(ref.prefix))
            .map((root) => root.id);
        if (matches.length === 1) return matches[0]!;
        if (matches.length === 0) return this.resolveHash(ref, { kind: 'global' });
        throw new Error(`Ambiguous hash prefix '#${ref.prefix}'`);
    }

    async hashCandidates(scope: HashScope): Promise<B64Hash[]> {
        if (scope.kind === 'global') return [...this.roots.keys()];

        const root = this.roots.get(scope.objectId);
        const object = root?.object;
        if (object === undefined) return [...this.roots.keys()];

        const dag = await object.getScopedDag();
        const hashes: B64Hash[] = [];
        for await (const entry of dag.loadAllEntries()) hashes.push(entry.hash);
        return hashes;
    }
}

export function kindFromType(type: string): RootKind {
    if (type === RDB_TYPE_ID) return 'database';
    if (type === RCATALOG_TYPE_ID) return 'catalog';
    if (type === RSCHEMA_TYPE_ID) return 'schema';
    if (type === RTABLE_GROUP_TYPE_ID) return 'group';
    return 'other';
}

function rootKindToAliasScope(kind: RootKind): AliasScope {
    if (kind === 'database') return 'db';
    if (kind === 'catalog') return 'catalog';
    if (kind === 'schema') return 'schema';
    if (kind === 'group') return 'group';
    throw new Error(`Root kind '${kind}' is not aliasable`);
}

function refText(ref: NameOrHashRef): string {
    return ref.kind === 'name' ? ref.text : `#${ref.prefix}`;
}

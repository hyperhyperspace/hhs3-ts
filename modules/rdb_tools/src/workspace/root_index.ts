import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RObject } from "@hyper-hyper-space/hhs3_mvt";
import type { RDb, RSchema, RTable, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import { RDB_TYPE_ID, RSCHEMA_TYPE_ID, RTABLE_GROUP_TYPE_ID } from "@hyper-hyper-space/hhs3_rdb";
import type {
    HashRef,
    HashScope,
    NameOrHashRef,
    ResolvedGroupRef,
    ResolvedLogTarget,
    ResolvedSchemaRef,
    ResolvedTableRef,
    TableRef,
} from "@hyper-hyper-space/hhs3_rdb_lang";

import type { AliasScope, AliasTable } from "../session/aliases.js";

export type RootKind = 'database' | 'schema' | 'group' | 'other';

export type RootRecord = {
    id: B64Hash;
    type: string;
    kind: RootKind;
    name?: string;
    object?: RObject;
};

export type RootResolveContext = {
    aliases?: AliasTable;
};

export class RootIndex {
    private readonly roots = new Map<B64Hash, RootRecord>();

    upsert(record: RootRecord): void {
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

    async resolveGroup(ref: NameOrHashRef, ctx: RootResolveContext = {}): Promise<ResolvedGroupRef> {
        const root = await this.resolveRoot(ref, ['group'], ctx);
        return { id: root.id, group: root.object as RTableGroup | undefined };
    }

    async resolveDatabase(ref: NameOrHashRef, ctx: RootResolveContext = {}): Promise<{ id: B64Hash; db?: RDb }> {
        const root = await this.resolveRoot(ref, ['database'], ctx);
        return { id: root.id, db: root.object as RDb | undefined };
    }

    async resolveTable(ref: TableRef, ctx: RootResolveContext = {}): Promise<ResolvedTableRef> {
        if (ref.group === undefined) throw new Error(`Table '${ref.table}' requires a group qualifier`);
        const group = await this.resolveGroup(ref.group, ctx);
        if (group.group === undefined) throw new Error(`Group '${refText(ref.group)}' is not loaded`);
        const table = await group.group.getTable(ref.table);
        return { groupId: group.id, group: group.group, tableName: ref.table, table };
    }

    async resolveLogTarget(ref: NameOrHashRef, ctx: RootResolveContext = {}): Promise<ResolvedLogTarget> {
        if (ref.kind === 'name' && ref.text.includes('.')) {
            const parts = ref.text.split('.');
            const groupRef: NameOrHashRef = { kind: 'name', text: parts[0], parts: [parts[0]], span: ref.span };
            const table = await this.resolveTable({ group: groupRef, table: parts.slice(1).join('.'), span: ref.span }, ctx);
            return {
                kind: 'table',
                id: table.table.getId(),
                object: table.table as RTable & { getScopedDag(): ReturnType<RTable['getScopedDag']> },
                groupId: table.groupId,
                tableName: table.tableName,
            };
        }

        const root = await this.resolveRoot(ref, ['database', 'schema', 'group'], ctx);
        if (root.object === undefined) throw new Error(`Root '${root.id}' is not loaded`);
        if (root.kind === 'database') return { kind: 'database', id: root.id, object: root.object as RDb & ResolvedLogTarget['object'] };
        if (root.kind === 'schema') return { kind: 'schema', id: root.id, object: root.object as RSchema & ResolvedLogTarget['object'] };
        return { kind: 'group', id: root.id, object: root.object as RTableGroup & ResolvedLogTarget['object'] };
    }

    async resolveHash(ref: HashRef, scope: HashScope): Promise<B64Hash> {
        const candidates = await this.hashCandidates(scope);
        const matches = candidates.filter((hash) => hash.startsWith(ref.prefix));
        if (matches.length === 1) return matches[0];
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
            if (matches.length === 1) return matches[0].id;
            if (matches.length > 1) throw new Error(`Ambiguous root name '${ref.text}'`);
            if (this.roots.has(ref.text)) {
                const root = this.roots.get(ref.text)!;
                if (kinds.includes(root.kind)) return ref.text;
            }
            throw new Error(`Unknown root '${ref.text}'`);
        }
        return this.resolveHash(ref, { kind: 'global' });
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
    if (type === RSCHEMA_TYPE_ID) return 'schema';
    if (type === RTABLE_GROUP_TYPE_ID) return 'group';
    return 'other';
}

function rootKindToAliasScope(kind: RootKind): AliasScope {
    if (kind === 'database') return 'db';
    if (kind === 'schema') return 'schema';
    if (kind === 'group') return 'group';
    throw new Error(`Root kind '${kind}' is not aliasable`);
}

function refText(ref: NameOrHashRef): string {
    return ref.kind === 'name' ? ref.text : `#${ref.prefix}`;
}

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RObject } from "@hyper-hyper-space/hhs3_mvt";
import type { RDb, RSchema, RTable, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import { RDB_TYPE_ID, RSCHEMA_TYPE_ID, RTABLE_GROUP_TYPE_ID } from "@hyper-hyper-space/hhs3_rdb";
import type {
    HashScope,
    NameOrHashRef,
    ResolvedGroupRef,
    ResolvedLogTarget,
    ResolvedSchemaRef,
    ResolvedTableRef,
    TableRef,
} from "@hyper-hyper-space/hhs3_rdb_lang";

export type RootKind = 'database' | 'schema' | 'group' | 'other';

export type RootRecord = {
    id: B64Hash;
    type: string;
    kind: RootKind;
    name?: string;
    object?: RObject;
};

export class RootIndex {
    private readonly roots = new Map<B64Hash, RootRecord>();
    private readonly aliases = new Map<string, B64Hash>();

    upsert(record: RootRecord): void {
        const existing = this.roots.get(record.id);
        this.roots.set(record.id, { ...existing, ...record });
        if (record.name !== undefined) this.aliases.set(record.name, record.id);
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

    setName(id: B64Hash, name: string): void {
        const root = this.roots.get(id);
        if (root === undefined) throw new Error(`Cannot name unknown root '${id}'`);
        this.upsert({ ...root, name });
    }

    setAlias(name: string, id: B64Hash): void {
        if (!this.roots.has(id)) throw new Error(`Cannot alias unknown root '${id}'`);
        this.aliases.set(name, id);
    }

    exportNames(): { names: { [id: string]: string }; aliases: { [name: string]: B64Hash } } {
        const names: { [id: string]: string } = {};
        for (const root of this.roots.values()) {
            if (root.name !== undefined) names[root.id] = root.name;
        }
        return { names, aliases: Object.fromEntries(this.aliases.entries()) };
    }

    list(kind?: RootKind): RootRecord[] {
        const roots = [...this.roots.values()];
        return kind === undefined ? roots : roots.filter((root) => root.kind === kind);
    }

    get(id: B64Hash): RootRecord | undefined {
        return this.roots.get(id);
    }

    async resolveSchema(ref: NameOrHashRef): Promise<ResolvedSchemaRef> {
        const root = await this.resolveRoot(ref, ['schema']);
        return { id: root.id, schema: root.object as RSchema | undefined };
    }

    async resolveGroup(ref: NameOrHashRef): Promise<ResolvedGroupRef> {
        const root = await this.resolveRoot(ref, ['group']);
        return { id: root.id, group: root.object as RTableGroup | undefined };
    }

    async resolveDatabase(ref: NameOrHashRef): Promise<{ id: B64Hash; db?: RDb }> {
        const root = await this.resolveRoot(ref, ['database']);
        return { id: root.id, db: root.object as RDb | undefined };
    }

    async resolveTable(ref: TableRef): Promise<ResolvedTableRef> {
        if (ref.group === undefined) throw new Error(`Table '${ref.table}' requires a group qualifier`);
        const group = await this.resolveGroup(ref.group);
        if (group.group === undefined) throw new Error(`Group '${refText(ref.group)}' is not loaded`);
        const table = await group.group.getTable(ref.table);
        return { groupId: group.id, group: group.group, tableName: ref.table, table };
    }

    async resolveLogTarget(ref: NameOrHashRef): Promise<ResolvedLogTarget> {
        if (ref.kind === 'name' && ref.text.includes('.')) {
            const parts = ref.text.split('.');
            const groupRef: NameOrHashRef = { kind: 'name', text: parts[0], parts: [parts[0]], span: ref.span };
            const table = await this.resolveTable({ group: groupRef, table: parts.slice(1).join('.'), span: ref.span });
            return {
                kind: 'table',
                id: table.table.getId(),
                object: table.table as RTable & { getScopedDag(): ReturnType<RTable['getScopedDag']> },
                groupId: table.groupId,
                tableName: table.tableName,
            };
        }

        const root = await this.resolveRoot(ref, ['database', 'schema', 'group']);
        if (root.object === undefined) throw new Error(`Root '${root.id}' is not loaded`);
        if (root.kind === 'database') return { kind: 'database', id: root.id, object: root.object as RDb & ResolvedLogTarget['object'] };
        if (root.kind === 'schema') return { kind: 'schema', id: root.id, object: root.object as RSchema & ResolvedLogTarget['object'] };
        return { kind: 'group', id: root.id, object: root.object as RTableGroup & ResolvedLogTarget['object'] };
    }

    async resolveHash(ref: NameOrHashRef, scope: HashScope): Promise<B64Hash> {
        if (ref.kind === 'name') {
            const aliased = this.aliases.get(ref.text);
            return aliased ?? ref.text;
        }

        const candidates = await this.hashCandidates(scope);
        const matches = candidates.filter((hash) => hash.startsWith(ref.prefix));
        if (matches.length === 1) return matches[0];
        if (matches.length === 0) throw new Error(`Unknown hash prefix '#${ref.prefix}'`);
        throw new Error(`Ambiguous hash prefix '#${ref.prefix}'`);
    }

    private async resolveRoot(ref: NameOrHashRef, kinds: RootKind[]): Promise<RootRecord> {
        const id = await this.resolveRootId(ref);
        const root = this.roots.get(id);
        if (root === undefined) throw new Error(`Unknown root '${refText(ref)}'`);
        if (!kinds.includes(root.kind)) {
            throw new Error(`Root '${refText(ref)}' is a ${root.kind}, expected ${kinds.join(' or ')}`);
        }
        return root;
    }

    private async resolveRootId(ref: NameOrHashRef): Promise<B64Hash> {
        if (ref.kind === 'name') {
            const direct = this.aliases.get(ref.text);
            if (direct !== undefined) return direct;
            const matches = [...this.roots.values()].filter((root) => root.name === ref.text);
            if (matches.length === 1) return matches[0].id;
            if (matches.length > 1) throw new Error(`Ambiguous root name '${ref.text}'`);
            if (this.roots.has(ref.text)) return ref.text;
            throw new Error(`Unknown root '${ref.text}'`);
        }
        return this.resolveHash(ref, { kind: 'global' });
    }

    private async hashCandidates(scope: HashScope): Promise<B64Hash[]> {
        if (scope.kind === 'global') return [...new Set([...this.roots.keys(), ...this.aliases.values()])];

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

function refText(ref: NameOrHashRef): string {
    return ref.kind === 'name' ? ref.text : `#${ref.prefix}`;
}

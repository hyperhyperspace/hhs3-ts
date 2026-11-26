// Helpers the ingestion suites share: target wrappers, the writer record as
// RdbProjection keeps it, and reads of rdb and of the target.

import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { Row, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import {
    AdapterConfig, AuthorSetting, BidirectionalTarget, DEFAULT_KEY_DOMAIN, IngestSettle, KeyIndex, OpEvent, projectGroup,
    QueryableTarget, RowAction, rowToUpsertAction, WriterRecordStore,
} from "@hyper-hyper-space/hhs3_rdb_adapter";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

// A target whose FIRST commitIngest is silently dropped, then delegates as
// normal: a crash in the window AFTER the rdb append but BEFORE the target's
// settle. The append lands and its hash is recorded, so the outbox holds the
// records the next cycle's end of a pass settles. Everything else, the key
// table and the writer record included, is the inner target's.
export class CrashBeforeSettle {
    constructor(inner: BidirectionalTarget) {
        let commits = 0;
        return new Proxy(inner, {
            get(target, prop, receiver) {
                const value = Reflect.get(target, prop, receiver);
                if (prop !== 'commitIngest') return typeof value === 'function' ? value.bind(target) : value;
                return async (settle: IngestSettle): Promise<void> => {
                    commits += 1;
                    if (commits === 1) return;   // the "crash": drop the settle
                    return target.commitIngest(settle);
                };
            },
        }) as unknown as CrashBeforeSettle;
    }
}
export interface CrashBeforeSettle extends BidirectionalTarget {}

// A row's values as a sorted list, for whole-row comparisons.
export function sorted(values: { [column: string]: unknown } | undefined): string {
    return values === undefined ? '<no row>' : JSON.stringify(Object.entries(values).sort());
}

// The row ops the group holds for `table`, oldest first, as written.
export async function rowOps(group: RTableGroup, table: string): Promise<{ action: string; values: { [c: string]: unknown }; nulls?: string[] }[]> {
    const ops: { action: string; values: { [c: string]: unknown }; nulls?: string[] }[] = [];
    for await (const entry of (await group.getScopedDag()).loadAllEntries()) {
        const payload = entry.payload as { action?: string; table?: string; op?: unknown; writes?: { table: string; op: unknown }[] };
        const writes = payload.action === 'bundle' ? payload.writes ?? []
            : payload.action === 'row' && payload.table !== undefined ? [{ table: payload.table, op: payload.op }] : [];
        for (const write of writes) {
            if (write.table === table) ops.push(write.op as { action: string; values: { [c: string]: unknown }; nulls?: string[] });
        }
    }
    return ops;
}

// Records the writer in the target, as RdbProjection does at open: the writer
// key, when given, the hash suite and the author setting.
export async function recordWriter(target: BidirectionalTarget, author: AuthorSetting, writer?: OwnIdentity): Promise<number | undefined> {
    let writerKeyId: number | undefined;
    if (writer !== undefined) {
        writerKeyId = await (target as unknown as KeyIndex).registerKey(DEFAULT_KEY_DOMAIN, writer.keyId, serializePublicKeyToBase64(writer.publicKey));
    }
    await (target as unknown as WriterRecordStore).recordWriter({
        ...(writerKeyId !== undefined ? { writerKeyId } : {}), hashSuite: 'sha-256', author,
    });
    return writerKeyId;
}

// Projects a group and records its writer, so the target is set up as
// RdbProjection sets it up: capture fills an insert's empty author column with
// the writer key (HIP-29).
export async function projectAs(group: RTableGroup, target: BidirectionalTarget, writer: OwnIdentity, config: AdapterConfig = {}): Promise<void> {
    await projectGroup(group, target, config);
    await recordWriter(target, config.author ?? 'auto', writer);
}

// A failure event for the settle cases, which the target stores as given.
export function failureEvent(groupId: B64Hash, opHash: string, localId: number): OpEvent {
    return { origin: 'ingestion', direction: 'failure', groupId, opHash, kind: 'insert', table: 'tags', localId };
}

// The number of entries in a group's DAG.
export async function entryCount(group: RTableGroup): Promise<number> {
    let n = 0;
    for await (const _entry of (await group.getScopedDag()).loadAllEntries()) n++;
    return n;
}

// rdb's row of a table as projection writes it, for a revert.
export async function rdbRowAction(group: RTableGroup, table: string, rowId: B64Hash): Promise<Extract<RowAction, { kind: 'upsert-row' }>> {
    const row = (await rdbRows(group, table)).find((r) => r.rowId === rowId)!;
    return rowToUpsertAction(row, table, (await group.getView()).getSchemaView(), {}) as Extract<RowAction, { kind: 'upsert-row' }>;
}

// The target's row of a table by local id, as a query reads it.
export async function localRow(target: BidirectionalTarget, table: string, localId: number): Promise<{ [column: string]: unknown } | undefined> {
    return (await (target as unknown as QueryableTarget).executeQuery({ table })).find((r) => r.id === localId);
}

// Live rows of an rdb group table (the assertion target: what ingestion wrote).
export async function rdbRows(group: RTableGroup, table: string): Promise<Row[]> {
    const view = await (await group.getTable(table)).getView();
    return view.query({});
}


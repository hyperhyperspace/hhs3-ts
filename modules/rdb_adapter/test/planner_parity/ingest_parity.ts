import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import {
    IDENTITIES_TABLE, identityRow, RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory, TableDef,
    usersSchemaTables,
} from "@hyper-hyper-space/hhs3_rdb";

import { createMockRContext } from "@hyper-hyper-space/hhs3_rdb_adapter_test_gen";
import { changesToEntries, type IngestPlan, type MappingLookup } from "../../src/ingest.js";

import { generateCapturedBatch } from "./ingest_generate.js";
import { resolveFuzzSweepOptions, type ResolvedFuzzSweepOptions } from "@hyper-hyper-space/hhs3_rdb_adapter_test_gen";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function open(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

function counterUuid(): () => string {
    let n = 0;
    return () => 'u' + (++n);
}

function opCount(plan: IngestPlan): number {
    return plan.entries.reduce((n, e) => n + e.ops.length, 0);
}

function flattenSources(plan: IngestPlan): Set<string | number> {
    const ids = new Set<string | number>();
    for (const e of plan.entries) {
        for (const op of e.ops) ids.add(op.change.id);
    }
    return ids;
}

// Appends each planned op. An op that names a row rdb doesn't have, as its row
// or in a foreign key, is left out: in ingestion rdb refuses it. That starts
// with the refused replayed insert's row, and an insert left out this way adds
// its own row, so a later change of that row is left out too.
async function applyPlan(group: RTableGroupImpl, plan: IngestPlan, writer: OwnIdentity, absentRow?: string): Promise<void> {
    const absent = new Set(absentRow === undefined ? [] : [absentRow]);
    for (const entry of plan.entries) {
        const text = JSON.stringify(entry.ops.map((o) => o.write.op));
        if ([...absent].some((rowId) => text.includes(JSON.stringify(rowId)))) {
            for (const o of entry.ops) if (o.kind === 'insert') absent.add(o.write.op.rowId);
            continue;
        }
        await group.bundle(entry.ops.map((o) => o.write), writer);
    }
}

function mismatch(kind: string, seed: number, batch: number, extra: string): Error {
    return new Error(`kind=${kind} seed=${seed} batch=${batch}\n${extra}`);
}

export async function runIngestPlannerSweep(options: ResolvedFuzzSweepOptions): Promise<void> {
    for (const seed of options.seeds) {
        const ctx = createMockRContext({ selfValidate: true });
        ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
        ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
        const writer = await makeIdentity();
        const schemaInit = await RSchemaImpl.create({
            name: `planner:ingest_${seed}`,
            creators: [{ keyId: writer.keyId, publicKey: writer.publicKey }],
            tables: [
                open('posts', { title: { type: 'string' }, note: { type: 'string', nullable: true } }),
                open('comments', {
                    body: { type: 'string' },
                    post: { type: 'string', nullable: true },
                    parent: { type: 'string', nullable: true },
                }, { fks: { post: 'posts', parent: 'comments' } }),
                open('ledger', {
                    ref: { type: 'string', pub: true, readonly: true },
                    memo: { type: 'string', nullable: true },
                    amount: { type: 'decimal', constraints: { scale: 2 } },
                }),
                usersSchemaTables().find((t) => t.name === IDENTITIES_TABLE)!,
            ],
        });
        const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
        const pinned = await (await schema.getScopedDag()).getFrontier();

        for (let b = 0; b < options.ingestBatches; b++) {
            const generated = generateCapturedBatch(seed * 1009 + b, options.ingestChanges, { crashReplay: b === 0 });
            const lookup: MappingLookup = (table, localId) => {
                const m = generated.replayLookup;
                if (m !== undefined && m.table === table && m.localId === localId) return m;
                return undefined;
            };

            // The writer signs every entry, so each group registers it in a
            // local identity provider.
            const makeGroup = async (suffix: string) => {
                const init = await RTableGroupImpl.create({
                    name: `ingest-${seed}-${b}-${suffix}`,
                    seed: `ingest-${seed}-${b}-${suffix}`,
                    schemaRef: schema.getId(),
                    schemaVersion: pinned,
                    idProvider: IDENTITIES_TABLE,
                    initialRows: { [IDENTITIES_TABLE]: [identityRow('writer', writer)] },
                });
                return (await ctx.createObject(init)) as RTableGroupImpl;
            };
            const group = await makeGroup('plan');
            const schemaView = (await group.getView()).getSchemaView();
            // One profile (HIP-29): each change is one op, a refusal, or nothing to
            // write. The batch's inserts name no author, as if the tables had no
            // author column, where the writer key signs them.
            const plan = changesToEntries(generated.batch, schemaView, lookup, { authorColumn: false }, writer.keyId, counterUuid());

            const changeCount = generated.batch.changes.length;
            if (opCount(plan) + plan.rejects.length + plan.nothing.length !== changeCount) {
                throw mismatch('ingest-count', seed, b,
                    `ops=${opCount(plan)} rejects=${plan.rejects.length} nothing=${plan.nothing.length} changes=${changeCount}`);
            }
            if (plan.entries.some((e) => e.ops.length !== 1)) {
                throw mismatch('ingest-one-op-per-entry', seed, b, plan.entries.map((e) => e.ops.length).join(','));
            }

            const changes = generated.batch.changes;
            for (let i = 0; i < changes.length - 1; i++) {
                const a = changes[i], d = changes[i + 1];
                if (a.kind !== 'insert' || d.kind !== 'delete') continue;
                if (a.table !== d.table || a.localId !== d.localId) continue;
                const src = flattenSources(plan);
                const rejected = new Set(plan.rejects.map((r) => r.change?.id));
                const covered = (src.has(a.id) ? 1 : 0) + (src.has(d.id) ? 1 : 0)
                    + (rejected.has(a.id) ? 1 : 0) + (rejected.has(d.id) ? 1 : 0);
                if (covered !== 2) {
                    throw mismatch('ingest-insert-delete-cancel', seed, b,
                        `insert id=${a.id} delete id=${d.id} covered=${covered}`);
                }
            }

            // An insert on a local id that already has an identity is refused,
            // naming that identity's row.
            if (generated.replayLookup !== undefined) {
                const replay = generated.replayLookup;
                const insert = changes.find((c) => c.kind === 'insert' && c.table === replay.table && c.localId === replay.localId);
                const refused = plan.rejects.find((r) => r.change?.id === insert?.id);
                if (insert !== undefined && refused?.rowId !== replay.rowId) {
                    throw mismatch('ingest-identity-refused', seed, b, `rowId=${refused?.rowId} expected=${replay.rowId}`);
                }
            }

            try {
                await applyPlan(group, plan, writer, generated.replayLookup?.rowId);
            } catch (e) {
                throw mismatch('ingest-bundle', seed, b, String(e));
            }
            process.stdout.write('.');
        }
        process.stdout.write(`\n  seed=${seed} ingestBatches=${options.ingestBatches}\n`);
    }
}

export async function runIngestPlannerFromArgv(): Promise<void> {
    await runIngestPlannerSweep(resolveFuzzSweepOptions(process.argv.slice(2)));
}

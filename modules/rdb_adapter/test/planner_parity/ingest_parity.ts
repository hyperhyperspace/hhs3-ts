import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { deriveRowId, RSchemaImpl, rSchemaFactory, RTableGroupImpl, rTableGroupFactory, TableDef } from "@hyper-hyper-space/hhs3_rdb";

import { createMockRContext } from "@hyper-hyper-space/hhs3_rdb_adapter_test_gen";
import { changesToEntries, type IngestPlan, type MappingLookup } from "../../src/ingest.js";
import type { AdapterConfig } from "../../src/types.js";

import { fingerprintRdbRows } from "./fingerprint.js";
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

function rejectIds(plan: IngestPlan): string {
    return plan.rejects.map((r) => String(r.change?.id ?? '')).sort().join(',');
}

function opCount(plan: IngestPlan): number {
    return plan.entries.reduce((n, e) => n + e.ops.length, 0);
}

function flattenSources(plan: IngestPlan): Set<string | number> {
    const ids = new Set<string | number>();
    for (const e of plan.entries) {
        for (const op of e.ops) {
            for (const s of op.sources) ids.add(s.id);
        }
    }
    return ids;
}

async function applyPlan(group: RTableGroupImpl, plan: IngestPlan, writer: OwnIdentity): Promise<void> {
    for (const entry of plan.entries) {
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

            const makeGroup = async (suffix: string) => {
                const init = await RTableGroupImpl.create({
                    name: `ingest-${seed}-${b}-${suffix}`,
                    seed: `ingest-${seed}-${b}-${suffix}`,
                    schemaRef: schema.getId(),
                    schemaVersion: pinned,
                });
                return (await ctx.createObject(init)) as RTableGroupImpl;
            };
            const groupA = await makeGroup('opt');
            const groupB = await makeGroup('naive');
            const schemaView = (await groupA.getView()).getSchemaView();

            const optCfg: AdapterConfig = { updateMerge: true, fkBundling: true };
            const naiveCfg: AdapterConfig = { updateMerge: false, fkBundling: false };
            const opt = changesToEntries(generated.batch, schemaView, lookup, optCfg, writer.keyId, counterUuid());
            const naive = changesToEntries(generated.batch, schemaView, lookup, naiveCfg, writer.keyId, counterUuid());

            if (rejectIds(opt) !== rejectIds(naive)) {
                throw mismatch('ingest-reject-ids', seed, b,
                    `opt=${rejectIds(opt)} naive=${rejectIds(naive)}`);
            }

            const changeCount = generated.batch.changes.length;
            if (opCount(naive) + naive.rejects.length !== changeCount) {
                throw mismatch('ingest-flags-off-count', seed, b,
                    `ops=${opCount(naive)} rejects=${naive.rejects.length} changes=${changeCount}`);
            }

            for (const e of opt.entries) {
                for (const op of e.ops) {
                    if (op.fallback !== undefined) {
                        if (op.fallback.length < 2 || op.fallback.length !== op.sources.length) {
                            throw mismatch('ingest-fallback-shape', seed, b,
                                `fallback=${op.fallback.length} sources=${op.sources.length}`);
                        }
                    }
                }
            }

            const changes = generated.batch.changes;
            for (let i = 0; i < changes.length - 1; i++) {
                const a = changes[i], d = changes[i + 1];
                if (a.kind !== 'insert' || d.kind !== 'delete') continue;
                if (a.table !== d.table || a.localId !== d.localId) continue;
                const src = flattenSources(naive);
                const rejected = new Set(naive.rejects.map((r) => r.change?.id));
                const covered = (src.has(a.id) ? 1 : 0) + (src.has(d.id) ? 1 : 0)
                    + (rejected.has(a.id) ? 1 : 0) + (rejected.has(d.id) ? 1 : 0);
                if (covered !== 2) {
                    throw mismatch('ingest-insert-delete-cancel', seed, b,
                        `insert id=${a.id} delete id=${d.id} covered=${covered}`);
                }
            }

            if (generated.replayLookup !== undefined) {
                const replayOp = opt.entries.flatMap((e) => e.ops).find((o) => o.kind === 'insert'
                    && o.table === generated.replayLookup!.table && o.localId === generated.replayLookup!.localId);
                const expectedRowId = deriveRowId(generated.replayLookup.uuid, writer.keyId);
                if (replayOp === undefined || replayOp.reusedIdentity !== true || replayOp.write.op.action !== 'insert'
                    || replayOp.write.op.rowId !== expectedRowId) {
                    throw mismatch('ingest-replay-uuid', seed, b,
                        `reused=${replayOp?.reusedIdentity} rowId=${replayOp?.write.op.action === 'insert' ? replayOp.write.op.rowId : '?'}`
                        + ` expected=${expectedRowId}`);
                }
            }

            try {
                await applyPlan(groupA, opt, writer);
            } catch (e) {
                throw mismatch('ingest-bundle-opt', seed, b, String(e));
            }
            try {
                await applyPlan(groupB, naive, writer);
            } catch (e) {
                throw mismatch('ingest-bundle-naive', seed, b, String(e));
            }

            const fpA = await fingerprintRdbRows(await groupA.getView());
            const fpB = await fingerprintRdbRows(await groupB.getView());
            if (fpA !== fpB) {
                throw mismatch('ingest-live-view', seed, b, `opt=${fpA}\nnaive=${fpB}`);
            }
            process.stdout.write('.');
        }
        process.stdout.write(`\n  seed=${seed} ingestBatches=${options.ingestBatches}\n`);
    }
}

export async function runIngestPlannerFromArgv(): Promise<void> {
    await runIngestPlannerSweep(resolveFuzzSweepOptions(process.argv.slice(2)));
}

import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity, B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { version, Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "../mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../../src/rtable_group/group.js";
import { deriveRowId } from "../../src/rtable/hash.js";
import type { RowWrite } from "../../src/rtable/payload.js";
import { rowWriteParts } from "../../src/rtable/payload.js";
import type { ColumnDef, MigrationRule, TableDef } from "../../src/rschema/payload.js";
import type { BundleWrite } from "../../src/rtable_group/interfaces.js";

import { pickConcurrentAt, recordCheckpoint } from "./checkpoints.js";
import { PRNG } from "./prng.js";
import type { GroupHistory } from "./parity.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function open(name: string, columns: TableDef['columns'], extra?: Partial<TableDef>): TableDef {
    return { name, columns, restrictions: [{ on: 'all', rule: { p: 'true' } }], ...extra };
}

const PROJECT_POOL = 5;
const TASK_POOL = 10;
const NOTE_POOL = 8;

const projectUuid = (i: number): string => `p-${i}`;
const taskUuid = (i: number): string => `t-${i}`;
const noteUuid = (i: number): string => `n-${i}`;
const projectRowId = (i: number): B64Hash => deriveRowId(projectUuid(i));
const taskRowId = (i: number): B64Hash => deriveRowId(taskUuid(i));
const noteRowId = (i: number): B64Hash => deriveRowId(noteUuid(i));

// The default of `tasks.ref` when it is a foreign key: project 0, which the
// history also deletes, so the default's target goes away and comes back as
// the table is reset.
const DEFAULT_PROJECT = projectRowId(0);

const REF_PLAIN: ColumnDef = { type: 'string', nullable: true };
const REF_FK: ColumnDef = { type: 'string', nullable: true, default: DEFAULT_PROJECT };

function tasksDef(refIsFk: boolean): TableDef {
    return open('tasks', {
        title: { type: 'string' },
        memo: { type: 'string', nullable: true },
        ref: refIsFk ? REF_FK : REF_PLAIN,
    }, refIsFk ? { fks: { ref: 'projects' } } : {});
}

function notesDef(): TableDef {
    return open('notes', { text: { type: 'string' }, tag: { type: 'string', nullable: true, default: 'none' } });
}

// Null writes, and schema changes that put an operation's columns and tables
// out from under it. One group: `projects`, `tasks` (a nullable `memo`, and a
// nullable `ref` that deploys drop and add again, as a plain column or as a
// foreign key to `projects` with a default) and `notes` (a nullable `tag` with
// a default). The history inserts and updates with values, nulls and columns
// left out, deletes the default's target, bundles a write to `tasks` with one
// to `notes`, and deploys re-adds of `ref` and resets of `tasks` and `notes`,
// each at a possibly concurrent checkpoint. So an insert or an update is often
// concurrent with a re-add of a column it carries, and a bundle with a reset
// of a table it writes to. Rejected attempts are skipped.
export async function generateNullsHistory(seed: number, ops: number): Promise<GroupHistory> {
    const prng = new PRNG(seed);
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = await makeIdentity();
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: `parity:nulls_${seed}`,
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [open('projects', { name: { type: 'string' } }), tasksDef(false), notesDef()],
    }))) as RSchemaImpl;
    const group = (await ctx.createObject(await RTableGroupImpl.create({
        name: `parity-nulls-${seed}`,
        seed: `parity-nulls-${seed}`,
        schemaRef: schema.getId(),
        schemaVersion: await (await schema.getScopedDag()).getFrontier(),
    }))) as RTableGroupImpl;
    const rawDag = (await ctx.getDag(group.getId()))!;

    const projects = await group.getTable('projects');
    const tasks = await group.getTable('tasks');
    const notes = await group.getTable('notes');
    const checkpoints: Version[] = [version(group.getId())];

    // whether `ref` is a foreign key in the schema object's latest version
    let refIsFk = false;

    const groupFrontier = async () => (await group.getScopedDag()).getFrontier();
    const liveOf = async (table: string, at: Version, pool: number, rowId: (i: number) => B64Hash): Promise<number[]> => {
        const view = await (await group.getView(at, at)).getTableView(table);
        const live: number[] = [];
        for (let i = 0; i < pool; i++) if (await view.hasRow(rowId(i))) live.push(i);
        return live;
    };
    const pick = <T>(arr: T[]): T | undefined => arr.length === 0 ? undefined : arr[prng.nextInt(0, arr.length - 1)];

    // A value, a null, or nothing for `memo`.
    const memoWrite = (write: RowWrite): void => {
        const roll = prng.nextInt(0, 2);
        if (roll === 0) write['memo'] = `m-${prng.nextInt(0, 3)}`;
        else if (roll === 1) write['memo'] = null;
    };
    // A value, a null, or nothing for `ref`. The value is a live project where
    // `ref` is a foreign key at `at`, and now and then a stray text where it is
    // a plain column.
    const refWrite = async (write: RowWrite, at: Version): Promise<void> => {
        const roll = prng.nextInt(0, 3);
        if (roll === 0) return;
        if (roll === 1) { write['ref'] = null; return; }
        const isFk = 'ref' in (await group.getView(at, at)).getSchemaView().getFKs('tasks');
        const target = pick(await liveOf('projects', at, PROJECT_POOL, projectRowId));
        if (!isFk && prng.next() < 0.5) write['ref'] = `text-${prng.nextInt(0, 3)}`;
        else if (target !== undefined) write['ref'] = projectRowId(target);
    };

    for (let opIndex = 0; opIndex < ops; opIndex++) {
        const at = pickConcurrentAt(prng, checkpoints);
        const atV = at ?? await groupFrontier();
        const roll = prng.nextInt(0, 99);

        try {
            if (roll < 12) {
                // insert a project
                const i = prng.nextInt(0, PROJECT_POOL - 1);
                await projects.insert(projectUuid(i), { name: `p-${i}` }, undefined, at);
            } else if (roll < 20) {
                // delete a project: a target of `ref`, now and then the default's
                const target = pick(await liveOf('projects', atV, PROJECT_POOL, projectRowId));
                if (target === undefined) continue;
                await projects.delete(projectRowId(target), undefined, at);
            } else if (roll < 40) {
                // insert a task with a value, a null or nothing in each nullable column
                const write: RowWrite = { title: `t-${prng.nextInt(0, 5)}` };
                memoWrite(write);
                await refWrite(write, atV);
                await tasks.insert(taskUuid(prng.nextInt(0, TASK_POOL - 1)), write, undefined, at);
            } else if (roll < 58) {
                // update a task: its title, or a value or a null in a nullable column
                const target = pick(await liveOf('tasks', atV, TASK_POOL, taskRowId));
                if (target === undefined) continue;
                const write: RowWrite = {};
                if (prng.next() < 0.5) write['title'] = `t-${prng.nextInt(0, 5)}`;
                memoWrite(write);
                await refWrite(write, atV);
                if (Object.keys(write).length === 0) write['memo'] = null;
                await tasks.update(taskRowId(target), write, undefined, at);
            } else if (roll < 63) {
                // delete a task
                const target = pick(await liveOf('tasks', atV, TASK_POOL, taskRowId));
                if (target === undefined) continue;
                await tasks.delete(taskRowId(target), undefined, at);
            } else if (roll < 73) {
                // insert a note, or set its tag to a value or to null
                const i = prng.nextInt(0, NOTE_POOL - 1);
                const tag = prng.next() < 0.5 ? null : `g-${prng.nextInt(0, 2)}`;
                if ((await liveOf('notes', atV, NOTE_POOL, noteRowId)).includes(i)) {
                    await notes.update(noteRowId(i), { tag }, undefined, at);
                } else {
                    await notes.insert(noteUuid(i), prng.next() < 0.5 ? { text: 'x' } : { text: 'x', tag }, undefined, at);
                }
            } else if (roll < 85) {
                // a bundle that writes to `tasks` and to `notes`
                const ti = prng.nextInt(0, TASK_POOL - 1);
                const ni = prng.nextInt(0, NOTE_POOL - 1);
                const taskWrite: RowWrite = { title: `t-${prng.nextInt(0, 5)}` };
                memoWrite(taskWrite);
                await refWrite(taskWrite, atV);
                const taskLive = (await liveOf('tasks', atV, TASK_POOL, taskRowId)).includes(ti);
                const noteLive = (await liveOf('notes', atV, NOTE_POOL, noteRowId)).includes(ni);
                const writes: BundleWrite[] = [
                    { table: 'tasks', op: taskLive
                        ? { action: 'update', rowId: taskRowId(ti), ...rowWriteParts(taskWrite) }
                        : { action: 'insert', rowId: taskRowId(ti), uuid: taskUuid(ti), ...rowWriteParts(taskWrite) } },
                    { table: 'notes', op: noteLive
                        ? { action: 'update', rowId: noteRowId(ni), ...rowWriteParts({ tag: prng.next() < 0.5 ? null : 'b' }) }
                        : { action: 'insert', rowId: noteRowId(ni), uuid: noteUuid(ni), ...rowWriteParts({ text: 'b', tag: null }) } },
                ];
                await group.bundle(writes, undefined, at);
            } else {
                // a schema deploy: `ref` dropped and added again, or a table reset
                const kind = pick(['readd-ref', 'readd-ref', 'reset-tasks', 'reset-notes'])!;
                let rules: MigrationRule[];
                let nextRefIsFk: boolean = refIsFk;
                if (kind === 'readd-ref') {
                    nextRefIsFk = !refIsFk;
                    rules = [
                        ...(refIsFk ? [{ rule: 'set-fks', table: 'tasks', fks: {} } as MigrationRule] : []),
                        { rule: 'drop-column', table: 'tasks', column: 'ref' },
                        { rule: 'add-column', table: 'tasks', column: 'ref', def: nextRefIsFk ? REF_FK : REF_PLAIN },
                        ...(nextRefIsFk ? [{ rule: 'set-fks', table: 'tasks', fks: { ref: 'projects' } } as MigrationRule] : []),
                    ];
                } else if (kind === 'reset-tasks') {
                    rules = [{ rule: 'drop-table', table: 'tasks' }, { rule: 'add-table', def: tasksDef(refIsFk) }];
                } else {
                    rules = [{ rule: 'drop-table', table: 'notes' }, { rule: 'add-table', def: notesDef() }];
                }
                await schema.updateSchema(rules, admin, kind);
                refIsFk = nextRefIsFk;
                await group.deploy(await (await schema.getScopedDag()).getFrontier(), undefined, at);
            }
        } catch {
            continue;   // a rejected attempt: not recorded
        }

        await recordCheckpoint(checkpoints, await groupFrontier());
    }

    return { group, rawDag, checkpoints, seed };
}

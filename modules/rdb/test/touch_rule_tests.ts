// The touch rule (src/rtable/touch.ts), on production views, checked against
// today's exact reads at every completion.
//
// A bound group is read at a version D while observes o1 and o2 are pending,
// each adding its version V_o. A view built with those pending observes
// decides a read only when no pending observe touches it, and answers at D.
// The oracle: wherever the view decides a read, the exact read (a view with
// nothing pending) at every completion D ∪ (V_o for o in S), for every subset
// S of the pending observes, gives the same answer. Verdicts of the bound
// group are injected through a stub, as in verdict_reads_tests.ts.
//
// Besides soundness, each configuration checks:
//   - exactness: an observe whose version D already holds touches nothing, so
//     with only such observes pending the read is today's read at D;
//   - monotonicity: deciding a pending observe, live (D grows by its version)
//     or void (it leaves the pending set), keeps every decided read decided
//     with the same answer;
//   - supports: an undecided read is supported by pending observes only.
//
// The rule is also re-implemented here, with switches, to run broken variants
// (no slot check, no concurrent comparison, candidates gathered at D only)
// and the sound but coarser variant that compares every entry. Each broken
// variant has a pinned configuration where it decides a wrong answer, so a
// change to the fixture that loses the counterexample fails a test.

import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { set } from "@hyper-hyper-space/hhs3_util";
import { createBasicCrypto, HASH_SHA256, keyIdFromPublicKey, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity, PublicKey } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import type { EntryMetaFilter, EntryPredicate, Position } from "@hyper-hyper-space/hhs3_dag";
import { version, refVersionAtOrAbove, truthOf, everyTruth, someTruth, FALSE, undecidedOn } from "@hyper-hyper-space/hhs3_mvt";
import type { PendingRefAdvance, Truth, Verdict, Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import type { RTableImpl } from "../src/rtable/rtable.js";
import { RTableViewImpl } from "../src/rtable/view.js";
import type { ColumnValue, TableViewTarget } from "../src/rtable/view.js";
import { deriveRowId } from "../src/rtable/hash.js";
import type { MigrationRule, TableDef } from "../src/rschema/payload.js";
import type { OpVoidDetail } from "../src/rtable_group/op_void.js";
import { freshVerdictEvaluation } from "../src/rtable_group/verdict_evaluation.js";
import { rowTag, colTag } from "../src/rtable_group/scopes.js";
import { columnSlot, existenceSlot, slotsTouchedByRule } from "../src/rschema/resolve.js";

const TABLE = 'caps';

// TOUCHR_SCALE multiplies the sampled trials, for a longer run by hand.
const SCALE = Number(process.env['TOUCHR_SCALE'] ?? '1');

// A fixed ed25519 key pair, so that every run builds the same entries: the
// schema's id carries the creator's key, and every hash of the group follows
// from it, including the tie-breaks between concurrent writes.
const ADMIN_SECRET = '1adc0e07ae10caa63dc35e3f37737acbb3ee092c32946449d972d49bc854172e';
const ADMIN_PUBLIC = '2a95339a1daf6f83612af582bd4031bcdeab11f6d262335ce627bb45db92fbb2';

function fromHex(hex: string): Uint8Array {
    return new Uint8Array(hex.match(/../g)!.map((byte) => parseInt(byte, 16)));
}

function fixedAdmin(): OwnIdentity {
    const publicKey: PublicKey = { suite: SIGNING_ED25519, key: fromHex(ADMIN_PUBLIC) };
    return { publicKey, secretKey: fromHex(ADMIN_SECRET), keyId: keyIdFromPublicKey(publicKey, createBasicCrypto().hash(HASH_SHA256)) };
}

const capsTable: TableDef = {
    name: TABLE,
    columns: {
        grantee: { type: 'string', pub: true },
        active: { type: 'string', pub: true, default: 'yes' },
    },
    concurrentDeletes: true,
    restrictions: [{ on: 'all', rule: { p: 'true' } }],
};

class Rng {
    private s: number;
    constructor(seed: number) { this.s = seed >>> 0; }
    next(): number {
        let t = (this.s += 0x6D2B79F5) >>> 0;
        t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    }
    int(lo: number, hi: number): number { return lo + Math.floor(this.next() * (hi - lo + 1)); }
    pick<T>(xs: T[]): T { return xs[this.int(0, xs.length - 1)]!; }
}

type Fixture = {
    group: RTableGroupImpl;
    schema: RSchemaImpl;
    table: RTableImpl;
    rowOps: B64Hash[];
    entries: B64Hash[];
    labels: Map<string, B64Hash>;
    nameOf: Map<B64Hash, string>;
    r: B64Hash;
    s: B64Hash;
};

// Row r: I inserts it (alice, active yes) and I2 is a concurrent duplicate
// insert (carol). Above I, U1 writes grantee bob, U2 writes active no, and Dr
// deletes it, all concurrently; U3, above U1, writes bob again. Row s: J
// inserts it (bob, active by default), Us writes active no, Ds deletes it
// after Us, and M, above U1 and Us, writes grantee carol. Two schema updates,
// concurrent in the schema DAG, are deployed at I and at J: Dep1 drops
// `active` and re-adds it with default `no`, so every earlier write to it
// belongs to a dead incarnation; Dep2 changes only the table's rules.
async function createFixture(): Promise<Fixture> {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);

    const admin = fixedAdmin();
    const schemaInit = await RSchemaImpl.create({
        name: 'touchrule:schema',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [capsTable],
    });
    const schema = (await ctx.createObject(schemaInit)) as RSchemaImpl;
    const pinned = await (await schema.getScopedDag()).getFrontier();
    const groupInit = await RTableGroupImpl.create({
        name: 'touchrule', seed: 'touchrule', schemaRef: schema.getId(), schemaVersion: pinned,
    });
    const group = (await ctx.createObject(groupInit)) as RTableGroupImpl;
    const table = await group.getTable(TABLE);
    const base = await (await group.getScopedDag()).getFrontier();

    const r = deriveRowId('r');
    const s = deriveRowId('s');
    const I = await table.insert('r', { grantee: 'alice', active: 'yes' }, undefined, base);
    const I2 = await table.insert('r', { grantee: 'carol' }, undefined, base);
    const J = await table.insert('s', { grantee: 'bob' }, undefined, base);
    const U1 = await table.update(r, { grantee: 'bob' }, undefined, version(I));
    const U2 = await table.update(r, { active: 'no' }, undefined, version(I));
    const U3 = await table.update(r, { grantee: 'bob' }, undefined, version(U1));
    const Dr = await table.delete(r, undefined, version(I));
    const Us = await table.update(s, { active: 'no' }, undefined, version(J));
    const M = await table.update(s, { grantee: 'carol' }, undefined, version(U1, Us));
    const Ds = await table.delete(s, undefined, version(Us));

    const reset: MigrationRule[] = [
        { rule: 'drop-column', table: TABLE, column: 'active' },
        { rule: 'add-column', table: TABLE, column: 'active', def: { type: 'string', pub: true, default: 'no' } },
    ];
    const rulesOnly: MigrationRule[] = [
        { rule: 'set-restrictions', table: TABLE, restrictions: [{ on: 'insert', rule: { p: 'true' } }] },
    ];
    const s1 = await schema.updateSchema(reset, admin, 'reset active', pinned);
    const s2 = await schema.updateSchema(rulesOnly, admin, 'rules only', pinned);
    const Dep1 = await group.deploy(version(s1), undefined, version(I));
    const Dep2 = await group.deploy(version(s2), undefined, version(J));

    const named: [string, B64Hash][] = [
        ...[...base].map((h, i): [string, B64Hash] => [`G${i}`, h]),
        ['I', I], ['I2', I2], ['J', J], ['U1', U1], ['U2', U2], ['U3', U3], ['Dr', Dr], ['Us', Us], ['M', M], ['Ds', Ds],
        ['Dep1', Dep1], ['Dep2', Dep2],
    ];
    const labels = new Map(named);
    const nameOf = new Map(named.map(([n, h]) => [h, n]));
    const rowOps = [I, I2, J, U1, U2, U3, Dr, Us, M, Ds];
    return { group, schema, table, rowOps, entries: named.map(([, h]) => h), labels, nameOf, r, s };
}

type Status = Map<B64Hash, 'live' | 'void'>;

function targetFor(fx: Fixture, status: Status): TableViewTarget {
    const target = Object.create(fx.table) as TableViewTarget;
    target.entryStatusIn = async (_evaluation, hash): Promise<Verdict<OpVoidDetail>> =>
        (status.get(hash) ?? 'live') === 'live' ? { status: 'live' } : { status: 'void', reason: { kind: 'undecided-cycle' } };
    return target;
}

function union(...ps: Position[]): Version {
    const result = version();
    for (const p of ps) for (const h of p) result.add(h);
    return result;
}

function sameValue(a: json.Literal | undefined, b: json.Literal | undefined): boolean {
    if (a === undefined || b === undefined) return a === b;
    return json.toStringNormalized(a) === json.toStringNormalized(b);
}

type Pending = { name: string; version: Version };

function pendingAdvances(pending: Pending[]): PendingRefAdvance[] {
    return pending.map((o) => ({ entry: o.name, version: o.version, verdict: undecidedOn([o.name]) }));
}

type Read =
    | { kind: 'row'; rowId: B64Hash; label: string }
    | { kind: 'column'; rowId: B64Hash; column: string; label: string }
    | { kind: 'exists'; where: { [field: string]: json.Literal }; label: string };

function readsOf(fx: Fixture): Read[] {
    return [
        { kind: 'row', rowId: fx.r, label: 'live(r)' },
        { kind: 'row', rowId: fx.s, label: 'live(s)' },
        { kind: 'column', rowId: fx.r, column: 'grantee', label: 'r.grantee' },
        { kind: 'column', rowId: fx.r, column: 'active', label: 'r.active' },
        { kind: 'column', rowId: fx.s, column: 'grantee', label: 's.grantee' },
        { kind: 'column', rowId: fx.s, column: 'active', label: 's.active' },
        { kind: 'exists', where: { grantee: 'bob' }, label: 'exists bob' },
        { kind: 'exists', where: { grantee: 'carol' }, label: 'exists carol' },
        { kind: 'exists', where: { grantee: 'alice' }, label: 'exists alice' },
        { kind: 'exists', where: { active: 'yes' }, label: 'exists active=yes' },
        { kind: 'exists', where: { active: 'no' }, label: 'exists active=no' },
        { kind: 'exists', where: { grantee: 'bob', active: 'yes' }, label: 'exists bob, active=yes' },
    ];
}

type Answer = { decided: false; support: Set<string> } | { decided: true; value: string };

function answerOfTruth(t: Truth): Answer {
    return t.status === 'undecided' ? { decided: false, support: new Set(t.support) } : { decided: true, value: t.status };
}

function answerOfColumn(c: ColumnValue): Answer {
    if (c.status === 'undecided') return { decided: false, support: new Set(c.support) };
    return { decided: true, value: c.value === undefined ? '-' : json.toStringNormalized(c.value) };
}

async function viewAnswer(view: RTableViewImpl, read: Read): Promise<Answer> {
    if (read.kind === 'row') return answerOfTruth((await view.rowLiveness(read.rowId)).truth);
    if (read.kind === 'column') return answerOfColumn(await view.columnValue(read.rowId, read.column));
    return answerOfTruth(await view.existsMatching(read.where));
}

// The production rule: a view at D with the pending observes.
async function ruleAnswer(target: TableViewTarget, D: Version, from: Version, pending: Pending[], read: Read): Promise<Answer> {
    return viewAnswer(new RTableViewImpl(target, D, from, freshVerdictEvaluation(), undefined, pendingAdvances(pending)), read);
}

// Today's read: a view at `at` with nothing pending.
async function exactAnswer(target: TableViewTarget, at: Version, from: Version, read: Read): Promise<Answer> {
    return viewAnswer(new RTableViewImpl(target, at, from, freshVerdictEvaluation()), read);
}

function show(answer: Answer): string {
    return answer.decided ? answer.value : `undecided {${[...answer.support].sort().join(',')}}`;
}

function sameAnswer(a: Answer, b: Answer): boolean {
    return a.decided && b.decided && a.value === b.value;
}

// One configuration: verdicts, D, the pending observes and the horizon.
type Config = { status: Status; D: Version; pending: Pending[]; from: Version };

function describe(fx: Fixture, config: Config, read: Read): string {
    const names = (p: Position) => '{' + [...p].map((h) => fx.nameOf.get(h) ?? h.slice(0, 6)).sort().join(',') + '}';
    const voids = [...config.status].filter(([, s]) => s === 'void').map(([h]) => fx.nameOf.get(h) ?? h).sort().join(',');
    return `${read.label} at D=${names(config.D)}, ${config.pending.map((o) => `${o.name}=${names(o.version)}`).join(' ')}, `
        + `from=${names(config.from)}, void={${voids}}`;
}

type Tally = {
    configs: number; decided: number; undecided: number; violations: number;
    monotoneChecks: number; shortcutChecks: number; togetherOnly: number; examples: string[]; togetherExamples: string[];
};

function newTally(): Tally {
    return { configs: 0, decided: 0, undecided: 0, violations: 0, monotoneChecks: 0, shortcutChecks: 0, togetherOnly: 0, examples: [], togetherExamples: [] };
}

function violation(tally: Tally, message: string): void {
    tally.violations++;
    if (tally.examples.length < 5) tally.examples.push(message);
}

// Runs every check of one configuration on the production rule.
async function checkConfig(fx: Fixture, config: Config, tally: Tally): Promise<void> {
    const { status, D, pending, from } = config;
    const target = targetFor(fx, status);
    const causal = await fx.group.getCausalDag();
    const subsets: Pending[][] = [[], [pending[0]!], [pending[1]!], pending];
    tally.configs++;

    for (const read of readsOf(fx)) {
        const answer = await ruleAnswer(target, D, from, pending, read);
        const where = describe(fx, config, read);
        const exact: Answer[] = [];
        for (const subset of subsets) exact.push(await exactAnswer(target, union(D, ...subset.map((o) => o.version)), from, read));

        // A read that the two observes change only together: each alone leaves
        // its answer as it is at D. The rule must not decide it.
        if (sameAnswer(exact[0]!, exact[1]!) && sameAnswer(exact[0]!, exact[2]!) && !sameAnswer(exact[0]!, exact[3]!)) {
            tally.togetherOnly++;
            if (tally.togetherExamples.length < 2) tally.togetherExamples.push(`${where}: ${show(exact[0]!)} at D and with either observe, ${show(exact[3]!)} with both; the rule says ${show(answer)}`);
            if (answer.decided) violation(tally, `${where}: changed only by both observes together, yet decided ${show(answer)}`);
        }

        if (!answer.decided) {
            tally.undecided++;
            const names = new Set(pending.map((o) => o.name));
            if ([...answer.support].some((n) => !names.has(n)) || answer.support.size === 0) {
                violation(tally, `${where}: undecided with support ${show(answer)}`);
            }
            continue;
        }
        tally.decided++;

        // soundness: every completion agrees
        for (const [i, subset] of subsets.entries()) {
            if (!sameAnswer(answer, exact[i]!)) {
                violation(tally, `${where}: the rule says ${show(answer)}, the completion with {${subset.map((o) => o.name)}} says ${show(exact[i]!)}`);
                break;
            }
        }

        // monotonicity: decide one pending observe, live or void
        for (const [i, decided] of pending.entries()) {
            const rest = pending.filter((_, j) => j !== i);
            for (const [label, at] of [['live', union(D, decided.version)], ['void', D]] as const) {
                const after = await ruleAnswer(target, at, from, rest, read);
                tally.monotoneChecks++;
                if (!sameAnswer(answer, after)) {
                    violation(tally, `${where}: deciding ${decided.name} ${label} turns ${show(answer)} into ${show(after)}`);
                }
            }
        }
    }

    // exactness: observes whose versions D holds touch nothing
    for (const o of pending) {
        if (!await refVersionAtOrAbove(causal, D, o.version)) continue;
        for (const read of readsOf(fx)) {
            tally.shortcutChecks++;
            const answer = await ruleAnswer(target, D, from, [o], read);
            const exact = await exactAnswer(target, D, from, read);
            if (!answer.decided || !sameAnswer(answer, exact)) {
                violation(tally, `${describe(fx, config, read)}: ${o.name} is in D's history, yet the read is ${show(answer)}, not ${show(exact)}`);
            }
        }
    }
}

// Every D, V1 and V2 among single entries, V1 before V2 in fixture order,
// with every verdict live and the horizon D ∪ V1 ∪ V2.
async function enumerate(fx: Fixture, tally: Tally): Promise<void> {
    const status: Status = new Map();
    for (const d of fx.entries) {
        for (const [i, v1] of fx.entries.entries()) {
            for (const v2 of fx.entries.slice(i)) {
                const pending: Pending[] = [{ name: 'o1', version: version(v1) }, { name: 'o2', version: version(v2) }];
                await checkConfig(fx, { status, D: version(d), pending, from: union(version(d), version(v1), version(v2)) }, tally);
            }
        }
    }
}

function randomPosition(fx: Fixture, rng: Rng): Version {
    const p = version();
    const k = rng.int(1, 2);
    for (let i = 0; i < k; i++) p.add(rng.pick(fx.entries));
    return p;
}

// Random verdicts (each row op void with probability 1/4), D and pending
// versions of one or two entries, and a horizon that sometimes reaches past
// them.
async function sample(fx: Fixture, seed: number, trials: number, tally: Tally): Promise<void> {
    const rng = new Rng(seed);
    for (let t = 0; t < trials * SCALE; t++) {
        const status: Status = new Map();
        for (const h of fx.rowOps) status.set(h, rng.next() < 0.25 ? 'void' : 'live');
        const D = randomPosition(fx, rng);
        const pending: Pending[] = [{ name: 'o1', version: randomPosition(fx, rng) }, { name: 'o2', version: randomPosition(fx, rng) }];
        const from = union(D, pending[0]!.version, pending[1]!.version, rng.next() < 0.5 ? randomPosition(fx, rng) : version());
        await checkConfig(fx, { status, D, pending, from }, tally);
    }
}

// --- A re-implementation of the rule, with switches -------------------------
//
// Used only to run variants of the rule: the broken ones must fail on their
// pinned configuration, and comparing every entry must be sound but coarser.

type Variant = { name: string; slots: boolean; concurrent: boolean; union: boolean; predicate: boolean };

const RULE: Variant = { name: 'the rule', slots: true, concurrent: true, union: true, predicate: true };
const NO_SLOTS: Variant = { ...RULE, name: 'no slot check', slots: false };
const NO_CONCURRENT: Variant = { ...RULE, name: 'no concurrent comparison', concurrent: false };
const AT_D_ONLY: Variant = { ...RULE, name: 'candidates gathered at D only', union: false };
const EVERY_ENTRY: Variant = { ...RULE, name: 'every entry', predicate: false };

type Queries = { slots: string[]; history: EntryMetaFilter[]; concurrent: EntryMetaFilter[] };

class VariantRule {
    constructor(
        private readonly fx: Fixture,
        private readonly target: TableViewTarget,
        private readonly status: Status,
        private readonly from: Version,
        private readonly variant: Variant,
    ) {}

    private live: EntryPredicate = async (hash) => (this.status.get(hash) ?? 'live') === 'live';

    private async touches(D: Version, o: Pending, queries: Queries): Promise<boolean> {
        const wider = union(D, o.version);
        if (this.variant.slots) {
            const slots = await this.target.schemaSlotsBetween(D, wider, this.from);
            if (queries.slots.some((slot) => slots.has(slot))) return true;
        }
        const dag = await this.fx.table.getScopedDag();
        const predicate = this.variant.predicate ? this.live : undefined;
        for (const filter of queries.history) {
            if (!set.eq(await dag.findCoverWithFilter(D, filter, predicate), await dag.findCoverWithFilter(wider, filter, predicate))) return true;
        }
        if (this.variant.concurrent) {
            for (const filter of queries.concurrent) {
                if (!set.eq(await dag.findConcurrentCoverWithFilter(this.from, D, filter, predicate),
                    await dag.findConcurrentCoverWithFilter(this.from, wider, filter, predicate))) return true;
            }
        }
        return false;
    }

    private async touching(D: Version, pending: Pending[], queries: Queries): Promise<Truth | undefined> {
        const names: string[] = [];
        for (const o of pending) if (await this.touches(D, o, queries)) names.push(o.name);
        return names.length > 0 ? undecidedOn(names) : undefined;
    }

    private async exactView(at: Version): Promise<RTableViewImpl> {
        return new RTableViewImpl(this.target, at, this.from, freshVerdictEvaluation());
    }

    async rowLiveness(D: Version, pending: Pending[], rowId: B64Hash): Promise<Truth> {
        const incarnation = (await this.target.resolveSchemaView(D, this.from)).getTableIncarnation(TABLE);
        const tag = incarnation === undefined ? undefined : rowTag(incarnation, rowId);
        const touched = await this.touching(D, pending, {
            slots: [existenceSlot(TABLE)],
            history: tag === undefined ? [] : [{ containsValues: { rows: [tag] } }],
            concurrent: tag === undefined ? [] : [{ containsValues: { barrier: ['t'], rows: [tag] } }],
        });
        return touched ?? (await (await this.exactView(D)).rowLiveness(rowId)).truth;
    }

    async columnValue(D: Version, pending: Pending[], rowId: B64Hash, column: string): Promise<ColumnValue> {
        const incarnation = (await this.target.resolveSchemaView(D, this.from)).getColumnIncarnation(TABLE, column);
        const touched = await this.touching(D, pending, {
            slots: [existenceSlot(TABLE), columnSlot(TABLE, column)],
            history: incarnation === undefined ? [] : [{ containsValues: { cols: [colTag(rowId, incarnation, column)] } }],
            concurrent: [],
        });
        return (touched as ColumnValue | undefined) ?? (await this.exactView(D)).columnValue(rowId, column);
    }

    async existsMatching(D: Version, pending: Pending[], where: { [field: string]: json.Literal }): Promise<Truth> {
        const fields = Object.keys(where);
        const touched = await this.touching(D, pending, {
            slots: [existenceSlot(TABLE), ...fields.map((f) => columnSlot(TABLE, f))], history: [], concurrent: [],
        });
        if (touched !== undefined) return touched;
        const schema = await this.target.resolveSchemaView(D, this.from);
        if (!schema.hasTable(TABLE)) return FALSE;
        const pub = new Set(schema.getPubColumns(TABLE));
        if (fields.some((f) => !pub.has(f))) return FALSE;
        const def = schema.getTable(TABLE)!;

        const gatherAt = this.variant.union ? union(D, ...pending.map((o) => o.version)) : D;
        const gatherer = new RTableViewImpl(this.target, D, this.from, freshVerdictEvaluation()) as unknown as {
            candidateRowIds(w: object, at: Version): Promise<B64Hash[]>;
        };
        const candidates = await gatherer.candidateRowIds(where, gatherAt);
        return someTruth(candidates, async (rowId) => everyTruth([
            ...fields.map((field) => async (): Promise<Truth> => {
                const read = await this.columnValue(D, pending, rowId, field);
                if (read.status === 'undecided') return read;
                const value = read.value ?? def.columns[field]?.default;
                return truthOf(value !== undefined && sameValue(value, where[field]));
            }),
            async () => this.rowLiveness(D, pending, rowId),
        ], (check) => check()));
    }

    async answer(D: Version, pending: Pending[], read: Read): Promise<Answer> {
        if (read.kind === 'row') return answerOfTruth(await this.rowLiveness(D, pending, read.rowId));
        if (read.kind === 'column') return answerOfColumn(await this.columnValue(D, pending, read.rowId, read.column));
        return answerOfTruth(await this.existsMatching(D, pending, read.where));
    }
}

// The first decided answer of the variant that some completion contradicts.
async function variantViolation(fx: Fixture, variant: Variant, config: Config, read: Read): Promise<string | undefined> {
    const target = targetFor(fx, config.status);
    const rule = new VariantRule(fx, target, config.status, config.from, variant);
    const answer = await rule.answer(config.D, config.pending, read);
    if (!answer.decided) return undefined;
    const subsets: Pending[][] = [[], [config.pending[0]!], [config.pending[1]!], config.pending];
    for (const subset of subsets) {
        const exact = await exactAnswer(target, union(config.D, ...subset.map((o) => o.version)), config.from, read);
        if (!sameAnswer(answer, exact)) {
            return `${describe(fx, config, read)}: "${variant.name}" says ${show(answer)}, the completion with {${subset.map((o) => o.name)}} says ${show(exact)}`;
        }
    }
    return undefined;
}

// One configuration per broken variant, found by search, on which that
// variant decides an answer some completion contradicts.
type Pin = { variant: Variant; read: string; D: string[]; o1: string[]; o2: string[]; from: string[]; voids: string[] };

const PINNED: Pin[] = [
    { variant: NO_SLOTS, read: 'exists active=no', D: ['G0'], o1: ['G0'], o2: ['Dep1'], from: ['Dep1', 'G0'], voids: [] },
    { variant: NO_CONCURRENT, read: 'live(s)', D: ['Us'], o1: ['U1'], o2: ['I'], from: ['Ds', 'I', 'U1', 'U2', 'Us'], voids: ['I2'] },
    { variant: AT_D_ONLY, read: 'exists alice', D: ['G0'], o1: ['G0'], o2: ['I'], from: ['G0', 'I'], voids: [] },
];

function pinnedConfig(fx: Fixture, pin: Pin): Config {
    const at = (names: string[]) => version(...names.map((n) => fx.labels.get(n)!));
    return {
        status: new Map(pin.voids.map((n) => [fx.labels.get(n)!, 'void' as const])),
        D: at(pin.D),
        pending: [{ name: 'o1', version: at(pin.o1) }, { name: 'o2', version: at(pin.o2) }],
        from: at(pin.from),
    };
}

function report(name: string, tally: Tally): void {
    console.log(`      ${name}: ${JSON.stringify({ ...tally, examples: undefined, togetherExamples: undefined })}`);
    for (const example of tally.examples) console.log(`        ${example}`);
    for (const example of tally.togetherExamples) console.log(`        only together: ${example}`);
}

const touchRuleTests = {
    title: '[TOUCHR] The touch rule on production views, checked against exact reads at every completion',
    tests: [
        {
            name: '[TOUCHR01] enumerated: a decided read holds at every completion, the shortcut is exact, and deciding an observe never undoes a decided read',
            invoke: async () => {
                const fx = await createFixture();
                const tally = newTally();
                await enumerate(fx, tally);
                report('enumerated', tally);
                assertTrue(tally.violations === 0, 'the rule broke a guarantee');
                assertTrue(tally.decided > 0 && tally.undecided > 0 && tally.shortcutChecks > 0, 'the checks ran');
                assertTrue(tally.togetherOnly > 0, 'the fixture keeps a read that two observes change only together');
            }
        },
        {
            name: '[TOUCHR02] sampled with void verdicts and wider horizons: the same guarantees',
            invoke: async () => {
                const fx = await createFixture();
                const tally = newTally();
                await sample(fx, 20261104, 150, tally);
                report('sampled', tally);
                assertTrue(tally.violations === 0, 'the rule broke a guarantee');
                assertTrue(tally.decided > 0 && tally.undecided > 0, 'both outcomes occurred');
            }
        },
        {
            name: '[TOUCHR03] each broken variant decides a wrong answer on its pinned configuration, where the rule holds',
            invoke: async () => {
                const fx = await createFixture();
                for (const pin of PINNED) {
                    const config = pinnedConfig(fx, pin);
                    const read = readsOf(fx).find((r) => r.label === pin.read)!;
                    const wrong = await variantViolation(fx, pin.variant, config, read);
                    console.log(`      ${wrong ?? `"${pin.variant.name}" no longer fails on its pinned configuration`}`);
                    assertTrue(wrong !== undefined, `the fixture lost its counterexample to "${pin.variant.name}"`);
                    const tally = newTally();
                    await checkConfig(fx, config, tally);
                    report(`the rule there`, tally);
                    assertTrue(tally.violations === 0, 'the rule broke a guarantee on a pinned configuration');
                }
            }
        },
        {
            name: '[TOUCHR04] comparing every entry instead of the live ones is sound too, and decides less',
            invoke: async () => {
                const fx = await createFixture();
                const rng = new Rng(7);
                let live = 0;
                let every = 0;
                const violations: string[] = [];
                for (let t = 0; t < 120 * SCALE; t++) {
                    const status: Status = new Map();
                    for (const h of fx.rowOps) status.set(h, rng.next() < 0.25 ? 'void' : 'live');
                    const D = randomPosition(fx, rng);
                    const pending: Pending[] = [{ name: 'o1', version: randomPosition(fx, rng) }, { name: 'o2', version: randomPosition(fx, rng) }];
                    const from = union(D, pending[0]!.version, pending[1]!.version);
                    const config: Config = { status, D, pending, from };
                    const target = targetFor(fx, status);
                    const coarse = new VariantRule(fx, target, status, from, EVERY_ENTRY);
                    for (const read of readsOf(fx)) {
                        if ((await ruleAnswer(target, D, from, pending, read)).decided) live++;
                        if ((await coarse.answer(D, pending, read)).decided) every++;
                        const wrong = await variantViolation(fx, EVERY_ENTRY, config, read);
                        if (wrong !== undefined) violations.push(wrong);
                    }
                }
                console.log(`      live entries: ${live} decided; every entry: ${every} decided`);
                for (const v of violations.slice(0, 3)) console.log(`        ${v}`);
                assertTrue(violations.length === 0, 'comparing every entry decided a read that some completion contradicts');
                assertTrue(every < live, 'comparing live entries decides more');
            }
        },
        {
            name: '[TOUCHR05] cases: a void write touches nothing and the same write live does; a version D holds touches nothing; a column write that only becomes concurrent touches nothing; a delete that goes from after D to concurrent touches; a deploy that resets a searched default touches with no candidate at D',
            invoke: async () => {
                const fx = await createFixture();
                const at = (...names: string[]) => version(...names.map((n) => fx.labels.get(n)!));
                const voids = (...names: string[]): Status => new Map(names.map((n) => [fx.labels.get(n)!, 'void' as const]));
                const read = (label: string) => readsOf(fx).find((r) => r.label === label)!;
                const answerAt = async (status: Status, D: Version, pending: [string, Version][], from: Version, label: string) =>
                    ruleAnswer(targetFor(fx, status), D, from, pending.map(([name, v]) => ({ name, version: v })), read(label));

                // U1 writes r.grantee = bob above I.
                const voidU1 = await answerAt(voids('U1'), at('I'), [['o1', at('U1')]], at('I', 'U1'), 'r.grantee');
                assertTrue(voidU1.decided && voidU1.value === '"alice"', `a void write touches nothing (${show(voidU1)})`);
                const liveU1 = await answerAt(new Map(), at('I'), [['o1', at('U1')]], at('I', 'U1'), 'r.grantee');
                assertTrue(!liveU1.decided && liveU1.support.has('o1'), `the same write live touches (${show(liveU1)})`);

                // U1 is in the history of U3.
                for (const r of readsOf(fx)) {
                    const shortcut = await ruleAnswer(targetFor(fx, new Map()), at('U3'), at('U3'), [{ name: 'o1', version: at('U1') }], r);
                    const exact = await exactAnswer(targetFor(fx, new Map()), at('U3'), at('U3'), r);
                    assertTrue(sameAnswer(shortcut, exact), `${r.label}: a version D holds touches nothing (${show(shortcut)} vs ${show(exact)})`);
                }

                // With Us imported, U1 (bob, above I) is concurrent with {I, Us} and in neither history.
                const onlyConcurrent = await answerAt(new Map(), at('I'), [['o1', at('Us')]], at('I', 'Us', 'U1'), 'r.grantee');
                assertTrue(onlyConcurrent.decided && onlyConcurrent.value === '"alice"', `a column write that only becomes concurrent touches nothing (${show(onlyConcurrent)})`);

                // Ds deletes s after Us: invisible from D = {Us}, concurrent with {Us, U1}.
                const deleteConcurrent = await answerAt(new Map(), at('Us'), [['o1', at('U1')]], at('Ds', 'U1', 'Us'), 'live(s)');
                assertTrue(!deleteConcurrent.decided && deleteConcurrent.support.has('o1'), `a delete that goes from after D to concurrent touches (${show(deleteConcurrent)})`);

                // Dep1 resets `active` with default no; at D nothing carries active = no.
                const resetDefault = await answerAt(new Map(), at('G0'), [['o1', at('Dep1')]], at('Dep1', 'G0'), 'exists active=no');
                assertTrue(!resetDefault.decided && resetDefault.support.has('o1'), `a deploy that resets a searched default touches (${show(resetDefault)})`);
            }
        },
        {
            name: '[TOUCHR06] two deploys on concurrent branches each touch a read of both columns on its own, and every completion folds the other in as concurrent',
            invoke: async () => {
                // caps has x and y, both defaulting to n; r writes neither. SA resets
                // x to default p, SB resets y to default q, concurrently, deployed on
                // two branches of the group above r's insert. The horizon holds both
                // deploys, as foreignFrom holds every pending version, so each
                // completion folds the other branch's deploy in as a concurrent
                // barrier: two deploys cannot change a read only together. Reads that
                // two observes change only together come from row writes ([TOUCHR01]).
                // Each observe must still touch the read on its own.
                const ctx = createMockRContext({ selfValidate: true });
                ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
                ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
                const admin = fixedAdmin();
                const xy: TableDef = {
                    name: TABLE,
                    columns: {
                        grantee: { type: 'string', pub: true },
                        x: { type: 'string', pub: true, default: 'n' },
                        y: { type: 'string', pub: true, default: 'n' },
                    },
                    concurrentDeletes: true,
                    restrictions: [{ on: 'all', rule: { p: 'true' } }],
                };
                const schema = (await ctx.createObject(await RSchemaImpl.create({
                    name: 'touchrule:xy', creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }], tables: [xy],
                }))) as RSchemaImpl;
                const pinned = await (await schema.getScopedDag()).getFrontier();
                const group = (await ctx.createObject(await RTableGroupImpl.create({
                    name: 'touchrule-xy', seed: 'touchrule-xy', schemaRef: schema.getId(), schemaVersion: pinned,
                }))) as RTableGroupImpl;
                const table = await group.getTable(TABLE);
                const base = await (await group.getScopedDag()).getFrontier();
                const I = await table.insert('r', { grantee: 'bob' }, undefined, base);
                const reset = (column: string, value: string): MigrationRule[] => [
                    { rule: 'drop-column', table: TABLE, column },
                    { rule: 'add-column', table: TABLE, column, def: { type: 'string', pub: true, default: value } },
                ];
                const sA = await schema.updateSchema(reset('x', 'p'), admin, 'x to p', pinned);
                const sB = await schema.updateSchema(reset('y', 'q'), admin, 'y to q', pinned);
                const depA = await group.deploy(version(sA), undefined, version(I));
                const depB = await group.deploy(version(sB), undefined, version(I));

                const target = Object.create(table) as TableViewTarget;
                target.entryStatusIn = async (): Promise<Verdict<OpVoidDetail>> => ({ status: 'live' });
                const read: Read = { kind: 'exists', where: { x: 'p', y: 'q' }, label: 'exists x=p, y=q' };
                const D = version(I);
                const from = version(depA, depB);
                const oA: Pending = { name: 'oA', version: version(depA) };
                const oB: Pending = { name: 'oB', version: version(depB) };

                const exact = async (...ps: Pending[]) => exactAnswer(target, union(D, ...ps.map((o) => o.version)), from, read);
                const alone = [await exact(), await exact(oA), await exact(oB)];
                const both = await exact(oA, oB);
                console.log(`      exact: ${alone.map(show).join(', ')} at D, with A, with B; ${show(both)} with both`);
                assertTrue(sameAnswer(alone[0]!, { decided: true, value: 'false' }) && sameAnswer(alone[1]!, both)
                    && sameAnswer(alone[2]!, both) && sameAnswer(both, { decided: true, value: 'true' }),
                    'each completion with a deploy sees both resets');

                for (const pending of [[oA, oB], [oA], [oB]]) {
                    const answer = await ruleAnswer(target, D, from, pending, read);
                    console.log(`      the rule with {${pending.map((o) => o.name)}} pending: ${show(answer)}`);
                    assertTrue(!answer.decided && pending.every((o) => answer.support.has(o.name)),
                        'each deploy touches the read on its own');
                }
            }
        },
    ]
};

export { touchRuleTests };

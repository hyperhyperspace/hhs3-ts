import { testing } from "@hyper-hyper-space/hhs3_util";

import {
    VerdictEvaluation, Truth, Verdict, TRUE, FALSE, undecidedOn,
    allTruth, anyTruth, notTruth, everyTruth, someTruth, verdictTruth,
} from "../src/verdict_evaluation.js";

// A synthetic program: each key's rule reads other keys through `read`.
type Read = (key: string) => Promise<Truth>;
type Rule = (read: Read) => Promise<Truth>;
type Program = { [key: string]: Rule };

const at = (key: string): Rule => (read) => read(key);
const not = (rule: Rule): Rule => async (read) => notTruth(await rule(read));
const and = (...rules: Rule[]): Rule => (read) => everyTruth(rules, (rule) => rule(read));
const or = (...rules: Rule[]): Rule => (read) => someTruth(rules, (rule) => rule(read));
const yes: Rule = async () => TRUE;

function verdictOf(t: Truth): Verdict<string> {
    if (t.status === 'true') return { status: 'live' };
    if (t.status === 'false') return { status: 'void', reason: 'rule' };
    return t;
}

function label(v: Verdict<string>): string {
    return v.status === 'void' ? 'void:' + v.reason : v.status;
}

async function solveIn(program: Program, order: string[]): Promise<{ [key: string]: string }> {
    const evaluation = new VerdictEvaluation<string>('cycle');
    const resolve = (key: string): Promise<Verdict<string>> =>
        evaluation.resolve(key, async () => verdictOf(await program[key](async (k) => verdictTruth(await resolve(k)))));
    const result: { [key: string]: string } = {};
    for (const key of order) result[key] = label(await resolve(key));
    return result;
}

function permutations<T>(items: T[]): T[][] {
    if (items.length <= 1) return [items];
    const result: T[][] = [];
    items.forEach((item, i) => {
        for (const rest of permutations([...items.slice(0, i), ...items.slice(i + 1)])) result.push([item, ...rest]);
    });
    return result;
}

// Resolves the program from every entry order, each in a fresh evaluation,
// and checks every order gives `expected`.
async function expectInEveryOrder(program: Program, expected: { [key: string]: string }, what: string): Promise<void> {
    for (const order of permutations(Object.keys(program))) {
        const result = await solveIn(program, order);
        for (const key of Object.keys(expected)) {
            testing.assertEquals(result[key], expected[key], `${what}: '${key}' entered in order ${order.join(',')}`);
        }
    }
}

async function testPositiveLoop() {
    await expectInEveryOrder({ p: at('p') }, { p: 'void:rule' }, 'p <- p');
    await expectInEveryOrder({ p: at('q'), q: at('p') }, { p: 'void:rule', q: 'void:rule' }, 'p <- q <- p');
}

async function testNegativeRings() {
    await expectInEveryOrder({ a: not(at('b')), b: not(at('a')) },
        { a: 'void:cycle', b: 'void:cycle' }, '2-ring');
    await expectInEveryOrder({ a: not(at('c')), b: not(at('a')), c: not(at('b')) },
        { a: 'void:cycle', b: 'void:cycle', c: 'void:cycle' }, '3-ring');
    await expectInEveryOrder({ p: not(at('p')) }, { p: 'void:cycle' }, 'p <- not p');
}

async function testWellFoundedNotKripkeKleene() {
    await expectInEveryOrder({ p: at('p'), q: not(at('p')) }, { p: 'void:rule', q: 'live' }, 'p <- p; q <- not p');
}

async function testDependentOfCollapsedCore() {
    // a reads d, but `d or true` does not rest on d, so d is above the core
    await expectInEveryOrder({
        a: and(not(at('b')), or(at('d'), yes)),
        b: not(at('a')),
        d: not(at('a')),
    }, { a: 'void:cycle', b: 'void:cycle', d: 'live' }, 'core {a, b} with dependent d');
}

async function testArmOrderDoesNotReachCollapse() {
    const expected = { m: 'void:cycle', c: 'void:cycle', a: 'live', b: 'live' };
    const rest = { b: yes, c: not(at('m')), a: not(at('m')) };
    await expectInEveryOrder({ m: and(or(at('a'), at('b')), at('c')), ...rest }, expected, 'm = (a or b) and c');
    await expectInEveryOrder({ m: and(or(at('b'), at('a')), at('c')), ...rest }, expected, 'm = (b or a) and c');
}

// m reads x only once y is decided true; y is decided only inside the solve.
const ifThen = (cond: string, then: string): Rule => async (read) => {
    const c = await read(cond);
    if (c.status === 'undecided') return undecidedOn(c.support);
    return c.status === 'true' ? read(then) : FALSE;
};

async function testGrowth() {
    await expectInEveryOrder({
        m: ifThen('y', 'x'),
        y: not(at('n')),
        n: and(at('n'), at('m')),
        x: at('m'),
    }, { m: 'void:rule', y: 'live', n: 'void:rule', x: 'void:rule' }, 'x joins the component during the solve');
}

async function testMergeWithOuterFrame() {
    // i's component first looks complete, but g, reached during its solve,
    // reads o below it
    await expectInEveryOrder({
        o: at('i'),
        i: ifThen('j', 'g'),
        j: not(at('k')),
        k: and(at('k'), at('i')),
        g: at('o'),
    }, { o: 'void:rule', i: 'void:rule', j: 'live', k: 'void:rule', g: 'void:rule' }, 'g reaches below the root');
}

async function testThrowUnwinds() {
    const evaluation = new VerdictEvaluation<string>('cycle');
    let failing = true;
    const diagnoseA = async (): Promise<Verdict<string>> => {
        await evaluation.resolve('b', async () => ({ status: 'live' }));
        if (failing) throw new Error('boom');
        return { status: 'live' };
    };
    let threw = false;
    try { await evaluation.resolve('a', diagnoseA); } catch { threw = true; }
    testing.assertTrue(threw, 'the diagnosis error propagates');
    failing = false;
    testing.assertEquals((await evaluation.resolve('a', diagnoseA)).status, 'live', 'a resolves after the failed attempt');
}

const VALUES: Truth[] = [TRUE, FALSE, undecidedOn(['u'])];

function completions(inputs: Truth[]): boolean[][] {
    if (inputs.length === 0) return [[]];
    const [first, ...rest] = inputs;
    const tails = completions(rest);
    const heads = first.status === 'true' ? [true] : first.status === 'false' ? [false] : [true, false];
    return heads.flatMap((h) => tails.map((t) => [h, ...t]));
}

function vectors(n: number): Truth[][] {
    if (n === 0) return [[]];
    return vectors(n - 1).flatMap((v) => VALUES.map((x, i) => [...v, i === 2 ? undecidedOn(['u' + v.length]) : x]));
}

async function testKleeneGuarantees() {
    const rules: [string, (ts: Truth[]) => Truth, (bs: boolean[]) => boolean][] = [
        ['and', allTruth, (bs) => bs.every((b) => b)],
        ['or', anyTruth, (bs) => bs.some((b) => b)],
        ['not', (ts) => notTruth(ts[0]), (bs) => !bs[0]],
    ];
    for (const [name, rule, exact] of rules) {
        for (const n of name === 'not' ? [1] : [1, 2, 3]) {
            for (const inputs of vectors(n)) {
                const result = rule(inputs);
                const outcomes = completions(inputs).map(exact);
                if (inputs.every((t) => t.status !== 'undecided')) {
                    testing.assertEquals(result.status, outcomes[0] ? 'true' : 'false', `${name} is exact on decided inputs`);
                }
                if (result.status !== 'undecided') {
                    testing.assertTrue(outcomes.every((o) => o === (result.status === 'true')), `${name} is sound`);
                } else {
                    const expected = new Set(inputs.flatMap((t) => t.status === 'undecided' ? [...t.support] : []));
                    testing.assertEquals([...result.support].sort().join(), [...expected].sort().join(),
                        `${name}'s undecided result rests on its undecided inputs`);
                }
                inputs.forEach((t, i) => {
                    if (t.status !== 'undecided') return;
                    for (const refined of [TRUE, FALSE]) {
                        const after = rule(inputs.map((x, j) => j === i ? refined : x));
                        if (result.status !== 'undecided') {
                            testing.assertEquals(after.status, result.status, `${name} is monotone`);
                        }
                    }
                });
            }
        }
    }

    // the lazy forms agree with the strict ones in every order
    for (const inputs of vectors(3)) {
        for (const order of permutations([0, 1, 2])) {
            const ordered = order.map((i) => inputs[i]);
            testing.assertEquals((await everyTruth(ordered, async (t) => t)).status, allTruth(inputs).status, 'everyTruth matches allTruth');
            testing.assertEquals((await someTruth(ordered, async (t) => t)).status, anyTruth(inputs).status, 'someTruth matches anyTruth');
        }
    }
}

export const verdictEvaluationSuite = {
    title: '[VERDICT] Well-founded verdict evaluation',
    tests: [
        { name: '[VERDICT01] a positive loop is void with its own reason', invoke: testPositiveLoop },
        { name: '[VERDICT02] negative rings collapse to void', invoke: testNegativeRings },
        { name: '[VERDICT03] p <- p; q <- not p makes q live (well-founded, not Kripke-Kleene)', invoke: testWellFoundedNotKripkeKleene },
        { name: '[VERDICT04] a dependent of a collapsed core gets a real verdict', invoke: testDependentOfCollapsedCore },
        { name: '[VERDICT05] arm order does not reach the collapse', invoke: testArmOrderDoesNotReachCollapse },
        { name: '[VERDICT06] an entry reached during the solve joins the component', invoke: testGrowth },
        { name: '[VERDICT07] a component that reaches below its root merges with the outer one', invoke: testMergeWithOuterFrame },
        { name: '[VERDICT08] a throwing diagnosis unwinds the stack', invoke: testThrowUnwinds },
        { name: '[VERDICT09] Kleene combinators are exact, sound and monotone', invoke: testKleeneGuarantees },
    ],
};

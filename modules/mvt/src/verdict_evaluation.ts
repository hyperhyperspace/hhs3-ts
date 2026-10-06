// Well-founded verdicts for entries whose validity depends on other entries'
// verdicts (modules/rdb/VOID_SEMANTICS.md, section 4).
//
// A verdict is `live`, `void` with a reason, or `undecided`. Undecided exists
// only inside one VerdictEvaluation, for entries of the component being
// solved; every stored answer is live or void.
//
// `resolve(key, diagnose)` runs Tarjan's algorithm over the entries the
// diagnoses read. A read of an entry still on the stack lowers the reader's
// lowlink and returns undecided. When an entry finishes with its lowlink equal
// to its index it is the root of a complete component, which is solved here:
//
//   1. the alternating fixpoint from all-undecided (L = known live, Up =
//      possibly live) gives the well-founded value of every member;
//   2. members still undecided collapse to void bottom-up: build the graph of
//      undecided members from their supports, collapse its bottom strongly
//      connected components, and re-solve the rest;
//   3. a last evaluation of every member, all values decided, yields reasons.
//
// A read during the solve that reaches an entry not yet seen pushes a new
// frame. If it reads a member it joins the component and the solve restarts
// with it; if it reads a frame below the root, the component is not complete
// after all and the root returns undecided to its reader.
//
// Requires ONE sequential traversal per evaluation: the running stack
// identifies the reader of each resolve.

export type Undecided = { status: 'undecided'; support: ReadonlySet<string> };
export type Truth = { status: 'true' } | { status: 'false' } | Undecided;
export type Verdict<R> =
    | { status: 'live' }
    | { status: 'void'; reason: R }
    | Undecided;

export const TRUE: Truth = { status: 'true' };
export const FALSE: Truth = { status: 'false' };

export function truthOf(value: boolean): Truth {
    return value ? TRUE : FALSE;
}

export function undecidedOn(support: Iterable<string>): Undecided {
    return { status: 'undecided', support: new Set(support) };
}

export function verdictTruth<R>(verdict: Verdict<R>): Truth {
    if (verdict.status === 'live') return TRUE;
    if (verdict.status === 'void') return FALSE;
    return verdict;
}

export function notTruth(t: Truth): Truth {
    if (t.status === 'true') return FALSE;
    if (t.status === 'false') return TRUE;
    return t;
}

// Kleene `and`: false if any input is false, else undecided (supports united)
// if any input is undecided, else true.
export function allTruth(truths: Iterable<Truth>): Truth {
    let support: Set<string> | undefined;
    for (const t of truths) {
        if (t.status === 'false') return FALSE;
        if (t.status === 'undecided') support = unite(support, t.support);
    }
    return support === undefined ? TRUE : { status: 'undecided', support };
}

// Kleene `or`: true if any input is true, else undecided (supports united) if
// any input is undecided, else false.
export function anyTruth(truths: Iterable<Truth>): Truth {
    let support: Set<string> | undefined;
    for (const t of truths) {
        if (t.status === 'true') return TRUE;
        if (t.status === 'undecided') support = unite(support, t.support);
    }
    return support === undefined ? FALSE : { status: 'undecided', support };
}

// `and` over lazily tested items, stopping at the first false. The order of
// `items` changes which tests run, never the result.
export async function everyTruth<T>(items: Iterable<T>, test: (item: T) => Promise<Truth>): Promise<Truth> {
    let support: Set<string> | undefined;
    for (const item of items) {
        const t = await test(item);
        if (t.status === 'false') return FALSE;
        if (t.status === 'undecided') support = unite(support, t.support);
    }
    return support === undefined ? TRUE : { status: 'undecided', support };
}

// `or` over lazily tested items, stopping at the first true.
export async function someTruth<T>(items: Iterable<T>, test: (item: T) => Promise<Truth>): Promise<Truth> {
    let support: Set<string> | undefined;
    for (const item of items) {
        const t = await test(item);
        if (t.status === 'true') return TRUE;
        if (t.status === 'undecided') support = unite(support, t.support);
    }
    return support === undefined ? FALSE : { status: 'undecided', support };
}

function unite(into: Set<string> | undefined, more: ReadonlySet<string>): Set<string> {
    const result = into ?? new Set<string>();
    for (const key of more) result.add(key);
    return result;
}

type Frame<R> = {
    key: string;
    index: number;
    lowlink: number;
    diagnose: () => Promise<Verdict<R>>;
    // This member's value in the solve that owns it; undefined outside a solve.
    value: Verdict<R> | undefined;
};

type FixpointResult = { live: Set<string>; possible: Set<string> } | 'grown' | 'merged';

export class VerdictEvaluation<R> {
    private readonly answers = new Map<string, Verdict<R>>();
    private readonly onStack = new Map<string, Frame<R>>();
    private readonly stack: Frame<R>[] = [];
    private readonly running: Frame<R>[] = [];
    private nextIndex = 0;

    constructor(private readonly undecidedReason: R) {}

    async resolve(key: string, diagnose: () => Promise<Verdict<R>>): Promise<Verdict<R>> {
        const answered = this.answers.get(key);
        if (answered !== undefined) return answered;

        const open = this.onStack.get(key);
        if (open !== undefined) {
            this.lowerReader(open.index);
            return this.readValue(open);
        }

        const frame: Frame<R> = { key, index: this.nextIndex, lowlink: this.nextIndex, diagnose, value: undefined };
        this.nextIndex++;
        this.onStack.set(key, frame);
        this.stack.push(frame);

        try {
            const verdict = await this.run(frame);
            if (frame.lowlink === frame.index) await this.complete(frame, verdict);
        } catch (e) {
            this.unwindFrom(frame);
            throw e;
        }

        this.lowerReader(frame.lowlink);
        return this.answers.get(key) ?? this.readValue(frame);
    }

    private async run(frame: Frame<R>): Promise<Verdict<R>> {
        this.running.push(frame);
        try {
            return await frame.diagnose();
        } finally {
            this.running.pop();
        }
    }

    private lowerReader(index: number): void {
        const reader = this.running[this.running.length - 1];
        if (reader !== undefined && index < reader.lowlink) reader.lowlink = index;
    }

    private readValue(frame: Frame<R>): Verdict<R> {
        if (frame.value === undefined || frame.value.status === 'undecided') {
            return { status: 'undecided', support: new Set([frame.key]) };
        }
        return frame.value;
    }

    private async complete(root: Frame<R>, rootVerdict: Verdict<R>): Promise<void> {
        const start = this.stack.lastIndexOf(root);
        if (this.stack.length === start + 1 && rootVerdict.status !== 'undecided') {
            this.finish(start, new Map([[root.key, rootVerdict]]));
            return;
        }
        const verdicts = await this.solve(root, start);
        if (verdicts === undefined) {
            for (let i = start; i < this.stack.length; i++) this.stack[i].value = undefined;
            return;
        }
        this.finish(start, verdicts);
    }

    private finish(start: number, verdicts: Map<string, Verdict<R>>): void {
        for (let i = start; i < this.stack.length; i++) {
            const frame = this.stack[i];
            this.answers.set(frame.key, verdicts.get(frame.key)!);
            this.onStack.delete(frame.key);
            frame.value = undefined;
        }
        this.stack.length = start;
    }

    private unwindFrom(frame: Frame<R>): void {
        const start = this.stack.lastIndexOf(frame);
        if (start < 0) return;
        for (let i = start; i < this.stack.length; i++) {
            this.onStack.delete(this.stack[i].key);
            this.stack[i].value = undefined;
        }
        this.stack.length = start;
    }

    // The final verdict of every member, or undefined when the component turned
    // out to reach a frame below its root.
    private async solve(root: Frame<R>, start: number): Promise<Map<string, Verdict<R>> | undefined> {
        restart: for (;;) {
            const members = this.stack.slice(start);
            const collapsed = new Set<string>();
            for (;;) {
                const fixpoint = await this.fixpoint(root, start, members, collapsed);
                if (fixpoint === 'merged') return undefined;
                if (fixpoint === 'grown') continue restart;
                const { live, possible } = fixpoint;

                const undecided = members.filter((m) =>
                    !collapsed.has(m.key) && possible.has(m.key) && !live.has(m.key));
                if (undecided.length > 0) {
                    const graph = await this.supportGraph(root, start, members, undecided, live, possible, collapsed);
                    if (graph === 'merged') return undefined;
                    if (graph === 'grown') continue restart;
                    for (const key of bottomComponents(graph)) collapsed.add(key);
                    continue;
                }

                const verdicts = await this.finalVerdicts(root, start, members, live, collapsed);
                if (verdicts === 'merged') return undefined;
                if (verdicts === 'grown') continue restart;
                return verdicts;
            }
        }
    }

    // The alternating fixpoint over the members not yet collapsed, from all
    // undecided: grow L with Up fixed, then rebuild Up from L, until Up stops
    // shrinking.
    private async fixpoint(
        root: Frame<R>, start: number, members: Frame<R>[], collapsed: Set<string>,
    ): Promise<FixpointResult> {
        const open = members.filter((m) => !collapsed.has(m.key));
        const live = new Set<string>();
        let possible = new Set(open.map((m) => m.key));
        for (;;) {
            for (let changed = true; changed;) {
                changed = false;
                for (const m of open) {
                    if (live.has(m.key)) continue;
                    this.interpret(members, live, possible, collapsed);
                    const verdict = await this.run(m);
                    const status = this.componentStatus(root, start, members.length);
                    if (status !== 'same') return status;
                    if (verdict.status === 'live') {
                        live.add(m.key);
                        changed = true;
                    }
                }
            }

            const next = new Set(live);
            for (let changed = true; changed;) {
                changed = false;
                for (const m of open) {
                    if (next.has(m.key)) continue;
                    this.interpret(members, live, next, collapsed);
                    const verdict = await this.run(m);
                    const status = this.componentStatus(root, start, members.length);
                    if (status !== 'same') return status;
                    if (verdict.status !== 'void') {
                        next.add(m.key);
                        changed = true;
                    }
                }
            }

            for (const key of next) {
                if (!possible.has(key)) throw new Error(`verdict evaluation: '${key}' became possibly live again (a rule is not monotone)`);
            }
            if (next.size === possible.size) return { live, possible };
            possible = next;
        }
    }

    // Edges from each undecided member to the undecided members in its support,
    // under the fixpoint's values.
    private async supportGraph(
        root: Frame<R>, start: number, members: Frame<R>[], undecided: Frame<R>[],
        live: Set<string>, possible: Set<string>, collapsed: Set<string>,
    ): Promise<Map<string, Set<string>> | 'grown' | 'merged'> {
        const undecidedKeys = new Set(undecided.map((m) => m.key));
        const graph = new Map<string, Set<string>>();
        this.interpret(members, live, possible, collapsed);
        for (const m of undecided) {
            const verdict = await this.run(m);
            const status = this.componentStatus(root, start, members.length);
            if (status !== 'same') return status;
            const edges = new Set<string>();
            if (verdict.status === 'undecided') {
                for (const key of verdict.support) if (undecidedKeys.has(key)) edges.add(key);
            }
            graph.set(m.key, edges);
        }
        return graph;
    }

    private async finalVerdicts(
        root: Frame<R>, start: number, members: Frame<R>[], live: Set<string>, collapsed: Set<string>,
    ): Promise<Map<string, Verdict<R>> | 'grown' | 'merged'> {
        this.interpret(members, live, live, collapsed);
        const verdicts = new Map<string, Verdict<R>>();
        for (const m of members) {
            if (collapsed.has(m.key)) {
                verdicts.set(m.key, { status: 'void', reason: this.undecidedReason });
                continue;
            }
            const verdict = await this.run(m);
            const status = this.componentStatus(root, start, members.length);
            if (status !== 'same') return status;
            if (verdict.status === 'undecided' || (verdict.status === 'live') !== live.has(m.key)) {
                throw new Error(`verdict evaluation: member '${m.key}' disagrees with its well-founded value`);
            }
            verdicts.set(m.key, verdict);
        }
        return verdicts;
    }

    // Members in `live` are live, members outside `possible` (and collapsed
    // ones) are void, the rest are undecided.
    private interpret(members: Frame<R>[], live: Set<string>, possible: Set<string>, collapsed: Set<string>): void {
        for (const m of members) {
            if (collapsed.has(m.key) || !possible.has(m.key)) {
                m.value = { status: 'void', reason: this.undecidedReason };
            } else if (live.has(m.key)) {
                m.value = { status: 'live' };
            } else {
                m.value = { status: 'undecided', support: new Set([m.key]) };
            }
        }
    }

    private componentStatus(root: Frame<R>, start: number, memberCount: number): 'same' | 'grown' | 'merged' {
        for (let i = start; i < this.stack.length; i++) {
            if (this.stack[i].lowlink < root.index) {
                root.lowlink = this.stack[i].lowlink;
                return 'merged';
            }
        }
        return this.stack.length > start + memberCount ? 'grown' : 'same';
    }
}

// Keys of the strongly connected components with no edge leaving them.
function bottomComponents(graph: Map<string, Set<string>>): string[] {
    const index = new Map<string, number>();
    const lowlink = new Map<string, number>();
    const onStack = new Set<string>();
    const stack: string[] = [];
    const componentOf = new Map<string, number>();
    const components: string[][] = [];
    let next = 0;

    const connect = (v: string): void => {
        index.set(v, next);
        lowlink.set(v, next);
        next++;
        stack.push(v);
        onStack.add(v);
        for (const w of graph.get(v) ?? []) {
            if (!index.has(w)) {
                connect(w);
                lowlink.set(v, Math.min(lowlink.get(v)!, lowlink.get(w)!));
            } else if (onStack.has(w)) {
                lowlink.set(v, Math.min(lowlink.get(v)!, index.get(w)!));
            }
        }
        if (lowlink.get(v) === index.get(v)) {
            const component: string[] = [];
            let w: string;
            do {
                w = stack.pop()!;
                onStack.delete(w);
                componentOf.set(w, components.length);
                component.push(w);
            } while (w !== v);
            components.push(component);
        }
    };

    for (const v of [...graph.keys()].sort()) if (!index.has(v)) connect(v);

    const bottom: string[] = [];
    components.forEach((component, c) => {
        const leaves = component.some((v) => [...(graph.get(v) ?? [])].some((w) => componentOf.get(w) !== c));
        if (!leaves) bottom.push(...component);
    });
    return bottom;
}

import { B64Hash, sha256 } from "@hyper-hyper-space/hhs3_crypto";
import { set } from "@hyper-hyper-space/hhs3_util";
import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { computeMeet, dag } from "../src/index.js";

const createDag = (): dag.Dag => {
    const store = new dag.store.MemDagStorage();
    const index = dag.idx.topo.createDagTopoIndex(
        store,
        new dag.idx.topo.mem.MemTopoIndexStore()
    );

    return dag.create(store, index, sha256);
};

// Meet (greatest lower bound) of the given points, each taken as a singleton
// position, folded through the dag's findForkPosition.
const expectMeet = async (
    targetDag: dag.Dag,
    points: B64Hash[],
    expected: Set<B64Hash>,
) => {
    const positions = points.map((h) => dag.position(h));
    const meet = await computeMeet(positions, (a, b) =>
        targetDag.findForkPosition(a, b).then((f) => f.commonFrontier),
    );
    assertTrue(set.eq(meet, expected));
};

const meetTestSuite = {
    title: "[MEET] Position meet (greatest lower bound) tests",

    tests: [
        {
            name: "[MEET_00] Single position meets to itself (small DAG)",
            invoke: async () => {
                // DAG:
                // a -> b
                const d = createDag();

                const a = await d.append({ a: 1 }, {});
                const b = await d.append({ b: 1 }, {}, dag.position(a));

                await expectMeet(d, [b], dag.position(b));
            }
        },
        {
            name: "[MEET_01] Linear chain meets to the earlier point (small DAG)",
            invoke: async () => {
                // DAG:
                // a -> b -> c
                const d = createDag();

                const a = await d.append({ a: 1 }, {});
                const b = await d.append({ b: 1 }, {}, dag.position(a));
                const c = await d.append({ c: 1 }, {}, dag.position(b));

                await expectMeet(d, [b, c], dag.position(b));
            }
        },
        {
            name: "[MEET_02] Concurrent siblings meet to their shared parent (small DAG)",
            invoke: async () => {
                // DAG:
                // r -> p -> b1 ---\
                //        \-> b2 ---> m
                const d = createDag();

                const r = await d.append({ r: 1 }, {});
                const p = await d.append({ p: 1 }, {}, dag.position(r));
                const b1 = await d.append({ b1: 1 }, {}, dag.position(p));
                const b2 = await d.append({ b2: 1 }, {}, dag.position(p));
                await d.append({ m: 1 }, {}, dag.position(b1, b2));

                // meet sits strictly below both inputs (the antichain would keep both)
                await expectMeet(d, [b1, b2], dag.position(p));
            }
        },
        {
            name: "[MEET_03] Three concurrent children meet to their parent (small DAG)",
            invoke: async () => {
                // DAG:
                // p -> c1
                //  \-> c2
                //  \-> c3
                const d = createDag();

                const p = await d.append({ p: 1 }, {});
                const c1 = await d.append({ c1: 1 }, {}, dag.position(p));
                const c2 = await d.append({ c2: 1 }, {}, dag.position(p));
                const c3 = await d.append({ c3: 1 }, {}, dag.position(p));

                await expectMeet(d, [c1, c2, c3], dag.position(p));
            }
        },
        {
            name: "[MEET_04] Points at different depths meet to the shared ancestor (small DAG)",
            invoke: async () => {
                // DAG:
                // a -> s1 -> s2 -> s3
                //       \-> b1 -> b2
                const d = createDag();

                const a = await d.append({ a: 1 }, {});
                const s1 = await d.append({ s1: 1 }, {}, dag.position(a));
                const s2 = await d.append({ s2: 1 }, {}, dag.position(s1));
                const s3 = await d.append({ s3: 1 }, {}, dag.position(s2));

                const b1 = await d.append({ b1: 1 }, {}, dag.position(s1));
                const b2 = await d.append({ b2: 1 }, {}, dag.position(b1));

                await expectMeet(d, [s3, b2], dag.position(s1));
            }
        },
        {
            name: "[MEET_05] Disconnected components meet to the empty set (small DAG)",
            invoke: async () => {
                // DAG (two disconnected components):
                // r1 -> a1
                // r2 -> c2
                const d = createDag();

                const r1 = await d.append({ r1: 1 }, {});
                const a1 = await d.append({ a1: 1 }, {}, dag.position(r1));

                const r2 = await d.append({ r2: 1 }, {});
                const c2 = await d.append({ c2: 1 }, {}, dag.position(r2));

                await expectMeet(d, [a1, c2], dag.position());
            }
        },
        {
            name: "[MEET_06] Two merges of the same parents meet to a multi-element frontier (small DAG)",
            invoke: async () => {
                // DAG:
                // r -> p --\  /--> x1
                //           \/
                //           /\
                // r -> q --/  \--> x2
                //
                // (x1 and x2 each merge p and q)
                const d = createDag();

                const r = await d.append({ r: 1 }, {});
                const p = await d.append({ p: 1 }, {}, dag.position(r));
                const q = await d.append({ q: 1 }, {}, dag.position(r));
                const x1 = await d.append({ x1: 1 }, {}, dag.position(p, q));
                const x2 = await d.append({ x2: 1 }, {}, dag.position(p, q));

                await expectMeet(d, [x1, x2], dag.position(p, q));
            }
        },
        {
            name: "[MEET_07] Dominated elements are ignored (small DAG)",
            invoke: async () => {
                // DAG:
                // a -> b -> c -> e
                const d = createDag();

                const a = await d.append({ a: 1 }, {});
                const b = await d.append({ b: 1 }, {}, dag.position(a));
                const c = await d.append({ c: 1 }, {}, dag.position(b));
                const e = await d.append({ e: 1 }, {}, dag.position(c));

                // b is an ancestor of e, so the meet is just b
                await expectMeet(d, [e, b], dag.position(b));
            }
        },
        {
            name: "[MEET_08] Empty input meets to the empty set (small DAG)",
            invoke: async () => {
                const d = createDag();
                await expectMeet(d, [], dag.position());
            }
        }
    ]
};

export { meetTestSuite };

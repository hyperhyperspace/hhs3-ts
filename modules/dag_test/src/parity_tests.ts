import { Hash, sha256 } from "@hyper-hyper-space/hhs3_crypto";
import { dag, Dag, EntryMetaFilter, MetaProps, position, Position } from "@hyper-hyper-space/hhs3_dag";
import { set } from "@hyper-hyper-space/hhs3_util";
import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { DagFactory } from "./backend_tests.js";
import {
    createRandomBranchingDags,
    createRandomDags,
    createRandomDag,
} from "./dag_create.js";

function createGoldStandard(): Dag {
    const store = new dag.store.MemDagStorage();
    const index = dag.idx.level.createDagLevelIndex(
        store,
        new dag.idx.level.mem.MemLevelIndexStore()
    );
    return dag.create(store, index, sha256);
}

async function testForkPositionParity(
    cases: { dags: Array<[Dag, Dag]>; branches: Array<[Position, Position]> }
) {
    for (let i = 0; i < cases.dags.length; i++) {
        const [gold, tested] = cases.dags[i];
        const [a, b] = cases.branches[i];

        const f1 = await gold.findForkPosition(a, b);
        const f2 = await tested.findForkPosition(a, b);

        assertTrue(set.eq(f1.common, f2.common), `common mismatch at i=${i}`);
        assertTrue(set.eq(f1.commonFrontier, f2.commonFrontier), `commonFrontier mismatch at i=${i}`);
        assertTrue(set.eq(f1.forkA, f2.forkA), `forkA mismatch at i=${i}`);
        assertTrue(set.eq(f1.forkB, f2.forkB), `forkB mismatch at i=${i}`);

        process.stdout.write(".");
    }
    process.stdout.write("\n");
}

async function testMinimalCoverParity(
    cases: { dags: Array<[Dag, Dag]>; branches: Array<[Position, Position]> }
) {
    for (let i = 0; i < cases.dags.length; i++) {
        const [gold, tested] = cases.dags[i];
        const [b1, b2] = cases.branches[i];

        const positions: Position[] = [
            b1,
            b2,
            new Set<Hash>([...b1, ...b2]),
            await gold.getFrontier(),
        ];

        for (let j = 0; j < positions.length; j++) {
            const coverGold = await gold.findMinimalCover(positions[j]);
            const coverTested = await tested.findMinimalCover(positions[j]);
            assertTrue(
                set.eq(coverGold, coverTested),
                `minimal cover mismatch at i=${i} j=${j}`
            );
        }

        process.stdout.write(".");
    }
    process.stdout.write("\n");
}

async function testMetaFilterParity(factory: DagFactory) {
    const goldDag = createGoldStandard();
    const testedDag = await factory();

    const seed = 1771;
    const size = 200;

    const [branchA, branchB] = await createRandomDag(goldDag, seed, size, { addMeta: true });
    await dag.copy(goldDag, testedDag);

    const positions: Array<Position> = [
        branchA,
        branchB,
        new Set<Hash>([...branchA, ...branchB]),
        await goldDag.getFrontier(),
    ];

    const metas: Array<MetaProps> = [];
    for await (const entry of goldDag.loadAllEntries()) {
        metas.push(entry.meta);
    }

    const sample1 = metas[0];
    const sample2 = metas[Math.max(0, metas.length - 1)];

    const bucketSample = Object.keys(sample1["bucket"])[0];
    const paritySample = Object.keys(sample1["parity"])[0];
    const tagSample = Object.keys(sample2["tag"])[0];
    const tierSample = Object.keys(sample2["tier"])[0];

    const filters: EntryMetaFilter[] = [
        { containsValues: { bucket: [bucketSample] } },
        { containsValues: { bucket: [bucketSample], parity: [paritySample] } },
        { containsKeys: ["bucket", "tag"] },
        { containsValues: { tag: [tagSample] } },
        { containsValues: { tier: [tierSample] }, containsKeys: ["bucket"] },
    ];

    for (let i = 0; i < filters.length; i++) {
        for (let j = 0; j < positions.length; j++) {
            const coverGold = await goldDag.findCoverWithFilter(positions[j], filters[i]);
            const coverTested = await testedDag.findCoverWithFilter(positions[j], filters[i]);
            assertTrue(
                set.eq(coverGold, coverTested),
                `meta filter cover mismatch: filter=${i} position=${j}`
            );
        }
    }
}

export function createParitySuite(
    tag: string,
    factory: DagFactory
): { title: string; tests: Array<{ name: string; invoke: () => Promise<void> }> } {

    const goldFactory: () => Dag = createGoldStandard;

    const pairConstrs: [() => Dag | Promise<Dag>, () => Dag | Promise<Dag>] = [
        goldFactory,
        factory,
    ];

    const t = (n: number) => `[${tag}_${String(n).padStart(2, '0')}]`;

    return {
        title: `\n[${tag}] Parity Tests (vs mem+level gold standard)\n`,
        tests: [
            {
                name: `${t(0)} Fork position parity on small random branching DAGs`,
                invoke: async () => {
                    const cases = await createRandomBranchingDags(pairConstrs, 93, 200, { progressBar: true });
                    await testForkPositionParity(cases);
                },
            },
            {
                name: `${t(1)} Fork position parity on small random DAGs`,
                invoke: async () => {
                    const cases = await createRandomDags(pairConstrs, 33, 200, { progressBar: true });
                    await testForkPositionParity(cases);
                },
            },
            {
                name: `${t(2)} Minimal cover parity on small random branching DAGs`,
                invoke: async () => {
                    const cases = await createRandomBranchingDags(pairConstrs, 93, 200, { progressBar: true });
                    await testMinimalCoverParity(cases);
                },
            },
            {
                name: `${t(3)} Minimal cover parity on small random DAGs`,
                invoke: async () => {
                    const cases = await createRandomDags(pairConstrs, 33, 200, { progressBar: true });
                    await testMinimalCoverParity(cases);
                },
            },
            {
                name: `${t(4)} Meta filter cover parity on small random DAG`,
                invoke: async () => {
                    await testMetaFilterParity(factory);
                },
            },
        ],
    };
}

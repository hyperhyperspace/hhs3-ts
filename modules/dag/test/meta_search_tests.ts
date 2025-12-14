import { dag, EntryMetaFilter, MetaProps, Position } from "../src/index";
import { position } from "../src/index";

import { assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test";

import { createRandomDag, createD3 } from "./utils/dag_create";
import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { set } from "@hyper-hyper-space/hhs3_util";
import { label } from "./utils/dag_diagram";

const pp = (ns: Set<Hash>) => Array.from(ns).map(label).sort();

const collectMetas = async (d: dag.Dag): Promise<Array<MetaProps>> => {
    const metas: Array<MetaProps> = [];
    for await (const entry of d.loadAllEntries()) {
        metas.push(entry.meta);
    }
    return metas;
};

const runMetaParity = async (constrs: [() => dag.Dag, () => dag.Dag], options?: {size?: number, seed?: number}) => {
    const dagA = constrs[0]();
    const dagB = constrs[1]();

    const seed = options?.seed ?? 1771;
    const size = options?.size ?? 24;

    const [branchA, branchB] = await createRandomDag(dagA, seed, size, {addMeta: true});
    await dag.copy(dagA, dagB);

    const positions: Array<Position> = [
        branchA,
        branchB,
        new Set<Hash>([...branchA, ...branchB]),
        await dagA.getFrontier()
    ];

    const metas = await collectMetas(dagA);
    const sample1 = metas[0];
    const sample2 = metas[Math.max(0, metas.length - 1)];

    const bucketSample = Object.keys(sample1["bucket"])[0];
    const paritySample = Object.keys(sample1["parity"])[0];
    const tagSample = Object.keys(sample2["tag"])[0];
    const tierSample = Object.keys(sample2["tier"])[0];

    const filters: EntryMetaFilter[] = [];
    filters.push({ containsValues: { bucket: [bucketSample] } });
    filters.push({ containsValues: { bucket: [bucketSample], parity: [paritySample] } });
    filters.push({ containsKeys: ["bucket", "tag"] });
    filters.push({ containsValues: { tag: [tagSample] } });
    filters.push({ containsValues: { tier: [tierSample] }, containsKeys: ["bucket"] });

    for (let i = 0; i < filters.length; i++) {
        for (let j = 0; j < positions.length; j++) {
            const coverA = await dagA.findCoverWithFilter(positions[j], filters[i]);
            const coverB = await dagB.findCoverWithFilter(positions[j], filters[i]);

            const coversMatch = set.eq(coverA, coverB);
            if (!coversMatch) {
                console.log("--- meta parity mismatch ---");
                console.log("filter idx:", i, "position idx:", j);
                console.log("filter:", JSON.stringify(filters[i]));
                console.log("coverA:", pp(coverA));
                console.log("coverB:", pp(coverB));
            }
            assertTrue(coversMatch, `filter ${i} position ${j} mismatch`);
        }
    }
};

const topoFlatDagPairConstr: [() => dag.Dag, () => dag.Dag] = [
    () => {
        const store = new dag.store.MemDagStorage();
        const index = dag.idx.topo.createDagTopoIndex(
            store,
            new dag.idx.topo.mem.MemTopoIndexStore()
        );
        return dag.create(store, index);
    },
    () => {
        const store = new dag.store.MemDagStorage();
        const index = dag.idx.flat.createFlatIndex(
            store,
            new dag.idx.flat.mem.MemFlatIndexStore()
        );
        return dag.create(store, index);
    }
];

const metaSearchSuite = {
    title: "[META] Test Meta Search Solutions",
    tests: [
        {
            name: "[META_00] Basic meta property covering tests using flat index",
            invoke: async () => {
                const store = new dag.store.MemDagStorage();
                const flatIndex = dag.idx.flat.createFlatIndex(
                    store,
                    new dag.idx.flat.mem.MemFlatIndexStore()
                );
                const d3 = dag.create(store, flatIndex);
                const h = await createD3(d3);

                const cp1 = await d3.findCoverWithFilter(
                    position(h["b1"], h["b2"]),
                    { containsKeys: ["p1"] }
                );
                const cp1ok = set.eq(cp1, position(h["b1"], h["b2"]));

                assertTrue(cp1ok, "filter on p1 key failed for d3");

                const cp2 = await d3.findCoverWithFilter(
                    position(h["b1"], h["b2"]),
                    { containsValues: { p2: ["2"] } }
                );
                const cp2ok = set.eq(cp2, position(h["b2"]));

                assertTrue(cp2ok, "filter on p2 value failed for d3");

                const cp23 = await d3.findCoverWithFilter(
                    position(h["b1"], h["b2"]),
                    { containsValues: { p2: ["3"] } }
                );
                const cp23ok = set.eq(cp23, position());

                assertTrue(cp23ok, "filter on p2=3 value failed for d3");

                const cp2too = await d3.findCoverWithFilter(
                    position(h["c1"], h["b2"]),
                    { containsKeys: ["p1", "p2"] }
                );
                const cp2toook = set.eq(cp2too, position(h["c1"], h["b2"]));

                assertTrue(cp2toook, "filter on p1 and p2 keys failed for d3");

                const cp23too = await d3.findCoverWithFilter(
                    position(h["c1"], h["b2"]),
                    { containsValues: { p1: ["1"] }, containsKeys: ["p2"] }
                );
                const cp23toook = set.eq(cp23too, position(h["c1"], h["b2"]));

                assertTrue(cp23toook, "filter on p1=1 and p2 key failed for d3");
            }
        },
        {
            name: "[META_01] Basic meta property concurrent covering tests using flat index",
            invoke: async () => {
                const store = new dag.store.MemDagStorage();
                const flatIndex = dag.idx.flat.createFlatIndex(
                    store,
                    new dag.idx.flat.mem.MemFlatIndexStore()
                );
                const d3 = dag.create(store, flatIndex);
                const h = await createD3(d3);

                const cc1 = await d3.findConcurrentCoverWithFilter(
                    position(h["c1"], h["b2"]),
                    position(h["b1"]),
                    { containsKeys: ["p1"] }
                );
                const cc1ok = set.eq(cc1, position(h["b2"]));

                assertTrue(
                    cc1ok,
                    "concurrent (b1) covering filter on p1 key failed for d3"
                );

                const cc2 = await d3.findConcurrentCoverWithFilter(
                    position(h["c1"]),
                    position(h["b1"]),
                    { containsValues: { p2: ["4"] } }
                );
                const cc2ok = set.eq(cc2, position());

                assertTrue(
                    cc2ok,
                    "concurrent (b1) covering filter on p2=4 value failed for d3"
                );

                const cc3 = await d3.findConcurrentCoverWithFilter(
                    position(h["d1"], h["d2"]),
                    position(h["d1"]),
                    { containsKeys: ["p1"] }
                );
                const cc3ok = set.eq(cc3, position(h["d2"]));

                assertTrue(
                    cc3ok,
                    "concurrent (d1) covering filter on p1 key failed for d3"
                );

                const cc4 = await d3.findConcurrentCoverWithFilter(
                    position(h["d1"], h["d2"], h["b2"]),
                    position(h["d1"]),
                    { containsKeys: ["p1"] }
                );
                const cc4ok = set.eq(cc4, position(h["d2"], h["b2"]));

                assertTrue(
                    cc4ok,
                    "concurrent (d1) covering filter on p1 key failed for d3 (v2)"
                );
            }
        },
        {
            name: "[META_02] Pseudo-random cover filters topo vs flat parity on small random DAGs",
            invoke: async () => {
                await runMetaParity(topoFlatDagPairConstr, {size: 1000, seed: 1771});
            }
        },
        {
            name: "[META_03] Pseudo-random cover filters topo vs flat parity on medium random DAGs",
            invoke: async () => {
                await runMetaParity(topoFlatDagPairConstr, {size: 5000, seed: 1776});
            }
        },
        {
            name: "[META_04] Pseudo-random cover filters topo vs flat parity on medium random DAGs",
            invoke: async () => {
                await runMetaParity(topoFlatDagPairConstr, {size: 10000, seed: 1774});
            }
        }
        
    ]
};

export { metaSearchSuite };


import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { MetaProps, Position } from "../../src/dag_defs";
import { Dag, position } from "../../src/dag";
import { draw, label } from "./dag_diagram";
import { dag } from "../../src/index";
import { MultiMap } from "@hyper-hyper-space/hhs3_util";
import { json } from "@hyper-hyper-space/hhs3_json";

const createDeterministicMeta = (nodeIndex: number, prng: PRNG): MetaProps => ({
    bucket: json.toSet([Boolean(nodeIndex % 4).toString()]),
    parity: json.toSet(nodeIndex % 2 === 0 ? 'even' : 'odd'),
    tier: json.toSet([prng.nextInt(0, 3).toString()]),
    tag: json.toSet([`tag-${prng.nextInt(0, 1_000_000)}`])
});

export async function createD1(dag: Dag): Promise<[Position, Position]> {
    const a = await dag.append({'a': 1}, {});
    const b1 = await dag.append({'b1': 1}, {'p1': json.toSet(['1'])} , position(a));
    const b2 = await dag.append({'b2': 1}, {'p1': json.toSet(['1']), 'p2': json.toSet(['2'])}, position(a));
    const c1 = await dag.append({'c1': 1}, {'p1': json.toSet(['1']), 'p2': json.toSet(['3'])}, position(b1));

    return [new Set([b2]), new Set([c1])];
}

export async function createD2(dag: Dag): Promise<[Position, Position]> {
    const a = await dag.append({'a': 1}, {});
    const b1 = await dag.append({'b1': 1}, {}, position(a));
    const b2 = await dag.append({'b2': 1}, {}, position(a));
    const c1 = await dag.append({'c1': 1}, {}, position(b1));
    const c2 = await dag.append({'c2': 1}, {}, position(b2));
    
    return [new Set([b2]), new Set([c1, c2])];
}

export async function createD3(dag: Dag): Promise<{ [key: string]: Hash }> {
    const a = await dag.append({'a': 1}, {});
    const b1 = await dag.append({'b1': 1}, {'p1': json.toSet(['1'])}, position(a));
    const b2 = await dag.append({'b2': 1}, {'p1': json.toSet(['1']), 'p2': json.toSet(['2'])}, position(a));
    const c1 = await dag.append({'c1': 1}, {'p1': json.toSet(['1']), 'p2': json.toSet(['3'])}, position(b1));
    const d1 = await dag.append({'d1': 1}, {'p1': json.toSet(['1']), 'p2': json.toSet(['4'])}, position(c1));
    const d2 = await dag.append({'d2': 1}, {'p1': json.toSet(['1']), 'p2': json.toSet(['5'])}, position(c1));

    return {'a': a, 'b1': b1, 'b2': b2, 'c1': c1, 'd1': d1, 'd2': d2};
}

export async function createD4(dag: Dag): Promise<{ [key: string]: Hash }> {
    const a = await dag.append({'a': 1}, {});
    const b1 = await dag.append({'b1': 1}, {}, position(a));
    const b2 = await dag.append({'b2': 1}, {}, position(a));
    const c1 = await dag.append({'c1': 1}, {}, position(b1));
    const c2 = await dag.append({'c2': 1}, {}, position(b2));
    
    return {'a': a, 'b1': b1, 'b2': b2, 'c1': c1, 'c2': c2};
}

export async function createRandomBranchingDags(constrs: [()=>dag.Dag, ()=>dag.Dag], seed: number, size: number, options?: { progressBar?: boolean, instanceCount?: number }): Promise<{ dags: Array<[dag.Dag, dag.Dag]>, branches: Array<[Position, Position]>}> {

    const dags: Array<[dag.Dag, dag.Dag]> = [];
    const branches: Array<[Position, Position]> = [];

    const seeds = new PRNG(seed);

    for (let i=0; i<(options?.instanceCount || 20); i++) {

        const d1 = constrs[0]();
        const d2 = constrs[1]();


        const [b1, b2] = await createBranchingDag(d1, seeds.nextInt(0, 2000000000), size);

        await dag.copy(d1, d2);

        dags.push([d1, d2]);
        branches.push([b1, b2]);
        if (options?.progressBar) {
            process.stdout.write(".");
        }
    }

    if (options?.progressBar) {
        process.stdout.write("\n");
    }

    return {dags: dags, branches: branches};
}

export async function createBranchingDag(dag: Dag, seed: number, size: number): Promise<[Position, Position]> {

    const prng = new PRNG(seed);

    await appendNodesToDag(dag, prng.nextInt(0, 2000000), size * 0.2, await dag.getFrontier());

    const seeds = [prng.nextInt(0, 2000000), prng.nextInt(0, 2000000)];

    return (await appendBranchesToDag(dag, seeds, [size * .4, size * .4], await dag.getFrontier())) as [Position, Position];
}

export async function appendBranchesToDag(dag: Dag, seeds: Array<number>, sizes: Array<number>, start: Position): Promise<Array<Position>> {

    const frontiers = new Array<Position>();

    for (let i=0; i<seeds.length; i++) {
        const seed = seeds[i];
        const size = sizes[i];

        frontiers.push(await appendNodesToDag(dag, seed, size, new Set([...start])));
    }

    return frontiers;

}

export async function appendNodesToDag(dag: Dag, seed: number, size: number, start: Position): Promise<Position> {

    const prng = new PRNG(seed);

    let frontier = new Set([...start]);

    let i=0;

    while (i<size) {

        let after = new Set<Hash>();

        if (prng.next() < 0.5) {
            after = frontier;
            frontier = new Set<Hash>()
        } else {
            for (const f of frontier) {
                const x = prng.next();
                if (x < 0.7) {
                    after.add(f);
                    frontier.delete(f);
                }
            }

            if (after.size === 0) {
                after = frontier;
                frontier = new Set<Hash>();
            }
        }

        let limit = prng.nextInt(i+1, i+7);

        if (limit > size) {
            limit = size;
        }

        while (i<limit) {
            const nodeIndex = i;
            const id = nodeIndex + ":" + prng.nextInt(0, 2000000000);
            const meta = createDeterministicMeta(nodeIndex, prng);
            const n = await dag.append({id: id}, meta, after);
            frontier.add(n);

            i = i + 1;
        }


    }

    return frontier;

}

export async function createRandomDisconnectedDags(constrs: [()=>dag.Dag, ()=>dag.Dag], seed: number, size: number, options?: {connectedBranches?: boolean, progressBar?: boolean}): Promise<{ dags: Array<[dag.Dag, dag.Dag]>, branches: Array<[Position, Position]>}> {
    const dags: Array<[dag.Dag, dag.Dag]> = [];
    const branches: Array<[Position, Position]> = [];

    const seeds = new PRNG(seed);

    for (let i=0; i<20; i++) {

        const d1 = constrs[0]();
        const d2 = constrs[1]();

        const [b1, b2] = await createRandomDag(d1, seeds.nextInt(0, 2000000000), size/2);
        const [c1, c2] = await createRandomDag(d1, seeds.nextInt(0, 2000000000), size/2);

        await dag.copy(d1, d2);

        dags.push([d1, d2]);

        if (options?.connectedBranches) {
            branches.push([new Set([...b1, ...c1]), new Set([...b2, ...c2])]);
        } else {
            branches.push([b1, c2]);
        }

        
        if (options?.progressBar) {
            process.stdout.write(".");
        }
    }

    if (options?.progressBar) {
        process.stdout.write("\n");
    }

    return {dags: dags, branches: branches};
}

export async function createRandomDags(constrs: [()=>dag.Dag, ()=>dag.Dag], seed: number, size: number, options?: { progressBar?: boolean }): Promise<{ dags: Array<[dag.Dag, dag.Dag]>, branches: Array<[Position, Position]>}> {
    const dags: Array<[dag.Dag, dag.Dag]> = [];
    const branches: Array<[Position, Position]> = [];

    const seeds = new PRNG(seed);

    for (let i=0; i<20; i++) {

        const d1 = constrs[0]();
        const d2 = constrs[1]();

        const [b1, b2] = await createRandomDag(d1, seeds.nextInt(0, 2000000000), size);

        await dag.copy(d1, d2);

        dags.push([d1, d2]);
        branches.push([b1, b2]);
        if (options?.progressBar) {
            process.stdout.write(".");
        }
    }

    if (options?.progressBar) {
        process.stdout.write("\n");
    }

    return {dags: dags, branches: branches};
}

export async function createRandomDag(dag: Dag, seed: number, size: number): Promise<[Position, Position]> {

    const prng = new PRNG(seed);

    const preds = new MultiMap<Hash, Hash>();

    let i = 0;
    let nodes = new Array<Hash>();
    
    while (i<size) {

        let after = new Set<Hash>();

        if (nodes.length > 0) {
            const fanout = prng.nextInt(1, 5);

            for (let j=0; j<fanout; j++) {
                const k = prng.nextInt(0, nodes.length-1);
                after.add(nodes[k]);
            }
        }

        after = minimalCover(after, preds);
        const nodeIndex = i;
        const id = nodeIndex + ":" + prng.nextInt(0, 2000000000);
        const meta = createDeterministicMeta(nodeIndex, prng);
        const n = await dag.append({id: id}, meta, after);

        nodes.push(n);

        for (const pred of after) {
            preds.add(n, pred);
        }

        i = i + 1;
    }

    const fanout1 = prng.nextInt(1, 7);
    const fanout2 = prng.nextInt(1, 7);

    const branch1 = new Set<Hash>();
    const branch2 = new Set<Hash>();

    for (let i=0; i<fanout1; i++) {
        branch1.add(nodes[prng.nextInt(0, nodes.length-1)]);
    }

    for (let i=0; i<fanout2; i++) {
        branch2.add(nodes[prng.nextInt(0, nodes.length-1)]);
    }

    //return [minimalCover(branch1, preds), minimalCover(branch2, preds)]
    return [branch1, branch2]
}

const minimalCover = (nodes: Set<Hash>, preds: MultiMap<Hash, Hash>) => {

    const maxCover = new Set<Hash>([...nodes]);

    const pending = new Set<Hash>([...nodes]);
    const visited = new Set<Hash>();
    
    while (pending.size > 0) {
        const n = pending.values().next().value!;
        pending.delete(n);
        visited.add(n);

        for (const pred of preds.get(n)) {
            maxCover.delete(pred);
            if (!visited.has(pred)) {
                pending.add(pred);
            }
        }
    }
    
    return maxCover;

};


class PRNG {
    private state: number;

    constructor(seed: number) {
        this.state = seed >>> 0; // force uint32
    }

    // Returns float in [0, 1)
    next(): number {
        // LCG parameters from Numerical Recipes
        this.state = (1664525 * this.state + 1013904223) >>> 0;
        return this.state / 0x100000000;
    }

    // Returns integer in [min, max]
    nextInt(min: number, max: number): number {
        return Math.floor(this.next() * (max - min + 1)) + min;
    }

    // Returns float in [min, max)
    nextFloat(min: number, max: number): number {
        return this.next() * (max - min) + min;
    }
}


export const createD1s = async () => {

    const dags: Array<dag.Dag> = [];

    const store1 = new dag.store.MemDagStorage();
    const topoIndex = dag.idx.topo.createDagTopoIndex(store1, new dag.idx.topo.mem.MemTopoIndexStore());
    const d1Topo = dag.create(store1, topoIndex)

    const store2 = new dag.store.MemDagStorage();
    const flatIndex = dag.idx.flat.createFlatIndex(store2, new dag.idx.flat.mem.MemFlatIndexStore());

    const d1Flat = dag.create(store2, flatIndex);

    await createD1(d1Topo);
    const bs = await createD1(d1Flat);

    return [[d1Topo, d1Flat],bs];
}
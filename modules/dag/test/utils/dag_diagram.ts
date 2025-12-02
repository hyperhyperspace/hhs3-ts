import { Hash } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { Dag } from "../../src/dag";

import { writeFileSync } from 'fs';
import { exec } from 'child_process';

export function label(h: Hash) { return "_" + h.replace(/[^a-zA-Z0-9]/g, "").slice(-6, -1); }

export type DrawOptions = { 
    namedSets?: Array<[string, Set<Hash>]>,
    tags?: Map<Hash, string>,
    filter?: (n: Hash) => Promise<boolean>,
    prev?: (n: Hash) => Promise<Set<Hash>>
 }

export function fullLabel(h: Hash, options?: DrawOptions): string {
    let result = label(h);   

    for (const [name, s] of options?.namedSets || []) {
        if (s.has(h)) {
            result = name+'_' + result;
        }
    }

    const t = options?.tags?.get(h);

    if (t !== undefined) {
        result = result + '_' + t;
    }

    return result;
}

export async function draw(dag: Dag, name: string, options?: DrawOptions) {
    let g1 = await graph(dag, name, options);

    writeFileSync(name+".dot", g1, "utf8");
    exec("dot -Tpng "+name+".dot -o "+name+".png", (error, stdout, stderr) => {
        if (error) {
            console.error(`Error: ${error.message}`);
            return;
        }
        if (stderr) {
            console.error(`Stderr: ${stderr}`);
            return;
        }
        if (stdout) {
            console.log(`Output:\n${stdout}`);
        }
    });

}

export async function graph(dag: Dag, name: string, options?: DrawOptions): Promise<string> {

    let result = "digraph " + name + " {\n";

    /*
    const done = new Set<Hash>();
    const queue = new Set<Hash>();

    for (const h of (await dag.getFrontier())) {
        queue.add(h);
    }

    while (queue.size > 0) {
        const h = queue.values().next().value!;
        */
    for await (const e of dag.loadAllEntries()) {

        const h = e.hash;

        //queue.delete(h);
        //done.add(h);

        if (options?.filter === undefined || await options?.filter(h)) {

            const preds = options?.prev !== undefined ? 
                [...(await options?.prev(h))] :
                [...json.fromSet((await dag.loadHeader(h))?.prevEntryHashes)];

            /*for (const pred of preds) {
                if (!done.has(pred)) {
                    queue.add(pred);
                }
            }*/

            if (preds.length === 0) {
                const line = fullLabel(h, options) + ";\n";
                result = result + "    " + line;
            } else {
                let line = fullLabel(h, options) + " -> { "
                for (const pred of preds) {
                    line = line + fullLabel(pred, options) + " ";
                }
                line = line + "};\n";
                result = result + "    " + line;
            }
        }
    }

    result = result + "}\n";

    return result;
}
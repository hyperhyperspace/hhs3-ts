import { benchmarkSuite as forkBench } from "./forking_benchmarks";
import { coverBenchmarkSuite as coverBench } from "./cover_benchmarks";
import { testing } from "@hyper-hyper-space/hhs3_util/";

async function main() {

    const allBenchmarks = new Map<string, Array<{name: string, invoke: () => Promise<void>}>>();

    const filters = process.argv.slice(2);

    allBenchmarks.set(forkBench.title, forkBench.tests);
    allBenchmarks.set(coverBench.title, coverBench.tests);

    console.log('Running benchmarks for Hyper Hyper Space v3 DAG module' + (filters.length > 0? ' (applying filter: ' + filters.toString() + ')' : '') + '\n');    

    for (const [title, tests] of allBenchmarks.entries()) {
        console.log(title);

        for (const test of tests) {

            let match = true;
            for (const filter of filters) {
                match = match && (/*title.indexOf(filter) >= 0 || */test.name.indexOf(filter) >= 0)
            }

            if (match) {
                const result = await testing.run(test.name, test.invoke, {silent: true});

                if (!result) {
                    return;
                }
            } else {
                await testing.skip(test.name);
            }
        }

        console.log();
    }
}

main();

import { runBrowserSyncMeshTests } from "./sync_mesh_tests.js";

async function main(): Promise<void> {
    await runBrowserSyncMeshTests();
    console.log('rdb_repl_web sync mesh tests ok');
}

void main();

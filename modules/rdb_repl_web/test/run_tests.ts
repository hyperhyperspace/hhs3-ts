import { createBrowserMesh } from "@hyper-hyper-space/hhs3_mesh_browser";

// The browser mesh factory's behavior is covered in the hhs3_mesh_browser
// package. Here we only smoke-check that rdb_repl_web wires it in.
async function main(): Promise<void> {
    if (typeof createBrowserMesh !== "function") {
        throw new Error("createBrowserMesh should be importable from hhs3_mesh_browser");
    }
    console.log("rdb_repl_web smoke ok (browser mesh factory wired via hhs3_mesh_browser)");
}

void main();

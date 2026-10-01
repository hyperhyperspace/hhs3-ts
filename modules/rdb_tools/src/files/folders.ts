// The host's folders for `\project files`, on Node. Paths resolve against `cwd`.

import { resolve } from "node:path";

import { NodeDirectory } from "@hyper-hyper-space/hhs3_rdb_files_node";
import type { FilesDirectoryFactory } from "@hyper-hyper-space/hhs3_rdb_repl";

export function nodeFilesDirectories(cwd: string = process.cwd()): FilesDirectoryFactory {
    return ({ path }) => NodeDirectory.open(resolve(cwd, path));
}

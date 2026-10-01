import { stdin, stdout } from "node:process";
import { createInterface, type Interface } from "node:readline/promises";

import type { Prompter } from "@hyper-hyper-space/hhs3_rhost_node";

import { promptSecret } from "../repl/prompt.js";

// Asks on the terminal; not interactive when stdin isn't one.
export function ttyPrompter(): Prompter {
    const interactive = stdin.isTTY === true;
    let rl: Interface | undefined;
    const open = (): Interface => {
        if (!interactive) throw new Error('no terminal to ask on');
        rl ??= createInterface({ input: stdin, output: stdout });
        return rl;
    };
    return {
        interactive,
        say(line) { stdout.write(line + '\n'); },
        async ask(question) { return (await open().question(question)).trim(); },
        async secret(question) { return promptSecret(open(), question); },
        close() { rl?.close(); rl = undefined; },
    };
}

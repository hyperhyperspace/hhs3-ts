import { spawn } from "node:child_process";
import { resolve } from "node:path";

export type Run = { code: number; stdout: string; stderr: string };

const MODULE_DIR = process.cwd();

// The loader flags the tests run with, with relative --import paths made
// absolute so a bin can run from another folder.
function loaderArgs(): string[] {
    const absolute = (spec: string) => (spec.startsWith('.') ? resolve(MODULE_DIR, spec) : spec);
    const args = process.execArgv;
    const out: string[] = [];
    for (let i = 0; i < args.length; i++) {
        const arg = args[i]!;
        if (arg === '--import' && i + 1 < args.length) out.push(arg, absolute(args[++i]!));
        else if (arg.startsWith('--import=')) out.push(`--import=${absolute(arg.slice('--import='.length))}`);
        else out.push(arg);
    }
    return out;
}

// Runs a bin from source, through the same loader as the tests, in `cwd`
// (the module folder by default). The loader resolves from `cwd`, so another
// folder has to be inside the module (test-build/ is).
export function runBin(bin: 'rpack' | 'rhost' | 'rkeys', args: string[], input?: string, cwd = MODULE_DIR): Promise<Run> {
    return new Promise((done, fail) => {
        const child = spawn(process.execPath, [...loaderArgs(), resolve(MODULE_DIR, 'bin', `${bin}.ts`), ...args], {
            cwd,
            env: process.env,
            stdio: [input === undefined ? 'ignore' : 'pipe', 'pipe', 'pipe'],
        });
        let stdout = '';
        let stderr = '';
        child.stdout!.on('data', (chunk) => { stdout += chunk; });
        child.stderr!.on('data', (chunk) => { stderr += chunk; });
        child.on('error', fail);
        child.on('close', (code) => done({ code: code ?? 1, stdout, stderr }));
        if (input !== undefined) child.stdin!.end(input);
    });
}

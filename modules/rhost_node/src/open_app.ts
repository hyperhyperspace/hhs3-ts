import { promises as fs } from "node:fs";
import { join, resolve } from "node:path";

import type { IssueReporter } from "@hyper-hyper-space/hhs3_mesh";
import { Rhost, parseAppConfig, type AppConfig } from "@hyper-hyper-space/hhs3_rhost";

import { nodePlatform, type NodePlatformOptions } from "./platform.js";

export const APP_CONFIG_FILE = 'app.json';

export async function readAppConfig(dir: string): Promise<AppConfig> {
    const path = join(resolve(dir), APP_CONFIG_FILE);
    let text: string;
    try {
        text = await fs.readFile(path, 'utf8');
    } catch (e) {
        if ((e as NodeJS.ErrnoException).code === 'ENOENT') throw new Error(`no ${APP_CONFIG_FILE} in ${resolve(dir)}; run rhost init`);
        throw e;
    }
    let value: unknown;
    try {
        value = JSON.parse(text);
    } catch (e) {
        throw new Error(`${path}: not valid JSON: ${(e as Error).message}`);
    }
    return parseAppConfig(value, path);
}

export type OpenAppOptions = NodePlatformOptions & { report?: IssueReporter };

// An app folder with its app.json, on the Node platform. Keys come from
// app.json's `keystore` when it names one (`options.keystore` overrides it).
export async function openApp(dir: string, options: OpenAppOptions = {}): Promise<Rhost> {
    const config = await readAppConfig(dir);
    const { report, ...platformOptions } = options;
    const keystore = platformOptions.keystore ?? config.keystore;
    const platform = nodePlatform(dir, { ...platformOptions, ...(keystore !== undefined ? { keystore } : {}) });
    return Rhost.open(config, platform, report !== undefined ? { report } : {});
}

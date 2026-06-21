import type { RDb, RSchema, RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import type { LoggableObject } from "../bind/context.js";
import { renderOp } from "./render.js";

export type DumpOptions = {
    includeUnknown?: boolean;
};

export async function dumpObject(object: LoggableObject, options: DumpOptions = {}): Promise<string> {
    const statements: string[] = [];
    const dag = await object.getScopedDag();
    for await (const entry of dag.loadAllEntries()) {
        const rendered = renderOp(entry.payload, { at: entry.header.prevEntryHashes });
        if (options.includeUnknown === false && rendered.startsWith('-- unknown payload')) continue;
        statements.push(rendered);
    }
    return statements.join('\n\n');
}

export async function dumpSchema(schema: RSchema & LoggableObject, options?: DumpOptions): Promise<string> {
    return dumpObject(schema, options);
}

export async function dumpGroup(group: RTableGroup & LoggableObject, options?: DumpOptions): Promise<string> {
    return dumpObject(group, options);
}

export async function dumpDatabase(db: RDb & LoggableObject, options?: DumpOptions): Promise<string> {
    return dumpObject(db, options);
}

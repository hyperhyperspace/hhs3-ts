import type { NameOrHashRef } from "@hyper-hyper-space/hhs3_rdb_lang";

import type { RdbSession } from "./session.js";
import type { RootResolveContext } from "../workspace/root_index.js";

export function rootCtx(session: RdbSession): RootResolveContext {
    return { aliases: session.aliases };
}

export function nameOrHashRef(text: string): NameOrHashRef {
    if (text.startsWith('#')) {
        return { kind: 'hash', prefix: text.slice(1), span: { start: 0, end: text.length, line: 1, column: 1 } };
    }
    return { kind: 'name', text, parts: text.split('.'), span: { start: 0, end: text.length, line: 1, column: 1 } };
}

import type { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";

import { renderOp, type RenderOptions } from "./render.js";

export function firstNonCommentLine(sql: string): string {
    const lines = sql.split('\n');
    for (const line of lines) {
        if (!line.startsWith('--')) return line;
    }
    return lines[0] ?? '';
}

export function renderLogOpLine(
    payload: json.Literal,
    prev: B64Hash[],
    options: RenderOptions,
): string {
    const sql = renderOp(payload, {
        ...options,
        aliasMode: false,
        comments: false,
        profile: 'full',
        at: json.toSet(prev),
    });
    return firstNonCommentLine(sql);
}

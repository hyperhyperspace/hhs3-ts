import type { LangDiagnostic } from "@hyper-hyper-space/hhs3_rdb_lang";

// `firstLine` is the line of `file` the diagnosed text starts on; spans are
// relative to that text.
export function formatDiagnostics(diagnostics: LangDiagnostic[], file?: string, hints?: string[], firstLine = 1): string {
    const lines = diagnostics.map((diagnostic) => {
        const loc = diagnostic.span === undefined
            ? ''
            : `${file ?? '<input>'}:${diagnostic.span.line + firstLine - 1}:${diagnostic.span.column}: `;
        return `${loc}${diagnostic.severity} ${diagnostic.code}: ${diagnostic.message}`;
    });
    for (const hint of hints ?? []) {
        if (hint.length > 0) lines.push(hint);
    }
    return lines.join('\n');
}

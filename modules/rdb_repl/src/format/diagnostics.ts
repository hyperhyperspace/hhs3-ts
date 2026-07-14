import type { LangDiagnostic } from "@hyper-hyper-space/hhs3_rdb_lang";

export function formatDiagnostics(diagnostics: LangDiagnostic[], file?: string, hints?: string[]): string {
    const lines = diagnostics.map((diagnostic) => {
        const loc = diagnostic.span === undefined
            ? ''
            : `${file ?? '<input>'}:${diagnostic.span.line}:${diagnostic.span.column}: `;
        return `${loc}${diagnostic.severity} ${diagnostic.code}: ${diagnostic.message}`;
    });
    for (const hint of hints ?? []) {
        if (hint.length > 0) lines.push(hint);
    }
    return lines.join('\n');
}

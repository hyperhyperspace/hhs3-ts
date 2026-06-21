export type TextSpan = {
    start: number;
    end: number;
    line: number;
    column: number;
};

export type DiagnosticSeverity = 'error' | 'warning';

export type LangDiagnostic = {
    code: string;
    message: string;
    span?: TextSpan;
    severity: DiagnosticSeverity;
};

export type LangResultOk<T> = { ok: true; value: T };
export type LangResultErr = { ok: false; diagnostics: LangDiagnostic[] };
export type Result<T> = LangResultOk<T> | LangResultErr;

export class DiagnosticBag {
    private readonly diagnostics: LangDiagnostic[] = [];

    add(code: string, message: string, span?: TextSpan, severity: DiagnosticSeverity = 'error'): void {
        const diagnostic: LangDiagnostic = { code, message, severity };
        if (span !== undefined) diagnostic.span = span;
        this.diagnostics.push(diagnostic);
    }

    hasErrors(): boolean {
        return this.diagnostics.some((d) => d.severity === 'error');
    }

    all(): LangDiagnostic[] {
        return [...this.diagnostics];
    }

    merge(other: DiagnosticBag | LangDiagnostic[]): void {
        const incoming = Array.isArray(other) ? other : other.all();
        this.diagnostics.push(...incoming);
    }
}

export function ok<T>(value: T): Result<T> {
    return { ok: true, value };
}

export function err<T = never>(diagnostics: LangDiagnostic[]): Result<T> {
    return { ok: false, diagnostics };
}

export function spanFromOffsets(source: string, start: number, end: number): TextSpan {
    let line = 1;
    let column = 1;

    for (let i = 0; i < start && i < source.length; i += 1) {
        if (source[i] === '\n') {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }

    return { start, end, line, column };
}

export function combineSpans(a: TextSpan, b: TextSpan): TextSpan {
    return {
        start: Math.min(a.start, b.start),
        end: Math.max(a.end, b.end),
        line: a.start <= b.start ? a.line : b.line,
        column: a.start <= b.start ? a.column : b.column,
    };
}

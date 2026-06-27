import { DiagnosticBag, err, ok, Result, spanFromOffsets } from "../diagnostics.js";
import { KEYWORDS, Token } from "./tokens.js";

function isIdentStart(ch: string): boolean {
    return /[A-Za-z_]/.test(ch);
}

function isIdentPart(ch: string): boolean {
    return /[A-Za-z0-9_]/.test(ch);
}

function isHashPart(ch: string): boolean {
    return /[A-Za-z0-9_+/=-]/.test(ch);
}

function token(source: string, kind: Token['kind'], text: string, start: number, end: number, value?: Token['value']): Token {
    const t: Token = {
        kind,
        text,
        upper: text.toUpperCase(),
        span: spanFromOffsets(source, start, end),
    };
    if (value !== undefined) t.value = value;
    return t;
}

export function lex(source: string): Result<Token[]> {
    const diagnostics = new DiagnosticBag();
    const tokens: Token[] = [];
    let i = 0;

    const push = (kind: Token['kind'], text: string, start: number, end: number, value?: Token['value']) => {
        tokens.push(token(source, kind, text, start, end, value));
    };

    while (i < source.length) {
        const ch = source[i];

        if (/\s/.test(ch)) {
            i += 1;
            continue;
        }

        const start = i;

        if (ch === '-' && source[i + 1] === '-') {
            i += 2;
            while (i < source.length && source[i] !== '\n') i += 1;
            continue;
        }

        if (ch === '/' && source[i + 1] === '*') {
            i += 2;
            while (i < source.length && !(source[i] === '*' && source[i + 1] === '/')) i += 1;
            if (i >= source.length) {
                diagnostics.add('LEX_UNEXPECTED_CHAR', 'Unterminated block comment', spanFromOffsets(source, start, i));
            } else {
                i += 2;
            }
            continue;
        }

        if (isIdentStart(ch)) {
            i += 1;
            while (i < source.length) {
                if (isIdentPart(source[i])) {
                    i += 1;
                    continue;
                }
                if (source[i] === '.' && i + 1 < source.length && isIdentStart(source[i + 1])) {
                    i += 2;
                    while (i < source.length && isIdentPart(source[i])) i += 1;
                    continue;
                }
                if (source[i] === ':' && i + 1 < source.length && isIdentStart(source[i + 1])) {
                    i += 2;
                    while (i < source.length && isIdentPart(source[i])) i += 1;
                    continue;
                }
                break;
            }
            const text = source.substring(start, i);
            const upper = text.toUpperCase();
            if (upper === 'TRUE') push('keyword', text, start, i, true);
            else if (upper === 'FALSE') push('keyword', text, start, i, false);
            else if (upper === 'NULL') push('keyword', text, start, i, null);
            else push(KEYWORDS.has(upper) ? 'keyword' : 'identifier', text, start, i);
            continue;
        }

        if (ch === '$') {
            i += 1;
            if (i >= source.length || !isIdentStart(source[i])) {
                diagnostics.add('LEX_UNEXPECTED_CHAR', "Expected variable name after '$'", spanFromOffsets(source, start, i));
                continue;
            }
            while (i < source.length && isIdentPart(source[i])) i += 1;
            if (source[i] === '.' && i + 1 < source.length && isIdentStart(source[i + 1])) {
                i += 2;
                while (i < source.length && isIdentPart(source[i])) i += 1;
            }
            push('variable', source.substring(start, i), start, i);
            continue;
        }

        if (ch === '#') {
            i += 1;
            if (i >= source.length || !isHashPart(source[i])) {
                diagnostics.add('LEX_UNEXPECTED_CHAR', "Expected hash prefix after '#'", spanFromOffsets(source, start, i));
                continue;
            }
            while (i < source.length && isHashPart(source[i])) i += 1;
            push('hash', source.substring(start, i), start, i);
            continue;
        }

        if (ch === "'") {
            i += 1;
            let value = '';
            let closed = false;
            while (i < source.length) {
                if (source[i] === "'") {
                    if (source[i + 1] === "'") {
                        value += "'";
                        i += 2;
                        continue;
                    }
                    i += 1;
                    closed = true;
                    break;
                }
                value += source[i];
                i += 1;
            }
            if (!closed) {
                diagnostics.add('LEX_UNEXPECTED_CHAR', 'Unterminated string literal', spanFromOffsets(source, start, i));
            }
            push('string', source.substring(start, i), start, i, value);
            continue;
        }

        if (/[0-9]/.test(ch) || (ch === '-' && /[0-9]/.test(source[i + 1] ?? ''))) {
            i += 1;
            while (i < source.length && /[0-9]/.test(source[i])) i += 1;
            if (source[i] === '.' && /[0-9]/.test(source[i + 1] ?? '')) {
                i += 1;
                while (i < source.length && /[0-9]/.test(source[i])) i += 1;
            }
            const text = source.substring(start, i);
            push('number', text, start, i, Number(text));
            continue;
        }

        const two = source.substring(i, i + 2);
        if (['=>', '!=', '<=', '>=', '<>'].includes(two)) {
            i += 2;
            push('operator', two === '<>' ? '!=' : two, start, i);
            continue;
        }

        if (['=', '<', '>', '*'].includes(ch)) {
            i += 1;
            push('operator', ch, start, i);
            continue;
        }

        if (['(', ')', '{', '}', '[', ']', ',', ';'].includes(ch)) {
            i += 1;
            push('punctuation', ch, start, i);
            continue;
        }

        diagnostics.add('LEX_UNEXPECTED_CHAR', `Unexpected character '${ch}'`, spanFromOffsets(source, start, start + 1));
        i += 1;
    }

    push('eof', '', source.length, source.length);
    return diagnostics.hasErrors() ? err(diagnostics.all()) : ok(tokens);
}

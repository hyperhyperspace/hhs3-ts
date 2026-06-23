/**
 * Statement-boundary scanner — the rdb_lang analog of sqlite3_complete().
 *
 * Determines whether a text buffer constitutes a complete, terminated
 * statement by walking the character stream (handling strings, comments, and
 * bracket depth) without running the full grammar parser. This separates the
 * "is the user done typing?" question from the "is it valid?" question; the
 * latter is left to parseScript/parseStatement.
 *
 * The canonical terminator is `;` at bracket depth 0. Semicolons inside
 * BUNDLE ( ... ) are at depth > 0 and are correctly treated as separators,
 * not statement terminators.
 */

export type ScanStatus =
    | { kind: 'complete' }
    | { kind: 'incomplete-bracket'; depth: number }
    | { kind: 'incomplete-string' }
    | { kind: 'incomplete-comment' }
    | { kind: 'no-terminator' };

export function scanStatement(text: string): ScanStatus {
    let i = 0;
    let depth = 0;
    // True when the last non-whitespace, non-comment character we saw at
    // depth 0 was a `;`. A trailing line comment does not clear this flag,
    // so `SELECT 1; -- done` is correctly classified as complete.
    let afterTopLevelSemi = false;

    while (i < text.length) {
        const ch = text[i];

        // Whitespace: preserve afterTopLevelSemi
        if (ch === ' ' || ch === '\t' || ch === '\r' || ch === '\n') {
            i++;
            continue;
        }

        // Line comment: preserve afterTopLevelSemi (comment doesn't reset it)
        if (ch === '-' && text[i + 1] === '-') {
            i += 2;
            while (i < text.length && text[i] !== '\n') i++;
            continue;
        }

        // Block comment
        if (ch === '/' && text[i + 1] === '*') {
            i += 2;
            while (i < text.length && !(text[i] === '*' && text[i + 1] === '/')) i++;
            if (i >= text.length) return { kind: 'incomplete-comment' };
            i += 2;
            // Block comment does not reset afterTopLevelSemi
            continue;
        }

        // String literal (single-quoted, '' escaping)
        if (ch === "'") {
            i++;
            let closed = false;
            while (i < text.length) {
                if (text[i] === "'") {
                    if (text[i + 1] === "'") { i += 2; continue; }
                    i++;
                    closed = true;
                    break;
                }
                i++;
            }
            if (!closed) return { kind: 'incomplete-string' };
            afterTopLevelSemi = false;
            continue;
        }

        // Bracket open
        if (ch === '(' || ch === '[' || ch === '{') {
            depth++;
            afterTopLevelSemi = false;
            i++;
            continue;
        }

        // Bracket close
        if (ch === ')' || ch === ']' || ch === '}') {
            if (depth > 0) depth--;
            afterTopLevelSemi = false;
            i++;
            continue;
        }

        // Statement terminator
        if (ch === ';') {
            if (depth === 0) afterTopLevelSemi = true;
            // Semicolons inside brackets (e.g. BUNDLE body) are separators,
            // not top-level terminators, so we leave afterTopLevelSemi alone
            // only when at depth 0.
            i++;
            continue;
        }

        // Any other character (identifier, keyword, operator, number, …)
        afterTopLevelSemi = false;
        i++;
    }

    if (depth > 0) return { kind: 'incomplete-bracket', depth };
    if (afterTopLevelSemi) return { kind: 'complete' };
    return { kind: 'no-terminator' };
}

/** Returns true if the text contains any non-whitespace, non-comment content. */
function hasRealContent(text: string): boolean {
    let i = 0;
    while (i < text.length) {
        const ch = text[i];
        if (ch === ' ' || ch === '\t' || ch === '\r' || ch === '\n') { i++; continue; }
        if (ch === '-' && text[i + 1] === '-') { i += 2; while (i < text.length && text[i] !== '\n') i++; continue; }
        if (ch === '/' && text[i + 1] === '*') {
            i += 2;
            while (i < text.length && !(text[i] === '*' && text[i + 1] === '/')) i++;
            if (i < text.length) i += 2;
            continue;
        }
        return true;
    }
    return false;
}

/**
 * Split a multi-statement script text into individual statement strings.
 * Returns complete statements found; any incomplete (unterminated) trailing
 * content is returned as the last element only if it contains real tokens —
 * trailing comments/whitespace after the final `;` are silently dropped.
 */
export function splitStatements(text: string): string[] {
    const statements: string[] = [];
    let i = 0;
    let stmtStart = 0;
    let depth = 0;

    const flush = (end: number) => {
        const stmt = text.slice(stmtStart, end).trim();
        if (stmt.length > 0) statements.push(stmt);
        stmtStart = end;
    };

    while (i < text.length) {
        const ch = text[i];

        // Whitespace
        if (ch === ' ' || ch === '\t' || ch === '\r' || ch === '\n') { i++; continue; }

        // Line comment
        if (ch === '-' && text[i + 1] === '-') {
            i += 2;
            while (i < text.length && text[i] !== '\n') i++;
            continue;
        }

        // Block comment
        if (ch === '/' && text[i + 1] === '*') {
            i += 2;
            while (i < text.length && !(text[i] === '*' && text[i + 1] === '/')) i++;
            if (i < text.length) i += 2;
            continue;
        }

        // String literal
        if (ch === "'") {
            i++;
            while (i < text.length) {
                if (text[i] === "'") {
                    if (text[i + 1] === "'") { i += 2; continue; }
                    i++;
                    break;
                }
                i++;
            }
            continue;
        }

        // Brackets
        if (ch === '(' || ch === '[' || ch === '{') { depth++; i++; continue; }
        if (ch === ')' || ch === ']' || ch === '}') { if (depth > 0) depth--; i++; continue; }

        // Top-level `;` — statement boundary
        if (ch === ';' && depth === 0) {
            i++;
            flush(i);
            stmtStart = i;
            continue;
        }

        i++;
    }

    // Include trailing content only if it contains real tokens (not just comments/whitespace)
    const tail = text.slice(stmtStart);
    if (hasRealContent(tail)) statements.push(tail.trim());

    return statements;
}

// Writing source: edit a target-catalog.sql until it describes a target model,
// touching only what differs.
//
// The units are schema statements, the tables of a schema, the columns of a
// table (a foreign key is part of its column, as REFERENCES), the options of a
// table, the params, the groups and the FILES. The parser's spans locate each
// one.
//
//   - an unchanged unit is kept verbatim, comments and layout included;
//   - a changed unit is replaced by its source form;
//   - a new unit goes after its last sibling, with that sibling's indentation
//     (new schemas before CREATE CATALOG, a new param into PARAMS or a new
//     PARAMS clause);
//   - a unit that's gone is removed with its separator and the comment lines
//     directly above it;
//   - a table whose identity provider changed is replaced whole.
//
// Reading the result back must give the target; from empty text the result
// is the target rendered whole.

import type { CreateCatalogStatement, CreateSchemaStatement, SourceKeyLabels, TextSpan } from "@hyper-hyper-space/hhs3_rdb_lang";
import {
    parseScript, renderColumnDef, renderSourceCatalog, renderSourceFiles, renderSourceGroup, renderSourceParam, renderSourceParams,
    renderSourceSchema, renderTableDef, renderTableOptions,
} from "@hyper-hyper-space/hhs3_rdb_lang";

import type { KeyDirectory } from "./keys.js";
import {
    creatorsKey, filesKey, groupKey, modelDifferences, orderByBindings, sameColumn, sameConcurrentDeletes, sameIdProvider, sameRestrictions,
    sameTable, type CatalogModel, type SchemaModel,
} from "./model.js";
import { SOURCE_FILE } from "./project.js";
import { isBlankSql, readSource } from "./source.js";

export class WriteSourceError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'WriteSourceError';
    }
}

// Insertions at one position land in `order` (then list order): a table's
// options before the table that follows it.
type Edit = { start: number; end: number; text: string; order?: number };

function applyEdits(text: string, edits: Edit[]): string {
    const indexed = edits.map((edit, i) => ({ edit, i }));
    const sorted = indexed.sort((a, b) => b.edit.start - a.edit.start || b.edit.end - a.edit.end
        || (b.edit.order ?? 0) - (a.edit.order ?? 0) || b.i - a.i).map((x) => x.edit);
    let out = text;
    let limit = Infinity;
    for (const edit of sorted) {
        if (edit.end > limit) throw new WriteSourceError('overlapping source edits (a bug in rpack)');
        out = out.slice(0, edit.start) + edit.text + out.slice(edit.end);
        limit = edit.start;
    }
    return out;
}

function lineStart(text: string, pos: number): number {
    return text.lastIndexOf('\n', pos - 1) + 1;
}

// The whitespace before `pos` on its line, when only whitespace is there.
function indentAt(text: string, pos: number): string | undefined {
    const before = text.slice(lineStart(text, pos), pos);
    return /^[ \t]*$/.test(before) ? before : undefined;
}

// Where removing a unit starting at `start` begins: its line, and the comment
// lines directly above it, when the unit starts its line.
function removalStart(text: string, start: number): number {
    if (indentAt(text, start) === undefined) return start;
    let from = lineStart(text, start);
    while (from > 0) {
        const prev = lineStart(text, from - 1);
        if (!text.slice(prev, from - 1).trim().startsWith('--')) break;
        from = prev;
    }
    return from;
}

// Past trailing spaces and one line break, when nothing else follows on the
// line.
function removalEnd(text: string, end: number): number {
    const match = /^[ \t]*\n/.exec(text.slice(end));
    return match === null ? end : end + match[0].length;
}

// Removes whole lines from `start` to `end`; with a blank line on both sides,
// one of the two goes too.
function removal(text: string, start: number, end: number): Edit {
    const blankBefore = start >= 2 && text[start - 1] === '\n' && text[start - 2] === '\n';
    const blankAfter = text[end] === '\n';
    return { start, end: blankBefore && blankAfter ? end + 1 : end, text: '' };
}

// The position just past a comma that follows `end` (over whitespace), if any.
function commaAfter(text: string, end: number): number | undefined {
    const match = /^\s*,/.exec(text.slice(end));
    return match === null ? undefined : end + match[0].length;
}

// The position of a comma before `start` (over whitespace and comments).
function commaBefore(text: string, start: number): number | undefined {
    let i = start - 1;
    while (i >= 0) {
        const c = text[i];
        if (c === ',') return i;
        if (/\s/.test(c)) { i -= 1; continue; }
        const line = lineStart(text, i);
        if (text.slice(line, i + 1).trim().startsWith('--')) { i = line - 1; continue; }
        return undefined;
    }
    return undefined;
}

type ListItem = { span: TextSpan; remove?: boolean; replace?: string };

// Edits to a comma-separated list: replacements in place, removals with their
// separators, and new items after the last one that stays.
function listEdits(
    text: string,
    items: ListItem[],
    append: string[],
    opts: { separator: (indent: string) => string; openAt: number; openText: (items: string[]) => string },
): Edit[] {
    const edits: Edit[] = [];
    for (const item of items) {
        if (item.replace !== undefined && !item.remove) edits.push({ start: item.span.start, end: item.span.end, text: item.replace });
    }
    const kept = items.filter((i) => !i.remove);
    if (kept.length === 0 && items.length > 0) {
        const first = items[0].span;
        const last = items[items.length - 1].span;
        const end = commaAfter(text, last.end) ?? last.end;
        edits.push({ start: first.start, end, text: append.join(opts.separator(indentAt(text, first.start) ?? '  ')) });
        return edits;
    }

    // Removed runs: before a kept item, with the comma after the run; at the
    // end, with its trailing comma or the comma before it.
    let i = 0;
    while (i < items.length) {
        if (!items[i].remove) { i += 1; continue; }
        let j = i;
        while (j + 1 < items.length && items[j + 1].remove) j += 1;
        const first = items[i].span;
        const last = items[j].span;
        const trailing = commaAfter(text, last.end);
        if (j < items.length - 1 || trailing !== undefined) {
            edits.push(removal(text, removalStart(text, first.start), removalEnd(text, trailing ?? last.end)));
        } else {
            const before = commaBefore(text, first.start) ?? first.start;
            edits.push({ start: before, end: last.end, text: '' });
        }
        i = j + 1;
    }

    if (append.length > 0) {
        if (kept.length === 0) {
            edits.push({ start: opts.openAt, end: opts.openAt, text: opts.openText(append) });
        } else {
            const anchor = kept[kept.length - 1].span;
            const indent = indentAt(text, anchor.start) ?? '  ';
            edits.push({ start: anchor.end, end: anchor.end, text: append.map((a) => opts.separator(indent) + a).join(''), order: 1 });
        }
    }
    return edits;
}

function sourceSchema(schema: SchemaModel) {
    return {
        name: schema.name,
        creators: schema.creators,
        ...(schema.hashAlgorithm !== undefined ? { hashAlgorithm: schema.hashAlgorithm } : {}),
        tables: [...schema.tables.values()],
    };
}

// The whole target, rendered.
export function renderSource(target: CatalogModel, labels?: SourceKeyLabels): string {
    const parts = [...target.schemas.values()].map((s) => renderSourceSchema(sourceSchema(s), labels));
    parts.push(renderSourceCatalog({
        name: target.name,
        creators: target.creators,
        ...(target.seed !== undefined ? { seed: target.seed } : {}),
        ...(target.hashAlgorithm !== undefined ? { hashAlgorithm: target.hashAlgorithm } : {}),
        params: [...target.params.values()],
        groups: orderByBindings([...target.groups.values()]),
        files: [...target.files.values()],
    }, labels));
    return `${parts.join('\n\n')}\n`;
}

// The end of a statement, past its `;`.
function statementEnd(text: string, span: TextSpan): number {
    const match = /^\s*;/.exec(text.slice(span.end));
    return match === null ? span.end : span.end + match[0].length;
}

function schemaEdits(text: string, statement: CreateSchemaStatement, current: SchemaModel, target: SchemaModel, labels: SourceKeyLabels): Edit[] {
    if (creatorsKey(current.creators) !== creatorsKey(target.creators) || current.hashAlgorithm !== target.hashAlgorithm) {
        return [{ start: statement.span.start, end: statementEnd(text, statement.span), text: renderSourceSchema(sourceSchema(target), labels) }];
    }
    const edits: Edit[] = [];
    const tableItems: ListItem[] = [];
    for (const decl of statement.tables) {
        const wanted = target.tables.get(decl.name);
        if (wanted === undefined) { tableItems.push({ span: decl.span, remove: true }); continue; }
        tableItems.push({ span: decl.span });
        const def = current.tables.get(decl.name)!;
        if (sameTable(def, wanted)) continue;
        if (!sameIdProvider(def, wanted)) {
            edits.push({ start: decl.span.start, end: decl.span.end, text: renderTableDef(wanted) });
            continue;
        }

        const columnItems: ListItem[] = decl.columns.map((column) => {
            const after = wanted.columns[column.name];
            if (after === undefined) return { span: column.span, remove: true };
            const same = sameColumn(def.columns[column.name], after) && def.fks?.[column.name] === wanted.fks?.[column.name];
            return same ? { span: column.span } : { span: column.span, replace: renderColumnDef(column.name, after, wanted.fks?.[column.name]) };
        });
        const declared = new Set(decl.columns.map((c) => c.name));
        const newColumns = Object.entries(wanted.columns).filter(([name]) => !declared.has(name))
            .map(([name, column]) => renderColumnDef(name, column, wanted.fks?.[name]));
        edits.push(...listEdits(text, columnItems, newColumns, {
            separator: (indent) => `,\n${indent}`,
            openAt: decl.body.start + 1,
            openText: (items) => `\n    ${items.join(',\n    ')}\n  `,
        }));

        if (!sameConcurrentDeletes(def, wanted) || !sameRestrictions(def, wanted)) {
            const options = renderTableOptions(wanted);
            if (decl.options.length === 0) {
                edits.push({ start: decl.body.end, end: decl.body.end, text: options });
            } else {
                const first = decl.options[0].span.start;
                const last = decl.options[decl.options.length - 1].span.end;
                edits.push(options.length === 0
                    ? { start: decl.body.end, end: last, text: '' }
                    : { start: first, end: last, text: options.replace(/^\s+/, '') });
            }
        }
    }
    const declared = new Set(statement.tables.map((t) => t.name));
    const newTables = [...target.tables.values()].filter((t) => !declared.has(t.name)).map((t) => renderTableDef(t));
    edits.push(...listEdits(text, tableItems, newTables, {
        separator: (indent) => `,\n\n${indent}`,
        openAt: text.indexOf('(', statement.body.start) + 1,
        openText: (items) => `\n  ${items.join(',\n\n  ')}\n`,
    }));
    return edits;
}

function catalogEdits(text: string, statement: CreateCatalogStatement, current: CatalogModel, target: CatalogModel, labels: SourceKeyLabels): Edit[] {
    if (current.name !== target.name || creatorsKey(current.creators) !== creatorsKey(target.creators)
        || current.seed !== target.seed || current.hashAlgorithm !== target.hashAlgorithm) {
        return [{
            start: statement.span.start,
            end: statementEnd(text, statement.span),
            text: renderSourceCatalog({
                name: target.name, creators: target.creators,
                ...(target.seed !== undefined ? { seed: target.seed } : {}),
                ...(target.hashAlgorithm !== undefined ? { hashAlgorithm: target.hashAlgorithm } : {}),
                params: [...target.params.values()], groups: orderByBindings([...target.groups.values()]),
                files: [...target.files.values()],
            }, labels),
        }];
    }
    const edits: Edit[] = [];

    const paramItems: ListItem[] = statement.params.map((param) => {
        const wanted = target.params.get(param.name);
        if (wanted === undefined) return { span: param.span, remove: true };
        return wanted.type === current.params.get(param.name)?.type ? { span: param.span } : { span: param.span, replace: renderSourceParam(wanted) };
    });
    const newParams = [...target.params.values()].filter((p) => !statement.params.some((q) => q.name === p.name));
    if (statement.params.length === 0 && newParams.length > 0) {
        edits.push({ start: statement.body.start, end: statement.body.start, text: `${renderSourceParams(newParams)} ` });
    } else {
        edits.push(...listEdits(text, paramItems, newParams.map(renderSourceParam), {
            separator: () => ', ',
            openAt: statement.params[0]?.span.start ?? statement.body.start,
            openText: (items) => items.join(', '),
        }));
    }

    // Groups and FILES share the body's list, in source order.
    const groupItems: ListItem[] = statement.groups.map((group) => {
        const wanted = target.groups.get(group.name);
        if (wanted === undefined) return { span: group.span, remove: true };
        return groupKey(wanted) === groupKey(current.groups.get(group.name)!)
            ? { span: group.span }
            : { span: group.span, replace: renderSourceGroup(wanted, { labels }) };
    });
    const filesItems: ListItem[] = statement.files.map((files) => {
        const wanted = target.files.get(files.name);
        if (wanted === undefined) return { span: files.span, remove: true };
        return filesKey(wanted) === filesKey(current.files.get(files.name)!)
            ? { span: files.span }
            : { span: files.span, replace: renderSourceFiles(wanted) };
    });
    const bodyItems = [...groupItems, ...filesItems].sort((a, b) => a.span.start - b.span.start);
    const declared = new Set(statement.groups.map((g) => g.name));
    const newGroups = orderByBindings([...target.groups.values()].filter((g) => !declared.has(g.name)), declared);
    const declaredFiles = new Set(statement.files.map((f) => f.name));
    const newFiles = [...target.files.values()].filter((f) => !declaredFiles.has(f.name));
    const appended = [...newGroups.map((g) => renderSourceGroup(g, { labels })), ...newFiles.map((f) => renderSourceFiles(f))];
    edits.push(...listEdits(text, bodyItems, appended, {
        separator: (indent) => `,\n${indent}`,
        openAt: text.indexOf('(', statement.body.start) + 1,
        openText: (items) => `\n  ${items.join(',\n  ')}\n`,
    }));
    return edits;
}

// `text` without its CREATE CATALOG VERSION clause, when that names one of
// `stale` (a parent's version). Schema VERSIONs are the author's and stay. Text
// that doesn't parse is returned as it is; reading it reports the error.
export function stripCatalogVersion(text: string, stale: Set<string>): { text: string; removed?: string } {
    const parsed = parseScript(text, { catalogVersionOptional: true });
    if (!parsed.ok) return { text };
    for (const statement of parsed.value.statements) {
        if (statement.kind !== 'create-catalog') continue;
        const { version, versionSpan } = statement;
        if (version === undefined || versionSpan === undefined || !stale.has(version)) return { text };
        let start = versionSpan.start;
        while (start > 0 && (text[start - 1] === ' ' || text[start - 1] === '\t')) start -= 1;
        let end = versionSpan.end;
        // A clause alone on its line goes with its line break.
        if (start > 0 && text[start - 1] === '\n' && /^[ \t]*\n/.test(text.slice(end))) {
            end += /^[ \t]*\n/.exec(text.slice(end))![0].length;
        }
        return { text: text.slice(0, start) + text.slice(end), removed: version };
    }
    return { text };
}

// `text` edited to describe `target`. `keys` and `signer` read the text and
// the result, as target-catalog.sql is read for a release.
export async function writeSource(text: string, target: CatalogModel, keys: KeyDirectory, signer: string): Promise<string> {
    const labels = keys.labels();
    let result: string;
    if (isBlankSql(text)) {
        result = renderSource(target, labels);
    } else {
        const read = await readSource(text, keys, signer);
        const current = read.model;
        const edits: Edit[] = [];
        let catalog: CreateCatalogStatement | undefined;
        for (const statement of read.script.statements) {
            if (statement.kind === 'create-schema') {
                const wanted = target.schemas.get(statement.name);
                if (wanted === undefined) {
                    edits.push(removal(text, removalStart(text, statement.span.start), removalEnd(text, statementEnd(text, statement.span))));
                } else {
                    edits.push(...schemaEdits(text, statement, current.schemas.get(statement.name)!, wanted, labels));
                }
            } else if (statement.kind === 'create-catalog') {
                catalog = statement;
                edits.push(...catalogEdits(text, statement, current, target, labels));
            }
        }
        const newSchemas = [...target.schemas.values()].filter((s) => !current.schemas.has(s.name));
        if (newSchemas.length > 0 && catalog !== undefined) {
            const at = removalStart(text, catalog.span.start);
            edits.push({ start: at, end: at, text: newSchemas.map((s) => `${renderSourceSchema(sourceSchema(s), labels)}\n\n`).join('') });
        }
        result = applyEdits(text, edits);
    }

    const check = await readSource(result, keys, signer);
    const left = modelDifferences(check.model, target);
    if (left.length > 0) throw new WriteSourceError(`the written ${SOURCE_FILE} doesn't describe the target (a bug in rpack): ${left.join('; ')}`);
    return result;
}

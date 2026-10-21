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

import type { CreateCatalogStatement, CreateSchemaStatement, ImportSchemaStatement, SourceKeyLabels, TextSpan } from "@hyper-hyper-space/hhs3_rdb_lang";
import type { TableDef } from "@hyper-hyper-space/hhs3_rdb";
import {
    parseScript, renderColumnDef, renderIdent, renderLiteral, renderMigrationRule, renderSchemaParamDecl, renderSchemaParamDecls, renderSourceCatalog, renderSourceFiles, renderSourceGroup, renderSourceParam, renderSourceParams,
    renderSourceSchema, renderTableDef, renderTableOptions,
} from "@hyper-hyper-space/hhs3_rdb_lang";

import type { SchemaReleaseFile } from "./format.js";
import type { KeyDirectory } from "./keys.js";
import {
    creatorsKey, filesKey, groupSourceKey, modelDifferences, orderByBindings, paramDifferences, sameColumn, sameConcurrentDeletes, sameIdProvider, sameParamDecl, sameRestrictions,
    sameTable, tableDifferences, type CatalogModel, type SchemaModel,
} from "./model.js";
import { SOURCE_FILE } from "./project.js";
import { diffTables } from "./schema_diff.js";
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
        ...(schema.params !== undefined && schema.params.size > 0 ? { params: [...schema.params.values()] } : {}),
        ...(schema.hashAlgorithm !== undefined ? { hashAlgorithm: schema.hashAlgorithm } : {}),
        tables: [...schema.tables.values()],
    };
}

function renderSchemaEntry(schema: SchemaModel, labels?: SourceKeyLabels, versions?: string[]): string {
    const listed = versions !== undefined && versions.length > 0
        ? versions
        : schema.origin?.kind === 'imported' ? schema.origin.versions : undefined;
    if (listed === undefined) return renderSourceSchema(sourceSchema(schema), labels);
    let text = `IMPORT SCHEMA ${renderIdent(schema.name)} VERSION ${listed.map((v) => renderLiteral(v)).join(', ')};`;
    if (schema.origin?.kind === 'imported') {
        const block = synthesizeImportedPatch(schema.name, schema.origin.upstream.tables, schema.tables);
        if (block.length > 0) text += `\n\n${block}`;
    }
    return text;
}

// The whole target, rendered. `importedVersions` names schemas that are imported
// even when the model carries no origin (a merged release's schema).
export function renderSource(target: CatalogModel, labels?: SourceKeyLabels, importedVersions?: Map<string, string[]>): string {
    const parts = [...target.schemas.values()].map((s) => renderSchemaEntry(s, labels, importedVersions?.get(s.name)));
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

function importSchemaEdits(text: string, statement: ImportSchemaStatement, target: SchemaModel, labels: SourceKeyLabels, versions?: string[]): Edit[] {
    const listed = versions ?? (target.origin?.kind === 'imported' ? target.origin.versions : undefined);
    if (listed !== undefined && listed.length > 0) {
        const same = statement.versions.length === listed.length && statement.versions.every((v, i) => v === listed[i]);
        if (same) return [];
        return [{ start: statement.versionsSpan.start, end: statement.versionsSpan.end, text: `VERSION ${listed.map((v) => renderLiteral(v)).join(', ')}` }];
    }
    return [{ start: statement.span.start, end: statementEnd(text, statement.span), text: renderSourceSchema(sourceSchema(target), labels) }];
}

function schemaEdits(text: string, statement: CreateSchemaStatement, current: SchemaModel, target: SchemaModel, labels: SourceKeyLabels): Edit[] {
    if (creatorsKey(current.creators) !== creatorsKey(target.creators) || current.hashAlgorithm !== target.hashAlgorithm) {
        return [{ start: statement.span.start, end: statementEnd(text, statement.span), text: renderSourceSchema(sourceSchema(target), labels) }];
    }
    const edits: Edit[] = [];

    const currentParams = current.params ?? new Map();
    const targetParams = target.params ?? new Map();
    if (paramDifferences(target.name, currentParams, targetParams).length > 0) {
        const stmtParams = statement.params ?? [];
        const paramItems: ListItem[] = stmtParams.map((p) => {
            const wanted = targetParams.get(p.name);
            if (wanted === undefined) return { span: p.span, remove: true };
            const cur = currentParams.get(p.name);
            return cur !== undefined && sameParamDecl(cur, wanted)
                ? { span: p.span }
                : { span: p.span, replace: renderSchemaParamDecl(wanted) };
        });
        const newParams = [...targetParams.values()].filter((p) => !stmtParams.some((q) => q.name === p.name));
        if (stmtParams.length === 0 && newParams.length > 0) {
            edits.push({ start: statement.body.start, end: statement.body.start, text: `${renderSchemaParamDecls(newParams)} ` });
        } else {
            edits.push(...listEdits(text, paramItems, newParams.map(renderSchemaParamDecl), {
                separator: () => ', ',
                openAt: stmtParams[0]?.span.start ?? statement.body.start,
                openText: (items) => items.join(', '),
            }));
        }
    }

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
        return groupSourceKey(wanted) === groupSourceKey(current.groups.get(group.name)!)
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

// After the IMPORT version list is updated, a branch merge may need one ALTER
// SCHEMA that reproduces the merged tables from the new upstream state.
async function reconcileImportedPatches(
    text: string,
    target: CatalogModel,
    keys: KeyDirectory,
    signer: string,
    imports: Map<string, SchemaReleaseFile> | undefined,
): Promise<string> {
    let read;
    try {
        read = await readSource(text, keys, signer, SOURCE_FILE, { imports });
    } catch {
        return text;
    }
    const edits: Edit[] = [];
    for (const statement of read.script.statements) {
        if (statement.kind !== 'import-schema') continue;
        const wanted = target.schemas.get(statement.name);
        const current = read.model.schemas.get(statement.name);
        if (wanted === undefined || current?.origin?.kind !== 'imported') continue;
        if (tableDifferences(statement.name, current.tables, wanted.tables).length === 0
            && paramDifferences(statement.name, current.params, wanted.params).length === 0) continue;
        if (paramDifferences(statement.name, current.origin.upstream.params, wanted.params).length > 0) {
            throw new WriteSourceError(`schema ${statement.name} changes its parameters, which a local ALTER SCHEMA cannot express`);
        }
        const block = synthesizeImportedPatch(statement.name, current.origin.upstream.tables, wanted.tables);
        const alters = read.script.statements.filter((s) => s.kind === 'alter-schema' && s.schema.kind === 'name' && s.schema.text === statement.name);
        if (block.length === 0) {
            for (const alter of alters) {
                edits.push(removal(text, removalStart(text, alter.span.start), removalEnd(text, statementEnd(text, alter.span))));
            }
        } else if (alters.length === 0) {
            const end = statementEnd(text, statement.span);
            edits.push({ start: end, end, text: `\n\n${block}` });
        } else {
            const first = alters[0]!;
            edits.push({ start: first.span.start, end: statementEnd(text, first.span), text: block });
            for (const alter of alters.slice(1)) {
                edits.push(removal(text, removalStart(text, alter.span.start), removalEnd(text, statementEnd(text, alter.span))));
            }
        }
    }
    return edits.length === 0 ? text : applyEdits(text, edits);
}

function synthesizeImportedPatch(name: string, upstream: Map<string, TableDef>, target: Map<string, TableDef>): string {
    for (const [table, def] of upstream) {
        const wanted = target.get(table);
        if (wanted !== undefined && !sameIdProvider(def, wanted)) {
            throw new WriteSourceError(`schema ${name} changes its IDENTITY PROVIDER, which ALTER SCHEMA cannot express`);
        }
    }
    for (const [table, def] of target) {
        const had = upstream.get(table);
        if (had !== undefined && !sameIdProvider(had, def)) {
            throw new WriteSourceError(`schema ${name} changes its IDENTITY PROVIDER, which ALTER SCHEMA cannot express`);
        }
    }
    const working = new Map<string, TableDef>();
    const desired = new Map<string, TableDef>();
    const reset: string[] = [];
    for (const [table, def] of upstream) {
        const columns = { ...def.columns };
        const wanted = target.get(table);
        if (wanted !== undefined) {
            for (const [column, col] of Object.entries(def.columns)) {
                const after = wanted.columns[column];
                if (after !== undefined && !sameColumn(col, after)) {
                    delete columns[column];
                    reset.push(`  ${renderMigrationRule({ rule: 'drop-column', table, column })}, -- column ${table}.${column} was reset in a parent release`);
                    reset.push(`  ${renderMigrationRule({ rule: 'add-column', table, column, def: after })},`);
                }
            }
        }
        working.set(table, { ...def, columns });
    }
    for (const [table, def] of target) {
        const columns = { ...def.columns };
        const had = upstream.get(table);
        if (had !== undefined) {
            for (const column of Object.keys(def.columns)) {
                const before = had.columns[column];
                if (before !== undefined && !sameColumn(before, def.columns[column]!)) delete columns[column];
            }
        }
        desired.set(table, { ...def, columns });
    }
    const diff = diffTables(name, working, desired);
    if (diff.refusals.length > 0) throw new WriteSourceError(diff.refusals.map((r) => r.message).join('; '));
    const lines = [...reset, ...diff.rules.map((rule) => `  ${renderMigrationRule(rule)},`)];
    if (lines.length === 0) return '';
    lines[lines.length - 1] = lines[lines.length - 1]!.replace(/,\s*$/, '');
    return `ALTER SCHEMA ${renderIdent(name)} AS (\n${lines.join('\n')}\n);`;
}

// `text` edited to describe `target`. `keys` and `signer` read the text and
// the result, as target-catalog.sql is read for a release.
export async function writeSource(
    text: string,
    target: CatalogModel,
    keys: KeyDirectory,
    signer: string,
    options: { imports?: Map<string, SchemaReleaseFile>; importedVersions?: Map<string, string[]> } = {},
): Promise<string> {
    const labels = keys.labels();
    let result: string;
    if (isBlankSql(text)) {
        result = renderSource(target, labels, options.importedVersions);
    } else {
        const read = await readSource(text, keys, signer, SOURCE_FILE, { imports: options.imports });
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
            } else if (statement.kind === 'import-schema') {
                const wanted = target.schemas.get(statement.name);
                if (wanted === undefined) {
                    edits.push(removal(text, removalStart(text, statement.span.start), removalEnd(text, statementEnd(text, statement.span))));
                } else {
                    edits.push(...importSchemaEdits(text, statement, wanted, labels, options.importedVersions?.get(statement.name)));
                }
            } else if (statement.kind === 'create-catalog') {
                catalog = statement;
                edits.push(...catalogEdits(text, statement, current, target, labels));
            }
        }
        const newSchemas = [...target.schemas.values()].filter((s) => !current.schemas.has(s.name));
        if (newSchemas.length > 0 && catalog !== undefined) {
            const at = removalStart(text, catalog.span.start);
            edits.push({ start: at, end: at, text: newSchemas.map((s) => `${renderSchemaEntry(s, labels, options.importedVersions?.get(s.name))}\n\n`).join('') });
        }
        result = applyEdits(text, edits);
    }
    result = await reconcileImportedPatches(result, target, keys, signer, options.imports);

    const check = await readSource(result, keys, signer, SOURCE_FILE, { imports: options.imports });
    const left = modelDifferences(check.model, target);
    if (left.length > 0) throw new WriteSourceError(`the written ${SOURCE_FILE} doesn't describe the target (a bug in rpack): ${left.join('; ')}`);
    return result;
}

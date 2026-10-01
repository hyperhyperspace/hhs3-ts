// Reading a catalog repository's sources.
//
// target-catalog.sql is the desired state: CREATE SCHEMA statements and one
// CREATE CATALOG. AT LATEST may be written on a group; a hash or a version set
// may not. A catalog VERSION, NOTE, or BY may be written when it
// agrees with the release. A schema may state its own VERSION, which the
// release honors; otherwise, while the file is evaluated, it is created at the
// catalog's version. rpack
// checks those forms, then evaluates the text in a scratch runtime where each
// developer key is a stand-in that can sign, and reads the model back from the
// created payloads, with every stand-in replaced by the real key. The signed
// release still supplies its own version and signature.
//
// upgrade-manual.sql holds the explicit first steps of the next release: ALTER
// SCHEMA statements on schemas of its parents, compiled against the parents'
// views the way the binder compiles an ALTER SCHEMA.

import {
    catalogGroupHash,
    type CreateRCatalogPayload, type CreateRSchemaPayload, type MigrationRule, type RSchema, type RSchemaView, type TableDef,
} from "@hyper-hyper-space/hhs3_rdb";
import {
    buildAlterColumnsOf, compileMigrationRules, parseScript,
    type AstScript, type AstStatement, type AuthorExpr, type LangDiagnostic, type MigrationRuleExpr, type TextSpan, type VersionExpr,
} from "@hyper-hyper-space/hhs3_rdb_lang";
import { LanguageError, RdbRuntime } from "@hyper-hyper-space/hhs3_rdb_runtime";

import { createStandIns, replaceKeys, type KeyDirectory } from "./keys.js";
import { filesModelOf, groupModelOf, type CatalogModel, type FilesModel, type GroupModel, type SchemaModel } from "./model.js";
import { SOURCE_FILE, UPGRADE_MANUAL_FILE } from "./project.js";

export type Where = { file: string; line: number; column: number };

export type SourceIssue = { message: string; where?: Where };

export function formatWhere(where: Where | undefined): string {
    return where === undefined ? '' : `${where.file}:${where.line}:${where.column}: `;
}

export class SourceError extends Error {
    constructor(readonly issues: SourceIssue[]) {
        super(issues.map((i) => `${formatWhere(i.where)}${i.message}`).join('\n'));
        this.name = 'SourceError';
    }
}

function whereOf(file: string, span: TextSpan | undefined): Where | undefined {
    return span === undefined ? undefined : { file, line: span.line, column: span.column };
}

function issuesOf(file: string, diagnostics: LangDiagnostic[]): SourceIssue[] {
    return diagnostics.map((d) => ({ message: d.message, ...(d.span !== undefined ? { where: whereOf(file, d.span) } : {}) }));
}

// Positions of the units of target-catalog.sql, for messages.
export type Locator = (unit: string) => Where | undefined;

export const unitKey = {
    schema: (schema: string) => `schema ${schema}`,
    table: (schema: string, table: string) => `table ${schema} ${table}`,
    column: (schema: string, table: string, column: string) => `column ${schema} ${table} ${column}`,
    group: (group: string) => `group ${group}`,
    files: (files: string) => `files ${files}`,
    param: (param: string) => `param ${param}`,
    catalog: () => 'catalog',
};

export type ReadSource = {
    text: string;
    script: AstScript;
    model: CatalogModel;
    // Every stand-in key id and public key, which nothing produced may contain.
    standIns: string[];
    // Schema name -> the VERSION its CREATE SCHEMA states. A schema that
    // states none is absent.
    schemaVersions: Map<string, string>;
    locate: Locator;
};

// The version a source's CREATE CATALOG evaluates at when the statement omits
// VERSION and no release version is expected. The model has no version.
const SOURCE_CATALOG_VERSION = '0.0.1';

// What a stated catalog clause is compared to. An omitted field is not a
// mismatch: version and note are supplied by the release, and BY by the signer.
export type SourceExpect = {
    version?: string;
    note?: string;
};

export async function readSource(
    text: string, keys: KeyDirectory, signer: string, file = SOURCE_FILE, expect: SourceExpect = {},
): Promise<ReadSource> {
    const parsed = parseScript(text, { catalogVersionOptional: true });
    if (!parsed.ok) throw new SourceError(issuesOf(file, parsed.diagnostics));
    const script = parsed.value;
    checkSourceForms(script, file, signer, keys, expect);

    if (keys.get(signer) === undefined) {
        throw new SourceError([{ message: `rpack.json's key '${signer}' is not in your keystore` }]);
    }
    const standIns = await createStandIns(keys);
    const runtime = await RdbRuntime.openMemory({ keyVault: standIns.vault });
    try {
        await runtime.session.unlockKey(signer, standIns.passphrase);
        runtime.session.selectAuthor(signer);

        const catalogVersion = expect.version ?? SOURCE_CATALOG_VERSION;
        let run;
        try {
            run = await runtime.execute(textForEval(text, script), {
                source: { catalogVersion, schemaVersion: schemaStampOf(script, catalogVersion) },
            });
        } catch (err) {
            throw new SourceError(err instanceof LanguageError ? issuesOf(file, err.diagnostics).map(explainLabel) : [{ message: messageOf(err) }]);
        }

        const schemas = new Map<string, SchemaModel>();
        let catalog: CreateRCatalogPayload | undefined;
        for (const item of run.results) {
            const result = item.result;
            if (result.kind !== 'create-plan') continue;
            if (result.plan.kind === 'create-schema') {
                const payload = result.plan.payload as CreateRSchemaPayload;
                schemas.set(payload.name, schemaModelOf(payload));
            } else if (result.plan.kind === 'create-catalog') {
                catalog = result.plan.payload as CreateRCatalogPayload;
            }
        }
        if (catalog === undefined) throw new SourceError([{ message: `${file} has no CREATE CATALOG` }]);

        const defNames = new Map((catalog.add ?? []).map((def) => [catalogGroupHash(def), def.name]));
        const groups = new Map<string, GroupModel>();
        for (const def of catalog.add ?? []) {
            const schema = (await runtime.workspace.replica.getObject(def.schemaRef)) as unknown as RSchema;
            groups.set(def.name, groupModelOf(def, schema.getName(), (hash) => defNames.get(hash)!));
        }
        const files = new Map<string, FilesModel>();
        for (const def of catalog.files ?? []) files.set(def.name, filesModelOf(def, (hash) => defNames.get(hash)!));

        const model: CatalogModel = {
            name: catalog.name,
            creators: catalog.creators,
            ...(catalog.seed !== undefined ? { seed: catalog.seed } : {}),
            ...(catalog.hashAlgorithm !== undefined ? { hashAlgorithm: catalog.hashAlgorithm } : {}),
            params: new Map((catalog.params ?? []).map((p) => [p.name, p])),
            schemas,
            groups,
            files,
        };
        return {
            text,
            script,
            model: replaceKeys(model, standIns.replace),
            standIns: [...standIns.replace.keys()],
            schemaVersions: statedSchemaVersions(script),
            locate: locatorOf(script, file),
        };
    } finally {
        await runtime.close();
    }
}

function schemaModelOf(payload: CreateRSchemaPayload): SchemaModel {
    return {
        name: payload.name,
        creators: payload.creators,
        ...(payload.hashAlgorithm !== undefined ? { hashAlgorithm: payload.hashAlgorithm } : {}),
        tables: new Map<string, TableDef>(payload.tables.map((t) => [t.name, t])),
    };
}

function explainLabel(issue: SourceIssue): SourceIssue {
    const match = /Unknown variable '\$([^']+)'/.exec(issue.message);
    if (match === null) return issue;
    return { ...issue, message: `$${match[1]} is not a key in your keystore (${SOURCE_FILE} names keys by their keystore labels)` };
}

function messageOf(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
}

// A BY that matches the signer is dropped before evaluation. The scratch
// runtime signs with a stand-in under the signer's label, and a #prefix of
// the real key id does not resolve there. The selected author is that stand-in.
function textForEval(text: string, script: AstScript): string {
    for (const statement of script.statements) {
        if (statement.kind !== 'create-catalog' || statement.author === undefined) continue;
        const author = statement.author;
        let start = author.span.start;
        while (start > 0 && /\s/.test(text[start - 1] ?? '')) start -= 1;
        if (text.slice(start - 2, start).toUpperCase() !== 'BY') return text;
        start -= 2;
        while (start > 0 && /\s/.test(text[start - 1] ?? '')) start -= 1;
        return text.slice(0, start) + text.slice(author.span.end);
    }
    return text;
}

// The version CREATE CATALOG is bound with: its VERSION, or the default. A
// CREATE SCHEMA with no VERSION is created at this, so resolution sees the
// catalog semver; a stated one is kept. Schemas are bound before the catalog,
// so it is taken from the parsed statement.
function schemaStampOf(script: AstScript, fallback: string): string {
    for (const statement of script.statements) {
        if (statement.kind === 'create-catalog') return statement.version ?? fallback;
    }
    return fallback;
}

function statedSchemaVersions(script: AstScript): Map<string, string> {
    const out = new Map<string, string>();
    for (const statement of script.statements) {
        if (statement.kind === 'create-schema' && statement.version !== undefined) out.set(statement.name, statement.version);
    }
    return out;
}

function checkSourceForms(script: AstScript, file: string, signer: string, keys: KeyDirectory, expect: SourceExpect): void {
    const issues: SourceIssue[] = [];
    const at = (span: TextSpan) => whereOf(file, span);
    const names = new Set<string>();
    let catalogs = 0;
    for (const statement of script.statements) {
        if (statement.kind === 'create-schema') {
            if (catalogs > 0) issues.push({ message: 'schemas come before the CREATE CATALOG', where: at(statement.span) });
            if (names.has(statement.name)) issues.push({ message: `schema ${statement.name} is defined twice`, where: at(statement.span) });
            names.add(statement.name);
        } else if (statement.kind === 'create-catalog') {
            catalogs += 1;
            if (catalogs > 1) issues.push({ message: `${file} holds one CREATE CATALOG`, where: at(statement.span) });
            if (expect.version !== undefined && statement.version !== undefined && statement.version !== expect.version) {
                issues.push({
                    message: `CREATE CATALOG: VERSION '${statement.version}' does not match the release version '${expect.version}'`,
                    where: at(statement.span),
                });
            }
            if (expect.note !== undefined && statement.note !== undefined && statement.note !== expect.note) {
                issues.push({
                    message: `NOTE '${statement.note}' does not match version.json's note '${expect.note}'`,
                    where: at(statement.span),
                });
            }
            if (statement.author !== undefined && !byMatches(statement.author, signer, keys)) {
                issues.push({
                    message: `${describeBy(statement.author)} does not match rpack.json's key '${signer}'`,
                    where: at(statement.span),
                });
            }
            for (const group of statement.groups) {
                if (group.schemaVersion !== undefined && group.schemaVersion.kind !== 'latest') {
                    issues.push({
                        message: `TABLEGROUP ${group.name}: remove ${describeAt(group.schemaVersion)}; the release pins each group's schema`,
                        where: at(group.span),
                    });
                }
            }
        } else {
            issues.push({
                message: `${statementLabel(statement)} does not belong in ${file}; it holds only CREATE SCHEMA statements and one CREATE CATALOG`,
                where: at(statement.span),
            });
        }
    }
    if (catalogs === 0 && issues.length === 0) issues.push({ message: `${file} has no CREATE CATALOG` });
    if (issues.length > 0) throw new SourceError(issues);
}

function byMatches(author: AuthorExpr, signer: string, keys: KeyDirectory): boolean {
    if (author.kind === 'nobody') return false;
    if (author.kind === 'variable') return author.name === signer;
    const matches = keys.keys().filter((key) => key.keyId.startsWith(author.prefix));
    return matches.length === 1 && matches[0]!.label === signer;
}

function describeBy(author: AuthorExpr): string {
    if (author.kind === 'nobody') return 'BY NOBODY';
    if (author.kind === 'variable') return `BY $${author.name}`;
    return `BY #${author.prefix}`;
}

function describeAt(version: VersionExpr): string {
    if (version.kind === 'latest') return 'AT LATEST';
    if (version.kind === 'hash') return `AT #${version.hash.prefix}`;
    const members = version.members.map((member) => member.kind === 'hash' ? `#${member.prefix}` : member.text);
    return `AT {${members.join(', ')}}`;
}

function statementLabel(statement: AstStatement): string {
    switch (statement.kind) {
        case 'create-database': return `CREATE DATABASE ${statement.name}`;
        case 'create-schema': return 'CREATE SCHEMA';
        case 'create-catalog': return 'CREATE CATALOG';
        case 'alter-catalog': return 'ALTER CATALOG';
        case 'update-catalog': return 'UPDATE CATALOG';
        case 'use-database': return 'USE DATABASE';
        case 'alter-schema': return 'ALTER SCHEMA';
        case 'update-ref': return 'UPDATE REF';
        case 'insert': return 'INSERT';
        case 'update': return 'UPDATE';
        case 'delete': return 'DELETE';
        case 'bundle': return 'BUNDLE';
        case 'set-view': return 'SET VIEW';
        case 'select': return 'SELECT';
        case 'log': return 'LOG';
        case 'put-file': return 'PUT';
        case 'get-file': return 'GET';
        case 'list-files': return 'LIST';
    }
}

function locatorOf(script: AstScript, file: string): Locator {
    const units = new Map<string, TextSpan>();
    for (const statement of script.statements) {
        if (statement.kind === 'create-schema') {
            units.set(unitKey.schema(statement.name), statement.span);
            for (const table of statement.tables) {
                units.set(unitKey.table(statement.name, table.name), table.span);
                for (const column of table.columns) units.set(unitKey.column(statement.name, table.name, column.name), column.span);
            }
        } else if (statement.kind === 'create-catalog') {
            units.set(unitKey.catalog(), statement.span);
            for (const group of statement.groups) units.set(unitKey.group(group.name), group.span);
            for (const files of statement.files) units.set(unitKey.files(files.name), files.span);
            for (const param of statement.params) units.set(unitKey.param(param.name), param.span);
        }
    }
    return (unit) => whereOf(file, units.get(unit));
}

// A hand-written rule, with where upgrade-manual.sql has it.
export type HandRule = { rule: MigrationRule; where: Where };

export type ReadNext = {
    // Schema name -> its rules, in file order.
    rules: Map<string, HandRule[]>;
};

// `views` gives the parents' view of each schema by name.
export function readNext(text: string, views: (schema: string) => RSchemaView | undefined, file = UPGRADE_MANUAL_FILE): ReadNext {
    const parsed = parseScript(text);
    if (!parsed.ok) throw new SourceError(issuesOf(file, parsed.diagnostics));

    const issues: SourceIssue[] = [];
    const exprs = new Map<string, MigrationRuleExpr[]>();
    for (const statement of parsed.value.statements) {
        const where = whereOf(file, statement.span);
        if (statement.kind !== 'alter-schema') {
            issues.push({ message: `${file} holds only ALTER SCHEMA statements`, where });
            continue;
        }
        if (statement.schema.kind !== 'name') { issues.push({ message: 'name the schema, not its hash', where }); continue; }
        const name = statement.schema.text;
        if (statement.version !== undefined) issues.push({ message: 'remove VERSION; the release supplies it', where });
        if (statement.author !== undefined) issues.push({ message: "remove BY; the release is signed with rpack.json's key", where });
        if (statement.at !== undefined) issues.push({ message: "remove AT; the rules apply at the release's parents", where });
        if (statement.note !== undefined) issues.push({ message: "remove NOTE; the note in version.json sets it", where });
        if (views(name) === undefined) {
            issues.push({ message: `schema ${name} is not in the release's parents; a new schema is created from ${SOURCE_FILE} alone`, where });
            continue;
        }
        exprs.set(name, [...(exprs.get(name) ?? []), ...statement.rules]);
    }
    if (issues.length > 0) throw new SourceError(issues);

    const rules = new Map<string, HandRule[]>();
    for (const [name, list] of exprs) {
        let compiled: MigrationRule[];
        try {
            compiled = compileMigrationRules(list, buildAlterColumnsOf(views(name)!, list));
        } catch (err) {
            throw new SourceError([{ message: `ALTER SCHEMA ${name}: ${messageOf(err)}`, where: whereOf(file, list[0]?.span) }]);
        }
        rules.set(name, compiled.map((rule, i) => ({ rule, where: whereOf(file, list[i]!.span)! })));
    }
    return { rules };
}

// Whether a -next file holds anything but whitespace and comments.
export function isBlankSql(text: string | undefined): boolean {
    if (text === undefined) return true;
    return text.split('\n').every((line) => {
        const trimmed = line.trim();
        return trimmed.length === 0 || trimmed.startsWith('--');
    });
}

/** C-SQL (causal SQL) command reference for \\help commands. */
import type { AstStatement } from "../syntax/ast.js";

export type LangCommandSection = 'creation' | 'catalog' | 'schema' | 'refs' | 'data' | 'query' | 'files';

export type LangCommandRef = {
    /** Leading keywords, e.g. "CREATE DATABASE" */
    command: string;
    section: LangCommandSection;
    /** BNF-ish syntax template (may span multiple lines) */
    syntax: string;
    /** Short user-facing summary of what the statement does (shown in \\help commands) */
    description: string;
    /** Links entry to AstStatement for coverage tests */
    kind: AstStatement['kind'];
};

export const LANG_COMMAND_SECTIONS: readonly LangCommandSection[] = [
    'creation',
    'catalog',
    'schema',
    'refs',
    'data',
    'query',
    'files',
];

export const LANG_COMMAND_REFS: readonly LangCommandRef[] = [
    {
        command: 'CREATE DATABASE',
        section: 'creation',
        kind: 'create-database',
        syntax: [
            "CREATE DATABASE name USING CATALOG catalogRef [AT release] [SEED '...']",
            '  [CREATORS (value, ...)] [WITH PARAMS (:name = value, ...)]',
            "  [HASH ALGORITHM '...'] [BY author];",
        ].join('\n'),
        description: "Creates a database from a catalog release and creates its table groups. release is a version string ('1.0.0'), #hash or LATEST (the default); a version string or LATEST must match exactly one release. WITH PARAMS supplies every param the release declares ($name for an identity param). CREATORS restrict who may deploy releases into the database (UPDATE CATALOG) and, unless the catalog says otherwise (ALLOW DEPLOY IF), who may deploy schema versions to its groups. The database becomes the current one.",
    },
    {
        command: 'USE DATABASE',
        section: 'creation',
        kind: 'use-database',
        syntax: 'USE DATABASE databaseRef;',
        description: 'Sets the current database: bare group names (group.table) resolve within it.',
    },
    {
        command: 'CREATE CATALOG',
        section: 'catalog',
        kind: 'create-catalog',
        syntax: [
            "CREATE CATALOG name [CREATORS (value, ...)] VERSION '<semver>' [PARAMS (:name type, ...)] [SEED '...'] AS (",
            '  TABLEGROUP name USING SCHEMA schemaRef [AT version]',
            '    [BIND binding => groupName [, binding => groupName]*]',
            '    [USING IDENTITIES providerRef]',
            '    [ALLOW DEPLOY IF predicate]',
            '    [ALLOW UPDATE REF binding IF predicate]*',
            '    [WITH ROWS (table (col = value | :param | publicKey(:param), ...), ...)],',
            '  FILES name [BIND alias => groupName]',
            '    USING IDENTITIES alias.table',
            '    ALLOW WRITE IF predicate,',
            '  ...',
            ") [NOTE '...'] [BY author];",
        ].join('\n'),
        description: "Creates a catalog: a signed DAG of releases describing a database's table groups and file stores. The genesis is the first release. CREATORS (default: the author) are the only keys that may sign releases. BIND names another table group of the catalog. :param values in WITH ROWS are supplied when the release is deployed. Without ALLOW DEPLOY IF, a group's schema deploys are restricted to the database creators. FILES defines a file store (a blob store and a file map per database) bound to one table group: its writers are the keys of the identity table that pass ALLOW WRITE IF, whose tables are all alias.<table>. Without BIND the alias is the group name; BIND is needed only when that name is ambiguous (BIND u => #hash). FILES definitions are immutable: publish a new name to change one.",
    },
    {
        command: 'ALTER CATALOG',
        section: 'catalog',
        kind: 'alter-catalog',
        syntax: [
            "ALTER CATALOG catalogRef VERSION '<semver>' [PARAMS (:name type, ...)] [AS (",
            '  UPDATE SCHEMA schemaRef TO version ON groupName,',
            '  ADD TABLEGROUP name USING SCHEMA schemaRef ... (as in CREATE CATALOG),',
            '  ADD FILES name ... (as in CREATE CATALOG),',
            '  ...',
            ")] [NOTE '...'] [BY author] [AT {#release, ...} | AT LATEST];",
        ].join('\n'),
        description: "Publishes a release: the version changes, new table groups and new FILES relative to its parents, the latest releases at AT (default LATEST; hashes only, since releases can share a version). With several parents the release is a merge, and must UPDATE SCHEMA every group they disagree on. The version must be greater than every parent's. Nothing changes in a database until the release is deployed with UPDATE CATALOG.",
    },
    {
        command: 'UPDATE CATALOG',
        section: 'catalog',
        kind: 'update-catalog',
        syntax: "UPDATE CATALOG catalogRef TO release ON databaseRef [WITH PARAMS (:name = value, ...)] [NOTE '...'] [BY author];",
        description: "Deploys another catalog release into a database: creates its new table groups, deploys the new schema versions (bound groups first, advancing their dependents' refs), then records the release. The release must not be at or below one already deployed; a release concurrent with a deployed one merges with it (both stay deployed, each group at the union of its versions). release is a version string, #hash or LATEST. WITH PARAMS supplies the params the release adds. Other replicas adopt the release when it is in their adoption range (see \\adopt).",
    },
    {
        command: 'CREATE SCHEMA',
        section: 'creation',
        kind: 'create-schema',
        syntax: [
            "CREATE SCHEMA name [CREATORS (value, ...)] [VERSION '<semver>'] [HASH ALGORITHM '...'] AS (",
            '  TABLE tableName (',
            '    column type [MIN v] [MAX v] [NULL] [DEFAULT value] [PUB] [READONLY] [REFERENCES refTable], ...',
            '  ) [CONCURRENT DELETES [true|false]] [IDENTITY PROVIDER [(keyIdCol, publicKeyCol)]]',
            '    [ALLOW op IF predicate], ...',
            ');',
        ].join('\n'),
        description: "Defines a schema: tables, columns, allow rules, and an optional identity provider. VERSION is the schema's first version (default 0.0.1); later ALTER SCHEMA versions must increase. Column type is one of string[(n)], integer, float, boolean, json, bigint, decimal(p, s), bytes[(n)], identity (n = maxLength; p, s = precision, scale, with p = * for no precision limit; identity stores a key-hash string). A creator is $name, #keyIdPrefix, a key-id string, or publicKey('<base64>'). Names that collide with keywords are double-quoted: \"identity\". MIN/MAX give inclusive bounds (integer/bigint/decimal only); write bigint/decimal literals as quoted strings so they stay exact, and json values as JSON '<json text>'. Values are rejected, never rounded. Mark FK columns with REFERENCES refTable (or REFERENCES binding.table for a bound group); insert their values as #rowIdPrefix. For identity columns, insert $name or #keyIdPrefix.",
    },
    {
        command: 'ALTER SCHEMA',
        section: 'schema',
        kind: 'alter-schema',
        syntax: [
            "ALTER SCHEMA schemaRef [VERSION '<semver>'] AS (",
            '  ADD TABLE tableName (',
            '    column type [MIN v] [MAX v] [NULL] [DEFAULT value] [PUB] [READONLY] [REFERENCES refTable], ...',
            '  ) [CONCURRENT DELETES [true|false]] [IDENTITY PROVIDER [(keyIdCol, publicKeyCol)]]',
            '    [ALLOW op IF predicate], ...],',
            '  ADD COLUMN table.column type [MIN v] [MAX v] [NULL] [DEFAULT value] [PUB] [READONLY],',
            '  DROP TABLE tableName,',
            '  DROP COLUMN table.column,',
            '  SET CONCURRENT DELETES table true|false,',
            '  SET FKS table (col REFERENCES refTable, ...),',
            '  SET ALLOW RULES table (ALLOW op IF predicate, ...)',
            ") [NOTE '...'] [AT version] [BY author];",
        ].join('\n'),
        description: "Migrates a schema with add/drop table or column, FK, allow-rule, and concurrent-delete changes. VERSION is the update's schema version: it must be above every version at AT, and defaults to the next patch. Where concurrent updates write the same slot, the higher version wins. SET FKS table (col REFERENCES refTable, ...) sets a table's foreign keys. Requires an author.",
    },
    {
        command: 'UPDATE REF',
        section: 'refs',
        kind: 'update-ref',
        syntax: 'UPDATE REF binding TO version ON [db.]group [AT version] [BY author];',
        description: 'Advances the observed version of a bound group on a table group. Gated by ALLOW UPDATE REF IF when present.',
    },
    {
        command: 'INSERT',
        section: 'data',
        kind: 'insert',
        syntax: 'INSERT INTO [[db.]group.]table (col, ...) VALUES (value, ...) [BY author] [AT version];',
        description: 'Inserts a row into a table. Supply uuid for deterministic row identity. For REFERENCES columns, pass the FK value as a #rowIdPrefix of the target row. Write json values as JSON \'<json text>\' (no null inside).',
    },
    {
        command: 'UPDATE',
        section: 'data',
        kind: 'update',
        syntax: 'UPDATE [[db.]group.]table SET col = value [, ...] WHERE rowId = #prefix [BY author] [AT version];',
        description: 'Updates columns on an existing row, identified by rowId hash prefix.',
    },
    {
        command: 'DELETE',
        section: 'data',
        kind: 'delete',
        syntax: 'DELETE FROM [[db.]group.]table WHERE rowId = #prefix [BY author] [AT version];',
        description: 'Deletes a row by rowId hash prefix.',
    },
    {
        command: 'BUNDLE',
        section: 'data',
        kind: 'bundle',
        syntax: [
            'BUNDLE ON [db.]group (',
            '  INSERT INTO table (...) VALUES (...);',
            '  UPDATE table SET ... WHERE rowId = #prefix;',
            '  DELETE FROM table WHERE rowId = #prefix;',
            ') [BY author] [AT version];',
        ].join('\n'),
        description: 'Runs multiple writes as one signed operation on a table group. Put BY on the BUNDLE, not on inner writes.',
    },
    {
        command: 'SELECT',
        section: 'query',
        kind: 'select',
        syntax: [
            'SELECT * | col [, ...] FROM [[db.]group.]table',
            '  [WHERE predicate]',
            '  [ORDER BY col [ASC|DESC] [, ...]]',
            '  [LIMIT n] [OFFSET n]',
            '  [AT version] [FROM version];',
        ].join('\n'),
        description: 'Queries rows from a table at an optional view frontier. Read-only.',
    },
    {
        command: 'SET VIEW',
        section: 'query',
        kind: 'set-view',
        syntax: 'SET VIEW AT version [FROM version];',
        description: 'Sets the session default view frontier used when statements omit an AT clause.',
    },
    {
        command: 'LOG',
        section: 'query',
        kind: 'log',
        syntax: '[EXPLAIN] LOG targetRef [AT version] [FROM version] [LIMIT n] [OFFSET n];',
        description: 'Shows paginated operation history for a schema, catalog, table group, table or database. Group and table logs include status (OK/Cancelled) for void-checkable ops and a truncated reverse-render op preview. EXPLAIN adds a reason column (populated for Cancelled ops only). JSON output carries raw payload rows only. Read-only.',
    },
    {
        command: 'PUT',
        section: 'files',
        kind: 'put-file',
        syntax: [
            "PUT FILE 'local/path' INTO [db.]files [AT 'path'] [IN KEY | IN COMMON] [BY author];",
            "PUT STRING 'text' INTO [db.]files AT 'path' [IN KEY | IN COMMON] [BY author];",
            "PUT B64 'base64' INTO [db.]files AT 'path' [IN KEY | IN COMMON] [BY author];",
        ].join('\n'),
        description: "Adds a file to a FILES member: a local file (read by the host), a string's UTF-8 bytes, or base64. A string gets no trailing newline unless it ends in \\n ('hello\\n'); \\r, \\t and \\\\ also work. AT defaults to the local file's name. The file goes in the common section, or with IN KEY in the author's own section. Every op is signed, so PUT needs an author (not NOBODY) that ALLOW WRITE IF admits. The bytes are uploaded unless the store has them already, and the file replaces whatever is at that path in that section.",
    },
    {
        command: 'GET',
        section: 'files',
        kind: 'get-file',
        syntax: "GET 'path' FROM [db.]files [IN KEY [author] | IN COMMON] [HASH 'prefix'] [AS B64] [TO 'local/path'];",
        description: "Reads a file of a FILES member: into a local file with TO (written by the host), or inline as UTF-8 text, or base64 with AS B64 (up to 1 MiB). IN KEY reads an owner's section ($name, #prefix, or the current author). When several versions share the path, HASH picks one. The file must be complete on this replica. Read-only.",
    },
    {
        command: 'LIST',
        section: 'files',
        kind: 'list-files',
        syntax: "LIST ['path/prefix'] FROM [db.]files [IN COMMON | IN KEY [author]];",
        description: 'Lists the files of a FILES member: section, owner key, path, size, file hash, and whether the bytes are complete on this replica. A prefix matches whole path segments. Read-only.',
    },
];

export function findLangCommandRefs(query?: string): LangCommandRef[] {
    if (query === undefined || query === '') return [...LANG_COMMAND_REFS];
    const prefix = query.toUpperCase();
    return LANG_COMMAND_REFS.filter((ref) => ref.command.toUpperCase().startsWith(prefix));
}

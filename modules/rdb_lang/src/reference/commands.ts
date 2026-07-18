/** C-SQL (causal SQL) command reference for \\help commands. */
import type { AstStatement } from "../syntax/ast.js";

export type LangCommandSection = 'creation' | 'schema' | 'refs' | 'data' | 'query';

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
    'schema',
    'refs',
    'data',
    'query',
];

export const LANG_COMMAND_REFS: readonly LangCommandRef[] = [
    {
        command: 'CREATE DATABASE',
        section: 'creation',
        kind: 'create-database',
        syntax: "CREATE DATABASE name [SEED '...'] [CREATORS (value, ...)];",
        description: 'Creates a database. Optional SEED and CREATORS pin identity and restrict who may add schemas and table groups.',
    },
    {
        command: 'CREATE SCHEMA',
        section: 'creation',
        kind: 'create-schema',
        syntax: [
            'CREATE SCHEMA name [CREATORS (value, ...)] AS (',
            '  TABLE tableName (',
            '    column type [MIN v] [MAX v] [NULL] [DEFAULT value] [PUB] [READONLY] [REFERENCES refTable], ...',
            '  ) [CONCURRENT DELETES [true|false]] [IDENTITY PROVIDER [(keyIdCol, publicKeyCol)]]',
            '    [ALLOW op IF predicate], ...',
            ');',
        ].join('\n'),
        description: "Defines a schema: tables, columns, allow rules, and an optional identity provider. Column type is one of string[(n)], integer, float, boolean, json, bigint, decimal(p, s), bytes[(n)] (n = maxLength; p, s = precision, scale). MIN/MAX give inclusive bounds (integer/bigint/decimal only); write bigint/decimal literals as quoted strings so they stay exact. Values are rejected, never rounded. Mark FK columns with REFERENCES refTable (or REFERENCES binding.table for a bound group); insert their values as #rowIdPrefix.",
    },
    {
        command: 'CREATE TABLEGROUP',
        section: 'creation',
        kind: 'create-tablegroup',
        syntax: [
            "CREATE TABLEGROUP name [SEED '...']",
            '  USING SCHEMA schemaRef [AT version]',
            '  [BIND binding => groupRef]*',
            '  [USING IDENTITIES providerRef]',
            '  [ALLOW UPDATE SCHEMA IF predicate]',
            '  [ALLOW UPDATE REF binding IF predicate]*',
            '  [WITH ROWS (table (col = value, ...), ...)]',
            '  [BY author] [AT version];',
        ].join('\n'),
        description: 'Creates a table group from a schema, optional bindings, identity provider, deploy/ref gates, and genesis rows.',
    },
    {
        command: 'ADD SCHEMA',
        section: 'creation',
        kind: 'add-member',
        syntax: "ADD SCHEMA schemaRef TO databaseRef [NOTE '...'] [AT version] [BY author];",
        description: 'Adds a schema to a database (advisory membership).',
    },
    {
        command: 'ADD TABLEGROUP',
        section: 'creation',
        kind: 'add-member',
        syntax: "ADD TABLEGROUP groupRef TO databaseRef [NOTE '...'] [AT version] [BY author];",
        description: 'Adds a table group to a database (advisory membership).',
    },
    {
        command: 'ALTER SCHEMA',
        section: 'schema',
        kind: 'alter-schema',
        syntax: [
            'ALTER SCHEMA schemaRef AS (',
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
            ') [AT version] [BY author];',
        ].join('\n'),
        description: 'Migrates a schema with add/drop table or column, FK, allow-rule, and concurrent-delete changes. SET FKS table (col REFERENCES refTable, ...) sets a table\'s foreign keys. Requires an author.',
    },
    {
        command: 'UPDATE SCHEMA',
        section: 'refs',
        kind: 'update-schema',
        syntax: 'UPDATE SCHEMA schemaRef TO version ON groupRef [AT version] [BY author];',
        description: 'Deploys a schema version on a table group. Gated by ALLOW UPDATE SCHEMA IF when present.',
    },
    {
        command: 'UPDATE REF',
        section: 'refs',
        kind: 'update-ref',
        syntax: 'UPDATE REF binding TO version ON groupRef [AT version] [BY author];',
        description: 'Advances the observed version of a bound group on a table group. Gated by ALLOW UPDATE REF IF when present.',
    },
    {
        command: 'INSERT',
        section: 'data',
        kind: 'insert',
        syntax: 'INSERT INTO [group.]table (col, ...) VALUES (value, ...) [BY author] [AT version];',
        description: 'Inserts a row into a table. Supply uuid for deterministic row identity. For REFERENCES columns, pass the FK value as a #rowIdPrefix of the target row.',
    },
    {
        command: 'UPDATE',
        section: 'data',
        kind: 'update',
        syntax: 'UPDATE [group.]table SET col = value [, ...] WHERE rowId = #prefix [BY author] [AT version];',
        description: 'Updates columns on an existing row, identified by rowId hash prefix.',
    },
    {
        command: 'DELETE',
        section: 'data',
        kind: 'delete',
        syntax: 'DELETE FROM [group.]table WHERE rowId = #prefix [BY author] [AT version];',
        description: 'Deletes a row by rowId hash prefix.',
    },
    {
        command: 'BUNDLE',
        section: 'data',
        kind: 'bundle',
        syntax: [
            'BUNDLE ON groupRef (',
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
            'SELECT * | col [, ...] FROM [group.]table',
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
        description: 'Shows paginated operation history for a schema, table group, or database. Group and table logs include status (OK/Cancelled) for void-checkable ops and a truncated reverse-render op preview. EXPLAIN adds a reason column (populated for Cancelled ops only). JSON output carries raw payload rows only. Read-only.',
    },
];

export function findLangCommandRefs(query?: string): LangCommandRef[] {
    if (query === undefined || query === '') return [...LANG_COMMAND_REFS];
    const prefix = query.toUpperCase();
    return LANG_COMMAND_REFS.filter((ref) => ref.command.toUpperCase().startsWith(prefix));
}

export type LangCommonRef = {
    command: 'COMMON';
    /** Multi-line summary of shared clauses and reference forms */
    syntax: string;
    description: string;
};

export const LANG_COMMON_REF: LangCommonRef = {
    command: 'COMMON',
    syntax: [
        'BY author          $name or #prefix signs the op; NOBODY forces unauthored; omitted uses session default',
        'AT version         LATEST, #prefix, {#a, #b}, or version alias; causal placement on the target DAG',
        'FROM version       range lower bound on SELECT / SET VIEW',
        'nameRef            workspace name or #idPrefix for schemas, catalogs, databases, groups, tables',
        '[[db.]group.]table table reference; a bare group resolves in the current database (USE DATABASE)',
        'rowId = #prefix    row target for UPDATE / DELETE',
        '$var               identity or session variable ($author, $me, …); host-resolved',
        ':param             catalog param in WITH ROWS; supplied by WITH PARAMS when a release is deployed',
        '#prefix            hash-prefix literal for ids, versions, key ids',
        'uuid               reserved INSERT pseudo-column (not a schema column)',
        "SEED '...'          deterministic object identity on CREATE DATABASE / CREATE CATALOG",
        'CREATORS (...)     signing keys of a schema or catalog; deploy authority of a database',
    ].join('\n'),
    description: 'Shared trailing clauses and reference forms used across C-SQL statements.',
};

export function isLangCommonHelpQuery(query?: string): boolean {
    return query !== undefined && query.toLowerCase() === 'common';
}

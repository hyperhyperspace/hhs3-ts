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
        'nameRef            workspace name or #idPrefix for schemas, groups, databases, tables',
        '[group.]table      qualified or unqualified table (default group from session)',
        'rowId = #prefix    row target for UPDATE / DELETE',
        '$var               identity or session variable ($author, $me, …); host-resolved',
        '#prefix            hash-prefix literal for ids, versions, key ids',
        'uuid               reserved INSERT / WITH ROWS pseudo-column (not a schema column)',
        "SEED '...'          deterministic object identity on CREATE DATABASE / CREATE TABLEGROUP",
        'CREATORS (...)     restrict who may ADD members; $name or #prefix',
    ].join('\n'),
    description: 'Shared trailing clauses and reference forms used across C-SQL statements.',
};

export function isLangCommonHelpQuery(query?: string): boolean {
    return query !== undefined && query.toLowerCase() === 'common';
}

import type { TextSpan } from "../diagnostics.js";

export type TokenKind =
    | 'identifier'
    | 'keyword'
    | 'variable'
    | 'hash'
    | 'string'
    | 'number'
    | 'operator'
    | 'punctuation'
    | 'eof';

export type Token = {
    kind: TokenKind;
    text: string;
    upper: string;
    span: TextSpan;
    value?: string | number | boolean | null;
};

export const KEYWORDS = new Set([
    'ADD',
    'ALL',
    'ALLOW',
    'ALTER',
    'AND',
    'AS',
    'ASC',
    'AT',
    'AUTO',
    'BIND',
    'BOOLEAN',
    'BUNDLE',
    'BY',
    'COLUMN',
    'CONCURRENT',
    'CREATE',
    'CREATORS',
    'DATABASE',
    'DEFAULT',
    'DELETE',
    'DELETES',
    'DESC',
    'DROP',
    'EXISTS',
    'EXPLAIN',
    'FALSE',
    'FLOAT',
    'FROM',
    'FKS',
    'IDENTITY',
    'IF',
    'INSERT',
    'INTEGER',
    'IS',
    'INTO',
    'JSON',
    'LATEST',
    'LIKE',
    'LIMIT',
    'LOG',
    'NO',
    'NOBODY',
    'NOT',
    'NOTE',
    'NULL',
    'OFFSET',
    'ON',
    'OR',
    'ORDER',
    'PROVIDER',
    'PUB',
    'READONLY',
    'REFERENCES',
    'REF',
    'RULES',
    'ROWS',
    'SCHEMA',
    'SEED',
    'SELECT',
    'SET',
    'STRING',
    'TABLE',
    'TABLEGROUP',
    'TO',
    'TRUE',
    'UPDATE',
    'USING',
    'VALUES',
    'VIEW',
    'WHERE',
    'WITH',
]);

export const PHASE2_KEYWORDS = new Set([
    'ALTER',
    'BUNDLE',
    'DELETE',
    'UPDATE',
]);

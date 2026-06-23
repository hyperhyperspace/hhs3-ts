import { assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { scanStatement, splitStatements } from "../src/syntax/scanner.js";

export const scannerTests = {
    title: '[RDB_LANG:SCAN] Statement scanner',
    tests: [
        {
            name: '[SCAN01] complete simple statement',
            invoke: async () => {
                assertEquals(scanStatement('CREATE DATABASE db;').kind, 'complete');
                assertEquals(scanStatement('SELECT * FROM t.products;').kind, 'complete');
                assertEquals(scanStatement('LOG shop;').kind, 'complete');
            },
        },
        {
            name: '[SCAN02] no-terminator without trailing semicolon',
            invoke: async () => {
                assertEquals(scanStatement('CREATE TABLEGROUP users USING SCHEMA s').kind, 'no-terminator');
                assertEquals(scanStatement('LOG shop LIMIT 10').kind, 'no-terminator');
                assertEquals(scanStatement('').kind, 'no-terminator');
            },
        },
        {
            name: '[SCAN03] trailing comment after semicolon is complete',
            invoke: async () => {
                // The main bug fixed: endsWith(";") fails on these
                assertEquals(scanStatement('LOG shop; -- done').kind, 'complete');
                assertEquals(scanStatement('SELECT * FROM t.x;\n-- next block').kind, 'complete');
                assertEquals(scanStatement('CREATE DATABASE db; /* comment */').kind, 'complete');
            },
        },
        {
            name: '[SCAN04] unclosed string is incomplete-string',
            invoke: async () => {
                assertEquals(scanStatement("INSERT INTO t (name) VALUES ('hello").kind, 'incomplete-string');
                // Escaped quote does not close the string
                assertEquals(scanStatement("LOG 'it''s").kind, 'incomplete-string');
            },
        },
        {
            name: '[SCAN05] closed string with semicolon in it is complete',
            invoke: async () => {
                // Semicolon inside string must not be treated as terminator
                const s = "INSERT INTO t (name) VALUES ('foo;bar');";
                assertEquals(scanStatement(s).kind, 'complete');
            },
        },
        {
            name: '[SCAN06] unclosed block comment is incomplete-comment',
            invoke: async () => {
                assertEquals(scanStatement('SELECT * FROM t /* started').kind, 'incomplete-comment');
            },
        },
        {
            name: '[SCAN07] unclosed bracket is incomplete-bracket',
            invoke: async () => {
                const r = scanStatement('CREATE SCHEMA s AS (\n  TABLE t (v string)');
                assertEquals(r.kind, 'incomplete-bracket');
                // Outer AS ( is still open (depth 1); the TABLE (...) paren closed itself
                if (r.kind === 'incomplete-bracket') assertEquals(r.depth, 1);
            },
        },
        {
            name: '[SCAN08] BUNDLE with inner semicolons waits for closing paren',
            invoke: async () => {
                // Inner semicolons must not trigger early completion
                const line1 = "BUNDLE ON shop (";
                const line2 = "BUNDLE ON shop (\n  UPDATE products SET name = 'x' WHERE rowId = #abc;";
                const line3 = "BUNDLE ON shop (\n  UPDATE products SET name = 'x' WHERE rowId = #abc;\n  INSERT INTO products (sku) VALUES ('B');";
                const complete = "BUNDLE ON shop (\n  UPDATE products SET name = 'x' WHERE rowId = #abc;\n  INSERT INTO products (sku) VALUES ('B');\n);";

                assertEquals(scanStatement(line1).kind, 'incomplete-bracket', 'after open paren');
                assertEquals(scanStatement(line2).kind, 'incomplete-bracket', 'after first inner semi');
                assertEquals(scanStatement(line3).kind, 'incomplete-bracket', 'after second inner semi');
                assertEquals(scanStatement(complete).kind, 'complete', 'after closing );');
            },
        },
        {
            name: '[SCAN09] multiline CREATE SCHEMA is incomplete until closing );',
            invoke: async () => {
                const partial = `CREATE SCHEMA s CREATORS ($admin) AS (
  TABLE identities (
    keyId string PUB READONLY,
    publicKey string PUB READONLY
  ) IDENTITY PROVIDER ALLOW insert IF true`;
                assertEquals(scanStatement(partial).kind, 'incomplete-bracket');

                const complete = partial + '\n);';
                assertEquals(scanStatement(complete).kind, 'complete');
            },
        },
        {
            name: '[SCAN10] splitStatements basic',
            invoke: async () => {
                const stmts = splitStatements('SELECT * FROM a.t;\nSELECT * FROM b.t;');
                assertEquals(stmts.length, 2, 'two statements');
                assertEquals(stmts[0], 'SELECT * FROM a.t;');
                assertEquals(stmts[1], 'SELECT * FROM b.t;');
            },
        },
        {
            name: '[SCAN11] splitStatements with comments and blanks',
            invoke: async () => {
                const text = `
-- create tables
CREATE DATABASE mydb;

-- now select
SELECT * FROM mydb.t; -- done
`;
                const stmts = splitStatements(text);
                assertEquals(stmts.length, 2, 'two statements');
                assertEquals(stmts[0].includes('CREATE DATABASE'), true, 'first stmt');
                assertEquals(stmts[1].includes('SELECT'), true, 'second stmt');
            },
        },
        {
            name: '[SCAN12] splitStatements preserves BUNDLE as single statement',
            invoke: async () => {
                const text = `BUNDLE ON shop (
  UPDATE products SET name = 'x' WHERE rowId = #abc;
  INSERT INTO products (sku) VALUES ('B');
);
SELECT * FROM shop.products;`;
                const stmts = splitStatements(text);
                assertEquals(stmts.length, 2, 'two statements: bundle + select');
                assertEquals(stmts[0].startsWith('BUNDLE'), true, 'first is bundle');
                assertEquals(stmts[1].startsWith('SELECT'), true, 'second is select');
            },
        },
        {
            name: '[SCAN13] splitStatements returns incomplete tail',
            invoke: async () => {
                const stmts = splitStatements('SELECT * FROM t;\nCREATE DATABASE');
                assertEquals(stmts.length, 2, 'one complete + one incomplete tail');
                assertEquals(stmts[1], 'CREATE DATABASE', 'tail returned as-is');
            },
        },
    ],
};

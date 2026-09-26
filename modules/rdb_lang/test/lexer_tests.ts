import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { lex } from "../src/syntax/lexer.js";

export const lexerTests = {
    title: '[RDB_LANG:LEX] Lexer',
    tests: [
        {
            name: '[LEX01] tokenizes phase 1 names, hashes, variables, strings and separators',
            invoke: async () => {
                const result = lex("SELECT name FROM shop.products WHERE sku = 'A''1' AT {#abc, #def};");
                assertTrue(result.ok, 'lexing should succeed');
                if (!result.ok) return;
                const texts = result.value.map((t) => t.text).filter((t) => t !== '');
                assertEquals(texts.join('|'), "SELECT|name|FROM|shop.products|WHERE|sku|=|'A''1'|AT|{|#abc|,|#def|}|;", 'token text sequence');
                const stringToken = result.value.find((t) => t.kind === 'string');
                assertEquals(stringToken?.value, "A'1", 'SQL string escaping');
            },
        },
        {
            name: '[LEX02] tokenizes qualified column identifiers in EXISTS WHERE',
            invoke: async () => {
                const result = lex('EXISTS users.identities WHERE users.identities.keyId = profiles.keyId');
                assertTrue(result.ok, 'lexing should succeed');
                if (!result.ok) return;
                const identifiers = result.value.filter((t) => t.kind === 'identifier').map((t) => t.text);
                assertTrue(identifiers.includes('users.identities.keyId'), 'qualified exists column token');
                assertTrue(identifiers.includes('profiles.keyId'), 'qualified gated column token');
            },
        },
        {
            name: '[LEX03] tokenizes arithmetic operators, unsigned numbers with exponents, and -- comments',
            invoke: async () => {
                const result = lex('a+1*b-2.5e-3 >= -1E+2 -- trailing\n<=');
                assertTrue(result.ok, 'lexing should succeed');
                if (!result.ok) return;
                const tokens = result.value.filter((t) => t.text !== '');
                assertEquals(tokens.map((t) => t.text).join('|'), 'a|+|1|*|b|-|2.5e-3|>=|-|1E+2|<=', 'token text sequence');
                const numbers = tokens.filter((t) => t.kind === 'number').map((t) => t.value);
                assertEquals(JSON.stringify(numbers), JSON.stringify([1, 0.0025, 100]), 'number values are unsigned');
            },
        },
        {
            name: '[LEX04] double-quoted identifier parts escape keywords',
            invoke: async () => {
                const result = lex('"identity" users."identity"."table" "a""b" "NOT" NOT');
                assertTrue(result.ok, 'lexing should succeed');
                if (!result.ok) return;
                const tokens = result.value.filter((t) => t.kind !== 'eof');
                assertEquals(tokens.map((t) => `${t.kind}:${t.text}`).join('|'),
                    'identifier:identity|identifier:users.identity.table|identifier:a"b|identifier:NOT|keyword:NOT',
                    'quoted parts lex to unquoted identifier text');
                assertTrue(tokens.slice(0, 4).every((t) => t.quoted === true), 'quoted tokens are flagged');
                assertTrue(tokens[4].quoted === undefined, 'bare keyword is not flagged');

                for (const bad of ['"unterminated', '""', '"a.b"', '"s:t"']) {
                    assertTrue(!lex(bad).ok, `${bad} should not lex`);
                }
            },
        },
    ],
};

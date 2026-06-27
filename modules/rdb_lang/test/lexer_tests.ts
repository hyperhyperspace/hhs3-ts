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
    ],
};

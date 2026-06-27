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
            name: '[LEX02] tokenizes dotted variable member access',
            invoke: async () => {
                const result = lex('EXISTS users.identities WHERE keyId = $row.keyId');
                assertTrue(result.ok, 'lexing should succeed');
                if (!result.ok) return;
                const variable = result.value.find((t) => t.kind === 'variable');
                assertEquals(variable?.text, '$row.keyId', 'dotted variable token');
            },
        },
    ],
};

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import type { AstStatement } from "../src/syntax/ast.js";
import { findLangCommandRefs, LANG_COMMAND_REFS } from "../src/reference/commands.js";
import { isLangCommonHelpQuery, LANG_COMMON_REF } from "../src/reference/common.js";

const ALL_KINDS: AstStatement['kind'][] = [
    'create-database',
    'create-schema',
    'create-tablegroup',
    'add-member',
    'alter-schema',
    'update-schema',
    'update-ref',
    'insert',
    'update',
    'delete',
    'bundle',
    'set-view',
    'select',
    'log',
];

export const referenceTests = {
    title: '[RDB_LANG:REF] Command reference',
    tests: [
        {
            name: '[REF01] LANG_COMMAND_REFS has 15 entries',
            invoke: async () => {
                assertEquals(LANG_COMMAND_REFS.length, 15, 'entry count');
            },
        },
        {
            name: '[REF02] every AstStatement kind appears at least once',
            invoke: async () => {
                const covered = new Set(LANG_COMMAND_REFS.map((ref) => ref.kind));
                for (const kind of ALL_KINDS) {
                    assertTrue(covered.has(kind), `missing kind ${kind}`);
                }
            },
        },
        {
            name: '[REF03] ADD SCHEMA and ADD TABLEGROUP are distinct entries',
            invoke: async () => {
                const commands = LANG_COMMAND_REFS.map((ref) => ref.command);
                assertTrue(commands.includes('ADD SCHEMA'), 'ADD SCHEMA present');
                assertTrue(commands.includes('ADD TABLEGROUP'), 'ADD TABLEGROUP present');
                const pairs = LANG_COMMAND_REFS.map((ref) => `${ref.command}:${ref.kind}`);
                assertEquals(new Set(pairs).size, pairs.length, 'no duplicate command entries');
            },
        },
        {
            name: '[REF04] findLangCommandRefs filters by prefix',
            invoke: async () => {
                const create = findLangCommandRefs('CREATE');
                assertEquals(create.length, 3, 'CREATE prefix count');
                assertTrue(create.every((ref) => ref.command.startsWith('CREATE')), 'CREATE prefix match');
                assertEquals(findLangCommandRefs('NOPE').length, 0, 'unknown prefix');
                assertEquals(findLangCommandRefs().length, 15, 'no filter returns all');
            },
        },
        {
            name: '[REF05] LANG_COMMON_REF documents shared clauses',
            invoke: async () => {
                assertEquals(LANG_COMMON_REF.command, 'COMMON', 'common command label');
                assertTrue(LANG_COMMON_REF.syntax.includes('BY author'), 'BY clause');
                assertTrue(LANG_COMMON_REF.syntax.includes('AT version'), 'AT clause');
                assertTrue(LANG_COMMON_REF.syntax.includes('rowId = #prefix'), 'rowId clause');
            },
        },
        {
            name: '[REF06] isLangCommonHelpQuery recognizes common filter',
            invoke: async () => {
                assertTrue(isLangCommonHelpQuery('common'), 'common lowercase');
                assertTrue(isLangCommonHelpQuery('COMMON'), 'common uppercase');
                assertTrue(!isLangCommonHelpQuery('CREATE'), 'command prefix is not common');
                assertTrue(!isLangCommonHelpQuery(undefined), 'undefined is not common');
            },
        },
    ],
};

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { lowerRestrictionPredicate } from "../src/compile/query.js";
import { columnSetFromTableDecl, columnsOfFromTableDecls } from "../src/compile/rule_scope.js";
import { parseStatement } from "../src/syntax/parser.js";

export const parserTests = {
    title: '[RDB_LANG:PARSE] Parser',
    tests: [
        {
            name: '[PARSE01] parses phase 1 CREATE SCHEMA',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA shop CREATORS ($admin) AS (
                      TABLE products (
                        sku string PUB READONLY,
                        name string,
                        price integer DEFAULT 0
                      ) ALLOW all IF true
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok) return;
                assertEquals(result.value.kind, 'create-schema', 'statement kind');
                if (result.value.kind !== 'create-schema') return;
                assertEquals(result.value.tables.length, 1, 'one table');
                assertEquals(result.value.tables[0].columns.length, 3, 'three columns');
            },
        },
        {
            name: '[PARSE01b] parses colon-qualified schema names',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA hhs:users AS (
                      TABLE identities (
                        keyId string
                      )
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-schema') return;
                assertEquals(result.value.name, 'hhs:users', 'schema name should preserve colon hierarchy');
            },
        },
        {
            name: '[PARSE02] parses update/delete/bundle statements',
            invoke: async () => {
                const result = parseStatement("UPDATE shop.products SET name = 'x' WHERE rowId = 'row-1';");
                assertTrue(result.ok, 'UPDATE should parse');
                if (!result.ok) return;
                assertEquals(result.value.kind, 'update', 'statement kind');
            },
        },
        {
            name: '[PARSE03] parses phase 1 EXISTS allow predicates',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA shop AS (
                      TABLE orders (
                        buyer string
                      ) ALLOW insert IF EXISTS users.caps WHERE label = 'buyer' AND grantee = $author
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok) return;
                assertEquals(result.value.kind, 'create-schema', 'statement kind');
                if (result.value.kind !== 'create-schema') return;
                assertEquals(result.value.tables[0].options[0].kind, 'allow-rule', 'allow rule option');
            },
        },
        {
            name: '[PARSE04] parses SET VIEW',
            invoke: async () => {
                const result = parseStatement('SET VIEW AT {#abc, #def} FROM #abc;');
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok) return;
                assertEquals(result.value.kind, 'set-view', 'statement kind');
                if (result.value.kind !== 'set-view') return;
                assertEquals(result.value.at.kind, 'set', 'AT version');
                assertEquals(result.value.from?.kind, 'hash', 'FROM version');
            },
        },
        {
            name: '[PARSE04b] parses version set with bare name aliases',
            invoke: async () => {
                const result = parseStatement('SET VIEW AT {cut, #abc};');
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok) return;
                assertEquals(result.value.kind, 'set-view', 'statement kind');
                if (result.value.kind !== 'set-view') return;
                assertEquals(result.value.at.kind, 'set', 'AT version set');
                if (result.value.at.kind !== 'set') return;
                assertEquals(result.value.at.members.length, 2, 'two version members');
                assertEquals(result.value.at.members[0].kind, 'name', 'first member is bare name');
                if (result.value.at.members[0].kind === 'name') assertEquals(result.value.at.members[0].text, 'cut', 'cut alias name');
                assertEquals(result.value.at.members[1].kind, 'hash', 'second member is hash');
            },
        },
        {
            name: '[PARSE05] parses unqualified table references',
            invoke: async () => {
                const select = parseStatement('SELECT * FROM products;');
                assertTrue(select.ok, 'SELECT should parse');
                if (select.ok && select.value.kind === 'select') assertEquals(select.value.table.group, undefined, 'SELECT table is unqualified');

                const insert = parseStatement("INSERT INTO products (sku) VALUES ('A');");
                assertTrue(insert.ok, 'INSERT should parse');
                if (insert.ok && insert.value.kind === 'insert') assertEquals(insert.value.table.group, undefined, 'INSERT table is unqualified');

                const update = parseStatement("UPDATE products SET sku = 'B' WHERE rowId = 'row-1';");
                assertTrue(update.ok, 'UPDATE should parse');
                if (update.ok && update.value.kind === 'update') assertEquals(update.value.table.group, undefined, 'UPDATE table is unqualified');

                const del = parseStatement("DELETE FROM products WHERE rowId = 'row-1';");
                assertTrue(del.ok, 'DELETE should parse');
                if (del.ok && del.value.kind === 'delete') assertEquals(del.value.table.group, undefined, 'DELETE table is unqualified');
            },
        },
        {
            name: '[PARSE06] parses multiple distinct ALLOW rules on one table',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA shop AS (
                      TABLE products (
                        sku string PUB READONLY
                      ) ALLOW insert IF true ALLOW update IF rowAuthor = $author
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-schema') return;
                assertEquals(result.value.tables[0].options.length, 2, 'two table options');
                assertEquals(result.value.tables[0].options[0].kind, 'allow-rule', 'first option is allow');
                assertEquals(result.value.tables[0].options[1].kind, 'allow-rule', 'second option is allow');
            },
        },
        {
            name: '[PARSE07] rejects duplicate ALLOW rules for the same op',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA shop AS (
                      TABLE products (
                        sku string PUB READONLY
                      ) ALLOW insert IF true ALLOW insert IF rowAuthor = $author
                    );
                `);
                assertTrue(!result.ok, 'duplicate ALLOW insert should fail');
                if (!result.ok) assertTrue(result.diagnostics[0].message.includes('Duplicate ALLOW insert'), 'diagnostic mentions duplicate op');
            },
        },
        {
            name: '[PARSE08] rejects ALLOW all mixed with specific ALLOW rules',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA shop AS (
                      TABLE products (
                        sku string PUB READONLY
                      ) ALLOW all IF true ALLOW insert IF true
                    );
                `);
                assertTrue(!result.ok, 'ALLOW all plus ALLOW insert should fail');
                if (!result.ok) assertTrue(result.diagnostics[0].message.includes('ALLOW all cannot be combined'), 'diagnostic mentions mixed all');
            },
        },
        {
            name: '[PARSE09] parses SET ALLOW RULES migration',
            invoke: async () => {
                const result = parseStatement(`
                    ALTER SCHEMA shop AS (
                      SET ALLOW RULES products (
                        ALLOW insert IF true,
                        ALLOW update IF rowAuthor = $author
                      )
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'alter-schema') return;
                assertEquals(result.value.rules[0].kind, 'set-allow-rules', 'migration rule kind');
                if (result.value.rules[0].kind !== 'set-allow-rules') return;
                assertEquals(result.value.rules[0].allowRules.length, 2, 'two allow rules');
            },
        },
        {
            name: '[PARSE10] parses ALLOW UPDATE SCHEMA IF predicates',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE TABLEGROUP shop_prod USING SCHEMA shop
                      ALLOW UPDATE SCHEMA IF EXISTS users.caps WHERE label = 'deployer' AND grantee = $author;
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-tablegroup') return;
                assertTrue(result.value.canDeploy !== undefined, 'canDeploy predicate is present');
            },
        },
        {
            name: '[PARSE10b] parses ALLOW UPDATE REF gate predicates',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE TABLEGROUP docs_gated USING SCHEMA shop
                      BIND users => users
                      ALLOW UPDATE REF users IF EXISTS users.caps WHERE label = 'manager' AND grantee = $author;
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-tablegroup') return;
                assertEquals(result.value.canObserve.length, 1, 'one canObserve gate');
                assertEquals(result.value.canObserve[0].binding, 'users', 'gate binds users');
            },
        },
        {
            name: '[PARSE10c] rejects deprecated CAN DEPLOY SCHEMA syntax',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE TABLEGROUP shop_prod USING SCHEMA shop
                      CAN DEPLOY SCHEMA IF true;
                `);
                assertTrue(!result.ok, 'CAN DEPLOY SCHEMA should fail');
                if (!result.ok) {
                    assertTrue(result.diagnostics.some((d) => d.message.includes('Unexpected CREATE TABLEGROUP clause')),
                        'diagnostic mentions unexpected clause');
                }
            },
        },
        {
            name: '[PARSE10d] rejects row ALLOW rules on CREATE TABLEGROUP',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE TABLEGROUP shop_prod USING SCHEMA shop
                      ALLOW insert IF true;
                `);
                assertTrue(!result.ok, 'ALLOW insert on tablegroup should fail');
                if (!result.ok) {
                    assertTrue(result.diagnostics.some((d) => d.message.includes('Expected ALLOW UPDATE SCHEMA or ALLOW UPDATE REF')),
                        'diagnostic mentions expected tablegroup allow forms');
                }
            },
        },
        {
            name: '[PARSE10e] rejects deprecated ALLOW DEPLOY SCHEMA syntax',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE TABLEGROUP shop_prod USING SCHEMA shop
                      ALLOW DEPLOY SCHEMA IF true;
                `);
                assertTrue(!result.ok, 'ALLOW DEPLOY SCHEMA should fail');
                if (!result.ok) {
                    assertTrue(result.diagnostics.some((d) => d.message.includes('Expected ALLOW UPDATE SCHEMA or ALLOW UPDATE REF')),
                        'diagnostic mentions expected tablegroup allow forms');
                }
            },
        },
        {
            name: '[PARSE11] parses single UPDATE REF binding',
            invoke: async () => {
                const result = parseStatement('UPDATE REF users TO LATEST ON shop_prod;');
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'update-ref') return;
                assertEquals(result.value.ref.kind, 'name', 'ref is a binding name');
                if (result.value.ref.kind === 'name') assertEquals(result.value.ref.text, 'users', 'ref binding');
            },
        },
        {
            name: '[PARSE12] rejects table-qualified UPDATE REF',
            invoke: async () => {
                const result = parseStatement('UPDATE REF users.caps TO LATEST ON shop_prod;');
                assertTrue(!result.ok, 'table-qualified ref should fail');
                if (!result.ok) assertTrue(result.diagnostics[0].message.includes('not group.table'), 'diagnostic mentions group.table');
            },
        },
        {
            name: '[PARSE13] parses publicKey value calls',
            invoke: async () => {
                const result = parseStatement("INSERT INTO users.identities (keyId, publicKey) VALUES ($admin, publicKey($admin));");
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'insert') return;
                assertEquals(result.value.values[1].kind, 'call', 'second value is a function call');
                if (result.value.values[1].kind === 'call') assertEquals(result.value.values[1].name, 'publicKey', 'function name');
            },
        },
        {
            name: '[PARSE14] parses default and explicit identity provider tables',
            invoke: async () => {
                const defaults = parseStatement(`
                    CREATE SCHEMA users_schema AS (
                      TABLE identities (
                        keyId string PUB READONLY,
                        publicKey string PUB READONLY
                      ) IDENTITY PROVIDER ALLOW insert IF true
                    );
                `);
                assertTrue(defaults.ok, 'default provider columns should parse');
                if (defaults.ok && defaults.value.kind === 'create-schema') {
                    const provider = defaults.value.tables[0].options[0];
                    assertEquals(provider.kind, 'identity-provider', 'provider option');
                    if (provider.kind === 'identity-provider') {
                        assertEquals(provider.keyIdColumn, 'keyId', 'default key column');
                        assertEquals(provider.publicKeyColumn, 'publicKey', 'default public key column');
                    }
                }

                const explicit = parseStatement(`
                    CREATE SCHEMA users_schema AS (
                      TABLE people (
                        kid string PUB READONLY,
                        pubkey string PUB READONLY
                      ) IDENTITY PROVIDER (kid, pubkey)
                    );
                `);
                assertTrue(explicit.ok, 'explicit provider columns should parse');
                if (explicit.ok && explicit.value.kind === 'create-schema') {
                    const provider = explicit.value.tables[0].options[0];
                    assertEquals(provider.kind, 'identity-provider', 'provider option');
                    if (provider.kind === 'identity-provider') {
                        assertEquals(provider.keyIdColumn, 'kid', 'explicit key column');
                        assertEquals(provider.publicKeyColumn, 'pubkey', 'explicit public key column');
                    }
                }
            },
        },
        {
            name: '[PARSE15] parses USING IDENTITIES tablegroup provider selection',
            invoke: async () => {
                const local = parseStatement('CREATE TABLEGROUP users USING SCHEMA users_schema USING IDENTITIES identities;');
                assertTrue(local.ok, 'local provider should parse');
                if (local.ok && local.value.kind === 'create-tablegroup') assertEquals(local.value.idProvider, 'identities', 'local provider');

                const foreign = parseStatement('CREATE TABLEGROUP app USING SCHEMA app_schema BIND users => users USING IDENTITIES users.identities;');
                assertTrue(foreign.ok, 'foreign provider should parse');
                if (foreign.ok && foreign.value.kind === 'create-tablegroup') assertEquals(foreign.value.idProvider, 'users.identities', 'foreign provider');
            },
        },
        {
            name: '[PARSE16] rejects old tablegroup IDENTITY PROVIDER syntax',
            invoke: async () => {
                const result = parseStatement('CREATE TABLEGROUP app USING SCHEMA app_schema IDENTITY PROVIDER users.identities;');
                assertTrue(!result.ok, 'old tablegroup provider syntax should fail');
            },
        },
        {
            name: '[PARSE17] rejects removed owner-oriented syntax',
            invoke: async () => {
                assertTrue(!parseStatement("INSERT INTO users.caps (label) VALUES ('writer') OWNED BY $alice;").ok,
                    'INSERT OWNED BY should fail');
                assertTrue(!parseStatement("CREATE SCHEMA s AS (TABLE t (v string) ALLOW update IF OWNER IS $author);").ok,
                    'OWNER IS should fail');
                assertTrue(!parseStatement("CREATE SCHEMA s AS (TABLE t (v string) ALLOW insert IF EXISTS caps WHERE label = 'x' OWNED BY $author);").ok,
                    'EXISTS OWNED BY should fail');
            },
        },
        {
            name: '[PARSE18] parses BY author clause on authored statements',
            invoke: async () => {
                const insert = parseStatement("INSERT INTO shop.products (sku) VALUES ('A') BY $alice;");
                assertTrue(insert.ok, 'INSERT BY $alice should parse');
                if (insert.ok && insert.value.kind === 'insert') {
                    assertEquals(insert.value.author?.kind, 'variable', 'insert author is a variable ref');
                    if (insert.value.author?.kind === 'variable') assertEquals(insert.value.author.name, 'alice', 'insert author name');
                }

                const update = parseStatement("UPDATE shop.products SET sku = 'B' WHERE rowId = #ab BY #c0ffee AT LATEST;");
                assertTrue(update.ok, 'UPDATE BY #prefix AT LATEST should parse');
                if (update.ok && update.value.kind === 'update') {
                    assertEquals(update.value.author?.kind, 'hash', 'update author is a hash ref');
                    if (update.value.author?.kind === 'hash') assertEquals(update.value.author.prefix, 'c0ffee', 'update author prefix');
                    assertEquals(update.value.at?.kind, 'latest', 'update keeps AT clause alongside BY');
                }

                const anon = parseStatement("DELETE FROM shop.products WHERE rowId = #ab BY NOBODY;");
                assertTrue(anon.ok, 'DELETE BY NOBODY should parse');
                if (anon.ok && anon.value.kind === 'delete') assertEquals(anon.value.author?.kind, 'nobody', 'delete author is nobody');

                const deploy = parseStatement('UPDATE SCHEMA s TO LATEST ON g BY $deployer;');
                assertTrue(deploy.ok, 'UPDATE SCHEMA BY should parse');
                if (deploy.ok && deploy.value.kind === 'update-schema') assertEquals(deploy.value.author?.kind, 'variable', 'update schema author ref');

                const alter = parseStatement('ALTER SCHEMA s AS (DROP TABLE t) BY $admin;');
                assertTrue(alter.ok, 'ALTER BY should parse');
                if (alter.ok && alter.value.kind === 'alter-schema') assertEquals(alter.value.author?.kind, 'variable', 'alter author ref');
            },
        },
        {
            name: '[PARSE19] BY clause requires an identity and is rejected on bundle inner writes',
            invoke: async () => {
                assertTrue(!parseStatement("INSERT INTO shop.products (sku) VALUES ('A') BY;").ok,
                    'BY without an identity should fail');
                assertTrue(!parseStatement("INSERT INTO shop.products (sku) VALUES ('A') BY 'alice';").ok,
                    'BY with a string literal should fail');

                const bundleOk = parseStatement("BUNDLE ON g (INSERT INTO g.t (v) VALUES ('x');) BY $alice;");
                assertTrue(bundleOk.ok, 'BUNDLE-level BY should parse');
                if (bundleOk.ok && bundleOk.value.kind === 'bundle') assertEquals(bundleOk.value.author?.kind, 'variable', 'bundle author ref');

                assertTrue(!parseStatement("BUNDLE ON g (INSERT INTO g.t (v) VALUES ('x') BY $alice;);").ok,
                    'BY on a bundle inner write should fail');
            },
        },
        {
            name: '[PARSE20] parses UPDATE SCHEMA with trailing causal AT',
            invoke: async () => {
                const result = parseStatement('UPDATE SCHEMA shop TO LATEST ON g BY $admin AT {#cut};');
                assertTrue(result.ok, 'UPDATE SCHEMA with BY and trailing AT should parse');
                if (!result.ok || result.value.kind !== 'update-schema') return;
                assertEquals(result.value.author?.kind, 'variable', 'author present');
                assertEquals(result.value.at?.kind, 'set', 'causal AT present');
            },
        },
        {
            name: '[PARSE22] parses and lowers table-qualified correlation in allow rule predicates',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA users AS (
                      TABLE profiles (
                        keyId string PUB READONLY
                      ) ALLOW insert IF keyId = $author AND EXISTS users.identities WHERE users.identities.keyId = profiles.keyId
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-schema') return;
                const allow = result.value.tables[0].options.find((o) => o.kind === 'allow-rule');
                assertTrue(allow !== undefined && allow.kind === 'allow-rule', 'allow rule option');
                if (allow === undefined || allow.kind !== 'allow-rule') return;

                const table = result.value.tables[0];
                const columnsOf = columnsOfFromTableDecls(result.value.tables);
                const scope = {
                    gated: { name: table.name, columns: columnSetFromTableDecl(table) },
                    columnsOf,
                };
                const lowered = lowerRestrictionPredicate(allow.predicate, scope);
                assertEquals(lowered.p, 'and', 'top-level predicate is AND');
                if (lowered.p !== 'and') return;
                const exists = lowered.args[1];
                assertEquals(exists.p, 'exists', 'second arm is EXISTS');
                if (exists.p !== 'exists') return;
                assertEquals(exists.where.keyId, '$row.keyId', 'EXISTS WHERE correlates via gated table column');
            },
        },
        {
            name: '[PARSE23] rejects removed $row surface syntax',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA s AS (
                      TABLE t (v string PUB READONLY)
                        ALLOW insert IF EXISTS u WHERE v = $row.v
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-schema') return;
                const allow = result.value.tables[0].options.find((o) => o.kind === 'allow-rule');
                if (allow === undefined || allow.kind !== 'allow-rule') return;
                const table = result.value.tables[0];
                const scope = {
                    gated: { name: table.name, columns: columnSetFromTableDecl(table) },
                    columnsOf: columnsOfFromTableDecls(result.value.tables),
                };
                let threw = false;
                try {
                    lowerRestrictionPredicate(allow.predicate, scope);
                } catch {
                    threw = true;
                }
                assertTrue(threw, 'lowering $row should fail');
            },
        },
        {
            name: '[PARSE24] allows unqualified columns when unambiguous',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA s AS (
                      TABLE profiles (keyId string PUB READONLY)
                        ALLOW update IF keyId = $author
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-schema') return;
                const allow = result.value.tables[0].options.find((o) => o.kind === 'allow-rule');
                if (allow === undefined || allow.kind !== 'allow-rule') return;
                const table = result.value.tables[0];
                const scope = {
                    gated: { name: table.name, columns: columnSetFromTableDecl(table) },
                    columnsOf: columnsOfFromTableDecls(result.value.tables),
                };
                const lowered = lowerRestrictionPredicate(allow.predicate, scope);
                assertEquals(lowered.p, 'cmp', 'top-level cmp');
                if (lowered.p !== 'cmp') return;
                assertTrue('col' in lowered.left && lowered.left.col === 'keyId', 'unqualified keyId resolves to gated table');
            },
        },
        {
            name: '[PARSE25] requires qualification for ambiguous EXISTS correlation',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA s AS (
                      TABLE profiles (keyId string PUB READONLY)
                        ALLOW insert IF EXISTS identities WHERE keyId = keyId,
                      TABLE identities (keyId string PUB READONLY)
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-schema') return;
                const profiles = result.value.tables.find((t) => t.name === 'profiles');
                assertTrue(profiles !== undefined, 'profiles table');
                if (profiles === undefined) return;
                const allow = profiles.options.find((o) => o.kind === 'allow-rule');
                if (allow === undefined || allow.kind !== 'allow-rule') return;
                const scope = {
                    gated: { name: profiles.name, columns: columnSetFromTableDecl(profiles) },
                    columnsOf: columnsOfFromTableDecls(result.value.tables),
                };
                let threw = false;
                try {
                    lowerRestrictionPredicate(allow.predicate, scope);
                } catch (e) {
                    threw = e instanceof Error && e.message.includes('ambiguous');
                }
                assertTrue(threw, 'ambiguous bare keyId should require qualification');
            },
        },
        {
            name: '[PARSE26] requires AS alias for self-referential EXISTS',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA s AS (
                      TABLE caps (label string PUB READONLY, grantee string PUB READONLY)
                        ALLOW insert IF EXISTS caps WHERE label = 'manager'
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-schema') return;
                const allow = result.value.tables[0].options.find((o) => o.kind === 'allow-rule');
                if (allow === undefined || allow.kind !== 'allow-rule') return;
                const table = result.value.tables[0];
                const scope = {
                    gated: { name: table.name, columns: columnSetFromTableDecl(table) },
                    columnsOf: columnsOfFromTableDecls(result.value.tables),
                };
                let threw = false;
                try {
                    lowerRestrictionPredicate(allow.predicate, scope);
                } catch (e) {
                    threw = e instanceof Error && e.message.includes('self-referential');
                }
                assertTrue(threw, 'self-referential EXISTS without alias should fail');
            },
        },
        {
            name: '[PARSE27] rejects top-level column references in group gates',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE TABLEGROUP g USING SCHEMA s
                      ALLOW UPDATE SCHEMA IF keyId = $author;
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-tablegroup') return;
                const scope = { columnsOf: () => undefined };
                let threw = false;
                try {
                    lowerRestrictionPredicate(result.value.canDeploy!, scope);
                } catch (e) {
                    threw = e instanceof Error && e.message.includes('not allowed');
                }
                assertTrue(threw, 'top-level column in canDeploy should fail');
            },
        },
        {
            name: '[PARSE28] parses #prefix hash values in INSERT VALUES',
            invoke: async () => {
                const result = parseStatement("INSERT INTO profiles (ownerId, label) VALUES (#abc, 'Admin');");
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'insert') return;
                assertEquals(result.value.values[0].kind, 'hash', 'first value is a hash ref');
                if (result.value.values[0].kind === 'hash') assertEquals(result.value.values[0].prefix, 'abc', 'hash prefix');
            },
        },
        {
            name: '[PARSE29] parses SEED on CREATE DATABASE and CREATE TABLEGROUP',
            invoke: async () => {
                const db = parseStatement("CREATE DATABASE app SEED 'db-seed';");
                assertTrue(db.ok && db.value.kind === 'create-database', 'database parse');
                if (db.ok && db.value.kind === 'create-database') assertEquals(db.value.seed, 'db-seed', 'database seed');

                const group = parseStatement("CREATE TABLEGROUP g SEED 'g-seed' USING SCHEMA shop;");
                assertTrue(group.ok && group.value.kind === 'create-tablegroup', 'tablegroup parse');
                if (group.ok && group.value.kind === 'create-tablegroup') assertEquals(group.value.seed, 'g-seed', 'group seed');
            },
        },
        {
            name: '[PARSE30] parses uuid pseudo-column on INSERT',
            invoke: async () => {
                const result = parseStatement("INSERT INTO products (uuid, sku) VALUES ('u1', 'A');");
                assertTrue(result.ok && result.value.kind === 'insert', 'insert parse');
                if (!result.ok || result.value.kind !== 'insert') return;
                assertEquals(result.value.columns[0], 'uuid', 'uuid column');
            },
        },
        {
            name: '[PARSE31] rejects uuid as schema column name',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA bad AS (
                      TABLE t (
                        uuid string
                      )
                    );
                `);
                assertTrue(!result.ok, 'parse should fail for uuid column');
            },
        },
    ],
};

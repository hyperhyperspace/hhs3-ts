import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { compileTable } from "../src/compile/create.js";
import { lowerRestrictionPredicate, lowerRowFilter } from "../src/compile/query.js";
import { columnSetFromTableDecl, columnsOfFromTableDecls } from "../src/compile/rule_scope.js";
import { parseStatement } from "../src/syntax/parser.js";
import { createTestBindContext } from "./mock_bind_context.js";

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
        {
            name: '[PARSE32] parses precise column types + parameters + MIN/MAX',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA finance AS (
                      TABLE ledger (
                        seq bigint PUB,
                        memo string(64),
                        blob bytes(32),
                        amount decimal(18, 2),
                        qty integer MIN 0 MAX 100
                      )
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-schema') return;
                const cols = result.value.tables[0].columns;
                const byName = (n: string) => cols.find((c) => c.name === n)!;

                assertEquals(byName('seq').type, 'bigint', 'seq is bigint');
                assertEquals(byName('memo').type, 'string', 'memo is string');
                assertEquals(byName('memo').constraints?.maxLength, 64, 'string(64) -> maxLength 64');
                assertEquals(byName('blob').type, 'bytes', 'blob is bytes');
                assertEquals(byName('blob').constraints?.maxLength, 32, 'bytes(32) -> maxLength 32');
                assertEquals(byName('amount').type, 'decimal', 'amount is decimal');
                assertEquals(byName('amount').constraints?.precision, 18, 'decimal precision');
                assertEquals(byName('amount').constraints?.scale, 2, 'decimal scale');
                assertTrue(byName('qty').constraints?.min !== undefined, 'MIN bound present');
                assertTrue(byName('qty').constraints?.max !== undefined, 'MAX bound present');
            },
        },
        {
            name: '[PARSE33] parses precise types in ALTER SCHEMA ADD COLUMN',
            invoke: async () => {
                const result = parseStatement(`
                    ALTER SCHEMA finance AS (
                      ADD COLUMN ledger.balance decimal(20, 4) MIN 0
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'alter-schema') return;
                const rule = result.value.rules[0];
                assertEquals(rule.kind, 'add-column', 'add-column rule');
                if (rule.kind !== 'add-column') return;
                assertEquals(rule.column.type, 'decimal', 'added column is decimal');
                assertEquals(rule.column.constraints?.precision, 20, 'precision');
                assertEquals(rule.column.constraints?.scale, 4, 'scale');
                assertTrue(rule.column.constraints?.min !== undefined, 'MIN bound present');
            },
        },
        {
            name: '[PARSE34] LIKE lowers to a like atom: verbatim pattern, column pattern, ESCAPE normalized',
            invoke: async () => {
                const json = (v: unknown) => JSON.stringify(v);

                assertEquals(json(lowerAllow("name LIKE 'ok-%'")),
                    json({ p: 'like', value: { col: 'name' }, pattern: { lit: 'ok-%' } }), 'prefix pattern kept verbatim');
                assertEquals(json(lowerAllow("name LIKE 'a_c'")),
                    json({ p: 'like', value: { col: 'name' }, pattern: { lit: 'a_c' } }), 'no wildcard splitting, _ kept');
                assertEquals(json(lowerAllow("name LIKE 'exact'")),
                    json({ p: 'like', value: { col: 'name' }, pattern: { lit: 'exact' } }), 'no-wildcard LIKE stays a like');
                assertEquals(json(lowerAllow("name LIKE t.tag")),
                    json({ p: 'like', value: { col: 'name' }, pattern: { col: 'tag' } }), 'column pattern');
                assertEquals(json(lowerAllow("name LIKE '100!%!_x!!' ESCAPE '!'")),
                    json({ p: 'like', value: { col: 'name' }, pattern: { lit: '100\\%\\_x!' } }), 'ESCAPE rewritten to canonical backslash form');
                assertEquals(json(lowerAllow("name LIKE 'a\\b%' ESCAPE ''")),
                    json({ p: 'like', value: { col: 'name' }, pattern: { lit: 'a\\\\b%' } }), "ESCAPE '' makes backslash literal");
                assertEquals(json(lowerAllow("name LIKE 'a\\%' escape '\\'")),
                    json({ p: 'like', value: { col: 'name' }, pattern: { lit: 'a\\%' } }), 'ESCAPE is case-insensitive and backslash is a no-op');

                expectLowerThrows("name LIKE 'a\\'", 'unescaped', 'trailing lone backslash');
                expectLowerThrows("name LIKE 'a!' ESCAPE '!'", 'ESCAPE character', 'pattern ending with its escape char');
                expectLowerThrows("name LIKE 'a' ESCAPE '!!'", 'single character', 'multi-character ESCAPE');
                expectLowerThrows("name LIKE t.tag ESCAPE '!'", 'literal pattern', 'ESCAPE on a column pattern');
                expectLowerThrows("name LIKE 5", 'must be a string', 'non-string literal pattern');

                const where = await lowerWhere("kind LIKE 'fr_it%'");
                assertEquals(json(where), json({ p: 'like', value: { col: 'kind' }, pattern: { lit: 'fr_it%' } }), 'SELECT WHERE LIKE');
                const whereEscaped = await lowerWhere("kind LIKE '#_%' ESCAPE '#'");
                assertEquals(json(whereEscaped), json({ p: 'like', value: { col: 'kind' }, pattern: { lit: '\\_%' } }), 'SELECT WHERE LIKE ESCAPE');
                const whereCol = await lowerWhere('kind LIKE label');
                assertEquals(json(whereCol), json({ p: 'like', value: { col: 'kind' }, pattern: { col: 'label' } }), 'SELECT WHERE LIKE column');
            },
        },
        {
            name: '[PARSE35] expression grammar: arithmetic precedence, length(), unary minus, exponents',
            invoke: async () => {
                const json = (v: unknown) => JSON.stringify(v);
                const col = (c: string) => ({ col: c });
                const lit = (v: unknown) => ({ lit: v });
                const fn = (f: string, ...args: unknown[]) => ({ fn: f, args });

                assertEquals(json(lowerAllow('n + 1 * 2 >= m - 3')),
                    json({ p: 'cmp', cmp: 'ge', left: fn('add', col('n'), fn('mul', lit(1), lit(2))), right: fn('sub', col('m'), lit(3)) }),
                    '* binds tighter than +, and both tighter than comparison');
                assertEquals(json(lowerAllow('(n + 1) * 2 = 6')),
                    json({ p: 'cmp', cmp: 'eq', left: fn('mul', fn('add', col('n'), lit(1)), lit(2)), right: lit(6) }),
                    'parentheses group arithmetic');
                assertEquals(json(lowerAllow('n - m - 1 = 0')),
                    json({ p: 'cmp', cmp: 'eq', left: fn('sub', fn('sub', col('n'), col('m')), lit(1)), right: lit(0) }),
                    '- is left-associative');
                assertEquals(json(lowerAllow('length(name) < 8')),
                    json({ p: 'cmp', cmp: 'lt', left: fn('len', col('name')), right: lit(8) }), 'length() lowers to len');
                assertEquals(json(lowerAllow('LENGTH(name) + 1 < 8')),
                    json({ p: 'cmp', cmp: 'lt', left: fn('add', fn('len', col('name')), lit(1)), right: lit(8) }), 'LENGTH is case-insensitive');
                assertEquals(json(lowerAllow('n = -1')), json({ p: 'cmp', cmp: 'eq', left: col('n'), right: lit(-1) }), 'negative literal');
                assertEquals(json(lowerAllow('n - -1 = 0')),
                    json({ p: 'cmp', cmp: 'eq', left: fn('sub', col('n'), lit(-1)), right: lit(0) }), 'binary minus of a negative literal');
                assertEquals(json(lowerAllow('n = -(2)')), json({ p: 'cmp', cmp: 'eq', left: col('n'), right: lit(-2) }), 'unary minus folds through parentheses');
                assertEquals(json(lowerAllow('n = 0 - 0')), json({ p: 'cmp', cmp: 'eq', left: col('n'), right: fn('sub', lit(0), lit(0)) }), '0 - 0 stays arithmetic');
                assertEquals(json(lowerAllow('n < 1.5e3')), json({ p: 'cmp', cmp: 'lt', left: col('n'), right: lit(1500) }), 'exponent literal');
                assertEquals(json(lowerAllow('n < 1e-7')), json({ p: 'cmp', cmp: 'lt', left: col('n'), right: lit(1e-7) }), 'negative exponent literal');
                assertEquals(json(lowerAllow('n = 1 -- trailing comment\n')), json({ p: 'cmp', cmp: 'eq', left: col('n'), right: lit(1) }), '-- is still a comment');

                expectParseError('ALLOW insert IF -n = 1', 'Unary minus', 'unary minus on a column');
                expectParseError('ALLOW insert IF name', 'Expected a condition', 'a bare value is not a condition');
                expectParseError('ALLOW insert IF n = (m > 1)', 'Expected a value', 'a condition is not a value');
                expectLowerThrows("name = '$author'", 'reserved', "quoted '$author' is rejected");
                expectLowerThrows("EXISTS t AS t2 WHERE t2.name = '$row.name'", 'reserved', "quoted '$row' term in EXISTS WHERE is rejected");

                const where = await lowerWhere('qty * 2 > price - 1');
                assertEquals(json(where),
                    json({ p: 'cmp', cmp: 'gt', left: fn('mul', col('qty'), lit(2)), right: fn('sub', col('price'), lit(1)) }),
                    'SELECT WHERE arithmetic');
                const whereLen = await lowerWhere("length(kind) = 5 AND kind != '$x'");
                assertEquals(json(whereLen),
                    json({ p: 'and', args: [{ p: 'cmp', cmp: 'eq', left: fn('len', col('kind')), right: lit(5) }, { p: 'cmp', cmp: 'ne', left: col('kind'), right: lit('$x') }] }),
                    "SELECT WHERE length(), and '$' strings stay plain literals in queries");
            },
        },
        {
            name: '[PARSE36] expression grammar: AND/OR/NOT precedence and grouping',
            invoke: async () => {
                const json = (v: unknown) => JSON.stringify(v);
                const eq = (c: string, v: number) => ({ p: 'cmp', cmp: 'eq', left: { col: c }, right: { lit: v } });

                assertEquals(json(lowerAllow('n = 1 OR n = 2 AND m = 3')),
                    json({ p: 'or', args: [eq('n', 1), { p: 'and', args: [eq('n', 2), eq('m', 3)] }] }), 'AND binds tighter than OR');
                assertEquals(json(lowerAllow('(n = 1 OR n = 2) AND m = 3')),
                    json({ p: 'and', args: [{ p: 'or', args: [eq('n', 1), eq('n', 2)] }, eq('m', 3)] }), 'parenthesized OR inside AND');
                assertEquals(json(lowerAllow('n = 1 AND n = 2 AND m = 3')),
                    json({ p: 'and', args: [eq('n', 1), eq('n', 2), eq('m', 3)] }), 'an unparenthesized chain is one group');
                assertEquals(json(lowerAllow('(n = 1 AND n = 2) AND m = 3')),
                    json({ p: 'and', args: [{ p: 'and', args: [eq('n', 1), eq('n', 2)] }, eq('m', 3)] }), 'a parenthesized group keeps its nesting');
                assertEquals(json(lowerAllow('n = 1 AND (n = 2 AND m = 3)')),
                    json({ p: 'and', args: [eq('n', 1), { p: 'and', args: [eq('n', 2), eq('m', 3)] }] }), 'nesting on the right is kept too');
                assertEquals(json(lowerAllow('((n = 1))')), json(eq('n', 1)), 'redundant parentheses collapse');
                assertEquals(json(lowerAllow('TRUE')), json({ p: 'true' }), 'TRUE as a condition');
                assertEquals(json(lowerAllow('flag = TRUE')),
                    json({ p: 'cmp', cmp: 'eq', left: { col: 'flag' }, right: { lit: true } }), 'TRUE as a value');
                assertEquals(json(lowerAllow("(EXISTS t AS t2 WHERE t2.name = 'x') OR n = 1")),
                    json({ p: 'or', args: [{ p: 'exists', table: 't', where: { name: 'x' } }, eq('n', 1)] }), 'a parenthesized EXISTS ends its WHERE');

                const not = await lowerWhere('NOT qty = 1 AND qty = 2');
                assertEquals(json(not),
                    json({ p: 'and', args: [{ p: 'not', arg: eq('qty', 1) }, eq('qty', 2)] }), 'NOT binds tighter than AND');
            },
        },
        {
            name: '[PARSE37] negative literals in value positions',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA s AS (
                      TABLE t (a integer DEFAULT -1 MIN -10 MAX -2, b float DEFAULT -1.5e-3)
                    );
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-schema') return;
                const [a, b] = result.value.tables[0].columns;
                const litOf = (v: unknown) => (v as { kind: string; value: unknown }).value;
                assertEquals(litOf(a.defaultValue), -1, 'negative DEFAULT');
                assertEquals(litOf(a.constraints?.min), -10, 'negative MIN');
                assertEquals(litOf(a.constraints?.max), -2, 'negative MAX');
                assertEquals(litOf(b.defaultValue), -0.0015, 'negative float DEFAULT with exponent');

                const insert = parseStatement("INSERT INTO g.t (a, b) VALUES (-3, [-1, 2]);");
                assertTrue(insert.ok && insert.value.kind === 'insert', 'insert parse');
                if (!insert.ok || insert.value.kind !== 'insert') return;
                assertEquals(litOf(insert.value.values[0]), -3, 'negative VALUES literal');
                assertEquals(JSON.stringify(litOf(insert.value.values[1])), '[-1,2]', 'negative number inside a JSON literal');

                const update = parseStatement("UPDATE g.t SET a = -4 WHERE rowId = #abc;");
                assertTrue(update.ok && update.value.kind === 'update', 'update parse');
                if (update.ok && update.value.kind === 'update') assertEquals(litOf(update.value.values[0].value), -4, 'negative SET literal');
            },
        },
        {
            name: '[PARSE38] double-quoted identifiers name keyword-colliding tables and columns',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA s AS (
                      TABLE "table" ("identity" identity READONLY, "length" integer READONLY, "escape" string READONLY, "string" string)
                        ALLOW insert IF "table"."identity" = $author AND length("table"."escape") < "table"."length"
                          AND "table"."escape" LIKE 'x%' AND (EXISTS "table" AS t WHERE t."identity" = "table"."identity")
                    );
                `);
                assertTrue(result.ok, `parse should succeed: ${JSON.stringify(result.ok ? '' : result.diagnostics.map((d) => d.message))}`);
                if (!result.ok || result.value.kind !== 'create-schema') return;
                const def = compileTable(result.value.tables[0]);
                assertEquals(def.name, 'table', 'quoted table name');
                assertEquals(Object.keys(def.columns).join(','), 'identity,length,escape,string', 'quoted column names');
                assertEquals(def.columns['string'].type, 'string', 'a quoted name is never a type');
                assertEquals(JSON.stringify(def.restrictions?.[0].rule), JSON.stringify({
                    p: 'and',
                    args: [
                        { p: 'cmp', cmp: 'eq', left: { col: 'identity' }, right: { lit: '$author' } },
                        { p: 'cmp', cmp: 'lt', left: { fn: 'len', args: [{ col: 'escape' }] }, right: { col: 'length' } },
                        { p: 'like', value: { col: 'escape' }, pattern: { lit: 'x%' } },
                        { p: 'exists', table: 'table', where: { identity: '$row.identity' } },
                    ],
                }), 'quoted names lower like bare ones');

                const notFn = parseStatement('CREATE SCHEMA s AS (TABLE t (a integer) ALLOW insert IF "length"(1) = 1);');
                assertTrue(!notFn.ok, 'a quoted "length" is not the length function');
                const unquoted = parseStatement('CREATE SCHEMA s AS (TABLE t (identity identity));');
                assertTrue(!unquoted.ok, 'an unquoted keyword is still not a column name');
            },
        },
        {
            name: "[PARSE39] JSON '<json text>' writes any json value; null inside JSON is rejected",
            invoke: async () => {
                const doc = { k: ['v', "it's", 'a\nb'], n: [1, 2.5] };
                const text = `JSON '{"k": ["v", "it''s", "a\\nb"], "n": [1, 2.5]}'`;
                const same = (a: unknown, what: string) =>
                    assertEquals(JSON.stringify(a), JSON.stringify(doc), what);

                const schema = parseStatement(`CREATE SCHEMA s AS (
                    TABLE docs ("json" json PUB READONLY DEFAULT ${text})
                      ALLOW insert IF EXISTS docs AS d WHERE d."json" = ${text}
                );`);
                assertTrue(schema.ok, `schema parses: ${JSON.stringify(schema.ok ? '' : schema.diagnostics.map((d) => d.message))}`);
                if (!schema.ok || schema.value.kind !== 'create-schema') return;
                const def = compileTable(schema.value.tables[0]);
                same(def.columns['json'].default, 'DEFAULT JSON literal');
                const rule = def.restrictions?.[0].rule;
                same(rule?.p === 'exists' ? rule.where['json'] : undefined, 'EXISTS where-value JSON literal');

                const insert = parseStatement(`INSERT INTO docs ("json") VALUES (${text});`);
                assertTrue(insert.ok && insert.value.kind === 'insert', 'INSERT parses');
                if (insert.ok && insert.value.kind === 'insert') {
                    const v = insert.value.values[0];
                    same(v.kind === 'literal' ? v.value : undefined, 'INSERT JSON literal');
                }
                const update = parseStatement(`UPDATE docs SET "json" = ${text} WHERE rowId = #abc;`);
                assertTrue(update.ok && update.value.kind === 'update', 'UPDATE parses');
                if (update.ok && update.value.kind === 'update') {
                    const v = update.value.values[0].value;
                    same(v.kind === 'literal' ? v.value : undefined, 'UPDATE SET JSON literal');
                }

                const errorOf = (value: string) => {
                    const r = parseStatement(`INSERT INTO docs (d) VALUES (${value});`);
                    return r.ok ? '' : r.diagnostics.map((d) => d.message).join('; ');
                };
                assertTrue(errorOf(`JSON 'nope'`).includes('Invalid JSON in JSON literal'), 'invalid JSON text');
                assertTrue(errorOf(`JSON 'null'`).includes('cannot contain null'), 'JSON null');
                assertTrue(errorOf(`JSON '[1, null]'`).includes('cannot contain null'), 'nested null');
                assertTrue(errorOf('[1, null]').includes('cannot contain null'), 'nested null in the bracket form');
                assertTrue(errorOf('["x"]').includes(`JSON '...'`), 'the bracket form points strings at JSON literals');
            },
        },
    ],
};

function expectParseError(allow: string, messagePart: string, why: string) {
    const result = parseStatement(`CREATE SCHEMA s AS (TABLE t (name string READONLY, n integer READONLY, m integer READONLY) ${allow});`);
    const messages = result.ok ? [] : result.diagnostics.map((d) => d.message);
    assertTrue(messages.some((m) => m.includes(messagePart)), `${why}: expected a diagnostic mentioning '${messagePart}', got ${JSON.stringify(messages)}`);
}

function lowerAllow(predicate: string) {
    const result = parseStatement(`CREATE SCHEMA s AS (TABLE t (name string READONLY, tag string READONLY, n integer READONLY, m integer READONLY, flag boolean READONLY) ALLOW insert IF ${predicate});`);
    if (!result.ok || result.value.kind !== 'create-schema') {
        throw new Error(`parse failed for '${predicate}': ${JSON.stringify(result.ok ? result.value.kind : result.diagnostics)}`);
    }
    const table = result.value.tables[0];
    const allow = table.options.find((o) => o.kind === 'allow-rule');
    if (allow === undefined || allow.kind !== 'allow-rule') throw new Error('no allow rule');
    return lowerRestrictionPredicate(allow.predicate, {
        gated: { name: table.name, columns: columnSetFromTableDecl(table) },
        columnsOf: columnsOfFromTableDecls(result.value.tables),
    });
}

function expectLowerThrows(predicate: string, messagePart: string, why: string) {
    let message = '';
    try {
        lowerAllow(predicate);
    } catch (e) {
        message = e instanceof Error ? e.message : String(e);
    }
    assertTrue(message.includes(messagePart), `${why}: expected an error mentioning '${messagePart}', got '${message}'`);
}

async function lowerWhere(where: string) {
    const result = parseStatement(`SELECT * FROM g.items WHERE ${where};`);
    if (!result.ok || result.value.kind !== 'select' || result.value.where === undefined) {
        throw new Error(`parse failed for '${where}'`);
    }
    return lowerRowFilter(result.value.where, createTestBindContext(undefined as never));
}

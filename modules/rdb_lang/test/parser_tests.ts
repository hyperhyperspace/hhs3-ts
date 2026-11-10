import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { compileTable } from "../src/compile/create.js";
import { lowerRestrictionPredicate, lowerRowFilter } from "../src/compile/query.js";
import { columnSetFromTableDecl, columnsOfFromTableDecls } from "../src/compile/rule_scope.js";
import { parseStatement } from "../src/syntax/parser.js";
import { createTestBindContext } from "./mock_bind_context.js";
import type { CatalogGroupExpr } from "../src/syntax/ast.js";
import { formatPredicate } from "@hyper-hyper-space/hhs3_rdb";
import type { Where, WhereLeaf } from "@hyper-hyper-space/hhs3_rdb";

// The single group of `CREATE CATALOG c VERSION '1.0.0' AS (<body>)`.
function catalogGroup(body: string): CatalogGroupExpr {
    const result = parseStatement(`CREATE CATALOG c VERSION '1.0.0' AS (${body});`);
    if (!result.ok) throw new Error(`parse should succeed: ${result.diagnostics.map((d) => d.message).join('; ')}`);
    if (result.value.kind !== 'create-catalog' || result.value.groups.length !== 1) throw new Error('expected one catalog group');
    return result.value.groups[0];
}

function parseMessages(sql: string): string[] {
    const result = parseStatement(sql);
    return result.ok ? [] : result.diagnostics.map((d) => d.message);
}

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
                      ) ALLOW insert IF EXISTS (users.caps WHERE label = 'buyer' AND grantee = $author)
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
            name: '[PARSE10] parses ALLOW DEPLOY IF predicates on catalog groups',
            invoke: async () => {
                const group = catalogGroup(`
                    TABLEGROUP shop_prod USING SCHEMA shop
                      ALLOW DEPLOY IF EXISTS (users.caps WHERE label = 'deployer' AND grantee = $author)
                `);
                assertTrue(group.canDeploy !== undefined, 'canDeploy predicate is present');
            },
        },
        {
            name: '[PARSE10b] parses ALLOW UPDATE REF gate predicates',
            invoke: async () => {
                const group = catalogGroup(`
                    TABLEGROUP docs_gated USING SCHEMA shop
                      BIND users => users
                      ALLOW UPDATE REF users IF EXISTS (users.caps WHERE label = 'manager' AND grantee = $author)
                `);
                assertEquals(group.canObserve.length, 1, 'one canObserve gate');
                assertEquals(group.canObserve[0].binding, 'users', 'gate binds users');
            },
        },
        {
            name: '[PARSE10c] rejects deprecated CAN DEPLOY SCHEMA syntax',
            invoke: async () => {
                const result = parseStatement(`CREATE CATALOG c VERSION '1.0.0' AS (
                    TABLEGROUP shop_prod USING SCHEMA shop
                      CAN DEPLOY SCHEMA IF true
                );`);
                assertTrue(!result.ok, 'CAN DEPLOY SCHEMA should fail');
                if (!result.ok) {
                    assertTrue(result.diagnostics.some((d) => d.message.includes('Unexpected TABLEGROUP clause')),
                        'diagnostic mentions unexpected clause');
                }
            },
        },
        {
            name: '[PARSE10d] rejects row ALLOW rules on catalog groups',
            invoke: async () => {
                const result = parseStatement(`CREATE CATALOG c VERSION '1.0.0' AS (
                    TABLEGROUP shop_prod USING SCHEMA shop
                      ALLOW insert IF true
                );`);
                assertTrue(!result.ok, 'ALLOW insert on a catalog group should fail');
                if (!result.ok) {
                    assertTrue(result.diagnostics.some((d) => d.message.includes('Expected ALLOW DEPLOY IF or ALLOW UPDATE REF')),
                        'diagnostic mentions expected tablegroup allow forms');
                }
            },
        },
        {
            name: '[PARSE10e] rejects ALLOW UPDATE SCHEMA IF, replaced by ALLOW DEPLOY IF',
            invoke: async () => {
                const result = parseStatement(`CREATE CATALOG c VERSION '1.0.0' AS (
                    TABLEGROUP shop_prod USING SCHEMA shop
                      ALLOW UPDATE SCHEMA IF true
                );`);
                assertTrue(!result.ok, 'ALLOW UPDATE SCHEMA should fail');
                if (!result.ok) {
                    assertTrue(result.diagnostics.some((d) => d.message.includes('Expected ALLOW DEPLOY IF or ALLOW UPDATE REF')),
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
                const local = catalogGroup('TABLEGROUP users USING SCHEMA users_schema USING IDENTITIES identities');
                assertEquals(local.idProvider, 'identities', 'local provider');

                const foreign = catalogGroup('TABLEGROUP app USING SCHEMA app_schema BIND users => users USING IDENTITIES users.identities');
                assertEquals(foreign.idProvider, 'users.identities', 'foreign provider');
            },
        },
        {
            name: '[PARSE16] rejects old tablegroup IDENTITY PROVIDER syntax',
            invoke: async () => {
                const result = parseStatement("CREATE CATALOG c VERSION '1.0.0' AS (TABLEGROUP app USING SCHEMA app_schema IDENTITY PROVIDER users.identities);");
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
                assertTrue(!parseStatement("CREATE SCHEMA s AS (TABLE t (v string) ALLOW insert IF EXISTS (caps WHERE label = 'x') OWNED BY $author);").ok,
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

                const deploy = parseStatement("UPDATE CATALOG c TO '1.1.0' ON db BY $deployer;");
                assertTrue(deploy.ok, 'UPDATE CATALOG BY should parse');
                if (deploy.ok && deploy.value.kind === 'update-catalog') assertEquals(deploy.value.author?.kind, 'variable', 'update catalog author ref');

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
            name: '[PARSE20] parses ALTER CATALOG with BY and a trailing insertion-point AT',
            invoke: async () => {
                const result = parseStatement("ALTER CATALOG c VERSION '1.1.0' AS (UPDATE SCHEMA shop TO LATEST ON g) BY $admin AT {#cut};");
                assertTrue(result.ok, 'ALTER CATALOG with BY and trailing AT should parse');
                if (!result.ok || result.value.kind !== 'alter-catalog') return;
                assertEquals(result.value.author?.kind, 'variable', 'author present');
                assertEquals(result.value.at?.kind, 'set', 'causal AT present');
                const change = result.value.changes[0];
                assertTrue(change?.kind === 'update-schema' && change.version.kind === 'latest', 'the body AT is the version, not the insertion point');
            },
        },
        {
            name: '[PARSE22] parses and lowers table-qualified correlation in allow rule predicates',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA users AS (
                      TABLE profiles (
                        keyId string PUB READONLY
                      ) ALLOW insert IF keyId = $author AND EXISTS (users.identities WHERE users.identities.keyId = profiles.keyId)
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
                assertEquals((exists.where as WhereLeaf).keyId, '$row.keyId', 'EXISTS WHERE correlates via gated table column');
            },
        },
        {
            name: '[PARSE23] rejects removed $row surface syntax',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE SCHEMA s AS (
                      TABLE t (v string PUB READONLY)
                        ALLOW insert IF EXISTS (u WHERE v = $row.v)
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
                        ALLOW insert IF EXISTS (identities WHERE keyId = keyId),
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
                        ALLOW insert IF EXISTS (caps WHERE label = 'manager')
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
                const group = catalogGroup('TABLEGROUP g USING SCHEMA s ALLOW DEPLOY IF keyId = $author');
                const scope = { columnsOf: () => undefined };
                let threw = false;
                try {
                    lowerRestrictionPredicate(group.canDeploy!, scope);
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
            name: '[PARSE29] parses SEED on CREATE DATABASE and CREATE CATALOG',
            invoke: async () => {
                const db = parseStatement("CREATE DATABASE app USING CATALOG c SEED 'db-seed';");
                assertTrue(db.ok && db.value.kind === 'create-database', 'database parse');
                if (db.ok && db.value.kind === 'create-database') assertEquals(db.value.seed, 'db-seed', 'database seed');

                const catalog = parseStatement("CREATE CATALOG c VERSION '1.0.0' SEED 'c-seed' AS (TABLEGROUP g USING SCHEMA shop);");
                assertTrue(catalog.ok && catalog.value.kind === 'create-catalog', 'catalog parse');
                if (catalog.ok && catalog.value.kind === 'create-catalog') assertEquals(catalog.value.seed, 'c-seed', 'catalog seed');
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
                expectLowerThrows("EXISTS (t AS t2 WHERE t2.name = '$row.name')", 'reserved', "quoted '$row' term in EXISTS WHERE is rejected");

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
                assertEquals(json(lowerAllow("EXISTS (t AS t2 WHERE t2.name = 'x') OR n = 1")),
                    json({ p: 'or', args: [{ p: 'exists', table: 't', where: { name: 'x' } }, eq('n', 1)] }), 'an EXISTS ends at its closing parenthesis');

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
                          AND "table"."escape" LIKE 'x%' AND EXISTS ("table" AS t WHERE t."identity" = "table"."identity")
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
                const text = `JSON '{"k": ["v", "it''s", "a\\\\nb"], "n": [1, 2.5]}'`;
                const same = (a: unknown, what: string) =>
                    assertEquals(JSON.stringify(a), JSON.stringify(doc), what);

                const schema = parseStatement(`CREATE SCHEMA s AS (
                    TABLE docs ("json" json PUB READONLY DEFAULT ${text})
                      ALLOW insert IF EXISTS (docs AS d WHERE d."json" = ${text})
                );`);
                assertTrue(schema.ok, `schema parses: ${JSON.stringify(schema.ok ? '' : schema.diagnostics.map((d) => d.message))}`);
                if (!schema.ok || schema.value.kind !== 'create-schema') return;
                const def = compileTable(schema.value.tables[0]);
                same(def.columns['json'].default, 'DEFAULT JSON literal');
                const rule = def.restrictions?.[0].rule;
                same(rule?.p === 'exists' ? (rule.where as WhereLeaf)['json'] : undefined, 'EXISTS where-value JSON literal');

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
        {
            name: '[PARSE40] parses CREATE CATALOG: creators, version, params, groups, comma bindings, param rows',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE CATALOG editor CREATORS ($dev) VERSION '1.0.0'
                      PARAMS (:admin identity, :limit integer)
                    AS (
                      TABLEGROUP user USING SCHEMA hhs:user AT LATEST
                        USING IDENTITIES identities
                        WITH ROWS (identities (keyId = :admin, publicKey = publicKey(:admin), name = 'Admin'),
                                   caps (label = 'manager', grantee = :admin)),
                      TABLEGROUP doc USING SCHEMA hhs:doc AT LATEST
                        BIND user => user, other => #abc USING IDENTITIES user.identities
                        ALLOW UPDATE REF user IF EXISTS (caps WHERE caps.grantee = $author)
                        ALLOW DEPLOY IF EXISTS (user.caps WHERE caps.grantee = $author)
                    ) NOTE 'initial' BY $dev;
                `);
                assertTrue(result.ok, `parse should succeed: ${JSON.stringify(result.ok ? '' : result.diagnostics.map((d) => d.message))}`);
                if (!result.ok || result.value.kind !== 'create-catalog') return;
                const stmt = result.value;
                assertEquals(stmt.name, 'editor', 'catalog name');
                assertEquals(stmt.version, '1.0.0', 'release version');
                assertEquals(stmt.params.map((p) => `${p.name}:${p.type}`).join(','), 'admin:identity,limit:integer', 'param decls');
                assertEquals(stmt.groups.map((g) => g.name).join(','), 'user,doc', 'two groups');
                assertEquals(stmt.note, 'initial', 'note');
                assertEquals(stmt.author?.kind, 'variable', 'author');
                const [user, doc] = stmt.groups;
                const row = user.initialRows[0];
                assertEquals(row.values[0].value.kind, 'param', ':admin is a param');
                const pk = row.values[1].value;
                assertTrue(pk.kind === 'call' && pk.args[0].kind === 'param', 'publicKey(:admin) takes a param');
                assertEquals(doc.bindings.map((b) => `${b.name}=>${b.group.kind}`).join(','), 'user=>name,other=>hash',
                    'a comma continues the BIND list when another alias => follows');
                assertEquals(doc.idProvider, 'user.identities', 'the clause after the bindings still parses');
                assertTrue(doc.canDeploy !== undefined && doc.canObserve.length === 1, 'deploy and observe gates');
            },
        },
        {
            name: '[PARSE41] parses ALTER CATALOG; its trailing AT takes hashes or LATEST, never a version string',
            invoke: async () => {
                const result = parseStatement(`
                    ALTER CATALOG editor VERSION '1.1.0'
                      PARAMS (:support identity)
                    AS (
                      UPDATE SCHEMA hhs:doc TO LATEST ON doc,
                      ADD TABLEGROUP comments USING SCHEMA hhs:comments AT LATEST
                        BIND doc => doc, user => user USING IDENTITIES user.identities
                    ) NOTE 'comments' BY $dev AT {#h1, #h2};
                `);
                assertTrue(result.ok, `parse should succeed: ${JSON.stringify(result.ok ? '' : result.diagnostics.map((d) => d.message))}`);
                if (!result.ok || result.value.kind !== 'alter-catalog') return;
                assertEquals(result.value.changes.map((c) => c.kind).join(','), 'update-schema,add-group', 'body items');
                assertTrue(result.value.at?.kind === 'set' && result.value.at.members.length === 2, 'the insertion point is a hash set');
                assertEquals(result.value.params[0]?.name, 'support', 'new param');

                const latest = parseStatement("ALTER CATALOG editor VERSION '1.2.0' AT LATEST;");
                assertTrue(latest.ok && latest.value.kind === 'alter-catalog' && latest.value.at?.kind === 'latest'
                    && latest.value.changes.length === 0, 'AT LATEST, and a release with no body');

                const semver = parseMessages("ALTER CATALOG editor VERSION '1.2.0' AS (UPDATE SCHEMA s TO LATEST ON g) AT '1.1.0';");
                assertTrue(semver.some((m) => m.includes('not a version string')), `a semver insertion point is a parse error: ${semver}`);
                assertTrue(parseMessages("ALTER CATALOG editor AS (UPDATE SCHEMA s TO LATEST ON g);").some((m) => m.includes('requires VERSION')),
                    'a release needs a version');
            },
        },
        {
            name: '[PARSE42] parses CREATE DATABASE USING CATALOG with a release selection, creators, params and BY',
            invoke: async () => {
                const result = parseStatement(`
                    CREATE DATABASE editor_prod USING CATALOG editor AT '1.0.0'
                      CREATORS ($santi) WITH PARAMS (:admin = $santi, :limit = 10) BY $santi;
                `);
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'create-database') return;
                assertEquals(result.value.release?.kind, 'semver', 'semver selection');
                assertEquals(result.value.params.map((p) => p.name).join(','), 'admin,limit', 'params');
                assertEquals(result.value.creators.length, 1, 'creators');
                assertEquals(result.value.author?.kind, 'variable', 'author');

                const byHash = parseStatement('CREATE DATABASE d USING CATALOG #cat AT {#rel};');
                assertTrue(byHash.ok && byHash.value.kind === 'create-database' && byHash.value.release?.kind === 'version'
                    && byHash.value.catalog.kind === 'hash', 'hash catalog and release');
                const noRelease = parseStatement('CREATE DATABASE d USING CATALOG editor;');
                assertTrue(noRelease.ok && noRelease.value.kind === 'create-database' && noRelease.value.release === undefined,
                    'the release defaults to the latest one');
                assertTrue(parseMessages('CREATE DATABASE d CREATORS ($a);').some((m) => m.includes('requires USING CATALOG')),
                    'a database needs a catalog');
            },
        },
        {
            name: '[PARSE43] parses UPDATE CATALOG ... ON and USE DATABASE',
            invoke: async () => {
                const result = parseStatement("UPDATE CATALOG editor TO '1.1.0' ON editor_prod WITH PARAMS (:support = $bob) NOTE 'go' BY $santi;");
                assertTrue(result.ok, 'parse should succeed');
                if (!result.ok || result.value.kind !== 'update-catalog') return;
                assertTrue(result.value.release.kind === 'semver' && result.value.release.version === '1.1.0', 'release selection');
                assertEquals(result.value.params[0]?.name, 'support', 'params');
                assertEquals(result.value.note, 'go', 'note');
                assertTrue(result.value.database.kind === 'name' && result.value.database.text === 'editor_prod', 'target database');

                const latest = parseStatement('UPDATE CATALOG editor TO LATEST ON #db;');
                assertTrue(latest.ok && latest.value.kind === 'update-catalog' && latest.value.release.kind === 'version', 'LATEST');

                const use = parseStatement('USE DATABASE editor_prod;');
                assertTrue(use.ok && use.value.kind === 'use-database', 'USE DATABASE');
            },
        },
        {
            name: '[PARSE44] removed statements point at their catalog replacements',
            invoke: async () => {
                const expect = (sql: string, part: string) => {
                    const messages = parseMessages(sql);
                    assertTrue(messages.some((m) => m.includes(part)), `${sql}: expected '${part}', got ${JSON.stringify(messages)}`);
                };
                expect('CREATE TABLEGROUP g USING SCHEMA s;', 'CREATE TABLEGROUP was removed');
                expect('ADD SCHEMA s TO db;', 'ADD SCHEMA and ADD TABLEGROUP were removed');
                expect('ADD TABLEGROUP g TO db;', 'ADD SCHEMA and ADD TABLEGROUP were removed');
                expect('UPDATE SCHEMA s TO LATEST ON g;', 'UPDATE CATALOG ... ON database');
                const next = parseStatement("UPDATE shop.products SET name = 'x' WHERE rowId = 'r';");
                assertTrue(next.ok, 'a table named after a statement keyword still updates');
            },
        },
        {
            name: '[PARSE45] three-part names and :param tokens',
            invoke: async () => {
                const insert = parseStatement("INSERT INTO prod.shop.products (sku) VALUES ('A');");
                assertTrue(insert.ok && insert.value.kind === 'insert', 'db.group.table parses');
                if (insert.ok && insert.value.kind === 'insert') {
                    const ref = insert.value.table;
                    assertTrue(ref.database?.kind === 'name' && ref.database.text === 'prod', 'database part');
                    assertTrue(ref.group?.kind === 'name' && ref.group.text === 'shop', 'group part');
                    assertEquals(ref.table, 'products', 'table part');
                }
                const observe = parseStatement('UPDATE REF users TO LATEST ON prod.docs;');
                assertTrue(observe.ok && observe.value.kind === 'update-ref' && observe.value.group.kind === 'name'
                    && observe.value.group.parts.join('/') === 'prod/docs', 'db.group target');
                assertTrue(parseMessages('SELECT * FROM a.b.c.d;').some((m) => m.includes('[db.]group.table')), 'four parts are rejected');

                const params = parseStatement("CREATE CATALOG c VERSION '1.0.0' PARAMS (:who identity) AS (TABLEGROUP g USING SCHEMA s WITH ROWS (t (x = :who)));");
                assertTrue(params.ok, ':param lexes in WITH ROWS and PARAMS');
                assertTrue(!parseStatement('SELECT * FROM t WHERE x = : y;').ok, 'a bare colon is still unexpected');
            },
        },
        {
            name: '[PARSE46] VERSION on CREATE SCHEMA and ALTER SCHEMA is optional and parses before AS',
            invoke: async () => {
                const versioned = parseStatement("CREATE SCHEMA shop CREATORS ($dev) VERSION '1.2.0' AS (TABLE t (x string));");
                assertTrue(versioned.ok && versioned.value.kind === 'create-schema', 'CREATE SCHEMA with VERSION parses');
                if (versioned.ok && versioned.value.kind === 'create-schema') assertEquals(versioned.value.version, '1.2.0', 'schema version');

                const unversioned = parseStatement('CREATE SCHEMA shop AS (TABLE t (x string));');
                assertTrue(unversioned.ok && unversioned.value.kind === 'create-schema' && unversioned.value.version === undefined,
                    'CREATE SCHEMA without VERSION parses with no version');

                const alter = parseStatement("ALTER SCHEMA shop VERSION '1.3.0' AS (ADD COLUMN t.y string NULL) NOTE 'n' BY $dev;");
                assertTrue(alter.ok && alter.value.kind === 'alter-schema', 'ALTER SCHEMA with VERSION parses');
                if (alter.ok && alter.value.kind === 'alter-schema') {
                    assertEquals(alter.value.version, '1.3.0', 'update version');
                    assertEquals(alter.value.note, 'n', 'trailing clauses still parse');
                }
                const bare = parseStatement('ALTER SCHEMA shop AS (ADD COLUMN t.y string NULL);');
                assertTrue(bare.ok && bare.value.kind === 'alter-schema' && bare.value.version === undefined,
                    'ALTER SCHEMA without VERSION parses with no version');
            },
        },
        {
            name: '[PARSE47] source mode: CREATE CATALOG may omit VERSION only with catalogVersionOptional',
            invoke: async () => {
                const sql = 'CREATE CATALOG c AS (TABLEGROUP g USING SCHEMA s);';
                assertTrue(parseMessages(sql).some((m) => m.includes('requires VERSION')), 'by default a catalog needs a version');

                const source = parseStatement(sql, { catalogVersionOptional: true });
                assertTrue(source.ok && source.value.kind === 'create-catalog' && source.value.version === undefined,
                    'with the option the statement parses with no version');

                const explicit = parseStatement("CREATE CATALOG c VERSION '2.0.0' AS (TABLEGROUP g USING SCHEMA s);", { catalogVersionOptional: true });
                assertTrue(explicit.ok && explicit.value.kind === 'create-catalog' && explicit.value.version === '2.0.0',
                    'an explicit version still parses under the option');
            },
        },
        {
            name: '[PARSE48] CREATE CATALOG carries the span of its VERSION clause',
            invoke: async () => {
                const sql = "CREATE CATALOG c CREATORS ($dev) VERSION '2.0.0' AS (TABLEGROUP g USING SCHEMA s);";
                const stated = parseStatement(sql);
                assertTrue(stated.ok && stated.value.kind === 'create-catalog', 'it parses');
                if (!stated.ok || stated.value.kind !== 'create-catalog') return;
                const span = stated.value.versionSpan!;
                assertEquals(sql.slice(span.start, span.end), "VERSION '2.0.0'", 'the span covers the keyword and the string');

                const omitted = parseStatement('CREATE CATALOG c AS (TABLEGROUP g USING SCHEMA s);', { catalogVersionOptional: true });
                assertTrue(omitted.ok && omitted.value.kind === 'create-catalog' && omitted.value.versionSpan === undefined, 'no clause, no span');
            },
        },
        {
            name: '[PARSE49] parses FILES items in CREATE CATALOG and ADD FILES in ALTER CATALOG; FILES and WRITE are contextual',
            invoke: async () => {
                const created = parseStatement(`CREATE CATALOG c VERSION '1.0.0' AS (
                    TABLEGROUP user USING SCHEMA s USING IDENTITIES identities,
                    FILES media USING IDENTITIES user.identities
                      ALLOW WRITE IF EXISTS (user.caps WHERE user.caps.label = 'writer' AND user.caps.grantee = $author),
                    FILES files BIND u => #ab12cd34 USING IDENTITIES u.identities ALLOW WRITE IF true
                );`);
                assertTrue(created.ok, `CREATE CATALOG with FILES parses: ${created.ok ? '' : created.diagnostics.map((d) => d.message).join('; ')}`);
                if (!created.ok || created.value.kind !== 'create-catalog') return;
                assertEquals(created.value.groups.length, 1, 'one group');
                const [media, files] = created.value.files;
                assertEquals(media.name, 'media', 'the first FILES');
                assertTrue(media.binding === undefined, 'no BIND');
                assertEquals(media.idProvider, 'user.identities', 'the identity table');
                assertEquals(media.canWrite.kind, 'exists', 'the write predicate');
                assertEquals(files.name, 'files', 'files is a valid FILES name');
                assertEquals(files.binding?.name, 'u', 'the BIND alias');
                assertTrue(files.binding?.group.kind === 'hash' && files.binding.group.prefix === 'ab12cd34', 'the BIND target by hash');
                assertEquals(files.canWrite.kind, 'true', 'ALLOW WRITE IF true');

                const altered = parseStatement(`ALTER CATALOG c VERSION '1.1.0' AS (
                    ADD FILES attachments USING IDENTITIES user.identities ALLOW WRITE IF true,
                    ADD TABLEGROUP files USING SCHEMA s
                ) BY $dev;`);
                assertTrue(altered.ok && altered.value.kind === 'alter-catalog', 'ALTER CATALOG with ADD FILES parses');
                if (!altered.ok || altered.value.kind !== 'alter-catalog') return;
                const [addFiles, addGroup] = altered.value.changes;
                assertTrue(addFiles.kind === 'add-files' && addFiles.files.name === 'attachments', 'ADD FILES');
                assertTrue(addGroup.kind === 'add-group' && addGroup.group.name === 'files', 'files stays a group name');

                const names = parseStatement('CREATE SCHEMA files AS (TABLE files (write string, files string) ALLOW all IF true);');
                assertTrue(names.ok, 'files and write stay valid schema, table and column names');
                const select = parseStatement('SELECT write, files FROM files.files WHERE write = files;');
                assertTrue(select.ok, 'and valid in queries');
            },
        },
        {
            name: '[PARSE50] FILES requires USING IDENTITIES and ALLOW WRITE IF, and binds one group',
            invoke: async () => {
                const item = (body: string) => parseMessages(`CREATE CATALOG c VERSION '1.0.0' AS (FILES m ${body});`);
                assertTrue(item('ALLOW WRITE IF true').some((m) => m.includes('FILES m requires USING IDENTITIES')), 'no identity table');
                assertTrue(item('USING IDENTITIES u.identities').some((m) => m.includes('FILES m requires ALLOW WRITE IF')), 'no write predicate');
                assertTrue(item('BIND a => x, b => y USING IDENTITIES a.identities ALLOW WRITE IF true')
                    .some((m) => m.includes('FILES binds exactly one TABLEGROUP')), 'a second binding');
                assertTrue(item('USING IDENTITIES u.identities ALLOW DEPLOY IF true').some((m) => m.includes('Expected ALLOW WRITE IF')),
                    'ALLOW takes WRITE on a FILES');
                assertTrue(parseMessages("ALTER CATALOG c VERSION '1.1.0' AS (DROP x);").some((m) => m.includes('ADD FILES')),
                    'the ALTER CATALOG error names ADD FILES');
            },
        },
        {
            name: '[PARSE51] parses PUT, GET and LIST on FILES; their words stay valid names',
            invoke: async () => {
                const put = parseStatement("PUT FILE 'a/b.png' INTO app.media AT 'pics/b.png' IN KEY BY $admin;");
                assertTrue(put.ok && put.value.kind === 'put-file', 'PUT FILE parses');
                if (!put.ok || put.value.kind !== 'put-file') return;
                assertEquals(`${put.value.source.kind} ${put.value.files.text} ${put.value.at} ${put.value.section} ${put.value.author?.kind}`,
                    'file app.media pics/b.png key variable', 'its clauses');
                const str = parseStatement("PUT STRING 'hi' INTO media AT 'x.txt';");
                assertTrue(str.ok && str.value.kind === 'put-file' && str.value.source.kind === 'string' && str.value.section === 'common', 'PUT STRING');
                const b64 = parseStatement("PUT B64 'AAE=' INTO media AT 'x.bin' IN COMMON;");
                assertTrue(b64.ok && b64.value.kind === 'put-file' && b64.value.source.kind === 'b64', 'PUT B64');
                assertTrue(parseMessages("PUT STRING 'hi' INTO media;").some((m) => m.includes("PUT STRING needs AT 'path'")), 'STRING needs AT');
                assertTrue(parseMessages("PUT DATA 'hi' INTO media AT 'x';").some((m) => m.includes('FILE, STRING or B64')), 'an unknown source');

                const get = parseStatement("GET 'x.txt' FROM media IN KEY $alice HASH 'dPu+' TO 'out/x.txt';");
                assertTrue(get.ok && get.value.kind === 'get-file', 'GET parses');
                if (!get.ok || get.value.kind !== 'get-file') return;
                assertEquals(`${get.value.section} ${get.value.owner?.kind} ${get.value.hash} ${get.value.to} ${get.value.asB64}`,
                    'key variable dPu+ out/x.txt false', 'its clauses');
                const inline = parseStatement("GET 'x.bin' FROM media IN KEY AS B64;");
                assertTrue(inline.ok && inline.value.kind === 'get-file' && inline.value.asB64 && inline.value.owner?.kind === 'current', 'GET AS B64, bare IN KEY');
                assertTrue(parseMessages("GET 'x' FROM media AS B64 TO 'y';").some((m) => m.includes('AS B64 is for inline output')), 'AS B64 with TO');

                const list = parseStatement("LIST 'docs' FROM media IN KEY #ab12;");
                assertTrue(list.ok && list.value.kind === 'list-files' && list.value.prefix === 'docs' && list.value.owner?.kind === 'hash', 'LIST parses');
                const bare = parseStatement('LIST FROM media;');
                assertTrue(bare.ok && bare.value.kind === 'list-files' && bare.value.prefix === undefined && bare.value.section === undefined, 'a bare LIST');

                assertTrue(parseStatement('CREATE SCHEMA s AS (TABLE put (get string, list string, key string, common string) ALLOW all IF true);').ok,
                    'put, get, list, key and common stay valid names');
                assertTrue(parseStatement('SELECT get, list FROM g.put WHERE key = common;').ok, 'and valid in queries');
            },
        },
        {
            name: '[PARSE52] EXISTS is delimited: the form without parentheses is a parse error that shows the new one, and a nested EXISTS is a syntax error',
            invoke: async () => {
                const sql = (predicate: string) =>
                    `CREATE SCHEMA s AS (TABLE t (name string READONLY) ALLOW insert IF ${predicate}, TABLE caps (label string PUB, grantee string PUB));`;
                const rule = (predicate: string) => parseMessages(sql(predicate));
                const ruleOk = (predicate: string) => parseStatement(sql(predicate)).ok;

                assertTrue(ruleOk("EXISTS (caps WHERE label = 'writer' AND grantee = $author)"), 'the delimited form parses');
                assertTrue(ruleOk("EXISTS (caps AS c WHERE c.label = 'writer') OR EXISTS (caps AS c WHERE c.label = 'manager')"),
                    'the parenthesis that matches EXISTS ( ends the atom: no outer parentheses are needed in a group');
                assertTrue(ruleOk("EXISTS (caps WHERE (label = 'writer') AND (grantee = $author))"), 'a parenthesis inside the atom groups');
                assertTrue(ruleOk("name = 'x' AND EXISTS (caps WHERE label = 'writer') AND name = 'y'"), 'the condition goes on after the atom');

                const old = rule("EXISTS caps WHERE label = 'writer' AND grantee = $author");
                assertTrue(old.some((m) => m === 'EXISTS takes its table and filter in parentheses: write EXISTS (caps WHERE ...)'),
                    `today's form is an error that shows the new one, got ${JSON.stringify(old)}`);
                assertEquals(old.length, 1, 'and it is the only error: the old form is still read to its end');
                const oldAlias = rule("EXISTS caps AS c WHERE c.label = 'writer'");
                assertTrue(oldAlias.some((m) => m.includes('write EXISTS (caps AS c WHERE ...)')), `the alias is shown too, got ${JSON.stringify(oldAlias)}`);
                assertTrue(rule("(EXISTS caps WHERE label = 'writer') OR name = 'x'").some((m) => m.includes('write EXISTS (caps WHERE ...)')),
                    'also inside parentheses');

                assertTrue(rule("EXISTS (caps WHERE label = 'writer' AND EXISTS (caps WHERE grantee = $author))")
                    .some((m) => m === 'EXISTS cannot appear inside the filter of another EXISTS'), 'a nested EXISTS is a syntax error');
                assertTrue(rule("EXISTS (caps WHERE label = 'writer'").length > 0, 'an unclosed EXISTS is an error');
                assertTrue(rule('EXISTS (caps)').some((m) => m.includes('EXISTS requires a WHERE clause')), 'an EXISTS with no filter is an error');

                // IN is an operator only inside an EXISTS filter
                assertTrue(ruleOk("EXISTS (caps WHERE label IN ('writer', 'manager'))"), 'IN parses inside a filter');
                assertTrue(rule("name IN ('a', 'b')").length > 0, 'IN outside an EXISTS is not an operator');
                assertTrue(parseStatement('CREATE SCHEMA s AS (TABLE "in" (in string PUB, x string) ALLOW insert IF EXISTS (u.t WHERE in = x));').ok,
                    '`in` stays a valid name, also as the first word of a filter term');
                assertTrue(parseStatement("SELECT in FROM g.t WHERE in = 'x';").ok, 'and valid in queries');
                assertTrue(parseMessages("SELECT * FROM g.t WHERE name IN ('a', 'b');").length > 0, 'a query does not take IN');
            },
        },
        {
            name: '[PARSE53] an EXISTS filter lowers to a where tree: a conjunction is one leaf, OR and IN are groups, and parentheses keep their nesting',
            invoke: async () => {
                const where = (filter: string) => {
                    const lowered = lowerGated(`EXISTS (grants WHERE ${filter})`);
                    if (lowered.p !== 'exists') throw new Error('expected an exists atom');
                    return JSON.stringify(lowered.where);
                };
                const same = (filter: string, expected: Where, what: string) => assertEquals(where(filter), JSON.stringify(expected), what);

                same("label = 'writer'", { label: 'writer' }, 'an equality is a one-field leaf');
                same("'writer' = label", { label: 'writer' }, 'either way round');
                same("label = 'writer' AND grantee = $author", { label: 'writer', grantee: '$author' }, 'a conjunction of equalities is one leaf');
                same("(label = 'writer' AND grantee = $author)", { label: 'writer', grantee: '$author' }, 'also in parentheses');
                same("label = 'writer' AND (grantee = $author AND scope = docs.area)", { label: 'writer', grantee: '$author', scope: '$row.area' },
                    'and when part of it is parenthesized: it is still one leaf');
                same("label = 'writer' OR label = 'manager'", { $or: [{ label: 'writer' }, { label: 'manager' }] }, 'an OR is an $or of its members');
                same("label = 'writer' AND grantee = $author OR label = 'manager'",
                    { $or: [{ label: 'writer', grantee: '$author' }, { label: 'manager' }] }, 'AND binds tighter than OR');
                same("grantee = $author AND (label = 'writer' OR scope = 'all')",
                    { $and: [{ grantee: '$author' }, { $or: [{ label: 'writer' }, { scope: 'all' }] }] }, 'a group beside a leaf, under an AND');
                same("(label = 'writer' OR scope = 'all') AND grantee = $author AND grants.resource = docs.resource",
                    { $and: [{ $or: [{ label: 'writer' }, { scope: 'all' }] }, { grantee: '$author', resource: '$row.resource' }] },
                    'the leaf of an AND sits where its first equality is');
                same("grantee = $author AND (label = 'writer' OR scope = 'all') AND grants.resource = docs.resource",
                    { $and: [{ grantee: '$author', resource: '$row.resource' }, { $or: [{ label: 'writer' }, { scope: 'all' }] }] },
                    'equalities on both sides of a group share the one leaf');
                same("(label = 'a' OR label = 'b') AND (scope = 'x' OR scope = 'y')",
                    { $and: [{ $or: [{ label: 'a' }, { label: 'b' }] }, { $or: [{ scope: 'x' }, { scope: 'y' }] }] }, 'an AND of two groups has no leaf');
                same("(label = 'a' OR label = 'b') OR scope = 'x'",
                    { $or: [{ $or: [{ label: 'a' }, { label: 'b' }] }, { scope: 'x' }] }, 'a parenthesized OR keeps its nesting');
                same("label = 'a' OR label = 'b' OR scope = 'x'",
                    { $or: [{ label: 'a' }, { label: 'b' }, { scope: 'x' }] }, 'an unparenthesized chain is one group');
                same("(grantee = $author AND (label = 'a' OR label = 'b')) AND (scope = 'x' OR scope = 'y')",
                    { $and: [{ $and: [{ grantee: '$author' }, { $or: [{ label: 'a' }, { label: 'b' }] }] }, { $or: [{ scope: 'x' }, { scope: 'y' }] }] },
                    'a parenthesized AND that holds a group keeps its nesting');

                // the example rule of HIP-21
                same("grantee = $author AND label IN ('writer', 'manager') AND grants.resource IN (docs.resource, '*')",
                    { $and: [
                        { grantee: '$author' },
                        { $or: [{ label: 'writer' }, { label: 'manager' }] },
                        { $or: [{ resource: '$row.resource' }, { resource: '*' }] },
                    ] }, 'IN lists are groups beside the leaf');

                // what a filter does not take
                for (const filter of ["label != 'x'", "scope < 'x'", "label LIKE 'x%'", "NOT label = 'x'", 'true', "label = 'x' AND scope > 'a'"]) {
                    expectThrowsWith(() => where(filter), 'EXISTS WHERE supports only = and IN, combined with AND / OR', `${filter} is not a filter term`);
                }
                expectThrowsWith(() => where("label = 'a' + 'b'"), 'EXISTS WHERE value must be a literal', 'a value is not an expression');
                expectThrowsWith(() => where("docs.kind = 'x'"), "EXISTS WHERE filter must reference the EXISTS table, not 'docs.kind'",
                    'a comparison with no column of the EXISTS table');
                expectThrowsWith(() => where("'x' = 'y'"), 'EXISTS WHERE filter must reference the EXISTS table', 'two values');
                expectThrowsWith(() => where('grants.label = grants.scope'), 'EXISTS WHERE cannot compare two EXISTS table columns', 'two columns of the EXISTS table');
            },
        },
        {
            name: '[PARSE54] IN in an EXISTS filter: one value is the equality, a repeated value gives equal members, a list holds every kind of value, and each renders back as IN',
            invoke: async () => {
                const lower = (filter: string) => {
                    const lowered = lowerGated(`EXISTS (grants WHERE ${filter})`, [':tier']);
                    if (lowered.p !== 'exists') throw new Error('expected an exists atom');
                    return lowered;
                };
                const check = (filter: string, expected: Where, rendered: string, what: string) => {
                    const lowered = lower(filter);
                    assertEquals(JSON.stringify(lowered.where), JSON.stringify(expected), what);
                    const text = formatPredicate(lowered, { gatedTable: 'docs' });
                    assertEquals(text, `EXISTS (grants WHERE ${rendered})`, `${what}: rendered`);
                    const back = lowerGated(text, [':tier']);
                    assertEquals(JSON.stringify(back), JSON.stringify(lowered), `${what}: the rendered text lowers back to the same payload`);
                };

                check("label IN ('writer')", { label: 'writer' }, "grants.label = 'writer'", 'a list of one value is the equality');
                check("label IN ('writer', 'manager')", { $or: [{ label: 'writer' }, { label: 'manager' }] },
                    "grants.label IN ('writer', 'manager')", 'a list is an $or of one-field leaves');
                check("label IN ('writer', 'writer')", { $or: [{ label: 'writer' }, { label: 'writer' }] },
                    "grants.label IN ('writer', 'writer')", 'a repeated value gives two equal members');
                check("grantee IN ($author, 'public', :tier, docs.owner)",
                    { $or: [{ grantee: '$author' }, { grantee: 'public' }, { grantee: '$param.tier' }, { grantee: '$row.owner' }] },
                    "grants.grantee IN ($author, 'public', :tier, docs.owner)", 'a list holds a value of each kind');
                check("level IN (1, -2, 3)", { $or: [{ level: 1 }, { level: -2 }, { level: 3 }] }, 'grants.level IN (1, -2, 3)', 'numbers, a negative one included');
                check("label IN ('a', 'b') OR scope = 'x'", { $or: [{ $or: [{ label: 'a' }, { label: 'b' }] }, { scope: 'x' }] },
                    "grants.label IN ('a', 'b') OR grants.scope = 'x'", 'under an OR, IN is a group of its own');
                check("label = 'a' OR label = 'b' OR scope = 'x'", { $or: [{ label: 'a' }, { label: 'b' }, { scope: 'x' }] },
                    "grants.label = 'a' OR grants.label = 'b' OR grants.scope = 'x'", 'which the plain chain is not');
                check("label = 'a' OR label = 'b'", { $or: [{ label: 'a' }, { label: 'b' }] },
                    "grants.label IN ('a', 'b')", 'an OR of equalities on one column is the same payload as IN, and renders as IN');
                check("grantee = $author AND label IN ('a', 'b')", { $and: [{ grantee: '$author' }, { $or: [{ label: 'a' }, { label: 'b' }] }] },
                    "grants.grantee = $author AND grants.label IN ('a', 'b')", 'under an AND, IN is a group beside the leaf');
                check("grantee = $author AND label IN ('a')", { grantee: '$author', label: 'a' },
                    "grants.grantee = $author AND grants.label = 'a'", 'and a one-value IN joins the leaf');
                check("label IN ('a', 'b') AND scope IN ('x', 'y')", { $and: [{ $or: [{ label: 'a' }, { label: 'b' }] }, { $or: [{ scope: 'x' }, { scope: 'y' }] }] },
                    "grants.label IN ('a', 'b') AND grants.scope IN ('x', 'y')", 'two lists under an AND');
                check("label = 'a' AND label IN ('b', 'c')", { $and: [{ label: 'a' }, { $or: [{ label: 'b' }, { label: 'c' }] }] },
                    "grants.label = 'a' AND grants.label IN ('b', 'c')", 'a list on a column the leaf names is a group, not a second value of the leaf');

                expectThrowsWith(() => lower("$author IN (grantee, scope)"), 'EXISTS WHERE IN takes a column of the EXISTS table on its left', 'a value on the left');
                expectThrowsWith(() => lower("docs.owner IN ('a', 'b')"), "EXISTS WHERE filter must reference the EXISTS table, not 'docs.owner'", 'a gated column on the left');
                expectThrowsWith(() => lower('label IN (grants.scope, grants.grantee)'), 'EXISTS WHERE correlation must reference the gated table', 'EXISTS columns in the list');
                expectThrowsWith(() => lower("label IN ('a', 1 + 2)"), 'EXISTS WHERE value must be a literal', 'an expression in the list');
            },
        },
        {
            name: '[PARSE55] one rule for names in an EXISTS filter: a bare name must belong to exactly one of the two tables',
            invoke: async () => {
                // docs (owner, kind, resource) is gated; caps (label, owner, grantee) and docs itself are local targets
                const lower = (predicate: string) => lowerGated(predicate);
                const leaf = (predicate: string) => {
                    const lowered = lower(predicate);
                    return lowered.p === 'exists' ? JSON.stringify(lowered.where) : 'not an exists';
                };

                assertEquals(leaf("EXISTS (caps WHERE label = 'admin' AND grantee = $author)"), JSON.stringify({ label: 'admin', grantee: '$author' }),
                    'names only the EXISTS table has are its columns');
                assertEquals(leaf('EXISTS (caps WHERE grantee = kind)'), JSON.stringify({ grantee: '$row.kind' }),
                    'a name only the gated table has is a value');
                assertEquals(leaf('EXISTS (caps WHERE caps.owner = docs.owner)'), JSON.stringify({ owner: '$row.owner' }), 'qualified names are never ambiguous');

                expectThrowsWith(() => lower("EXISTS (caps WHERE caps.label = 'admin' AND owner = $author)"),
                    "ambiguous column 'owner'; qualify with table name (exists in both 'docs' and 'caps')",
                    'a bare name both tables have, as the column of a comparison');
                expectThrowsWith(() => lower("EXISTS (caps WHERE caps.label = 'admin' OR owner = $author)"), "ambiguous column 'owner'", 'also under an OR');
                expectThrowsWith(() => lower("EXISTS (caps WHERE owner IN ($author, 'x'))"), "ambiguous column 'owner'", 'and on the left of IN');
                expectThrowsWith(() => lower('EXISTS (caps WHERE caps.grantee = owner)'), "ambiguous column 'owner'", 'and as a value');
                expectThrowsWith(() => lower("EXISTS (caps WHERE caps.label = 'admin' AND kind = 'x')"),
                    "EXISTS WHERE filter must reference the EXISTS table: 'kind' is a column of 'docs', not of 'caps'",
                    'a bare name only the gated table has is not a filter on the target');
                expectThrowsWith(() => lower("EXISTS (caps WHERE kind IN ('x', 'y'))"), "'kind' is a column of 'docs', not of 'caps'", 'also on the left of IN');
                expectThrowsWith(() => lower("EXISTS (caps WHERE nope = 'x')"), "unknown column 'nope'", 'a name neither table has');

                // a target in a bound group: its columns are not known here
                assertEquals(leaf("EXISTS (auth.caps WHERE label = 'admin' AND grantee = $author)"), JSON.stringify({ label: 'admin', grantee: '$author' }),
                    "a bare name the gated table lacks is the target's");
                expectThrowsWith(() => lower("EXISTS (auth.caps WHERE label = 'admin' AND owner = $author)"), "ambiguous column 'owner'; qualify with table name",
                    'a bare name the gated table has is an error when the target is in a bound group');
                assertEquals(leaf("EXISTS (auth.caps WHERE auth.caps.owner = $author AND caps.label = docs.kind)"), JSON.stringify({ owner: '$author', label: '$row.kind' }),
                    'qualified with the target, by its full or bare name, it is fine');

                // a self-referential EXISTS: the alias is required and every name is qualified
                expectThrowsWith(() => lower("EXISTS (docs WHERE owner = $author)"), 'EXISTS docs is self-referential; use AS alias', 'the alias is required');
                expectThrowsWith(() => lower("EXISTS (docs AS d WHERE owner = $author)"), "ambiguous column 'owner'", 'a bare name is in both tables');
                assertEquals(leaf("EXISTS (docs AS d WHERE d.owner = $author AND d.kind = docs.kind)"), JSON.stringify({ owner: '$author', kind: '$row.kind' }),
                    'with every name qualified it lowers');

                // a gate has no gated table, so no bare name is ambiguous
                const gate = parseStatement("SELECT * FROM g.t WHERE EXISTS (auth.caps WHERE owner = $author OR label IN ('a', 'b'));");
                assertTrue(gate.ok && gate.value.kind === 'select' && gate.value.where !== undefined, 'the gate predicate parses');
                if (!gate.ok || gate.value.kind !== 'select' || gate.value.where === undefined) return;
                const lowered = lowerRestrictionPredicate(gate.value.where, { columnsOf: () => undefined });
                assertEquals(JSON.stringify(lowered), JSON.stringify({ p: 'exists', table: 'auth.caps', where: { $or: [{ owner: '$author' }, { $or: [{ label: 'a' }, { label: 'b' }] }] } }),
                    'in a gate every bare name is the target\'s');
            },
        },
        {
            name: '[PARSE56] a column named twice in one AND of an EXISTS filter is an error that suggests the rewrite',
            invoke: async () => {
                const lower = (filter: string) => lowerGated(`EXISTS (caps WHERE ${filter})`, [':tier']);
                expectThrowsWith(() => lower('grantee = $author AND grantee = docs.owner'),
                    'EXISTS WHERE compares caps.grantee twice in one AND, to $author and to docs.owner. '
                    + 'A row has one value for a column, so this holds only when the two are equal: '
                    + 'keep one comparison and compare the values outside the EXISTS, as in '
                    + '$author = docs.owner AND EXISTS (caps WHERE caps.grantee = $author)',
                    'the error names the column and both values, and shows the rewrite');
                expectThrowsWith(() => lower("label = 'writer' AND grantee = $author AND label = 'manager'"),
                    "to 'writer' and to 'manager'", 'literals are shown as written');
                expectThrowsWith(() => lower("label = 'a' AND (grantee = $author AND label = :tier)"), "to 'a' and to :tier",
                    'a parenthesized conjunction is part of the same leaf');
                expectThrowsWith(() => lower("label = 'a' AND label IN ('b')"), "to 'a' and to 'b'", 'a one-value IN is an equality of the same leaf');
                expectThrowsWith(() => lowerGated("EXISTS (docs AS d WHERE d.owner = $author AND d.owner = docs.owner)"),
                    '$author = docs.owner AND EXISTS (docs AS d WHERE d.owner = $author)', 'the alias is kept in the suggested rewrite');

                // the suggested rewrite compiles
                const rewritten = lowerGated('docs.owner = $author AND EXISTS (caps WHERE caps.grantee = $author)');
                assertEquals(rewritten.p, 'and', 'the rewrite is a comparison beside the EXISTS');
                // and a column in two arms of an OR, or in a leaf and a list, is not named twice
                assertTrue(lower("label = 'a' OR label = 'b'").p === 'exists', 'two arms of an OR');
                assertTrue(lower("label = 'a' AND label IN ('b', 'c')").p === 'exists', 'a leaf and a list');
            },
        },
    ],
};

// Lowers one rule of `docs`, a table gated beside the local tables `caps` and
// `grants`; `auth` names a bound group, whose tables are not known here.
function lowerGated(predicate: string, params: string[] = []) {
    const result = parseStatement(`CREATE SCHEMA s AS (
        TABLE docs (owner string READONLY, kind string READONLY, resource string READONLY, area string READONLY) ALLOW insert IF ${predicate},
        TABLE caps (label string PUB, owner string PUB, grantee string PUB),
        TABLE grants (label string PUB, grantee string PUB, resource string PUB, scope string PUB, level integer PUB)
    );`);
    if (!result.ok || result.value.kind !== 'create-schema') {
        throw new Error(`parse failed for '${predicate}': ${JSON.stringify(result.ok ? result.value.kind : result.diagnostics.map((d) => d.message))}`);
    }
    const table = result.value.tables[0];
    const allow = table.options.find((o) => o.kind === 'allow-rule');
    if (allow === undefined || allow.kind !== 'allow-rule') throw new Error('no allow rule');
    return lowerRestrictionPredicate(allow.predicate, {
        gated: { name: table.name, columns: columnSetFromTableDecl(table) },
        columnsOf: columnsOfFromTableDecls(result.value.tables),
        params: new Set(params.map((p) => p.substring(1))),
    });
}

function expectThrowsWith(fn: () => unknown, messagePart: string, why: string) {
    let message = '<no error>';
    try {
        fn();
    } catch (e) {
        message = e instanceof Error ? e.message : String(e);
    }
    assertTrue(message.includes(messagePart), `${why}: expected an error mentioning '${messagePart}', got '${message}'`);
}

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

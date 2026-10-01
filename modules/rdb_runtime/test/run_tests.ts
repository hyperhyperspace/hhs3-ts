import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519, type OwnIdentity, type SigningName } from "@hyper-hyper-space/hhs3_crypto";
import { RSchemaImpl, type RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import {
    executeText,
    LanguageError,
    keyPassphraseRequiredFromError,
    KeyPassphraseRequiredError,
    MemoryKeyVault,
    MemDagBackend,
    openMemWorkspace,
    RdbRuntime,
    RdbSession,
    resolveRowIdPrefix,
    decodePublicKey,
    encodePublicKey,
    type KeyVault,
    type KeyRecord,
} from "../src/index.js";

const tests = [
    {
        name: '[RDB_RT01] openMemWorkspace attaches backend and registers types',
        invoke: async () => {
            const workspace = await openMemWorkspace();
            try {
                assertTrue(workspace.replica.getRegistry() !== undefined, 'registry present');
                const roots = workspace.roots.list();
                assertEquals(roots.length, 0, 'empty roots');
            } finally {
                await workspace.close();
            }
        },
    },
    {
        name: '[RDB_RT02] executeText creates schema and group roots',
        invoke: async () => {
            const runtime = await RdbRuntime.openMemory({ keyVault: new FakeKeyVault() });
            try {
                const session = runtime.session;
                await session.createKey('alice', 'correct');
                session.selectAuthor('alice');
                const result = await runtime.execute(setupScript());
                assertEquals(result.results.length, 5, 'five statements');
                assertEquals(session.workspace.roots.list('schema').length, 1, 'schema indexed');
                assertEquals(session.workspace.roots.list('catalog').length, 1, 'catalog indexed');
                assertEquals(session.workspace.roots.list('database').length, 1, 'database indexed');
                assertEquals(session.workspace.roots.list('group').length, 1, 'group created by the database');
                assertEquals(session.workspace.roots.list('other').length, 0, 'the deploy gate is not a root');
                assertEquals(result.results[2]?.deploy?.created.length, 1, 'CREATE DATABASE reports the created group');
            } finally {
                await runtime.close();
            }
        },
    },
    {
        name: '[RDB_RT03b] createObject without createRoot is indexed via subscribeNewRoot',
        invoke: async () => {
            const workspace = await openMemWorkspace();
            try {
                const hashSuite = workspace.replica.getHashSuite();
                const creator = await createIdentity(SIGNING_ED25519, hashSuite);
                const init = await RSchemaImpl.create({
                    name: 'fetched_schema',
                    creators: [{ keyId: creator.keyId, publicKey: creator.publicKey }],
                    tables: [{ name: 't', columns: { x: { type: 'string' } } }],
                });
                const obj = await workspace.replica.createObject(init);
                const schemas = workspace.roots.list('schema');
                assertEquals(schemas.length, 1, 'schema indexed without createRoot');
                assertEquals(schemas[0]?.name, 'fetched_schema', 'schema name from create payload');
                assertEquals(schemas[0]?.id, obj.getId(), 'indexed id matches created object');
            } finally {
                await workspace.close();
            }
        },
    },
    {
        name: '[RDB_RT03] rehydrateRoots reloads objects from the same backend',
        invoke: async () => {
            const crypto = createBasicCrypto();
            const hashSuite = crypto.hash(HASH_SHA256);
            const backend = new MemDagBackend(hashSuite);
            const keyVault = new FakeKeyVault();

            const runtime1 = await RdbRuntime.open({ backend, hashSuite, crypto, keyVault });
            await runtime1.session.createKey('alice', 'correct');
            runtime1.session.selectAuthor('alice');
            await runtime1.execute(setupScript());
            await runtime1.close();

            const runtime2 = await RdbRuntime.open({ backend, hashSuite, crypto, keyVault });
            try {
                assertEquals(runtime2.session.workspace.roots.list('schema').length, 1, 'schema rehydrated');
                assertEquals(runtime2.session.workspace.roots.list('catalog').length, 1, 'catalog rehydrated');
                assertEquals(runtime2.session.workspace.roots.list('database').length, 1, 'database rehydrated');
                assertEquals(runtime2.session.workspace.roots.list('group').length, 1, 'group rehydrated');
                assertEquals(runtime2.session.workspace.roots.list('other').length, 0, 'the deploy gate stays hidden');
                const selected = await runtime2.execute("SELECT sku, name FROM shop_prod.products;");
                assertEquals(selected.results[0]?.result.kind, 'select', 'select result');
            } finally {
                await runtime2.close();
            }
        },
    },
    {
        name: '[RDB_RT04] aliases resolve version refs',
        invoke: async () => {
            const runtime = await RdbRuntime.openMemory({ keyVault: new FakeKeyVault() });
            try {
                await runtime.session.createKey('alice', 'correct');
                runtime.session.selectAuthor('alice');
                await runtime.execute(setupScript());
                const group = runtime.session.workspace.roots.list('group')[0]!;
                runtime.session.setCurrentGroup(group.id);
                const dag = await group.object!.getScopedDag();
                const hashes: string[] = [];
                for await (const entry of dag.loadAllEntries()) hashes.push(entry.hash);
                runtime.session.aliases.set('version', 'cut', hashes[hashes.length - 1]! as import("@hyper-hyper-space/hhs3_crypto").B64Hash);
                const view = await runtime.execute('SET VIEW AT {cut};');
                assertEquals(view.results[0]?.result.kind, 'set-view', 'view set');
            } finally {
                await runtime.close();
            }
        },
    },
    {
        name: '[RDB_RT05] locked key throws KeyPassphraseRequiredError',
        invoke: async () => {
            const runtime = await RdbRuntime.openMemory({ keyVault: new FakeKeyVault() });
            try {
                await runtime.session.createKey('alice', 'correct');
                runtime.session.selectAuthor('alice');
                await runtime.execute(setupScript());

                // shop_prod has no USING IDENTITIES, so its writes are never
                // signed; a schema change always is
                const fresh = new RdbSession({ workspace: runtime.workspace, keyVault: runtime.session.keyVault });
                let required: KeyPassphraseRequiredError | undefined;
                try {
                    await executeText(fresh, 'ALTER SCHEMA shop AS (ADD COLUMN products.note string NULL) BY $alice;');
                } catch (e) {
                    required = e instanceof KeyPassphraseRequiredError
                        ? e
                        : keyPassphraseRequiredFromError(e) ?? undefined;
                }
                assertTrue(required !== undefined, 'locked key fails');
            } finally {
                await runtime.close();
            }
        },
    },
    {
        name: '[RDB_RT06] rowId prefixes resolve against table view',
        invoke: async () => {
            const runtime = await RdbRuntime.openMemory({ keyVault: new FakeKeyVault() });
            try {
                await runtime.session.createKey('alice', 'correct');
                runtime.session.selectAuthor('alice');
                await runtime.execute(setupScript());
                const group = runtime.session.workspace.roots.list('group')[0]!;
                const groupObj = group.object as RTableGroup;
                const table = await (groupObj as any).getTable('products');
                const rowIds = await (await table.getView()).liveRowIds();
                assertEquals(rowIds.length, 1, 'one row');
                const at = await (await table.getScopedDag()).getFrontier();
                const resolved = await resolveRowIdPrefix(
                    rowIds[0]!.slice(0, 8),
                    {
                        groupId: group.id,
                        group: groupObj,
                        tableName: 'products',
                        table,
                    },
                    at,
                );
                assertEquals(resolved, rowIds[0], 'prefix resolves');
            } finally {
                await runtime.close();
            }
        },
    },
    {
        name: '[RDB_RT07] ref-auto-update emits structured events',
        invoke: async () => {
            const runtime = await RdbRuntime.openMemory({ keyVault: new FakeKeyVault(), refAutoUpdate: 'auto' });
            try {
                await runtime.session.createKey('alice', 'correct');
                runtime.session.selectAuthor('alice');
                await runtime.execute(crossGroupSetupScript());
                const inserted = await runtime.execute("INSERT INTO users.identities (name) VALUES ('ada');");
                const events = inserted.results[0]?.refUpdates ?? [];
                assertTrue(events.some((e) => e.kind === 'updated'), 'ref update event');
            } finally {
                await runtime.close();
            }
        },
    },
    {
        name: '[RDB_RT08] session aliases are isolated per session',
        invoke: async () => {
            const runtime = await RdbRuntime.openMemory({ keyVault: new FakeKeyVault() });
            try {
                const session2 = new RdbSession({ workspace: runtime.workspace, keyVault: runtime.session.keyVault });
                runtime.session.aliases.set('group', 'prod', 'abc123==' as import("@hyper-hyper-space/hhs3_crypto").B64Hash);
                assertEquals(session2.aliases.get('group', 'prod'), undefined, 'aliases isolated');
            } finally {
                await runtime.close();
            }
        },
    },
    {
        name: '[RDB_RT09] MemoryKeyVault creates and unlocks ephemeral identities',
        invoke: async () => {
            const vault = new MemoryKeyVault();
            const identity = await vault.create('alice', 'correct');

            assertEquals(identity.publicKey.suite, SIGNING_ED25519, 'defaults to Ed25519');
            assertEquals(vault.list().length, 1, 'key is listed');
            assertEquals(vault.resolveRecord('alice').keyId, identity.keyId, 'exact label resolves');
            assertEquals(vault.resolvePublic('alice').keyId, identity.keyId, 'public key resolves');

            const prefix = identity.keyId.slice(0, 8);
            const unlocked = await vault.unlock(`#${prefix}`, 'correct');
            assertEquals(unlocked.keyId, identity.keyId, 'key-id prefix unlocks');
            assertEquals(unlocked.secretKey.length, identity.secretKey.length, 'secret key is retained');

            await expectError(
                () => vault.unlock('alice', 'incorrect'),
                'Wrong passphrase',
                'wrong passphrase rejected',
            );
            await expectError(
                () => vault.create('alice', 'another'),
                'already exists',
                'duplicate label rejected',
            );
        },
    },
    {
        name: '[RDB_RT11] group names resolve within a database: db.group, the current database, or a unique match',
        invoke: async () => {
            const runtime = await RdbRuntime.openMemory({ keyVault: new FakeKeyVault() });
            try {
                await runtime.session.createKey('alice', 'correct');
                runtime.session.selectAuthor('alice');
                await runtime.execute(`
CREATE SCHEMA shop CREATORS ($me) AS (TABLE products (sku string) ALLOW all IF true);
CREATE SCHEMA misc CREATORS ($me) AS (TABLE notes (text string) ALLOW all IF true);
CREATE CATALOG shop_catalog VERSION '1.0.0' AS (TABLEGROUP shop_prod USING SCHEMA shop);
CREATE CATALOG misc_catalog VERSION '1.0.0' AS (TABLEGROUP notes USING SCHEMA misc);
CREATE DATABASE east USING CATALOG shop_catalog;
CREATE DATABASE west USING CATALOG shop_catalog;
CREATE DATABASE other USING CATALOG misc_catalog;
`);
                assertEquals(runtime.session.workspace.roots.list('group').length, 3, 'one group per database');

                // CREATE DATABASE made 'other' current: shop_prod is elsewhere.
                await expectRunError(runtime, "INSERT INTO shop_prod.products (sku) VALUES ('x');",
                    'use east.shop_prod or west.shop_prod', 'the error suggests the qualified names');

                await runtime.execute("INSERT INTO east.shop_prod.products (sku) VALUES ('e1');");
                await runtime.execute("USE DATABASE west; INSERT INTO shop_prod.products (sku) VALUES ('w1');");
                const east = await runtime.execute('SELECT sku FROM east.shop_prod.products;');
                const west = await runtime.execute('SELECT sku FROM shop_prod.products;');
                const skus = (r: typeof east) => r.results[0]!.result.kind === 'select'
                    ? r.results[0]!.result.rows.map((row) => row.values['sku']) : [];
                assertEquals(JSON.stringify(skus(east)), JSON.stringify(['e1']), 'east has its own row');
                assertEquals(JSON.stringify(skus(west)), JSON.stringify(['w1']), 'the current database is west');

                const fresh = new RdbSession({ workspace: runtime.workspace, keyVault: runtime.session.keyVault });
                await expectError(() => executeText(fresh, 'SELECT sku FROM shop_prod.products;'),
                    'Ambiguous group', 'without a current database, two matches are ambiguous');
                const unique = await executeText(fresh, 'SELECT text FROM notes.notes;');
                assertEquals(unique.results[0]?.result.kind, 'select', 'a unique name resolves across databases');
            } finally {
                await runtime.close();
            }
        },
    },
    {
        name: '[RDB_RT12] UPDATE CATALOG deploys a release; the group LOG labels the deploy',
        invoke: async () => {
            const runtime = await RdbRuntime.openMemory({ keyVault: new FakeKeyVault() });
            try {
                await runtime.session.createKey('alice', 'correct');
                runtime.session.selectAuthor('alice');
                await runtime.execute(setupScript());
                await runtime.execute(`
ALTER SCHEMA shop AS (ADD COLUMN products.price integer DEFAULT 0);
ALTER CATALOG shop_catalog VERSION '1.1.0' AS (UPDATE SCHEMA shop TO LATEST ON shop_prod);
`);
                const deployed = await runtime.execute("UPDATE CATALOG shop_catalog TO '1.1.0' ON shop_db;");
                const result = deployed.results[0]!.result;
                assertTrue(result.kind === 'update-catalog' && result.update.deployed.length === 1, 'one group deployed');
                assertTrue(result.kind === 'update-catalog' && result.update.commit !== undefined, 'the release is recorded');

                await runtime.execute("INSERT INTO shop_prod.products (sku, name, price) VALUES ('B', 'Gadget', 3);");
                const log = await runtime.execute('LOG shop_prod;');
                const logResult = log.results[0]!.result;
                assertTrue(logResult.kind === 'log', 'log result');
                if (logResult.kind !== 'log') return;
                assertEquals(logResult.renderContext.deployLabels !== undefined
                    && Object.values(logResult.renderContext.deployLabels).includes('shop_catalog 1.1.0'), true,
                'the deploy is labeled with the release that pins it');

                await expectRunError(runtime, "UPDATE CATALOG shop_catalog TO '9.9.9' ON shop_db;",
                    "no release '9.9.9'", 'an unknown release is rejected');
            } finally {
                await runtime.close();
            }
        },
    },
    {
        name: '[RDB_RT13] source mode: CREATE CATALOG without VERSION takes the given version',
        invoke: async () => {
            const runtime = await RdbRuntime.openMemory({ keyVault: new FakeKeyVault() });
            try {
                await runtime.session.createKey('alice', 'correct');
                runtime.session.selectAuthor('alice');
                const source = `
CREATE SCHEMA shop CREATORS ($alice) AS (TABLE products (sku string));
CREATE CATALOG shop_catalog CREATORS ($alice) AS (TABLEGROUP shop_prod USING SCHEMA shop);
`;
                await expectRunError(runtime, source, 'requires VERSION', 'outside source mode a catalog needs VERSION');

                const run = await runtime.execute(source, { source: { catalogVersion: '2.0.0', schemaVersion: '2.0.0' } });
                const schema = run.results[0]!.result;
                assertTrue(schema.kind === 'create-plan' && schema.plan.kind === 'create-schema', 'the schema is created');
                if (schema.kind !== 'create-plan' || schema.plan.kind !== 'create-schema') return;
                assertEquals(schema.plan.payload.version, '2.0.0', 'an omitted schema VERSION takes the catalog version');
                const created = run.results[1]!.result;
                assertTrue(created.kind === 'create-plan' && created.plan.kind === 'create-catalog', 'the catalog is created');
                if (created.kind !== 'create-plan' || created.plan.kind !== 'create-catalog') return;
                assertEquals(created.plan.payload.version, '2.0.0', 'with the source version');

                const explicit = await runtime.execute(`
CREATE SCHEMA other CREATORS ($alice) VERSION '0.9.0' AS (TABLE products (sku string));
CREATE CATALOG other_catalog CREATORS ($alice) AS (TABLEGROUP other_prod USING SCHEMA other);
`, { source: { catalogVersion: '2.0.0', schemaVersion: '2.0.0' } });
                const kept = explicit.results[0]!.result;
                assertTrue(kept.kind === 'create-plan' && kept.plan.kind === 'create-schema', 'the versioned schema is created');
                if (kept.kind !== 'create-plan' || kept.plan.kind !== 'create-schema') return;
                assertEquals(kept.plan.payload.version, '0.9.0', 'a written schema VERSION is kept');
            } finally {
                await runtime.close();
            }
        },
    },
    {
        name: '[RDB_RT14] a rejected create is a VALIDATION_REJECTED diagnostic at its statement',
        invoke: async () => {
            const runtime = await RdbRuntime.openMemory({ keyVault: new FakeKeyVault() });
            try {
                await runtime.session.createKey('alice', 'correct');
                runtime.session.selectAuthor('alice');
                await runtime.execute(`
CREATE SCHEMA stock CREATORS ($me) AS (TABLE items (label string, qty integer MIN 0 MAX 10));
CREATE CATALOG stock_catalog VERSION '1.0.0' PARAMS (:qty integer) AS (
  TABLEGROUP store USING SCHEMA stock WITH ROWS (items (label = 'start', qty = :qty))
);
`);
                let error: unknown;
                try {
                    await runtime.execute("CREATE SCHEMA other CREATORS ($me) AS (TABLE t (x string));\n\nCREATE DATABASE stock_db\n  USING CATALOG stock_catalog WITH PARAMS (:qty = 200);");
                } catch (e) {
                    error = e;
                }
                assertTrue(error instanceof LanguageError, `a LanguageError, got ${String(error)}`);
                if (!(error instanceof LanguageError)) return;
                const [diagnostic] = error.diagnostics;
                assertEquals(diagnostic?.code, 'VALIDATION_REJECTED', 'the rejection keeps its code');
                assertTrue(diagnostic?.message.includes('200 is out of range [0, 10]') ?? false, diagnostic?.message ?? 'no message');
                assertEquals(diagnostic?.span?.line, 3, 'reported at the CREATE DATABASE line');
            } finally {
                await runtime.close();
            }
        },
    },
    {
        name: '[RDB_RT10] MemoryKeyVault resolves exact labels before unambiguous prefixes',
        invoke: async () => {
            const vault = new MemoryKeyVault();
            const first = await vault.create('first', 'one');
            const prefixLabel = first.keyId.slice(0, 8);
            const second = await vault.create(prefixLabel, 'two');

            assertEquals(vault.resolveRecord(prefixLabel).keyId, second.keyId, 'exact label wins');
            assertEquals(vault.resolveRecord(`#${first.keyId}`).keyId, first.keyId, 'full key id resolves');
            await expectError(
                async () => vault.resolveRecord(''),
                'Ambiguous key prefix',
                'ambiguous prefix rejected',
            );
            await expectError(
                async () => vault.resolveRecord('not-a-key'),
                'Unknown key',
                'unknown key rejected',
            );
        },
    },
];

class FakeKeyVault implements KeyVault {
    private keys: StoredKeyRecord[] = [];

    list(): KeyRecord[] {
        return this.keys.map(({ label, keyId, publicKey }) => ({ label, keyId, publicKey }));
    }

    async create(label: string, _passphrase: string, signingName?: SigningName): Promise<OwnIdentity> {
        const hashSuite = createBasicCrypto().hash(HASH_SHA256);
        const identity = await createIdentity(signingName ?? SIGNING_ED25519, hashSuite);
        this.keys.push({ label, keyId: identity.keyId, publicKey: encodePublicKey(identity.publicKey) });
        return identity;
    }

    async unlock(labelOrPrefix: string, _passphrase: string): Promise<OwnIdentity> {
        const record = this.resolveRecord(labelOrPrefix);
        const hashSuite = createBasicCrypto().hash(HASH_SHA256);
        const identity = await createIdentity(SIGNING_ED25519, hashSuite);
        return { ...identity, keyId: record.keyId };
    }

    resolvePublic(labelOrPrefix: string) {
        const record = this.resolveRecord(labelOrPrefix);
        return { keyId: record.keyId, publicKey: decodePublicKey(record.publicKey) };
    }

    resolveRecord(labelOrPrefix: string): KeyRecord {
        const normalized = labelOrPrefix.startsWith('#') ? labelOrPrefix.slice(1) : labelOrPrefix;
        const labelMatch = this.keys.find((key) => key.label === normalized);
        if (labelMatch !== undefined) return labelMatch;

        const keyMatches = this.keys.filter((key) => key.keyId.startsWith(normalized));
        if (keyMatches.length === 1) return keyMatches[0]!;
        if (keyMatches.length === 0) throw new Error(`Unknown key '${labelOrPrefix}'`);
        throw new Error(`Ambiguous key prefix '${labelOrPrefix}'`);
    }
}

type StoredKeyRecord = KeyRecord;

async function expectError(
    invoke: () => unknown | Promise<unknown>,
    message: string,
    description: string,
): Promise<void> {
    let thrown: unknown;
    try {
        await invoke();
    } catch (e) {
        thrown = e;
    }
    assertTrue(
        thrown instanceof Error && thrown.message.includes(message),
        description,
    );
}

function setupScript(): string {
    return `
CREATE SCHEMA shop CREATORS ($me) AS (
  TABLE products (
    sku string PUB READONLY,
    name string
  )
);
CREATE CATALOG shop_catalog VERSION '1.0.0' AS (
  TABLEGROUP shop_prod USING SCHEMA shop
);
CREATE DATABASE shop_db USING CATALOG shop_catalog;
INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');
SELECT sku, name FROM shop_prod.products;
`;
}

function crossGroupSetupScript(): string {
    return `
CREATE SCHEMA users_schema CREATORS ($me) AS (
  TABLE identities (name string) ALLOW all IF true
);
CREATE SCHEMA shop CREATORS ($me) AS (
  TABLE orders (
    customer string REFERENCES users.identities,
    label string
  ) ALLOW all IF true
);
CREATE CATALOG shop_catalog VERSION '1.0.0' AS (
  TABLEGROUP users USING SCHEMA users_schema,
  TABLEGROUP shop_prod USING SCHEMA shop BIND users => users
);
CREATE DATABASE shop_db USING CATALOG shop_catalog;
`;
}

async function expectRunError(runtime: RdbRuntime, sql: string, message: string, description: string): Promise<void> {
    await expectError(() => runtime.execute(sql), message, description);
}

async function main() {
    console.log('Running tests for Hyper Hyper Space v3 rdb_runtime module\n');
    for (const test of tests) {
        testing.exitIfFailed(await testing.run(test.name, test.invoke));
    }
}

main();

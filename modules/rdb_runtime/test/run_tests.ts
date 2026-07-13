import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519, type OwnIdentity, type SigningName } from "@hyper-hyper-space/hhs3_crypto";
import type { RTableGroup } from "@hyper-hyper-space/hhs3_rdb";
import { testing } from "@hyper-hyper-space/hhs3_util";
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import {
    executeText,
    keyPassphraseRequiredFromError,
    KeyPassphraseRequiredError,
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
                assertEquals(result.results.length, 4, 'four statements');
                assertEquals(session.workspace.roots.list('schema').length, 1, 'schema indexed');
                assertEquals(session.workspace.roots.list('group').length, 1, 'group indexed');
            } finally {
                await runtime.close();
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
                assertEquals(runtime2.session.workspace.roots.list('group').length, 1, 'group rehydrated');
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

                const fresh = new RdbSession({ workspace: runtime.workspace, keyVault: runtime.session.keyVault });
                let required: KeyPassphraseRequiredError | undefined;
                try {
                    await executeText(fresh, "INSERT INTO shop_prod.products (sku, name) VALUES ('B', 'Gadget') BY $alice;");
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
                const events = inserted.results[0]?.events ?? [];
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
        const match = this.keys.find((k) => k.label === normalized || k.keyId.startsWith(normalized));
        if (match === undefined) throw new Error(`Unknown key '${labelOrPrefix}'`);
        return match;
    }
}

type StoredKeyRecord = KeyRecord;

function setupScript(): string {
    return `
CREATE SCHEMA shop CREATORS ($me) AS (
  TABLE products (
    sku string PUB READONLY,
    name string
  )
);
CREATE TABLEGROUP shop_prod USING SCHEMA shop;
INSERT INTO shop_prod.products (sku, name) VALUES ('A', 'Widget');
SELECT sku, name FROM shop_prod.products;
`;
}

function crossGroupSetupScript(): string {
    return `
CREATE SCHEMA users_schema CREATORS ($me) AS (
  TABLE identities (name string) ALLOW all IF true
);
CREATE TABLEGROUP users USING SCHEMA users_schema;
CREATE SCHEMA shop CREATORS ($me) AS (
  TABLE orders (
    customer string REFERENCES users.identities,
    label string
  ) ALLOW all IF true
);
CREATE TABLEGROUP shop_prod USING SCHEMA shop BIND users => users;
`;
}

async function main() {
    console.log('Running tests for Hyper Hyper Space v3 rdb_runtime module\n');
    for (const test of tests) {
        testing.exitIfFailed(await testing.run(test.name, test.invoke));
    }
}

main();

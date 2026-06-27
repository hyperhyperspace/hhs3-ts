import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";

import { bind } from "../src/bind/bind.js";
import { compileCreate } from "../src/compile/create.js";
import { parseStatement } from "../src/syntax/parser.js";
import { createTestBindContext } from "./mock_bind_context.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

const schemaBody = `
  TABLE t (
    name string
  ) ALLOW all IF true
`;

async function bindCreateSchema(sql: string, context: ReturnType<typeof createTestBindContext>) {
    const parsed = parseStatement(sql);
    assertTrue(parsed.ok, `parse should succeed: ${sql}`);
    if (!parsed.ok) throw new Error(parsed.diagnostics[0].message);
    const bound = await bind(parsed.value, context);
    assertTrue(bound.ok, `bind should succeed: ${sql}`);
    if (!bound.ok) throw new Error(bound.diagnostics[0].message);
    assertEquals(bound.value.kind, 'create-schema', 'statement kind');
    if (bound.value.kind !== 'create-schema') throw new Error('expected create-schema');
    return bound.value;
}

export const creatorResolutionTests = {
    title: '[RDB_LANG:CREATORS] Keystore creator resolution',
    tests: [
        {
            name: '[CREATORS01] CREATORS ($admin) binds with identity variable',
            invoke: async () => {
                const ctx = createMockRContext();
                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const lang = createTestBindContext(ctx, { admin, me: admin });
                const bound = await bindCreateSchema(`CREATE SCHEMA s CREATORS ($admin) AS (${schemaBody});`, lang);
                const plan = await compileCreate(bound);
                assertEquals(plan.kind, 'create-schema', 'create plan kind');
                if (plan.kind !== 'create-schema') return;
                assertEquals(plan.payload.creators[0].keyId, admin.keyId, 'creator keyId');
                assertEquals(plan.payload.creators[0].publicKey, serializePublicKeyToBase64(admin.publicKey), 'creator publicKey');
            },
        },
        {
            name: '[CREATORS02] CREATORS (#prefix) binds via keystore lookup',
            invoke: async () => {
                const ctx = createMockRContext();
                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const lang = createTestBindContext(ctx, { admin, me: admin });
                const prefix = admin.keyId.slice(0, 8);
                const bound = await bindCreateSchema(`CREATE SCHEMA s CREATORS (#${prefix}) AS (${schemaBody});`, lang);
                const plan = await compileCreate(bound);
                assertEquals(plan.kind, 'create-schema', 'create plan kind');
                if (plan.kind !== 'create-schema') return;
                assertEquals(plan.payload.creators[0].keyId, admin.keyId, 'creator keyId');
            },
        },
        {
            name: '[CREATORS03] CREATORS key-id literal binds via keystore lookup',
            invoke: async () => {
                const ctx = createMockRContext();
                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const lang = createTestBindContext(ctx, { admin, me: admin });
                const bound = await bindCreateSchema(`CREATE SCHEMA s CREATORS ('${admin.keyId}') AS (${schemaBody});`, lang);
                const plan = await compileCreate(bound);
                assertEquals(plan.kind, 'create-schema', 'create plan kind');
                if (plan.kind !== 'create-schema') return;
                assertEquals(plan.payload.creators[0].keyId, admin.keyId, 'creator keyId');
            },
        },
        {
            name: '[CREATORS04] unknown creator hash prefix fails binding',
            invoke: async () => {
                const ctx = createMockRContext();
                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const lang = createTestBindContext(ctx, { admin, me: admin });
                const parsed = parseStatement(`CREATE SCHEMA s CREATORS (#deadbeef) AS (${schemaBody});`);
                assertTrue(parsed.ok, 'parse should succeed');
                if (!parsed.ok) return;
                const bound = await bind(parsed.value, lang);
                assertTrue(!bound.ok, 'bind should fail for unknown creator key');
                if (!bound.ok) {
                    assertTrue(bound.diagnostics[0].message.includes('Unknown key'), 'bind diagnostic mentions unknown key');
                }
            },
        },
        {
            name: '[CREATORS05] hash/literal require resolvePublicKey host hook',
            invoke: async () => {
                const ctx = createMockRContext();
                const admin = await createIdentity(SIGNING_ED25519, hashSuite);
                const lang = createTestBindContext(ctx, { admin, me: admin });
                delete (lang as { resolvePublicKey?: unknown }).resolvePublicKey;

                for (const sql of [
                    `CREATE SCHEMA s CREATORS (#${admin.keyId.slice(0, 8)}) AS (${schemaBody});`,
                    `CREATE SCHEMA s CREATORS ('${admin.keyId}') AS (${schemaBody});`,
                ]) {
                    const parsed = parseStatement(sql);
                    assertTrue(parsed.ok, `parse should succeed: ${sql}`);
                    if (!parsed.ok) continue;
                    const bound = await bind(parsed.value, lang);
                    assertTrue(!bound.ok, `bind should fail without resolvePublicKey: ${sql}`);
                    if (!bound.ok) {
                        assertTrue(
                            bound.diagnostics[0].message.includes('requires a keystore host'),
                            'bind diagnostic mentions keystore host requirement',
                        );
                    }
                }
            },
        },
    ],
};

// Conformance-style tests for rdb_keys: author_key_id, registerKey, duplicate collapse.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { deriveRowId } from "@hyper-hyper-space/hhs3_rdb";
import { serializePublicKeyToBase64 } from "@hyper-hyper-space/hhs3_mvt";
import {
    DEFAULT_KEY_DOMAIN, KeyIndex, MemoryTarget, projectGroup,
} from "@hyper-hyper-space/hhs3_rdb_adapter";

import { createGroup } from "./group_fixture.js";
import { TargetFactory, memoryHarness } from "./projection_reader.js";

type NamedTest = { name: string; invoke: () => Promise<void> };

function asKeys(target: object): KeyIndex {
    return target as KeyIndex;
}

export function createKeysSuite(label: string, factory: TargetFactory): { title: string; tests: NamedTest[] } {
    return {
        title: `[${label}] rdb_keys / author_key_id`,
        tests: [
            {
                name: `[${label}K01] author projects as author_key_id; keyHashForId recovers the key hash`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    const ledger = await group.getTable('ledger');
                    await ledger.insert('l1', { ref: 'R-1', amount: '1.00' }, admin);

                    const { target, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);
                        const keys = asKeys(target);
                        const authorId = await keys.idForKeyHash(DEFAULT_KEY_DOMAIN, admin.keyId);
                        assertTrue(authorId !== undefined, 'author key was interned');
                        assertEquals(
                            await keys.keyHashForId(DEFAULT_KEY_DOMAIN, authorId!),
                            admin.keyId,
                            'keyHashForId recovers author key hash');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}K02] registerKey is idempotent and stores public_key`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    const { target, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);
                        const keys = asKeys(target);
                        const pk = serializePublicKeyToBase64(admin.publicKey);
                        const id1 = await keys.registerKey(DEFAULT_KEY_DOMAIN, admin.keyId, pk);
                        const id2 = await keys.registerKey(DEFAULT_KEY_DOMAIN, admin.keyId, pk);
                        assertEquals(id1, id2, 'registerKey is idempotent');
                        assertEquals(
                            await keys.publicKeyForId(DEFAULT_KEY_DOMAIN, id1),
                            pk,
                            'public key stored');
                    } finally {
                        await cleanup?.();
                    }
                },
            },
            {
                name: `[${label}K03] duplicate authors of the same key collapse to one id`,
                invoke: async () => {
                    const { group, admin } = await createGroup();
                    const ledger = await group.getTable('ledger');
                    await ledger.insert('l1', { ref: 'R-1', amount: '1.00' }, admin);
                    await ledger.insert('l2', { ref: 'R-2', amount: '2.00' }, admin);

                    const { target, cleanup } = await factory();
                    try {
                        await projectGroup(group, target);
                        const keys = asKeys(target);
                        const id = await keys.idForKeyHash(DEFAULT_KEY_DOMAIN, admin.keyId);
                        assertTrue(id !== undefined, 'single id for the author key');
                        assertEquals(id, await keys.idForKeyHash(DEFAULT_KEY_DOMAIN, admin.keyId));
                    } finally {
                        await cleanup?.();
                    }
                },
            },
        ],
    };
}

export function memoryKeysSuite(): { title: string; tests: NamedTest[] } {
    return createKeysSuite('ADPT-MEM', memoryHarness);
}

export const memoryAuthorIdTest: NamedTest = {
    name: '[ADPT-MEMK00] MemoryTarget stores numeric author_key_id on the row',
    invoke: async () => {
        const { group, admin } = await createGroup();
        const ledger = await group.getTable('ledger');
        await ledger.insert('l1', { ref: 'R-1', amount: '1.00' }, admin);
        const target = new MemoryTarget();
        await projectGroup(group, target);
        const row = target.getRowByRowId('ledger', deriveRowId('l1', admin.keyId));
        assertTrue(row !== undefined && typeof row.author === 'number', 'author is numeric');
        assertEquals(
            await target.keyHashForId(DEFAULT_KEY_DOMAIN, row!.author!),
            admin.keyId,
            'author_key_id maps back to key hash');
    },
};

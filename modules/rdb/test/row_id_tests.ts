import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { deriveRowId, checkRowId } from "../src/rtable/hash.js";

async function testDeterminism() {
    assertTrue(deriveRowId('uuid-1') === deriveRowId('uuid-1'), 'anonymous derivation should be deterministic');
    assertTrue(
        deriveRowId('uuid-1', 'alice') === deriveRowId('uuid-1', 'alice'),
        'owned derivation should be deterministic');
}

async function testOwnerPartitionsIdSpace() {
    const anonymous = deriveRowId('uuid-1');
    const aliceOwned = deriveRowId('uuid-1', 'alice');
    const bobOwned = deriveRowId('uuid-1', 'bob');

    assertTrue(anonymous !== aliceOwned, 'anonymous and owned ids should differ');
    assertTrue(aliceOwned !== bobOwned, 'different owners should produce different ids');
}

async function testUuidAffectsId() {
    assertTrue(deriveRowId('uuid-1') !== deriveRowId('uuid-2'), 'different uuids should produce different ids');
    assertTrue(
        deriveRowId('uuid-1', 'alice') !== deriveRowId('uuid-2', 'alice'),
        'different uuids should produce different ids for the same owner');
}

async function testCheckRowId() {
    const rowId = deriveRowId('uuid-1', 'alice');

    assertTrue(checkRowId(rowId, 'uuid-1', 'alice'), 'matching row id should verify');
    assertFalse(checkRowId(rowId, 'uuid-2', 'alice'), 'wrong uuid should fail verification');
    assertFalse(checkRowId(rowId, 'uuid-1', 'bob'), 'wrong owner should fail verification');
    assertFalse(checkRowId(rowId, 'uuid-1'), 'anonymous claim over owned id should fail verification');

    const anonymousId = deriveRowId('uuid-1');
    assertTrue(checkRowId(anonymousId, 'uuid-1'), 'matching anonymous row id should verify');
    assertFalse(checkRowId(anonymousId, 'uuid-1', 'alice'), 'owned claim over anonymous id should fail verification');
}

export const rowIdTests = {
    title: '[ROWID] Row id derivation tests',
    tests: [
        { name: '[ROWID01] Determinism', invoke: testDeterminism },
        { name: '[ROWID02] Owner partitions id space', invoke: testOwnerPartitionsIdSpace },
        { name: '[ROWID03] Uuid affects id', invoke: testUuidAffectsId },
        { name: '[ROWID04] Row id verification', invoke: testCheckRowId },
    ],
};

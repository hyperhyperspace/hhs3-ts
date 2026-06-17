import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { json } from "@hyper-hyper-space/hhs3_json";

import { validateRSchemaPayloadFormat } from "../src/rschema/validate.js";
import { CreateRSchemaPayload, SchemaUpdatePayload } from "../src/rschema/payload.js";
import { TableDef } from "../src/rschema/payload.js";

import { validateRowOpFormat } from "../src/rtable/validate.js";
import { InsertRowPayload, UpdateRowPayload, DeleteRowPayload } from "../src/rtable/payload.js";
import { deriveRowId } from "../src/rtable/hash.js";

import { validateTableGroupPayloadFormat } from "../src/rtable_group/validate.js";
import { CreateTableGroupPayload, RowEnvelopePayload, BundlePayload } from "../src/rtable_group/payload.js";

import { validateRDbPayloadFormat } from "../src/rdb/validate.js";

function ordersTable(): TableDef {
    return {
        name: 'orders',
        columns: { customer: { type: 'string' }, total: { type: 'float' } },
    };
}

function linesTable(): TableDef {
    return {
        name: 'lines',
        columns: { order: { type: 'string' }, qty: { type: 'integer' } },
        fks: { order: 'orders' },
    };
}

function validInsert(): InsertRowPayload {
    const uuid = 'uuid-1';
    const owner = 'alice';
    return {
        action: 'insert',
        rowId: deriveRowId(uuid, owner),
        uuid,
        owner,
        values: { customer: 'c#7', total: 90 },
    };
}

// RSchema payloads

async function testRSchemaCreate() {
    const create: CreateRSchemaPayload = {
        action: 'create',
        seed: 'seed-1',
        name: 'shop',
        creators: [{ keyId: 'alice', publicKey: 'pem...' }],
        tables: [ordersTable(), linesTable()],
    };
    assertTrue(validateRSchemaPayloadFormat(create), 'well-formed schema create should validate');

    assertFalse(validateRSchemaPayloadFormat({ ...create, action: 'nope' }), 'unknown action should not validate');
    assertFalse(validateRSchemaPayloadFormat({ ...create, extra: 1 } as json.Literal), 'extra keys should not validate (strict format)');
    assertFalse(validateRSchemaPayloadFormat({ ...create, creators: [] }),
        'schema create without creators should not validate');
    assertFalse(validateRSchemaPayloadFormat({
        ...create,
        gates: { deploy: { p: 'exists', table: 'users.caps', where: { label: 'deploy' }, owner: '$author' } },
    } as json.Literal), 'schema create with gates should not validate (deploy gating moved to the group)');
    assertFalse(
        validateRSchemaPayloadFormat({ ...create, tables: [linesTable()] }),
        'schema whose local FK target is missing should not validate');
    assertFalse(
        validateRSchemaPayloadFormat({ ...create, tables: [ordersTable(), ordersTable()] }),
        'duplicate table names should not validate');
}

async function testRSchemaUpdate() {
    const update: SchemaUpdatePayload = {
        action: 'schema-update',
        migration: [{ rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'new' } }],
        note: 'add order status',
        author: 'alice',
        signature: 'sig...',
    };
    assertTrue(validateRSchemaPayloadFormat(update), 'well-formed rules-only schema update should validate');

    const { author: _author, signature: _signature, ...unsigned } = update;
    assertFalse(validateRSchemaPayloadFormat(unsigned as json.Literal),
        'unsigned schema update should not validate');

    assertFalse(validateRSchemaPayloadFormat({
        ...update,
        tables: [ordersTable()],
    } as json.Literal), 'schema update carrying table defs should not validate (rules-only)');

    assertFalse(
        validateRSchemaPayloadFormat({ ...update, migration: [] }),
        'schema update without migration rules should not validate');
    assertFalse(
        validateRSchemaPayloadFormat({
            ...update,
            migration: [{ rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string' } }],
        }),
        'migration adding a non-nullable column without default should not validate');
}

// RTable row ops

async function testInsertRowOp() {
    const insert = validInsert();
    assertTrue(validateRowOpFormat(insert), 'well-formed insert should validate');

    const anonymous: InsertRowPayload = {
        action: 'insert',
        rowId: deriveRowId('uuid-2'),
        uuid: 'uuid-2',
        values: { customer: 'c#9', total: 10 },
    };
    assertTrue(validateRowOpFormat(anonymous), 'anonymous insert should validate');

    assertFalse(validateRowOpFormat({ ...insert, rowId: deriveRowId('other-uuid', insert.owner) }),
        'rowId not matching (uuid, owner) should not validate');
    assertFalse(validateRowOpFormat({ ...insert, owner: 'bob' }),
        'rowId not matching claimed owner should not validate');
    assertFalse(validateRowOpFormat({ ...insert, owner: ['alice'] }),
        'owner as an array should not validate (v1: single owner)');
    assertFalse(validateRowOpFormat({ ...insert, values: { '2bad': 1 } }),
        'invalid column name should not validate');
}

async function testUpdateAndDeleteRowOps() {
    const rowId = deriveRowId('uuid-1', 'alice');

    const update: UpdateRowPayload = { action: 'update', rowId, values: { total: 95 } };
    assertTrue(validateRowOpFormat(update), 'well-formed update should validate');

    assertFalse(validateRowOpFormat({ action: 'update', rowId, values: {} }),
        'update with no values should not validate');

    const del: DeleteRowPayload = { action: 'delete', rowId };
    assertTrue(validateRowOpFormat(del), 'well-formed delete should validate');

    assertFalse(validateRowOpFormat({ action: 'delete' } as json.Literal), 'delete without rowId should not validate');
}

// RTableGroup payloads

async function testGroupCreate() {
    const initialAdminCap: InsertRowPayload = {
        action: 'insert',
        rowId: deriveRowId('seed-admin', 'alice'),
        uuid: 'seed-admin',
        owner: 'alice',
        values: { label: 'admin' },
    };

    const create: CreateTableGroupPayload = {
        action: 'create',
        seed: 'seed-1',
        schemaRef: 'schemaObjectId',
        schemaVersion: json.toSet(['hash1', 'hash2']),
        initialRows: { caps: [initialAdminCap] },
        bindings: { users: 'usersGroupObjectId' },
    };
    assertTrue(validateTableGroupPayloadFormat(create), 'well-formed group create should validate');

    const { schemaRef: _schemaRef, ...noSchemaRef } = create;
    assertFalse(validateTableGroupPayloadFormat(noSchemaRef as json.Literal), 'group create without schemaRef should not validate');

    assertFalse(validateTableGroupPayloadFormat({
        ...create,
        initialRows: { caps: [{ ...initialAdminCap, author: 'alice', signature: 'sig...' }] },
    } as json.Literal), 'initial row carrying authoring should not validate (fiat data)');

    assertFalse(validateTableGroupPayloadFormat({
        ...create,
        initialRows: { caps: [{ action: 'delete', rowId: 'x' }] },
    } as json.Literal), 'initial row that is not an insert should not validate');

    assertFalse(validateTableGroupPayloadFormat({
        ...create,
        bindings: { '2bad': 'someGroupId' },
    } as json.Literal), 'binding with invalid name should not validate');

    const withDeploy: CreateTableGroupPayload = {
        ...create,
        canDeploy: { p: 'exists', table: 'users.caps', where: { label: 'deploy' }, owner: '$author' },
    };
    assertTrue(validateTableGroupPayloadFormat(withDeploy), 'group create with canDeploy should validate');

    assertFalse(validateTableGroupPayloadFormat({
        ...create,
        canDeploy: { p: 'owner', is: '$author' },
    } as json.Literal), 'canDeploy with an owner atom should not validate (object context: no subject row)');

    assertFalse(validateTableGroupPayloadFormat({
        ...create,
        canDeploy: { p: 'exists', table: 'users.caps', owner: '$rowOwner' },
    } as json.Literal), 'canDeploy using $rowOwner should not validate (object context)');
}

async function testRowEnvelope() {
    const envelope: RowEnvelopePayload = {
        action: 'row',
        table: 'orders',
        op: validInsert(),
    };
    assertTrue(validateTableGroupPayloadFormat(envelope), 'well-formed row envelope should validate');

    assertFalse(validateTableGroupPayloadFormat({ ...envelope, table: '2bad' }), 'invalid table name should not validate');
    assertFalse(validateTableGroupPayloadFormat({ ...envelope, op: { action: 'nope' } }), 'invalid inner op should not validate');
}

async function testBundle() {
    const lineInsert: InsertRowPayload = {
        action: 'insert',
        rowId: deriveRowId('uuid-3', 'alice'),
        uuid: 'uuid-3',
        owner: 'alice',
        values: { order: 'someRowId', qty: 2 },
    };

    const bundle: BundlePayload = {
        action: 'bundle',
        writes: [
            { table: 'orders', op: validInsert() },
            { table: 'lines', op: lineInsert },
        ],
    };
    assertTrue(validateTableGroupPayloadFormat(bundle), 'well-formed bundle should validate');

    assertFalse(validateTableGroupPayloadFormat({ action: 'bundle', writes: [] }), 'empty bundle should not validate');
    assertFalse(validateTableGroupPayloadFormat({ action: 'bundle', writes: [{ table: '2bad', op: validInsert() }] }),
        'bundle with invalid table name should not validate');
    assertFalse(validateTableGroupPayloadFormat({ action: 'bundle', writes: [{ table: 'orders', op: { action: 'nope' } }] }),
        'bundle with invalid inner op should not validate');
}

async function testGroupRefAdvance() {
    const refAdvance = {
        action: 'ref-advance',
        refId: 'schemaObjectId',
        refVersion: json.toSet(['hash3']),
    };
    assertTrue(validateTableGroupPayloadFormat(refAdvance), 'canonical ref-advance should validate');

    assertFalse(validateTableGroupPayloadFormat({ action: 'ref-advance', refId: 'x' } as json.Literal),
        'ref-advance without refVersion should not validate');
}

// RDb payloads

async function testRDbPayloads() {
    assertTrue(validateRDbPayloadFormat({ action: 'create', seed: 'seed-1', name: 'mydb' }),
        'well-formed rdb create should validate');

    assertTrue(validateRDbPayloadFormat({ action: 'add-schema', schemaId: 'schemaObjectId', note: 'the shop schema' }),
        'well-formed add-schema should validate');
    assertTrue(validateRDbPayloadFormat({ action: 'add-schema', schemaId: 'schemaObjectId' }),
        'add-schema without note should validate');
    assertTrue(validateRDbPayloadFormat({ action: 'add-schema', schemaId: 'schemaObjectId', note: '2 free-form! text' }),
        'note is free-form: any bounded string validates (it is never resolved)');
    assertFalse(validateRDbPayloadFormat({ action: 'add-schema', schemaId: 'schemaObjectId', note: 7 } as json.Literal),
        'add-schema with a non-string note should not validate');

    assertTrue(validateRDbPayloadFormat({ action: 'add-group', groupId: 'groupObjectId', note: 'main deployment' }),
        'well-formed add-group should validate');
    assertFalse(validateRDbPayloadFormat({ action: 'add-group' } as json.Literal),
        'add-group without groupId should not validate');

    assertFalse(validateRDbPayloadFormat({ action: 'register-schema', name: 'shop', schemaId: 'x' }),
        'dropped catalog actions should not validate');
    assertFalse(validateRDbPayloadFormat({ action: 'nope' }), 'unknown action should not validate');
}

export const payloadFormatTests = {
    title: '[FORMAT] Payload format tests',
    tests: [
        { name: '[FORMAT01] RSchema create', invoke: testRSchemaCreate },
        { name: '[FORMAT02] RSchema update', invoke: testRSchemaUpdate },
        { name: '[FORMAT03] RTable insert', invoke: testInsertRowOp },
        { name: '[FORMAT04] RTable update and delete', invoke: testUpdateAndDeleteRowOps },
        { name: '[FORMAT05] RTableGroup create', invoke: testGroupCreate },
        { name: '[FORMAT06] RTableGroup row envelope', invoke: testRowEnvelope },
        { name: '[FORMAT07] RTableGroup bundle', invoke: testBundle },
        { name: '[FORMAT08] RTableGroup ref-advance', invoke: testGroupRefAdvance },
        { name: '[FORMAT09] RDb catalog payloads', invoke: testRDbPayloads },
    ],
};

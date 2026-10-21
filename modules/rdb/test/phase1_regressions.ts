// Regressions for the pre-existing bugs fixed alongside IMPORT_PATCHES.
import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { formatValidationFailure, version, type Version } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { validateSchemaParams } from "../src/rschema/validate.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import { validateRCatalogPayloadFormat } from "../src/rcatalog/validate.js";
import { RCatalogImpl, rCatalogFactory } from "../src/rcatalog/rcatalog.js";
import { applyReleaseBody, emptyParentFold } from "../src/rcatalog/resolve.js";
import type { CatalogGroupDef } from "../src/rcatalog/payload.js";
import { RTableGroupImpl, rTableGroupFactory } from "../src/rtable_group/group.js";
import { RTableViewImpl } from "../src/rtable/view.js";

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

async function testDecimalParamsRejected() {
    const reason = validateSchemaParams([{ name: 'rate', type: 'decimal', default: '1.5' }], false);
    assertTrue(reason !== undefined && reason.includes('no scale'), `decimal schema param should be rejected, got ${reason}`);

    const admin = await makeIdentity();
    const payload = await RCatalogImpl.create({
        name: 'cat',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        author: admin,
        version: '1.0.0',
        params: [{ name: 'rate', type: 'decimal' }],
    });
    const result = validateRCatalogPayloadFormat(payload as unknown as json.Literal);
    assertTrue(!result.valid && formatValidationFailure(result.why).includes('no scale'),
        `decimal catalog param should be rejected, got ${result.valid ? 'ok' : formatValidationFailure(result.why)}`);
}

async function testMissingSchemaSemverThrows() {
    const def: CatalogGroupDef = {
        name: 'g',
        seedSource: 'rdb',
        schemaRef: 'schema-not-local',
        schemaVersion: json.toSet(['entry-hash']),
    };
    let thrown = '';
    try {
        applyReleaseBody('release-hash', {
            version: '1.0.0',
            add: [def],
            author: 'author',
            signature: 'sig',
        }, [], emptyParentFold(), new Map(), true);
    } catch (err) {
        thrown = err instanceof Error ? err.message : String(err);
    }
    assertTrue(thrown.includes('schema-not-local') && thrown.includes('not available locally'),
        `a missing schema semver should throw, got '${thrown}'`);
}

async function schemaFrontier(schema: RSchemaImpl): Promise<Version> {
    return version(...await (await schema.getScopedDag()).getFrontier());
}

async function testExistsMatchingMissingTable() {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    const admin = await makeIdentity();
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: 'posts:schema',
        version: '1.0.0',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        tables: [{ name: 'posts', columns: { title: { type: 'string', pub: true } } }],
    }))) as RSchemaImpl;
    const group = (await ctx.createObject(await RTableGroupImpl.create({
        name: 'posts',
        seed: 'posts-group',
        schemaRef: schema.getId(),
        schemaVersion: await schemaFrontier(schema),
    }))) as RTableGroupImpl;
    const at = version(...await (await group.getScopedDag()).getFrontier());
    const view = await group.makeTable('missing').getView(at, at) as RTableViewImpl;
    assertEquals((await view.existsMatching({ title: 'x' })).status, 'false',
        'existsMatching on a table the schema does not have fails closed');
}

async function testDeployParamType() {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RTableGroupImpl.typeId, rTableGroupFactory);
    ctx.getRegistry().register(RCatalogImpl.typeId, rCatalogFactory);
    const admin = await makeIdentity();
    const schema = (await ctx.createObject(await RSchemaImpl.create({
        name: 'dep:schema',
        version: '1.0.0',
        creators: [{ keyId: admin.keyId, publicKey: admin.publicKey }],
        params: [{ name: 'max_qty', type: 'integer' }],
        tables: [{ name: 'orders', columns: { qty: { type: 'integer' } } }],
    }))) as RSchemaImpl;
    const group = (await ctx.createObject(await RTableGroupImpl.create({
        name: 'orders',
        seed: 'dep-type',
        schemaRef: schema.getId(),
        schemaVersion: await schemaFrontier(schema),
    }))) as RTableGroupImpl;
    const next = await schema.updateSchema([
        { rule: 'add-column', table: 'orders', column: 'note', def: { type: 'string', nullable: true } },
    ], admin, { version: '1.1.0' });
    let message = '';
    try {
        await group.deploy(version(next), undefined, undefined, { max_qty: 'nope' });
    } catch (err) {
        message = err instanceof Error ? err.message : String(err);
    }
    assertTrue(message.includes('max_qty') && message.includes('integer'),
        `a deploy param of the wrong type should be rejected, got '${message}'`);
}

export const phase1RegressionTests = {
    title: '[REGRESS] Pre-existing bug regressions',
    tests: [
        { name: '[REGRESS01] decimal params are rejected', invoke: testDecimalParamsRejected },
        { name: '[REGRESS02] a missing schema semver throws', invoke: testMissingSchemaSemverThrows },
        { name: '[REGRESS03] existsMatching fails closed when the table is absent', invoke: testExistsMatchingMissingTable },
        { name: '[REGRESS04] a schema deploy type-checks param values', invoke: testDeployParamType },
    ],
};

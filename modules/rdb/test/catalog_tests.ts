import { assertTrue, assertFalse, assertEquals } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, HASH_SHA256, createIdentity, SIGNING_ED25519 } from "@hyper-hyper-space/hhs3_crypto";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version, Version, RContext, signPayload } from "@hyper-hyper-space/hhs3_mvt";

import { createMockRContext } from "./mock_rcontext.js";
import { RSchemaImpl, rSchemaFactory } from "../src/rschema/rschema.js";
import type { TableDef } from "../src/rschema/payload.js";
import { RCatalogImpl, rCatalogFactory } from "../src/rcatalog/rcatalog.js";
import { catalogGroupHash } from "../src/rcatalog/payload.js";
import type { CatalogGroupDef } from "../src/rcatalog/payload.js";
import { compareSemver, semverInRange, majorRange, isValidSemver } from "../src/rcatalog/semver.js";

const crypto = createBasicCrypto();
const hashSuite = crypto.hash(HASH_SHA256);

async function makeIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

function identitiesTable(): TableDef {
    return {
        name: 'identities',
        columns: {
            keyId: { type: 'string', pub: true, readonly: true },
            publicKey: { type: 'string', pub: true, readonly: true },
            name: { type: 'string', nullable: true },
        },
        idProvider: { keyIdColumn: 'keyId', publicKeyColumn: 'publicKey' },
    };
}

function capsTable(): TableDef {
    return {
        name: 'caps',
        columns: {
            label: { type: 'string', pub: true, readonly: true },
            grantee: { type: 'identity', pub: true, readonly: true },
        },
    };
}

function docsTable(): TableDef {
    return {
        name: 'docs',
        columns: { title: { type: 'string' } },
        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'user.caps', where: { grantee: '$author' } } }],
    };
}

async function frontierOf(obj: { getScopedDag(): Promise<{ getFrontier(): Promise<Version> }> }): Promise<Version> {
    return (await obj.getScopedDag()).getFrontier();
}

async function createEnv() {
    const ctx = createMockRContext({ selfValidate: true });
    ctx.getRegistry().register(RSchemaImpl.typeId, rSchemaFactory);
    ctx.getRegistry().register(RCatalogImpl.typeId, rCatalogFactory);

    const dev = await makeIdentity();
    const creators = [{ keyId: dev.keyId, publicKey: dev.publicKey }];

    const userSchema = (await ctx.createObject(await RSchemaImpl.create({
        name: 'hhs:user', creators, tables: [identitiesTable(), capsTable()],
    }))) as RSchemaImpl;
    const docSchema = (await ctx.createObject(await RSchemaImpl.create({
        name: 'hhs:doc', creators, tables: [docsTable()],
    }))) as RSchemaImpl;
    const noteSchema = (await ctx.createObject(await RSchemaImpl.create({
        name: 'hhs:note', creators, tables: [{ name: 'notes', columns: { body: { type: 'string' } } }],
    }))) as RSchemaImpl;

    return { ctx, dev, creators, userSchema, docSchema, noteSchema };
}

function userDef(schema: RSchemaImpl, pin: Version, extra?: Partial<CatalogGroupDef>): CatalogGroupDef {
    return {
        name: 'user',
        seedSource: 'rdb',
        schemaRef: schema.getId(),
        schemaVersion: json.toSet([...pin]),
        idProvider: 'identities',
        initialRows: {
            identities: [{ values: { name: 'Admin' }, params: { keyId: { param: 'admin' }, publicKey: { param: 'admin', fn: 'publicKey' } } }],
            caps: [{ values: { label: 'manager' }, params: { grantee: { param: 'admin' } } }],
        },
        ...extra,
    };
}

function withoutUndefined(def: CatalogGroupDef): CatalogGroupDef {
    const out = { ...def } as { [key: string]: unknown };
    for (const key of Object.keys(out)) if (out[key] === undefined) delete out[key];
    return out as CatalogGroupDef;
}

function docDef(schema: RSchemaImpl, pin: Version, userHash: B64Hash, extra?: Partial<CatalogGroupDef>): CatalogGroupDef {
    return withoutUndefined({
        name: 'doc',
        seedSource: 'rdb',
        schemaRef: schema.getId(),
        schemaVersion: json.toSet([...pin]),
        bindings: { user: userHash },
        idProvider: 'user.identities',
        ...extra,
    });
}

async function createCatalog(ctx: RContext, dev: OwnIdentity, add: CatalogGroupDef[]) {
    const payload = await RCatalogImpl.create({
        name: 'editor',
        creators: [{ keyId: dev.keyId, publicKey: dev.publicKey }],
        author: dev,
        version: '1.0.0',
        add,
        params: [{ name: 'admin', type: 'identity' }],
        note: 'initial',
    });
    return (await ctx.createObject(payload)) as RCatalogImpl;
}

async function rejects(fn: () => Promise<unknown>, reason: string, message: string): Promise<void> {
    let error: string | undefined;
    try { await fn(); } catch (e) { error = (e as Error).message; }
    assertTrue(error !== undefined, message);
    assertTrue(error!.includes(reason), `${message} (expected '${reason}', got '${error}')`);
}

export const catalogTests = {
    title: '[CAT] RCatalog tests',
    tests: [
        {
            name: '[CAT01] semver parsing, ordering and caret ranges',
            invoke: async () => {
                assertTrue(isValidSemver('1.2.3'), '1.2.3 is valid');
                assertFalse(isValidSemver('1.2'), 'two components are invalid');
                assertFalse(isValidSemver('01.2.3'), 'leading zeros are invalid');
                assertFalse(isValidSemver('1.2.3-beta'), 'pre-release tags are invalid');
                assertTrue(compareSemver('1.10.0', '1.9.9') > 0, 'minor compares numerically');
                assertTrue(compareSemver('2.0.0', '1.99.99') > 0, 'major dominates');
                assertTrue(semverInRange('1.5.2', '^1'), '^1 includes 1.5.2');
                assertFalse(semverInRange('2.0.0', '^1'), '^1 excludes 2.0.0');
                assertTrue(semverInRange('0.3.9', '^0.3'), '^0.3 includes 0.3.9');
                assertFalse(semverInRange('0.4.0', '^0.3'), '^0.3 excludes 0.4.0');
                assertTrue(semverInRange('9.9.9', '*'), '* includes everything');
                assertTrue(semverInRange('1.2.3', '1.2.3'), 'an exact range includes itself');
                assertFalse(semverInRange('1.2.4', '1.2.3'), 'an exact range excludes others');
                assertEquals(majorRange('3.1.4'), '^3', 'the major range of 3.1.4 is ^3');
            }
        },
        {
            name: '[CAT02] the genesis is a signed first release; its pins are creation deps',
            invoke: async () => {
                const { ctx, dev, userSchema } = await createEnv();
                const pin = await frontierOf(userSchema);
                const def = userDef(userSchema, pin);
                const catalog = await createCatalog(ctx, dev, [def]);

                const view = await catalog.getView();
                assertEquals(view.getName(), 'editor', 'catalog name');
                assertEquals(view.getReleaseHashes().length, 1, 'one release');
                const genesis = view.getRelease(catalog.getId())!;
                assertEquals(genesis.version, '1.0.0', 'genesis version');
                assertEquals(genesis.parents.length, 0, 'the genesis has no parents');
                const hash = catalogGroupHash(def);
                assertTrue(genesis.groups.has(hash), 'the genesis group is in the state');
                assertEquals(genesis.groups.get(hash)!.version.join(','), [...pin].sort().join(','), 'the group is at its pin');
                assertEquals(view.getReferencedSchemas().join(','), userSchema.getId(), 'the genesis pins are referenced');

                const deps = await rCatalogFactory.extractCreationForeignDeps!(catalog.createOp, ctx);
                assertEquals(deps!.length, 1, 'one creation dep');
                assertEquals(deps![0].objectId, userSchema.getId(), 'the dep is the pinned schema');
            }
        },
        {
            name: '[CAT03] a genesis with a bad signature or a non-creator author is rejected',
            invoke: async () => {
                const { ctx, dev, userSchema } = await createEnv();
                const other = await makeIdentity();
                const pin = await frontierOf(userSchema);

                const tampered = await RCatalogImpl.create({
                    name: 'editor', creators: [{ keyId: dev.keyId, publicKey: dev.publicKey }], author: dev,
                    version: '1.0.0', add: [userDef(userSchema, pin)], params: [{ name: 'admin', type: 'identity' }],
                });
                tampered.note = 'tampered after signing';
                await rejects(() => ctx.createObject(tampered), 'could not be verified', 'a tampered genesis is rejected');

                const foreign = await RCatalogImpl.create({
                    name: 'editor', creators: [{ keyId: dev.keyId, publicKey: dev.publicKey }], author: other,
                    version: '1.0.0', add: [userDef(userSchema, pin)], params: [{ name: 'admin', type: 'identity' }],
                });
                await rejects(() => ctx.createObject(foreign), 'is not one of its creators', 'a genesis signed by a non-creator is rejected');

                const positioned = await RCatalogImpl.create({
                    name: 'editor', creators: [{ keyId: dev.keyId, publicKey: dev.publicKey }], author: dev,
                    version: '1.0.0', add: [userDef(userSchema, pin)], params: [{ name: 'admin', type: 'identity' }],
                });
                const resigned = await signPayload({ ...positioned, signature: '' } as unknown as json.LiteralMap, dev, version('elsewhere'));
                await rejects(() => ctx.createObject(resigned), 'could not be verified', 'a genesis signed at a non-empty position is rejected');
            }
        },
        {
            name: '[CAT04] release versions must exceed every parent; a patch branch after a newer minor is fine',
            invoke: async () => {
                const { ctx, dev, userSchema } = await createEnv();
                const catalog = await createCatalog(ctx, dev, [userDef(userSchema, await frontierOf(userSchema))]);
                const genesis = version(catalog.getId());

                await rejects(() => catalog.release({ version: '1.0.0' }, dev), 'is not greater than parent', 'an equal version is rejected');
                await rejects(() => catalog.release({ version: '0.9.0' }, dev), 'is not greater than parent', 'a lower version is rejected');

                const minor = await catalog.release({ version: '1.1.0' }, dev, genesis);
                const patch = await catalog.release({ version: '1.0.1' }, dev, genesis);

                const view = await catalog.getView();
                assertEquals(view.getMaximalReleases().join(','), [minor, patch].sort().join(','), 'two concurrent releases');
                assertTrue(view.isReleaseBelow(catalog.getId(), minor), 'the genesis is below 1.1.0');
                assertFalse(view.isReleaseBelow(patch, minor), '1.0.1 is not below 1.1.0');

                await rejects(() => catalog.release({ version: '1.1.0' }, dev), 'is not greater than parent', 'a merge must exceed both parents');
                const merge = await catalog.release({ version: '1.2.0' }, dev);
                const mergeState = (await catalog.getView()).getRelease(merge)!;
                assertEquals(mergeState.parents.join(','), [minor, patch].sort().join(','), 'the merge has both parents');
            }
        },
        {
            name: '[CAT05] a release pinning an undeclared schema is rejected; publishRelease declares it first',
            invoke: async () => {
                const { ctx, dev, userSchema, noteSchema } = await createEnv();
                const catalog = await createCatalog(ctx, dev, [userDef(userSchema, await frontierOf(userSchema))]);

                const notes: CatalogGroupDef = {
                    name: 'notes', seedSource: 'rdb', schemaRef: noteSchema.getId(),
                    schemaVersion: json.toSet([...(await frontierOf(noteSchema))]),
                };
                await rejects(() => catalog.release({ version: '1.1.0', add: [notes] }, dev),
                    'which is not declared', 'a release referencing an undeclared schema is rejected');

                const published = await catalog.publishRelease({ version: '1.1.0', add: [notes] }, dev);
                assertTrue(published.declare !== undefined, 'the schema is declared first');
                const view = await catalog.getView();
                assertTrue(view.getReferencedSchemas().includes(noteSchema.getId()), 'the new schema is referenced');
                const state = view.getRelease(published.release)!;
                assertEquals(state.parents.join(','), catalog.getId(), 'the declare is skipped: the genesis is the parent');
                assertTrue(state.groups.has(catalogGroupHash(notes)), 'the new group is in the state');

                const deps = catalog.extractForeignDeps(
                    (await (await catalog.getScopedDag()).loadEntry(published.release))!.payload, version(published.declare!));
                assertEquals(deps!.map((d) => d.objectId).join(','), noteSchema.getId(), 'the release depends on its new schema');
                assertEquals((catalog.extractForeignDeps(
                    (await (await catalog.getScopedDag()).loadEntry(published.declare!))!.payload, version(catalog.getId())) ?? []).length,
                    0, 'a declare has no deps');
            }
        },
        {
            name: '[CAT06] change checks: unknown groups, no-ops and versions below a parent are rejected',
            invoke: async () => {
                const { ctx, dev, userSchema } = await createEnv();
                const pin = await frontierOf(userSchema);
                const def = userDef(userSchema, pin);
                const catalog = await createCatalog(ctx, dev, [def]);
                const hash = catalogGroupHash(def);

                await userSchema.updateSchema([{ rule: 'add-column', table: 'caps', column: 'note', def: { type: 'string', nullable: true } }], dev);
                const v2 = await frontierOf(userSchema);

                await rejects(() => catalog.release({ version: '1.1.0', changes: { unknown: { schema: userSchema.getId(), version: json.toSet([...v2]) } } }, dev),
                    'change names unknown group', 'a change of an unknown group is rejected');
                await rejects(() => catalog.release({ version: '1.1.0', changes: { [hash]: { schema: userSchema.getId(), version: json.toSet([...pin]) } } }, dev),
                    'does not change its version', 'a no-op change is rejected');

                const up = await catalog.release({ version: '1.1.0', changes: { [hash]: { schema: userSchema.getId(), version: json.toSet([...v2]) } } }, dev);
                assertEquals((await catalog.getView()).getRelease(up)!.groups.get(hash)!.version.join(','), [...v2].sort().join(','), 'the change applies');

                await rejects(() => catalog.release({ version: '1.2.0', changes: { [hash]: { schema: userSchema.getId(), version: json.toSet([...pin]) } } }, dev),
                    'is below its version in parent', 'a change below the parent version is rejected');
                await rejects(() => catalog.release({ version: '1.2.0', add: [def] }, dev),
                    'is already defined', 're-adding an existing definition is rejected');
            }
        },
        {
            name: '[CAT07] merges must set groups their parents disagree on; one-branch groups need nothing',
            invoke: async () => {
                const { ctx, dev, userSchema, noteSchema } = await createEnv();
                const pin = await frontierOf(userSchema);
                const def = userDef(userSchema, pin);
                const catalog = await createCatalog(ctx, dev, [def]);
                const hash = catalogGroupHash(def);
                const genesis = version(catalog.getId());

                await userSchema.updateSchema([{ rule: 'add-column', table: 'caps', column: 'a', def: { type: 'string', nullable: true } }], dev, undefined, pin);
                const va = await frontierOf(userSchema);
                await userSchema.updateSchema([{ rule: 'add-column', table: 'identities', column: 'b', def: { type: 'string', nullable: true } }], dev, undefined, pin);
                const both = await frontierOf(userSchema);
                const vb = version(...[...both].filter((h) => !va.has(h)));

                const left = await catalog.release({ version: '1.1.0', changes: { [hash]: { schema: userSchema.getId(), version: json.toSet([...va]) } } }, dev, genesis);
                const notes: CatalogGroupDef = {
                    name: 'notes', seedSource: 'rdb', schemaRef: noteSchema.getId(),
                    schemaVersion: json.toSet([...(await frontierOf(noteSchema))]),
                };
                const right = (await catalog.publishRelease({
                    version: '1.0.1',
                    changes: { [hash]: { schema: userSchema.getId(), version: json.toSet([...vb]) } },
                    add: [notes],
                }, dev, genesis)).release;

                await rejects(() => catalog.release({ version: '1.2.0' }, dev),
                    'the merge must set it', 'a merge that leaves a disagreeing group unset is rejected');

                const merge = await catalog.release({
                    version: '1.2.0', changes: { [hash]: { schema: userSchema.getId(), version: json.toSet([...both]) } },
                }, dev);
                const state = (await catalog.getView()).getRelease(merge)!;
                assertEquals(state.parents.join(','), [left, right].sort().join(','), 'the merge has both parents');
                assertEquals(state.groups.get(hash)!.version.join(','), [...both].sort().join(','), 'the merge sets the disagreeing group');
                assertTrue(state.groups.has(catalogGroupHash(notes)), 'a group added on one branch is carried into the merge');
            }
        },
        {
            name: '[CAT08] group definition rules: names, bindings, aliases, deploy authority and row templates',
            invoke: async () => {
                const { ctx, dev, userSchema, docSchema } = await createEnv();
                const userPin = await frontierOf(userSchema);
                const docPin = await frontierOf(docSchema);
                const user = userDef(userSchema, userPin);
                const catalog = await createCatalog(ctx, dev, [user]);
                const userHash = catalogGroupHash(user);

                const publish = (def: CatalogGroupDef, v: string) => catalog.publishRelease({ version: v, add: [def] }, dev);

                await rejects(() => publish(docDef(docSchema, docPin, userHash, { name: 'user' }), '1.1.0'),
                    'is already used', 'a reused name is rejected');
                await rejects(() => publish(docDef(docSchema, docPin, 'not-a-group'), '1.1.0'),
                    'is not defined earlier', 'a binding to an undefined group is rejected');
                await rejects(() => publish(docDef(docSchema, docPin, userHash, { bindings: { owner: userHash }, idProvider: undefined }), '1.1.0'),
                    "schema target group 'user' is not bound", 'the schema target group must be bound');
                await rejects(() => publish(docDef(docSchema, docPin, userHash, {
                    idProvider: undefined,
                    canDeploy: { p: 'exists', table: 'user.caps', where: { grantee: '$author' } },
                }), '1.1.0'), 'requires an identity provider', 'an ALLOW DEPLOY IF over $author without a provider is rejected');

                await rejects(() => catalog.publishRelease({
                    version: '1.1.0',
                    add: [userDef(userSchema, userPin, {
                        name: 'staff',
                        initialRows: { caps: [{ values: { label: 'x' }, params: { grantee: { param: 'nobody' } } }] },
                    })],
                }, dev), "param ':nobody' is not declared", 'an undeclared row param is rejected');
                await rejects(() => catalog.publishRelease({
                    version: '1.1.0',
                    params: [{ name: 'helper', type: 'identity' }],
                    add: [userDef(userSchema, userPin, {
                        name: 'staff',
                        initialRows: { identities: [{ values: {}, params: { keyId: { param: 'admin' }, publicKey: { param: 'helper', fn: 'publicKey' } } }] },
                    })],
                }, dev), 'must come from the same identity param', 'provider keyId and publicKey must come from the same identity param');
                await rejects(() => catalog.release({ version: '1.1.0', params: [{ name: 'admin', type: 'identity' }] }, dev),
                    'is already declared', 'a param declared twice is rejected');

                const doc = docDef(docSchema, docPin, userHash, {
                    canDeploy: { p: 'exists', table: 'user.caps', where: { grantee: '$author' } },
                });
                const ok = await publish(doc, '1.1.0');
                const state = (await catalog.getView()).getRelease(ok.release)!;
                assertTrue(state.groups.has(catalogGroupHash(doc)), 'a valid bound definition is accepted');
            }
        },
    ],
};

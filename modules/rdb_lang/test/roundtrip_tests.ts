import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { createBasicCrypto, createIdentity, HASH_SHA256, SIGNING_ED25519, type OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import { json } from "@hyper-hyper-space/hhs3_json";
import { version } from "@hyper-hyper-space/hhs3_mvt";
import {
    RDbImpl, RSchemaImpl, RTableGroupImpl, normalizeDecimal, usersSchemaTables, validateRSchemaPayloadFormat,
    type CmpOp, type ColumnDef, type ColumnType, type MigrationRule, type Operand, type OpTag, type Predicate,
    type SchemaUpdatePayload, type TableDef,
} from "@hyper-hyper-space/hhs3_rdb";

import { createMockRContext } from "../../rdb/test/mock_rcontext.js";

import { bind } from "../src/bind/bind.js";
import { compileCreate } from "../src/compile/create.js";
import { compileMigrationRules } from "../src/compile/ddl.js";
import { lowerRestrictionPredicate } from "../src/compile/query.js";
import { columnSetFromTableDef, type ColumnsOf } from "../src/compile/rule_scope.js";
import { renderOp } from "../src/reverse/render.js";
import { parseStatement } from "../src/syntax/parser.js";
import type { AstStatement } from "../src/syntax/ast.js";
import { createTestBindContext } from "./mock_bind_context.js";

// Lossless rendering: for every valid payload p, compile(parse(render(p))) == p.
// Text is replayed with no keystore, as a full-profile dump must be.

const hashSuite = createBasicCrypto().hash(HASH_SHA256);

function keystorelessContext() {
    const lang = createTestBindContext(createMockRContext(), {});
    delete (lang as { resolvePublicKey?: unknown }).resolvePublicKey;
    return lang;
}

function parseOrThrow(text: string): AstStatement {
    const parsed = parseStatement(text);
    if (!parsed.ok) throw new Error(`parse failed: ${parsed.diagnostics.map((d) => d.message).join('; ')}`);
    return parsed.value;
}

async function recompileCreate(text: string): Promise<json.Literal> {
    const bound = await bind(parseOrThrow(text), keystorelessContext());
    if (!bound.ok) throw new Error(`bind failed: ${bound.diagnostics.map((d) => d.message).join('; ')}`);
    const kind = bound.value.kind;
    if (kind !== 'create-database' && kind !== 'create-schema' && kind !== 'create-tablegroup') {
        throw new Error(`expected a CREATE statement, got ${kind}`);
    }
    return (await compileCreate(bound.value)).payload as unknown as json.Literal;
}

function sameOrExplain(what: string, text: string, original: unknown, back: unknown): void {
    const a = json.toStringNormalized(original as json.Literal);
    const b = json.toStringNormalized(back as json.Literal);
    if (a !== b) {
        throw new Error(`${what}: round trip changed the payload\n--- rendered ---\n${text}\n--- original ---\n${a}\n--- recompiled ---\n${b}`);
    }
}

async function assertCreateRoundTrip(payload: unknown, what: string): Promise<string> {
    const text = renderOp(payload as json.Literal);
    let back: json.Literal;
    try {
        back = await recompileCreate(text);
    } catch (e) {
        throw new Error(`${what}: ${e instanceof Error ? e.message : String(e)}\n--- rendered ---\n${text}`);
    }
    sameOrExplain(what, text, payload, back);
    return text;
}

function columnsOfDefs(defs: TableDef[]): ColumnsOf {
    const map = new Map(defs.map((d) => [d.name, columnSetFromTableDef(d)]));
    return (ref) => (ref.includes('.') ? undefined : map.get(ref));
}

async function newIdentity(): Promise<OwnIdentity> {
    return createIdentity(SIGNING_ED25519, hashSuite);
}

// ---- fixtures ----

const eq = (left: Operand, right: Operand): Predicate => ({ p: 'cmp', cmp: 'eq', left, right });
const col = (c: string): Operand => ({ col: c });
const lit = (v: json.Literal): Operand => ({ lit: v });

function kitchenSinkTables(): TableDef[] {
    return [
        {
            name: 'table',
            columns: {
                identity: { type: 'identity', pub: true, readonly: true },
                length: { type: 'integer', pub: true, readonly: true, constraints: { min: '-10', max: '100' }, default: -3 },
                escape: { type: 'string', pub: true, readonly: true, constraints: { maxLength: 16 } },
                price: { type: 'decimal', readonly: true, constraints: { scale: 2 } },
                amount: { type: 'decimal', nullable: true, constraints: { precision: 10, scale: 3, min: '-1.500', max: '99.000' } },
                ratio: { type: 'float', default: -1.5e-7 },
                big: { type: 'bigint', readonly: true, default: '-123456789012345678901' },
                blob: { type: 'bytes', constraints: { maxLength: 8 }, default: 'AAEC' },
                doc: { type: 'json', default: { tags: ["it's", 'a\nb'], seq: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], nested: [1, [-2.5, true]] } },
                flag: { type: 'boolean', pub: true, readonly: true, default: false },
                owner: { type: 'string', readonly: true, default: "it's $5 -- /* ok */" },
            },
            fks: { owner: 'items', escape: 'users.identities' },
            concurrentDeletes: false,
            restrictions: [
                {
                    on: 'insert',
                    rule: {
                        p: 'and',
                        args: [
                            { p: 'or', args: [eq(col('identity'), lit('$author')), eq(col('rowAuthor'), lit('$author'))] },
                            { p: 'and', args: [
                                { p: 'cmp', cmp: 'le', left: { fn: 'add', args: [col('length'), { fn: 'mul', args: [lit(2), lit(-3)] }] }, right: lit(-1) },
                                { p: 'cmp', cmp: 'lt', left: { fn: 'len', args: [col('escape')] }, right: { fn: 'sub', args: [col('length'), { fn: 'sub', args: [lit(1), lit(2)] }] } },
                            ] },
                            { p: 'like', value: col('escape'), pattern: lit("100\\% it's_\\\\") },
                            { p: 'exists', table: 'table', where: { identity: '$row.identity', flag: true } },
                        ],
                    },
                },
                {
                    on: 'update',
                    rule: {
                        p: 'or',
                        args: [
                            { p: 'cmp', cmp: 'ge', left: { fn: 'mul', args: [{ fn: 'add', args: [col('price'), lit('1.50')] }, lit('2.00')] }, right: lit('0.00') },
                            { p: 'or', args: [
                                { p: 'cmp', cmp: 'gt', left: col('big'), right: lit('99999999999999999999') },
                                { p: 'like', value: col('owner'), pattern: col('escape') },
                            ] },
                            { p: 'exists', table: 'users.identities', where: { keyId: '$author' } },
                            { p: 'false' },
                        ],
                    },
                },
                { on: 'delete', rule: { p: 'true' } },
            ],
        },
        {
            name: 'items',
            columns: {
                keyId: { type: 'string', pub: true, readonly: true },
                publicKey: { type: 'string', pub: true, readonly: true },
                note: { type: 'string', nullable: true, pub: true },
            },
            idProvider: { keyIdColumn: 'publicKey', publicKeyColumn: 'keyId' },
            concurrentDeletes: true,
            restrictions: [{ on: 'all', rule: { p: 'exists', table: 'table', where: { escape: '$row.keyId', identity: '$author', length: -3 } } }],
        },
        {
            name: 'identities',
            columns: { keyId: { type: 'string', pub: true, readonly: true } },
            restrictions: [
                // same bare name as the cross-group target: the renderer must alias it
                { on: 'insert', rule: { p: 'exists', table: 'users.identities', where: { keyId: '$row.keyId' } } },
            ],
        },
    ];
}

// ---- random generator of valid-leaning schemas (invalid ones are skipped) ----

function mulberry32(seed: number): () => number {
    let s = seed;
    return () => {
        s = (s + 0x6D2B79F5) | 0;
        let t = Math.imul(s ^ (s >>> 15), 1 | s);
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}

type Rng = () => number;
const pick = <T>(r: Rng, xs: readonly T[]): T => xs[Math.floor(r() * xs.length)];
const shuffle = <T>(r: Rng, xs: readonly T[]): T[] => {
    const out = [...xs];
    for (let i = out.length - 1; i > 0; i--) {
        const j = Math.floor(r() * (i + 1));
        [out[i], out[j]] = [out[j], out[i]];
    }
    return out;
};

// Names include C-SQL keywords and contextual words, so quoting is exercised.
const TABLE_NAMES = ['items', 'caps', 'table', 'identity', 't', 'order', 'users', 'identities'];
const COLUMN_NAMES = ['name', 'qty', 'price', 'amount', 'flag', 'identity', 'length', 'escape', 'label', 'note',
    'limit', 'string', 'json', 'all', 'hash', 'algorithm', 'value', 'true', 'null'];
const TYPES: ColumnType[] = ['string', 'integer', 'float', 'boolean', 'bigint', 'decimal', 'bytes', 'identity', 'json'];
const STRINGS = ['abc', "it's", 'a--b', '/* x */', 'line\nbreak', 'back\\slash', '', 'Ünïcode ✓', 'x"y', '50%'];
const LIKE_PATTERNS = ['a%', '_b%', '100\\%', 'x\\\\y', "it's%", '%', '', '_', '%\\_%'];
const ORDERED: ColumnType[] = ['integer', 'float', 'string', 'bigint', 'decimal'];
const COMPARABLE: ColumnType[] = ['string', 'integer', 'float', 'boolean', 'bigint', 'decimal', 'bytes', 'identity'];
const CMP_OPS: CmpOp[] = ['eq', 'ne', 'lt', 'le', 'gt', 'ge'];

function randomLiteral(r: Rng, type: ColumnType, scale = 2): json.Literal {
    switch (type) {
        case 'string': return pick(r, STRINGS);
        case 'integer': return Math.floor(r() * 101) - 50;
        case 'float': return pick(r, [0.5, -2.25, 1.5e-7, 3.75, -1e-9, 1234.5]);
        case 'boolean': return r() < 0.5;
        case 'bigint': return pick(r, ['0', '-5', '123456789012345678901', '42']);
        case 'decimal': return normalizeDecimal(((Math.floor(r() * 2000) - 1000) / 10 ** scale).toFixed(scale), scale)!;
        case 'bytes': return pick(r, ['AAEC', 'AQID', 'SGk=']);
        case 'identity': return pick(r, ['keyhash1', 'abcDEF123']);
        case 'json': return pick<json.Literal>(r, [
            [1, 2, 3], 7, true, [[1], [-2.5]], [], -0.5, 'top-level string', {},
            { tags: ["it's", 'a"b', 'back\\slash', 'line\nbreak\ttab'], nested: { n: -1.5e-7, ok: false } },
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], [{ k: 'v' }, 'x', 1],
        ]);
    }
}

function randomColumn(r: Rng): ColumnDef {
    const type = pick(r, TYPES);
    const def: ColumnDef = { type };
    let scale = 2;
    if (type === 'decimal') {
        scale = pick(r, [0, 2, 3]);
        const precision = pick(r, [undefined, 6, 10]);
        def.constraints = precision === undefined ? { scale } : { precision, scale };
    } else if ((type === 'string' || type === 'bytes') && r() < 0.3) {
        def.constraints = { maxLength: 16 };
    } else if (type === 'integer' && r() < 0.3) {
        def.constraints = { min: '-100', max: '100' };
    }
    if (r() < 0.3) def.nullable = true;
    if (r() < 0.6) def.pub = true;
    if (r() < 0.7) def.readonly = true;
    if (r() < 0.3) def.default = randomLiteral(r, type, scale);
    return def;
}

type GenScope = { table: TableDef; tables: TableDef[] };

function readonlyCols(scope: GenScope, type: ColumnType): string[] {
    const cols = Object.entries(scope.table.columns).filter(([, d]) => d.readonly && d.type === type).map(([n]) => n);
    return type === 'string' ? [...cols, 'rowAuthor'] : cols;
}

function scaleOf(def: ColumnDef | undefined): number {
    return def?.constraints?.scale ?? 2;
}

function randomOperand(r: Rng, scope: GenScope, type: ColumnType, depth: number): Operand {
    if ((type === 'integer' || type === 'bigint' || type === 'decimal') && depth < 2 && r() < 0.25) {
        return { fn: pick(r, ['add', 'sub', 'mul'] as const), args: [randomOperand(r, scope, type, depth + 1), randomOperand(r, scope, type, depth + 1)] };
    }
    if (type === 'integer' && depth < 2 && r() < 0.15) return { fn: 'len', args: [randomOperand(r, scope, 'string', depth + 1)] };
    const cols = readonlyCols(scope, type);
    if (cols.length > 0 && r() < 0.6) return { col: pick(r, cols) };
    if ((type === 'identity' || type === 'string') && r() < 0.3) return { lit: '$author' };
    return { lit: randomLiteral(r, type) };
}

function randomExists(r: Rng, scope: GenScope): Predicate {
    if (r() < 0.15) return { p: 'exists', table: 'users.identities', where: { keyId: '$author' } };
    const target = pick(r, scope.tables);
    const pubCols = Object.entries(target.columns).filter(([, d]) => d.pub).map(([n]) => n);
    const fields = shuffle(r, pubCols).slice(0, 1 + Math.floor(r() * 2));
    if (fields.length === 0) return { p: 'exists', table: target.name, where: { rowAuthor: '$author' } };
    const where: { [field: string]: json.Literal } = {};
    for (const field of fields) {
        const def = target.columns[field];
        const rowCols = readonlyCols(scope, def.type).filter((c) => c !== 'rowAuthor');
        const roll = r();
        if ((def.type === 'string' || def.type === 'identity') && roll < 0.3) where[field] = '$author';
        else if (rowCols.length > 0 && roll < 0.6) where[field] = `$row.${pick(r, rowCols)}`;
        else where[field] = randomLiteral(r, def.type, scaleOf(def));
    }
    return { p: 'exists', table: target.name, where };
}

function randomPredicate(r: Rng, scope: GenScope, depth: number): Predicate {
    const roll = r();
    if (depth < 3 && roll < 0.3) {
        const n = 2 + Math.floor(r() * 2);
        return { p: r() < 0.5 ? 'and' : 'or', args: Array.from({ length: n }, () => randomPredicate(r, scope, depth + 1)) };
    }
    if (roll < 0.55) {
        const type = pick(r, COMPARABLE);
        const cmp = pick(r, ORDERED.includes(type) ? CMP_OPS : ['eq', 'ne'] as CmpOp[]);
        return { p: 'cmp', cmp, left: randomOperand(r, scope, type, 0), right: randomOperand(r, scope, type, 0) };
    }
    if (roll < 0.7) {
        const strCols = readonlyCols(scope, 'string');
        return {
            p: 'like',
            value: r() < 0.7 ? { col: pick(r, strCols) } : { lit: pick(r, STRINGS) },
            pattern: r() < 0.2 ? { col: pick(r, strCols) } : { lit: pick(r, LIKE_PATTERNS) },
        };
    }
    if (roll < 0.9) return randomExists(r, scope);
    return { p: r() < 0.5 ? 'true' : 'false' };
}

function randomTables(r: Rng): TableDef[] {
    const names = shuffle(r, TABLE_NAMES).slice(0, 1 + Math.floor(r() * 3));
    const tables: TableDef[] = names.map((name) => {
        const columns: { [c: string]: ColumnDef } = {};
        for (const c of shuffle(r, COLUMN_NAMES).slice(0, 1 + Math.floor(r() * 5))) columns[c] = randomColumn(r);
        return { name, columns };
    });
    for (const table of tables) {
        const cols = Object.keys(table.columns);
        if (r() < 0.3) table.fks = { [pick(r, cols)]: r() < 0.7 ? pick(r, tables).name : 'users.identities' };
        if (r() < 0.3) table.concurrentDeletes = r() < 0.5;
        const ops: OpTag[] = r() < 0.2 ? ['all'] : (['insert', 'update', 'delete'] as OpTag[]).filter(() => r() < 0.5);
        if (ops.length > 0) table.restrictions = ops.map((on) => ({ on, rule: randomPredicate(r, { table, tables }, 0) }));
    }
    return tables;
}

export const roundTripTests = {
    title: '[RDB_LANG:ROUNDTRIP] Lossless render / parse / compile',
    tests: [
        {
            name: '[RT01] kitchen-sink CREATE SCHEMA round-trips (keywords, arithmetic, LIKE escapes, EXISTS aliasing, decimal(*, s))',
            invoke: async () => {
                const admin = await newIdentity();
                const other = await newIdentity();
                const payload = await RSchemaImpl.create({
                    name: 'table', creators: [admin, other], tables: kitchenSinkTables(), hashAlgorithm: 'sha256',
                });
                const valid = validateRSchemaPayloadFormat(payload as unknown as json.Literal);
                assertTrue(valid.valid, `fixture must be valid: ${JSON.stringify(valid)}`);
                const text = await assertCreateRoundTrip(payload, 'kitchen sink');
                assertTrue(text.includes('decimal(*, 2)'), 'precision-less decimal renders with *');
                assertTrue(text.includes(`publicKey('`), 'creators render as public keys');
                assertTrue(text.includes("HASH ALGORITHM 'sha256'"), 'hash algorithm renders');
                assertTrue(text.includes('EXISTS users.identities AS i WHERE i.keyId = identities.keyId'), 'same-named cross-group EXISTS is aliased');
            },
        },
        {
            name: '[RT02] the Users schema round-trips',
            invoke: async () => {
                const admin = await newIdentity();
                const payload = await RSchemaImpl.create({ name: 'users_schema', creators: [admin], tables: usersSchemaTables() });
                const text = await assertCreateRoundTrip(payload, 'users schema');
                assertTrue(text.includes('"identity" string'), 'endpoints.identity is quoted');
            },
        },
        {
            name: '[RT03] generated CREATE SCHEMA payloads round-trip',
            invoke: async () => {
                const admin = await newIdentity();
                // Each construct must show up in some valid case, or the generator has drifted.
                const constructs: { [name: string]: RegExp } = {
                    'ALLOW rule': /\bALLOW (insert|update|delete|all) IF /, 'nested group': /\(\(|AND \(|OR \(/,
                    'arithmetic': / [+*-] /, 'length()': /length\(/, 'LIKE': / LIKE /, 'EXISTS': /EXISTS /,
                    'EXISTS alias': /EXISTS \S+ AS /, 'cross-group EXISTS': /EXISTS users\.identities/,
                    '$row correlation': /WHERE [^;]*= "?[a-z]+"?\."?[a-z]+"?/, 'quoted identifier': /"[a-z]+"/,
                    'decimal(*, s)': /decimal\(\*, /, 'decimal(p, s)': /decimal\(\d+, /, 'negative literal': /[=<>] -\d/,
                    'FK': /REFERENCES/, 'CONCURRENT DELETES': /CONCURRENT DELETES/,
                    'JSON literal': /DEFAULT JSON '/, 'JSON where-value': /= JSON '/,
                };
                const seen = new Set<string>();
                let checked = 0;
                const iterations = 300;
                for (let seed = 1; seed <= iterations; seed++) {
                    const payload = await RSchemaImpl.create({ name: 's', creators: [admin], tables: randomTables(mulberry32(seed)) });
                    if (!validateRSchemaPayloadFormat(payload as unknown as json.Literal).valid) continue;
                    const text = await assertCreateRoundTrip(payload, `seed ${seed}`);
                    for (const [name, re] of Object.entries(constructs)) if (re.test(text)) seen.add(name);
                    checked += 1;
                }
                assertTrue(checked >= iterations / 2, `most generated schemas should be valid (checked ${checked} of ${iterations})`);
                const missing = Object.keys(constructs).filter((name) => !seen.has(name));
                assertEquals(missing.join(', '), '', 'every construct is exercised by some generated case');
            },
        },
        {
            name: '[RT04] CREATE DATABASE round-trips (keyword name, seed, public-key creators, hash algorithm)',
            invoke: async () => {
                const admin = await newIdentity();
                const payload = await RDbImpl.create({ seed: "seed 'x'", name: 'order', creators: [admin], hashAlgorithm: 'sha256' });
                const text = await assertCreateRoundTrip(payload, 'database');
                assertTrue(text.startsWith('CREATE DATABASE "order"'), 'keyword database name is quoted');
            },
        },
        {
            name: '[RT05] ALTER SCHEMA round-trips its rules and NOTE',
            invoke: async () => {
                const base = kitchenSinkTables();
                const added: TableDef = {
                    name: 'limit',
                    columns: { value: { type: 'decimal', readonly: true, constraints: { scale: 4 } } },
                    restrictions: [{ on: 'all', rule: { p: 'cmp', cmp: 'gt', left: col('value'), right: lit('0.0000') } }],
                };
                const migration: MigrationRule[] = [
                    { rule: 'add-table', def: added },
                    { rule: 'add-column', table: 'items', column: 'json', def: { type: 'json', nullable: true, default: [-1] } },
                    { rule: 'drop-column', table: 'table', column: 'identity' },
                    { rule: 'set-concurrent-deletes', table: 'table', value: true },
                    { rule: 'set-fks', table: 'table', fks: { owner: 'limit' } },
                    { rule: 'set-restrictions', table: 'items', restrictions: [
                        { on: 'insert', rule: { p: 'or', args: [{ p: 'and', args: [eq(col('keyId'), lit('$author')), { p: 'true' }] }, { p: 'like', value: col('note'), pattern: lit('a\\_b%') }] } },
                    ] },
                    { rule: 'drop-table', table: 'identities' },
                ];
                const payload: SchemaUpdatePayload = {
                    action: 'schema-update', migration, note: "v2: it's -- fine", author: 'AUTHORKEY', signature: 'SIG',
                };
                const text = renderOp(payload as unknown as json.Literal, { schemaRef: 'SCHEMAREF' });
                const ast = parseOrThrow(text);
                if (ast.kind !== 'alter-schema') throw new Error(`expected alter-schema, got ${ast.kind}`);
                assertEquals(ast.note, payload.note, 'NOTE round-trips');
                const rules = compileMigrationRules(ast.rules, columnsOfDefs([...base, added]));
                sameOrExplain('alter schema', text, migration, rules);
            },
        },
        {
            name: '[RT06] CREATE TABLEGROUP gates, identity provider and hash algorithm round-trip',
            invoke: async () => {
                const tables = kitchenSinkTables();
                const canDeploy: Predicate = {
                    p: 'or',
                    args: [
                        { p: 'exists', table: 'table', where: { identity: '$author', flag: true } },
                        { p: 'exists', table: 'users.caps', where: { label: "it's", grantee: '$author' } },
                    ],
                };
                const canObserve: { [binding: string]: Predicate } = {
                    users: { p: 'and', args: [{ p: 'and', args: [{ p: 'exists', table: 'items', where: { keyId: '$author' } }, { p: 'true' }] }, { p: 'false' }] },
                };
                const payload = await RTableGroupImpl.create({
                    name: 'identity', seed: 'seed', schemaRef: 'SCHEMAREF', schemaVersion: version('VERSIONA'),
                    bindings: { users: 'GROUPREF' }, idProvider: 'users.identities', canDeploy, canObserve, hashAlgorithm: 'sha256',
                });
                const text = renderOp(payload as unknown as json.Literal);
                const ast = parseOrThrow(text);
                if (ast.kind !== 'create-tablegroup') throw new Error(`expected create-tablegroup, got ${ast.kind}`);
                assertEquals(ast.name, 'identity', 'keyword group name');
                assertEquals(ast.hashAlgorithm, 'sha256', 'HASH ALGORITHM round-trips');
                assertEquals(ast.idProvider, 'users.identities', 'identity provider');
                const scope = { columnsOf: columnsOfDefs(tables) };
                if (ast.canDeploy === undefined) throw new Error('missing canDeploy');
                sameOrExplain('canDeploy', text, canDeploy, lowerRestrictionPredicate(ast.canDeploy, scope));
                assertEquals(ast.canObserve.length, 1, 'one observe gate');
                sameOrExplain('canObserve', text, canObserve.users, lowerRestrictionPredicate(ast.canObserve[0].predicate, scope));
            },
        },
    ],
};

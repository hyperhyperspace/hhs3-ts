import { testing } from '@hyper-hyper-space/hhs3_util';
import { checkFormat, Type, Format, FormatOptions } from '../format.js';
import { Literal, LiteralMap } from '../literal.js';
import { isLiteral, isWellFormedString, literalProblem, MAX_LITERAL_DEPTH, toStringNormalized } from '../normalize.js';

async function testStrictRejectsUnknownKeys() {
    const format: Format = {
        name: Type.String,
        age: Type.Int32,
    };

    const valid = { name: 'Alice', age: 30 };
    const extra = { name: 'Alice', age: 30, email: 'a@b.com' };

    testing.assertTrue(checkFormat(format, valid), 'exact match should pass in strict mode');
    testing.assertFalse(checkFormat(format, extra), 'extra keys should fail in strict mode (default)');
    testing.assertFalse(checkFormat(format, extra, { strict: true }), 'extra keys should fail with explicit strict: true');
}

async function testNonStrictAllowsUnknownKeys() {
    const format: Format = {
        name: Type.String,
        age: Type.Int32,
    };

    const extra = { name: 'Alice', age: 30, email: 'a@b.com' };
    testing.assertTrue(checkFormat(format, extra, { strict: false }), 'extra keys should pass with strict: false');
}

async function testStrictRejectsNestedUnknownKeys() {
    const innerFormat: Format = {
        x: Type.Int32,
    };

    const format: Format = {
        point: innerFormat,
    };

    const valid = { point: { x: 5 } };
    const extra = { point: { x: 5, y: 10 } };

    testing.assertTrue(checkFormat(format, valid), 'nested exact match should pass');
    testing.assertFalse(checkFormat(format, extra), 'nested extra keys should fail in strict mode');
    testing.assertTrue(checkFormat(format, extra, { strict: false }), 'nested extra keys should pass with strict: false');
}

async function testStrictWithOptionalFields() {
    const format: Format = {
        name: Type.String,
        age: [Type.Option, Type.Int32],
    };

    const withOptional = { name: 'Bob', age: 25 };
    const withoutOptional = { name: 'Bob' };
    const withExtra = { name: 'Bob', hobby: 'chess' };
    const withOptionalAndExtra = { name: 'Bob', age: 25, hobby: 'chess' };

    testing.assertTrue(checkFormat(format, withOptional), 'with optional field should pass');
    testing.assertTrue(checkFormat(format, withoutOptional), 'without optional field should pass');
    testing.assertFalse(checkFormat(format, withExtra), 'extra key without optional should fail strict');
    testing.assertFalse(checkFormat(format, withOptionalAndExtra), 'extra key with optional should fail strict');
}

async function testStrictWithArrayElements() {
    const format: Format = [Type.Array, {
        id: Type.Int32,
    }];

    const valid: Literal = [{ id: 1 }, { id: 2 }];
    const extra: Literal = [{ id: 1 }, { id: 2, name: 'x' }];

    testing.assertTrue(checkFormat(format, valid), 'array of exact objects should pass');
    testing.assertFalse(checkFormat(format, extra), 'array with extra keys in element should fail strict');
    testing.assertTrue(checkFormat(format, extra, { strict: false }), 'array with extra keys should pass non-strict');
}

async function testStrictWithBoundedArray() {
    const format: Format = [Type.BoundedArray, {
        val: Type.String,
    }, 10];

    const valid: Literal = [{ val: 'a' }];
    const extra: Literal = [{ val: 'a', extra: 'b' }];

    testing.assertTrue(checkFormat(format, valid), 'bounded array of exact objects should pass');
    testing.assertFalse(checkFormat(format, extra), 'bounded array with extra keys should fail strict');
}

async function testStrictWithUnion() {
    const formatA: Format = { kind: [Type.Constant, 'a'], x: Type.Int32 };
    const formatB: Format = { kind: [Type.Constant, 'b'], y: Type.String };
    const format: Format = [Type.Union, [formatA, formatB]];

    const validA = { kind: 'a', x: 42 };
    const validB = { kind: 'b', y: 'hello' };
    const extraA = { kind: 'a', x: 42, z: true };

    testing.assertTrue(checkFormat(format, validA), 'union match A should pass');
    testing.assertTrue(checkFormat(format, validB), 'union match B should pass');
    testing.assertFalse(checkFormat(format, extraA), 'union with extra keys should fail strict');
    testing.assertTrue(checkFormat(format, extraA, { strict: false }), 'union with extra keys should pass non-strict');
}

async function testBoundedMapBasic() {
    const format: Format = [Type.BoundedMap,
        [Type.BoundedString, 32],
        { val: Type.Int32 },
        3,
    ];

    const valid = { a: { val: 1 }, b: { val: 2 } };
    const empty = {};
    const tooMany = { a: { val: 1 }, b: { val: 2 }, c: { val: 3 }, d: { val: 4 } };
    const badValue = { a: { val: 'not a number' } };
    const extraField = { a: { val: 1, extra: true } };
    const keyTooLong = { ['x'.repeat(33)]: { val: 1 } };

    testing.assertTrue(checkFormat(format, valid), 'valid bounded map should pass');
    testing.assertTrue(checkFormat(format, empty), 'empty map should pass');
    testing.assertFalse(checkFormat(format, tooMany), 'exceeding max entries should fail');
    testing.assertFalse(checkFormat(format, badValue as any), 'bad value format should fail');
    testing.assertFalse(checkFormat(format, extraField), 'extra fields in value should fail strict');
    testing.assertFalse(checkFormat(format, keyTooLong), 'key exceeding max length should fail');
}

async function testBoundedMapRejectsNonObjects() {
    const format: Format = [Type.BoundedMap, Type.String, Type.Int32, 10];

    testing.assertFalse(checkFormat(format, 'not an object'), 'string should fail');
    testing.assertFalse(checkFormat(format, 42), 'number should fail');
    testing.assertFalse(checkFormat(format, [1, 2, 3]), 'array should fail');
    testing.assertFalse(checkFormat(format, true), 'boolean should fail');
}

async function testBoundedMapNonStrictAllowsExtraFields() {
    const format: Format = [Type.BoundedMap,
        Type.String,
        { val: Type.Int32 },
        10,
    ];

    const extra = { a: { val: 1, extra: true } };
    testing.assertFalse(checkFormat(format, extra), 'extra fields in value should fail strict');
    testing.assertTrue(checkFormat(format, extra, { strict: false }), 'extra fields should pass non-strict');
}

async function testStrictPreservesExistingBehavior() {
    testing.assertTrue(checkFormat(Type.String, 'hello'), 'string format should pass');
    testing.assertFalse(checkFormat(Type.String, 42), 'string format should reject number');
    testing.assertTrue(checkFormat(Type.Int32, 42), 'int32 format should pass');
    testing.assertTrue(checkFormat(Type.Boolean, true), 'boolean format should pass');
    testing.assertTrue(checkFormat(Type.Something, 'anything'), 'something format should pass');
    testing.assertTrue(checkFormat([Type.Constant, 'add'], 'add'), 'constant format should pass');
    testing.assertFalse(checkFormat([Type.Constant, 'add'], 'remove'), 'constant format should reject mismatch');
}

const formatTests = {
    title: '[FORMAT] checkFormat strict mode',
    tests: [
        { name: '[FORMAT_00] strict rejects unknown keys', invoke: testStrictRejectsUnknownKeys },
        { name: '[FORMAT_01] non-strict allows unknown keys', invoke: testNonStrictAllowsUnknownKeys },
        { name: '[FORMAT_02] strict rejects nested unknown keys', invoke: testStrictRejectsNestedUnknownKeys },
        { name: '[FORMAT_03] strict with optional fields', invoke: testStrictWithOptionalFields },
        { name: '[FORMAT_04] strict with array elements', invoke: testStrictWithArrayElements },
        { name: '[FORMAT_05] strict with bounded array', invoke: testStrictWithBoundedArray },
        { name: '[FORMAT_06] strict with union', invoke: testStrictWithUnion },
        { name: '[FORMAT_07] preserves existing non-object behavior', invoke: testStrictPreservesExistingBehavior },
        { name: '[FORMAT_08] bounded map basic', invoke: testBoundedMapBasic },
        { name: '[FORMAT_09] bounded map rejects non-objects', invoke: testBoundedMapRejectsNonObjects },
        { name: '[FORMAT_10] bounded map non-strict allows extra fields', invoke: testBoundedMapNonStrictAllowsExtraFields },
    ]
};

async function testCanonicalKeepsArrayOrder() {
    const arr: Literal = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
    const text = toStringNormalized(arr);
    testing.assertEquals(text, '[0,1,2,3,4,5,6,7,8,9,10,11]', 'arrays serialize in index order');
    testing.assertEquals(JSON.stringify(JSON.parse(text)), JSON.stringify(arr), '12-element array round-trips');
}

async function testCanonicalEscapesStrings() {
    const value: Literal = { 'k"\n': ['line\nbreak\ttab', 'back\\slash "q"', '\u0001', 'Ünï ✓'], a: -0, b: 1.5e-7, c: false };
    const text = toStringNormalized(value);
    testing.assertTrue(text.startsWith('{"a":0,"b":1.5e-7,"c":false,'), `object keys sorted, -0 as 0 (got ${text})`);
    testing.assertTrue([...text].every((ch) => ch.charCodeAt(0) >= 0x20), 'no control character is left raw in the text');
    testing.assertEquals(toStringNormalized(JSON.parse(text)), text, 'the text parses back to a value with the same text');
    testing.assertEquals(JSON.parse(text)['k"\n'][0], 'line\nbreak\ttab', 'control characters survive JSON.parse');
}

async function testCanonicalRejectsNonLiterals() {
    const throws = (v: unknown) => { try { toStringNormalized(v as Literal); return false; } catch { return true; } };
    testing.assertTrue(throws(null), 'top-level null throws');
    testing.assertTrue(throws([1, null]), 'nested null throws');
    testing.assertTrue(throws({ a: Number.NaN }), 'NaN throws');
}

async function testIsLiteral() {
    testing.assertTrue(isLiteral({ a: [1, 'x', true, { b: [] }] }), 'nested literal');
    testing.assertFalse(isLiteral(null), 'null');
    testing.assertFalse(isLiteral([1, null]), 'nested null in array');
    testing.assertFalse(isLiteral({ a: { b: null } }), 'nested null in object');
    testing.assertFalse(isLiteral([Number.POSITIVE_INFINITY]), 'infinity');
    testing.assertFalse(isLiteral(undefined), 'undefined');
}

async function testIsWellFormedString() {
    testing.assertTrue(isWellFormedString(''), 'empty string');
    testing.assertTrue(isWellFormedString('plain ✓'), 'BMP text');
    testing.assertTrue(isWellFormedString('a😀b'), 'a surrogate pair');
    testing.assertTrue(isWellFormedString('x�y'), 'U+FFFD is an ordinary character');
    testing.assertFalse(isWellFormedString('x\ud800y'), 'lone high surrogate');
    testing.assertFalse(isWellFormedString('x\ud800'), 'lone high surrogate at the end');
    testing.assertFalse(isWellFormedString('\udc00x'), 'lone low surrogate at the start');
    testing.assertFalse(isWellFormedString('\ude00\ud83d'), 'a pair in the wrong order');
    testing.assertFalse(isWellFormedString('\ud83d😀'), 'a high surrogate before a pair');
}

async function testLiteralProblem() {
    testing.assertTrue(literalProblem({ a: [1, 'x�y', true, { b: [] }] }) === undefined, 'a literal has no problem');

    testing.assertFalse(isLiteral('x\ud800y'), 'ill-formed string');
    testing.assertFalse(isLiteral({ a: [{ b: 'x\udc00' }] }), 'ill-formed string at depth');
    testing.assertFalse(isLiteral({ a: { 'k\ud800': 1 } }), 'ill-formed key at depth');

    testing.assertEquals(literalProblem([1, null]), 'null', 'names null');
    testing.assertEquals(literalProblem({ a: Number.NaN }), 'a non-finite number', 'names a non-finite number');
    testing.assertEquals(literalProblem(['x\ud800y']), 'a string that is not well-formed Unicode', 'names an ill-formed string');
    testing.assertEquals(literalProblem({ 'k\ud800': 1 }), 'a key that is not well-formed Unicode', 'names an ill-formed key');
    testing.assertEquals(literalProblem([undefined]), 'a value of type undefined', 'names an illegal type');
}

const fromCodePoints = (...points: number[]) => String.fromCodePoint(...points);

// The examples RFC 8785 gives. Their characters are built from code points, so
// that what this test checks does not depend on how this file is encoded.
async function testRfc8785Examples() {
    const BS = fromCodePoints(0x5c), DQ = fromCodePoints(0x22), APOS = fromCodePoints(0x27);
    const quoted = (s: string) => DQ + s + DQ;
    const EURO = fromCodePoints(0x20ac), CR = fromCodePoints(0x0d), DALET = fromCodePoints(0xfb33);
    const GRIN = fromCodePoints(0x1f600), C80 = fromCodePoints(0x80), OUML = fromCodePoints(0xf6);

    // Section 3.2.3. Keys sort by UTF-16 code unit: the emoji is the pair
    // D83D DE00, so it sorts before U+FB33 although its code point is higher.
    const sorting: LiteralMap = {};
    sorting[EURO] = 'Euro Sign';
    sorting[CR] = 'Carriage Return';
    sorting[DALET] = 'Hebrew Letter Dalet With Dagesh';
    sorting['1'] = 'One';
    sorting[GRIN] = 'Emoji: Grinning Face';
    sorting[C80] = 'Control';
    sorting[OUML] = 'Latin Small Letter O With Diaeresis';
    testing.assertEquals(toStringNormalized(sorting), '{' + [
        quoted(BS + 'r') + ':' + quoted('Carriage Return'),
        quoted('1') + ':' + quoted('One'),
        quoted(C80) + ':' + quoted('Control'),
        quoted(OUML) + ':' + quoted('Latin Small Letter O With Diaeresis'),
        quoted(EURO) + ':' + quoted('Euro Sign'),
        quoted(GRIN) + ':' + quoted('Emoji: Grinning Face'),
        quoted(DALET) + ':' + quoted('Hebrew Letter Dalet With Dagesh'),
    ].join(',') + '}', 'section 3.2.3: keys sorted by UTF-16 code unit');

    // Section 3.2.2, without the null the RFC lists under "literals" (a Literal
    // has no null). The string is: EURO $ U+000F U+000A A ' B " \ \ " /
    const primitives: Literal = {
        numbers: [333333333.33333329, 1E30, 4.50, 2e-3, 0.000000000000000000000000001],
        string: EURO + '$' + fromCodePoints(0x0f, 0x0a) + 'A' + APOS + 'B' + DQ + BS + BS + DQ + '/',
        literals: [true, false],
    };
    testing.assertEquals(toStringNormalized(primitives),
        '{' + quoted('literals') + ':[true,false],'
            + quoted('numbers') + ':[333333333.3333333,1e+30,4.5,0.002,1e-27],'
            + quoted('string') + ':' + quoted(EURO + '$' + BS + 'u000f' + BS + 'n' + 'A' + APOS + 'B'
                + BS + DQ + BS + BS + BS + BS + BS + DQ + '/') + '}',
        'section 3.2.2: numbers and string escapes');

    // Appendix B: a double, as its 64 bits in hex, and its text.
    const samples: [string, string][] = [
        ['0000000000000000', '0'],
        ['8000000000000000', '0'],
        ['0000000000000001', '5e-324'],
        ['8000000000000001', '-5e-324'],
        ['7fefffffffffffff', '1.7976931348623157e+308'],
        ['ffefffffffffffff', '-1.7976931348623157e+308'],
        ['4340000000000000', '9007199254740992'],
        ['c340000000000000', '-9007199254740992'],
        ['4430000000000000', '295147905179352830000'],
        ['44b52d02c7e14af5', '9.999999999999997e+22'],
        ['44b52d02c7e14af6', '1e+23'],
        ['44b52d02c7e14af7', '1.0000000000000001e+23'],
        ['444b1ae4d6e2ef4e', '999999999999999700000'],
        ['444b1ae4d6e2ef4f', '999999999999999900000'],
        ['444b1ae4d6e2ef50', '1e+21'],
        ['3eb0c6f7a0b5ed8c', '9.999999999999997e-7'],
        ['3eb0c6f7a0b5ed8d', '0.000001'],
        ['41b3de4355555553', '333333333.3333332'],
        ['41b3de4355555554', '333333333.33333325'],
        ['41b3de4355555555', '333333333.3333333'],
        ['41b3de4355555556', '333333333.3333334'],
        ['41b3de4355555557', '333333333.33333343'],
        ['becbf647612f3696', '-0.0000033333333333333333'],
        ['43143ff3c1cb0959', '1424953923781206.2'],
    ];
    const view = new DataView(new ArrayBuffer(8));
    for (const [bits, text] of samples) {
        view.setBigUint64(0, BigInt('0x' + bits));
        testing.assertEquals(toStringNormalized(view.getFloat64(0)), text, 'appendix B: ' + bits);
    }
}

function nested(depth: number, leaf: Literal, wrap: (inner: Literal) => Literal): Literal {
    let value = leaf;
    for (let i = 0; i < depth; i++) value = wrap(value);
    return value;
}

// A scalar has depth 0; an array or object has one more than its deepest
// member. A Literal nests at most MAX_LITERAL_DEPTH levels.
async function testNestingLimit() {
    const inArray = (inner: Literal): Literal => [inner];
    const inObject = (inner: Literal): Literal => ({ a: inner });
    const tooDeep = 'more than ' + MAX_LITERAL_DEPTH + ' levels of nesting';

    testing.assertEquals(MAX_LITERAL_DEPTH, 128, 'the limit is 128 levels');

    testing.assertTrue(literalProblem('scalar', 0) === undefined, 'a scalar has depth 0');
    testing.assertEquals(literalProblem([], 0), 'more than 0 levels of nesting', 'an empty array has depth 1');
    testing.assertTrue(literalProblem({}, 1) === undefined, 'an empty object fits in one level');

    const wraps: [string, (inner: Literal) => Literal][] = [['arrays', inArray], ['objects', inObject]];
    for (const [label, wrap] of wraps) {
        testing.assertTrue(isLiteral(nested(MAX_LITERAL_DEPTH, 1, wrap)), `${MAX_LITERAL_DEPTH} levels of ${label} is a Literal`);
        testing.assertEquals(literalProblem(nested(MAX_LITERAL_DEPTH + 1, 1, wrap)), tooDeep, `one more level of ${label} is not`);
    }

    testing.assertEquals(literalProblem({ shallow: [1], deep: nested(MAX_LITERAL_DEPTH, 1, inArray) }), tooDeep,
        'the deepest member decides');

    const wrapped = { payload: nested(MAX_LITERAL_DEPTH, 1, inArray) };
    testing.assertTrue(literalProblem(wrapped, MAX_LITERAL_DEPTH + 1) === undefined,
        'a caller can allow for its own level of wrapping');
    testing.assertEquals(literalProblem([wrapped], MAX_LITERAL_DEPTH + 1), 'more than ' + (MAX_LITERAL_DEPTH + 1) + ' levels of nesting',
        'the message names the bound in use');

    testing.assertEquals(literalProblem(nested(MAX_LITERAL_DEPTH, null as unknown as Literal, inArray)), 'null',
        'another problem at the deepest allowed level is still named');

    // JSON.parse reads this; the count rejects it long before the stack runs out.
    const veryDeep: unknown = JSON.parse('['.repeat(100000) + ']'.repeat(100000));
    testing.assertEquals(literalProblem(veryDeep), tooDeep, 'a value 100000 levels deep is named, not a stack overflow');
}

const canonicalTests = {
    title: '[CANONICAL] toStringNormalized (RFC 8785) and isLiteral',
    tests: [
        { name: '[CANONICAL_00] arrays keep index order', invoke: testCanonicalKeepsArrayOrder },
        { name: '[CANONICAL_01] strings fully escaped, keys sorted', invoke: testCanonicalEscapesStrings },
        { name: '[CANONICAL_02] rejects null and non-finite numbers', invoke: testCanonicalRejectsNonLiterals },
        { name: '[CANONICAL_03] isLiteral rejects null at any depth', invoke: testIsLiteral },
        { name: '[CANONICAL_04] isWellFormedString rejects lone surrogates', invoke: testIsWellFormedString },
        { name: '[CANONICAL_05] isLiteral rejects ill-formed strings and keys, literalProblem names the problem', invoke: testLiteralProblem },
        { name: '[CANONICAL_06] toStringNormalized gives the text of the RFC 8785 examples', invoke: testRfc8785Examples },
        { name: '[CANONICAL_07] a Literal nests at most 128 levels', invoke: testNestingLimit },
    ]
};

const allSuites = [formatTests, canonicalTests];

async function main() {
    const filters = process.argv.slice(2);
    console.log('Running tests for HHSv3 json module' + (filters.length > 0 ? ` (filter: ${filters})` : '') + '\n');

    for (const suite of allSuites) {
        console.log(suite.title);
        for (const test of suite.tests) {
            let match = true;
            for (const filter of filters) {
                match = match && test.name.indexOf(filter) >= 0;
            }
            if (match) {
                testing.exitIfFailed(await testing.run(test.name, test.invoke));
            } else {
                await testing.skip(test.name);
            }
        }
        console.log();
    }
}

main();

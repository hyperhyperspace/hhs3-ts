import { testing } from '@hyper-hyper-space/hhs3_util';
import { checkFormat, Type, Format, FormatOptions } from '../format.js';
import { Literal } from '../literal.js';

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
    ]
};

const allSuites = [formatTests];

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
                const result = await testing.run(test.name, test.invoke);
                if (!result) return;
            } else {
                await testing.skip(test.name);
            }
        }
        console.log();
    }
}

main();

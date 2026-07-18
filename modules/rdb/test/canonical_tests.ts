import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import {
    isCanonicalBigint, normalizeBigint,
    isCanonicalDecimal, normalizeDecimal, parseDecimal, formatDecimal,
    isCanonicalBase64, normalizeBase64, base64ByteLen,
    intInRange, bigintInRange, decInRange,
    compareNumericStr, bigintArith, decimalArith,
} from "../src/rschema/canonical.js";

async function testBigintCanonical() {
    assertTrue(isCanonicalBigint('0'), '0 is canonical');
    assertTrue(isCanonicalBigint('123'), 'positive is canonical');
    assertTrue(isCanonicalBigint('-123'), 'negative is canonical');
    assertFalse(isCanonicalBigint('-0'), '-0 is not canonical');
    assertFalse(isCanonicalBigint('007'), 'leading zeros not canonical');
    assertFalse(isCanonicalBigint('+5'), 'leading plus not canonical');
    assertFalse(isCanonicalBigint('1.0'), 'decimal point not a bigint');

    assertTrue(normalizeBigint('007') === '7', 'normalize strips leading zeros');
    assertTrue(normalizeBigint('-0') === '0', 'normalize collapses -0 to 0');
    assertTrue(normalizeBigint(42) === '42', 'normalize accepts a number');
    assertTrue(normalizeBigint(4.2) === undefined, 'normalize rejects a non-integer number');
    assertTrue(normalizeBigint('abc') === undefined, 'normalize rejects non-numeric');
}

async function testDecimalCanonical() {
    assertTrue(isCanonicalDecimal('10.50', 2), 'exact-scale decimal is canonical');
    assertFalse(isCanonicalDecimal('10.5', 2), 'wrong-scale decimal is not canonical');
    assertFalse(isCanonicalDecimal('10.500', 2), 'over-scale decimal is not canonical');
    assertTrue(isCanonicalDecimal('0.00', 2), 'canonical zero at scale 2');
    assertFalse(isCanonicalDecimal('-0.00', 2), '-0.00 is not canonical (should be 0.00)');
    assertTrue(isCanonicalDecimal('-0.50', 2), 'negative nonzero is canonical');
    assertTrue(isCanonicalDecimal('5', 0), 'scale-0 decimal with no point is canonical');
    assertFalse(isCanonicalDecimal('5.0', 0), 'scale-0 decimal must have no point');

    // precision bound
    assertTrue(isCanonicalDecimal('0.99', 2, 2), '0.99 fits precision 2');
    assertFalse(isCanonicalDecimal('1.00', 2, 2), '1.00 (unscaled 100) exceeds precision 2');

    assertTrue(normalizeDecimal('1.5', 2) === '1.50', 'normalize pads to scale');
    assertTrue(normalizeDecimal('1', 2) === '1.00', 'normalize integer input to scale');
    assertTrue(normalizeDecimal(1.5, 2) === '1.50', 'normalize numeric input');
    assertTrue(normalizeDecimal('1.555', 2) === undefined, 'normalize rejects over-scale (never rounds)');
    assertTrue(normalizeDecimal('-0.00', 2) === '0.00', 'normalize collapses -0.00');

    const p = parseDecimal('-12.34');
    assertTrue(p !== undefined && p.unscaled === BigInt(-1234) && p.scale === 2, 'parseDecimal splits sign/scale');
    assertTrue(formatDecimal(BigInt(-50), 2) === '-0.50', 'formatDecimal renders negative sub-one');
    assertTrue(formatDecimal(BigInt(0), 2) === '0.00', 'formatDecimal renders canonical zero');
    assertTrue(formatDecimal(BigInt(1250), 2) === '12.50', 'formatDecimal renders integer+fraction');
}

async function testBase64Canonical() {
    assertTrue(isCanonicalBase64(''), 'empty is canonical base64');
    assertTrue(isCanonicalBase64('AAAA'), 'AAAA is canonical (3 zero bytes)');
    assertTrue(isCanonicalBase64('AA=='), 'AA== is canonical (1 byte)');
    assertFalse(isCanonicalBase64('AB=='), 'AB== has non-zero padding bits -> not canonical');
    assertFalse(isCanonicalBase64('AAA'), 'length not a multiple of 4');
    assertTrue(base64ByteLen('AA==') === 1, 'AA== decodes to 1 byte');
    assertTrue(base64ByteLen('AAAA') === 3, 'AAAA decodes to 3 bytes');
    assertTrue(normalizeBase64('AB==') === 'AA==', 'normalize zeroes the padding bits');
    assertTrue(normalizeBase64('!!!') === undefined, 'normalize rejects invalid base64');
}

async function testRanges() {
    assertTrue(intInRange(5, { min: '0', max: '10' }), '5 in [0,10]');
    assertFalse(intInRange(-1, { min: '0' }), '-1 below min 0');
    assertTrue(bigintInRange('100', { max: '100' }), '100 <= max 100 (inclusive)');
    assertFalse(bigintInRange('101', { max: '100' }), '101 > max 100');
    assertTrue(decInRange('10.50', { min: '0.00', max: '100.00' }), 'decimal within range');
    assertFalse(decInRange('100.01', { max: '100.00' }), 'decimal above max');
}

async function testNumericCompare() {
    // bigint compares by value, not lexically ('9' > '10' lexically, but 9 < 10)
    assertTrue(compareNumericStr('9', '10', 'bigint') < 0, 'bigint 9 < 10 by value');
    assertTrue(compareNumericStr('-5', '3', 'bigint') < 0, 'bigint -5 < 3');
    assertTrue(compareNumericStr('10', '10', 'bigint') === 0, 'bigint equal');

    // decimal compares by value across differing scales
    assertTrue(compareNumericStr('2.5', '2.50', 'decimal') === 0, 'decimal 2.5 == 2.50');
    assertTrue(compareNumericStr('2.5', '2.05', 'decimal') > 0, 'decimal 2.5 > 2.05');
    assertTrue(compareNumericStr('9.99', '10.00', 'decimal') < 0, 'decimal 9.99 < 10.00 (not lexical)');

    // integer/float compare numerically
    assertTrue(compareNumericStr(2, 10, 'integer') < 0, 'integer 2 < 10');
    assertTrue(compareNumericStr(1.5, 1.25, 'float') > 0, 'float 1.5 > 1.25');
}

async function testArithmetic() {
    assertTrue(bigintArith('add', '9', '10') === '19', 'bigint add');
    assertTrue(bigintArith('sub', '5', '8') === '-3', 'bigint sub');
    assertTrue(bigintArith('mul', '12', '12') === '144', 'bigint mul');
    assertTrue(bigintArith('add', '007', '1') === undefined, 'bigint arith rejects non-canonical input');

    // huge exact bigint arithmetic (beyond Number range)
    assertTrue(bigintArith('mul', '10000000000000000000', '2') === '20000000000000000000', 'exact large bigint mul');

    // decimal add/sub align to the larger scale; mul sums scales
    assertTrue(decimalArith('add', '1.50', '2.25') === '3.75', 'decimal add same scale');
    assertTrue(decimalArith('add', '1.5', '2.25') === '3.75', 'decimal add aligns scales');
    assertTrue(decimalArith('sub', '2.00', '0.50') === '1.50', 'decimal sub');
    assertTrue(decimalArith('mul', '1.50', '2.00') === '3.0000', 'decimal mul sums scales');
}

export const canonicalTests = {
    title: '[CANON] Canonical carriers, ranges, compare and arithmetic',
    tests: [
        { name: '[CANON01] bigint canonical form', invoke: testBigintCanonical },
        { name: '[CANON02] decimal canonical form', invoke: testDecimalCanonical },
        { name: '[CANON03] base64 canonical form', invoke: testBase64Canonical },
        { name: '[CANON04] range checks', invoke: testRanges },
        { name: '[CANON05] numeric comparison', invoke: testNumericCompare },
        { name: '[CANON06] exact arithmetic', invoke: testArithmetic },
    ],
};

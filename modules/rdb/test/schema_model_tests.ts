import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { json } from "@hyper-hyper-space/hhs3_json";

import {
    ColumnDef, TableDef, tableDefFormat,
    Predicate, Restriction,
    MigrationRule, migrationRuleFormat,
} from "../src/rschema/payload.js";
import { defaultRestrictionRule, splitTableRef } from "../src/rschema/payload.js";
import {
    validateTableDef, validateSchemaTables,
    validatePredicate, validateFKs, validateRestrictions,
    validateMigrationRule,
    validateColumnDef,
    columnValueMatchesType, columnValueValid, columnValueValidReason,
    isValidName, isValidSchemaName, isValidTableRef,
    MAX_EXPR_DEPTH,
} from "../src/rschema/validate.js";

function ordersTable(): TableDef {
    return {
        name: 'orders',
        columns: {
            customer: { type: 'string' },
            total: { type: 'float' },
            notes: { type: 'string', nullable: true },
        },
    };
}

function linesTable(): TableDef {
    return {
        name: 'lines',
        columns: {
            order: { type: 'string' },
            sku: { type: 'string' },
            qty: { type: 'integer' },
        },
        fks: { order: 'orders' },
    };
}

function capsTable(): TableDef {
    return {
        name: 'caps',
        columns: {
            label: { type: 'string', pub: true },
            grantee: { type: 'string', pub: true },
            note: { type: 'string', nullable: true },
        },
        concurrentDeletes: true,
        restrictions: [{ on: 'insert', rule: { p: 'false' } }],
    };
}

async function testNames() {
    assertTrue(isValidName('orders'), 'plain name should be valid');
    assertTrue(isValidName('_private2'), 'underscore-led name should be valid');
    assertFalse(isValidName('2orders'), 'digit-led name should be invalid');
    assertFalse(isValidName('or ders'), 'name with space should be invalid');
    assertFalse(isValidName(''), 'empty name should be invalid');
    assertFalse(isValidName('a.b'), 'qualified name should not be a plain name');

    assertTrue(isValidSchemaName('shop'), 'plain schema name should be valid');
    assertTrue(isValidSchemaName('hhs:users'), 'colon-qualified schema name should be valid');
    assertTrue(isValidSchemaName('acme:shop_v2'), 'multi-segment schema name should allow underscores and digits');
    assertFalse(isValidSchemaName('hhs:'), 'trailing colon should be invalid');
    assertFalse(isValidSchemaName(':users'), 'leading colon should be invalid');
    assertFalse(isValidSchemaName('hhs:2users'), 'colon segment must start with identifier start');
    assertFalse(isValidSchemaName('hhs.users'), 'dot-qualified schema name should be invalid');

    assertTrue(isValidTableRef('orders'), 'local ref should be valid');
    assertTrue(isValidTableRef('users.capabilities'), 'qualified ref should be valid');
    assertFalse(isValidTableRef('a.b.c'), 'doubly-qualified ref should be invalid');

    const [group, table] = splitTableRef('users.capabilities');
    assertTrue(group === 'users' && table === 'capabilities', 'splitTableRef should split qualified refs');
    const [noGroup, local] = splitTableRef('orders');
    assertTrue(noGroup === undefined && local === 'orders', 'splitTableRef should pass local refs through');
}

async function testColumnTypes() {
    assertTrue(columnValueMatchesType('x', 'string'), 'string value should match string column');
    assertTrue(columnValueMatchesType(42, 'integer'), 'integer value should match integer column');
    assertFalse(columnValueMatchesType(4.2, 'integer'), 'float value should not match integer column');
    assertTrue(columnValueMatchesType(4.2, 'float'), 'float value should match float column');
    assertTrue(columnValueMatchesType(true, 'boolean'), 'boolean value should match boolean column');
    assertTrue(columnValueMatchesType({ a: [1, 2] }, 'json'), 'object value should match json column');
    assertFalse(columnValueMatchesType('x', 'integer'), 'string value should not match integer column');

    // integer is now bounded to the JS safe-integer range
    assertFalse(columnValueMatchesType(Number.MAX_SAFE_INTEGER + 1, 'integer'),
        'an unsafe integer should not match integer column');

    // new string-carried types
    assertTrue(columnValueMatchesType('123', 'bigint'), 'canonical bigint string should match bigint column');
    assertFalse(columnValueMatchesType('007', 'bigint'), 'non-canonical bigint (leading zeros) should not match');
    assertFalse(columnValueMatchesType(123, 'bigint'), 'a number should not match a bigint column carrier');
    assertTrue(columnValueMatchesType('AAAA', 'bytes'), 'canonical base64 should match bytes column');
    assertFalse(columnValueMatchesType('AAA', 'bytes'), 'non-multiple-of-4 base64 should not match bytes column');

    // columnValueValid is constraint-aware
    const dec2: ColumnDef = { type: 'decimal', constraints: { scale: 2 } };
    assertTrue(columnValueValid('10.50', dec2), 'canonical decimal at scale should be valid');
    assertFalse(columnValueValid('10.5', dec2), 'decimal with wrong scale should be rejected (never rounded)');
    assertFalse(columnValueValid('10.505', dec2), 'over-scale decimal should be rejected (never rounded)');

    const boundedInt: ColumnDef = { type: 'integer', constraints: { min: '0', max: '1000' } };
    assertTrue(columnValueValid(500, boundedInt), 'in-range integer should be valid');
    assertFalse(columnValueValid(-1, boundedInt), 'below-min integer should be rejected');
    assertFalse(columnValueValid(1001, boundedInt), 'above-max integer should be rejected');

    const boundedBig: ColumnDef = { type: 'bigint', constraints: { min: '0' } };
    assertTrue(columnValueValid('99999999999999999999999999', boundedBig), 'large canonical bigint should be valid');
    assertFalse(columnValueValid('-1', boundedBig), 'below-min bigint should be rejected');

    const cappedStr: ColumnDef = { type: 'string', constraints: { maxLength: 3 } };
    assertTrue(columnValueValid('abc', cappedStr), 'string within maxLength should be valid');
    assertFalse(columnValueValid('abcd', cappedStr), 'string over maxLength should be rejected');
}

async function testColumnConstraintValidation() {
    // decimal requires a scale
    assertTrue(validateColumnDef({ type: 'decimal', constraints: { scale: 2 } }) === undefined,
        'decimal with scale should validate');
    assertTrue(validateColumnDef({ type: 'decimal' }) !== undefined,
        'decimal without a scale should not validate');
    assertTrue(validateColumnDef({ type: 'decimal', constraints: { scale: 2, precision: 1 } }) !== undefined,
        'decimal with precision < scale should not validate');
    assertTrue(validateColumnDef({ type: 'decimal', constraints: { scale: 2, precision: 4 } }) === undefined,
        'decimal with precision >= scale should validate');

    // min <= max
    assertTrue(validateColumnDef({ type: 'integer', constraints: { min: '5', max: '1' } }) !== undefined,
        'min > max should not validate');
    assertTrue(validateColumnDef({ type: 'integer', constraints: { min: '1', max: '5' } }) === undefined,
        'min <= max should validate');

    // non-canonical bound rejected
    assertTrue(validateColumnDef({ type: 'bigint', constraints: { min: '007' } }) !== undefined,
        'non-canonical bigint bound should not validate');

    // default must satisfy type + constraints
    assertTrue(validateColumnDef({ type: 'decimal', constraints: { scale: 2 }, default: '1.00' }) === undefined,
        'in-form decimal default should validate');
    assertTrue(validateColumnDef({ type: 'decimal', constraints: { scale: 2 }, default: '1.5' }) !== undefined,
        'wrong-scale decimal default should not validate');

    // anti-fungibility: inapplicable constraint keys are a hard reject
    assertTrue(validateColumnDef({ type: 'string', constraints: { min: '0' } }) !== undefined,
        'min on a string column should be rejected (not applicable)');
    assertTrue(validateColumnDef({ type: 'integer', constraints: { maxLength: 4 } }) !== undefined,
        'maxLength on an integer column should be rejected (not applicable)');
    assertTrue(validateColumnDef({ type: 'float', constraints: { min: '0' } }) !== undefined,
        'any constraint on a float column should be rejected (float takes none)');
    assertTrue(validateColumnDef({ type: 'boolean', constraints: { maxLength: 1 } }) !== undefined,
        'any constraint on a boolean column should be rejected');
    assertTrue(validateColumnDef({ type: 'string', constraints: { maxLength: 8 } }) === undefined,
        'maxLength on a string column should validate');
    assertTrue(validateColumnDef({ type: 'bytes', constraints: { maxLength: 32 } }) === undefined,
        'maxLength on a bytes column should validate');
    assertTrue(validateColumnDef({ type: 'bigint', constraints: { min: '0', max: '100' } }) === undefined,
        'min/max on a bigint column should validate');
}

async function testColumnValueReasons() {
    // valid values yield no reason
    assertTrue(columnValueValidReason('ok', { type: 'string' }) === undefined, 'valid string has no reason');
    assertTrue(columnValueValidReason('10.50', { type: 'decimal', constraints: { scale: 2 } }) === undefined,
        'valid decimal has no reason');

    // reasons distinguish carrier mismatch from constraint violations
    const cappedStr: ColumnDef = { type: 'string', constraints: { maxLength: 3 } };
    const tooLong = columnValueValidReason('abcd', cappedStr);
    assertTrue(tooLong !== undefined && tooLong.includes('maxLength 3') && tooLong.includes('length 4'),
        `string over maxLength names the limit: ${tooLong}`);
    const notString = columnValueValidReason(5, cappedStr);
    assertTrue(notString !== undefined && notString.includes('expected a string') && notString.includes('number'),
        `wrong carrier names the expected/actual type: ${notString}`);

    const boundedInt: ColumnDef = { type: 'integer', constraints: { min: '0', max: '1000' } };
    const oob = columnValueValidReason(1001, boundedInt);
    assertTrue(oob !== undefined && oob.includes('out of range') && oob.includes('[0, 1000]'),
        `out-of-range integer names the bounds: ${oob}`);

    const dec2: ColumnDef = { type: 'decimal', constraints: { scale: 2 } };
    const badScale = columnValueValidReason('10.5', dec2);
    assertTrue(badScale !== undefined && badScale.includes('canonical decimal') && badScale.includes('scale=2'),
        `wrong-scale decimal names the scale: ${badScale}`);

    const bigCol: ColumnDef = { type: 'bigint' };
    const badBig = columnValueValidReason('007', bigCol);
    assertTrue(badBig !== undefined && badBig.includes('canonical bigint'),
        `non-canonical bigint is reported as such: ${badBig}`);

    const bytes8: ColumnDef = { type: 'bytes', constraints: { maxLength: 1 } };
    const tooManyBytes = columnValueValidReason('AAAA', bytes8);   // 'AAAA' -> 3 bytes
    assertTrue(tooManyBytes !== undefined && tooManyBytes.includes('byte length') && tooManyBytes.includes('maxLength 1'),
        `over-length bytes names the byte count: ${tooManyBytes}`);

    // the boolean wrapper stays consistent with the reason function
    assertTrue(columnValueValid('abc', cappedStr) === (columnValueValidReason('abc', cappedStr) === undefined),
        'columnValueValid agrees with columnValueValidReason (valid)');
    assertFalse(columnValueValid('abcd', cappedStr), 'columnValueValid agrees with columnValueValidReason (invalid)');
}

async function testTableDefFormatAndValidation() {
    const orders = ordersTable();
    assertTrue(json.checkFormat(tableDefFormat, orders), 'well-formed table def should pass format');
    assertTrue(validateTableDef(orders) === undefined, 'well-formed table def should validate');

    const badType = { name: 'orders', columns: { x: { type: 'varchar' } } };
    assertFalse(json.checkFormat(tableDefFormat, badType as json.Literal), 'unknown column type should fail format');

    const noColumns: TableDef = { name: 'orders', columns: {} };
    assertTrue(json.checkFormat(tableDefFormat, noColumns), 'empty columns is structurally fine');
    assertTrue(validateTableDef(noColumns) !== undefined, 'table without columns should not validate');

    const badDefault: TableDef = { name: 'orders', columns: { x: { type: 'integer', default: 'nope' } } };
    assertTrue(validateTableDef(badDefault) !== undefined, 'default not matching column type should not validate');

    const badColumnName: TableDef = { name: 'orders', columns: { '2x': { type: 'string' } } };
    assertTrue(validateTableDef(badColumnName) !== undefined, 'invalid column name should not validate');

    const colonTableName: TableDef = { name: 'my:table', columns: { x: { type: 'string' } } };
    assertTrue(
        validateTableDef(colonTableName)?.includes("invalid table name 'my:table'") ?? false,
        'colon in table name should produce a specific reason');

    // pub and readonly are independent ColumnDef modifiers
    const withModifiers: TableDef = { name: 'caps', columns: {
        label: { type: 'string', pub: true, readonly: true },
        tag: { type: 'string', pub: true },
        code: { type: 'string', readonly: true },
    } };
    assertTrue(json.checkFormat(tableDefFormat, withModifiers) && validateTableDef(withModifiers) === undefined,
        'pub/readonly modifiers in any combination should pass format and validate');
}

async function testRestrictionDefaults() {
    const insertDefault = defaultRestrictionRule('insert');
    assertTrue(insertDefault.p === 'true', 'insert should default to allowed for anyone');

    const updateDefault = defaultRestrictionRule('update');
    const deleteDefault = defaultRestrictionRule('delete');
    assertTrue(json.toStringNormalized(updateDefault) === json.toStringNormalized({ p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: '$author' } })
        && json.toStringNormalized(deleteDefault) === json.toStringNormalized({ p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: '$author' } }),
        'update/delete should default to insert-author-only (anonymous rows immutable)');
}

async function testFKs() {
    const columns = linesTable().columns;

    assertTrue(validateFKs({ order: 'orders' }, columns) === undefined, 'FK over existing column should validate');
    assertTrue(validateFKs({ order: 'users.identities' }, columns) === undefined, 'qualified FK target should validate');

    assertTrue(validateFKs({ nope: 'orders' }, columns) !== undefined, 'FK over missing column should not validate');
    assertTrue(validateFKs({ order: 'a.b.c' }, columns) !== undefined, 'FK with invalid target ref should not validate');
    assertTrue(validateFKs({ '2bad': 'orders' }) !== undefined, 'FK with invalid column name should not validate');
}

async function testPredicates() {
    assertTrue(validatePredicate({ p: 'true' }), 'true should validate');
    assertTrue(validatePredicate({ p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: '$author' } }),
        'author-is-author should validate');
    assertFalse(validatePredicate({ p: 'owner', is: '$author' }),
        'owner atom should not validate');
    assertFalse(validatePredicate({ p: 'not', arg: { p: 'true' } }), 'negation should not validate (positive logic only)');
    assertFalse(validatePredicate({ p: 'nope' }), 'unknown predicate should not validate');

    assertTrue(validatePredicate({ p: 'exists', table: 'users.caps', where: { label: 'write', grantee: '$author' } }),
        'exists with where + author term should validate');
    assertTrue(validatePredicate({ p: 'exists', table: 'caps', where: { label: 'admin' } }),
        'exists with where only should validate');
    assertTrue(validatePredicate({ p: 'exists', table: 'caps', where: { granted_to: '$author' } }),
        'exists with a $-term in a where value should validate');

    assertFalse(validatePredicate({ p: 'exists', table: 'orders' }),
        'vacuous exists should not validate');
    assertFalse(validatePredicate({ p: 'exists', table: 'orders', pk: 'someRowId' }),
        'exists with a concrete row id should not validate (schemas never name row ids)');
    assertFalse(validatePredicate({ p: 'exists', table: 'orders', pkOf: 'order' }),
        'exists referencing subject row values should not validate');
    assertFalse(validatePredicate({ p: 'exists', table: 'a.b.c', where: { grantee: '$author' } }),
        'exists with invalid table ref should not validate');
    assertFalse(validatePredicate({ p: 'exists', table: 'orders', where: { '2bad': 1 } }),
        'where with invalid field name should not validate');
    assertFalse(validatePredicate({ p: 'exists', table: 'caps', where: { label: '$nope' } }),
        '$-prefixed strings that are not terms should not validate (reserved)');
    assertFalse(validatePredicate({ p: 'exists', table: 'caps', ownerIs: 'author' }),
        'old ownerIs form should not validate');

    const authorOrAdmin: Predicate = {
        p: 'or', args: [
            { p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: '$author' } },
            { p: 'exists', table: 'users.caps', where: { role: 'admin', grantee: '$author' } },
        ],
    };
    assertTrue(validatePredicate(authorOrAdmin), 'author-or-admin disjunction should validate');

    assertFalse(validatePredicate({ p: 'and', args: [] }), 'empty conjunction should not validate');

    let deep: json.Literal = { p: 'true' };
    for (let i = 0; i <= MAX_EXPR_DEPTH + 1; i++) {
        deep = { p: 'and', args: [deep] };
    }
    assertFalse(validatePredicate(deep), 'predicate beyond max depth should not validate');

    // cmp / str atoms + operands (Tier 1)
    assertTrue(validatePredicate({ p: 'cmp', cmp: 'eq', left: { col: 'a' }, right: { lit: 1 } }),
        'cmp over a $row column and a literal should validate');
    assertTrue(validatePredicate({ p: 'cmp', cmp: 'ge', left: { col: 'a' }, right: { fn: 'add', args: [{ col: 'b' }, { lit: 1 }] } }),
        'cmp with a nested arithmetic operand should validate');
    assertTrue(validatePredicate({ p: 'cmp', cmp: 'lt', left: { fn: 'len', args: [{ col: 'name' }] }, right: { lit: 8 } }),
        'cmp over len() should validate');
    assertTrue(validatePredicate({ p: 'str', str: 'prefix', value: { col: 'path' }, sub: { lit: '/x' } }),
        'str prefix should validate');

    assertFalse(validatePredicate({ p: 'cmp', cmp: 'eq', left: { col: 'a' } }),
        'cmp missing an operand should not validate');
    assertFalse(validatePredicate({ p: 'cmp', cmp: 'between', left: { col: 'a' }, right: { lit: 1 } }),
        'unknown cmp operator should not validate');
    assertFalse(validatePredicate({ p: 'cmp', cmp: 'eq', left: { col: 'a' }, right: { lit: 1 }, extra: 1 }),
        'extra keys on a cmp atom should not validate');
    assertFalse(validatePredicate({ p: 'cmp', cmp: 'eq', left: { col: '2bad' }, right: { lit: 1 } }),
        'cmp over an invalid column name should not validate');
    assertFalse(validatePredicate({ p: 'str', str: 'matches', value: { col: 'a' }, sub: { lit: 'x' } }),
        'unknown str operator should not validate');
    assertFalse(validatePredicate({ p: 'cmp', cmp: 'eq', left: { fn: 'div', args: [{ col: 'a' }, { lit: 1 }] }, right: { lit: 1 } }),
        'unknown arithmetic fn should not validate');
    assertFalse(validatePredicate({ p: 'cmp', cmp: 'eq', left: { fn: 'add', args: [{ col: 'a' }] }, right: { lit: 1 } }),
        'add with wrong arity should not validate');

    // $row.<col> as an exists where-value (Tier 2 correlation)
    assertTrue(validatePredicate({ p: 'exists', table: 'grants', where: { resource: '$row.resource', grantee: '$author' } }),
        'exists correlating a where field to $row.<col> should validate');
    assertFalse(validatePredicate({ p: 'exists', table: 'grants', where: { resource: '$row.2bad' } }),
        'a malformed $row term in a where value should not validate (reserved)');
}

async function testPredicateContexts() {
    // 'object' context: no subject-row fields, but $author where-values are valid.
    assertTrue(validatePredicate({ p: 'exists', table: 'users.caps', where: { label: 'deploy', grantee: '$author' } }, 'object'),
        'canDeploy-style predicate should validate in object context');
    assertFalse(validatePredicate({ p: 'owner', is: '$author' }, 'object'),
        'owner atom should not validate');
    assertFalse(validatePredicate({ p: 'exists', table: 'caps', where: { granted_to: '$rowOwner' } }, 'object'),
        '$rowOwner in a where value should not validate');
    assertFalse(validatePredicate({
        p: 'and', args: [{ p: 'true' }, { p: 'cmp', cmp: 'eq', left: { col: 'a' }, right: { lit: 1 } }],
    }, 'object'), 'context should be enforced through and/or nesting');

    // $row references have no subject row in object context
    assertFalse(validatePredicate({ p: 'cmp', cmp: 'eq', left: { col: 'a' }, right: { lit: 1 } }, 'object'),
        'a $row column operand should not validate in object context');
    assertFalse(validatePredicate({ p: 'exists', table: 'caps', where: { resource: '$row.resource' } }, 'object'),
        '$row.<col> in a where value should not validate in object context');
    assertTrue(validatePredicate({ p: 'cmp', cmp: 'eq', left: { lit: 1 }, right: { lit: 1 } }, 'object'),
        'a literal-only cmp is well-formed even in object context');
}

function docsTable(restrictions: Restriction[]): TableDef {
    return {
        name: 'docs',
        columns: { body: { type: 'string' }, cfg: { type: 'string' } },
        restrictions,
    };
}

async function testSchemaTables() {
    assertTrue(validateSchemaTables([ordersTable(), linesTable()]) === undefined, 'consistent schema should validate');

    assertTrue(validateSchemaTables([ordersTable(), ordersTable()]) !== undefined, 'duplicate table names should not validate');

    assertTrue(validateSchemaTables([linesTable()]) !== undefined, 'local FK target missing from schema should not validate');

    const crossGroup = docsTable([{ on: 'update', rule: { p: 'exists', table: 'users.caps', where: { label: 'write', grantee: '$author' } } }]);
    assertTrue(validateSchemaTables([crossGroup]) === undefined, 'qualified restriction target should be deferred to binding-time checking');

    const localSearch = docsTable([{ on: 'all', rule: { p: 'exists', table: 'caps', where: { label: 'write', grantee: '$author' } } }]);
    assertTrue(validateSchemaTables([localSearch, capsTable()]) === undefined,
        'local restriction over a pub column should validate');

    const overNonPub = docsTable([{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { note: 'x', grantee: '$author' } } }]);
    assertTrue(validateSchemaTables([overNonPub, capsTable()]) !== undefined,
        'restriction where over a non-pub column should not validate');

    const overMissingField = docsTable([{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { nope: 'x' } } }]);
    assertTrue(validateSchemaTables([overMissingField, capsTable()]) !== undefined,
        'restriction where over a missing column should not validate');

    const nestedAtoms = docsTable([{
        on: 'update',
        rule: { p: 'or', args: [
            { p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: '$author' } },
            { p: 'and', args: [
                { p: 'exists', table: 'caps', where: { label: 'write', grantee: '$author' } },
                { p: 'exists', table: 'caps', where: { grantee: '$author' } },
            ] },
        ] },
    }]);
    assertTrue(validateSchemaTables([nestedAtoms, capsTable()]) === undefined,
        'exists atoms nested under and/or should be checked and validate');

    const nestedBadAtom = docsTable([{
        on: 'update',
        rule: { p: 'or', args: [
            { p: 'cmp', cmp: 'eq', left: { col: 'rowAuthor' }, right: { lit: '$author' } },
            { p: 'exists', table: 'caps', where: { note: 'x' } },
        ] },
    }]);
    assertTrue(validateSchemaTables([nestedBadAtom, capsTable()]) !== undefined,
        'a non-pub where nested under and/or should not validate');

    // Tier 1: cmp/str over the declaring table's own columns ($row.<col>),
    // which must be readonly and type-coherent.
    const itemsWith = (rule: Predicate): TableDef => ({
        name: 'items',
        columns: {
            priority: { type: 'integer', readonly: true },
            status: { type: 'string' },                       // mutable
            resource: { type: 'string', readonly: true },
        },
        restrictions: [{ on: 'insert', rule }],
    });

    assertTrue(validateSchemaTables([itemsWith({ p: 'cmp', cmp: 'lt', left: { col: 'priority' }, right: { lit: 3 } })]) === undefined,
        'cmp over a readonly integer column vs an integer literal should validate');
    assertTrue(validateSchemaTables([itemsWith({ p: 'cmp', cmp: 'eq', left: { col: 'status' }, right: { lit: 'open' } })]) !== undefined,
        'cmp over a mutable (non-readonly) column should not validate');
    assertTrue(validateSchemaTables([itemsWith({ p: 'cmp', cmp: 'eq', left: { col: 'missing' }, right: { lit: 1 } })]) !== undefined,
        'cmp over a missing column should not validate');
    assertTrue(validateSchemaTables([itemsWith({ p: 'cmp', cmp: 'eq', left: { col: 'priority' }, right: { lit: 'x' } })]) !== undefined,
        'cmp comparing an integer column to a string literal should not validate (type mismatch)');
    assertTrue(validateSchemaTables([itemsWith({ p: 'cmp', cmp: 'eq', left: { fn: 'add', args: [{ col: 'resource' }, { lit: 1 }] }, right: { lit: 2 } })]) !== undefined,
        'arithmetic over a string column should not validate');

    // Tier 2: $row.<col> correlated to a pub field of the target table; the
    // declaring column must be readonly and types must match.
    const grants: TableDef = {
        name: 'grants',
        columns: { resource: { type: 'string', pub: true, readonly: true } },
    };
    const correlate = (subjectCol: string): TableDef => ({
        name: 'items',
        columns: {
            resource: { type: 'string', readonly: true },
            mutableResource: { type: 'string' },
            qty: { type: 'integer', readonly: true },
        },
        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'grants', where: { resource: '$row.' + subjectCol } } }],
    });

    assertTrue(validateSchemaTables([correlate('resource'), grants]) === undefined,
        'exists correlating a readonly subject column to a matching pub target field should validate');
    assertTrue(validateSchemaTables([correlate('mutableResource'), grants]) !== undefined,
        'a $row correlation over a mutable subject column should not validate');
    assertTrue(validateSchemaTables([correlate('qty'), grants]) !== undefined,
        'a $row correlation with mismatched column types should not validate');
}

async function testMigrationRules() {
    const addTable: MigrationRule = { rule: 'add-table', def: ordersTable() };
    assertTrue(json.checkFormat(migrationRuleFormat, addTable), 'add-table should pass format');
    assertTrue(validateMigrationRule(addTable) === undefined, 'add-table should validate');

    const dropTable: MigrationRule = { rule: 'drop-table', table: 'orders' };
    assertTrue(json.checkFormat(migrationRuleFormat, dropTable) && validateMigrationRule(dropTable) === undefined, 'drop-table should validate');

    const addNullable: MigrationRule = { rule: 'add-column', table: 'orders', column: 'tag', def: { type: 'string', nullable: true } };
    assertTrue(validateMigrationRule(addNullable) === undefined, 'adding a nullable column should validate');

    const addWithDefault: MigrationRule = { rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'new' } };
    assertTrue(validateMigrationRule(addWithDefault) === undefined, 'adding a non-nullable column with default should validate');

    const addNoDefault: MigrationRule = { rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string' } };
    assertTrue(validateMigrationRule(addNoDefault) !== undefined, 'adding a non-nullable column without default should not validate (old rows cannot be revised)');

    const rename = { rule: 'rename-column', table: 'orders', from: 'notes', to: 'comments' };
    assertFalse(json.checkFormat(migrationRuleFormat, rename as json.Literal),
        'rename-column should not exist (names are slot identities)');

    const recompute = {
        rule: 'recompute-column', table: 'orders', column: 'total',
        expr: { op: 'const', value: 0 },
    };
    assertFalse(json.checkFormat(migrationRuleFormat, recompute as json.Literal),
        'recompute-column should not exist (no value-transforming migrations in v1)');
}

async function testSlotWriteRules() {
    const setMode: MigrationRule = { rule: 'set-concurrent-deletes', table: 'orders', value: false };
    assertTrue(json.checkFormat(migrationRuleFormat, setMode) && validateMigrationRule(setMode) === undefined,
        'set-concurrent-deletes should validate');

    const setFKs: MigrationRule = { rule: 'set-fks', table: 'lines', fks: { order: 'orders' } };
    assertTrue(json.checkFormat(migrationRuleFormat, setFKs) && validateMigrationRule(setFKs) === undefined,
        'set-fks should validate');
    assertTrue(validateMigrationRule({ rule: 'set-fks', table: 'lines', fks: { order: 'a.b.c' } }) !== undefined,
        'set-fks with invalid target should not validate');

    const setRestrictions: MigrationRule = {
        rule: 'set-restrictions', table: 'caps',
        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { label: 'admin', grantee: '$author' } } }],
    };
    assertTrue(json.checkFormat(migrationRuleFormat, setRestrictions) && validateMigrationRule(setRestrictions) === undefined,
        'set-restrictions should validate');
    assertTrue(validateMigrationRule({
        rule: 'set-restrictions', table: 'caps',
        restrictions: [{ on: 'insert', rule: { p: 'nope' } }],
    } as unknown as MigrationRule) !== undefined, 'set-restrictions with invalid predicate should not validate');

    const setGates = {
        rule: 'set-gates',
        gates: { deploy: { p: 'exists', table: 'users.caps', where: { label: 'deploy', grantee: '$author' } } },
    };
    assertFalse(json.checkFormat(migrationRuleFormat, setGates as json.Literal),
        'set-gates should not exist (deploy gating is RTableGroup instance policy, not schema)');
}

export const schemaModelTests = {
    title: '[MODEL] Schema model tests',
    tests: [
        { name: '[MODEL01] Names and table refs', invoke: testNames },
        { name: '[MODEL02] Column types', invoke: testColumnTypes },
        { name: '[MODEL02b] Column constraint validation', invoke: testColumnConstraintValidation },
        { name: '[MODEL02c] Column value rejection reasons', invoke: testColumnValueReasons },
        { name: '[MODEL03] Table def format and validation', invoke: testTableDefFormatAndValidation },
        { name: '[MODEL04] Restriction defaults', invoke: testRestrictionDefaults },
        { name: '[MODEL05] FKs', invoke: testFKs },
        { name: '[MODEL06] Predicates', invoke: testPredicates },
        { name: '[MODEL07] Predicate contexts', invoke: testPredicateContexts },
        { name: '[MODEL08] Schema table sets', invoke: testSchemaTables },
        { name: '[MODEL09] Migration rules', invoke: testMigrationRules },
        { name: '[MODEL10] Slot-write rules', invoke: testSlotWriteRules },
    ],
};

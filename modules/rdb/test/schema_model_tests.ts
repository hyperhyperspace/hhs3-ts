import { assertTrue, assertFalse } from "@hyper-hyper-space/hhs3_util/dist/test.js";
import { json } from "@hyper-hyper-space/hhs3_json";

import {
    TableDef, tableDefFormat,
    Predicate, Restriction,
    MigrationRule, migrationRuleFormat,
} from "../src/rschema/payload.js";
import { defaultRestrictionRule, splitTableRef } from "../src/rschema/payload.js";
import {
    validateTableDef, validateSchemaTables,
    validatePredicate, validateFKs, validateRestrictions,
    validateMigrationRule,
    columnValueMatchesType,
    isValidName, isValidTableRef,
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
}

async function testTableDefFormatAndValidation() {
    const orders = ordersTable();
    assertTrue(json.checkFormat(tableDefFormat, orders), 'well-formed table def should pass format');
    assertTrue(validateTableDef(orders), 'well-formed table def should validate');

    const badType = { name: 'orders', columns: { x: { type: 'varchar' } } };
    assertFalse(json.checkFormat(tableDefFormat, badType as json.Literal), 'unknown column type should fail format');

    const noColumns: TableDef = { name: 'orders', columns: {} };
    assertTrue(json.checkFormat(tableDefFormat, noColumns), 'empty columns is structurally fine');
    assertFalse(validateTableDef(noColumns), 'table without columns should not validate');

    const badDefault: TableDef = { name: 'orders', columns: { x: { type: 'integer', default: 'nope' } } };
    assertFalse(validateTableDef(badDefault), 'default not matching column type should not validate');

    const badColumnName: TableDef = { name: 'orders', columns: { '2x': { type: 'string' } } };
    assertFalse(validateTableDef(badColumnName), 'invalid column name should not validate');

    // pub and readonly are independent ColumnDef modifiers
    const withModifiers: TableDef = { name: 'caps', columns: {
        label: { type: 'string', pub: true, readonly: true },
        tag: { type: 'string', pub: true },
        code: { type: 'string', readonly: true },
    } };
    assertTrue(json.checkFormat(tableDefFormat, withModifiers) && validateTableDef(withModifiers),
        'pub/readonly modifiers in any combination should pass format and validate');
}

async function testRestrictionDefaults() {
    const insertDefault = defaultRestrictionRule('insert');
    assertTrue(insertDefault.p === 'true', 'insert should default to allowed for anyone');

    const updateDefault = defaultRestrictionRule('update');
    const deleteDefault = defaultRestrictionRule('delete');
    assertTrue(updateDefault.p === 'owner' && (updateDefault as { is: string }).is === '$author'
        && deleteDefault.p === 'owner' && (deleteDefault as { is: string }).is === '$author',
        'update/delete should default to author-owned-only (anonymous rows immutable)');
}

async function testFKs() {
    const columns = linesTable().columns;

    assertTrue(validateFKs({ order: 'orders' }, columns), 'FK over existing column should validate');
    assertTrue(validateFKs({ order: 'users.identities' }, columns), 'qualified FK target should validate');

    assertFalse(validateFKs({ nope: 'orders' }, columns), 'FK over missing column should not validate');
    assertFalse(validateFKs({ order: 'a.b.c' }, columns), 'FK with invalid target ref should not validate');
    assertFalse(validateFKs({ '2bad': 'orders' }), 'FK with invalid column name should not validate');
}

async function testPredicates() {
    assertTrue(validatePredicate({ p: 'true' }), 'true should validate');
    assertTrue(validatePredicate({ p: 'owner', is: '$author' }), 'owner-is-author should validate');
    assertTrue(validatePredicate({ p: 'owner', is: '$rowOwner' }), 'owner-is-rowOwner should validate (trivially true, but well-formed)');
    assertFalse(validatePredicate({ p: 'owner' }), 'owner without a term should not validate');
    assertFalse(validatePredicate({ p: 'owner', is: 'author' }), 'owner with a non-$ term should not validate');
    assertFalse(validatePredicate({ p: 'owner', is: '$author', extra: 1 }), 'extra keys on atoms should not validate');
    assertFalse(validatePredicate({ p: 'not', arg: { p: 'true' } }), 'negation should not validate (positive logic only)');
    assertFalse(validatePredicate({ p: 'nope' }), 'unknown predicate should not validate');

    assertTrue(validatePredicate({ p: 'exists', table: 'users.caps', where: { label: 'write' }, owner: '$author' }),
        'exists with where + owner should validate');
    assertTrue(validatePredicate({ p: 'exists', table: 'caps', owner: '$author' }),
        'exists with owner only should validate');
    assertTrue(validatePredicate({ p: 'exists', table: 'caps', owner: '$rowOwner' }),
        'exists with owner: $rowOwner should validate in row context');
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
    assertFalse(validatePredicate({ p: 'exists', table: 'a.b.c', owner: '$author' }),
        'exists with invalid table ref should not validate');
    assertFalse(validatePredicate({ p: 'exists', table: 'orders', where: { '2bad': 1 } }),
        'where with invalid field name should not validate');
    assertFalse(validatePredicate({ p: 'exists', table: 'caps', where: { label: '$nope' } }),
        '$-prefixed strings that are not terms should not validate (reserved)');
    assertFalse(validatePredicate({ p: 'exists', table: 'caps', ownerIs: 'author' }),
        'old ownerIs form should not validate');

    const ownerOrAdmin: Predicate = {
        p: 'or', args: [
            { p: 'owner', is: '$author' },
            { p: 'exists', table: 'users.caps', where: { role: 'admin' }, owner: '$author' },
        ],
    };
    assertTrue(validatePredicate(ownerOrAdmin), 'owner-or-admin disjunction should validate');

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
    assertTrue(validatePredicate({ p: 'exists', table: 'grants', where: { resource: '$row.resource' }, owner: '$author' }),
        'exists correlating a where field to $row.<col> should validate');
    assertFalse(validatePredicate({ p: 'exists', table: 'grants', where: { resource: '$row.2bad' } }),
        'a malformed $row term in a where value should not validate (reserved)');
}

async function testPredicateContexts() {
    // 'object' context: no subject row -> no owner atom, no $rowOwner anywhere
    assertTrue(validatePredicate({ p: 'exists', table: 'users.caps', where: { label: 'deploy' }, owner: '$author' }, 'object'),
        'canDeploy-style predicate should validate in object context');
    assertFalse(validatePredicate({ p: 'owner', is: '$author' }, 'object'),
        'owner atom should not validate in object context (no subject row)');
    assertFalse(validatePredicate({ p: 'exists', table: 'caps', owner: '$rowOwner' }, 'object'),
        '$rowOwner should not validate in object context');
    assertFalse(validatePredicate({ p: 'exists', table: 'caps', where: { granted_to: '$rowOwner' } }, 'object'),
        '$rowOwner in a where value should not validate in object context');
    assertFalse(validatePredicate({
        p: 'and', args: [{ p: 'true' }, { p: 'owner', is: '$author' }],
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
    assertTrue(validateSchemaTables([ordersTable(), linesTable()]), 'consistent schema should validate');

    assertFalse(validateSchemaTables([ordersTable(), ordersTable()]), 'duplicate table names should not validate');

    assertFalse(validateSchemaTables([linesTable()]), 'local FK target missing from schema should not validate');

    const crossGroup = docsTable([{ on: 'update', rule: { p: 'exists', table: 'users.caps', where: { label: 'write' }, owner: '$author' } }]);
    assertTrue(validateSchemaTables([crossGroup]), 'qualified restriction target should be deferred to binding-time checking');

    const localSearch = docsTable([{ on: 'all', rule: { p: 'exists', table: 'caps', where: { label: 'write' }, owner: '$author' } }]);
    assertTrue(validateSchemaTables([localSearch, capsTable()]),
        'local restriction over a pub column should validate');

    const overNonPub = docsTable([{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { note: 'x' }, owner: '$author' } }]);
    assertFalse(validateSchemaTables([overNonPub, capsTable()]),
        'restriction where over a non-pub column should not validate');

    const overMissingField = docsTable([{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { nope: 'x' } } }]);
    assertFalse(validateSchemaTables([overMissingField, capsTable()]),
        'restriction where over a missing column should not validate');

    const nestedAtoms = docsTable([{
        on: 'update',
        rule: { p: 'or', args: [
            { p: 'owner', is: '$author' },
            { p: 'and', args: [
                { p: 'exists', table: 'caps', where: { label: 'write' }, owner: '$author' },
                { p: 'exists', table: 'caps', owner: '$author' },
            ] },
        ] },
    }]);
    assertTrue(validateSchemaTables([nestedAtoms, capsTable()]),
        'exists atoms nested under and/or should be checked and validate');

    const nestedBadAtom = docsTable([{
        on: 'update',
        rule: { p: 'or', args: [
            { p: 'owner', is: '$author' },
            { p: 'exists', table: 'caps', where: { note: 'x' } },
        ] },
    }]);
    assertFalse(validateSchemaTables([nestedBadAtom, capsTable()]),
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

    assertTrue(validateSchemaTables([itemsWith({ p: 'cmp', cmp: 'lt', left: { col: 'priority' }, right: { lit: 3 } })]),
        'cmp over a readonly integer column vs an integer literal should validate');
    assertFalse(validateSchemaTables([itemsWith({ p: 'cmp', cmp: 'eq', left: { col: 'status' }, right: { lit: 'open' } })]),
        'cmp over a mutable (non-readonly) column should not validate');
    assertFalse(validateSchemaTables([itemsWith({ p: 'cmp', cmp: 'eq', left: { col: 'missing' }, right: { lit: 1 } })]),
        'cmp over a missing column should not validate');
    assertFalse(validateSchemaTables([itemsWith({ p: 'cmp', cmp: 'eq', left: { col: 'priority' }, right: { lit: 'x' } })]),
        'cmp comparing an integer column to a string literal should not validate (type mismatch)');
    assertFalse(validateSchemaTables([itemsWith({ p: 'cmp', cmp: 'eq', left: { fn: 'add', args: [{ col: 'resource' }, { lit: 1 }] }, right: { lit: 2 } })]),
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
        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'grants', where: { resource: '$row.' + subjectCol }, owner: '$author' } }],
    });

    assertTrue(validateSchemaTables([correlate('resource'), grants]),
        'exists correlating a readonly subject column to a matching pub target field should validate');
    assertFalse(validateSchemaTables([correlate('mutableResource'), grants]),
        'a $row correlation over a mutable subject column should not validate');
    assertFalse(validateSchemaTables([correlate('qty'), grants]),
        'a $row correlation with mismatched column types should not validate');
}

async function testMigrationRules() {
    const addTable: MigrationRule = { rule: 'add-table', def: ordersTable() };
    assertTrue(json.checkFormat(migrationRuleFormat, addTable), 'add-table should pass format');
    assertTrue(validateMigrationRule(addTable), 'add-table should validate');

    const dropTable: MigrationRule = { rule: 'drop-table', table: 'orders' };
    assertTrue(json.checkFormat(migrationRuleFormat, dropTable) && validateMigrationRule(dropTable), 'drop-table should validate');

    const addNullable: MigrationRule = { rule: 'add-column', table: 'orders', column: 'tag', def: { type: 'string', nullable: true } };
    assertTrue(validateMigrationRule(addNullable), 'adding a nullable column should validate');

    const addWithDefault: MigrationRule = { rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string', default: 'new' } };
    assertTrue(validateMigrationRule(addWithDefault), 'adding a non-nullable column with default should validate');

    const addNoDefault: MigrationRule = { rule: 'add-column', table: 'orders', column: 'status', def: { type: 'string' } };
    assertFalse(validateMigrationRule(addNoDefault), 'adding a non-nullable column without default should not validate (old rows cannot be revised)');

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
    assertTrue(json.checkFormat(migrationRuleFormat, setMode) && validateMigrationRule(setMode),
        'set-concurrent-deletes should validate');

    const setFKs: MigrationRule = { rule: 'set-fks', table: 'lines', fks: { order: 'orders' } };
    assertTrue(json.checkFormat(migrationRuleFormat, setFKs) && validateMigrationRule(setFKs),
        'set-fks should validate');
    assertFalse(validateMigrationRule({ rule: 'set-fks', table: 'lines', fks: { order: 'a.b.c' } }),
        'set-fks with invalid target should not validate');

    const setRestrictions: MigrationRule = {
        rule: 'set-restrictions', table: 'caps',
        restrictions: [{ on: 'insert', rule: { p: 'exists', table: 'caps', where: { label: 'admin' }, owner: '$author' } }],
    };
    assertTrue(json.checkFormat(migrationRuleFormat, setRestrictions) && validateMigrationRule(setRestrictions),
        'set-restrictions should validate');
    assertFalse(validateMigrationRule({
        rule: 'set-restrictions', table: 'caps',
        restrictions: [{ on: 'insert', rule: { p: 'nope' } }],
    } as unknown as MigrationRule), 'set-restrictions with invalid predicate should not validate');

    const setGates = {
        rule: 'set-gates',
        gates: { deploy: { p: 'exists', table: 'users.caps', where: { label: 'deploy' }, owner: '$author' } },
    };
    assertFalse(json.checkFormat(migrationRuleFormat, setGates as json.Literal),
        'set-gates should not exist (deploy gating is RTableGroup instance policy, not schema)');
}

export const schemaModelTests = {
    title: '[MODEL] Schema model tests',
    tests: [
        { name: '[MODEL01] Names and table refs', invoke: testNames },
        { name: '[MODEL02] Column types', invoke: testColumnTypes },
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

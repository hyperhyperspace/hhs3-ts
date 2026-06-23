import { json } from "@hyper-hyper-space/hhs3_json";

import { combineSpans, DiagnosticBag, err, ok, Result, TextSpan } from "../diagnostics.js";
import {
    AddMemberStatement, AllowOp, AllowRuleExpr, AlterSchemaStatement, AstScript, AstStatement, AuthorExpr, BundleStatement, BundleWriteStatement,
    ColumnDecl, ColumnTypeName, CreateDatabaseStatement, CreateSchemaStatement,
    CreateTableGroupStatement, DeleteStatement, DeploySchemaStatement, HashRef, InitialRow,
    InsertStatement, LogStatement, MigrationRuleExpr, NameOrHashRef, NameRef, OperandExpr,
    PredicateExpr, SelectStatement, SetViewStatement, TableDecl, TableOption, TableRef, UpdateRefStatement,
    UpdateStatement, ValueExpr, VersionExpr,
} from "./ast.js";
import { lex } from "./lexer.js";
import { Token } from "./tokens.js";

class Parser {
    private pos = 0;
    private readonly diagnostics = new DiagnosticBag();

    constructor(private readonly source: string, private readonly tokens: Token[]) {}

    parseScript(): Result<AstScript> {
        const statements: AstStatement[] = [];
        const start = this.peek().span;

        while (!this.isEof()) {
            this.consumeSemicolons();
            if (this.isEof()) break;
            const stmt = this.parseStatement();
            if (stmt !== undefined) statements.push(stmt);
            if (!this.matchPunctuation(';') && !this.isEof()) {
                this.diagnostics.add('PARSE_EXPECTED_TOKEN', "Expected ';' between statements", this.peek().span);
                this.synchronizeStatement();
            }
        }

        const end = statements.length > 0 ? statements[statements.length - 1].span : start;
        const script: AstScript = { kind: 'script', statements, span: combineSpans(start, end) };
        return this.diagnostics.hasErrors() ? err(this.diagnostics.all()) : ok(script);
    }

    parseStatement(): AstStatement | undefined {
        const tok = this.peek();

        if (this.matchKeyword('CREATE')) {
            const createTok = this.previous();
            if (this.matchKeyword('DATABASE')) return this.parseCreateDatabase(createTok.span);
            if (this.matchKeyword('SCHEMA')) return this.parseCreateSchema(createTok.span);
            if (this.matchKeyword('TABLEGROUP')) return this.parseCreateTableGroup(createTok.span);
            this.expected('DATABASE, SCHEMA or TABLEGROUP');
            return undefined;
        }

        if (this.matchKeyword('ADD')) {
            const start = this.previous().span;
            if (this.matchKeyword('SCHEMA')) return this.parseAddMember(start, 'schema');
            if (this.matchKeyword('TABLEGROUP')) return this.parseAddMember(start, 'tablegroup');
            this.expected('SCHEMA or TABLEGROUP');
            return undefined;
        }
        if (this.matchKeyword('ALTER')) return this.parseAlterSchema(this.previous().span);
        if (this.matchKeyword('DEPLOY')) return this.parseDeploySchema(this.previous().span);
        if (this.matchKeyword('UPDATE')) {
            const start = this.previous().span;
            if (this.matchKeyword('REF')) return this.parseUpdateRef(start);
            return this.parseUpdate(start);
        }
        if (this.matchKeyword('DELETE')) return this.parseDelete(this.previous().span);
        if (this.matchKeyword('BUNDLE')) return this.parseBundle(this.previous().span);
        if (this.matchKeyword('SET')) {
            const start = this.previous().span;
            if (this.matchKeyword('VIEW')) return this.parseSetView(start);
            this.expected('VIEW');
            return undefined;
        }
        if (this.matchKeyword('INSERT')) return this.parseInsert(this.previous().span);
        if (this.matchKeyword('SELECT')) return this.parseSelect(this.previous().span);
        if (this.matchKeyword('LOG')) return this.parseLog(this.previous().span);

        this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected token '${tok.text}'`, tok.span);
        this.synchronizeStatement();
        return undefined;
    }

    diagnosticsList(): DiagnosticBag {
        return this.diagnostics;
    }

    private parseCreateDatabase(start: TextSpan): CreateDatabaseStatement {
        const nameTok = this.expectIdentifierToken('database name');
        return { kind: 'create-database', name: nameTok.text, span: combineSpans(start, nameTok.span) };
    }

    private parseCreateSchema(start: TextSpan): CreateSchemaStatement {
        const name = this.expectIdentifierText('schema name');
        const creators: ValueExpr[] = [];
        if (this.matchKeyword('CREATORS')) {
            this.expectPunctuation('(');
            if (!this.checkPunctuation(')')) {
                do {
                    creators.push(this.parseValue());
                } while (this.matchPunctuation(','));
            }
            this.expectPunctuation(')');
        }
        this.expectKeyword('AS');
        this.expectPunctuation('(');
        const tables: TableDecl[] = [];
        while (!this.checkPunctuation(')') && !this.isEof()) {
            tables.push(this.parseTableDecl());
            if (!this.matchPunctuation(',')) break;
        }
        const close = this.expectPunctuation(')');
        return { kind: 'create-schema', name, creators, tables, span: combineSpans(start, close.span) };
    }

    private parseTableDecl(): TableDecl {
        const start = this.expectKeyword('TABLE').span;
        const name = this.expectIdentifierText('table name');
        this.expectPunctuation('(');
        const columns: ColumnDecl[] = [];
        while (!this.checkPunctuation(')') && !this.isEof()) {
            columns.push(this.parseColumnDecl());
            if (!this.matchPunctuation(',')) break;
        }
        let end = this.expectPunctuation(')').span;
        const options: TableOption[] = [];
        while (!this.isEof() && !this.checkPunctuation(',') && !this.checkPunctuation(')') && !this.checkPunctuation(';')) {
            const opt = this.parseTableOption();
            if (opt === undefined) break;
            end = opt.span;
            options.push(opt);
        }
        this.validateAllowRules(options.filter((opt): opt is { kind: 'allow-rule' } & AllowRuleExpr => opt.kind === 'allow-rule'));
        return { name, columns, options, span: combineSpans(start, end) };
    }

    private parseColumnDecl(): ColumnDecl {
        const start = this.peek().span;
        const name = this.expectIdentifierText('column name');
        const typeToken = this.advance();
        const type = this.columnTypeFromToken(typeToken);
        let nullable = false;
        let defaultValue: ValueExpr | undefined;
        let pub = false;
        let readonly = false;
        let references: string | undefined;
        let end = typeToken.span;
        const column: ColumnDecl = { name, type, nullable, pub, readonly, span: combineSpans(start, end) };

        this.parseColumnModifiers(column);

        return column;
    }

    private parseColumnModifiers(column: ColumnDecl): void {
        let end = column.span;
        while (!this.isEof() && !this.checkPunctuation(',') && !this.checkPunctuation(')')) {
            if (this.matchKeyword('NULL')) {
                column.nullable = true;
                end = this.previous().span;
            } else if (this.matchKeyword('DEFAULT')) {
                column.defaultValue = this.parseValue();
                end = column.defaultValue.span;
            } else if (this.matchKeyword('PUB')) {
                column.pub = true;
                end = this.previous().span;
            } else if (this.matchKeyword('READONLY')) {
                column.readonly = true;
                end = this.previous().span;
            } else if (this.matchKeyword('REFERENCES')) {
                const ref = this.expectIdentifierToken('referenced table');
                column.references = ref.text;
                end = ref.span;
            } else {
                break;
            }
        }
        column.span = combineSpans(column.span, end);
    }

    private parseTableOption(): TableOption | undefined {
        if (this.matchKeyword('NO')) {
            const start = this.previous().span;
            this.expectKeyword('CONCURRENT');
            const end = this.expectKeyword('DELETES').span;
            return { kind: 'concurrent-deletes', value: false, span: combineSpans(start, end) };
        }
        if (this.matchKeyword('CONCURRENT')) {
            const start = this.previous().span;
            const end = this.expectKeyword('DELETES').span;
            return { kind: 'concurrent-deletes', value: true, span: combineSpans(start, end) };
        }
        if (this.matchKeyword('IDENTITY')) {
            const start = this.previous().span;
            this.expectKeyword('PROVIDER');
            let keyIdColumn = 'keyId';
            let publicKeyColumn = 'publicKey';
            let end = this.previous().span;
            if (this.matchPunctuation('(')) {
                keyIdColumn = this.expectIdentifierText('key id column');
                this.expectPunctuation(',');
                publicKeyColumn = this.expectIdentifierText('public key column');
                end = this.expectPunctuation(')').span;
            }
            return { kind: 'identity-provider', keyIdColumn, publicKeyColumn, span: combineSpans(start, end) };
        }
        if (this.checkKeyword('ALLOW')) {
            const rule = this.parseAllowRule();
            return { kind: 'allow-rule', ...rule };
        }
        this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected table option '${this.peek().text}'`, this.peek().span);
        this.advance();
        return undefined;
    }

    private parseCreateTableGroup(start: TextSpan): CreateTableGroupStatement {
        const name = this.expectIdentifierText('tablegroup name');
        this.expectKeyword('USING');
        this.expectKeyword('SCHEMA');
        const schema = this.parseNameOrHash();
        let schemaVersion: VersionExpr | undefined;
        const bindings: CreateTableGroupStatement['bindings'] = [];
        let idProvider: string | undefined;
        let canDeploy: PredicateExpr | undefined;
        const initialRows: InitialRow[] = [];
        let end = schema.span;

        while (!this.isEof() && !this.checkPunctuation(';') && !this.checkPunctuation(')')) {
            if (this.matchKeyword('AT')) {
                schemaVersion = this.parseVersion();
                end = schemaVersion.span;
            } else if (this.matchKeyword('BIND')) {
                const bindStart = this.previous().span;
                const bindName = this.expectIdentifierText('binding name');
                this.expectOperator('=>');
                const group = this.parseNameOrHash();
                bindings.push({ name: bindName, group, span: combineSpans(bindStart, group.span) });
                end = group.span;
            } else if (this.matchKeyword('USING')) {
                const identities = this.expectIdentifierLike('IDENTITIES');
                if (identities.upper !== 'IDENTITIES') {
                    this.diagnostics.add('PARSE_EXPECTED_TOKEN', `Expected IDENTITIES, got '${identities.text}'`, identities.span);
                }
                const provider = this.expectIdentifierToken('identity provider');
                idProvider = provider.text;
                end = provider.span;
            } else if (this.matchKeyword('CAN')) {
                this.expectKeyword('DEPLOY');
                this.expectKeyword('IF');
                canDeploy = this.parsePredicate();
                end = canDeploy.span;
            } else if (this.matchKeyword('WITH')) {
                this.expectKeyword('ROWS');
                this.expectPunctuation('(');
                while (!this.checkPunctuation(')') && !this.isEof()) {
                    const row = this.parseInitialRow();
                    initialRows.push(row);
                    end = row.span;
                    if (!this.matchPunctuation(',')) break;
                }
                end = this.expectPunctuation(')').span;
            } else {
                this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected CREATE TABLEGROUP clause '${this.peek().text}'`, this.peek().span);
                this.advance();
            }
        }

        const stmt: CreateTableGroupStatement = { kind: 'create-tablegroup', name, schema, bindings, initialRows, span: combineSpans(start, end) };
        if (schemaVersion !== undefined) stmt.schemaVersion = schemaVersion;
        if (idProvider !== undefined) stmt.idProvider = idProvider;
        if (canDeploy !== undefined) stmt.canDeploy = canDeploy;
        return stmt;
    }

    private parseInitialRow(): InitialRow {
        const startTok = this.expectIdentifierToken('initial row table');
        this.expectPunctuation('(');
        const values: InitialRow['values'] = [];
        while (!this.checkPunctuation(')') && !this.isEof()) {
            const column = this.expectIdentifierText('initial row column');
            this.expectOperator('=');
            values.push({ column, value: this.parseValue() });
            if (!this.matchPunctuation(',')) break;
        }
        const end = this.expectPunctuation(')').span;
        return { table: startTok.text, values, span: combineSpans(startTok.span, end) };
    }

    private parseAddMember(start: TextSpan, member: 'schema' | 'tablegroup'): AddMemberStatement {
        const target = this.parseNameOrHash();
        this.expectKeyword('TO');
        const database = this.parseNameOrHash();
        let end = database.span;
        let note: string | undefined;
        while (!this.isEof() && !this.checkPunctuation(';')) {
            if (this.matchKeyword('NOTE')) {
                const tok = this.expectKind('string', 'NOTE text');
                note = tok.value as string;
                end = tok.span;
            } else {
                this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected ADD clause '${this.peek().text}'`, this.peek().span);
                this.advance();
            }
        }
        const stmt: AddMemberStatement = { kind: 'add-member', member, target, database, span: combineSpans(start, end) };
        if (note !== undefined) stmt.note = note;
        return stmt;
    }

    private parseAlterSchema(start: TextSpan): AlterSchemaStatement {
        this.expectKeyword('SCHEMA');
        const schema = this.parseNameOrHash();
        this.expectKeyword('AS');
        this.expectPunctuation('(');
        const rules: MigrationRuleExpr[] = [];
        while (!this.checkPunctuation(')') && !this.isEof()) {
            rules.push(this.parseMigrationRule());
            if (!this.matchPunctuation(',')) break;
        }
        let end = this.expectPunctuation(')').span;
        let at: VersionExpr | undefined;
        let author: AuthorExpr | undefined;
        while (!this.isEof() && !this.checkPunctuation(';')) {
            if (this.matchKeyword('AT')) {
                at = this.parseVersion();
                end = at.span;
            } else if (this.matchKeyword('BY')) {
                author = this.parseAuthor();
                end = author.span;
            } else {
                this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected ALTER SCHEMA clause '${this.peek().text}'`, this.peek().span);
                this.advance();
            }
        }
        const stmt: AlterSchemaStatement = { kind: 'alter-schema', schema, rules, span: combineSpans(start, end) };
        if (author !== undefined) stmt.author = author;
        if (at !== undefined) stmt.at = at;
        return stmt;
    }

    private parseMigrationRule(): MigrationRuleExpr {
        const start = this.peek().span;
        if (this.matchKeyword('ADD')) {
            if (this.matchKeyword('TABLE')) {
                const table = this.parseTableDeclBody(start);
                return { kind: 'add-table', table, span: table.span };
            }
            this.expectKeyword('COLUMN');
            const { table, column } = this.parseQualifiedColumn();
            const typeTok = this.advance();
            const type = this.columnTypeFromToken(typeTok);
            const col: ColumnDecl = { name: column, type, nullable: false, pub: false, readonly: false, span: combineSpans(start, typeTok.span) };
            this.parseColumnModifiers(col);
            return { kind: 'add-column', table, column: col, span: col.span };
        }
        if (this.matchKeyword('DROP')) {
            if (this.matchKeyword('TABLE')) {
                const tok = this.expectIdentifierToken('table name');
                return { kind: 'drop-table', table: tok.text, span: combineSpans(start, tok.span) };
            }
            this.expectKeyword('COLUMN');
            const q = this.parseQualifiedColumn();
            return { kind: 'drop-column', table: q.table, column: q.column, span: combineSpans(start, q.span) };
        }
        if (this.matchKeyword('SET')) {
            if (this.matchKeyword('CONCURRENT')) {
                this.expectKeyword('DELETES');
                const table = this.expectIdentifierToken('table name');
                const valueTok = this.expectIdentifierLike('boolean');
                const value = valueTok.upper === 'TRUE';
                if (valueTok.upper !== 'TRUE' && valueTok.upper !== 'FALSE') {
                    this.diagnostics.add('PARSE_EXPECTED_TOKEN', 'Expected true or false', valueTok.span);
                }
                return { kind: 'set-concurrent-deletes', table: table.text, value, span: combineSpans(start, valueTok.span) };
            }
            if (this.matchKeyword('FKS')) {
                const table = this.expectIdentifierToken('table name');
                this.expectPunctuation('(');
                const fks: { [column: string]: string } = {};
                while (!this.checkPunctuation(')') && !this.isEof()) {
                    const col = this.expectIdentifierText('FK column');
                    this.expectKeyword('REFERENCES');
                    const ref = this.expectIdentifierToken('referenced table');
                    fks[col] = ref.text;
                    if (!this.matchPunctuation(',')) break;
                }
                const end = this.expectPunctuation(')').span;
                return { kind: 'set-fks', table: table.text, fks, span: combineSpans(start, end) };
            }
            if (this.matchKeyword('ALLOW')) {
                this.expectKeyword('RULES');
                const table = this.expectIdentifierToken('table name');
                this.expectPunctuation('(');
                const allowRules: AllowRuleExpr[] = [];
                while (!this.checkPunctuation(')') && !this.isEof()) {
                    allowRules.push(this.parseAllowRule());
                    if (!this.matchPunctuation(',')) break;
                }
                const end = this.expectPunctuation(')').span;
                this.validateAllowRules(allowRules);
                return { kind: 'set-allow-rules', table: table.text, allowRules, span: combineSpans(start, end) };
            }
        }
        this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected migration rule '${this.peek().text}'`, this.peek().span);
        const tok = this.advance();
        return { kind: 'drop-table', table: tok.text, span: tok.span };
    }

    private parseTableDeclBody(start: TextSpan): TableDecl {
        const name = this.expectIdentifierText('table name');
        this.expectPunctuation('(');
        const columns: ColumnDecl[] = [];
        while (!this.checkPunctuation(')') && !this.isEof()) {
            columns.push(this.parseColumnDecl());
            if (!this.matchPunctuation(',')) break;
        }
        let end = this.expectPunctuation(')').span;
        const options: TableOption[] = [];
        while (!this.isEof() && !this.checkPunctuation(',') && !this.checkPunctuation(')') && !this.checkPunctuation(';')) {
            const opt = this.parseTableOption();
            if (opt === undefined) break;
            end = opt.span;
            options.push(opt);
        }
        this.validateAllowRules(options.filter((opt): opt is { kind: 'allow-rule' } & AllowRuleExpr => opt.kind === 'allow-rule'));
        return { name, columns, options, span: combineSpans(start, end) };
    }

    private parseDeploySchema(start: TextSpan): DeploySchemaStatement {
        this.expectKeyword('SCHEMA');
        const schema = this.parseNameOrHash();
        this.expectKeyword('AT');
        const version = this.parseVersion();
        this.expectKeyword('ON');
        const group = this.parseNameOrHash();
        let end = group.span;
        let at: VersionExpr | undefined;
        let author: AuthorExpr | undefined;
        while (!this.isEof() && !this.checkPunctuation(';')) {
            if (this.matchKeyword('AT')) {
                at = this.parseVersion();
                end = at.span;
            } else if (this.matchKeyword('BY')) {
                author = this.parseAuthor();
                end = author.span;
            } else {
                this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected DEPLOY clause '${this.peek().text}'`, this.peek().span);
                this.advance();
            }
        }
        const stmt: DeploySchemaStatement = { kind: 'deploy-schema', schema, version, group, span: combineSpans(start, end) };
        if (author !== undefined) stmt.author = author;
        if (at !== undefined) stmt.at = at;
        return stmt;
    }

    private parseUpdateRef(start: TextSpan): UpdateRefStatement {
        const ref = this.parseNameOrHash();
        if (ref.kind === 'name' && ref.parts.length !== 1) {
            this.diagnostics.add('PARSE_EXPECTED_TOKEN', 'Expected bound group reference, not group.table', ref.span);
        }
        this.expectKeyword('TO');
        const version = this.parseVersion();
        this.expectKeyword('ON');
        const group = this.parseNameOrHash();
        let end = group.span;
        let at: VersionExpr | undefined;
        if (this.matchKeyword('AT')) {
            at = this.parseVersion();
            end = at.span;
        }
        const stmt: UpdateRefStatement = { kind: 'update-ref', ref, version, group, span: combineSpans(start, end) };
        if (at !== undefined) stmt.at = at;
        return stmt;
    }

    private parseInsert(start: TextSpan, defaultGroup?: NameOrHashRef): InsertStatement {
        this.expectKeyword('INTO');
        const table = this.parseTableRef(defaultGroup);
        this.expectPunctuation('(');
        const columns = this.parseIdentifierList('column');
        this.expectPunctuation(')');
        this.expectKeyword('VALUES');
        this.expectPunctuation('(');
        const values: ValueExpr[] = [];
        if (!this.checkPunctuation(')')) {
            do {
                values.push(this.parseValue());
            } while (this.matchPunctuation(','));
        }
        let end = this.expectPunctuation(')').span;
        let at: VersionExpr | undefined;
        let author: AuthorExpr | undefined;
        while (!this.isEof() && !this.checkPunctuation(';') && !this.checkPunctuation(')')) {
            if (this.matchKeyword('AT')) {
                at = this.parseVersion();
                end = at.span;
            } else if (this.matchKeyword('BY')) {
                author = this.parseAuthor();
                end = author.span;
            } else {
                this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected INSERT clause '${this.peek().text}'`, this.peek().span);
                this.advance();
            }
        }
        const stmt: InsertStatement = { kind: 'insert', table, columns, values, span: combineSpans(start, end) };
        if (author !== undefined) stmt.author = author;
        if (at !== undefined) stmt.at = at;
        return stmt;
    }

    private parseUpdate(start: TextSpan, defaultGroup?: NameOrHashRef): UpdateStatement {
        const table = this.parseTableRef(defaultGroup);
        this.expectKeyword('SET');
        const values: UpdateStatement['values'] = [];
        do {
            const column = this.expectIdentifierText('updated column');
            this.expectOperator('=');
            values.push({ column, value: this.parseValue() });
        } while (this.matchPunctuation(','));
        this.expectKeyword('WHERE');
        const rowId = this.parseRowIdPredicate();
        let end = rowId.span;
        let at: VersionExpr | undefined;
        let author: AuthorExpr | undefined;
        while (!this.isEof() && !this.checkPunctuation(';') && !this.checkPunctuation(')')) {
            if (this.matchKeyword('AT')) {
                at = this.parseVersion();
                end = at.span;
            } else if (this.matchKeyword('BY')) {
                author = this.parseAuthor();
                end = author.span;
            } else {
                this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected UPDATE clause '${this.peek().text}'`, this.peek().span);
                this.advance();
            }
        }
        const stmt: UpdateStatement = { kind: 'update', table, values, rowId, span: combineSpans(start, end) };
        if (author !== undefined) stmt.author = author;
        if (at !== undefined) stmt.at = at;
        return stmt;
    }

    private parseDelete(start: TextSpan, defaultGroup?: NameOrHashRef): DeleteStatement {
        this.expectKeyword('FROM');
        const table = this.parseTableRef(defaultGroup);
        this.expectKeyword('WHERE');
        const rowId = this.parseRowIdPredicate();
        let end = rowId.span;
        let at: VersionExpr | undefined;
        let author: AuthorExpr | undefined;
        while (!this.isEof() && !this.checkPunctuation(';') && !this.checkPunctuation(')')) {
            if (this.matchKeyword('AT')) {
                at = this.parseVersion();
                end = at.span;
            } else if (this.matchKeyword('BY')) {
                author = this.parseAuthor();
                end = author.span;
            } else {
                this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected DELETE clause '${this.peek().text}'`, this.peek().span);
                this.advance();
            }
        }
        const stmt: DeleteStatement = { kind: 'delete', table, rowId, span: combineSpans(start, end) };
        if (author !== undefined) stmt.author = author;
        if (at !== undefined) stmt.at = at;
        return stmt;
    }

    private parseBundle(start: TextSpan): BundleStatement {
        this.expectKeyword('ON');
        const group = this.parseNameOrHash();
        this.expectPunctuation('(');
        const writes: BundleWriteStatement[] = [];
        while (!this.checkPunctuation(')') && !this.isEof()) {
            let write: BundleWriteStatement | undefined;
            if (this.matchKeyword('INSERT')) write = this.parseInsert(this.previous().span, group);
            else if (this.matchKeyword('UPDATE')) write = this.parseUpdate(this.previous().span, group);
            else if (this.matchKeyword('DELETE')) write = this.parseDelete(this.previous().span, group);
            else {
                this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected BUNDLE statement '${this.peek().text}'`, this.peek().span);
                this.advance();
            }
            if (write !== undefined) {
                // A bundle is a single signed op with one author; put BY on the
                // BUNDLE, not on individual writes.
                if (write.author !== undefined) {
                    this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', 'BY is not allowed on a bundle inner write; put BY on the BUNDLE', write.author.span);
                    delete write.author;
                }
                writes.push(write);
            }
            this.matchPunctuation(';');
        }
        let end = this.expectPunctuation(')').span;
        let at: VersionExpr | undefined;
        let author: AuthorExpr | undefined;
        while (!this.isEof() && !this.checkPunctuation(';')) {
            if (this.matchKeyword('AT')) {
                at = this.parseVersion();
                end = at.span;
            } else if (this.matchKeyword('BY')) {
                author = this.parseAuthor();
                end = author.span;
            } else {
                this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected BUNDLE clause '${this.peek().text}'`, this.peek().span);
                this.advance();
            }
        }
        const stmt: BundleStatement = { kind: 'bundle', group, writes, span: combineSpans(start, end) };
        if (author !== undefined) stmt.author = author;
        if (at !== undefined) stmt.at = at;
        return stmt;
    }

    private parseSetView(start: TextSpan): SetViewStatement {
        this.expectKeyword('AT');
        const at = this.parseVersion();
        let end = at.span;
        let from: VersionExpr | undefined;

        if (this.matchKeyword('FROM')) {
            from = this.parseVersion();
            end = from.span;
        }

        const stmt: SetViewStatement = { kind: 'set-view', at, span: combineSpans(start, end) };
        if (from !== undefined) stmt.from = from;
        return stmt;
    }

    private parseSelect(start: TextSpan): SelectStatement {
        let projection: '*' | string[];
        if (this.matchOperator('*')) {
            projection = '*';
        } else {
            projection = this.parseIdentifierList('projection');
        }
        this.expectKeyword('FROM');
        const table = this.parseTableRef();
        let where: PredicateExpr | undefined;
        const orderBy: SelectStatement['orderBy'] = [];
        let limit: number | undefined;
        let offset: number | undefined;
        let at: VersionExpr | undefined;
        let from: VersionExpr | undefined;
        let end = table.span;

        while (!this.isEof() && !this.checkPunctuation(';')) {
            if (this.matchKeyword('WHERE')) {
                where = this.parsePredicate();
                end = where.span;
            } else if (this.matchKeyword('ORDER')) {
                this.expectKeyword('BY');
                do {
                    const col = this.expectIdentifierToken('order column');
                    let dir: 'asc' | 'desc' | undefined;
                    if (this.matchKeyword('ASC')) dir = 'asc';
                    else if (this.matchKeyword('DESC')) dir = 'desc';
                    const item: { column: string; dir?: 'asc' | 'desc'; span: TextSpan } = { column: col.text, span: col.span };
                    if (dir !== undefined) item.dir = dir;
                    orderBy.push(item);
                    end = col.span;
                } while (this.matchPunctuation(','));
            } else if (this.matchKeyword('LIMIT')) {
                limit = this.expectInteger('LIMIT');
                end = this.previous().span;
            } else if (this.matchKeyword('OFFSET')) {
                offset = this.expectInteger('OFFSET');
                end = this.previous().span;
            } else if (this.matchKeyword('AT')) {
                at = this.parseVersion();
                end = at.span;
            } else if (this.matchKeyword('FROM')) {
                from = this.parseVersion();
                end = from.span;
            } else {
                this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected SELECT clause '${this.peek().text}'`, this.peek().span);
                this.advance();
            }
        }

        const stmt: SelectStatement = { kind: 'select', projection, table, orderBy, span: combineSpans(start, end) };
        if (where !== undefined) stmt.where = where;
        if (limit !== undefined) stmt.limit = limit;
        if (offset !== undefined) stmt.offset = offset;
        if (at !== undefined) stmt.at = at;
        if (from !== undefined) stmt.from = from;
        return stmt;
    }

    private parseLog(start: TextSpan): LogStatement {
        const target = this.parseNameOrHash();
        let at: VersionExpr | undefined;
        let limit: number | undefined;
        let offset: number | undefined;
        let end = target.span;
        while (!this.isEof() && !this.checkPunctuation(';')) {
            if (this.matchKeyword('AT')) {
                at = this.parseVersion();
                end = at.span;
            } else if (this.matchKeyword('LIMIT')) {
                limit = this.expectInteger('LIMIT');
                end = this.previous().span;
            } else if (this.matchKeyword('OFFSET')) {
                offset = this.expectInteger('OFFSET');
                end = this.previous().span;
            } else {
                this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', `Unexpected LOG clause '${this.peek().text}'`, this.peek().span);
                this.advance();
            }
        }
        const stmt: LogStatement = { kind: 'log', target, span: combineSpans(start, end) };
        if (at !== undefined) stmt.at = at;
        if (limit !== undefined) stmt.limit = limit;
        if (offset !== undefined) stmt.offset = offset;
        return stmt;
    }

    private parsePredicate(): PredicateExpr {
        return this.parseOr();
    }

    private parseOr(): PredicateExpr {
        let expr = this.parseAnd();
        while (this.matchKeyword('OR')) {
            const right = this.parseAnd();
            expr = expr.kind === 'or'
                ? { ...expr, args: [...expr.args, right], span: combineSpans(expr.span, right.span) }
                : { kind: 'or', args: [expr, right], span: combineSpans(expr.span, right.span) };
        }
        return expr;
    }

    private parseAnd(): PredicateExpr {
        let expr = this.parseNot();
        while (this.matchKeyword('AND')) {
            const right = this.parseNot();
            expr = expr.kind === 'and'
                ? { ...expr, args: [...expr.args, right], span: combineSpans(expr.span, right.span) }
                : { kind: 'and', args: [expr, right], span: combineSpans(expr.span, right.span) };
        }
        return expr;
    }

    private parseNot(): PredicateExpr {
        if (this.matchKeyword('NOT')) {
            const start = this.previous().span;
            const arg = this.parseNot();
            return { kind: 'not', arg, span: combineSpans(start, arg.span) };
        }
        return this.parsePredicatePrimary();
    }

    private parsePredicatePrimary(): PredicateExpr {
        if (this.matchPunctuation('(')) {
            const start = this.previous().span;
            const expr = this.parsePredicate();
            const end = this.expectPunctuation(')').span;
            return { ...expr, span: combineSpans(start, end) };
        }
        if (this.matchKeyword('TRUE')) return { kind: 'true', span: this.previous().span };
        if (this.matchKeyword('FALSE')) return { kind: 'false', span: this.previous().span };
        if (this.matchKeyword('EXISTS')) {
            const start = this.previous().span;
            const table = this.expectIdentifierToken('EXISTS table');
            let where: PredicateExpr | undefined;
            let end = table.span;
            if (this.matchKeyword('WHERE')) {
                where = this.parsePredicate();
                end = where.span;
            }
            if (where === undefined) {
                this.diagnostics.add('PARSE_EXPECTED_TOKEN', 'EXISTS requires a WHERE clause', table.span);
                where = { kind: 'false', span: table.span };
            }
            return { kind: 'exists', table: table.text, where, span: combineSpans(start, end) };
        }

        const left = this.parseOperand();
        if (this.matchKeyword('LIKE')) {
            const pattern = this.parseValue();
            return { kind: 'like', left, pattern, span: combineSpans(left.span, pattern.span) };
        }
        if (this.checkKind('operator') && ['=', '!=', '<', '<=', '>', '>='].includes(this.peek().text)) {
            const op = this.advance().text as '=' | '!=' | '<' | '<=' | '>' | '>=';
            const right = this.parseOperand();
            return { kind: 'comparison', op, left, right, span: combineSpans(left.span, right.span) };
        }

        this.diagnostics.add('PARSE_EXPECTED_TOKEN', 'Expected predicate comparison', left.span);
        return { kind: 'true', span: left.span };
    }

    private parseOperand(): OperandExpr {
        if (this.checkKind('identifier')) {
            const tok = this.advance();
            return { kind: 'column', name: tok.text, span: tok.span };
        }
        return this.parseValue();
    }

    private parseValue(): ValueExpr {
        const tok = this.peek();
        if (tok.kind === 'identifier' && this.peekNext().text === '(') {
            return this.parseValueCall();
        }
        if (tok.kind === 'string' || tok.kind === 'number' ||
            (tok.kind === 'keyword' && ['TRUE', 'FALSE', 'NULL'].includes(tok.upper))) {
            this.advance();
            return { kind: 'literal', value: tok.value as json.Literal | null, span: tok.span };
        }
        if (tok.kind === 'variable') {
            this.advance();
            return { kind: 'variable', name: tok.text.substring(1), span: tok.span };
        }
        if (this.checkPunctuation('[') || this.checkPunctuation('{')) {
            return this.parseJsonValue();
        }
        this.diagnostics.add('PARSE_EXPECTED_TOKEN', `Expected literal or variable, got '${tok.text}'`, tok.span);
        this.advance();
        return { kind: 'literal', value: null, span: tok.span };
    }

    private parseValueCall(): ValueExpr {
        const name = this.expectIdentifierToken('value function');
        this.expectPunctuation('(');
        const args: ValueExpr[] = [];
        if (!this.checkPunctuation(')')) {
            do {
                args.push(this.parseValue());
            } while (this.matchPunctuation(','));
        }
        const end = this.expectPunctuation(')').span;
        if (name.text !== 'publicKey') {
            this.diagnostics.add('PARSE_EXPECTED_TOKEN', `Unknown value function '${name.text}'`, name.span);
        }
        if (name.text === 'publicKey' && args.length !== 1) {
            this.diagnostics.add('PARSE_EXPECTED_TOKEN', 'publicKey() expects exactly one argument', combineSpans(name.span, end));
        }
        return { kind: 'call', name: name.text, args, span: combineSpans(name.span, end) };
    }

    private parseJsonValue(): ValueExpr {
        const start = this.peek();
        const open = start.text;
        const close = open === '[' ? ']' : '}';
        let depth = 0;
        let raw = '';
        let end = start.span;
        do {
            const tok = this.advance();
            raw += tok.text;
            end = tok.span;
            if (tok.text === open) depth += 1;
            if (tok.text === close) depth -= 1;
        } while (!this.isEof() && depth > 0);
        try {
            return { kind: 'literal', value: JSON.parse(raw) as json.Literal | null, span: combineSpans(start.span, end) };
        } catch {
            this.diagnostics.add('PARSE_UNEXPECTED_TOKEN', 'Invalid JSON literal', combineSpans(start.span, end));
            return { kind: 'literal', value: null, span: combineSpans(start.span, end) };
        }
    }

    private parseAuthor(): AuthorExpr {
        const start = this.peek().span;
        if (this.matchKeyword('NOBODY')) return { kind: 'nobody', span: this.previous().span };
        if (this.checkKind('variable')) {
            const tok = this.advance();
            return { kind: 'variable', name: tok.text.substring(1), span: tok.span };
        }
        if (this.checkKind('hash')) {
            const tok = this.advance();
            return { kind: 'hash', prefix: tok.text.substring(1), span: tok.span };
        }
        this.diagnostics.add('PARSE_EXPECTED_TOKEN', 'Expected $identity, #keyid or NOBODY after BY', start);
        return { kind: 'nobody', span: start };
    }

    private parseVersion(): VersionExpr {
        if (this.matchKeyword('LATEST')) return { kind: 'latest', span: this.previous().span };
        if (this.checkKind('hash')) {
            const hash = this.parseHashRef();
            return { kind: 'hash', hash, span: hash.span };
        }
        const start = this.expectPunctuation('{').span;
        const hashes: HashRef[] = [];
        if (!this.checkPunctuation('}')) {
            do {
                hashes.push(this.parseHashRef());
            } while (this.matchPunctuation(','));
        }
        const end = this.expectPunctuation('}').span;
        return { kind: 'set', hashes, span: combineSpans(start, end) };
    }

    private parseRowIdPredicate(): NameOrHashRef | ValueExpr {
        const field = this.expectIdentifierToken('row id field');
        if (field.text !== 'rowId') {
            this.diagnostics.add('PARSE_EXPECTED_TOKEN', "Expected rowId predicate", field.span);
        }
        this.expectOperator('=');
        if (this.checkKind('hash')) return this.parseHashRef();
        if (this.checkKind('identifier')) {
            const tok = this.advance();
            return this.nameRef(tok.text, tok.span);
        }
        return this.parseValue();
    }

    private parseQualifiedColumn(): { table: string; column: string; span: TextSpan } {
        const tok = this.expectIdentifierToken('qualified column');
        const parts = tok.text.split('.');
        if (parts.length !== 2) {
            this.diagnostics.add('PARSE_EXPECTED_TOKEN', 'Expected qualified column in the form table.column', tok.span);
            return { table: tok.text, column: tok.text, span: tok.span };
        }
        return { table: parts[0], column: parts[1], span: tok.span };
    }

    private parseTableRef(defaultGroup?: NameOrHashRef): TableRef {
        const tok = this.expectIdentifierToken('table reference');
        const parts = tok.text.split('.');
        if (parts.length === 1 && defaultGroup !== undefined) {
            return { group: defaultGroup, table: tok.text, span: combineSpans(defaultGroup.span, tok.span) };
        }
        if (parts.length === 1) {
            return { table: tok.text, span: tok.span };
        }
        if (parts.length !== 2) {
            this.diagnostics.add('PARSE_EXPECTED_TOKEN', 'Expected table reference in the form group.table', tok.span);
        }
        const groupText = parts.length >= 2 ? parts.slice(0, parts.length - 1).join('.') : tok.text;
        const table = parts.length >= 2 ? parts[parts.length - 1] : tok.text;
        return { group: this.nameRef(groupText, tok.span), table, span: tok.span };
    }

    private parseNameOrHash(): NameOrHashRef {
        if (this.checkKind('hash')) return this.parseHashRef();
        const tok = this.expectIdentifierToken('name or hash reference');
        return this.nameRef(tok.text, tok.span);
    }

    private parseHashRef(): HashRef {
        const tok = this.expectKind('hash', 'hash reference');
        return { kind: 'hash', prefix: tok.text.substring(1), span: tok.span };
    }

    private nameRef(text: string, span: TextSpan): NameRef {
        return { kind: 'name', text, parts: text.split('.'), span };
    }

    private parseIdentifierList(label: string): string[] {
        const items: string[] = [];
        if (!this.checkPunctuation(')') && !this.checkKeyword('FROM')) {
            do {
                items.push(this.expectIdentifierText(label));
            } while (this.matchPunctuation(','));
        }
        return items;
    }

    private columnTypeFromToken(tok: Token): ColumnTypeName {
        const type = tok.upper.toLowerCase();
        if (['string', 'integer', 'float', 'boolean', 'json'].includes(type)) return type as ColumnTypeName;
        this.diagnostics.add('PARSE_EXPECTED_TOKEN', `Expected column type, got '${tok.text}'`, tok.span);
        return 'string';
    }

    private parseAllowRule(): AllowRuleExpr {
        const start = this.expectKeyword('ALLOW').span;
        const op = this.opTag(this.expectIdentifierLike('allow op').text);
        this.expectKeyword('IF');
        const predicate = this.parsePredicate();
        return { op, predicate, span: combineSpans(start, predicate.span) };
    }

    private validateAllowRules(rules: AllowRuleExpr[]): void {
        const seen = new Map<AllowOp, TextSpan>();
        const allRule = rules.find((rule) => rule.op === 'all');
        const specificRule = rules.find((rule) => rule.op !== 'all');

        for (const rule of rules) {
            if (seen.has(rule.op)) {
                this.diagnostics.add('PARSE_EXPECTED_TOKEN', `Duplicate ALLOW ${rule.op} rule`, rule.span);
            }
            seen.set(rule.op, rule.span);
        }

        if (allRule !== undefined && specificRule !== undefined) {
            this.diagnostics.add('PARSE_EXPECTED_TOKEN',
                'ALLOW all cannot be combined with operation-specific ALLOW rules',
                specificRule.span);
        }
    }

    private opTag(text: string): AllowOp {
        const tag = text.toLowerCase();
        if (tag === 'insert' || tag === 'update' || tag === 'delete' || tag === 'all') return tag;
        this.diagnostics.add('PARSE_EXPECTED_TOKEN', `Unknown allow op '${text}'`, this.previous().span);
        return 'all';
    }

    private expectInteger(label: string): number {
        const tok = this.expectKind('number', `${label} integer`);
        const n = Number(tok.value);
        if (!Number.isInteger(n) || n < 0) {
            this.diagnostics.add('PARSE_EXPECTED_TOKEN', `${label} must be a non-negative integer`, tok.span);
            return 0;
        }
        return n;
    }

    private consumeSemicolons(): void {
        while (this.matchPunctuation(';')) {}
    }

    private synchronizeStatement(): void {
        while (!this.isEof() && !this.checkPunctuation(';')) this.advance();
        this.matchPunctuation(';');
    }

    private expected(expected: string): void {
        this.diagnostics.add('PARSE_EXPECTED_TOKEN', `Expected ${expected}, got '${this.peek().text}'`, this.peek().span);
    }

    private expectKeyword(keyword: string): Token {
        if (this.matchKeyword(keyword)) return this.previous();
        this.expected(keyword);
        return this.peek();
    }

    private expectPunctuation(text: string): Token {
        if (this.matchPunctuation(text)) return this.previous();
        this.expected(`'${text}'`);
        return this.peek();
    }

    private expectOperator(text: string): Token {
        if (this.matchOperator(text)) return this.previous();
        this.expected(`'${text}'`);
        return this.peek();
    }

    private expectIdentifierText(label: string): string {
        return this.expectIdentifierToken(label).text;
    }

    private expectIdentifierToken(label: string): Token {
        return this.expectKind('identifier', label);
    }

    private expectIdentifierLike(label: string): Token {
        if (this.checkKind('identifier') || this.checkKind('keyword')) return this.advance();
        this.expected(label);
        return this.peek();
    }

    private expectKind(kind: Token['kind'], label: string): Token {
        if (this.checkKind(kind)) return this.advance();
        this.expected(label);
        return this.peek();
    }

    private matchKeyword(keyword: string): boolean {
        if (this.checkKeyword(keyword)) {
            this.advance();
            return true;
        }
        return false;
    }

    private matchPunctuation(text: string): boolean {
        if (this.checkPunctuation(text)) {
            this.advance();
            return true;
        }
        return false;
    }

    private matchOperator(text: string): boolean {
        if (this.checkKind('operator') && this.peek().text === text) {
            this.advance();
            return true;
        }
        return false;
    }

    private checkKeyword(keyword: string): boolean {
        return this.peek().kind === 'keyword' && this.peek().upper === keyword;
    }

    private checkPunctuation(text: string): boolean {
        return this.peek().kind === 'punctuation' && this.peek().text === text;
    }

    private checkKind(kind: Token['kind']): boolean {
        return this.peek().kind === kind;
    }

    private isEof(): boolean {
        return this.peek().kind === 'eof';
    }

    private peek(): Token {
        return this.tokens[this.pos];
    }

    private peekNext(): Token {
        return this.tokens[this.pos + 1] ?? this.tokens[this.tokens.length - 1];
    }

    private previous(): Token {
        return this.tokens[this.pos - 1];
    }

    private advance(): Token {
        if (!this.isEof()) this.pos += 1;
        return this.previous();
    }
}

export function parseScript(text: string): Result<AstScript> {
    const lexed = lex(text);
    if (!lexed.ok) return lexed;
    return new Parser(text, lexed.value).parseScript();
}

export function parseStatement(text: string): Result<AstStatement> {
    const parsed = parseScript(text);
    if (!parsed.ok) return parsed;
    if (parsed.value.statements.length !== 1) {
        const diagnostics = new DiagnosticBag();
        diagnostics.add('PARSE_EXPECTED_TOKEN', `Expected one statement, got ${parsed.value.statements.length}`, parsed.value.span);
        return err(diagnostics.all());
    }
    return ok(parsed.value.statements[0]);
}

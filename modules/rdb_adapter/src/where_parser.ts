// A small predicate parser for the query builder. It is not C-SQL: no EXISTS,
// no joins, no OFFSET. Parameters are `:name`.

import type { json } from "@hyper-hyper-space/hhs3_json";

import type { BoundOperand, BoundWhereExpr } from "./types.js";

export type ParamRef = { kind: 'param'; name: string };
export type TemplateOperand = BoundOperand | ParamRef;

export type WhereTemplate =
    | { kind: 'cmp'; op: '=' | '!=' | '<' | '<=' | '>' | '>='; left: TemplateOperand; right: TemplateOperand }
    | { kind: 'isNull'; arg: TemplateOperand; not: boolean }
    | { kind: 'in'; arg: TemplateOperand; name: string; not: boolean }
    | { kind: 'like'; arg: TemplateOperand; pattern: TemplateOperand }
    | { kind: 'not'; arg: WhereTemplate }
    | { kind: 'and'; args: WhereTemplate[] }
    | { kind: 'or'; args: WhereTemplate[] };

type Token =
    | { kind: 'ident'; text: string }
    | { kind: 'string'; text: string }
    | { kind: 'number'; value: number }
    | { kind: 'param'; name: string }
    | { kind: 'op'; text: string }
    | { kind: 'end' };

const KEYWORDS = new Set(['AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'LIKE', 'TRUE', 'FALSE']);

export function parseWhere(source: string): WhereTemplate {
    const parser = new Parser(source);
    const expr = parser.parseOr();
    parser.expectEnd();
    return expr;
}

class Parser {
    private readonly tokens: Token[];
    private at = 0;

    constructor(source: string) {
        this.tokens = tokenize(source);
    }

    parseOr(): WhereTemplate {
        const args = [this.parseAnd()];
        while (this.eatKeyword('OR')) args.push(this.parseAnd());
        return args.length === 1 ? args[0]! : { kind: 'or', args };
    }

    private parseAnd(): WhereTemplate {
        const args = [this.parseNot()];
        while (this.eatKeyword('AND')) args.push(this.parseNot());
        return args.length === 1 ? args[0]! : { kind: 'and', args };
    }

    private parseNot(): WhereTemplate {
        if (this.eatKeyword('NOT')) return { kind: 'not', arg: this.parseNot() };
        return this.parsePrimary();
    }

    private parsePrimary(): WhereTemplate {
        if (this.eatOp('(')) {
            const expr = this.parseOr();
            this.expectOp(')');
            return expr;
        }
        const left = this.parseOperand();
        if (this.eatKeyword('IS')) {
            const not = this.eatKeyword('NOT');
            this.expectKeyword('NULL');
            return { kind: 'isNull', arg: left, not };
        }
        if (this.eatKeyword('NOT')) {
            this.expectKeyword('IN');
            return { kind: 'in', arg: left, name: this.expectParam(), not: true };
        }
        if (this.eatKeyword('IN')) return { kind: 'in', arg: left, name: this.expectParam(), not: false };
        if (this.eatKeyword('LIKE')) return { kind: 'like', arg: left, pattern: this.parseOperand() };
        const op = this.expectCompare();
        return { kind: 'cmp', op, left, right: this.parseOperand() };
    }

    private parseOperand(): TemplateOperand {
        return this.parseSum();
    }

    private parseSum(): TemplateOperand {
        let left = this.parseProduct();
        for (;;) {
            if (this.eatOp('+')) left = { kind: 'arith', op: '+', left: asBound(left), right: asBound(this.parseProduct()) };
            else if (this.eatOp('-')) left = { kind: 'arith', op: '-', left: asBound(left), right: asBound(this.parseProduct()) };
            else return left;
        }
    }

    private parseProduct(): TemplateOperand {
        let left = this.parseUnary();
        while (this.eatOp('*')) left = { kind: 'arith', op: '*', left: asBound(left), right: asBound(this.parseUnary()) };
        return left;
    }

    private parseUnary(): TemplateOperand {
        if (this.peekKeyword('LENGTH')) {
            this.at++;
            this.expectOp('(');
            const arg = this.parseOperand();
            this.expectOp(')');
            return { kind: 'length', arg: asBound(arg) };
        }
        if (this.eatOp('(')) {
            const inner = this.parseOperand();
            this.expectOp(')');
            return inner;
        }
        const token = this.tokens[this.at++] ?? { kind: 'end' as const };
        if (token.kind === 'param') return { kind: 'param', name: token.name };
        if (token.kind === 'number') return { kind: 'lit', value: token.value };
        if (token.kind === 'string') return { kind: 'lit', value: token.text };
        if (token.kind === 'ident') {
            const upper = token.text.toUpperCase();
            if (upper === 'TRUE') return { kind: 'lit', value: true };
            if (upper === 'FALSE') return { kind: 'lit', value: false };
            if (upper === 'NULL') return { kind: 'lit', value: null };
            return { kind: 'col', name: token.text };
        }
        throw new Error(`expected a value, got ${JSON.stringify(token)}`);
    }

    private expectCompare(): '=' | '!=' | '<' | '<=' | '>' | '>=' {
        const token = this.tokens[this.at];
        if (token?.kind === 'op' && (token.text === '=' || token.text === '!=' || token.text === '<>'
            || token.text === '<' || token.text === '<=' || token.text === '>' || token.text === '>=')) {
            this.at++;
            return token.text === '<>' ? '!=' : token.text;
        }
        throw new Error(`expected a comparison at ${this.at}`);
    }

    private expectParam(): string {
        const token = this.tokens[this.at++];
        if (token?.kind !== 'param') throw new Error('IN expects a :name parameter');
        return token.name;
    }

    private eatKeyword(word: string): boolean {
        const token = this.tokens[this.at];
        if (token?.kind === 'ident' && token.text.toUpperCase() === word) { this.at++; return true; }
        return false;
    }

    private peekKeyword(word: string): boolean {
        const token = this.tokens[this.at];
        return token?.kind === 'ident' && token.text.toUpperCase() === word;
    }

    private eatOp(text: string): boolean {
        const token = this.tokens[this.at];
        if (token?.kind === 'op' && token.text === text) { this.at++; return true; }
        return false;
    }

    private expectOp(text: string): void {
        if (!this.eatOp(text)) throw new Error(`expected '${text}'`);
    }

    private expectKeyword(word: string): void {
        if (!this.eatKeyword(word)) throw new Error(`expected ${word}`);
    }

    expectEnd(): void {
        if ((this.tokens[this.at]?.kind ?? 'end') !== 'end') throw new Error('unexpected input after the predicate');
    }
}

function asBound(operand: TemplateOperand): BoundOperand {
    if (operand.kind === 'param') throw new Error('a parameter cannot be an arithmetic operand');
    return operand;
}

function tokenize(source: string): Token[] {
    const tokens: Token[] = [];
    let i = 0;
    const two = new Set(['!=', '<=', '>=', '<>']);
    while (i < source.length) {
        const ch = source[i]!;
        if (/\s/.test(ch)) { i++; continue; }
        if (ch === ':' ) {
            i++;
            const start = i;
            while (i < source.length && /[A-Za-z0-9_]/.test(source[i]!)) i++;
            if (start === i) throw new Error('expected a parameter name');
            tokens.push({ kind: 'param', name: source.slice(start, i) });
            continue;
        }
        if (ch === '\'' || ch === '"') {
            const quote = ch;
            i++;
            let text = '';
            while (i < source.length && source[i] !== quote) {
                if (source[i] === '\\') { text += source[i + 1] ?? ''; i += 2; continue; }
                text += source[i++];
            }
            if (source[i] !== quote) throw new Error('unterminated string');
            i++;
            tokens.push({ kind: 'string', text });
            continue;
        }
        if (/[0-9]/.test(ch) || (ch === '-' && /[0-9]/.test(source[i + 1] ?? ''))) {
            const start = i;
            if (source[i] === '-') i++;
            while (i < source.length && /[0-9.]/.test(source[i]!)) i++;
            tokens.push({ kind: 'number', value: Number(source.slice(start, i)) });
            continue;
        }
        if (/[A-Za-z_]/.test(ch)) {
            const start = i;
            while (i < source.length && /[A-Za-z0-9_]/.test(source[i]!)) i++;
            tokens.push({ kind: 'ident', text: source.slice(start, i) });
            continue;
        }
        const pair = source.slice(i, i + 2);
        if (two.has(pair)) { tokens.push({ kind: 'op', text: pair }); i += 2; continue; }
        tokens.push({ kind: 'op', text: ch });
        i++;
    }
    tokens.push({ kind: 'end' });
    return tokens;
}

export function templateParams(template: WhereTemplate): string[] {
    const names: string[] = [];
    visit(template);
    return names;
    function visit(node: WhereTemplate): void {
        switch (node.kind) {
            case 'cmp': operand(node.left); operand(node.right); return;
            case 'isNull': operand(node.arg); return;
            case 'like': operand(node.arg); operand(node.pattern); return;
            case 'in': operand(node.arg); names.push(node.name); return;
            case 'not': visit(node.arg); return;
            case 'and':
            case 'or': node.args.forEach(visit); return;
        }
    }
    function operand(node: TemplateOperand): void {
        if (node.kind === 'param') names.push(node.name);
    }
}

export type BoundParam = json.Literal | null | Array<json.Literal | null>;

export function bindWhere(template: WhereTemplate, values: { [name: string]: BoundParam }): BoundWhereExpr {
    return bind(template);
    function bind(node: WhereTemplate): BoundWhereExpr {
        switch (node.kind) {
            case 'cmp': return { kind: 'cmp', op: node.op, left: bindOperand(node.left), right: bindOperand(node.right) };
            case 'isNull': return { kind: 'isNull', arg: bindOperand(node.arg), not: node.not };
            case 'like': return { kind: 'like', arg: bindOperand(node.arg), pattern: bindOperand(node.pattern) };
            case 'in': return { kind: 'in', arg: bindOperand(node.arg), values: list(node.name), not: node.not };
            case 'not': return { kind: 'not', arg: bind(node.arg) };
            case 'and': return { kind: 'and', args: node.args.map(bind) };
            case 'or': return { kind: 'or', args: node.args.map(bind) };
        }
    }
    function bindOperand(node: TemplateOperand): BoundOperand {
        if (node.kind === 'param') return { kind: 'lit', value: scalar(node.name) };
        if (node.kind === 'arith') return { kind: 'arith', op: node.op, left: bindOperand(node.left), right: bindOperand(node.right) };
        if (node.kind === 'length') return { kind: 'length', arg: bindOperand(node.arg) };
        return node;
    }
    function scalar(name: string): json.Literal | null {
        const value = values[name];
        if (value === undefined) throw new Error(`missing parameter :${name}`);
        if (Array.isArray(value)) throw new Error(`:${name} is a list`);
        return value;
    }
    function list(name: string): Array<json.Literal | null> {
        const value = values[name];
        if (value === undefined) throw new Error(`missing parameter :${name}`);
        if (Array.isArray(value)) return value;
        return [value];
    }
}

import { parseMeshConfig, type MeshConfig } from "@hyper-hyper-space/hhs3_mesh";
import type { AllowSource } from "@hyper-hyper-space/hhs3_rhost";

export const SYNC_USAGE =
    'Usage: \\sync fetch #<rdb-id> as <local-id> [<mesh-json>|@<file>]\n' +
    '       \\sync start <db> as <local-id> [allow <sources>] [<mesh-json>|@<file>]\n' +
    '       \\sync status [<db>]\n' +
    '       \\sync stop <id>\n' +
    '       \\sync peers <id>';

export type SyncStartCommand = {
    kind: 'start';
    database: string;
    localId: string;
    sources: AllowSource[];
    mesh: MeshConfig;
    meshPath?: string;
};

export type SyncFetchCommand = {
    kind: 'fetch';
    rdbId: string;
    localId: string;
    mesh: MeshConfig;
    meshPath?: string;
};

export type SyncCommand =
    | SyncStartCommand
    | SyncFetchCommand
    | { kind: 'status'; database?: string }
    | { kind: 'stop'; id: number }
    | { kind: 'peers'; id: number };

// SHA-256 / blake3 digestSize is 32 bytes; standard btoa is 44 chars (43 + '=').
const FULL_HASH_RE = /^[A-Za-z0-9+/]{43,}={0,2}$/;

export function parseSyncCommand(remainder: string): SyncCommand {
    const p = new Parser(remainder);
    p.skipWs();
    if (p.done()) return { kind: 'status' };

    const sub = p.readToken();
    switch (sub) {
        case 'start': return p.parseStart();
        case 'fetch': return p.parseFetch();
        case 'status': {
            p.skipWs();
            if (p.done()) return { kind: 'status' };
            const database = p.readToken();
            p.expectEnd();
            return { kind: 'status', database };
        }
        case 'stop':
        case 'peers': {
            p.skipWs();
            if (p.done()) throw new Error(`${SYNC_USAGE}`);
            const id = parseSessionId(p.readToken());
            p.expectEnd();
            return { kind: sub, id };
        }
        default:
            throw new Error(`${SYNC_USAGE}`);
    }
}

function parseSessionId(raw: string): number {
    if (!/^\d+$/.test(raw)) throw new Error(`\\sync stop/peers require a numeric session id, got '${raw}'`);
    return Number(raw);
}

function parseFullRdbId(token: string): string {
    if (!token.startsWith('#')) {
        throw new Error(`\\sync fetch requires a full #hash, got '${token}'`);
    }
    const id = token.slice(1);
    if (!FULL_HASH_RE.test(id)) {
        throw new Error(`\\sync fetch requires a full object hash (topics are exact ids), got '${token}'`);
    }
    return id;
}

class Parser {
    private readonly s: string;
    private i = 0;

    constructor(s: string) {
        this.s = s;
    }

    skipWs(): void {
        while (this.i < this.s.length && /\s/.test(this.s[this.i]!)) this.i += 1;
    }

    done(): boolean {
        this.skipWs();
        return this.i >= this.s.length;
    }

    expectEnd(): void {
        if (!this.done()) {
            throw new Error(`Unexpected token '${this.readToken()}'. ${SYNC_USAGE}`);
        }
    }

    peek(): string {
        return this.s[this.i] ?? '';
    }

    parseStart(): SyncStartCommand {
        this.skipWs();
        if (this.done()) throw new Error(SYNC_USAGE);
        const database = this.readToken();
        this.expectKeyword('as');
        const localId = this.readToken();

        let sources: AllowSource[] = [{ type: 'everyone' }];
        if (this.tryKeyword('allow')) {
            sources = this.parseSources();
            if (sources.length === 0) throw new Error('allow list is empty');
        }

        const mesh = this.parseMesh();
        return { kind: 'start', database, localId, sources, ...mesh };
    }

    parseFetch(): SyncFetchCommand {
        this.skipWs();
        if (this.done()) throw new Error(SYNC_USAGE);
        const rdbId = parseFullRdbId(this.readToken());
        this.expectKeyword('as');
        const localId = this.readToken();
        if (this.tryKeyword('allow')) {
            throw new Error('\\sync fetch does not take allow; use \\sync start to share');
        }
        const mesh = this.parseMesh();
        return { kind: 'fetch', rdbId, localId, ...mesh };
    }

    // No mesh argument is internet. `{...}` is inline JSON. `@path` is a file
    // the command reads.
    parseMesh(): { mesh: MeshConfig; meshPath?: string } {
        this.skipWs();
        if (this.done()) return { mesh: { scope: 'internet' } };
        if (this.peek() === '{') {
            const raw = this.readJsonObject();
            let value: unknown;
            try {
                value = JSON.parse(raw);
            } catch (err) {
                throw new Error(`mesh JSON is not valid: ${err instanceof Error ? err.message : String(err)}`);
            }
            const mesh = parseMeshConfig(value, '\\sync', 'mesh');
            this.expectEnd();
            return { mesh: { ...mesh, scope: mesh.scope ?? 'internet' } };
        }
        if (this.peek() === '@') {
            this.i += 1;
            const meshPath = this.readToken();
            if (meshPath.length === 0) throw new Error(`${SYNC_USAGE}`);
            this.expectEnd();
            return { mesh: { scope: 'internet' }, meshPath };
        }
        throw new Error(`Unexpected token '${this.readToken()}'. ${SYNC_USAGE}`);
    }

    readJsonObject(): string {
        const start = this.i;
        let depth = 0;
        let quote: '"' | undefined;
        let escape = false;
        while (this.i < this.s.length) {
            const ch = this.s[this.i]!;
            if (quote !== undefined) {
                if (escape) escape = false;
                else if (ch === '\\') escape = true;
                else if (ch === quote) quote = undefined;
                this.i += 1;
                continue;
            }
            if (ch === '"') {
                quote = '"';
                this.i += 1;
                continue;
            }
            if (ch === '{') {
                depth += 1;
                this.i += 1;
                continue;
            }
            if (ch === '}') {
                depth -= 1;
                this.i += 1;
                if (depth === 0) return this.s.slice(start, this.i);
                continue;
            }
            this.i += 1;
        }
        throw new Error('mesh JSON object is not closed');
    }

    parseSources(): AllowSource[] {
        this.skipWs();
        if (this.peek() === '[') {
            this.i += 1;
            const items: AllowSource[] = [];
            this.skipWs();
            if (this.peek() === ']') {
                this.i += 1;
                return items;
            }
            while (true) {
                items.push(this.parseOneSource(true));
                this.skipWs();
                if (this.peek() === ']') {
                    this.i += 1;
                    return items;
                }
                if (this.peek() === ',') {
                    this.i += 1;
                    continue;
                }
                throw new Error("Expected ',' or ']' in allow list");
            }
        }
        return [this.parseOneSource(false)];
    }

    parseOneSource(inList: boolean): AllowSource {
        this.skipWs();
        if (this.isBareEveryone()) {
            this.consumeWord('everyone');
            this.skipWs();
            if (this.tryKeyword('where')) {
                throw new Error("'everyone' cannot have a WHERE clause");
            }
            return { type: 'everyone' };
        }

        const pathStart = this.i;
        const group = this.readIdent();
        if (this.peek() !== '.') {
            throw new Error(`Expected group.table.column or everyone, got '${this.s.slice(pathStart).split(/\s/)[0]}'`);
        }
        this.i += 1;
        const table = this.readIdent(false);
        if (this.peek() !== '.') {
            throw new Error(`Expected group.table.column or everyone, got '${group}.${table}'`);
        }
        this.i += 1;
        const column = this.readIdent(false);
        let where: string | undefined;
        if (this.tryKeyword('where')) {
            where = this.readWhere(inList);
            if (where.length === 0) throw new Error('Expected a condition after where');
        }
        return { type: 'column', group, table, column, ...(where !== undefined ? { where } : {}) };
    }

    readIdent(skipWhitespace = true): string {
        if (skipWhitespace) this.skipWs();
        if (this.done() || !/[A-Za-z_]/.test(this.peek())) {
            throw new Error('Expected an identifier');
        }
        const start = this.i;
        this.i += 1;
        while (this.i < this.s.length && /[A-Za-z0-9_]/.test(this.s[this.i]!)) this.i += 1;
        return this.s.slice(start, this.i);
    }

    readToken(): string {
        this.skipWs();
        if (this.done()) throw new Error(SYNC_USAGE);
        const start = this.i;
        while (this.i < this.s.length && !/\s/.test(this.s[this.i]!)) this.i += 1;
        return this.s.slice(start, this.i);
    }

    expectKeyword(word: string): void {
        this.skipWs();
        if (!this.tryKeyword(word)) {
            throw new Error(`Expected '${word}'`);
        }
    }

    tryKeyword(word: string): boolean {
        this.skipWs();
        if (!this.matchWord(this.i, word)) return false;
        this.i += word.length;
        return true;
    }

    consumeWord(word: string): void {
        this.skipWs();
        if (!this.matchWord(this.i, word)) throw new Error(`Expected '${word}'`);
        this.i += word.length;
    }

    isBareEveryone(): boolean {
        this.skipWs();
        if (!this.matchWord(this.i, 'everyone')) return false;
        const after = this.i + 'everyone'.length;
        return this.s[after] !== '.';
    }

    matchWord(at: number, word: string): boolean {
        if (this.s.slice(at, at + word.length) !== word) return false;
        const after = at + word.length;
        if (after < this.s.length && /[A-Za-z0-9_]/.test(this.s[after]!)) return false;
        return true;
    }

    readWhere(inList: boolean): string {
        this.skipWs();
        const start = this.i;
        let quote: "'" | '"' | undefined;
        let depth = 0;
        while (this.i < this.s.length) {
            const ch = this.s[this.i]!;
            if (quote !== undefined) {
                if (ch === quote) {
                    if (quote === "'" && this.s[this.i + 1] === "'") {
                        this.i += 2;
                        continue;
                    }
                    quote = undefined;
                    this.i += 1;
                    continue;
                }
                this.i += 1;
                continue;
            }
            if (ch === "'" || ch === '"') {
                quote = ch;
                this.i += 1;
                continue;
            }
            if (ch === '(' || ch === '[') {
                depth += 1;
                this.i += 1;
                continue;
            }
            if ((ch === ')' || ch === ']') && depth > 0) {
                depth -= 1;
                this.i += 1;
                continue;
            }
            if (depth === 0) {
                if (inList && (ch === ',' || ch === ']')) break;
                if (!inList && (this.peek() === '{' || this.peek() === '@')) break;
            }
            this.i += 1;
        }
        return this.s.slice(start, this.i).trim();
    }
}

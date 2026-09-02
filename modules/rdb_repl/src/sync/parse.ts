export const SYNC_USAGE =
    'Usage: \\sync fetch #<rdb-id> as <local-id> on <localhost|internet> [--tracker URL] [--tracker-key KEYID] [--listen ADDR]\n' +
    '       \\sync start <db> as <local-id> [allow <sources>] on <localhost|internet> [--tracker URL] [--tracker-key KEYID] [--listen ADDR]\n' +
    '       \\sync status [<db>]\n' +
    '       \\sync stop <id>\n' +
    '       \\sync peers <id>';

export type SyncScope = 'localhost' | 'internet';

export type AllowSource =
    | { type: 'everyone' }
    | { type: 'column'; group: string; table: string; column: string; where?: string };

export type SyncStartCommand = {
    kind: 'start';
    database: string;
    localId: string;
    sources: AllowSource[];
    scope: SyncScope;
    tracker?: string;
    trackerKey?: string;
    listen?: string;
};

export type SyncFetchCommand = {
    kind: 'fetch';
    rdbId: string;
    localId: string;
    scope: SyncScope;
    tracker?: string;
    trackerKey?: string;
    listen?: string;
};

export type SyncCommand =
    | SyncStartCommand
    | SyncFetchCommand
    | { kind: 'status'; database?: string }
    | { kind: 'stop'; id: number }
    | { kind: 'peers'; id: number };

// SHA-256 / blake3 digestSize is 32 bytes; standard btoa is 44 chars (43 + '=').
const FULL_HASH_RE = /^[A-Za-z0-9+/]{43,}={0,2}$/;

const FLAG_NAMES = new Set(['--tracker', '--tracker-key', '--listen']);

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

export function allowIsEveryone(sources: AllowSource[]): boolean {
    return sources.length === 0 || sources.some((s) => s.type === 'everyone');
}

export function formatAllow(sources: AllowSource[]): string {
    if (allowIsEveryone(sources)) return 'everyone';
    const parts = sources.map(formatAllowSource);
    return parts.length === 1 ? parts[0]! : `[${parts.join(', ')}]`;
}

export function formatAllowSource(source: AllowSource): string {
    if (source.type === 'everyone') return 'everyone';
    const path = `${source.group}.${source.table}.${source.column}`;
    return source.where === undefined ? path : `${path} where ${source.where}`;
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

        this.expectKeyword('on');
        const scope = this.parseScope();
        const flags = this.parseFlags();
        this.expectEnd();
        return {
            kind: 'start',
            database,
            localId,
            sources,
            scope,
            ...flags,
        };
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
        this.expectKeyword('on');
        const scope = this.parseScope();
        const flags = this.parseFlags();
        this.expectEnd();
        return { kind: 'fetch', rdbId, localId, scope, ...flags };
    }

    parseScope(): SyncScope {
        const scopeTok = this.readToken();
        if (scopeTok !== 'localhost' && scopeTok !== 'internet') {
            throw new Error(`Expected localhost or internet after on, got '${scopeTok}'`);
        }
        return scopeTok;
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

    parseFlags(): { tracker?: string; trackerKey?: string; listen?: string } {
        const out: { tracker?: string; trackerKey?: string; listen?: string } = {};
        while (true) {
            this.skipWs();
            if (this.done() || !this.s.startsWith('--', this.i)) break;
            const name = this.readToken();
            if (!FLAG_NAMES.has(name)) throw new Error(`Unknown flag '${name}'. ${SYNC_USAGE}`);
            this.skipWs();
            if (this.done() || this.s.startsWith('--', this.i)) {
                throw new Error(`${name} requires a value`);
            }
            const value = this.readToken();
            if (name === '--tracker') {
                if (out.tracker !== undefined) throw new Error('duplicate --tracker');
                out.tracker = value;
            } else if (name === '--tracker-key') {
                if (out.trackerKey !== undefined) throw new Error('duplicate --tracker-key');
                out.trackerKey = value;
            } else {
                if (out.listen !== undefined) throw new Error('duplicate --listen');
                out.listen = value;
            }
        }
        return out;
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
                if (!inList && this.matchWord(this.i, 'on')) break;
                if (this.s.startsWith('--', this.i) && (this.i === 0 || /\s/.test(this.s[this.i - 1]!))) break;
            }
            this.i += 1;
        }
        return this.s.slice(start, this.i).trim();
    }
}

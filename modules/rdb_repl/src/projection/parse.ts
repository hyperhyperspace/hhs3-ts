export const PROJECT_USAGE =
    'Usage: \\project start <db> as <local-id> to <path>\n' +
    '       \\project status [<db>]\n' +
    '       \\project stop <id>\n' +
    '       \\project update <id>\n' +
    '       \\project events <id>\n' +
    '       \\project register-key <id> <keyHash> <publicKey>\n' +
    '       \\project resolve-key <id> <token>';

export type ProjectStartCommand = {
    kind: 'start';
    database: string;
    localId: string;
    path: string;
};

export type ProjectCommand =
    | ProjectStartCommand
    | { kind: 'status'; database?: string }
    | { kind: 'stop'; id: number }
    | { kind: 'update'; id: number }
    | { kind: 'events'; id: number }
    | { kind: 'register-key'; id: number; keyHash: string; publicKey: string }
    | { kind: 'resolve-key'; id: number; token: string };

export function parseProjectCommand(remainder: string): ProjectCommand {
    const p = new Parser(remainder);
    p.skipWs();
    if (p.done()) return { kind: 'status' };

    const sub = p.readToken();
    switch (sub) {
        case 'start': return p.parseStart();
        case 'status': {
            p.skipWs();
            if (p.done()) return { kind: 'status' };
            const database = p.readToken();
            p.expectEnd();
            return { kind: 'status', database };
        }
        case 'stop':
        case 'update':
        case 'events': {
            p.skipWs();
            if (p.done()) throw new Error(PROJECT_USAGE);
            const id = parseSessionId(p.readToken());
            p.expectEnd();
            return { kind: sub, id };
        }
        case 'register-key': {
            p.skipWs();
            if (p.done()) throw new Error(PROJECT_USAGE);
            const id = parseSessionId(p.readToken());
            const keyHash = p.readToken();
            const publicKey = p.readToken();
            p.expectEnd();
            return { kind: 'register-key', id, keyHash, publicKey };
        }
        case 'resolve-key': {
            p.skipWs();
            if (p.done()) throw new Error(PROJECT_USAGE);
            const id = parseSessionId(p.readToken());
            const token = p.readToken();
            p.expectEnd();
            return { kind: 'resolve-key', id, token };
        }
        default:
            throw new Error(PROJECT_USAGE);
    }
}

function parseSessionId(raw: string): number {
    if (!/^\d+$/.test(raw)) {
        throw new Error(`\\project stop/update/events require a numeric session id, got '${raw}'`);
    }
    return Number(raw);
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
            throw new Error(`Unexpected token '${this.readToken()}'. ${PROJECT_USAGE}`);
        }
    }

    peek(): string {
        return this.s[this.i] ?? '';
    }

    parseStart(): ProjectStartCommand {
        this.skipWs();
        if (this.done()) throw new Error(PROJECT_USAGE);
        const database = this.readToken();
        this.expectKeyword('as');
        const localId = this.readToken();
        this.expectKeyword('to');
        const path = this.readPath();
        this.expectEnd();
        return { kind: 'start', database, localId, path };
    }

    readPath(): string {
        this.skipWs();
        if (this.done()) throw new Error(PROJECT_USAGE);
        const quote = this.peek();
        if (quote === '"' || quote === "'") {
            this.i += 1;
            const start = this.i;
            while (this.i < this.s.length && this.s[this.i] !== quote) this.i += 1;
            if (this.i >= this.s.length) throw new Error(`Unclosed ${quote} in path`);
            const path = this.s.slice(start, this.i);
            this.i += 1;
            if (path.length === 0) throw new Error('Expected a path after to');
            return path;
        }
        return this.readToken();
    }

    readToken(): string {
        this.skipWs();
        if (this.done()) throw new Error(PROJECT_USAGE);
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

    matchWord(at: number, word: string): boolean {
        if (this.s.slice(at, at + word.length) !== word) return false;
        const after = at + word.length;
        if (after < this.s.length && /[A-Za-z0-9_]/.test(this.s[after]!)) return false;
        return true;
    }
}

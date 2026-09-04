// One-line diagnostic tracer. Callers gate with their own TRACE_* constants;
// this helper always prints when invoked.

let origin: number | undefined;
let last: number | undefined;

const HASH_MIN = 16;
const HASH_KEEP = 12;

function shortHashLike(value: string): string {
    const at = value.indexOf('@');
    if (at > HASH_MIN) {
        return shortHashLike(value.slice(0, at)) + value.slice(at);
    }
    if (value.includes('://') || value.includes(' ') || value.length <= HASH_MIN) return value;
    return value.slice(0, HASH_KEEP) + '…';
}

function formatValue(value: unknown): string {
    if (value === undefined || value === null) return String(value);
    if (typeof value === 'string') return shortHashLike(value);
    if (typeof value === 'number' || typeof value === 'boolean') return String(value);
    if (Array.isArray(value)) return '[' + value.map(formatValue).join(',') + ']';
    try {
        return JSON.stringify(value);
    } catch {
        return String(value);
    }
}

export function trace(event: string, fields?: Record<string, unknown>): void {
    const now = Date.now();
    if (origin === undefined) origin = now;
    const t = now - origin;
    const dt = last === undefined ? 0 : now - last;
    last = now;

    let line = `[hhs3 ${new Date(now).toISOString()} ${t} +${dt}] ${event}`;
    if (fields !== undefined) {
        for (const [key, value] of Object.entries(fields)) {
            if (value === undefined) continue;
            line += ` ${key}=${formatValue(value)}`;
        }
    }
    console.log(line);
}

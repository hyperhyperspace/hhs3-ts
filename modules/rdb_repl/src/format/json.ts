export function formatJson(value: unknown): string {
    return JSON.stringify(value, undefined, 2);
}

// Portable file paths for RFileMap elements.
//
// A path is '/'-separated and relative. The rules keep every valid path
// writable on the common desktop filesystems (APFS, ext4, NTFS), so a mount
// never has to invent a name for a synced file:
//
//   - well-formed UTF-16 (no lone surrogates), NFC-normalized;
//   - 1..MAX_PATH_BYTES UTF-8 bytes, 1..MAX_PATH_SEGMENTS segments of
//     1..MAX_SEGMENT_BYTES bytes each;
//   - no empty segment (so no leading, trailing or doubled '/'), '.' or '..';
//   - no C0 control, DEL, or any of \ : * ? " < > |;
//   - no segment ending in '.' or ' ';
//   - no Windows reserved device name (CON, PRN, AUX, NUL, COM1-9, LPT1-9),
//     compared case-insensitively on the part before the first '.'.
//
// Case-fold collisions are legal here; mounts give the colliding files
// distinct disk names.

export const MAX_PATH_BYTES = 1024;
export const MAX_PATH_SEGMENTS = 32;
export const MAX_SEGMENT_BYTES = 255;

const FORBIDDEN_CHARS = new Set(['\\', ':', '*', '?', '"', '<', '>', '|']);
const RESERVED_NAMES = new Set([
    'CON', 'PRN', 'AUX', 'NUL',
    'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
    'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9',
]);

const encoder = new TextEncoder();

function isWellFormed(s: string): boolean {
    for (let i = 0; i < s.length; i++) {
        const c = s.charCodeAt(i);
        if (c >= 0xd800 && c <= 0xdbff) {
            const d = i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
            if (d < 0xdc00 || d > 0xdfff) return false;
            i++;
        } else if (c >= 0xdc00 && c <= 0xdfff) {
            return false;
        }
    }
    return true;
}

function segmentReason(segment: string): string | undefined {
    if (segment.length === 0) return 'has an empty segment';
    if (segment === '.' || segment === '..') return `has a '${segment}' segment`;
    if (encoder.encode(segment).length > MAX_SEGMENT_BYTES) return `has a segment longer than ${MAX_SEGMENT_BYTES} bytes`;
    for (const ch of segment) {
        const code = ch.codePointAt(0)!;
        if (code < 0x20 || code === 0x7f) return 'contains a control character';
        if (FORBIDDEN_CHARS.has(ch)) return `contains '${ch}'`;
    }
    const last = segment[segment.length - 1];
    if (last === '.' || last === ' ') return `has a segment ending in '${last}'`;
    const dot = segment.indexOf('.');
    const stem = (dot < 0 ? segment : segment.slice(0, dot)).toUpperCase();
    if (RESERVED_NAMES.has(stem)) return `has the reserved name '${segment}'`;
    return undefined;
}

// undefined when `path` is valid, else why not.
export function filePathReason(path: unknown): string | undefined {
    if (typeof path !== 'string') return 'is not a string';
    if (path.length === 0) return 'is empty';
    if (!isWellFormed(path)) return 'is not well-formed Unicode';
    if (path.normalize('NFC') !== path) return 'is not NFC-normalized';
    if (encoder.encode(path).length > MAX_PATH_BYTES) return `is longer than ${MAX_PATH_BYTES} bytes`;
    const segments = path.split('/');
    if (segments.length > MAX_PATH_SEGMENTS) return `has more than ${MAX_PATH_SEGMENTS} segments`;
    for (const segment of segments) {
        const reason = segmentReason(segment);
        if (reason !== undefined) return reason;
    }
    return undefined;
}

export function isValidFilePath(path: unknown): path is string {
    return filePathReason(path) === undefined;
}

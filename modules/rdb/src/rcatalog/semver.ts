// Strict semantic versions (MAJOR.MINOR.PATCH, no pre-release or build tags)
// and the version ranges the adoption policy uses.
//
// Ranges:
//   '*'          every version
//   'M.m.p'      exactly that version
//   '^M'         >= M.0.0 and < (M+1).0.0
//   '^M.m'       as '^M.m.0'
//   '^M.m.p'     caret semantics: the leftmost non-zero component is fixed
//                (^1.2.3 = [1.2.3, 2.0.0), ^0.2.3 = [0.2.3, 0.3.0),
//                ^0.0.3 = [0.0.3, 0.0.4))

import { MAX_SEMVER_LENGTH } from "./payload.js";

export type SemVer = { major: number; minor: number; patch: number };

const SEMVER_REGEX = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;

function component(s: string): number | undefined {
    const n = Number(s);
    return Number.isSafeInteger(n) ? n : undefined;
}

export function parseSemver(s: string): SemVer | undefined {
    if (s.length > MAX_SEMVER_LENGTH) return undefined;
    const m = SEMVER_REGEX.exec(s);
    if (m === null) return undefined;
    const major = component(m[1]);
    const minor = component(m[2]);
    const patch = component(m[3]);
    if (major === undefined || minor === undefined || patch === undefined) return undefined;
    return { major, minor, patch };
}

export function isValidSemver(s: string): boolean {
    return parseSemver(s) !== undefined;
}

export function formatSemver(v: SemVer): string {
    return `${v.major}.${v.minor}.${v.patch}`;
}

function compareParsed(a: SemVer, b: SemVer): number {
    if (a.major !== b.major) return a.major < b.major ? -1 : 1;
    if (a.minor !== b.minor) return a.minor < b.minor ? -1 : 1;
    if (a.patch !== b.patch) return a.patch < b.patch ? -1 : 1;
    return 0;
}

// Compares two valid semver strings; throws on an invalid one.
export function compareSemver(a: string, b: string): number {
    const pa = parseSemver(a);
    const pb = parseSemver(b);
    if (pa === undefined) throw new Error(`invalid semver '${a}'`);
    if (pb === undefined) throw new Error(`invalid semver '${b}'`);
    return compareParsed(pa, pb);
}

export type SemverRange =
    | { kind: 'any' }
    | { kind: 'interval'; min: SemVer; maxExclusive: SemVer };

const CARET_REGEX = /^\^(0|[1-9]\d*)(?:\.(0|[1-9]\d*)(?:\.(0|[1-9]\d*))?)?$/;

export function parseSemverRange(s: string): SemverRange | undefined {
    const text = s.trim();
    if (text === '*') return { kind: 'any' };

    const exact = parseSemver(text);
    if (exact !== undefined) {
        return { kind: 'interval', min: exact, maxExclusive: { ...exact, patch: exact.patch + 1 } };
    }

    const m = CARET_REGEX.exec(text);
    if (m === null) return undefined;
    const major = component(m[1]);
    const minor = m[2] !== undefined ? component(m[2]) : 0;
    const patch = m[3] !== undefined ? component(m[3]) : 0;
    if (major === undefined || minor === undefined || patch === undefined) return undefined;
    const min = { major, minor, patch };

    if (major > 0 || m[2] === undefined) {
        return { kind: 'interval', min, maxExclusive: { major: major + 1, minor: 0, patch: 0 } };
    }
    if (minor > 0 || m[3] === undefined) {
        return { kind: 'interval', min, maxExclusive: { major: 0, minor: minor + 1, patch: 0 } };
    }
    return { kind: 'interval', min, maxExclusive: { major: 0, minor: 0, patch: patch + 1 } };
}

export function isValidSemverRange(s: string): boolean {
    return parseSemverRange(s) !== undefined;
}

export function semverInRange(version: string, range: string): boolean {
    const v = parseSemver(version);
    const r = parseSemverRange(range);
    if (v === undefined || r === undefined) return false;
    if (r.kind === 'any') return true;
    return compareParsed(v, r.min) >= 0 && compareParsed(v, r.maxExclusive) < 0;
}

// The default adoption range for a database created at `version`: every
// release of the same major version.
export function majorRange(version: string): string {
    const v = parseSemver(version);
    if (v === undefined) throw new Error(`invalid semver '${version}'`);
    return `^${v.major}`;
}

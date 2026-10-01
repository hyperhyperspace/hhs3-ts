import { compareSemver, parseSemver } from "@hyper-hyper-space/hhs3_rdb";
import { releaseTag, type ReleaseFile } from "@hyper-hyper-space/hhs3_rpack";

export class ShippedReleaseError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ShippedReleaseError';
    }
}

function label(file: ReleaseFile): string {
    return `${file.manifest.name} ${file.manifest.version} (tag ${releaseTag(file.manifest.release)})`;
}

// The release file rhost uses for each catalog, by catalog name: the one with
// the highest version. Each file carries its release's whole past, so older
// files add nothing. Two files with the same version for one catalog, or two
// catalogs with one name, are an error.
export function shippedReleases(files: ReleaseFile[]): Map<string, ReleaseFile> {
    const byName = new Map<string, ReleaseFile>();
    const versions = new Map<string, Map<string, ReleaseFile>>();
    for (const file of files) {
        const { name, catalog, version } = file.manifest;
        const current = byName.get(name);
        if (current !== undefined && current.manifest.catalog !== catalog) {
            throw new ShippedReleaseError(
                `two catalogs named '${name}' ship: #${current.manifest.catalog} and #${catalog}`,
            );
        }
        const seen = versions.get(name) ?? new Map<string, ReleaseFile>();
        const clash = seen.get(version);
        if (clash !== undefined) {
            throw new ShippedReleaseError(`two release files ship ${name} ${version}: ${label(clash)} and ${label(file)}`);
        }
        seen.set(version, file);
        versions.set(name, seen);
        if (current === undefined || compareSemver(version, current.manifest.version) > 0) byName.set(name, file);
    }
    return byName;
}

// Everything below the next major: a host admits every release its code can
// read, and holds the ones above it.
export function adoptionRangeFor(version: string): string {
    const parsed = parseSemver(version);
    if (parsed === undefined) throw new Error(`invalid semver '${version}'`);
    return `<${parsed.major + 1}.0.0`;
}

export function majorOf(version: string): number {
    const parsed = parseSemver(version);
    if (parsed === undefined) throw new Error(`invalid semver '${version}'`);
    return parsed.major;
}

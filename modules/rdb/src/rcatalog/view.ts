// RCatalogView: synchronous accessors over a catalog index at a version.

import { B64Hash, KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { Version, version } from "@hyper-hyper-space/hhs3_mvt";

import type { RCatalog, RCatalogView } from "./interfaces.js";
import type { CatalogGroupDef, SchemaCreator } from "./payload.js";
import { CatalogIndex, DeclareInfo, ReleaseState } from "./resolve.js";

export class RCatalogViewImpl implements RCatalogView {

    private readonly releases: B64Hash[];
    private readonly releaseSet: Set<B64Hash>;
    private readonly maximal: B64Hash[];

    constructor(
        private readonly target: RCatalog,
        private readonly index: CatalogIndex,
        private readonly at: Version,
        private readonly from: Version,
    ) {
        this.releases = index.releasesAt(at);
        this.releaseSet = new Set(this.releases);
        this.maximal = index.maximalReleasesAt(at);
    }

    getObject(): RCatalog { return this.target; }
    getVersion(): Version { return this.at; }
    getFromVersion(): Version { return this.from; }

    async getReferences(): Promise<B64Hash[]> {
        return this.getReferencedSchemas();
    }

    // The version at which the catalog references a schema: the union, over
    // the maximal releases, of the versions of the groups using it.
    async resolveRefVersion(refId: B64Hash): Promise<Version> {
        const resolved = version();
        for (const release of this.maximal) {
            for (const group of this.index.releaseState(release).groups.values()) {
                if (group.schema !== refId) continue;
                for (const h of group.version) resolved.add(h);
            }
        }
        return resolved;
    }

    getName(): string { return this.index.getGenesis().name; }
    getCreators(): SchemaCreator[] { return this.index.getCreators(); }
    isCreator(keyId: KeyId): boolean { return this.getCreators().some((c) => c.keyId === keyId); }

    getGenesisHash(): B64Hash { return this.index.getGenesisHash(); }

    getReleaseHashes(): B64Hash[] { return [...this.releases]; }

    getRelease(hash: B64Hash): ReleaseState | undefined {
        return this.releaseSet.has(hash) ? this.index.releaseState(hash) : undefined;
    }

    getMaximalReleases(): B64Hash[] { return [...this.maximal]; }

    findReleasesByVersion(v: string): B64Hash[] {
        return this.releases.filter((r) => this.index.releaseState(r).version === v).sort();
    }

    isReleaseBelow(a: B64Hash, b: B64Hash): boolean {
        if (!this.releaseSet.has(a) || !this.releaseSet.has(b)) return false;
        return this.index.isReleaseBelow(a, b);
    }

    getGroupDef(hash: B64Hash): CatalogGroupDef | undefined {
        for (const release of this.maximal) {
            const def = this.index.releaseState(release).defs.get(hash);
            if (def !== undefined) return def;
        }
        return undefined;
    }

    getReferencedSchemas(): B64Hash[] {
        return [...this.index.referencedSchemasAt(this.at)].sort();
    }

    getDeclares(): DeclareInfo[] {
        return this.index.declaresAt(this.at);
    }
}

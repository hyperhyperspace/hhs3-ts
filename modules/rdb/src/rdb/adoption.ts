// The adoption policy: which deployed releases this replica adopts.
//
// For every release the RDb has deployed (its whole deploy history, so a
// release stays adopted once a later one supersedes it) whose version is in
// the adoption range, and every member group of that release, it admits the
// release's version of the group into the group's RDeployGate. Admissions are
// permanent (the gate only appends), and a synced deploy to a version this
// replica has not admitted waits in the synchronizer until it is.
//
// No extra wait is needed here: a deployed release is valid locally only once
// its schemas are present at the versions it pins. If the policy never runs,
// synced deploys simply wait, which is the safe default.
//
// The range comes from the runtime config and defaults to the major version of
// the release the RDb was created at ('^<major>'): patches and minors are
// adopted automatically, a new major waits until the app widens the range.

import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import type { RContext } from "@hyper-hyper-space/hhs3_mvt";

import { ensureDeployGate } from "../rdeploy_gate/rdeploy_gate.js";
import { semverInRange } from "../rcatalog/semver.js";
import type { RDbResolution } from "./resolve.js";

export type AdoptionHost = {
    getContext(): RContext;
    getBackendLabel(): string;
    resolve(): Promise<RDbResolution>;
    getAdoptionRange(): Promise<string>;
};

// The deployed releases (history order) within the range.
export function adoptedReleases(resolution: RDbResolution, range: string): B64Hash[] {
    if (resolution.releases === undefined) return [];
    return resolution.history.filter((r) => {
        const state = resolution.releases!.get(r);
        return state !== undefined && semverInRange(state.version, range);
    });
}

// The deployed releases (history order) outside the range.
export function heldReleases(resolution: RDbResolution, range: string): B64Hash[] {
    if (resolution.releases === undefined) return [];
    return resolution.history.filter((r) => {
        const state = resolution.releases!.get(r);
        return state !== undefined && !semverInRange(state.version, range);
    });
}

// Runs the policy once. Returns the number of gate entries appended.
export async function runAdoptionPolicy(host: AdoptionHost): Promise<number> {
    const resolution = await host.resolve();
    if (resolution.membership === undefined || resolution.releases === undefined) return 0;

    const ctx = host.getContext();
    const range = await host.getAdoptionRange();
    let appended = 0;

    for (const release of adoptedReleases(resolution, range)) {
        const state = resolution.releases.get(release)!;
        for (const [defHash, group] of state.groups) {
            const member = resolution.membership.byHash.get(defHash);
            if (member === undefined) continue;
            if (await ctx.getObject(group.schema) === undefined) continue;   // not synced yet: a later run admits it

            const target = new Set(group.version);
            const gate = await ensureDeployGate(ctx, member.id, group.schema, host.getBackendLabel());
            if (await gate.isAdmitted(target)) continue;
            appended += (await gate.admit(target)).length;
        }
    }
    return appended;
}

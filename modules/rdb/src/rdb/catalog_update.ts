// The catalog update planner: deploys a catalog release into an RDb.
//
// Shared by C-SQL (CREATE DATABASE, UPDATE CATALOG) and, later, rpack.
//
//   plan     resolve the RDb as if the release were deployed; list the member
//            groups to create (at their pins, plus their gates) and the
//            deploys to bring every member to its target version, bottom-up
//            over bindings.
//   dry-run  validate the RDb op and each deploy of an existing group at its
//            planned position; report which group rejects.
//   apply    create the missing groups and FILES objects (blob store and file
//            map, after their group), deploy bound groups first and advance
//            their dependents' refs to them, deploy the dependents, and append
//            the update-catalog last: it is the commit point, so a replica
//            never sees a deployed release whose deploys are missing.
//
// A failed ref advance is reported without aborting. A failed deploy aborts
// before the commit. The planner is idempotent: re-running it for a release
// that is already deployed only catches up the deploys that are behind.
//
// The author signs a deploy or a ref advance only when the gate reads $author.
// The other ops go out anonymous, since a group without an identity provider
// rejects an author it cannot verify.
//
// Local deploys never wait on the admin's own gate; the admin's replica adopts
// through the policy once the commit lands.

import { json } from "@hyper-hyper-space/hhs3_json";
import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import {
    Version, formatValidationFailure, refVersionAtOrAbove,
    ValidationRejectedError,
} from "@hyper-hyper-space/hhs3_mvt";
import type { Payload } from "@hyper-hyper-space/hhs3_mvt";

import type { RSchema } from "../rschema/interfaces.js";
import type { RTableGroupImpl } from "../rtable_group/group.js";
import { ensureDeployGate } from "../rdeploy_gate/rdeploy_gate.js";
import { versionKey } from "../rcatalog/resolve.js";
import type { MemberFiles, MemberGroup } from "./instantiate.js";
import type { ParamValue } from "./payload.js";
import type { RDbImpl } from "./rdb.js";

export type PlannedDeploy = {
    member: MemberGroup;
    target: Version;
};

export type CatalogPlan = {
    rdb: RDbImpl;
    release: B64Hash;
    params: { [name: string]: ParamValue };
    note?: string;
    commit: boolean;                  // false when the release is already deployed (catch-up only)
    order: MemberGroup[];             // every member, bottom-up over bindings
    creates: MemberGroup[];           // members absent on this replica
    fileCreates: { id: B64Hash; payload: MemberFiles['store'] | MemberFiles['map'] }[];   // absent FILES objects
    deploys: PlannedDeploy[];         // bottom-up
    problems: string[];               // why the plan cannot be applied (empty when it can)
};

export type CatalogUpdateResult = {
    created: B64Hash[];
    deployed: { groupId: B64Hash; name: string; entry: B64Hash }[];
    observed: { groupId: B64Hash; binding: string; entry: B64Hash }[];
    observeFailures: { groupId: B64Hash; binding: string; message: string }[];
    commit?: B64Hash;
};

export class CatalogUpdateError extends Error {
    constructor(message: string, readonly problems: string[]) {
        super(message);
        this.name = 'CatalogUpdateError';
    }
}

function failureMessage(err: unknown): string {
    if (err instanceof ValidationRejectedError) return formatValidationFailure(err.why);
    return err instanceof Error ? err.message : String(err);
}

async function currentVersion(group: RTableGroupImpl): Promise<Version> {
    return group.resolveSchemaVersion(await (await group.getScopedDag()).getFrontier());
}

export async function planCatalogUpdate(rdb: RDbImpl, spec: {
    release: B64Hash;
    params?: { [name: string]: ParamValue };
    note?: string;
    author?: OwnIdentity;
}): Promise<CatalogPlan> {
    const ctx = rdb.getContext();
    const params = spec.params ?? {};
    const history = await rdb.getDeployHistory();
    const commit = !history.includes(spec.release);

    const resolution = commit ? await rdb.resolvePlanned(spec.release, params) : await rdb.resolve();
    const plan: CatalogPlan = {
        rdb, release: spec.release, params, commit,
        order: [], creates: [], fileCreates: [], deploys: [], problems: [],
    };
    if (spec.note !== undefined) plan.note = spec.note;

    if (resolution.unresolved !== undefined) {
        const reason = resolution.unresolved;
        plan.problems.push(reason.kind === 'invalid-params' ? reason.message : `the database does not resolve: ${reason.kind}`);
        return plan;
    }
    const membership = resolution.membership!;

    if (commit) {
        try {
            const prepared = await rdb.prepareUpdateCatalog(spec.release, params, spec.author, spec.note);
            const result = await rdb.validatePayload(prepared.payload, prepared.at);
            if (!result.valid) plan.problems.push(`update-catalog: ${formatValidationFailure(result.why)}`);
        } catch (err) {
            plan.problems.push(`update-catalog: ${failureMessage(err)}`);
        }
    }

    for (const hash of membership.order) {
        const member = membership.byHash.get(hash)!;
        plan.order.push(member);

        const obj = await ctx.getObject(member.id);
        if (obj === undefined) {
            plan.creates.push(member);
            if (versionKey(member.target) !== versionKey(json.fromSet(member.payload.schemaVersion))) {
                plan.deploys.push({ member, target: member.target });
            }
            continue;
        }

        const group = obj as RTableGroupImpl;
        const current = await currentVersion(group);
        const schema = await group.getSchemaObject() as RSchema;
        const causal = await schema.getCausalDag();
        if (await refVersionAtOrAbove(causal, current, member.target)) continue;          // up to date (or ahead)
        if (!await refVersionAtOrAbove(causal, member.target, current)) {
            plan.problems.push(`group '${member.name}' has diverged from its target version`);
            continue;
        }

        plan.deploys.push({ member, target: member.target });
        try {
            const prepared = await group.prepareDeploy(member.target, group.deployNeedsAuthor() ? spec.author : undefined);
            const result = await group.validatePayload(prepared.payload, prepared.at);
            if (!result.valid) plan.problems.push(`deploy of '${member.name}': ${formatValidationFailure(result.why)}`);
        } catch (err) {
            plan.problems.push(`deploy of '${member.name}': ${failureMessage(err)}`);
        }
    }

    for (const files of membership.files.values()) {
        if (await ctx.getObject(files.storeId) === undefined) plan.fileCreates.push({ id: files.storeId, payload: files.store });
        if (await ctx.getObject(files.mapId) === undefined) plan.fileCreates.push({ id: files.mapId, payload: files.map });
    }

    return plan;
}

export async function applyCatalogPlan(plan: CatalogPlan, author?: OwnIdentity): Promise<CatalogUpdateResult> {
    if (plan.problems.length > 0) {
        throw new CatalogUpdateError(`the catalog update cannot be applied: ${plan.problems.join('; ')}`, plan.problems);
    }

    const rdb = plan.rdb;
    const ctx = rdb.getContext();
    const backendLabel = rdb.getBackendLabel();
    const result: CatalogUpdateResult = { created: [], deployed: [], observed: [], observeFailures: [] };

    for (const member of plan.creates) {
        await ctx.createObject(member.payload, backendLabel);
        result.created.push(member.id);
    }
    for (const member of plan.order) {
        await ensureDeployGate(ctx, member.id, member.def.schemaRef, backendLabel);
    }
    for (const create of plan.fileCreates) {
        await ctx.createObject(create.payload as unknown as Payload, backendLabel);
        result.created.push(create.id);
    }

    const deployedIds = new Set<B64Hash>();
    const targets = new Map(plan.deploys.map((d) => [d.member.id, d.target]));
    for (const member of plan.order) {
        const group = await ctx.getObject(member.id) as RTableGroupImpl;

        // advance this group's refs to the bound groups deployed in this plan
        for (const [binding, boundId] of Object.entries(group.getBindings())) {
            if (!deployedIds.has(boundId)) continue;
            try {
                const bound = await ctx.getObject(boundId) as RTableGroupImpl;
                const frontier = await (await bound.getScopedDag()).getFrontier();
                const entry = await group.observe(binding, frontier, group.observeNeedsAuthor(binding) ? author : undefined);
                result.observed.push({ groupId: member.id, binding, entry });
            } catch (err) {
                result.observeFailures.push({ groupId: member.id, binding, message: failureMessage(err) });
            }
        }

        const target = targets.get(member.id);
        if (target === undefined) continue;
        try {
            const entry = await group.deploy(target, group.deployNeedsAuthor() ? author : undefined);
            result.deployed.push({ groupId: member.id, name: member.name, entry });
            deployedIds.add(member.id);
        } catch (err) {
            throw new CatalogUpdateError(`deploy of '${member.name}' failed: ${failureMessage(err)}`, [failureMessage(err)]);
        }
    }

    if (plan.commit) {
        result.commit = await rdb.updateCatalog(plan.release, plan.params, author, plan.note);
    }

    await rdb.adopt();
    return result;
}

export async function deployCatalogRelease(rdb: RDbImpl, spec: {
    release: B64Hash;
    params?: { [name: string]: ParamValue };
    note?: string;
    author?: OwnIdentity;
}): Promise<CatalogUpdateResult> {
    const plan = await planCatalogUpdate(rdb, spec);
    return applyCatalogPlan(plan, spec.author);
}

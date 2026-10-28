import { B64Hash } from "@hyper-hyper-space/hhs3_crypto";
import { KeyId } from "@hyper-hyper-space/hhs3_crypto";
import { version, Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    VerdictEvaluation, TRUE, FALSE, verdictTruth, notTruth, allTruth, anyTruth, someTruth, seeThroughCover,
} from "@hyper-hyper-space/hhs3_mvt";
import type { Truth, Undecided, Verdict } from "@hyper-hyper-space/hhs3_mvt";
import type { Position } from "@hyper-hyper-space/hhs3_dag";

import {
    CreateRCapPayload,
    GrantPayload,
    RevokePayload,
    CapPayload,
} from "./payload.js";

import type { RCap, RCapView } from "./interfaces.js";

// Why a grant or revoke is void: its author held no managing cap at the op, or
// a dependency cycle left it undecided (denied).
type CapVoidReason = 'unauthorized' | 'undecided-cycle';

type DefinitionRead = { origins: Set<B64Hash>; managedBy: string[] };

function absent(): DefinitionRead {
    return { origins: new Set(), managedBy: [] };
}

export class RCapViewImpl implements RCapView {

    private target: RCap;
    private at: Version;
    private from: Version;
    // Caches rejected promises too, so a long-lived view would need to evict them.
    private definitions = new Map<string, Promise<DefinitionRead>>();

    constructor(target: RCap, at: Version, from: Version) {
        this.target = target;
        this.at = at;
        this.from = from;
    }

    getObject(): RCap { return this.target; }
    getVersion(): Version { return this.at; }
    getFromVersion(): Version { return this.from; }

    async getReferences(): Promise<B64Hash[]> { return []; }
    async resolveRefVersion(_refId: B64Hash): Promise<Version> {
        throw new Error("RCap does not have outgoing references");
    }

    // An add-identity is permanent: losing the enroll capability concurrently
    // does not unwind a key that was admitted, so this cover has no liveness
    // filter.
    async isIdentity(keyId: KeyId): Promise<boolean> {
        if (this.target.isCreator(keyId)) return true;

        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { ids: [keyId] } });
        return cover.size > 0;
    }

    // A capability definition is one slot. create-cap (and the genesis create)
    // turns it on, delete-cap turns it off. Off means nobody holds it. A
    // re-create turns the same slot back on; it does not start a fresh
    // generation, and the grants and revokes of the slot stay about that slot.
    // The slot is off when a live delete-cap is in the cover at `at`, or
    // concurrent with `at` and visible from `from`. Otherwise the origins are
    // the live creates in the cover (a grant does not have to name one of
    // them), and managedBy comes from the greatest of them.
    //
    // Every create-cap and delete-cap is live: both are creator-only
    // (validateCreateCap, validateDeleteCap), creators are fixed in the create
    // payload, and opVerdict returns live for them. So these reads need no
    // liveness filter, and a delete-cap's effect never changes between horizons.
    // Letting capability managers delete capabilities would need a liveness
    // filter here, and the bounded delta's visitor would then see delete-cap
    // verdicts flip.
    private definition(capName: string): Promise<DefinitionRead> {
        let pending = this.definitions.get(capName);
        if (pending === undefined) {
            pending = this.readDefinition(capName);
            this.definitions.set(capName, pending);
        }
        return pending;
    }

    private async readDefinition(capName: string): Promise<DefinitionRead> {
        const scopedDag = await this.target.getScopedDag();
        const cover = await scopedDag.findCoverWithFilter(this.at, { containsValues: { caps: [capName] } });
        const concurrent = await scopedDag.findConcurrentCoverWithFilter(
            this.from, this.at, { containsValues: { caps: [capName], barrier: ['t'] } },
        );
        if (concurrent.size > 0) return absent();

        const origins = new Set<B64Hash>();
        for (const hash of cover) {
            const entry = await scopedDag.loadEntry(hash);
            if (entry === undefined) continue;
            const p = entry.payload as CapPayload;
            if (p.action === 'delete-cap' && p.capName === capName) return absent();
            const positive =
                (p.action === 'create-cap' && p.capName === capName) ||
                (p.action === 'create' && capName in (p as CreateRCapPayload).initialCaps);
            if (positive) origins.add(hash);
        }
        if (origins.size === 0) return absent();

        let best: B64Hash | undefined;
        for (const hash of origins) if (best === undefined || hash > best) best = hash;
        const bestEntry = await scopedDag.loadEntry(best!);
        const bp = bestEntry?.payload as CapPayload | undefined;
        const managedBy = bp?.action === 'create'
            ? (bp as CreateRCapPayload).initialCaps[capName]?.managedBy ?? []
            : bp?.action === 'create-cap' ? bp.managedBy : [];
        return { origins, managedBy };
    }

    async capabilityExists(capName: string): Promise<boolean> {
        return (await this.definition(capName)).origins.size > 0;
    }

    // Admissibility check: "would an op appended at `this.at` that requires `grantee` to
    // hold `capName` be admissible when observed from `this.from`?" The answer is a pure
    // function of (this.at, this.from, grantee, capName). Each grant and revoke has a
    // verdict, keyed by the op and `from`: its author held a managing cap at the op's
    // own position. Ops whose verdicts depend on each other are solved together by a
    // VerdictEvaluation (hhs3_mvt), which gives the well-founded verdicts; a revoke
    // ring that nothing outside decides is void, so every holder on it keeps the cap.
    //
    // A multi-hash `at` is one position. An external op is concurrent with it when it
    // is outside the past of `at` and not after every head. Every grant and revoke
    // of one (capability, grantee) pair is on one plane, including grants written
    // before a delete-cap. The slot holds when the capability is on and both of
    // these are true:
    //   1. the see-through cover of this pair's grants and revokes at `at` holds,
    //      with a live revoke clearing the slot unless an undecided grant dominates it;
    //   2. no live revoke of the pair is concurrent with `at` and visible from `from`.
    async hasCapability(grantee: KeyId, capName: string): Promise<boolean> {
        const holding = await this.holdingIn(new VerdictEvaluation<CapVoidReason>('undecided-cycle'), grantee, capName);
        if (holding.status === 'undecided') {
            throw new Error(`whether '${grantee}' holds '${capName}' is undecided outside the component being solved`);
        }
        return holding.status === 'true';
    }

    // Whether `grantee` holds `capName` here, within `evaluation`. It is undecided
    // only while the walks meet an op of the component being solved. `exclude` is
    // the op whose authorization this holding decides: an op is not its own barrier.
    private async holdingIn(
        evaluation: VerdictEvaluation<CapVoidReason>, grantee: KeyId, capName: string, exclude?: B64Hash,
    ): Promise<Truth> {
        if (this.target.isCreator(grantee)) return TRUE;
        if ((await this.definition(capName)).origins.size === 0) return FALSE;

        const scopedDag = await this.target.getScopedDag();
        const grantKey = capName + ':' + grantee;
        const truthOf = async (hash: B64Hash): Promise<Truth> => {
            if (hash === exclude) return FALSE;
            return verdictTruth(await this.opVerdict(evaluation, hash));
        };
        const walk = (
            cover: (accept: (hash: B64Hash) => Promise<boolean>) => Promise<Position>,
        ) => seeThroughCover((p) => scopedDag.findMinimalCover(p), cover, truthOf);

        const kill2 = await walk((accept) => scopedDag.findConcurrentCoverWithFilter(
            this.from, this.at, { containsValues: { grants: [grantKey], barrier: ['t'] } },
            async (hash) => accept(hash),
        ));
        if (kill2.live.size > 0) return FALSE;

        const cover = await walk((accept) => scopedDag.findCoverWithFilter(
            this.at, { containsValues: { grants: [grantKey] } },
            async (hash) => accept(hash),
        ));
        return allTruth([notTruth(anyTruth(kill2.undecided.values())), await this.slotTruth(scopedDag, cover)]);
    }

    // The cover's contribution to the slot. A live revoke clears it, unless an
    // undecided grant dominates that revoke (a re-grant not yet decided). An
    // undecided revoke leaves the slot undecided while any grant could hold.
    private async slotTruth(
        scopedDag: { loadEntry(h: B64Hash): Promise<{ payload: unknown } | undefined>; findMinimalCover(p: Position): Promise<Position> },
        cover: { live: Position; undecided: Map<B64Hash, Undecided> },
    ): Promise<Truth> {
        const kindOf = async (hash: B64Hash): Promise<'grant' | 'revoke' | 'other'> => {
            const entry = await scopedDag.loadEntry(hash);
            const action = (entry?.payload as CapPayload | undefined)?.action;
            if (action === 'grant') return 'grant';
            if (action === 'revoke') return 'revoke';
            return 'other';
        };

        const liveRevokes: B64Hash[] = [];
        let liveGrant = false;
        for (const hash of cover.live) {
            const kind = await kindOf(hash);
            if (kind === 'revoke') liveRevokes.push(hash);
            if (kind === 'grant') liveGrant = true;
        }
        const undecidedGrants: Truth[] = [];
        const undecidedRevokes: Truth[] = [];
        for (const [hash, truth] of cover.undecided) {
            const kind = await kindOf(hash);
            if (kind === 'grant') undecidedGrants.push(truth);
            else if (kind === 'revoke') undecidedRevokes.push(truth);
        }

        for (const revoke of liveRevokes) {
            let hidden = false;
            for (const hash of cover.undecided.keys()) {
                if ((await kindOf(hash)) !== 'grant') continue;
                const above = await scopedDag.findMinimalCover(version(revoke, hash));
                if (above.size === 1 && above.has(hash)) { hidden = true; break; }
            }
            if (!hidden) return FALSE;
        }
        if (liveRevokes.length > 0) return anyTruth(cover.undecided.values());

        if (undecidedRevokes.length > 0) {
            return liveGrant || undecidedGrants.length > 0 ? anyTruth([...undecidedRevokes, ...undecidedGrants]) : FALSE;
        }
        if (liveGrant) return TRUE;
        if (undecidedGrants.length > 0) return anyTruth(undecidedGrants);
        return FALSE;
    }

    // The verdict of a grant or revoke observed from this view's `from`: live when its
    // author is a creator or held one of the cap's managing caps at the op's own
    // position. The manager list is the definition in the op's own past, read at
    // (op, op): a delete-cap on another branch has not happened yet, so it must
    // not empty the list. Whether the author holds that manager still uses the
    // outer `from`. Keyed by op and `from` only, so every view sharing the
    // evaluation shares it.
    private opVerdict(evaluation: VerdictEvaluation<CapVoidReason>, hash: B64Hash): Promise<Verdict<CapVoidReason>> {
        const key = hash + '|' + [...this.from].sort().join(',');
        return evaluation.resolve(key, async () => {
            const entry = await (await this.target.getScopedDag()).loadEntry(hash);
            if (entry === undefined) return { status: 'live' };
            const p = entry.payload as GrantPayload | RevokePayload;
            const author = p.author as KeyId;
            if (this.target.isCreator(author)) return { status: 'live' };

            const atOp = version(hash);
            const managers = (await new RCapViewImpl(this.target, atOp, atOp).getManagedBy(p.capName))
                .filter((mgr) => mgr !== 'creator');
            const opView = new RCapViewImpl(this.target, atOp, this.from);
            const authorized = await someTruth(managers, (mgr) => opView.holdingIn(evaluation, author, mgr, hash));
            if (authorized.status === 'true') return { status: 'live' };
            if (authorized.status === 'false') return { status: 'void', reason: 'unauthorized' };
            return authorized;
        });
    }

    // Whether the grant, revoke or delete-cap `hash` is void observed from this
    // view's `from`; `at` plays no part. The bounded delta compares it at its two
    // horizons. One evaluation per call, as in hasCapability.
    async isOpVoid(hash: B64Hash): Promise<boolean> {
        const verdict = await this.opVerdict(new VerdictEvaluation<CapVoidReason>('undecided-cycle'), hash);
        if (verdict.status === 'undecided') {
            throw new Error(`the verdict of '${hash}' is undecided outside the component being solved`);
        }
        return verdict.status === 'void';
    }

    async getManagedBy(capName: string): Promise<string[]> {
        return (await this.definition(capName)).managedBy;
    }

    async currentCapCreationVersion(capName: string): Promise<Version> {
        return new Set((await this.definition(capName)).origins);
    }

    // Every capability that exists here. The names come from the genesis and from
    // every create-cap in the DAG: a filtered cover would return only the topmost
    // capability entries and miss a create-cap below another. A create-cap outside
    // the past of `at` does not make its capability exist here, so the
    // capabilityExists filter below keeps the list exact.
    async getCapabilities(): Promise<string[]> {
        const caps: string[] = [];
        const all = new Set(Object.keys(this.target.getInitialCaps()));

        const scopedDag = await this.target.getScopedDag();
        for await (const entry of scopedDag.loadAllEntries()) {
            const p = entry.payload as CapPayload;
            if (p.action === 'create-cap') all.add(p.capName);
        }

        for (const name of all) {
            if (await this.capabilityExists(name)) {
                caps.push(name);
            }
        }

        return caps;
    }
}

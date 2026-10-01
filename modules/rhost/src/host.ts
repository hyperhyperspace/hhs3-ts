// An app on one device, and its hosts. Each host is one database with its own
// key, replica, mesh and projection; it is also the unit of start and stop.
// The hosts share only what the app ships: its releases, params, allow list
// and projection.

import type { B64Hash, OwnIdentity } from "@hyper-hyper-space/hhs3_crypto";
import type { IssueReporter } from "@hyper-hyper-space/hhs3_mesh";
import { deserializePublicKeyFromBase64, serializePublicKeyToBase64, type Version } from "@hyper-hyper-space/hhs3_mvt";
import {
    CatalogUpdateError, RDbImpl, catalogStatus, compareSemver, deployCatalogRelease,
    type CatalogParamDecl, type ParamValue, type RCatalogImpl,
} from "@hyper-hyper-space/hhs3_rdb";
import type { RdbProjection } from "@hyper-hyper-space/hhs3_rdb_projection";
import { RdbRuntime, type KeyRecord, type KeyVault } from "@hyper-hyper-space/hhs3_rdb_runtime";
import type { ClientParam, ClientRelease, HostStatus, RhostClient } from "@hyper-hyper-space/hhs3_rhost_client";
import { installRelease, releaseTag, type ReleaseFile } from "@hyper-hyper-space/hhs3_rpack";

import { inProcessClient } from "./client.js";
import {
    checkPassphraseSource, effectiveConfig, listenPort, parseSyncConfig, projectionIndexSpec,
    type AppConfig, type EffectiveConfig, type HostKeyConfig, type HostRecord, type ParamsConfig, type SyncConfig,
} from "./config.js";
import { ParamError, resolveParams } from "./params.js";
import type { HostPlatform } from "./platform.js";
import { openProjection, type OpenProjectionOptions } from "./projection.js";
import { adoptionRangeFor, majorOf, shippedReleases } from "./releases.js";
import { serveClient } from "./serve.js";
import {
    allowIsEveryone, columnLookup, createAllowAuthorizer, fetchDatabase, startDatabaseSync, validateAllowSources,
    type DatabaseSync, type SyncMeshBuildRequest,
} from "./sync.js";

export type { HostStatus };

export const DEFAULT_HOST = 'default';

export type ShippedInfo = { name: string; version: string; release: B64Hash; tag: string };

export type UpdateReport = {
    host: string;
    shipped: ShippedInfo;
    appliedEntries: number;
    skippedEntries: number;
    adoptionRange: string;
    deployed?: ShippedInfo;
    // Why the shipped release was not deployed, when it wasn't.
    notDeployed?: string;
    // The params it needs that params don't set, when that's why.
    missingParams?: ClientParam[];
};

export type DeployReport = { host: string; deployed: ShippedInfo };

export type StartResult = { host: string; error?: string };

export type RhostOptions = { report?: IssueReporter };

// What create and join set up for a new host. `key` is a label or `#<key id>`
// in the platform's keystore; `passphrase` is where its passphrase comes from
// ('prompt' or 'env:<VAR>'), absent for a key stored without one.
export type HostSetup = { key: string; passphrase?: string; sync: SyncConfig };

// `catalog` picks the shipped catalog when the app ships several; `name` is
// the database's name (the host's by default).
export type CreateOptions = HostSetup & { catalog?: string; name?: string };

// `release` picks a release of the host's catalog instead of the shipped one;
// `params` sets the params it newly declares, in the same forms as app.json.
export type DeployOptions = { release?: string; params?: ParamsConfig };

// The params a release declares that the database doesn't have yet and that
// nothing sets.
export type ParamNeeds = { version: string; missing: ClientParam[] };

// Highest version first: "2.0.0 + 1.5.1".
export function formatReleases(releases: ClientRelease[]): string {
    if (releases.length === 0) return '(none)';
    return sortReleases(releases).map((r) => r.version).join(' + ');
}

function sortReleases<R extends ClientRelease>(releases: R[]): R[] {
    return [...releases].sort((a, b) => {
        const byVersion = a.version === '?' || b.version === '?' ? 0 : compareSemver(b.version, a.version);
        return byVersion !== 0 ? byVersion : (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0);
    });
}

function shippedInfo(file: ReleaseFile): ShippedInfo {
    const { name, version, release } = file.manifest;
    return { name, version, release, tag: releaseTag(release) };
}

function errorMessage(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
}

function paramList(names: string[]): string {
    return names.map((name) => `:${name}`).join(', ');
}

// "1.1.0 needs a value for :moderator (identity), which params don't set"
function missingParamsReason(version: string, missing: ClientParam[]): string {
    const what = missing.map((p) => `:${p.name} (${p.type})`).join(', ');
    return `${version} needs ${missing.length === 1 ? 'a value' : 'values'} for ${what}, which params don't set`;
}

// The shipped release is above every deployed one, and not deployed itself.
function shippedIsAhead(file: ReleaseFile, deployed: ClientRelease[]): boolean {
    if (deployed.some((r) => r.hash === file.manifest.release)) return false;
    return deployed.every((r) => r.version === '?' || compareSemver(file.manifest.version, r.version) > 0);
}

export class Rhost {
    readonly config: AppConfig;
    readonly platform: HostPlatform;
    readonly keyVault: KeyVault;
    readonly report: IssueReporter | undefined;
    private readonly shipped: Map<string, ReleaseFile>;
    private readonly opened = new Map<string, Host>();
    private readonly opening = new Map<string, Promise<Host>>();
    private readonly identities = new Map<string, Promise<OwnIdentity>>();

    private constructor(config: AppConfig, platform: HostPlatform, keyVault: KeyVault,
        shipped: Map<string, ReleaseFile>, options: RhostOptions) {
        this.config = config;
        this.platform = platform;
        this.keyVault = keyVault;
        this.shipped = shipped;
        this.report = options.report;
    }

    static async open(config: AppConfig, platform: HostPlatform, options: RhostOptions = {}): Promise<Rhost> {
        const keyVault = await platform.keyVault();
        const shipped = shippedReleases(await platform.readReleases(config.releases));
        return new Rhost(config, platform, keyVault, shipped, options);
    }

    // The release file shipped for each catalog, by catalog name.
    shippedReleases(): Map<string, ReleaseFile> {
        return new Map(this.shipped);
    }

    shippedFor(catalog: string): ReleaseFile | undefined {
        return this.shipped.get(catalog);
    }

    async hosts(): Promise<string[]> {
        return this.platform.hosts.list();
    }

    // `default`, or the only host there is.
    async defaultHost(): Promise<string> {
        const names = await this.hosts();
        if (names.includes(DEFAULT_HOST)) return DEFAULT_HOST;
        if (names.length === 1) return names[0]!;
        if (names.length === 0) throw new Error('this app has no hosts; create or join one');
        throw new Error(`this app has several hosts (${names.join(', ')}); name one`);
    }

    async host(name: string): Promise<Host> {
        const open = this.opened.get(name);
        if (open !== undefined) return open;
        let pending = this.opening.get(name);
        if (pending === undefined) {
            pending = this.openHost(name);
            this.opening.set(name, pending);
        }
        try {
            const host = await pending;
            this.opened.set(name, host);
            return host;
        } finally {
            this.opening.delete(name);
        }
    }

    private async openHost(name: string): Promise<Host> {
        const record = await this.platform.hosts.read(name);
        if (record === undefined) throw new Error(`no host '${name}'`);
        const runtime = await RdbRuntime.open({ backend: await this.platform.hosts.openReplica(name), keyVault: this.keyVault });
        try {
            const db = await runtime.workspace.replica.getObject(record.database);
            if (db === undefined || !(db instanceof RDbImpl)) {
                throw new Error(`host '${name}' has no database #${record.database} in its replica`);
            }
            return new Host(this, name, record, runtime, db);
        } catch (err) {
            await runtime.close();
            throw err;
        }
    }

    // Unlocks a host's key once per app. The key is looked up by its id, so
    // relabeling it in the keystore doesn't change which key a host signs with.
    async identity(host: string, key: HostKeyConfig): Promise<OwnIdentity> {
        let identity = this.identities.get(key.keyId);
        if (identity === undefined) {
            identity = this.unlock(host, key);
            this.identities.set(key.keyId, identity);
            identity.catch(() => this.identities.delete(key.keyId));
        }
        return identity;
    }

    private async unlock(host: string, key: HostKeyConfig): Promise<OwnIdentity> {
        this.requireKey(host, key);
        const passphrase = await this.platform.passphrase(key);
        let identity: OwnIdentity;
        try {
            identity = await this.keyVault.unlock(`#${key.keyId}`, passphrase);
        } catch (err) {
            throw new Error(`host '${host}': ${errorMessage(err)}`);
        }
        if (identity.keyId !== key.keyId || serializePublicKeyToBase64(identity.publicKey) !== key.publicKey) {
            throw new Error(`host '${host}': the keystore's key #${key.keyId.slice(0, 8)} doesn't match the public key its host.json records`);
        }
        return identity;
    }

    private keystoreName(): string {
        return this.platform.keystoreLocation ?? 'the keystore';
    }

    private requireKey(host: string, key: HostKeyConfig): void {
        try {
            if (this.keyVault.resolveRecord(`#${key.keyId}`).keyId === key.keyId) return;
        } catch {
            // reported below
        }
        throw new Error(`host '${host}' signs with the key '${key.label}' (#${key.keyId.slice(0, 8)}), `
            + `which isn't in ${this.keystoreName()}`);
    }

    // A key of the keystore, given by label or `#<key id>`, as host.json
    // records it.
    private hostKey(host: string, given: string, passphrase: string | undefined): HostKeyConfig {
        if (passphrase !== undefined) checkPassphraseSource(passphrase, `host '${host}'`);
        let record: KeyRecord;
        try {
            record = this.keyVault.resolveRecord(given);
        } catch (err) {
            const problem = errorMessage(err).startsWith('Ambiguous') ? `'${given}' names several keys` : `no key '${given}'`;
            throw new Error(`host '${host}': ${problem} in ${this.keystoreName()}`);
        }
        const { publicKey } = this.keyVault.resolvePublic(`#${record.keyId}`);
        return {
            label: record.label,
            keyId: record.keyId,
            publicKey: serializePublicKeyToBase64(publicKey),
            ...(passphrase !== undefined ? { passphrase } : {}),
        };
    }

    // The network settings of a new host, checked as host.json would be. Two
    // hosts of an app can't listen on the same port.
    private async hostSync(host: string, sync: SyncConfig): Promise<SyncConfig> {
        const checked = parseSyncConfig(sync, `host '${host}'`);
        const port = listenPort(checked.listen);
        if (port === undefined) return checked;
        for (const other of await this.hosts()) {
            let record: HostRecord | undefined;
            try {
                record = await this.platform.hosts.read(other);
            } catch {
                continue;
            }
            if (record !== undefined && listenPort(record.sync.listen) === port) {
                throw new Error(`host '${host}': port ${port} (sync.listen) is already used by host '${other}'`);
            }
        }
        return checked;
    }

    private async requireNew(name: string): Promise<void> {
        if ((await this.hosts()).includes(name) || await this.platform.hosts.read(name) !== undefined) {
            throw new Error(`host '${name}' already exists`);
        }
    }

    private pickCatalog(catalog: string | undefined): ReleaseFile {
        if (catalog !== undefined) {
            const file = this.shipped.get(catalog);
            if (file === undefined) throw new Error(`this app ships no catalog '${catalog}'`);
            return file;
        }
        if (this.shipped.size === 0) throw new Error('this app ships no release files');
        if (this.shipped.size > 1) {
            throw new Error(`several catalogs ship (${[...this.shipped.keys()].sort().join(', ')}); choose one`);
        }
        return [...this.shipped.values()][0]!;
    }

    // Creates a database from the shipped release, with the host's key as its
    // creator, and deploys the release. The host is not started.
    async create(name: string | undefined = DEFAULT_HOST, options: CreateOptions): Promise<Host> {
        await this.requireNew(name);
        const file = this.pickCatalog(options.catalog);
        const key = this.hostKey(name, options.key, options.passphrase);
        const sync = await this.hostSync(name, options.sync);
        const me = await this.identity(name, key);
        const { catalog, release, version } = file.manifest;

        await this.platform.hosts.create(name, async (backend) => {
            const runtime = await RdbRuntime.open({ backend, keyVault: this.keyVault });
            try {
                const replica = runtime.workspace.replica;
                await installRelease(replica, file);
                const index = await ((await replica.getObject(catalog)) as unknown as RCatalogImpl).getIndex();
                const params = resolveParams(this.config.params ?? {}, index.releaseState(release).params, this.keyVault, me);
                const dbName = options.name ?? name;
                const payload = await RDbImpl.create({
                    seed: runtime.session.createSeed('rdb', dbName),
                    name: dbName,
                    catalog,
                    release,
                    ...(Object.keys(params).length > 0 ? { params } : {}),
                    creators: [{ keyId: me.keyId, publicKey: me.publicKey }],
                });
                const db = await replica.createObject(payload, runtime.workspace.backendLabel);
                runtime.workspace.roots.registerObject(db.getId(), db, dbName);
                await deployCatalogRelease(db as RDbImpl, { release, author: me });
                await (db as RDbImpl).setAdoptionRange(adoptionRangeFor(version));
                return { database: db.getId(), catalog: file.manifest.name, created: true, key, sync };
            } catch (err) {
                if (err instanceof CatalogUpdateError) throw new Error(`${err.message}: ${err.problems.join('; ')}`);
                throw err;
            } finally {
                await runtime.close();
            }
        });
        return this.host(name);
    }

    // Fetches an existing database's genesis from its peers, over the new
    // host's own network settings, and installs the shipped release of its
    // catalog. Sync brings the rest once the host starts.
    async join(databaseId: B64Hash, name: string | undefined = DEFAULT_HOST, options: HostSetup): Promise<Host> {
        await this.requireNew(name);
        const key = this.hostKey(name, options.key, options.passphrase);
        const sync = await this.hostSync(name, options.sync);
        const me = await this.identity(name, key);

        await this.platform.hosts.create(name, async (backend) => {
            const runtime = await RdbRuntime.open({ backend, keyVault: this.keyVault });
            try {
                const replica = runtime.workspace.replica;
                const db = await fetchDatabase({
                    replica,
                    id: databaseId,
                    meshFactory: this.platform.meshFactory,
                    request: meshRequest(sync, me, this.report),
                    meshLabel: `rhost-join-${name}`,
                    backendLabel: runtime.workspace.backendLabel,
                }) as RDbImpl;
                const catalogId = db.getCatalogRef();
                const file = [...this.shipped.values()].find((f) => f.manifest.catalog === catalogId);
                if (file === undefined) {
                    throw new Error(`database #${databaseId} uses catalog #${catalogId}, which this app doesn't ship`);
                }
                await installRelease(replica, file);
                return { database: databaseId, catalog: file.manifest.name, created: false, key, sync };
            } finally {
                await runtime.close();
            }
        });
        return this.host(name);
    }

    // Removes this device's copy of a host: stops it, closes its replica and
    // deletes it. The database lives on in every other replica.
    async remove(name: string): Promise<void> {
        const open = this.opened.get(name);
        if (open !== undefined) {
            await open.close();
            this.opened.delete(name);
        }
        await this.platform.hosts.remove(name);
    }

    private async names(name: string | undefined): Promise<string[]> {
        return name === undefined ? this.hosts() : [name];
    }

    async update(name?: string): Promise<UpdateReport[]> {
        const reports: UpdateReport[] = [];
        for (const n of await this.names(name)) reports.push(await (await this.host(n)).update());
        return reports;
    }

    async status(name?: string): Promise<HostStatus[]> {
        const statuses: HostStatus[] = [];
        for (const n of await this.names(name)) statuses.push(await (await this.host(n)).status());
        return statuses;
    }

    // A named host that fails to start throws; with no name, each failure is
    // recorded and the other hosts still start.
    async start(name?: string): Promise<StartResult[]> {
        if (name !== undefined) {
            await (await this.host(name)).start();
            return [{ host: name }];
        }
        const results: StartResult[] = [];
        for (const n of await this.hosts()) {
            try {
                await (await this.host(n)).start();
                results.push({ host: n });
            } catch (err) {
                results.push({ host: n, error: errorMessage(err) });
            }
        }
        return results;
    }

    async stop(name?: string): Promise<void> {
        const targets = name === undefined ? [...this.opened.values()] : [this.opened.get(name)].filter((h) => h !== undefined);
        for (const host of targets) await host.stop();
    }

    // Stops every running host and closes every replica.
    async close(): Promise<void> {
        for (const [name, host] of [...this.opened]) {
            await host.close();
            this.opened.delete(name);
        }
    }
}

function meshRequest(sync: SyncConfig, identity: OwnIdentity, report: IssueReporter | undefined): SyncMeshBuildRequest {
    return {
        scope: sync.scope,
        identity,
        ...(sync.tracker !== undefined ? { trackerAddress: sync.tracker } : {}),
        ...(sync.trackerKey !== undefined ? { trackerKeyId: sync.trackerKey } : {}),
        ...(sync.listen !== undefined ? { listenAddress: sync.listen } : {}),
        ...(report !== undefined ? { report } : {}),
    };
}

type Services = {
    sync?: DatabaseSync;
    projection?: RdbProjection;
    closeListener?: () => Promise<void>;
    releaseLock?: () => Promise<void>;
};

// What deploying a release takes in params: the ones it declares that the
// database doesn't have yet (`needed`), the values set for them (`given`), and
// the ones nothing sets (`missing`).
type ParamPlan = {
    version: string;
    needed: Map<string, CatalogParamDecl>;
    given: ParamsConfig;
    missing: ClientParam[];
};

type DeployHold = { reason: string; missingParams?: ClientParam[] };

export class Host {
    readonly app: Rhost;
    readonly name: string;
    readonly record: HostRecord;
    readonly runtime: RdbRuntime;
    readonly db: RDbImpl;
    readonly config: EffectiveConfig;
    private services: Services | undefined;
    private starting: Promise<void> | undefined;
    private lastErrorMessage: string | undefined;
    private readonly listeners = new Set<(status: HostStatus) => void>();
    private readonly onDbChange = (_version: Version): void => { this.notify(); };
    private closed = false;
    private inProcess: RhostClient | undefined;

    constructor(app: Rhost, name: string, record: HostRecord, runtime: RdbRuntime, db: RDbImpl) {
        this.app = app;
        this.name = name;
        this.record = record;
        this.runtime = runtime;
        this.db = db;
        this.config = effectiveConfig(app.config, record);
    }

    get running(): boolean {
        return this.services !== undefined;
    }

    get lastError(): string | undefined {
        return this.lastErrorMessage;
    }

    get sync(): DatabaseSync | undefined {
        return this.services?.sync;
    }

    get projection(): RdbProjection | undefined {
        return this.services?.projection;
    }

    // The client interface, in process.
    get client(): RhostClient {
        this.inProcess ??= inProcessClient(this);
        return this.inProcess;
    }

    // The shipped release of this host's catalog, if the app ships it.
    shipped(): ReleaseFile | undefined {
        const file = this.app.shippedFor(this.record.catalog);
        return file !== undefined && file.manifest.catalog === this.db.getCatalogRef() ? file : undefined;
    }

    private requireShipped(): ReleaseFile {
        const file = this.app.shippedFor(this.record.catalog);
        if (file === undefined) throw new Error(`this app ships no release of catalog '${this.record.catalog}'`);
        if (file.manifest.catalog !== this.db.getCatalogRef()) {
            throw new Error(`host '${this.name}' uses catalog #${this.db.getCatalogRef()}, `
                + `but this app ships '${this.record.catalog}' as #${file.manifest.catalog}`);
        }
        return file;
    }

    private identity(): Promise<OwnIdentity> {
        return this.app.identity(this.name, this.config.key);
    }

    // Installs the shipped release, sets the adoption range to everything below
    // its next major, and deploys it when it's due. When it isn't, the report
    // says why; a missing param holds the deploy rather than failing it.
    async update(): Promise<UpdateReport> {
        const file = this.requireShipped();
        const installed = await installRelease(this.runtime.workspace.replica, file);
        const range = adoptionRangeFor(file.manifest.version);
        await this.db.setAdoptionRange(range);
        await this.paramPlan(file.manifest.release);

        const report: UpdateReport = {
            host: this.name,
            shipped: shippedInfo(file),
            appliedEntries: installed.appliedEntries,
            skippedEntries: installed.skippedEntries,
            adoptionRange: range,
        };
        const hold = await this.whyNotDeploy(file, (await catalogStatus(this.db)).deployed);
        if (hold !== undefined) {
            report.notDeployed = hold.reason;
            if (hold.missingParams !== undefined) report.missingParams = hold.missingParams;
            return report;
        }
        const params = await this.paramsFor(file.manifest.release);
        try {
            await deployCatalogRelease(this.db, {
                release: file.manifest.release, author: await this.identity(),
                ...(Object.keys(params).length > 0 ? { params } : {}),
            });
        } catch (err) {
            if (err instanceof CatalogUpdateError) {
                throw new Error(`deploying ${file.manifest.version} failed: ${err.problems.join('; ') || err.message}`);
            }
            throw err;
        }
        report.deployed = shippedInfo(file);
        this.notify();
        return report;
    }

    private async whyNotDeploy(file: ReleaseFile, deployed: ClientRelease[]): Promise<DeployHold | undefined> {
        const { version, release } = file.manifest;
        if (deployed.some((r) => r.hash === release)) return { reason: `${version} is already deployed` };
        const above = deployed.filter((r) => r.version !== '?' && compareSemver(version, r.version) <= 0);
        if (above.length > 0) return { reason: `${version} is not above the deployed ${formatReleases(deployed)}` };

        const autoDeploy = this.config.autoDeploy;
        if (autoDeploy === 'none') return { reason: 'autoDeploy is none' };
        const highest = sortReleases(deployed).find((r) => r.version !== '?');
        if (autoDeploy === 'minor' && highest !== undefined && majorOf(highest.version) !== majorOf(version)) {
            return { reason: `${version} is a major bump from ${highest.version}, and autoDeploy is minor` };
        }

        const unauthorized = this.whyNotAuthorized();
        if (unauthorized !== undefined) return { reason: unauthorized };

        const { missing } = await this.paramPlan(release);
        if (missing.length > 0) return { reason: missingParamsReason(version, missing), missingParams: missing };
        return undefined;
    }

    private whyNotAuthorized(): string | undefined {
        const creators = this.db.getCreators();
        if (creators.length > 0 && !this.db.isCreator(this.config.key.keyId)) {
            return `the key '${this.config.key.label}' is not a creator of this database`;
        }
        return undefined;
    }

    // Every name in the effective params and in `extra` must be a param that
    // `release` declares; a name in `extra` must also be one the database
    // doesn't have yet, since a param is set once.
    private async paramPlan(release: B64Hash, extra: ParamsConfig = {}): Promise<ParamPlan> {
        const state = (await (await this.catalog()).getIndex()).releaseState(release);
        const all: ParamsConfig = { ...this.config.params, ...extra };
        const undeclared = Object.keys(all).filter((name) => !state.params.has(name)).sort();
        if (undeclared.length > 0) {
            throw new ParamError(`${state.version} declares no param ${paramList(undeclared)}`);
        }
        const have = new Set(Object.keys(await this.db.getParams()));
        const already = Object.keys(extra).filter((name) => have.has(name)).sort();
        if (already.length > 0) {
            throw new ParamError(`this database already has ${paramList(already)}; a param is set once, by the release that declares it`);
        }
        const needed = new Map([...state.params].filter(([name]) => !have.has(name)));
        const given: ParamsConfig = {};
        const missing: ClientParam[] = [];
        for (const [name, decl] of [...needed].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
            const value = all[name];
            if (value !== undefined) given[name] = value;
            else missing.push({ name, type: decl.type });
        }
        return { version: state.version, needed, given, missing };
    }

    // Values for the params `release` declares that the database doesn't have
    // yet. `$me` is the host's key.
    private async paramsFor(release: B64Hash, extra?: ParamsConfig): Promise<{ [name: string]: ParamValue }> {
        const plan = await this.paramPlan(release, extra);
        if (plan.missing.length > 0) throw new ParamError(missingParamsReason(plan.version, plan.missing));
        if (plan.needed.size === 0) return {};
        const me = { keyId: this.config.key.keyId, publicKey: deserializePublicKeyFromBase64(this.config.key.publicKey) };
        return resolveParams(plan.given, plan.needed, this.app.keyVault, me);
    }

    private async catalog(): Promise<RCatalogImpl> {
        const catalog = await this.runtime.workspace.replica.getObject(this.db.getCatalogRef());
        if (catalog === undefined) throw new Error(`the catalog #${this.db.getCatalogRef()} isn't in this replica yet`);
        return catalog as unknown as RCatalogImpl;
    }

    // A release of the host's catalog: `M.m.p`, `M.m.p-<tag prefix>` when two
    // share a version, or `#<hash prefix>`.
    async findRelease(selector: string): Promise<B64Hash> {
        const catalog = await this.catalog();
        const index = await catalog.getIndex();
        const frontier = await (await catalog.getScopedDag()).getFrontier();
        let candidates: B64Hash[];
        if (selector.startsWith('#')) {
            candidates = index.releasesAt(frontier).filter((h) => h.startsWith(selector.slice(1)));
        } else {
            const m = /^(\d+\.\d+\.\d+)(?:-([0-9a-f]+))?$/.exec(selector);
            if (m === null) throw new Error(`'${selector}' is not a version, version-tag or #hash`);
            candidates = index.findReleasesByVersion(m[1]!, frontier);
            if (m[2] !== undefined) candidates = candidates.filter((h) => releaseTag(h).startsWith(m[2]!));
        }
        if (candidates.length === 0) throw new Error(`no release of '${this.record.catalog}' matches '${selector}'`);
        if (candidates.length > 1) {
            const labels = candidates.map((h) => `${index.releaseState(h).version}-${releaseTag(h)}`);
            throw new Error(`several releases match; name one: ${labels.join(', ')}`);
        }
        return candidates[0]!;
    }

    // The release a deploy targets: the shipped one, installed first, or the
    // one `selector` names in the host's catalog.
    private async deployTarget(selector: string | undefined): Promise<B64Hash> {
        const shipped = selector === undefined ? this.requireShipped() : this.shipped();
        if (shipped !== undefined) {
            await installRelease(this.runtime.workspace.replica, shipped);
            await this.db.setAdoptionRange(adoptionRangeFor(shipped.manifest.version));
        }
        return selector === undefined ? shipped!.manifest.release : this.findRelease(selector);
    }

    // The params a deploy of `release` (the shipped one by default) would
    // still need, given `params` on top of the effective ones.
    async paramNeeds(options: DeployOptions = {}): Promise<ParamNeeds> {
        const plan = await this.paramPlan(await this.deployTarget(options.release), options.params);
        return { version: plan.version, missing: plan.missing };
    }

    // Deploys a release whatever autoDeploy says: the shipped one, or the one
    // `release` names in the host's catalog. The key must still be allowed to
    // deploy, and the release must not be at or below a deployed one. `params`
    // sets the params the release newly declares that params don't; the
    // database keeps them, so nothing else is written.
    async deploy(options: DeployOptions = {}): Promise<DeployReport> {
        const release = await this.deployTarget(options.release);
        const index = await (await this.catalog()).getIndex();
        const version = index.releaseState(release).version;
        const deployed = (await catalogStatus(this.db)).deployed;
        if (deployed.some((r) => r.hash === release)) throw new Error(`${version} is already deployed`);
        if (deployed.some((r) => index.isReleaseBelow(release, r.hash))) {
            throw new Error(`${version} is below the deployed ${formatReleases(deployed)}`);
        }
        const unauthorized = this.whyNotAuthorized();
        if (unauthorized !== undefined) throw new Error(unauthorized);
        const params = await this.paramsFor(release, options.params);
        try {
            await deployCatalogRelease(this.db, { release, author: await this.identity(), ...(Object.keys(params).length > 0 ? { params } : {}) });
        } catch (err) {
            if (err instanceof CatalogUpdateError) {
                throw new Error(`deploying ${version} failed: ${err.problems.join('; ') || err.message}`);
            }
            throw err;
        }
        this.notify();
        return { host: this.name, deployed: { name: this.record.catalog, version, release, tag: releaseTag(release) } };
    }

    async status(): Promise<HostStatus> {
        const file = this.shipped();
        if (file !== undefined && !this.running) {
            // Only the runtime config: setAdoptionRange would also admit.
            this.db.setRuntimeConfig({ adoptionRange: adoptionRangeFor(file.manifest.version) });
        }
        const cs = await catalogStatus(this.db);
        const hostBehind = file !== undefined && shippedIsAhead(file, cs.deployed);
        const status: HostStatus = {
            host: this.name,
            database: this.record.database,
            catalog: this.record.catalog,
            created: this.record.created,
            adoptionRange: cs.adoptionRange,
            deployed: sortReleases(cs.deployed),
            adopted: sortReleases(cs.adopted),
            held: sortReleases(cs.held),
            members: cs.members,
            upgradeRequired: file !== undefined
                && cs.held.some((r) => r.version !== '?' && majorOf(r.version) > majorOf(file.manifest.version)),
            hostBehind,
            running: this.running,
        };
        if (hostBehind) {
            const hold = await this.holdForStatus(file!, cs.deployed);
            if (hold !== undefined) {
                status.notDeployed = hold.reason;
                if (hold.missingParams !== undefined) status.missingParams = hold.missingParams;
            }
        }
        const dbName = this.db.getName();
        if (dbName !== undefined) status.name = dbName;
        if (file !== undefined) status.shipped = shippedInfo(file);
        if (cs.unresolved !== undefined) status.unresolved = cs.unresolved;
        if (this.services?.sync !== undefined) status.peers = this.services.sync.peerCount();
        const files = this.services?.projection?.filesStatus() ?? [];
        if (files.length > 0) status.files = files;
        if (this.lastErrorMessage !== undefined) status.lastError = this.lastErrorMessage;
        return status;
    }

    // Why the next update won't deploy the shipped release. A release that
    // isn't installed yet can't be asked about its params; the update that
    // installs it will say.
    private async holdForStatus(file: ReleaseFile, deployed: ClientRelease[]): Promise<DeployHold | undefined> {
        try {
            return await this.whyNotDeploy(file, deployed);
        } catch (err) {
            return err instanceof ParamError ? { reason: err.message } : undefined;
        }
    }

    // Calls `listener` with a fresh status whenever the database changes, and
    // when the host starts, stops or fails.
    onStatus(listener: (status: HostStatus) => void): () => void {
        const first = this.listeners.size === 0;
        this.listeners.add(listener);
        if (first) void this.db.subscribe(this.onDbChange);
        return () => {
            this.listeners.delete(listener);
            if (this.listeners.size === 0) this.db.unsubscribe(this.onDbChange);
        };
    }

    private notify(): void {
        if (this.listeners.size === 0) return;
        void this.status().then((status) => {
            for (const listener of this.listeners) {
                try { listener(status); } catch { /* a listener's failure is its own */ }
            }
        }, () => { /* a status that can't be computed is skipped */ });
    }

    private fail(err: unknown): void {
        this.lastErrorMessage = errorMessage(err);
        this.notify();
    }

    // The configured file mounts, each opened through the platform.
    private fileMounts(): { files?: NonNullable<OpenProjectionOptions['files']> } {
        const mounts = this.config.projection.files ?? [];
        if (mounts.length === 0) return {};
        return { files: { mounts, open: (mount) => this.app.platform.filesDirectory(this.name, mount) } };
    }

    // Projects the database into its target and stops again, so the target is
    // current without the host running. A running host already projects, so
    // this does nothing.
    async project(): Promise<void> {
        if (this.closed) throw new Error(`host '${this.name}' is closed`);
        if (this.running) return;
        let releaseLock: (() => Promise<void>) | undefined;
        try {
            if (this.app.platform.acquireLock !== undefined) {
                releaseLock = await this.app.platform.acquireLock(this.name);
            }
            const identity = await this.identity();
            const target = await this.app.platform.projectionTarget(this.name, this.config.projection);
            const indexSpec = projectionIndexSpec(this.config.projection);
            const { projection } = await openProjection({
                db: this.db,
                ctx: this.runtime.workspace.replica,
                target,
                writer: identity,
                registerWriterKey: true,
                ...(indexSpec !== undefined ? { indexSpec } : {}),
                ...this.fileMounts(),
            });
            await projection.stop();
        } finally {
            if (releaseLock !== undefined) {
                try { await releaseLock(); } catch { /* best-effort */ }
            }
        }
    }

    // Takes the host's lock, updates it, then starts its sync and its
    // projection. On failure, whatever started is stopped again.
    async start(): Promise<void> {
        if (this.closed) throw new Error(`host '${this.name}' is closed`);
        if (this.services !== undefined) return;
        if (this.starting !== undefined) return this.starting;
        this.starting = this.runStart();
        try {
            await this.starting;
        } finally {
            this.starting = undefined;
        }
    }

    private async runStart(): Promise<void> {
        const services: Services = {};
        try {
            if (this.app.platform.acquireLock !== undefined) {
                services.releaseLock = await this.app.platform.acquireLock(this.name);
            }
            const identity = await this.identity();
            await this.update();

            const lookup = columnLookup(this.runtime.session);
            if (!allowIsEveryone(this.config.allow)) await validateAllowSources(this.config.allow, lookup);
            const built = await this.app.platform.meshFactory(meshRequest(this.config.sync, identity, this.app.report));
            services.sync = await startDatabaseSync({
                replica: this.runtime.workspace.replica,
                db: this.db,
                built,
                meshLabel: `rhost-${this.name}`,
                authorizer: createAllowAuthorizer(this.config.allow, lookup),
                ...(this.app.report !== undefined ? { report: this.app.report } : {}),
            });

            const target = await this.app.platform.projectionTarget(this.name, this.config.projection);
            const indexSpec = projectionIndexSpec(this.config.projection);
            const { projection } = await openProjection({
                db: this.db,
                ctx: this.runtime.workspace.replica,
                target,
                writer: identity,
                registerWriterKey: true,
                ...(indexSpec !== undefined ? { indexSpec } : {}),
                ...this.fileMounts(),
                onError: (err) => { this.fail(`projection: ${errorMessage(err)}`); },
            });
            services.projection = projection;
            this.services = services;

            if (this.app.platform.listen !== undefined) {
                services.closeListener = await this.app.platform.listen(this.name, (connection) => serveClient(this, connection));
            }
            this.lastErrorMessage = undefined;
            this.notify();
        } catch (err) {
            this.services = undefined;
            await stopServices(services);
            this.fail(err);
            throw err;
        }
    }

    async stop(): Promise<void> {
        if (this.starting !== undefined) {
            try { await this.starting; } catch { /* a failed start left nothing running */ }
        }
        const services = this.services;
        if (services === undefined) return;
        this.services = undefined;
        await stopServices(services);
        this.notify();
    }

    // Stops the host and closes its replica.
    async close(): Promise<void> {
        if (this.closed) return;
        await this.stop();
        this.closed = true;
        if (this.listeners.size > 0) this.db.unsubscribe(this.onDbChange);
        this.listeners.clear();
        await this.runtime.close();
    }
}

async function stopServices(services: Services): Promise<void> {
    if (services.closeListener !== undefined) {
        try { await services.closeListener(); } catch { /* best-effort */ }
    }
    if (services.projection !== undefined) {
        try { await services.projection.stop(); } catch { /* best-effort */ }
    }
    if (services.sync !== undefined) {
        try { await services.sync.stop(); } catch { /* best-effort */ }
    }
    if (services.releaseLock !== undefined) {
        try { await services.releaseLock(); } catch { /* best-effort */ }
    }
}

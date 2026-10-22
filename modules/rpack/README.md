# rpack

An application built on [Rdb](../rdb) keeps a full copy of its database on every device. The shape of that database — which tables exist, what a row is allowed to do, which groups a database is made of — changes over the life of the application. The copies do not update together. A device can be offline, an admin can wait, and the software on a device can be a version behind the catalog.

Three decisions have to be made, by three different parties, before a device starts using a new shape:

- The **developer** designs the catalog and publishes versions of it.
- The **admin** of one database decides that this instance will take a published version.
- Each **device** admits a version only when the software it is running can still read the data.

`rpack` is the developer's tool. It turns the catalog the developer wants into a signed release file. `rhost` runs on every device, the admin's included. It installs the releases the application ships, deploys them where that device's key has the authority, syncs, and keeps a local SQL database current for the application to read and write. The mechanics of both tools are in [IMPLEMENTATION.md](./IMPLEMENTATION.md). The host's own surface is in [rhost](../rhost).

## Three decisions, made at different times

A single migration, applied because the developer published it, would change every instance at once. That includes instances whose admin has not accepted the new shape, and devices whose installed software has no code for the new columns and rules. Reading those columns with old code, or enforcing new rules the user interface was not built for, is how a replica corrupts its own view of data it is supposed to keep.

So the three decisions stay separate, and each one is a different verb in [Rdb](../rdb#databases-catalogs-and-releases).

**Release** is the developer's. A catalog is a signed history of versions. Each version records what changed from its parents: group definitions it adds, and the schema version it sets for groups that already exist. Every replica checks that signature from the file itself. The developer can be offline, and the peer that delivered the file is not the authority.

**Deploy** is the admin's. Deploying records, inside that one database, which release the instance is on and the parameter values that release needs. A deploy moves forward only. When two releases are both deployed and neither contains the other — a 2.0.0 and a 1.5.1 fix that branched before it — the instance runs both, written `2.0.0 + 1.5.1`, until some release above both is deployed. Each group then aims at the union of the schema versions those releases name.

**Adopt** is each device's, and it is automatic. A deploy that arrives by sync is applied to a group only after this replica has admitted that schema version. The application decides the range from the release it ships. A build whose newest release is major `M` admits every deployed release below major `M+1`, and holds anything newer. The bound is an upper bound because this build can still read every older release. A range that started at major `M` would hold a 1.x fix the code understands, and the device would refuse data it is able to accept. Updating the application is what raises the ceiling. Until then the data stays on a shape the installed code understands, and the application can ask the person to update.

The admin's device is also just a device. Creating a database makes that host's key a creator, which is who may deploy when a group states no further rule. A group's `ALLOW DEPLOY IF` can name other keys. A host that joined deploys when the database names no creators, or when a group's `ALLOW DEPLOY IF` grants its key. Every other replica receives the new entries by synchronization and admits them when they fall inside its range. For those replicas the release file is what sets the range, and what lets the first start work offline: the file carries the release and its whole past, and installing it checks every signature from the file itself.

## What a release promises

A fix on an older line of the catalog has to be publishable after a newer line exists. 1.5.0 ships, then 2.0.0 on top of it, and then 1.5.1 is parented on 1.5.0. A database that already deployed 2.0.0 can deploy 1.5.1 as well, and then runs `2.0.0 + 1.5.1`. The next release, 2.0.1, takes both as parents, and the instance is a single release again. Where both sides changed the same slot, the higher schema version wins, and equal versions fall to the larger entry hash. The author's numbering is the tie-break.

The rule that makes this work is small. Versions are strict semver, and a release's version is above every parent's. 1.5.1 qualifies because its parent is 1.5.0 and its version is above that parent.

Schemas carry their own versions for this, separate from the catalog's. A schema can be shared by many catalogs, so its history is not the catalog's history. Inside one catalog, a release that changes a schema gives it that release's version, unless the source states a version of its own. A stated version on a schema that actually changed must be above every version that schema already has. An unchanged schema may state only a version it already has.

Two releases can end up with the same version number, for example when two people release at once. The file name carries a tag, the first eight hex digits of the release hash: `editor-2.0.0-8b21d0aa.rpack`. Where a version alone is ambiguous, the tag is part of the name: `2.0.0-8b21`.

Admission is stored in the replica, and it stays. A REPL session that widens the range on the host's store admits that release for the application too.

## The developer writes the catalog as it should be

The alters in a release depend on which parents it has. A merge of 2.0.0 and 1.5.1 produces different alters from a release whose only parent is 2.0.0, even when both are trying to reach the same catalog. The author is in charge of the destination. `rpack` derives the alters.

Each version under way is a folder. `target-catalog.sql` is the catalog as that version should leave it: `CREATE SCHEMA` statements and one `CREATE CATALOG`, with no `ALTER`. `version.json` names the parents. `upgrade-manual.sql` holds the steps the diff has to be told explicitly, almost always a reset. From those, `rpack` writes one schema update per changed schema and one catalog release that pins the new schema versions.

```sql
CREATE SCHEMA hhs:user CREATORS ($dev) AS ( ... );
CREATE SCHEMA hhs:doc CREATORS ($dev) AS ( ... );

CREATE CATALOG editor CREATORS ($dev) PARAMS (:admin identity) AS (
  TABLEGROUP user USING SCHEMA hhs:user
    USING IDENTITIES identities
    WITH ROWS (
      identities (keyId = :admin, publicKey = publicKey(:admin), name = 'Admin'),
      caps (label = 'manager', grantee = :admin)
    ),
  TABLEGROUP doc USING SCHEMA hhs:doc
    BIND user => user
    USING IDENTITIES user.identities
);
```

A group may say `AT LATEST`. That means the schema version this release pins. A hash, or a set of versions, would fight that pin, so the source refuses them. A catalog `VERSION`, `NOTE`, or `BY` may be written when it agrees with the release: the folder's version, the note in `version.json`, and the signing key in `rpack.json`.

Two kinds of edit are refused, because guessing would either destroy data or pretend two groups were one.

**A column redefined in place** — its type, default, nullability, or constraints — and a table's identity-provider flag flipped in place. The migration that changes those is a drop followed by an add, and the drop clears the values. The author says so. `upgrade-manual.sql` drops the column, `target-catalog.sql` describes the column with its new type, and the diff sees it missing and adds it back. Drop plus add in one update is the reset.

**A change to an existing group's bindings, gates, identity provider, or genesis rows.** A group's identity is that definition, together with the database and the parameter values used when it was created. A different definition is a new group.

A catalog parameter can be added. Removing one, or changing its type, is refused: databases that already stored a value would have nowhere to put it. A new parameter has to be given a value at deploy time, or the deploy waits.

Some edits release with a warning. Dropping a table or column, or changing foreign keys, restrictions, or concurrent deletes, without raising the major, is the author telling old software that it can still read this. Restrictions are included because a tighter rule and a looser one look the same to the diff. A new parameter warns because some deploy will have to supply it. A lower release left outside this release's past warns because a database running that lower release will keep running it beside the new one until a later release includes it.

A `FILES` definition is immutable in the same spirit as a group: a release may add one, and it may not change or remove one. People have stored bytes under it. When a later release drops a table or column that a `FILES` reads, the source can no longer name it, so it is left out. The draft warns that the `FILES` becomes read-only — writes stop, the files remain — and later releases stay quiet about it. Publishing a new `FILES` is how a writable folder comes back. Once the tables exist again, listing the same definition makes it writable again. The name cannot be reused for a different definition.

## One repository, every version

Computing an upgrade path means seeing every release that exists. A catalog therefore has one repository, on a single branch, and the files in it are the whole input, so a release can be rebuilt from the folder alone. An application target — desktop, web, server — has its own repository, with ordinary branches, and keeps a copy of the release files it ships.

```
editor/
  rpack.json              name, signing key, which folder made which release
  releases/               the release files, and nothing else
    editor-1.5.0-3f9a1c2e.rpack
    editor-2.0.0-8b21d0aa.rpack
  work/
    2.0.0/
      version.json        parents, and an optional note
      target-catalog.sql
      upgrade-manual.sql  resets the diff must be told
      test-data.sql       rows to insert after deploying this release
      staging.json        how the staging app projects and syncs
      .released/          the inputs and update.sql as signed
      build/              generated; not committed
      stage/              generated; not committed
```

`rpack.json` names the catalog and the label of the signing key in the developer's keystore (`~/.rdb/keys.json`, shared with `rdb`). Its `released` map records which folder produced which file. A folder absent from the map is a version still under way.

A version starts with `rpack new`. The default parents are every release with a lower version that is not already in the past of another release: the frontier just below this number. `target-catalog.sql` starts as the highest parent's released source, with the other parents' changes written in, so a release made from it unchanged only settles the versions the parents disagree on. The version must be above every parent. Several versions can be under way at once, each in its own folder.

```
rpack new 1.5.1                  # parent 1.5.0, even if 2.0.0 already exists
cd work/1.5.1
rpack status                     # version, parents, and what it would sign
rpack build                      # write build/update.sql, or the refusals
rpack release                    # sign, write releases/ and .released/
```

`rpack build` writes the derived statements to `build/update.sql` and prints that it did. A refusal prints the reason and writes nothing. `rpack release` signs that draft, checks the file in a fresh replica, checks that the file reads back as `target-catalog.sql`, and then writes the release file, `.released/`, and the `rpack.json` entry. The working files stay as they are. The same inputs give the same bytes. On a terminal it shows the same summary as `status` and asks before signing.

`rpack set base` moves a folder onto different parents and writes the starting files again. Work already done on the current base stops it, unless `--force` discards that work. `rpack log` shows the history, the lower releases that no higher release includes yet, folders whose files have changed since they were signed, and the versions under way.

### Making a release again

A release that turns out wrong before anyone else has it can be signed again. `rpack release --force` in its folder replaces the file with one made from the working files. Dependents — releases built on it — are made again from their own folders, on the new parent. A dependent that does not build stops the whole re-release, and nothing is written. The writes finish by removing the old files last, so an interrupted run leaves extra files and never a missing release.

A file that was already shared stays a valid release. A replica that has it holds it next to the new file of the same version. Re-releasing is for releases nobody has yet. Re-releasing the first release starts a new catalog: replicas of the old one take none of the new releases.

### Seeing the upgrade against data

`rpack stage`, in a version folder, builds an ordinary one-host application under `stage/` and replays every release in that version's past. Each release is deployed, and then the rows in that release's `test-data.sql` are inserted, so each upgrade meets the rows of the releases before it, written in that release's shape. At the end the database is projected once. `hosts/default/db/data.sqlite` is current without a host running, and the command prints what the last deploy did to the rows.

A folder that still makes its signed release is replayed from that file, and no key is needed. Any other folder replays its parents and then signs the planned release in memory. Nothing is written to `releases/`. Before the first release the creators are stand-in keys, so no passphrase is needed either. `staging.json` carries the host's network settings and the application's `allow`, projection, deploy policy, and parameters. Schema repositories have no groups to deploy, and `stage` refuses there.

Sample rows use `$labels` as stand-in keys. `$admin` is the staging database's admin, and the staging keystore holds those stand-ins with an empty passphrase so the host can run unattended.

## On each device

One application can serve many databases. A forum application may have one database per forum, each with its own admin and its own members. On a device the application is a folder of release files and settings, and each database is a **host** with a folder of its own:

```
my-app/
  app.json                 the same on every device
  catalogs/                the release files this build ships
  hosts/
    default/
      host.json            the database, the key, the network
      rdb/replica.rdb      this host's replica
      db/data.sqlite       the projection the application uses
      run/                 lock, socket, log of the running process
```

Hosts share the shipped releases and the application settings, and nothing at runtime. Each has its own replica, so names stay unique inside a host and removing a host is deleting its folder. Each signs with its own key and syncs on its own connections. A peer met in one host learns nothing about the others through them. With the command line, each host is its own process.

`app.json` is what this build of the software is. `releases` points at the folder of release files, copied from the catalog repository or pulled from the registry. For each catalog the newest file ships, and two files of the same version are an error. That file's major is the newest major this code understands, so each host admits every deployed release below the next major and holds anything newer.

`params` supplies values for creating a database and for parameters later releases declare. `$me` is the host's key, `$<label>` another key in the keystore. `autoDeploy` says when a host that is allowed to deploy should take the shipped release: `minor` (the default) takes patches and minor bumps, `major` takes anything, `none` waits for an explicit deploy. Only a release above every deployed one is deployed automatically. A release that declares a parameter `params` does not set is held too; status names the parameter, and supplying it is a deploy. The value is kept by the database. It is set once.

`allow` is which peers may replicate. Each entry is `everyone` or `group.table.column`, optionally with a condition, and a peer is accepted when its key appears there. With no `allow`, every peer is accepted. This is part of the application, and a host does not replace it.

`projection` is the local SQL file and, when set, the indexes and file mounts this platform uses. `keystore`, when set, is a keystore of the application's own; otherwise keys stay in the user's keystore, and the application folder holds none.

`host.json` is written when the host is created or joined. It records the database, the key this host signs with, and how it meets peers. One host, one writer: the application's writes, any deploys, and the sync handshake. A host may replace `params`, `autoDeploy`, `projection`, or the network settings. `releases`, `keystore`, and `allow` stay the application's. Both files refuse a field they do not know.

Creating a database deploys the shipped release with this host's key as creator. Joining fetches an existing database. Neither starts the host: a misconfigured device should not create a database by being launched. On start, a host installs the shipped release, admits everything below the next major, deploys when `autoDeploy` and the key's authority and the parameters all agree, then syncs and opens the projection.

Status is how a person tells the two directions of the gate apart.

- **Upgrade required.** A newer major is deployed in this database and this build is holding it. The software needs to be updated before this device can read that shape.
- **Host behind.** This build ships a release the host has not deployed. The reason is one of: `autoDeploy` is `none`, the release is a major and `autoDeploy` is `minor`, this key may not deploy, or a new parameter has no value.

The application itself uses the projection, `db/data.sqlite` unless configured otherwise, with ordinary SQL. Tables are named `<group>_<table>`. Writes are optimistic. The host ingests them, signs them, and replicates them. A write can still fail afterwards: rejected at ingestion when it breaks a rule, or voided later when a concurrent change wins, such as a revoked permission or a deleted parent. The row returns to its replicated state and an event is logged. `upgradeRequired` on the host's status is how the application asks the person to update.

`rhost_client` is the interface for what SQL does not cover: this host's key, registering someone else's public key, the event log, and status. The socket a running host listens on is read-only. Anything that signs, a deploy included, happens in a process that unlocks the key itself. A browser has no folder and no socket; the same `app.json` and the release bytes are handed to the browser platform, which keeps each host in IndexedDB. The tab that holds the lock runs the host.

## Schemas that more than one application can use

Comments, reactions, follows, and users get rewritten in every application if each catalog owns their history. In Rdb a schema is already its own object, pinned by the groups that use it. What a catalog release adds is a signature: the schema entries a catalog publishes are signed by that catalog's key. A shared schema cannot be signed in advance by every application that will use it, and an application has to be able to build without asking the network what the schema is today.

A schema therefore has its own `rpack` repository (`rpack init --schema`). Its `target-schema.sql` is one `CREATE SCHEMA`, with no `CREATORS`: the file is an author-agnostic template, and the catalog that imports it supplies the creator key. Releases are a content-addressed chain of steps, hashed and checked, and unsigned. The same commands build them: `new`, `status`, `build`, `release`, `log`, `verify`. `stage` belongs to catalogs, which have groups to deploy.

A catalog takes a published schema with `rpack import`. That copies the `.rpack` file into `imports/`, locks the step hash in `rpack.json`, and writes an `IMPORT SCHEMA` line into the version's `target-catalog.sql`. `build` and `release` read that vendored copy alone. The lock is what makes a later build reproduce the same schema when the registry has moved on.

```sql
IMPORT SCHEMA hhs:comments VERSION '1.1.0';
```

The first time a catalog imports a schema, even at 1.1.0, `rpack` replays the pack from its first step and keeps the pack's parent links. A later pack release that branched at 1.0.0 needs that parent to exist in the catalog's schema, or it has nothing to attach to. A group already deployed at an earlier pin keeps that pin's definition as it advances.

Once a schema is imported in a catalog, it stays imported. Once it is written inline with `CREATE SCHEMA`, it stays inline. The `imports` lock is the record of which is which. Two histories can pass through the same tables and then diverge; comparing the tables cannot tell them apart, and the parent links can.

`rpack publish` copies a release file into a directory registry. A directory can be copied and read offline, which is the whole distribution mechanism these tools use. The directory is `--registry`, then `$HHS_REGISTRY`, then `registry` in `rpack.json`, then `~/.hhs/registry`. A name with colons becomes directories: `hhs:comments` lives at `hhs/comments/`. Publishing refuses to put a schema and a catalog under the same name, or two catalogs with different ids.

An application build picks a catalog up with `rhost init --catalog`, or later with `rhost update --pull`. A pull takes a release that descends from the one already installed. Without `--major` it stays within the major this build already ships, because copying a newer major into `catalogs/` would be claiming the software understands it.

## The same schema, on different tables

A reusable schema cannot know the application's table names. `hhs:comments` refers to `target.subject` and to `auth.caps`. In this application the subject is `blog.posts`, and a second comment group on the same blog should use `blog.photos`. The catalog binds the schema's aliases when it creates the group, and may rename tables as it does:

```sql
TABLEGROUP post_comments USING SCHEMA hhs:comments
  BIND target => blog (subject => posts),
       auth   => users
  USING IDENTITIES auth.identities
  WITH PARAMS (:write_cap = 'commenter', :mod_cap = 'manager', :max_len = 1000)
```

The rename applies to references that were written in the schema: foreign keys, and `EXISTS` in the schema's `ALLOW` rules. Clauses the catalog itself wrote — `USING IDENTITIES`, `ALLOW DEPLOY IF`, `ALLOW UPDATE REF` — use the real table names, because the catalog author knows them. A table left out of the parentheses keeps its name. A schema update that starts mentioning `alias.table` with no mapping binds by that name, and the draft warns.

Several aliases may bind to the same group. A follows schema has `source` and `target`; both might be the blog. They share one observation of that group. The observed group has one version, and a snapshot of its tables is that one version. Every gate declared on those aliases has to hold. An alias with no gate adds no condition.

The map is fixed when the group is created. It may name a table the schema does not use yet, so a catalog can prepare for a schema upgrade, as long as the table exists in the bound group. An entry that renames a table to itself is omitted: leaving it out already means the name is unchanged.

## Values that differ per group

Two comment groups in one database can share `hhs:comments` and still disagree about the capability label that allows a write, or about the maximum length of a body. Those values have to live on the group. The group checks its rules on its own, without reading the catalog or the database, including on a replica that has not deployed the catalog entry that set the value.

A schema declares them:

```sql
CREATE SCHEMA hhs:comments VERSION '1.0.0'
  PARAMS (:write_cap string, :mod_cap string, :max_len integer DEFAULT 1000)
AS (
  TABLE comments (
    item string READONLY REFERENCES target.subject,
    body string
  )
    ALLOW insert IF length(body) <= :max_len
                AND EXISTS auth.caps WHERE label = :write_cap AND grantee = $author
);
```

A group supplies literals with `WITH PARAMS`. A parameter with no default has to be supplied. A parameter added to a schema that already exists must have a default, because groups created earlier can deploy the new version without mentioning it. An existing parameter stays: it cannot be removed or retyped. Parameters are legal in a table's row rules. Deploy gates, observation gates, file-write gates, and queries do not read them.

The database's own parameters, such as `:admin identity` on `CREATE CATALOG`, are chosen when that database is created — `$me` on the creating host is the usual admin — and they fill genesis rows through `WITH ROWS`. A schema parameter takes a literal. When several groups should share a literal, the script names it once:

```sql
CONSTANT :default_max_len = 1000;
```

A constant is a literal, visible from the line it is declared on, in `WITH PARAMS`, `WITH ROWS`, and literals inside `ALLOW` rules. Its name is its own: not a catalog parameter, and not a parameter of any schema in the script.

A group's values at creation are part of its identity, the same way its genesis rows are. Later releases revise a value by deploying a newer schema version that carries the new literals. The version has to change the schema — a rule, a column, or a new parameter — so there is a real step to deploy onto. A deploy is a barrier: rows concurrent with it are re-checked, and a revised limit or capability label takes part in that re-check. Writing the value without moving the version would revise those rows with no schema step to point at.

A value stays set once written. Deleting `:max_len = 500` from `WITH PARAMS` is refused. Returning to the schema's default means writing the default's value.

When two releases revise the same parameter concurrently, a causally later write wins. Among writes that are concurrent, the one aimed at the higher schema version wins, then the one with the larger schema-entry hash, then the larger hash of the write itself. The fold is per parameter: a later deploy that does not mention `:max_len` leaves the earlier write in place. Status reports parameters whose resolved value differs from the value the deployed releases call for.

`CREATE SCHEMA` may omit `CREATORS`. The signer of the statement is then the creator, which is the catalog's key when `rpack` is the one signing. A schema pack omits the clause because it has no signer yet.

## A local change on top of someone else's schema

An application often wants one extra column, or a tighter rule, on a schema it shares with other applications. The next upstream release still has to apply, and the upstream steps still have to be this schema's history, so a later branch has a parent to attach to. The local difference is written after the import, and `rpack` records it as its own step.

`target-catalog.sql` says what that difference is with an `ALTER SCHEMA` after the import. The statement carries no version, no parameters, no note, and no author. `rpack` chooses those, and parents the step on the imported ones.

```sql
IMPORT SCHEMA hhs:comments VERSION '1.1.0';

ALTER SCHEMA hhs:comments AS (
  ADD COLUMN comments.flagged boolean DEFAULT false,
  SET ALLOW RULES ON comments (
    ALLOW insert IF length(body) <= :max_len
                AND EXISTS auth.caps WHERE label = :write_cap AND grantee = $author
  )
);
```

`SET ALLOW RULES` and `SET FOREIGN KEYS` replace that table's whole slot, so the local text includes every rule and key that should remain.

The local step is numbered from the catalog version, with 1000 added to the major: catalog `1.1.0` writes schema version `1001.1.0`. Upstream versions stay below 1000. The two are siblings — the patch hangs off the previous upstream step, and so does the new upstream step — and when both write the same slot, the higher version wins. The local decision therefore keeps the slot as upstream moves. The draft warns when a newly imported step changes a slot the local `ALTER` still overrides, so the author can see the upstream edit they are hiding. Deleting the local rule makes the desired state upstream's again, and the draft writes a new patch that records that, because the old patch would otherwise keep winning.

The `ALTER` is applied to the imported tables on every release, including a release whose `upgrade-manual.sql` is empty. That is what keeps the local difference in the source. A column that exists upstream and needs a new type is dropped and re-added inside that `ALTER`, so the next release still reproduces it. The release that first drops the column people have already stored puts the `DROP COLUMN` in `upgrade-manual.sql` as well. A column that exists only because an older patch added it changes its `ADD COLUMN` in the `ALTER`, and the drop of the stored column is only in `upgrade-manual.sql`.

A parameter declared from a local patch would be numbered above 1000, and would then outrank any later upstream parameter of the same name. A parameter stays once declared, so the local `ALTER` declares none. The rules use literals, or a `CONSTANT`. This file belongs to one catalog, so a shared parameter name is unnecessary.

While upstream has not merged two branches, the import names both heads:

```sql
IMPORT SCHEMA hhs:comments VERSION '1.0.1', '1.1.0';
```

The two files have to share one first step, agree on the hash of every version they both contain, and neither version may already contain the other — if it does, name only the later one. The catalog's schema then holds both heads, and the usual schema resolution merges them. When upstream publishes the merge, importing that single version replaces the pair.

`rpack new` on several parents rewrites each `IMPORT SCHEMA` to the maximal upstream versions across those parents, and keeps or rebuilds the local `ALTER` so the new file still describes the merged schema.

## When a rule mentions another group's table

A reference that fails only when a device tries to use it would put a broken catalog in front of every replica. So a release that adds a group, or moves a group's schema version, is checked against the bound groups as those groups stand in the same release. Foreign keys and `EXISTS` atoms have to land on tables that exist. Columns searched by `EXISTS` have to be `PUB` and of a compatible type. `USING IDENTITIES` and `ALLOW DEPLOY IF` have to name real tables. `ALLOW UPDATE REF` is checked in the observed group's frame, in that group's real table names.

At runtime an `EXISTS` whose column is missing, or is not `PUB`, is false. A concurrent deploy can drop the column after the rule was written. False keeps the check monotonic: the rule fails closed, and validation does not abort on a schema this replica has only partly combined.

## Who may advance another group's version

`ALLOW UPDATE REF` gates who may move the observed version of a bound group. Today the gate is read in the observed group's frame. That works when the observed group itself can see the capability table, for example a blog that binds `users`, so a comment group can write its gate in the blog's names.

Two layouts have no such path. A data group may bind nothing, while the observer binds both that group and a separate users group and wants the users group's capabilities to decide who may advance it. A group created by another application knows nothing about this application's users, and the observer still needs to gate the advance with its own capabilities. Both are cases for reading the gate in the observer's frame, with the observer's aliases and the observer's table renames.

That is a follow-up, specified in [IMPLEMENTATION.md](./IMPLEMENTATION.md#advancing-an-observed-version). The gate text is fixed when the group is created. The same text names different tables in the two frames, so groups created under today's rule keep today's meaning when the follow-up lands.

## The path of one release

The developer writes `target-catalog.sql`, runs `rpack release`, and `rpack publish` copies the file into the registry. An application build copies it into `catalogs/`, or `rhost update --pull` fetches a descendant of the release it already has. On the admin's device, `rhost` deploys it into the databases that key may change, supplying any new parameter. Every other device receives the new entries by sync and admits them when they fall below the major its build understands. Updating the application raises that major. Until then the newer release is held, the data stays on the shape the software knows, and `upgradeRequired` is how the application asks the person to update.

[IMPLEMENTATION.md](./IMPLEMENTATION.md) is the rest: the repository fields, the diff, the release-file format, the import and patch procedure, the parameter fold, and the module layout.

import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { createAllowAuthorizer, formatAllowSource, parseAllowSource, type AllowSource, type ListedAt } from "../src/index.js";

export async function runSyncAuthorizerTests(): Promise<void> {
    const alice = 'alice-key';
    const bob = 'bob-key';
    const eve = 'eve-key';
    const sources: AllowSource[] = [
        { type: 'column', group: 'users', table: 'caps', column: 'grantee' },
        { type: 'column', group: 'users', table: 'identities', column: 'keyId' },
    ];
    const auth = createAllowAuthorizer(sources, async (source) => {
        if (source.column === 'grantee') return [alice];
        return [bob];
    });
    assertTrue(auth !== undefined, 'column union has an authorizer');
    assertTrue(await auth!.authorize(alice), 'first column match');
    assertTrue(await auth!.authorize(bob), 'second column match');
    assertTrue(!(await auth!.authorize(eve)), 'neither column match');

    assertTrue(
        createAllowAuthorizer([{ type: 'everyone' }], async () => []) === undefined,
        'everyone short-circuits to no authorizer',
    );
    assertTrue(
        createAllowAuthorizer(
            [{ type: 'everyone' }, { type: 'column', group: 'u', table: 't', column: 'c' }],
            async () => [eve],
        ) === undefined,
        'union containing everyone is open',
    );
}

// A bootstrapping host admits any peer and serves only listed ones. It stops
// bootstrapping the first time its own key is listed, and never starts again.
export async function runBootstrapAuthorizerTests(): Promise<void> {
    const me = 'my-key';
    const alice = 'alice-key';
    const eve = 'eve-key';
    const sources: AllowSource[] = [{ type: 'column', group: 'users', table: 'caps', column: 'grantee' }];

    let listed = [alice];
    let frontier: string[] | undefined = ['f1'];
    let lookups = 0;
    const found: ListedAt[] = [];
    const auth = createAllowAuthorizer(sources, async () => { lookups++; return listed; }, {
        frontier: async () => frontier,
        bootstrap: { self: me, found: async (at) => { found.push(at); } },
    })!;

    assertTrue(await auth.authorize(eve), 'bootstrapping: any key is admitted');
    assertTrue(await auth.authorize(alice), 'a listed key too');
    assertTrue(!(await auth.serves!(eve)), 'bootstrapping: a key that is not listed is not served');
    assertTrue(await auth.serves!(alice), 'a listed key is served');
    assertEquals(found.length, 0, 'the key has not been found');
    assertEquals(lookups, 1, 'the list is read once while its group does not change');

    // The key arrives. The frontier is not readable yet: bootstrapping goes on.
    listed = [alice, me];
    frontier = [];
    assertTrue(await auth.authorize(eve), 'with no position to record, the host keeps bootstrapping');
    assertEquals(found.length, 0, 'and reports nothing');

    // A call to authorize alone ends it, with no serve check in between.
    frontier = ['f2'];
    assertTrue(!(await auth.authorize(eve)), 'the first check after the key arrives refuses a key that is not listed');
    assertEquals(found.length, 1, 'found is called');
    assertEquals(found[0]?.group, 'users', 'with the group that listed the key');
    assertEquals(found[0]?.frontier.join(','), 'f2', "and that group's frontier");
    assertTrue(await auth.authorize(alice), 'listed keys are admitted');
    assertTrue(await auth.serves!(me), 'and the host is on its own list');

    // The key leaves the list: the host does not bootstrap again.
    listed = [alice];
    frontier = ['f3'];
    assertTrue(!(await auth.authorize(eve)), 'off its own list, the host still refuses a key that is not listed');
    assertTrue(await auth.authorize(alice), 'and still admits a listed one');
    listed = [alice, me];
    frontier = ['f4'];
    assertTrue(await auth.serves!(me), 'back on the list');
    assertEquals(found.length, 1, 'found is called once');

    // Without a bootstrap, an authorizer never admits a key that is not listed.
    const established = createAllowAuthorizer(sources, async () => [alice], { frontier: async () => ['f1'] })!;
    assertTrue(!(await established.authorize(eve)), 'an established host goes by its list, though its own key is not on it');
    assertTrue(!(await established.serves!(eve)), 'and serves by it');
}

export function runAllowSourceParseTests(): void {
    assertEquals(parseAllowSource('everyone').type, 'everyone', 'everyone');
    const column = parseAllowSource(' user.identities.keyId ');
    assertTrue(column.type === 'column' && column.group === 'user' && column.table === 'identities'
        && column.column === 'keyId' && column.where === undefined, 'group.table.column');
    const where = parseAllowSource("user.caps.grantee where label = 'writer'");
    assertEquals(formatAllowSource(where), "user.caps.grantee where label = 'writer'", 'where round-trips');
    for (const bad of ['user.identities', 'user..keyId', '1user.t.c', 'everyone where x']) {
        let threw = false;
        try { parseAllowSource(bad); } catch (e) {
            threw = true;
            assertTrue(String(e).includes(bad), `the error names '${bad}'`);
        }
        assertTrue(threw, `'${bad}' is rejected`);
    }
}

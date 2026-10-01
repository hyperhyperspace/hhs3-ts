import { assertEquals, assertTrue } from "@hyper-hyper-space/hhs3_util/dist/test.js";

import { createAllowAuthorizer, formatAllowSource, parseAllowSource, type AllowSource } from "../src/index.js";

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

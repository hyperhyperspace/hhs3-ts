

async function run(name:string, test: () => Promise<void>, options?: {silent?: boolean}) {
    console.log("\nRunning \"" + name + "\"");
    try {
        await test();
        if (!options?.silent) {
            console.log("Success");
        }
        return true;
    } catch (e: any) {
        console.log("Failure");
        if (e.msg !== undefined) {
            console.log(e.msg)
        } else {
            console.log(e);
        }
        return false;
    }
}

async function skip(name: string) {
    console.log("\nSkipping \"" + name + "\"");
}

function assertTrue(exp: boolean, msg?: string) {

    const err = new Error();

    if (!exp) throw new Error(msg || 'Failed assertion at\n' + err.stack)
}

function assertFalse(exp: boolean, msg?: string) {
    const err = new Error();

    if (exp) throw new Error(msg || 'Failed assertion at\n' + err.stack)
}

function assertEquals(received: any, expected: any, msg?: string) {
    const err = new Error();

    if (received !== expected) throw new Error((msg? msg + ' ': '') + 'Received ' + received + ' (expected ' + expected + ')\nFailed assertion at\n' + err.stack)
}

export { run, skip, assertTrue, assertFalse, assertEquals };
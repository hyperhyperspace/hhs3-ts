// Hooks around a target, for the suites. A hooked target passes every method
// through to the target it wraps, so a method a later change adds needs no
// wrapper of its own, and runs a hook first for the methods named.
//
// A hook can make a local write in the middle of a cycle (`beforeApply`,
// `beforeSettle`), or throw to stand for a crash at that point
// (`crashBefore`). The same works for any object the orchestrator calls, such
// as a group, so a hook can also make one append throw.

export type Hook = (...args: unknown[]) => void | Promise<void>;

export type MethodHooks = { [method: string]: Hook | undefined };

// Wraps `inner` so that each method named in `hooks` runs its hook first. A
// hook that throws makes the call throw without reaching `inner`.
export function hooked<T extends object>(inner: T, hooks: MethodHooks): T {
    return new Proxy(inner, {
        get(obj, prop, receiver) {
            const value = Reflect.get(obj, prop, receiver);
            if (typeof value !== 'function') return value;
            const hook = typeof prop === 'string' ? hooks[prop] : undefined;
            if (hook === undefined) return value.bind(obj);
            return async (...args: unknown[]) => {
                await hook(...args);
                return value.apply(obj, args);
            };
        },
    });
}

// Runs `fn` before each `apply` of the target: a change captured while a
// projection pass computes its delta.
export function beforeApply<T extends object>(inner: T, fn: Hook): T {
    return hooked(inner, { apply: fn });
}

// Runs `fn` before each `commitIngest` of the target: a change captured after
// ingestion and before its settle.
export function beforeSettle<T extends object>(inner: T, fn: Hook): T {
    return hooked(inner, { commitIngest: fn });
}

// The error a simulated crash throws.
export class SimulatedCrash extends Error {
    constructor(where: string) {
        super(`simulated crash before ${where}`);
        this.name = 'SimulatedCrash';
    }
}

// A hook that throws a SimulatedCrash on the `nth` call (1-based) of the
// method it is installed on, and lets every other call through. `calls()`
// tells how many calls it has seen.
export function throwOnCall(method: string, nth: number): Hook & { calls(): number } {
    let seen = 0;
    const hook = (async () => {
        seen += 1;
        if (seen === nth) throw new SimulatedCrash(`${method} call ${nth}`);
    }) as Hook & { calls(): number };
    hook.calls = () => seen;
    return hook;
}

// Wraps `inner` so that the `nth` call of `method` throws a SimulatedCrash.
export function crashBefore<T extends object>(inner: T, method: string, nth = 1): T {
    return hooked(inner, { [method]: throwOnCall(method, nth) });
}

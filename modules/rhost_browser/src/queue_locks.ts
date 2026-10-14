// An in-memory Web Locks manager: exclusive, not reentrant, queues, and
// `ifAvailable` fails at once when the name is held. A pending request whose
// signal aborts is dropped. Aborting a request that already holds the lock
// does not take it away. Tests use this; a browser uses `navigator.locks`.

export interface WebLockManager {
    request(
        name: string,
        options: { ifAvailable?: boolean; signal?: AbortSignal },
        callback: (lock: { name: string } | null) => Promise<unknown>,
    ): Promise<unknown>;
    query(): Promise<{ held?: { name?: string }[]; pending?: { name?: string }[] }>;
}

type Job = {
    name: string;
    ifAvailable: boolean;
    signal?: AbortSignal;
    callback: (lock: { name: string } | null) => Promise<unknown>;
    resolve: (value: unknown) => void;
    reject: (err: unknown) => void;
};

function abortError(): DOMException {
    return new DOMException('The operation was aborted.', 'AbortError');
}

export function createQueueLocks(): WebLockManager {
    const held = new Map<string, Job>();
    const queues = new Map<string, Job[]>();

    const grantNext = (name: string): void => {
        const queue = queues.get(name);
        if (queue === undefined || queue.length === 0) {
            queues.delete(name);
            return;
        }
        const next = queue.shift()!;
        if (queue.length === 0) queues.delete(name);
        if (next.signal?.aborted) {
            next.reject(abortError());
            grantNext(name);
            return;
        }
        grant(next);
    };

    const finish = (job: Job): void => {
        if (held.get(job.name) !== job) return;
        held.delete(job.name);
        grantNext(job.name);
    };

    const run = async (job: Job, lock: { name: string } | null): Promise<void> => {
        try {
            job.resolve(await job.callback(lock));
        } catch (err) {
            job.reject(err);
        } finally {
            if (lock !== null) finish(job);
        }
    };

    const grant = (job: Job): void => {
        held.set(job.name, job);
        void run(job, { name: job.name });
    };

    return {
        request(name, options, callback) {
            return new Promise((resolve, reject) => {
                if (options.signal?.aborted) {
                    reject(abortError());
                    return;
                }
                const job: Job = {
                    name,
                    ifAvailable: options.ifAvailable === true,
                    signal: options.signal,
                    callback,
                    resolve,
                    reject,
                };
                options.signal?.addEventListener('abort', () => {
                    const queue = queues.get(name);
                    const index = queue?.indexOf(job) ?? -1;
                    if (index < 0) return;
                    queue!.splice(index, 1);
                    if (queue!.length === 0) queues.delete(name);
                    reject(abortError());
                });
                if (!held.has(name)) {
                    grant(job);
                    return;
                }
                if (job.ifAvailable) {
                    void run(job, null);
                    return;
                }
                const queue = queues.get(name) ?? [];
                queue.push(job);
                queues.set(name, queue);
            });
        },
        async query() {
            const pending: { name: string }[] = [];
            for (const [name, jobs] of queues) {
                for (let i = 0; i < jobs.length; i++) pending.push({ name });
            }
            return { held: [...held.keys()].map((name) => ({ name })), pending };
        },
    };
}

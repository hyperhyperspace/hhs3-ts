import { IdbEnv } from "./idb_env.js";
import { IdbDagStore } from "./idb_dag_store.js";

// IdbDagStore that detects DAG growth from other browsing contexts (tabs) via a
// BroadcastChannel. On local commit it posts a message; peers listening on a
// channel of the same name re-read getFrontier() (at-least-once, no payload).
//
// A BroadcastChannel does not deliver a message to the same object that sent it,
// so reusing the observer's channel to post avoids re-firing this tab's own
// listeners (they are already fired directly by IdbDagStore.withTransaction).

export class BroadcastIdbDagStore extends IdbDagStore {

    private channelName: string;
    private observerChannel: BroadcastChannel | undefined = undefined;

    constructor(env: IdbEnv, dagId: number, dbName: string) {
        super(env, dagId);
        this.channelName = `hhs3-dag-idb:${dbName}:${dagId}`;
    }

    protected startExternalObserver(notify: () => void): unknown {
        const channel = new BroadcastChannel(this.channelName);
        channel.onmessage = () => notify();
        this.observerChannel = channel;
        return channel;
    }

    protected stopExternalObserver(handle: unknown): void {
        const channel = handle as BroadcastChannel;
        try { channel.close(); } catch (_e) { /* ignore */ }
        if (this.observerChannel === channel) {
            this.observerChannel = undefined;
        }
    }

    protected onCommitted(): void {
        if (this.observerChannel !== undefined) {
            // Reuse the observer channel: the sender never receives its own
            // message, so this won't double-fire our local listeners.
            this.observerChannel.postMessage(1);
            return;
        }
        // No local listeners, but other tabs may be observing.
        const channel = new BroadcastChannel(this.channelName);
        channel.postMessage(1);
        // Defer close so the queued message is dispatched first.
        setTimeout(() => {
            try { channel.close(); } catch (_e) { /* ignore */ }
        }, 0);
    }

    // Release the observer channel (if any). Called when the owning DAG is closed.
    close(): void {
        if (this.observerChannel !== undefined) {
            try { this.observerChannel.close(); } catch (_e) { /* ignore */ }
            this.observerChannel = undefined;
        }
    }
}

// deduplicating queue: inserting an element that is already in the queue does nothing

import { MultiMap } from "./multimap";

class LabelledQueue<Q, L> {

    queue: MultiMap<Q, L>;

    constructor() {
        this.queue = new MultiMap();
    }

    enqueue(value: Q, labels: Iterable<L>) {
        const key = Symbol();

        this.queue.addMany(value, labels);
    }

    dequeue(): {item: Q, labels: Set<L>} | undefined {

        if (this.queue.size === 0) {
            return undefined;
        }

        const firstKey = this.queue.keys().next().value as Q;

        if (firstKey === undefined) {
            return undefined;
        } else {
            const labels = this.queue.get(firstKey);

            this.queue.deleteKey(firstKey); // Remove the first entry

            return {item: firstKey, labels: labels};
        }
    }

    peek(): {item: Q, labels: Set<L>} | undefined {

        if (this.queue.size === 0) {
            return undefined;
        }
        const firstKey = this.queue.keys().next().value as Q;

        if (firstKey === undefined) {
            return undefined;
        } else {
            const labels = this.queue.get(firstKey);
            return {item: firstKey, labels: labels};
        }
    }


    isEmpty() {
        return this.queue.size === 0;
    }

    size() {
        return this.queue.size;
    }
}

export { LabelledQueue };
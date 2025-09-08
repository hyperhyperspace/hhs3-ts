// deduplicating queue: inserting an element that is already in the queue does nothing

class DedupQueue<T> {

    queue: Set<T>;

    constructor() {
        this.queue = new Set();
    }

    enqueue(value: T) {
        if (!this.queue.has(value)) {
            this.queue.add(value);
        }
    }

    dequeue() {
        const next = this.peek();

        if (next !== undefined) {
            this.queue.delete(next); // Remove the oldest entry
        }

        return next;
    }

    peek() {
        return this.queue.values().next().value;
    }

    values() {
        return this.queue.values();
    }

    isEmpty() {
        return this.queue.size === 0;
    }

    size() {
        return this.queue.size;
    }
}

export { DedupQueue };
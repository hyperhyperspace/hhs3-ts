class Queue<T> {
    
    queue: Map<Symbol, T>;
    
    constructor() {
        this.queue = new Map();
    }
    
    enqueue(value: T) {
        const key = Symbol();
        this.queue.set(key, value);
    }
    
    dequeue() {
        
        if (this.queue.size === 0) {
            return undefined;
        }
        const firstKey = this.queue.keys().next().value as Symbol;
        const firstValue = this.queue.get(firstKey);
        
        this.queue.delete(firstKey); // Remove the first entry
        return firstValue;
    }
    
    peek() {
        
        if (this.queue.size === 0) {
            return undefined;
        }
        const firstKey = this.queue.keys().next().value as Symbol;
        return this.queue.get(firstKey);
    }

    isEmpty() {
        return this.queue.size === 0;
    }
    
    size() {
        return this.queue.size;
    }

    values() {
        return this.queue.values();
    }
}

export { Queue };
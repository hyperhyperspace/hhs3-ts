type QueueNode<T> = {
  item: T;
  priority: number;
};

export class PriorityQueue<T> {
  private data: QueueNode<T>[] = [];

  // Insert an item with priority (lower number = higher priority)
  enqueue(item: T, priority: number = 0): void {
    this.data.push({ item, priority });
    this.bubbleUp(this.data.length - 1);
  }

  // Remove the item with the lowest priority value
  dequeue(): T | undefined {
    if (this.data.length === 0) return undefined;

    const min = this.data[0];
    const end = this.data.pop()!; // non-null because length > 0
    if (this.data.length > 0) {
      this.data[0] = end;
      this.sinkDown(0);
    }
    return min.item;
  }

  peek(): T | undefined {
    return this.data[0]?.item;
  }

  isEmpty(): boolean {
    return this.data.length === 0;
  }

  size(): number {
    return this.data.length;
  }

  // --- Private helper methods ---

  private bubbleUp(n: number): void {
    const element = this.data[n];
    while (n > 0) {
      const parentN = Math.floor((n - 1) / 2);
      const parent = this.data[parentN];
      if (element.priority >= parent.priority) break;
      this.data[parentN] = element;
      this.data[n] = parent;
      n = parentN;
    }
  }

  private sinkDown(n: number): void {
    const length = this.data.length;
    const element = this.data[n];

    while (true) {
      let leftN = 2 * n + 1;
      let rightN = 2 * n + 2;
      let swap: number | null = null;

      if (leftN < length) {
        const left = this.data[leftN];
        if (left.priority < element.priority) swap = leftN;
      }

      if (rightN < length) {
        const right = this.data[rightN];
        if (
          (swap === null && right.priority < element.priority) ||
          (swap !== null && right.priority < this.data[swap].priority)
        ) {
          swap = rightN;
        }
      }

      if (swap === null) break;
      this.data[n] = this.data[swap];
      this.data[swap] = element;
      n = swap;
    }
  }
}
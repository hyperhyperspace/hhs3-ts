export async function* filter<T>(
    source: AsyncIterable<T>,
    pred: (item: T) => boolean | Promise<boolean>
  ): AsyncIterable<T> {
    for await (const item of source) {
      if (await pred(item)) {
        yield item;
      }
    }
  }
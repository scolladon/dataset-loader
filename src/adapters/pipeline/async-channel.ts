/**
 * Bounded async channel for passing values from multiple concurrent producers
 * to a single consumer via async iteration.
 *
 * Producers call push() and await backpressure when the queue is full.
 * The consumer iterates with `for await (const item of channel)`.
 * Call close() to signal end-of-stream, or fail(err) to propagate an error.
 *
 * close() and fail() are idempotent. push() after close() or fail() rejects.
 * If the consumer breaks early out of `for await`, the iterator's return()
 * closes the channel and releases any stalled producers.
 */
export class AsyncChannel<T> {
  private readonly queue: T[] = []
  private readonly waitingProducers: Array<{
    resolve: () => void
    reject: (err: Error) => void
  }> = []
  private waiter?: {
    resolve: (result: IteratorResult<T>) => void
    reject: (err: Error) => void
  }
  private closed = false
  private channelError?: Error

  constructor(private readonly highWater = 16) {}

  push(item: T): Promise<void> {
    if (this.channelError) return Promise.reject(this.channelError)
    if (this.closed) {
      return Promise.reject(new Error('AsyncChannel: push on closed channel'))
    }
    if (this.waiter) {
      const { resolve } = this.waiter
      this.waiter = undefined
      resolve({ value: item, done: false })
      return Promise.resolve()
    }
    this.queue.push(item)
    if (this.queue.length < this.highWater) {
      return Promise.resolve()
    }
    return new Promise<void>((resolve, reject) => {
      this.waitingProducers.push({ resolve, reject })
    })
  }

  close(): void {
    if (this.closed || this.channelError) return
    this.closed = true
    for (const p of this.waitingProducers) p.resolve()
    this.waitingProducers.length = 0
    if (this.waiter) {
      const { resolve } = this.waiter
      this.waiter = undefined
      resolve({ value: undefined, done: true } as IteratorResult<T>)
    }
  }

  fail(err: Error): void {
    if (this.channelError || this.closed) return
    this.channelError = err
    for (const p of this.waitingProducers) p.reject(err)
    this.waitingProducers.length = 0
    if (this.waiter) {
      const { reject } = this.waiter
      this.waiter = undefined
      reject(err)
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    const cancel = (): void => {
      if (this.closed || this.channelError) return
      this.channelError = new Error('AsyncChannel: consumer cancelled')
      for (const p of this.waitingProducers) p.reject(this.channelError)
      this.waitingProducers.length = 0
    }
    return {
      next: (): Promise<IteratorResult<T>> => {
        if (this.queue.length > 0) {
          const value = this.queue.shift()!
          if (this.waitingProducers.length > 0) {
            this.waitingProducers.shift()!.resolve()
          }
          return Promise.resolve({ value, done: false })
        }
        if (this.channelError) return Promise.reject(this.channelError)
        if (this.closed) {
          return Promise.resolve({
            value: undefined,
            done: true,
          } as IteratorResult<T>)
        }
        return new Promise<IteratorResult<T>>((resolve, reject) => {
          this.waiter = { resolve, reject }
        })
      },
      return: (): Promise<IteratorResult<T>> => {
        cancel()
        return Promise.resolve({
          value: undefined,
          done: true,
        } as IteratorResult<T>)
      },
    }
  }
}

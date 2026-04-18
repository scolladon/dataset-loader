import { describe, expect, it } from 'vitest'
import { AsyncChannel } from '../../../src/adapters/pipeline/async-channel.js'

describe('AsyncChannel', () => {
  it('given no items pushed, when closed then iterated, then yields nothing', async () => {
    // Arrange
    const sut = new AsyncChannel<string>()

    // Act
    sut.close()
    const result: string[] = []
    for await (const item of sut) {
      result.push(item)
    }

    // Assert
    expect(result).toEqual([])
  })

  it('given items pushed before iteration, when iterated after close, then yields items in push order', async () => {
    // Arrange
    const sut = new AsyncChannel<string>(4)
    await sut.push('a')
    await sut.push('b')
    await sut.push('c')
    sut.close()

    // Act
    const result: string[] = []
    for await (const item of sut) {
      result.push(item)
    }

    // Assert
    expect(result).toEqual(['a', 'b', 'c'])
  })

  it('given concurrent producer and consumer, when producing items, then all items are yielded in order', async () => {
    // Arrange
    const sut = new AsyncChannel<number>(2)
    const result: number[] = []

    // Act
    const consumer = (async () => {
      for await (const item of sut) {
        result.push(item)
      }
    })()
    for (let i = 0; i < 5; i++) {
      await sut.push(i)
    }
    sut.close()
    await consumer

    // Assert
    expect(result).toEqual([0, 1, 2, 3, 4])
  })

  it('given channel at capacity, when producer pushes, then producer waits until consumer dequeues', async () => {
    // Arrange
    const sut = new AsyncChannel<string>(2)
    await sut.push('a')

    let secondPushResolved = false
    const pushPromise = sut.push('b').then(() => {
      secondPushResolved = true
    })

    // Act + Assert — producer stalled immediately
    expect(secondPushResolved).toBe(false)

    const iter = sut[Symbol.asyncIterator]()
    await iter.next() // dequeue 'a', unblock producer
    await pushPromise

    expect(secondPushResolved).toBe(true)
  })

  it('given consumer waiting for items, when fail called, then iteration throws the error', async () => {
    // Arrange
    const sut = new AsyncChannel<string>()
    const err = new Error('producer error')

    const iterPromise = (async () => {
      for await (const _ of sut) {
        /* consume */
      }
    })()
    await Promise.resolve() // let for-await enter waiting state

    // Act
    sut.fail(err)

    // Assert
    await expect(iterPromise).rejects.toThrow('producer error')
  })

  it('given producer waiting at capacity, when fail called, then push rejects with the error', async () => {
    // Arrange
    const sut = new AsyncChannel<string>(1)
    // With highWater=1: first push adds to queue, length=1, 1 < 1 is false → stalls
    const err = new Error('fail error')

    // Act
    const pushPromise = sut.push('a')
    sut.fail(err)

    // Assert
    await expect(pushPromise).rejects.toThrow('fail error')
  })

  it('given multiple concurrent producers, when iterated, then all items are received', async () => {
    // Arrange
    const sut = new AsyncChannel<number>(4)
    const result: number[] = []

    const consumer = (async () => {
      for await (const item of sut) {
        result.push(item)
      }
    })()

    // Act — two concurrent producers
    await Promise.all([
      Promise.all([0, 1, 2].map(n => sut.push(n))),
      Promise.all([3, 4, 5].map(n => sut.push(n))),
    ])
    sut.close()
    await consumer

    // Assert
    expect(result.sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5])
  })

  it('given item pushed with waiting consumer, when push called, then consumer receives item directly without queuing', async () => {
    // Arrange
    const sut = new AsyncChannel<string>()
    const iter = sut[Symbol.asyncIterator]()

    // Act — consumer starts waiting before producer pushes
    const nextPromise = iter.next()
    await sut.push('direct')
    const result = await nextPromise

    // Assert
    expect(result).toEqual({ value: 'direct', done: false })
  })

  it('given closed channel, when push called, then push rejects and pre-close items still drain', async () => {
    // Arrange
    const sut = new AsyncChannel<string>(4)
    await sut.push('before')
    sut.close()

    // Act & Assert — post-close push is rejected (no silent data loss)
    await expect(sut.push('after')).rejects.toThrow(/closed/i)

    // Assert — the pre-close item is still drained
    const result: string[] = []
    for await (const item of sut) {
      result.push(item)
    }
    expect(result).toEqual(['before'])
  })

  it('given close called twice, when draining, then it is idempotent', async () => {
    // Arrange
    const sut = new AsyncChannel<string>(4)
    await sut.push('x')

    // Act
    sut.close()
    sut.close()

    // Assert
    const result: string[] = []
    for await (const item of sut) {
      result.push(item)
    }
    expect(result).toEqual(['x'])
  })

  it('given fail called twice, when iterating, then the first error is preserved', async () => {
    // Arrange
    const sut = new AsyncChannel<string>()
    const first = new Error('first')
    const second = new Error('second')

    // Act
    sut.fail(first)
    sut.fail(second)

    // Assert — first error wins, subsequent fail calls are no-ops
    await expect(
      (async () => {
        for await (const _ of sut) {
          /* consume */
        }
      })()
    ).rejects.toThrow('first')
  })

  it('given consumer waiting with empty queue, when iterator.return called, then a subsequent push rejects instead of silently delivering to nobody', async () => {
    // Arrange — consumer is suspended in next() (waiter set) and then cancels
    const sut = new AsyncChannel<number>(4)
    const iter = sut[Symbol.asyncIterator]()
    const pending = iter.next() // suspends, sets waiter

    // Act — consumer cancels while still waiting
    // @ts-expect-error return is optional on AsyncIterator; we call it explicitly
    await iter.return!()

    // Assert — the suspended next() resolves done, and a later push is rejected
    await expect(pending).resolves.toEqual({ value: undefined, done: true })
    await expect(sut.push(1)).rejects.toThrow(/cancel|closed/i)
  })

  it('given channel already closed, when consumer breaks early, then cancel is a no-op (no error overrides clean close)', async () => {
    // Arrange — channel is closed before iteration starts
    const sut = new AsyncChannel<string>(4)
    await sut.push('one')
    sut.close()

    // Act — start iterating and immediately break; cancel() should early-return
    // because closed === true, so channelError stays unset and the drain is clean
    for await (const item of sut) {
      expect(item).toBe('one')
      break
    }

    // Assert — re-iteration after a clean close yields nothing (and no rejection)
    const rest: string[] = []
    for await (const item of sut) rest.push(item)
    expect(rest).toEqual([])
  })

  it('given fail then close then fail sequence, when iterating, then first error is preserved', async () => {
    // Arrange
    const sut = new AsyncChannel<string>()
    const first = new Error('first')

    // Act — fail wins, later close/fail are no-ops
    sut.fail(first)
    sut.close()
    sut.fail(new Error('second'))

    // Assert
    await expect(
      (async () => {
        for await (const _ of sut) {
          /* consume */
        }
      })()
    ).rejects.toThrow('first')
  })

  it('given producer stalled at capacity, when consumer breaks early, then the stalled push rejects rather than hanging', async () => {
    // Arrange — highWater=1 forces the second push to stall
    const sut = new AsyncChannel<number>(1)

    // Act — consume one item then break early while a producer is stalled
    const producer = Promise.all([sut.push(1), sut.push(2), sut.push(3)])

    const consumer = (async () => {
      for await (const item of sut) {
        if (item === 1) break
      }
    })()

    await consumer

    // Assert — stalled producers are released (rejected) instead of deadlocking
    await expect(producer).rejects.toThrow(/closed|cancel|consumer/i)
  })

  it('given failed channel, when push called without stalling, then push rejects immediately', async () => {
    // Arrange
    const sut = new AsyncChannel<string>()
    const err = new Error('channel error')
    sut.fail(err)

    // Act + Assert
    await expect(sut.push('x')).rejects.toThrow('channel error')
  })

  it('given failed channel, when close called, then iteration still rejects with the original error', async () => {
    // Arrange
    const sut = new AsyncChannel<string>()
    const err = new Error('fail first')
    sut.fail(err)

    // Act — close after fail should not swallow the error
    sut.close()

    // Assert
    await expect(
      (async () => {
        for await (const _ of sut) {
          /* consume */
        }
      })()
    ).rejects.toThrow('fail first')
  })

  it('given producers stalled at capacity, when close called, then all pending pushes resolve', async () => {
    // Arrange: highWater=1 → queue(length=1) >= 1 → first push stalls immediately
    const sut = new AsyncChannel<string>(1)
    const push1 = sut.push('a')
    const push2 = sut.push('b')

    // Act — close while both producers are waiting in waitingProducers
    sut.close()

    // Assert — both blocked pushes resolve (not reject)
    await expect(Promise.all([push1, push2])).resolves.toEqual([
      undefined,
      undefined,
    ])
  })

  it('given highWater of 1, when first item pushed without waiting consumer, then producer stalls immediately', async () => {
    // Arrange — highWater=1: queue.length(1) < 1 is false, so first push stalls
    const sut = new AsyncChannel<string>(1)

    let pushResolved = false
    const pushPromise = sut.push('a').then(() => {
      pushResolved = true
    })

    // Act + Assert — stalled before any dequeue
    expect(pushResolved).toBe(false)

    const iter = sut[Symbol.asyncIterator]()
    await iter.next() // dequeue 'a', unblock producer
    await pushPromise

    expect(pushResolved).toBe(true)
  })
})

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

  it('given closed channel, when push called, then item is queued and delivered on next iteration', async () => {
    // Arrange
    const sut = new AsyncChannel<string>(4)
    await sut.push('before')
    sut.close()

    // Act — push after close
    await sut.push('after')

    // Assert — both items are drained before the done signal
    const result: string[] = []
    for await (const item of sut) {
      result.push(item)
    }
    expect(result).toEqual(['before', 'after'])
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

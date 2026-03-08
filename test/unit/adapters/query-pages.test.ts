import { describe, expect, it, vi } from 'vitest'
import { queryPages } from '../../../src/adapters/query-pages.js'
import { type QueryResult } from '../../../src/ports/types.js'

async function collectPages<T>(
  iterable: AsyncIterable<QueryResult<T>>
): Promise<QueryResult<T>[]> {
  const result: QueryResult<T>[] = []
  for await (const page of iterable) {
    result.push(page)
  }
  return result
}

describe('queryPages', () => {
  it('given single page, when iterating, then yields one page', async () => {
    // Arrange
    const first: QueryResult<string> = {
      totalSize: 2,
      done: true,
      records: ['a', 'b'],
    }

    // Act
    const sut = await collectPages(queryPages(first, vi.fn()))

    // Assert
    expect(sut).toHaveLength(1)
    expect(sut[0].records).toEqual(['a', 'b'])
  })

  it('given multiple pages, when iterating, then yields all pages in order', async () => {
    // Arrange
    const first: QueryResult<string> = {
      totalSize: 3,
      done: false,
      nextRecordsUrl: '/next1',
      records: ['a'],
    }
    const second: QueryResult<string> = {
      totalSize: 3,
      done: false,
      nextRecordsUrl: '/next2',
      records: ['b'],
    }
    const third: QueryResult<string> = {
      totalSize: 3,
      done: true,
      records: ['c'],
    }
    const queryMore = vi
      .fn()
      .mockResolvedValueOnce(second)
      .mockResolvedValueOnce(third)

    // Act
    const sut = await collectPages(queryPages(first, queryMore))

    // Assert
    expect(sut).toHaveLength(3)
    expect(sut.map(p => p.records)).toEqual([['a'], ['b'], ['c']])
    expect(queryMore).toHaveBeenCalledWith('/next1')
    expect(queryMore).toHaveBeenCalledWith('/next2')
  })
})

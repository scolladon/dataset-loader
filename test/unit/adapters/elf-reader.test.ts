import { Readable } from 'node:stream'
import { describe, expect, it, vi } from 'vitest'
import { ElfReader } from '../../../src/adapters/readers/elf-reader.js'
import { DateBounds } from '../../../src/domain/date-bounds.js'
import { Watermark } from '../../../src/domain/watermark.js'
import { collectLines } from '../../fixtures/collect-lines.js'
import { makeSfPort } from '../../fixtures/sf-port.js'

describe('ElfReader', () => {
  it('given records exist with watermark, when fetching, then yields data lines skipping headers', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
          { Id: '0AT2', LogDate: '2026-03-02T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValueOnce(
          Readable.from([Buffer.from('header1\ndata1a\ndata1b\n')])
        )
        .mockResolvedValueOnce(
          Readable.from([Buffer.from('header2\ndata2a\n')])
        ),
    })

    // Act
    const sut = new ElfReader(sfPort, 'LightningPageView', 'Daily')
    const result = await sut.fetch(
      Watermark.fromString('2026-02-28T00:00:00.000Z')
    )
    const lines = await collectLines(result.lines)

    // Assert — blobs are fetched concurrently; order reflects completion, not insertion order
    expect([...lines].sort()).toEqual(['data1a', 'data1b', 'data2a'])
    expect(result.fileCount()).toBe(2)
    expect(result.watermark()?.toString()).toBe('2026-03-02T00:00:00.000Z')
  })

  it('given no records, when fetching, then yields zero lines and watermark is undefined', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi
        .fn()
        .mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toHaveLength(0)
    expect(result.fileCount()).toBe(0)
    expect(result.watermark()).toBeUndefined()
    expect(result.total).toEqual({ count: 0, unit: 'files' })
  })

  it('given firstPage with totalSize, when fetching, then total reports file count', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 7,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValue(Readable.from([Buffer.from('hdr\ndata\n')])),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    await collectLines(result.lines)

    // Assert
    expect(result.total).toEqual({ count: 7, unit: 'files' })
  })

  it('given watermark, when fetching, then includes watermark in SOQL', async () => {
    // Arrange
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Hourly')
    await sut.fetch(Watermark.fromString('2026-01-01T00:00:00.000Z'))

    // Assert
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LogDate > 2026-01-01T00:00:00.000Z')
  })

  it('given no watermark and no bounds, when fetching, then queries all records ascending without LIMIT (breaking change: old "latest-1" fallback removed)', async () => {
    // Arrange — regression guard for the ELF first-run fallback removal.
    // Previously the reader emitted `ORDER BY LogDate DESC LIMIT 1`; now it
    // always uses `ORDER BY LogDate ASC` with no limit regardless of watermark.
    const querySpy = vi.fn().mockResolvedValue({
      totalSize: 1,
      done: true,
      records: [
        { Id: '0AT1', LogDate: '2026-03-05T00:00:00.000Z', LogFile: '' },
      ],
    })
    const sfPort = makeSfPort({
      query: querySpy,
      getBlobStream: vi
        .fn()
        .mockResolvedValue(Readable.from([Buffer.from('hdr\ndata\n')])),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    await sut.fetch()

    // Assert
    const soql: string = querySpy.mock.calls[0][0]
    expect(soql).toContain(
      'SELECT Id, LogDate, LogFile FROM EventLogFile WHERE '
    )
    expect(soql).toContain('ORDER BY LogDate ASC')
    expect(soql).not.toContain('DESC')
    expect(soql).not.toContain('LIMIT')
    // Kills mutations that unconditionally push undefined `lower`/`upper`
    // into conds when no bounds/watermark: produces 'AND undefined' in SOQL.
    expect(soql).not.toContain('undefined')
    // Exactly one AND — between baseWhere and ORDER BY — i.e., the two
    // filter clauses `EventType = …` and `Interval = …` are the only
    // pieces of the WHERE. Kills join-separator mutations.
    expect(soql.match(/ AND /g)?.length).toBe(1)
  })

  it('given watermark, when fetching, then queries all records ascending without limit', async () => {
    // Arrange
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    await sut.fetch(Watermark.fromString('2026-01-01T00:00:00.000Z'))

    // Assert
    const soql: string = querySpy.mock.calls[0][0]
    expect(soql).toContain('ORDER BY LogDate ASC')
    expect(soql).not.toContain('LIMIT')
  })

  it('given blob content split across multiple chunks, when fetching, then reassembles lines correctly', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      // Blob split mid-line: 'header\nda' + 'ta1\ndata2\n'
      getBlobStream: vi
        .fn()
        .mockResolvedValue(
          Readable.from([
            Buffer.from('header\nda'),
            Buffer.from('ta1\ndata2\n'),
          ])
        ),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toEqual(['data1', 'data2'])
  })

  it('given blob with CRLF line endings, when fetching, then strips carriage returns from lines', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValue(
          Readable.from([Buffer.from('header\r\ndata1\r\ndata2\r\n')])
        ),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toEqual(['data1', 'data2'])
  })

  it('given blob stream that yields string chunks, when fetching, then parses lines correctly', async () => {
    // Arrange: Readable.from(['...']) yields string chunks, covering the typeof === 'string' branch
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValue(Readable.from(['header\ndata1\ndata2\n'])),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch(
      Watermark.fromString('2026-02-28T00:00:00.000Z')
    )
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toEqual(['data1', 'data2'])
  })

  it('given blob whose last line has a trailing carriage return and no final newline, when fetching, then strips the carriage return from tail', async () => {
    // Arrange: content ends with \r but no \n — the remainder becomes the tail and endsWith('\r') is true
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValue(Readable.from([Buffer.from('header\ndata1\r')])),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch(
      Watermark.fromString('2026-02-28T00:00:00.000Z')
    )
    const lines = await collectLines(result.lines)

    // Assert — tail '\r' is stripped; 'data1' is yielded
    expect(lines).toEqual(['data1'])
  })

  it('given blob with no trailing newline and only a header line, when fetching, then captures header from tail', async () => {
    // Arrange — blob has no \n at all: the entire content becomes "tail" with isFirstLine=true
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValue(
          Readable.from([Buffer.from('TIMESTAMP_DERIVED,USER_ID')])
        ),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toHaveLength(0)
    expect(await sut.header()).toBe('TIMESTAMP_DERIVED,USER_ID')
  })

  it('given blob with more than BATCH_SIZE lines, when fetching, then all lines are received across multiple flushes', async () => {
    // Arrange: 2001 data lines exceeds BATCH_SIZE=2000, triggering a mid-loop flushPending
    const rows = Array.from({ length: 2001 }, (_, i) => `data${i}`)
    const content = `header\n${rows.join('\n')}\n`
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValue(Readable.from([Buffer.from(content)])),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch(
      Watermark.fromString('2026-02-28T00:00:00.000Z')
    )
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toHaveLength(2001)
  })

  it('given blob with more than BATCH_SIZE lines, when fetching, then yields two batches of correct sizes', async () => {
    // Arrange: 2001 rows — kills pending.length >= BATCH_SIZE → > mutation (> yields 1 batch of 2001)
    const rows = Array.from({ length: 2001 }, (_, i) => `data${i}`)
    const content = `header\n${rows.join('\n')}\n`
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValue(Readable.from([Buffer.from(content)])),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch(
      Watermark.fromString('2026-02-28T00:00:00.000Z')
    )
    const batches: string[][] = []
    for await (const batch of result.lines) batches.push(batch)

    // Assert
    expect(batches).toHaveLength(2)
    expect(batches[0]).toHaveLength(2000)
    expect(batches[1]).toHaveLength(1)
  })

  it('given specific event type and interval, when fetching, then SOQL contains EventType, Interval, and field list', async () => {
    // Arrange — kills mutations to the EventType/Interval template strings (L41)
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    await sut.fetch(Watermark.fromString('2026-01-01T00:00:00.000Z'))

    // Assert
    const soql: string = querySpy.mock.calls[0][0]
    expect(soql).toContain("EventType = 'Login'")
    expect(soql).toContain("Interval = 'Daily'")
    expect(soql).toContain('SELECT Id, LogDate, LogFile FROM EventLogFile')
  })

  it('given a record, when fetching, then requests blob with correct Salesforce API URL', async () => {
    // Arrange — kills mutations to the blobUrl template string (L64)
    const getBlobSpy = vi
      .fn()
      .mockResolvedValue(Readable.from([Buffer.from('hdr\nrow1\n')]))
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1xx', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: getBlobSpy,
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    await collectLines(result.lines)

    // Assert
    expect(getBlobSpy).toHaveBeenCalledWith(
      '/services/data/v62.0/sobjects/EventLogFile/0AT1xx/LogFile'
    )
  })

  it('given blob ending with newline, when fetching, then no empty strings are yielded', async () => {
    // Arrange — kills tail.length > 0 → >= 0 and line.length > 0 → >= 0 mutations (L125/L127):
    // with those mutations an empty string would be pushed to pending and yielded
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValue(
          Readable.from([Buffer.from('header\ndata1\ndata2\n')])
        ),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch(
      Watermark.fromString('2026-02-28T00:00:00.000Z')
    )
    const lines = await collectLines(result.lines)

    // Assert — no empty strings; tail is '' after the final '\n', must not be pushed
    expect(lines).toEqual(['data1', 'data2'])
  })

  it('given invalid eventType, when creating reader, then throws', () => {
    // Arrange
    const sfPort = makeSfPort()

    // Act
    const act = () => new ElfReader(sfPort, 'bad type!', 'Daily')

    // Assert
    expect(act).toThrow('Invalid eventType')
  })

  it('given invalid interval, when creating reader, then throws', () => {
    // Arrange
    const sfPort = makeSfPort()

    // Act
    const act = () => new ElfReader(sfPort, 'Login', 'Weekly')

    // Assert
    expect(act).toThrow('Invalid interval')
  })

  it('given paginated ELF records, when fetching, then prefetches next page while streaming current page blobs', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: false,
        nextRecordsUrl: '/next1',
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      queryMore: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          { Id: '0AT2', LogDate: '2026-03-02T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValueOnce(Readable.from([Buffer.from('h1\na\n')]))
        .mockResolvedValueOnce(Readable.from([Buffer.from('h2\nb\n')])),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toHaveLength(2)
    expect(result.fileCount()).toBe(2)
    expect(result.watermark()?.toString()).toBe('2026-03-02T00:00:00.000Z')
  })

  it('given blob with empty lines, when fetching, then skips empty lines', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValue(
          Readable.from([Buffer.from('header\n\ndata1\n\ndata2\n')])
        ),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toEqual(['data1', 'data2'])
  })

  it('given multiple blobs with different download times, when fetching, then processes blobs in completion order', async () => {
    // Arrange
    const order: string[] = []
    let resolveBlob1!: (v: Readable) => void
    let resolveBlob2!: (v: Readable) => void
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
          { Id: '0AT2', LogDate: '2026-03-02T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockImplementationOnce(
          () =>
            new Promise<Readable>(resolve => {
              resolveBlob1 = (v: Readable) => {
                order.push('blob1-resolved')
                resolve(v)
              }
            })
        )
        .mockImplementationOnce(
          () =>
            new Promise<Readable>(resolve => {
              resolveBlob2 = (v: Readable) => {
                order.push('blob2-resolved')
                resolve(v)
              }
            })
        ),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const linesPromise = collectLines(result.lines)
    // Wait for async generator to start iterating and wire up getBlobStream mocks
    await vi.waitFor(() => expect(resolveBlob2).toBeDefined(), {
      timeout: 2000,
    })
    resolveBlob2(Readable.from([Buffer.from('h2\nfrom_blob2\n')]))
    resolveBlob1(Readable.from([Buffer.from('h1\nfrom_blob1\n')]))
    const lines = await linesPromise

    // Assert
    expect(lines).toEqual(['from_blob2', 'from_blob1'])
    expect(order).toEqual(['blob2-resolved', 'blob1-resolved'])
  })

  it('given two pages, when fetching, then blob fetching across pages runs concurrently', async () => {
    // Arrange
    const callOrder: string[] = []
    let resolvePage1Blob!: (v: Readable) => void

    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: false,
        nextRecordsUrl: '/next1',
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      queryMore: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          { Id: '0AT2', LogDate: '2026-03-02T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockImplementationOnce(
          () =>
            new Promise<Readable>(resolve => {
              callOrder.push('page1-blob-requested')
              resolvePage1Blob = (v: Readable) => {
                callOrder.push('page1-blob-resolved')
                resolve(v)
              }
            })
        )
        .mockImplementationOnce(() => {
          callOrder.push('page2-blob-requested')
          return Promise.resolve(Readable.from([Buffer.from('h2\ndata2\n')]))
        }),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const linesPromise = collectLines(result.lines)

    // page2 blob is requested concurrently while page1 blob is still pending
    await vi.waitFor(
      () => expect(callOrder).toContain('page2-blob-requested'),
      { timeout: 2000 }
    )
    expect(callOrder).not.toContain('page1-blob-resolved')

    resolvePage1Blob(Readable.from([Buffer.from('h1\ndata1\n')]))
    const lines = await linesPromise

    // Assert
    expect(lines).toHaveLength(2)
    expect(result.watermark()?.toString()).toBe('2026-03-02T00:00:00.000Z')
  })

  it('given getBlobStream rejects on second blob, when fetching, then error propagates', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
          { Id: '0AT2', LogDate: '2026-03-02T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValueOnce(Readable.from([Buffer.from('h\nok\n')]))
        .mockRejectedValueOnce(new Error('stream failure')),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()

    // Assert
    await expect(collectLines(result.lines)).rejects.toThrow('stream failure')
  })

  it('given records exist, when header() called after fetch streams data, then returns first CSV line', async () => {
    // Arrange
    const record = {
      Id: '0AT1',
      LogDate: '2024-01-01T00:00:00.000Z',
      LogFile: '',
    }
    const blob = Readable.from([
      Buffer.from('TIMESTAMP_DERIVED,USER_ID\n2024-01-01,user1'),
    ])
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [record],
      }),
      getBlobStream: vi.fn().mockResolvedValue(blob),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    for await (const _ of result.lines) {
      /* consume */
    }
    const header = await sut.header()

    // Assert
    expect(header).toBe('TIMESTAMP_DERIVED,USER_ID')
  })

  it('given no records, when header() called, then returns empty string', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi
        .fn()
        .mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    await sut.fetch()

    // Assert
    expect(await sut.header()).toBe('')
  })

  it('given 11 concurrent blobs with slow consumer, when fetching, then no MaxListenersExceeded warning is emitted', async () => {
    // Arrange
    const maxListenerWarnings: string[] = []
    const onWarning = (w: Error) => {
      if (w.name === 'MaxListenersExceededWarning')
        maxListenerWarnings.push(w.message)
    }
    process.on('warning', onWarning)

    try {
      const records = Array.from({ length: 11 }, (_, i) => ({
        Id: `0AT${i}`,
        LogDate: `2026-03-${String(i + 1).padStart(2, '0')}T00:00:00.000Z`,
        LogFile: '',
      }))
      const sfPort = makeSfPort({
        query: vi
          .fn()
          .mockResolvedValue({ totalSize: 11, done: true, records }),
        // 4 data lines per blob → 44 total, fills objectMode highWaterMark=16 to force backpressure
        getBlobStream: vi
          .fn()
          .mockImplementation(() =>
            Promise.resolve(
              Readable.from([
                Buffer.from('header\nline1\nline2\nline3\nline4\n'),
              ])
            )
          ),
      })
      const sut = new ElfReader(sfPort, 'LightningPageView', 'Daily')

      // Act
      const result = await sut.fetch(
        Watermark.fromString('2026-02-28T00:00:00.000Z')
      )
      const lines: string[] = []
      for await (const batch of result.lines) {
        lines.push(...batch)
        await new Promise<void>(resolve => setImmediate(resolve)) // slow consumer to induce backpressure
      }

      // Assert
      expect(lines).toHaveLength(44)
      await new Promise<void>(resolve => setImmediate(resolve))
      expect(maxListenerWarnings).toHaveLength(0)
    } finally {
      process.off('warning', onWarning)
    }
  })

  it('given pagination where last page is empty, when fetching, then watermark uses last record from non-empty page', async () => {
    // Arrange — kills L75 ConditionalExpression 'true' and EqualityOperator '>=0':
    // with mutation, lastRecord gets overwritten to undefined by the empty second page
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: false,
        nextRecordsUrl: '/next',
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      queryMore: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValue(Readable.from([Buffer.from('hdr\nrow\n')])),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch(
      Watermark.fromString('2026-02-28T00:00:00.000Z')
    )
    await collectLines(result.lines)

    // Assert — watermark comes from the last non-empty page, not overwritten to undefined
    expect(result.watermark()?.toString()).toBe('2026-03-01T00:00:00.000Z')
  })

  it('given blob whose tail is a bare carriage return, when fetching, then does not yield an empty string', async () => {
    // Arrange — kills L127 ConditionalExpression 'true' and EqualityOperator '>=0':
    // tail = '\r'; after CR-strip, line = ''; with mutation 'true/>=0' the empty string gets pushed
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValue(Readable.from([Buffer.from('header\ndata1\n\r')])),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch(
      Watermark.fromString('2026-02-28T00:00:00.000Z')
    )
    const lines = await collectLines(result.lines)

    // Assert — bare '\r' strips to ''; must not appear as an empty line
    expect(lines).toEqual(['data1'])
  })

  it('given done=true page with nextRecordsUrl, when fetching, then does not call queryMore', async () => {
    // Arrange — kills L82 LogicalOperator: !page.done && nextUrl → !page.done || nextUrl
    // With ||, done=true but nextUrl present would still trigger queryMore
    const queryMoreSpy = vi.fn()
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        nextRecordsUrl: '/services/data/v62.0/query/next',
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      queryMore: queryMoreSpy,
      getBlobStream: vi
        .fn()
        .mockResolvedValue(Readable.from([Buffer.from('hdr\nrow\n')])),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch(
      Watermark.fromString('2026-02-28T00:00:00.000Z')
    )
    await collectLines(result.lines)

    // Assert — done=true stops pagination even if nextRecordsUrl is present
    expect(queryMoreSpy).not.toHaveBeenCalled()
  })

  it('given blob stream, when fetching completes, then destroys the blob stream', async () => {
    // Arrange — kills L137 BlockStatement: finally { stream.destroy() } → {}
    const stream = Readable.from([Buffer.from('hdr\nrow\n')])
    const destroySpy = vi.spyOn(stream, 'destroy')
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi.fn().mockResolvedValue(stream),
    })

    // Act
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch(
      Watermark.fromString('2026-02-28T00:00:00.000Z')
    )
    await collectLines(result.lines)

    // Assert — stream.destroy() is called in finally block after processing
    expect(destroySpy).toHaveBeenCalled()
  })

  it('given bounds start-only and no watermark, when fetching, then SOQL has inclusive geq on LogDate ascending', async () => {
    // Arrange
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new ElfReader(
      sfPort,
      'Login',
      'Daily',
      DateBounds.from('2026-01-01T00:00:00.000Z', undefined)
    )
    await sut.fetch()

    // Assert
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LogDate >= 2026-01-01T00:00:00.000Z')
    expect(soql).toContain('ORDER BY LogDate ASC')
    expect(soql).not.toContain('LIMIT')
  })

  it('given bounds end-only and no watermark, when fetching, then SOQL has inclusive leq on LogDate ascending', async () => {
    // Arrange
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new ElfReader(
      sfPort,
      'Login',
      'Daily',
      DateBounds.from(undefined, '2026-01-31T23:59:59.999Z')
    )
    await sut.fetch()

    // Assert
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LogDate <= 2026-01-31T23:59:59.999Z')
    expect(soql).toContain('ORDER BY LogDate ASC')
    expect(soql).not.toContain('LIMIT')
    expect(soql).not.toMatch(/LogDate >= /)
    expect(soql).not.toMatch(/LogDate > /)
  })

  it('given bounds start and end and no watermark, when fetching, then SOQL has both geq and leq on LogDate joined with AND', async () => {
    // Arrange
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new ElfReader(
      sfPort,
      'Login',
      'Daily',
      DateBounds.from('2026-01-01T00:00:00.000Z', '2026-01-31T23:59:59.999Z')
    )
    await sut.fetch()

    // Assert — positioned ` AND ` assertions kill mutations on the
    // .join(' AND ') separator: any other separator would concatenate
    // the conditions without space + AND between them.
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain(' AND LogDate >= 2026-01-01T00:00:00.000Z')
    expect(soql).toContain(' AND LogDate <= 2026-01-31T23:59:59.999Z')
  })

  it('given bounds start and watermark, when fetching, then SOQL has only geq (start wins)', async () => {
    // Arrange — regression guard: --start-date always wins; no AND-ing with `LogDate > watermark`
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new ElfReader(
      sfPort,
      'Login',
      'Daily',
      DateBounds.from('2026-01-01T00:00:00.000Z', undefined)
    )
    await sut.fetch(Watermark.fromString('2026-02-01T00:00:00.000Z'))

    // Assert
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LogDate >= 2026-01-01T00:00:00.000Z')
    expect(soql).not.toContain('LogDate > 2026-02-01T00:00:00.000Z')
  })

  it('given bounds end and watermark but no start, when fetching, then SOQL has gt-watermark AND le-end', async () => {
    // Arrange
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new ElfReader(
      sfPort,
      'Login',
      'Daily',
      DateBounds.from(undefined, '2026-03-31T23:59:59.999Z')
    )
    await sut.fetch(Watermark.fromString('2026-01-01T00:00:00.000Z'))

    // Assert
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LogDate > 2026-01-01T00:00:00.000Z')
    expect(soql).toContain('LogDate <= 2026-03-31T23:59:59.999Z')
    expect(soql).toContain(' AND ')
  })
})

import { describe, expect, it, vi } from 'vitest'
import { SObjectReader } from '../../../src/adapters/readers/sobject-reader.js'
import { Watermark } from '../../../src/domain/watermark.js'
import { collectLines } from '../../fixtures/collect-lines.js'
import { makeSfPort } from '../../fixtures/sf-port.js'

describe('SObjectReader', () => {
  it('given records exist, when fetching, then yields CSV lines without header', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          {
            Id: '001A',
            Name: 'Acme',
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
          {
            Id: '001B',
            Name: 'Globex',
            LastModifiedDate: '2026-03-02T00:00:00.000Z',
          },
        ],
      }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name', 'LastModifiedDate'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toHaveLength(2)
    expect(lines[0]).toContain('001A')
    expect(lines[0]).toContain('Acme')
    expect(lines[1]).toContain('001B')
    expect(lines[1]).toContain('Globex')
    expect(lines.join('\n')).not.toMatch(/^"Id"/)
    expect(result.watermark()?.toString()).toBe('2026-03-02T00:00:00.000Z')
    expect(result.fileCount()).toBe(1)
  })

  it('given no records, when fetching, then yields zero lines and watermark is undefined', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi
        .fn()
        .mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toHaveLength(0)
    expect(result.watermark()).toBeUndefined()
    expect(result.fileCount()).toBe(0)
  })

  it('given paginated results, when fetching, then yields lines from all pages', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 3,
        done: false,
        nextRecordsUrl: '/next1',
        records: [
          { Id: '001', LastModifiedDate: '2026-01-01T00:00:00.000Z' },
          { Id: '002', LastModifiedDate: '2026-01-02T00:00:00.000Z' },
        ],
      }),
      queryMore: vi.fn().mockResolvedValue({
        totalSize: 3,
        done: true,
        records: [{ Id: '003', LastModifiedDate: '2026-01-03T00:00:00.000Z' }],
      }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'LastModifiedDate'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toHaveLength(3)
    expect(lines[0]).toContain('001')
    expect(lines[2]).toContain('003')
    expect(result.watermark()?.toString()).toBe('2026-01-03T00:00:00.000Z')
    expect(result.fileCount()).toBe(2)
  })

  it('given watermark and where clause, when fetching, then includes both in SOQL', async () => {
    // Arrange
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
      where: 'Industry != null',
    })
    await sut.fetch(Watermark.fromString('2026-01-01T00:00:00.000Z'))

    // Assert
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LastModifiedDate > 2026-01-01T00:00:00.000Z')
    expect(soql).toContain('(Industry != null)')
    expect(soql).toContain(' AND ')
  })

  it('given dateField not in fields, when fetching, then adds dateField to query but not to CSV output', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ Id: '001', LastModifiedDate: '2026-03-01T00:00:00.000Z' }],
      }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain('001')
    expect(lines[0]).not.toContain('LastModifiedDate')
    expect(result.watermark()?.toString()).toBe('2026-03-01T00:00:00.000Z')
  })

  it('given limit, when fetching, then includes LIMIT clause in SOQL', async () => {
    // Arrange
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
      queryLimit: 50,
    })
    await sut.fetch()

    // Assert
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LIMIT 50')
  })

  it('given null field value in record, when fetching, then converts to empty string in CSV', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          {
            Id: '001',
            Name: null,
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
        ],
      }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain('001')
    expect(lines[0]).toContain('""')
  })

  it('given field value with embedded comma and quotes, when fetching, then properly CSV-quoted', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          {
            Id: '001',
            Name: 'O"Brien, Jr.',
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
        ],
      }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain('O""Brien')
  })

  it('given relationship field Owner.Name, when creating reader, then does not throw', () => {
    // Arrange / Act / Assert
    const sfPort = makeSfPort()
    expect(
      () =>
        new SObjectReader(sfPort, {
          sobject: 'Contact',
          fields: ['Id', 'Owner.Name'],
          dateField: 'LastModifiedDate',
        })
    ).not.toThrow()
  })

  it('given relationship traversal field Owner.Name, when fetching, then extracts nested record value', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          {
            Id: '001',
            Owner: { Name: 'Jane Doe' },
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
        ],
      }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Contact',
      fields: ['Id', 'Owner.Name'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain('001')
    expect(lines[0]).toContain('Jane Doe')
  })

  it('given relationship traversal with null intermediate object, when fetching, then outputs empty string for that field', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          {
            Id: '001',
            Owner: null,
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
        ],
      }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Contact',
      fields: ['Id', 'Owner.Name'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain('""')
  })

  it('given relationship traversal field Owner.Name, when header called, then returns dotted name', async () => {
    // Arrange
    const sfPort = makeSfPort()

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Contact',
      fields: ['Id', 'Owner.Name'],
      dateField: 'LastModifiedDate',
    })

    // Assert
    expect(await sut.header()).toBe('Id,Owner.Name')
  })

  it('given invalid sobject name, when creating reader, then throws', () => {
    // Arrange
    const sfPort = makeSfPort()
    const act = () =>
      new SObjectReader(sfPort, {
        sobject: 'bad name!',
        fields: ['Id'],
        dateField: 'LastModifiedDate',
      })

    // Act & Assert
    expect(act).toThrow('Invalid sobject')
  })

  it('given invalid field name, when creating reader, then throws', () => {
    // Arrange
    const sfPort = makeSfPort()
    const act = () =>
      new SObjectReader(sfPort, {
        sobject: 'Account',
        fields: ['bad field!'],
        dateField: 'LastModifiedDate',
      })

    // Act & Assert
    expect(act).toThrow('Invalid field')
  })

  it('given invalid dateField name, when creating reader, then throws', () => {
    // Arrange
    const sfPort = makeSfPort()
    const act = () =>
      new SObjectReader(sfPort, {
        sobject: 'Account',
        fields: ['Id'],
        dateField: 'bad date!',
      })

    // Act & Assert
    expect(act).toThrow('Invalid dateField')
  })

  it('given single page with records, when fetching, then watermark is last record dateField value', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 3,
        done: true,
        records: [
          { Id: '001', LastModifiedDate: '2026-01-01T00:00:00.000Z' },
          { Id: '002', LastModifiedDate: '2026-01-02T00:00:00.000Z' },
          { Id: '003', LastModifiedDate: '2026-01-03T00:00:00.000Z' },
        ],
      }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    await collectLines(result.lines)

    // Assert
    expect(result.watermark()?.toString()).toBe('2026-01-03T00:00:00.000Z')
  })

  it('given fields [Id, Name, CreatedDate], when header() called, then returns comma-separated field names', async () => {
    // Arrange
    const sfPort = makeSfPort()

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name', 'CreatedDate'],
      dateField: 'CreatedDate',
    })

    // Assert
    expect(await sut.header()).toBe('Id,Name,CreatedDate')
  })

  it('given fields [Id, Name], when header() called before fetch(), then is available immediately', async () => {
    // Arrange
    const sfPort = makeSfPort()

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name'],
      dateField: 'CreatedDate',
    })

    // Assert — no fetch() called: header is derived from config
    expect(await sut.header()).toBe('Id,Name')
  })

  it('given no watermark and no where clause, when fetching, then SOQL does not contain WHERE', async () => {
    // Arrange — kills conditions.length > 0 → >= 0 mutation (would add ' WHERE ' with empty conditions)
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    await sut.fetch()

    // Assert
    const soql: string = querySpy.mock.calls[0][0]
    expect(soql).not.toContain('WHERE')
  })

  it('given no queryLimit, when fetching, then SOQL does not contain LIMIT', async () => {
    // Arrange — kills this.queryLimit ? ... : '' → always-truthy mutation
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    await sut.fetch()

    // Assert
    const soql: string = querySpy.mock.calls[0][0]
    expect(soql).not.toContain('LIMIT')
  })

  it('given Account with [Id, Name] fields, when fetching, then SOQL queries Account with those fields ordered by dateField', async () => {
    // Arrange — kills SOQL template string mutations (sobject name, field names, ORDER BY)
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name'],
      dateField: 'LastModifiedDate',
    })
    await sut.fetch()

    // Assert
    const soql: string = querySpy.mock.calls[0][0]
    expect(soql).toContain('SELECT Id, Name')
    expect(soql).toContain('FROM Account')
    expect(soql).toContain('ORDER BY LastModifiedDate ASC')
  })

  it('given dateField already in fields list, when fetching, then SOQL does not duplicate the dateField', async () => {
    // Arrange — kills config.fields.includes(config.dateField) → false mutation (always appends dateField)
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'LastModifiedDate'],
      dateField: 'LastModifiedDate',
    })
    await sut.fetch()

    // Assert — 'LastModifiedDate' should appear once in SELECT (not twice)
    const soql: string = querySpy.mock.calls[0][0]
    const selectPart = soql.split('FROM')[0]
    expect(selectPart.split('LastModifiedDate')).toHaveLength(2)
  })

  it('given relationship field with primitive intermediate value, when fetching, then outputs empty string for that field', async () => {
    // Arrange — kills current === null || typeof current !== 'object' → && mutation:
    // with &&, null check is missed and a primitive (non-null, non-object) won't early-return
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          {
            Id: '001',
            Owner: 'a-string',
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
        ],
      }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Contact',
      fields: ['Id', 'Owner.Name'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert — primitive 'Owner' has no 'Name' → resolveField returns null → empty string
    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain('""')
  })

  it('given relationship field with missing intermediate value, when fetching, then returns empty string not throws', async () => {
    // Arrange — kills L23 ConditionalExpression 'false' (typeof current !== 'object' → false)
    // With mutation: undefined === null || false = false → (undefined)['Name'] → TypeError
    // Original: undefined === null || typeof undefined !== 'object' → true → returns null → ''
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          {
            Id: '001',
            // Owner field absent → record['Owner'] = undefined
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
        ],
      }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Contact',
      fields: ['Id', 'Owner.Name'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert — undefined Owner → resolveField returns null → '' → quoted empty
    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain('""')
  })

  it('given done=true with nextRecordsUrl, when fetching, then does not call queryMore', async () => {
    // Arrange — kills L98 LogicalOperator: !done && nextUrl → !done || nextUrl
    // With ||, done=true but nextUrl present would still trigger queryMore
    const queryMoreSpy = vi.fn()
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        nextRecordsUrl: '/services/data/v62.0/query/next',
        records: [{ Id: '001', LastModifiedDate: '2026-01-01T00:00:00.000Z' }],
      }),
      queryMore: queryMoreSpy,
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    await collectLines(result.lines)

    // Assert — done=true stops pagination even if nextRecordsUrl is present
    expect(queryMoreSpy).not.toHaveBeenCalled()
  })

  it('given no conditions, when fetching, then SOQL has no spurious text before ORDER BY', async () => {
    // Arrange — kills L80:70 StringLiteral: '' (no-WHERE case) → "Stryker was here!"
    // With mutation, whereClause = "Stryker was here!" even when no watermark/where
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    await sut.fetch()

    // Assert — no WHERE clause injected; SOQL goes straight from FROM to ORDER BY
    const soql: string = querySpy.mock.calls[0][0]
    expect(soql).toContain('FROM Account ORDER BY')
  })

  it('given no queryLimit, when fetching, then SOQL has no LIMIT clause', async () => {
    // Arrange — kills L81:73 StringLiteral: '' (no-limit case) → "Stryker was here!"
    // With mutation, limitClause = "Stryker was here!" appended to every SOQL
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    await sut.fetch()

    // Assert — SOQL ends at ORDER BY ... ASC with nothing appended after
    const soql: string = querySpy.mock.calls[0][0]
    expect(soql).toMatch(/ORDER BY LastModifiedDate ASC$/)
  })

  it('given records, when fetching, then CSV lines have no trailing whitespace', async () => {
    // Arrange — kills L106 MethodExpression: .trimEnd() removal
    // csv-stringify adds trailing \n; without trimEnd each line ends with \n
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ Id: '001', LastModifiedDate: '2026-01-01T00:00:00.000Z' }],
      }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert — each yielded line has no trailing whitespace/newline
    expect(lines).toHaveLength(1)
    expect(lines[0]).not.toMatch(/\s$/)
  })

  it('given plain string field value, when fetching, then CSV wraps every field in double quotes', async () => {
    // Arrange — kills L108 BooleanLiteral: quoted: true → quoted: false
    // With quoted: false, plain strings like "Acme" are not force-quoted in CSV
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          {
            Id: '001',
            Name: 'Acme',
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
        ],
      }),
    })

    // Act
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert — plain values are quoted: "001","Acme" (not 001,Acme)
    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain('"001"')
    expect(lines[0]).toContain('"Acme"')
  })

  it.each([
    ['number', 42, '"42"'],
    ['boolean', true, '"true"'],
  ])('given %s field value, when fetching, then coerces via String() and CSV-quotes', async (_name, payload, expected) => {
    // Arrange — kills the typeof === 'string' short-circuit branch
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          {
            Id: '001',
            NumField: payload,
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
        ],
      }),
    })
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'NumField'],
      dateField: 'LastModifiedDate',
    })

    // Act
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(lines[0]).toContain(expected as string)
  })

  it.each([
    ['equals formula', '=HYPERLINK("http://evil","click")'],
    ['plus formula', '+SUM(A1:A10)'],
    ['minus formula', '-2+3'],
    ['at formula', '@SUM(1,2)'],
    ['leading pipe', "|cmd'/c calc'!A0"],
    ['tab-prefixed', '\texisting tab'],
    ['cr-prefixed', '\rcarriage'],
  ])('given field value starting with %s, when fetching, then prefixes with TAB to defuse spreadsheet formula evaluation', async (_name, payload) => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          {
            Id: '001',
            Name: payload,
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
        ],
      }),
    })
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name'],
      dateField: 'LastModifiedDate',
    })

    // Act
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert — field is wrapped in quotes AND prefixed with a TAB (with
    // any embedded " doubled per CSV escaping)
    const expected = `"\t${payload.replaceAll('"', '""')}"`
    expect(lines[0]).toContain(expected)
  })
})

describe('SObjectReader.project', () => {
  it('given project called twice on same reader, when calling, then throws Error', () => {
    // Arrange
    const sut = new SObjectReader(makeSfPort(), {
      sobject: 'Account',
      fields: ['Id', 'Name'],
      dateField: 'LastModifiedDate',
    })
    const layout = {
      targetSize: 2,
      outputIndex: new Int32Array([0, 1]),
      augmentSlots: [],
    }
    sut.project(layout)

    // Act / Assert
    expect(() => sut.project(layout)).toThrow(/project called twice/)
  })

  it('given layout whose outputIndex length differs from reader fields length, when calling project, then throws Error', () => {
    // Arrange
    const sut = new SObjectReader(makeSfPort(), {
      sobject: 'Account',
      fields: ['Id', 'Name'],
      dateField: 'LastModifiedDate',
    })
    const layout = {
      targetSize: 3,
      outputIndex: new Int32Array([0]), // length 1, reader has 2 fields
      augmentSlots: [],
    }

    // Act / Assert
    expect(() => sut.project(layout)).toThrow(/outputIndex length/)
  })
})

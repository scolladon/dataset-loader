import { describe, expect, it, vi } from 'vitest'
import { ElfBootstrapProvider } from '../../../../src/adapters/readers/elf-bootstrap-provider.js'
import type { SalesforcePort } from '../../../../src/ports/types.js'
import { makeSfPort } from '../../../fixtures/sf-port.js'

function portWithHeader(header: string | null): SalesforcePort {
  return makeSfPort({
    query: vi.fn().mockResolvedValue({
      totalSize: header === null ? 0 : 1,
      done: true,
      records: header === null ? [] : [{ LogFileFieldNames: header }],
    }),
  })
}

describe('ElfBootstrapProvider', () => {
  it('given a prior EventLogFile with N columns, when build, then emits N Text fields in source order', async () => {
    // Arrange
    const port = portWithHeader('EVENT_TYPE,TIMESTAMP_DERIVED,USER_ID')
    const sut = new ElfBootstrapProvider(port, {
      eventType: 'Login',
      interval: 'Daily',
      datasetName: 'LoginEvents',
      augmentColumns: {},
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields).toEqual([
      {
        name: 'EVENT_TYPE',
        label: 'EVENT_TYPE',
        type: 'Text',
        origin: 'reader',
      },
      {
        name: 'TIMESTAMP_DERIVED',
        label: 'TIMESTAMP_DERIVED',
        type: 'Text',
        origin: 'reader',
      },
      { name: 'USER_ID', label: 'USER_ID', type: 'Text', origin: 'reader' },
    ])
  })

  it('given augment columns, when build, then appended after reader fields with origin: augment', async () => {
    // Arrange
    const port = portWithHeader('A,B')
    const sut = new ElfBootstrapProvider(port, {
      eventType: 'Login',
      interval: 'Daily',
      datasetName: 'X',
      augmentColumns: { SourceOrgId: '00D000000000001' },
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(
      result.fields.map(f => ({ name: f.name, origin: f.origin }))
    ).toEqual([
      { name: 'A', origin: 'reader' },
      { name: 'B', origin: 'reader' },
      { name: 'SourceOrgId', origin: 'augment' },
    ])
  })

  it('given no prior EventLogFile in the source org, when build, then throws with a clear bootstrap-infeasible message', async () => {
    // Arrange
    const port = portWithHeader(null)
    const sut = new ElfBootstrapProvider(port, {
      eventType: 'Login',
      interval: 'Daily',
      datasetName: 'X',
      augmentColumns: {},
    })

    // Act / Assert — ELF bootstrap depends on a historical log to derive columns
    await expect(sut.buildSourceSchema()).rejects.toThrow(
      /ELF bootstrap requires at least one historical log/
    )
  })

  it('given build is invoked, when querying, then SOQL targets the event-type/interval pair and orders by LogDate DESC', async () => {
    // Arrange
    const port = portWithHeader('A')
    const sut = new ElfBootstrapProvider(port, {
      eventType: 'Login',
      interval: 'Hourly',
      datasetName: 'X',
      augmentColumns: {},
    })

    // Act
    await sut.buildSourceSchema()

    // Assert
    const querySpy = port.query as ReturnType<typeof vi.fn>
    const soql = querySpy.mock.calls[0][0] as string
    expect(soql).toContain("EventType = 'Login'")
    expect(soql).toContain("Interval = 'Hourly'")
    expect(soql).toContain('ORDER BY LogDate DESC')
    expect(soql).toContain('LIMIT 1')
  })

  it('given a header with quoted columns or trailing commas, when build, then parseCsvHeader normalises them', async () => {
    // Arrange — exercises the parseCsvHeader integration
    const port = portWithHeader('"COL_A","COL B","COL_C",')
    const sut = new ElfBootstrapProvider(port, {
      eventType: 'X',
      interval: 'Daily',
      datasetName: 'X',
      augmentColumns: {},
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields.map(f => f.name)).toEqual(['COL_A', 'COL B', 'COL_C'])
  })
})

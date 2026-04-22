import { describe, expect, it, vi } from 'vitest'
import {
  type CsvEntry,
  type ElfEntry,
  type SObjectEntry,
} from '../../../src/adapters/config-loader.js'
import { resolveProvidedFields } from '../../../src/domain/field-resolver.js'
import {
  type ReaderPort,
  type SalesforcePort,
} from '../../../src/ports/types.js'
import { makeSfPort } from '../../fixtures/sf-port.js'

function makeReader(header = ''): ReaderPort {
  return {
    header: vi.fn().mockResolvedValue(header),
    fetch: vi.fn(),
  }
}

const sobjectEntry: SObjectEntry = {
  sourceOrg: 'src',
  targetOrg: 'ana',
  targetDataset: 'DS',
  operation: 'Append',
  sObject: 'Account',
  fields: ['Id', 'Name'],
  dateField: 'LastModifiedDate',
}

const elfEntry: ElfEntry = {
  sourceOrg: 'src',
  targetOrg: 'ana',
  targetDataset: 'DS',
  operation: 'Append',
  eventLog: 'Login',
  interval: 'Daily',
}

const csvEntry: CsvEntry = {
  targetOrg: 'ana',
  targetDataset: 'DS',
  operation: 'Append',
  csvFile: './fake.csv',
}

describe('resolveProvidedFields', () => {
  it('given SObject entry, when resolving, then returns config-declared fields verbatim (no port call)', async () => {
    // Arrange
    const reader = makeReader()
    const sfPorts = new Map<string, SalesforcePort>([['src', makeSfPort()]])

    // Act
    const result = await resolveProvidedFields(sobjectEntry, reader, sfPorts)

    // Assert
    expect(result).toEqual(['Id', 'Name'])
    expect(reader.header).not.toHaveBeenCalled()
  })

  it('given CSV entry, when resolving, then returns parsed header from fetcher', async () => {
    // Arrange
    const reader = makeReader('Col1,Col2,Col3')
    const sfPorts = new Map<string, SalesforcePort>()

    // Act
    const result = await resolveProvidedFields(csvEntry, reader, sfPorts)

    // Assert
    expect(result).toEqual(['Col1', 'Col2', 'Col3'])
  })

  it('given ELF entry with valid LogFileFieldNames query, when resolving, then parses the header string', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ LogFileFieldNames: 'TIMESTAMP,USER_ID,URI' }],
      }),
    })
    const sfPorts = new Map<string, SalesforcePort>([['src', sfPort]])

    // Act
    const result = await resolveProvidedFields(elfEntry, makeReader(), sfPorts)

    // Assert
    expect(result).toEqual(['TIMESTAMP', 'USER_ID', 'URI'])
  })

  it('given ELF entry with no records, when resolving, then returns empty array', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi
        .fn()
        .mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    })
    const sfPorts = new Map<string, SalesforcePort>([['src', sfPort]])

    // Act
    const result = await resolveProvidedFields(elfEntry, makeReader(), sfPorts)

    // Assert
    expect(result).toEqual([])
  })

  it('given ELF entry with null LogFileFieldNames, when resolving, then returns empty array', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ LogFileFieldNames: null }],
      }),
    })
    const sfPorts = new Map<string, SalesforcePort>([['src', sfPort]])

    // Act
    const result = await resolveProvidedFields(elfEntry, makeReader(), sfPorts)

    // Assert
    expect(result).toEqual([])
  })

  it('given ELF entry when query throws, then swallows error and returns empty array', async () => {
    // Arrange — audit is the authoritative place to surface permission / connectivity issues
    const sfPort = makeSfPort({
      query: vi.fn().mockRejectedValue(new Error('permission denied')),
    })
    const sfPorts = new Map<string, SalesforcePort>([['src', sfPort]])

    // Act
    const result = await resolveProvidedFields(elfEntry, makeReader(), sfPorts)

    // Assert
    expect(result).toEqual([])
  })

  it('given ELF entry, when resolving, then SOQL includes eventLog, interval, and the DESC LIMIT 1 bootstrap', async () => {
    // Arrange — regression guard on the pre-existing bootstrap query shape
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })
    const sfPorts = new Map<string, SalesforcePort>([['src', sfPort]])

    // Act
    await resolveProvidedFields(elfEntry, makeReader(), sfPorts)

    // Assert
    const soql: string = querySpy.mock.calls[0][0]
    expect(soql).toContain("EventType = 'Login'")
    expect(soql).toContain("Interval = 'Daily'")
    expect(soql).toContain('ORDER BY LogDate DESC LIMIT 1')
  })
})

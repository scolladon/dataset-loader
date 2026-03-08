import { describe, it, expect, vi, beforeEach } from 'vitest'
import { loadConfig } from '../../src/core/config-loader.js'
import { type SfClient } from '../../src/core/sf-client.js'
import * as fs from 'node:fs/promises'

vi.mock('node:fs/promises')

function makeClient(orgInfo = { Id: '00D000000000001', Name: 'TestOrg' }): SfClient {
  return {
    query: vi.fn().mockResolvedValue({ totalSize: 1, done: true, records: [orgInfo] }),
  } as unknown as SfClient
}

const validElfConfig = {
  entries: [
    {
      type: 'elf',
      sourceOrg: 'source',
      analyticOrg: 'analytic',
      dataset: 'DS_ELF',
      eventType: 'LightningPageView',
      interval: 'Daily',
    },
  ],
}

const validSObjectConfig = {
  entries: [
    {
      type: 'sobject',
      sourceOrg: 'source',
      analyticOrg: 'analytic',
      dataset: 'DS_Account',
      sobject: 'Account',
      fields: ['Id', 'Name'],
    },
  ],
}

describe('ConfigLoader', () => {
  beforeEach(() => {
    vi.restoreAllMocks()
  })

  describe('schema validation', () => {
    it('given valid ELF config, when loading, then returns resolved entries', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(validElfConfig))
      const clients = new Map<string, SfClient>()

      // Act
      const sut = await loadConfig('config.json', clients)

      // Assert
      expect(sut).toHaveLength(1)
      expect(sut[0].entry.type).toBe('elf')
      expect(sut[0].index).toBe(0)
    })

    it('given valid SObject config, when loading, then returns resolved entries with default dateField', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(validSObjectConfig))
      const clients = new Map<string, SfClient>()

      // Act
      const sut = await loadConfig('config.json', clients)

      // Assert
      expect(sut).toHaveLength(1)
      const entry = sut[0].entry
      expect(entry.type).toBe('sobject')
      if (entry.type === 'sobject') {
        expect(entry.dateField).toBe('LastModifiedDate')
      }
    })

    it('given empty entries, when loading, then throws validation error', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify({ entries: [] }))

      // Act & Assert
      await expect(loadConfig('config.json', new Map())).rejects.toThrow()
    })

    it('given missing required fields, when loading, then throws validation error', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify({ entries: [{ type: 'elf' }] }))

      // Act & Assert
      await expect(loadConfig('config.json', new Map())).rejects.toThrow()
    })
  })

  describe('operation consistency', () => {
    it('given entries with conflicting operations on same dataset, when loading, then throws error', async () => {
      // Arrange
      const config = {
        entries: [
          { type: 'elf', sourceOrg: 'src', analyticOrg: 'ana', dataset: 'DS', eventType: 'E1', interval: 'Daily', operation: 'Append' },
          { type: 'elf', sourceOrg: 'src', analyticOrg: 'ana', dataset: 'DS', eventType: 'E2', interval: 'Daily', operation: 'Overwrite' },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(loadConfig('config.json', new Map())).rejects.toThrow(/conflicting operations/)
    })

    it('given entries with same operation on same dataset, when loading, then succeeds', async () => {
      // Arrange
      const config = {
        entries: [
          { type: 'elf', sourceOrg: 'src', analyticOrg: 'ana', dataset: 'DS', eventType: 'E1', interval: 'Daily' },
          { type: 'elf', sourceOrg: 'src', analyticOrg: 'ana', dataset: 'DS', eventType: 'E2', interval: 'Daily' },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act
      const sut = await loadConfig('config.json', new Map())

      // Assert
      expect(sut).toHaveLength(2)
    })
  })

  describe('augment column resolution', () => {
    it('given dynamic expressions, when loading, then resolves org info', async () => {
      // Arrange
      const config = {
        entries: [
          {
            type: 'elf',
            sourceOrg: 'source',
            analyticOrg: 'analytic',
            dataset: 'DS',
            eventType: 'E1',
            interval: 'Daily',
            augmentColumns: {
              OrgId: '$sourceOrg.Id',
              OrgName: '$sourceOrg.Name',
              AnalyticId: '$analyticOrg.Id',
              Env: 'Production',
            },
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))
      const sourceClient = makeClient({ Id: '00Dsrc', Name: 'SourceOrg' })
      const analyticClient = makeClient({ Id: '00Dana', Name: 'AnalyticOrg' })
      const clients = new Map<string, SfClient>([
        ['source', sourceClient],
        ['analytic', analyticClient],
      ])

      // Act
      const sut = await loadConfig('config.json', clients)

      // Assert
      expect(sut[0].augmentColumns).toEqual({
        OrgId: '00Dsrc',
        OrgName: 'SourceOrg',
        AnalyticId: '00Dana',
        Env: 'Production',
      })
    })

    it('given no augment columns, when loading, then returns empty object', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(validElfConfig))

      // Act
      const sut = await loadConfig('config.json', new Map())

      // Assert
      expect(sut[0].augmentColumns).toEqual({})
    })

    it('given dynamic expression but missing client, when loading, then throws error', async () => {
      // Arrange
      const config = {
        entries: [
          {
            type: 'elf',
            sourceOrg: 'source',
            analyticOrg: 'analytic',
            dataset: 'DS',
            eventType: 'E1',
            interval: 'Daily',
            augmentColumns: { OrgId: '$sourceOrg.Id' },
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(loadConfig('config.json', new Map())).rejects.toThrow(/No authenticated connection/)
    })
  })
})

import * as fs from 'node:fs/promises'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  entryLabel,
  parseConfig,
  type ResolvedEntry,
  resolveConfig,
} from '../../../src/adapters/config-loader.js'
import { type SalesforcePort } from '../../../src/ports/types.js'

vi.mock('node:fs/promises')

async function loadConfig(
  configPath: string,
  sfPorts: ReadonlyMap<string, SalesforcePort>
): Promise<ResolvedEntry[]> {
  const config = await parseConfig(configPath)
  return resolveConfig(config, sfPorts)
}

function makeSfPort(
  orgInfo = { Id: '00D000000000001', Name: 'TestOrg' }
): SalesforcePort {
  return {
    query: vi
      .fn()
      .mockResolvedValue({ totalSize: 1, done: true, records: [orgInfo] }),
    queryMore: vi.fn(),
    getBlob: vi.fn(),
    getBlobStream: vi.fn(),
    post: vi.fn(),
    patch: vi.fn(),
    del: vi.fn(),
    apiVersion: '62.0',
  }
}

const validElfConfig = {
  entries: [
    {
      type: 'elf',
      sourceOrg: 'source',
      targetOrg: 'analytic',
      targetDataset: 'DS_ELF',
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
      targetOrg: 'analytic',
      targetDataset: 'DS_Account',
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
      const sfPorts = new Map<string, SalesforcePort>()

      // Act
      const sut = await loadConfig('config.json', sfPorts)

      // Assert
      expect(sut).toHaveLength(1)
      expect(sut[0].entry.type).toBe('elf')
      expect(sut[0].index).toBe(0)
    })

    it('given valid SObject config, when loading, then returns resolved entries with default dateField', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify(validSObjectConfig)
      )
      const sfPorts = new Map<string, SalesforcePort>()

      // Act
      const sut = await loadConfig('config.json', sfPorts)

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
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({ entries: [{ type: 'elf' }] })
      )

      // Act & Assert
      await expect(loadConfig('config.json', new Map())).rejects.toThrow()
    })

    it('given invalid orgAlias with colons, when parsing, then rejects', async () => {
      // Arrange
      const config = {
        entries: [
          {
            type: 'elf',
            sourceOrg: 'src:org',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventType: 'Login',
            interval: 'Daily',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow()
    })

    it('given invalid sfIdentifier starting with number, when parsing, then rejects', async () => {
      // Arrange
      const config = {
        entries: [
          {
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: '1Field',
            eventType: 'Login',
            interval: 'Daily',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow()
    })

    it('given ELF config with name field, when parsing, then preserves name in entry', async () => {
      // Arrange
      const config = {
        entries: [
          {
            name: 'login-events',
            type: 'elf',
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventType: 'Login',
            interval: 'Daily',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect(sut.entries[0]).toHaveProperty('name', 'login-events')
    })

    it('given config with invalid name characters, when parsing, then throws validation error', async () => {
      // Arrange
      const config = {
        entries: [
          {
            name: 'has spaces!',
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventType: 'E',
            interval: 'Daily',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow()
    })

    it('given invalid interval value, when parsing, then rejects', async () => {
      // Arrange
      const config = {
        entries: [
          {
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventType: 'Login',
            interval: 'Weekly',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow()
    })

    it('given sobject entry with where and limit, when parsing, then accepts', async () => {
      // Arrange
      const config = {
        entries: [
          {
            type: 'sobject',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            sobject: 'Account',
            fields: ['Id', 'Name'],
            where: "Industry = 'Tech'",
            limit: 100,
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect(sut.entries).toHaveLength(1)
      const entry = sut.entries[0]
      expect(entry.type).toBe('sobject')
      if (entry.type === 'sobject') {
        expect(entry.where).toBe("Industry = 'Tech'")
        expect(entry.limit).toBe(100)
      }
    })
  })

  describe('operation consistency', () => {
    it('given entries with conflicting operations on same dataset, when loading, then throws error', async () => {
      // Arrange
      const config = {
        entries: [
          {
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventType: 'E1',
            interval: 'Daily',
            operation: 'Append',
          },
          {
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventType: 'E2',
            interval: 'Daily',
            operation: 'Overwrite',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(loadConfig('config.json', new Map())).rejects.toThrow(
        /conflicting operations/
      )
    })

    it('given entries with same operation on same dataset, when loading, then succeeds', async () => {
      // Arrange
      const config = {
        entries: [
          {
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventType: 'E1',
            interval: 'Daily',
          },
          {
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventType: 'E2',
            interval: 'Daily',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act
      const sut = await loadConfig('config.json', new Map())

      // Assert
      expect(sut).toHaveLength(2)
    })
  })

  describe('name uniqueness', () => {
    it('given entries with duplicate names, when parsing, then throws error with indices', async () => {
      // Arrange
      const config = {
        entries: [
          {
            name: 'my-entry',
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS1',
            eventType: 'E1',
            interval: 'Daily',
          },
          {
            name: 'other',
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS2',
            eventType: 'E2',
            interval: 'Daily',
          },
          {
            name: 'my-entry',
            type: 'sobject',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS3',
            sobject: 'Account',
            fields: ['Id'],
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        /Duplicate entry name 'my-entry'/
      )
    })

    it('given entries with unique names, when parsing, then succeeds', async () => {
      // Arrange
      const config = {
        entries: [
          {
            name: 'entry-a',
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS1',
            eventType: 'E1',
            interval: 'Daily',
          },
          {
            name: 'entry-b',
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS2',
            eventType: 'E2',
            interval: 'Daily',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect(sut.entries).toHaveLength(2)
    })

    it('given entries without names, when parsing, then succeeds without uniqueness check', async () => {
      // Arrange
      const config = {
        entries: [
          {
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS1',
            eventType: 'E1',
            interval: 'Daily',
          },
          {
            type: 'elf',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS2',
            eventType: 'E2',
            interval: 'Daily',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect(sut.entries).toHaveLength(2)
    })
  })

  describe('Given file-target entry (no targetOrg)', () => {
    describe('When parsing valid config', () => {
      it('Then parses successfully with file path as targetFile', async () => {
        // Arrange
        const config = {
          entries: [
            {
              type: 'elf',
              sourceOrg: 'src-org',
              eventType: 'Login',
              interval: 'Daily',
              targetFile: './output/login.csv',
            },
          ],
        }
        vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

        // Act & Assert
        await expect(parseConfig('config.json')).resolves.not.toThrow()
      })
    })

    describe('When augmentColumns reference $targetOrg.*', () => {
      it('Then throws validation error', async () => {
        // Arrange
        const config = {
          entries: [
            {
              type: 'elf',
              sourceOrg: 'src-org',
              eventType: 'Login',
              interval: 'Daily',
              targetFile: './output/login.csv',
              augmentColumns: { Org: '$targetOrg.Id' },
            },
          ],
        }
        vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

        // Act & Assert
        await expect(parseConfig('config.json')).rejects.toThrow(/\$targetOrg/)
      })
    })
  })

  describe('Given org-target entry (targetOrg present)', () => {
    describe('When targetDataset uses SF identifier', () => {
      it('Then parses successfully', async () => {
        // Arrange
        const config = {
          entries: [
            {
              type: 'elf',
              sourceOrg: 'src-org',
              targetOrg: 'my-org',
              targetDataset: 'LoginEvents',
              eventType: 'Login',
              interval: 'Daily',
            },
          ],
        }
        vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

        // Act & Assert
        await expect(parseConfig('config.json')).resolves.not.toThrow()
      })
    })

    describe('When targetOrg set but targetDataset missing', () => {
      it('Then throws validation error', async () => {
        // Arrange
        const config = {
          entries: [
            {
              type: 'elf',
              sourceOrg: 'src-org',
              targetOrg: 'my-org',
              eventType: 'Login',
              interval: 'Daily',
            },
          ],
        }
        vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

        // Act & Assert
        await expect(parseConfig('config.json')).rejects.toThrow()
      })
    })

    describe('When neither targetOrg nor targetFile set', () => {
      it('Then throws validation error', async () => {
        // Arrange
        const config = {
          entries: [
            {
              type: 'elf',
              sourceOrg: 'src-org',
              eventType: 'Login',
              interval: 'Daily',
            },
          ],
        }
        vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

        // Act & Assert
        await expect(parseConfig('config.json')).rejects.toThrow()
      })
    })
  })

  describe('CSV entry', () => {
    it('given valid CSV config with file target, when parsing, then returns csv entry', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              type: 'csv',
              sourceFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
            },
          ],
        })
      )

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect(sut.entries).toHaveLength(1)
      expect(sut.entries[0].type).toBe('csv')
      if (sut.entries[0].type === 'csv') {
        expect(sut.entries[0].sourceFile).toBe('./data/login-events.csv')
        expect(sut.entries[0].operation).toBe('Append')
      }
    })

    it('given CSV config with CRMA target, when parsing, then accepts targetOrg', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              type: 'csv',
              sourceFile: './data/login-events.csv',
              targetDataset: 'LoginEvents',
              targetOrg: 'my-org',
              operation: 'Overwrite',
            },
          ],
        })
      )

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect(sut.entries[0].type).toBe('csv')
      if (sut.entries[0].type === 'csv') {
        expect(sut.entries[0].targetOrg).toBe('my-org')
        expect(sut.entries[0].operation).toBe('Overwrite')
      }
    })

    it('given CSV config with empty sourceFile, when parsing, then rejects', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            { type: 'csv', sourceFile: '', targetFile: './out/data.csv' },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow()
    })

    it('given CSV config with $sourceOrg.Id in augmentColumns, when parsing, then rejects', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              type: 'csv',
              sourceFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              augmentColumns: { OrgId: '$sourceOrg.Id' },
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow()
    })

    it('given CSV config with $targetOrg.Name in augmentColumns, when parsing, then rejects', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              type: 'csv',
              sourceFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              augmentColumns: { OrgName: '$targetOrg.Name' },
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow()
    })

    it('given CSV config with literal augmentColumn value, when parsing, then accepts it', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              type: 'csv',
              sourceFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              augmentColumns: { Source: 'manual-export' },
            },
          ],
        })
      )

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect(sut.entries[0].type).toBe('csv')
    })

    it('given CSV config with targetOrg but invalid targetDataset, when parsing, then rejects', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              type: 'csv',
              sourceFile: './data/login-events.csv',
              targetOrg: 'my-org',
              targetDataset: './out/login-events.csv',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow()
    })

    it('given CSV config with absolute sourceFile, when parsing, then accepts', async () => {
      // Arrange — absolute paths are valid for CLI tools
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              type: 'csv',
              sourceFile: '/tmp/data.csv',
              targetFile: './out/data.csv',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).resolves.toBeDefined()
    })

    it('given CSV config with path traversal in sourceFile, when parsing, then rejects', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              type: 'csv',
              sourceFile: '../../etc/passwd',
              targetFile: './out/data.csv',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow()
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
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventType: 'E1',
            interval: 'Daily',
            augmentColumns: {
              OrgId: '$sourceOrg.Id',
              OrgName: '$sourceOrg.Name',
              AnalyticId: '$targetOrg.Id',
              Env: 'Production',
            },
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))
      const sfPorts = new Map<string, SalesforcePort>([
        ['source', makeSfPort({ Id: '00Dsrc', Name: 'SourceOrg' })],
        ['analytic', makeSfPort({ Id: '00Dana', Name: 'AnalyticOrg' })],
      ])

      // Act
      const sut = await loadConfig('config.json', sfPorts)

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

    it('given targetOrg Name expression, when loading, then resolves to target org name', async () => {
      // Arrange
      const config = {
        entries: [
          {
            type: 'elf',
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventType: 'E1',
            interval: 'Daily',
            augmentColumns: {
              TargetName: '$targetOrg.Name',
            },
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))
      const sfPorts = new Map<string, SalesforcePort>([
        ['analytic', makeSfPort({ Id: '00Dana', Name: 'AnalyticOrg' })],
      ])

      // Act
      const sut = await loadConfig('config.json', sfPorts)

      // Assert
      expect(sut[0].augmentColumns).toEqual({ TargetName: 'AnalyticOrg' })
    })

    it('given org query returns no records, when loading, then throws error', async () => {
      // Arrange
      const config = {
        entries: [
          {
            type: 'elf',
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventType: 'E1',
            interval: 'Daily',
            augmentColumns: { OrgId: '$sourceOrg.Id' },
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))
      const emptyPort = makeSfPort()
      vi.mocked(emptyPort.query).mockResolvedValue({
        totalSize: 0,
        done: true,
        records: [],
      })
      const sfPorts = new Map<string, SalesforcePort>([['source', emptyPort]])

      // Act & Assert
      await expect(loadConfig('config.json', sfPorts)).rejects.toThrow(
        /Organization query returned no records/
      )
    })

    it('given dynamic expression but missing client, when loading, then throws error', async () => {
      // Arrange
      const config = {
        entries: [
          {
            type: 'elf',
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventType: 'E1',
            interval: 'Daily',
            augmentColumns: { OrgId: '$sourceOrg.Id' },
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(loadConfig('config.json', new Map())).rejects.toThrow(
        /No authenticated connection/
      )
    })
  })

  describe('CSV resolving', () => {
    it('given CSV entry without targetOrg, when resolving config, then does not query any org', async () => {
      // Arrange
      const sfPort = makeSfPort()
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              type: 'csv',
              sourceFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              augmentColumns: { Source: 'manual' },
            },
          ],
        })
      )
      const sfPorts = new Map([['some-org', sfPort]])

      // Act
      const sut = await loadConfig('config.json', sfPorts)

      // Assert — no SF org was queried (no sourceOrg, no $targetOrg.* expressions)
      expect(sfPort.query).not.toHaveBeenCalled()
      expect(sut[0].augmentColumns).toEqual({ Source: 'manual' })
    })

    it('given CSV entry, when resolving, then augmentColumns are literal values (no dynamic resolution)', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              type: 'csv',
              sourceFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              augmentColumns: { Tag: 'batch-2026' },
            },
          ],
        })
      )

      // Act
      const sut = await loadConfig('config.json', new Map())

      // Assert
      expect(sut[0].augmentColumns).toEqual({ Tag: 'batch-2026' })
    })
  })

  describe('entryLabel', () => {
    it('given CSV entry without name, when getting label, then returns csv-prefixed sourceFile', () => {
      expect(
        entryLabel({
          type: 'csv',
          sourceFile: './data/login.csv',
          targetFile: './out/login.csv',
          operation: 'Append',
        })
      ).toBe('csv:./data/login.csv')
    })

    it('given CSV entry with name, when getting label, then returns name', () => {
      expect(
        entryLabel({
          type: 'csv',
          sourceFile: './data/login.csv',
          targetFile: './out/login.csv',
          operation: 'Append',
          name: 'login-data',
        })
      ).toBe('login-data')
    })

    it('given ELF entry without name, when getting label, then returns elf-prefixed eventType', () => {
      expect(
        entryLabel({
          type: 'elf',
          sourceOrg: 'prod',
          eventType: 'Login',
          interval: 'Daily',
          targetOrg: 'my-org',
          targetDataset: 'LoginEvents',
          operation: 'Append',
        })
      ).toBe('elf:Login')
    })

    it('given SObject entry without name, when getting label, then returns sobject-prefixed sobject', () => {
      expect(
        entryLabel({
          type: 'sobject',
          sourceOrg: 'prod',
          sobject: 'Account',
          fields: ['Id'],
          dateField: 'LastModifiedDate',
          targetOrg: 'my-org',
          targetDataset: 'Accounts',
          operation: 'Append',
        })
      ).toBe('sobject:Account')
    })
  })
})

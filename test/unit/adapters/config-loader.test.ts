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
      sourceOrg: 'source',
      targetOrg: 'analytic',
      targetDataset: 'DS_ELF',
      eventLog: 'LightningPageView',
      interval: 'Daily',
    },
  ],
}

const validSObjectConfig = {
  entries: [
    {
      sourceOrg: 'source',
      targetOrg: 'analytic',
      targetDataset: 'DS_Account',
      sObject: 'Account',
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
      expect('eventLog' in sut[0].entry).toBe(true)
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
      expect('sObject' in entry).toBe(true)
      if ('sObject' in entry) {
        expect(entry.dateField).toBe('LastModifiedDate')
      }
    })

    it('given empty entries, when loading, then throws validation error', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify({ entries: [] }))

      // Act & Assert
      await expect(loadConfig('config.json', new Map())).rejects.toThrow(
        /Too small/
      )
    })

    it('given missing required fields, when loading, then throws validation error', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({ entries: [{ eventLog: 'Login' }] })
      )

      // Act & Assert
      await expect(loadConfig('config.json', new Map())).rejects.toThrow(
        /Invalid input/
      )
    })

    it('given sobject entry with relationship traversal field Owner.Name, when parsing, then accepts the config', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS_Account',
            sObject: 'Contact',
            fields: ['Id', 'Owner.Name', 'Account.Type'],
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).resolves.toMatchObject({
        entries: [{ sObject: 'Contact' }],
      })
    })

    it('given sobject entry with invalid SOQL field path, when parsing, then rejects with field path message', async () => {
      // Arrange — kills L72: soqlRelationshipPath error message '' vs specific text
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              sourceOrg: 'source',
              targetOrg: 'analytic',
              targetDataset: 'DS_Account',
              sObject: 'Account',
              fields: ['@invalid!field'],
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'Must be a valid SOQL field or relationship path'
      )
    })

    it('given invalid orgAlias with colons, when parsing, then rejects', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'src:org',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Daily',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'Must be a valid org alias'
      )
    })

    it('given invalid sfIdentifier starting with number, when parsing, then rejects', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: '1Field',
            eventLog: 'Login',
            interval: 'Daily',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'Must be a valid Salesforce identifier'
      )
    })

    it('given ELF config with name field, when parsing, then preserves name in entry', async () => {
      // Arrange
      const config = {
        entries: [
          {
            name: 'login-events',
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventLog: 'Login',
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
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'E',
            interval: 'Daily',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'Must be alphanumeric'
      )
    })

    it('given invalid interval value, when parsing, then rejects', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Weekly',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(/Invalid option/)
    })

    it('given sobject entry with where and limit, when parsing, then accepts', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            sObject: 'Account',
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
      expect('sObject' in entry).toBe(true)
      if ('sObject' in entry) {
        expect(entry.where).toBe("Industry = 'Tech'")
        expect(entry.limit).toBe(100)
      }
    })

    it('given entry with multiple discriminator fields, when loading, then throws ambiguity error', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Daily',
            sObject: 'Account',
            fields: ['Id'],
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act / Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'found: eventLog, sObject'
      )
    })

    it('given entry with all three discriminator fields, when loading, then throws ambiguity error', async () => {
      // Arrange — kills present.length > 1 → present.length === 2 mutation
      const config = {
        entries: [
          {
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Daily',
            sObject: 'Account',
            fields: ['Id'],
            csvFile: './data.csv',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act / Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'found: eventLog, sObject, csvFile'
      )
    })

    it('given null entry, when loading, then throws validation error', async () => {
      // Arrange — kills val === null guard
      const config = { entries: [null] }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act / Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'Entry must be an object'
      )
    })

    it('given array entry instead of object, when loading, then throws validation error', async () => {
      // Arrange — kills Array.isArray guard
      const config = { entries: [['a', 'b']] }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act / Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'Entry must be an object'
      )
    })

    it('given entry with no discriminating field, when loading, then throws descriptive error', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act / Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'Entry must have one of: eventLog (ELF), sObject (SObject), or csvFile (CSV)'
      )
    })

    it('given non-object entry, when loading, then throws validation error', async () => {
      // Arrange
      const config = { entries: ['not-an-object'] }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act / Assert
      await expect(parseConfig('config.json')).rejects.toThrow()
    })
  })

  describe('operation consistency', () => {
    it('given entries with conflicting operations on same dataset, when loading, then throws error', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'E1',
            interval: 'Daily',
            operation: 'Append',
          },
          {
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'E2',
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
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'E1',
            interval: 'Daily',
          },
          {
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'E2',
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
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS1',
            eventLog: 'E1',
            interval: 'Daily',
          },
          {
            name: 'other',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS2',
            eventLog: 'E2',
            interval: 'Daily',
          },
          {
            name: 'my-entry',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS3',
            sObject: 'Account',
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
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS1',
            eventLog: 'E1',
            interval: 'Daily',
          },
          {
            name: 'entry-b',
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS2',
            eventLog: 'E2',
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
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS1',
            eventLog: 'E1',
            interval: 'Daily',
          },
          {
            sourceOrg: 'src',
            targetOrg: 'ana',
            targetDataset: 'DS2',
            eventLog: 'E2',
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

  it('given file-target entry (no targetOrg), when parsing valid config, then parses successfully with file path as targetFile', async () => {
    // Arrange
    const config = {
      entries: [
        {
          sourceOrg: 'src-org',
          eventLog: 'Login',
          interval: 'Daily',
          targetFile: './output/login.csv',
        },
      ],
    }
    vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

    // Act & Assert
    await expect(parseConfig('config.json')).resolves.toMatchObject({
      entries: [{ eventLog: 'Login', targetFile: './output/login.csv' }],
    })
  })

  it('given elf entry without operation, when parsing, then defaults to Append', async () => {
    // Arrange — kills L130: operation default 'Append' → ''
    vi.mocked(fs.readFile).mockResolvedValue(
      JSON.stringify({
        entries: [
          {
            sourceOrg: 'src-org',
            targetOrg: 'my-org',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Daily',
          },
        ],
      })
    )

    // Act
    const sut = await parseConfig('config.json')

    // Assert
    expect(sut.entries[0].operation).toBe('Append')
  })

  it('given elf entry with Hourly interval, when parsing, then accepts it', async () => {
    // Arrange — kills L151: enum ['Daily', ''] would reject 'Hourly'
    vi.mocked(fs.readFile).mockResolvedValue(
      JSON.stringify({
        entries: [
          {
            sourceOrg: 'src-org',
            targetOrg: 'my-org',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Hourly',
          },
        ],
      })
    )

    // Act
    const sut = await parseConfig('config.json')

    // Assert
    expect(sut.entries[0]).toMatchObject({ interval: 'Hourly' })
  })

  it('given file-target entry with plain augmentColumns, when parsing, then accepts without mustache error', async () => {
    // Arrange — kills L137: if (true) always validates MUSTACHE_TARGETORG, even for plain values
    vi.mocked(fs.readFile).mockResolvedValue(
      JSON.stringify({
        entries: [
          {
            sourceOrg: 'src-org',
            eventLog: 'Login',
            interval: 'Daily',
            targetFile: './output/login.csv',
            augmentColumns: { OrgName: 'StaticValue' },
          },
        ],
      })
    )

    // Act
    const sut = await parseConfig('config.json')

    // Assert — with L137 mutation, the plain value triggers a ZodError
    expect(sut.entries).toHaveLength(1)
  })

  it('given elf entry {{targetOrg.Id}} in augmentColumns when no targetOrg, when parsing, then error path starts with augmentColumns', async () => {
    // Arrange — kills L141 path: ['augmentColumns', key] → []
    vi.mocked(fs.readFile).mockResolvedValue(
      JSON.stringify({
        entries: [
          {
            sourceOrg: 'src-org',
            eventLog: 'Login',
            interval: 'Daily',
            targetFile: './output/login.csv',
            augmentColumns: { OrgId: '{{targetOrg.Id}}' },
          },
        ],
      })
    )

    // Act
    const error = await parseConfig('config.json').catch((e: unknown) => e)

    // Assert
    expect(
      (error as { issues: Array<{ path: string[] }> }).issues.some(i =>
        i.path.includes('augmentColumns')
      )
    ).toBe(true)
  })

  it('given augmentColumns key with dot notation, when parsing ELF entry, then accepts it', async () => {
    // Arrange
    vi.mocked(fs.readFile).mockResolvedValue(
      JSON.stringify({
        entries: [
          {
            sourceOrg: 'src-org',
            targetOrg: 'target-org',
            targetDataset: 'MyDataset',
            eventLog: 'Login',
            interval: 'Daily',
            augmentColumns: { 'Org.Name': 'MyOrg' },
          },
        ],
      })
    )

    // Act
    const sut = await parseConfig('config.json')

    // Assert
    expect('eventLog' in sut.entries[0]).toBe(true)
  })

  it('given {{targetOrg.Id}} in augmentColumns when no targetOrg, when parsing, then rejects', async () => {
    // Arrange
    vi.mocked(fs.readFile).mockResolvedValue(
      JSON.stringify({
        entries: [
          {
            sourceOrg: 'src-org',
            eventLog: 'Login',
            interval: 'Daily',
            targetFile: './output/login.csv',
            augmentColumns: { Org: '{{targetOrg.Id}}' },
          },
        ],
      })
    )

    // Act & Assert
    await expect(parseConfig('config.json')).rejects.toThrow(/targetOrg/)
  })

  it('given org-target entry with valid SF identifier as targetDataset, when parsing, then parses successfully', async () => {
    // Arrange
    const config = {
      entries: [
        {
          sourceOrg: 'src-org',
          targetOrg: 'my-org',
          targetDataset: 'LoginEvents',
          eventLog: 'Login',
          interval: 'Daily',
        },
      ],
    }
    vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

    // Act & Assert
    await expect(parseConfig('config.json')).resolves.toMatchObject({
      entries: [{ eventLog: 'Login', targetDataset: 'LoginEvents' }],
    })
  })

  it('given org-target entry with targetOrg but missing targetDataset, when parsing, then throws validation error', async () => {
    // Arrange
    const config = {
      entries: [
        {
          sourceOrg: 'src-org',
          targetOrg: 'my-org',
          eventLog: 'Login',
          interval: 'Daily',
        },
      ],
    }
    vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

    // Act & Assert
    await expect(parseConfig('config.json')).rejects.toThrow(
      'targetDataset is required when targetOrg is set'
    )
  })

  it('given org-target entry with both targetOrg and targetFile, when parsing, then throws validation error', async () => {
    // Arrange
    const config = {
      entries: [
        {
          sourceOrg: 'src-org',
          targetOrg: 'my-org',
          targetFile: './output.csv',
          targetDataset: 'DS',
          eventLog: 'Login',
          interval: 'Daily',
        },
      ],
    }
    vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

    // Act & Assert
    await expect(parseConfig('config.json')).rejects.toThrow(
      /Cannot specify both/
    )
  })

  it('given org-target entry with targetFile and targetDataset but no targetOrg, when parsing, then throws validation error', async () => {
    // Arrange
    const config = {
      entries: [
        {
          sourceOrg: 'src-org',
          targetFile: './output.csv',
          targetDataset: 'DS',
          eventLog: 'Login',
          interval: 'Daily',
        },
      ],
    }
    vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

    // Act & Assert — kills L117: message '' still contains 'targetDataset' via path in ZodError JSON,
    // but the exact message text is absent
    await expect(parseConfig('config.json')).rejects.toThrow(
      'targetDataset requires targetOrg to be set'
    )
  })

  it('given org-target entry with targetOrg but no targetDataset, when parsing, then error path includes targetDataset', async () => {
    // Arrange
    vi.mocked(fs.readFile).mockResolvedValue(
      JSON.stringify({
        entries: [
          {
            sourceOrg: 'src-org',
            targetOrg: 'my-org',
            eventLog: 'Login',
            interval: 'Daily',
          },
        ],
      })
    )

    // Act — kills L111 path: ['targetDataset'] → []: path mutation keeps message but drops the path field
    const error = await parseConfig('config.json').catch((e: unknown) => e)

    // Assert
    expect(
      (error as { issues: Array<{ path: string[] }> }).issues.some(i =>
        i.path.includes('targetDataset')
      )
    ).toBe(true)
  })

  it('given org-target entry with targetDataset but no targetOrg, when parsing, then error path includes targetDataset', async () => {
    // Arrange
    vi.mocked(fs.readFile).mockResolvedValue(
      JSON.stringify({
        entries: [
          {
            sourceOrg: 'src-org',
            targetFile: './output.csv',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Daily',
          },
        ],
      })
    )

    // Act — kills L118 path: ['targetDataset'] → []
    const error = await parseConfig('config.json').catch((e: unknown) => e)

    // Assert
    expect(
      (error as { issues: Array<{ path: string[] }> }).issues.some(i =>
        i.path.includes('targetDataset')
      )
    ).toBe(true)
  })

  it('given org-target entry with neither targetOrg nor targetFile, when parsing, then throws validation error', async () => {
    // Arrange
    const config = {
      entries: [
        {
          sourceOrg: 'src-org',
          eventLog: 'Login',
          interval: 'Daily',
        },
      ],
    }
    vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

    // Act & Assert
    await expect(parseConfig('config.json')).rejects.toThrow('Either targetOrg')
  })

  describe('CSV entry', () => {
    it('given valid CSV config with file target, when parsing, then returns csv entry', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
            },
          ],
        })
      )

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect(sut.entries).toHaveLength(1)
      expect('csvFile' in sut.entries[0]).toBe(true)
      if ('csvFile' in sut.entries[0]) {
        expect(sut.entries[0].csvFile).toBe('./data/login-events.csv')
        expect(sut.entries[0].operation).toBe('Append')
      }
    })

    it('given CSV config with CRM Analytics target, when parsing, then accepts targetOrg', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login-events.csv',
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
      expect('csvFile' in sut.entries[0]).toBe(true)
      if ('csvFile' in sut.entries[0]) {
        expect(sut.entries[0].targetOrg).toBe('my-org')
        expect(sut.entries[0].operation).toBe('Overwrite')
      }
    })

    it('given CSV config with empty csvFile, when parsing, then rejects', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            { csvFile: '', targetFile: './out/data.csv' },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(/Too small/)
    })

    it('given CSV config with {{sourceOrg.Id}} in augmentColumns value, when parsing, then rejects', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              augmentColumns: { OrgId: '{{sourceOrg.Id}}' },
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'dynamic expression'
      )
    })

    it('given CSV config with {{targetOrg.Name}} in augmentColumns value, when parsing, then rejects', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              augmentColumns: { OrgName: '{{targetOrg.Name}}' },
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'dynamic expression'
      )
    })

    it('given CSV config with static augmentColumns value, when parsing, then accepts it as literal', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              augmentColumns: { OrgId: 'static-value' },
            },
          ],
        })
      )

      // Act & Assert — any plain string is accepted as-is
      await expect(parseConfig('config.json')).resolves.toMatchObject({
        entries: [{ csvFile: './data/login-events.csv' }],
      })
    })

    it('given augmentColumns key with dot notation, when parsing, then accepts it', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              augmentColumns: { 'Org.Name': 'MyOrg' },
            },
          ],
        })
      )

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect('csvFile' in sut.entries[0]).toBe(true)
    })

    it('given CSV config with literal augmentColumn value, when parsing, then accepts it', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              augmentColumns: { Source: 'manual-export' },
            },
          ],
        })
      )

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect('csvFile' in sut.entries[0]).toBe(true)
    })

    it('given CSV config with targetOrg but invalid targetDataset, when parsing, then rejects', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login-events.csv',
              targetOrg: 'my-org',
              targetDataset: './out/login-events.csv',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'Must be a valid Salesforce identifier'
      )
    })

    it('given CSV config with absolute csvFile, when parsing, then accepts', async () => {
      // Arrange — absolute paths are valid for CLI tools
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: '/tmp/data.csv',
              targetFile: './out/data.csv',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).resolves.toMatchObject({
        entries: [{ csvFile: '/tmp/data.csv' }],
      })
    })

    it('given CSV config with path traversal in csvFile, when parsing, then rejects', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: '../../etc/passwd',
              targetFile: './out/data.csv',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'csvFile must not traverse parent directories'
      )
    })

    it('given config with path traversal in targetFile, when parsing, then rejects', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              eventLog: 'Login',
              interval: 'Daily',
              sourceOrg: 'src',
              targetFile: '../../etc/crontab',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'targetFile must not traverse parent directories'
      )
    })

    it('given ELF entry with unknown property, when parsing, then rejects with unrecognized key', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              eventLog: 'Login',
              interval: 'Daily',
              sourceOrg: 'src',
              targetOrg: 'ana',
              targetDataset: 'DS',
              unknownProp: 'value',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow('Unrecognized')
    })

    it('given SObject entry with unknown property, when parsing, then rejects with unrecognized key', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              sObject: 'Account',
              fields: ['Id'],
              sourceOrg: 'src',
              targetOrg: 'ana',
              targetDataset: 'DS',
              unknownProp: 'value',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow('Unrecognized')
    })

    it('given CSV entry with unknown property, when parsing, then rejects with unrecognized key', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login.csv',
              targetFile: './out/login.csv',
              unknownProp: 'value',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow('Unrecognized')
    })

    it('given CSV config with explicit operation Append, when parsing, then accepts it', async () => {
      // Arrange — kills L178: z.enum(['', 'Overwrite']) rejects explicit 'Append'
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              operation: 'Append',
            },
          ],
        })
      )

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect('csvFile' in sut.entries[0]).toBe(true)
      if ('csvFile' in sut.entries[0]) {
        expect(sut.entries[0].operation).toBe('Append')
      }
    })

    it('given CSV config with {{sourceOrg.Id}} in augmentColumns, when parsing, then error path starts with augmentColumns', async () => {
      // Arrange — kills L193 path: ['augmentColumns', key] → []
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              augmentColumns: { OrgId: '{{sourceOrg.Id}}' },
            },
          ],
        })
      )

      // Act
      const error = await parseConfig('config.json').catch((e: unknown) => e)

      // Assert
      expect(
        (error as { issues: Array<{ path: string[] }> }).issues.some(i =>
          i.path.includes('augmentColumns')
        )
      ).toBe(true)
    })
  })

  describe('augment column resolution', () => {
    it('given {{sourceOrg.Id}} mustache token, when loading, then resolves to source org id', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventLog: 'E1',
            interval: 'Daily',
            augmentColumns: { OrgId: '{{sourceOrg.Id}}' },
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
      expect(sut[0].augmentColumns).toEqual({ OrgId: '00Dsrc' })
      // Kills L388: query('') — empty string doesn't select org info
      expect(sfPorts.get('source')!.query).toHaveBeenCalledWith(
        'SELECT Id, Name FROM Organization LIMIT 1'
      )
    })

    it('given CSV entry alongside ELF entry with mustache, when loading, then resolves only ELF augment columns', async () => {
      // Arrange — kills L265: isCsvEntry guard in collectUniqueOrgs
      const config = {
        entries: [
          {
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventLog: 'E1',
            interval: 'Daily',
            augmentColumns: { OrgId: '{{sourceOrg.Id}}' },
          },
          {
            csvFile: './data/static.csv',
            targetFile: './out/static.csv',
            augmentColumns: { Tag: 'manual' },
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))
      const sfPorts = new Map<string, SalesforcePort>([
        ['source', makeSfPort({ Id: '00Dsrc', Name: 'SourceOrg' })],
        ['analytic', makeSfPort()],
      ])

      // Act
      const sut = await loadConfig('config.json', sfPorts)

      // Assert
      expect(sut[0].augmentColumns).toEqual({ OrgId: '00Dsrc' })
      expect(sut[1].augmentColumns).toEqual({ Tag: 'manual' })
    })

    it('given mixed static and {{sourceOrg.Name}} in value, when loading, then interpolates', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventLog: 'E1',
            interval: 'Daily',
            augmentColumns: { Label: 'PROD-{{sourceOrg.Name}}' },
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))
      const sfPorts = new Map<string, SalesforcePort>([
        ['source', makeSfPort({ Id: '00Dsrc', Name: 'MyOrg' })],
        ['analytic', makeSfPort({ Id: '00Dana', Name: 'AnalyticOrg' })],
      ])

      // Act
      const sut = await loadConfig('config.json', sfPorts)

      // Assert
      expect(sut[0].augmentColumns).toEqual({ Label: 'PROD-MyOrg' })
    })

    it('given multiple mustache tokens in same value, when loading, then resolves all', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventLog: 'E1',
            interval: 'Daily',
            augmentColumns: { Label: '{{sourceOrg.Name}}-{{targetOrg.Id}}' },
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))
      const sfPorts = new Map<string, SalesforcePort>([
        ['source', makeSfPort({ Id: '00Dsrc', Name: 'MyOrg' })],
        ['analytic', makeSfPort({ Id: '00Dana', Name: 'AnalyticOrg' })],
      ])

      // Act
      const sut = await loadConfig('config.json', sfPorts)

      // Assert
      expect(sut[0].augmentColumns).toEqual({ Label: 'MyOrg-00Dana' })
    })

    it('given unknown mustache token, when loading, then throws', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventLog: 'E1',
            interval: 'Daily',
            augmentColumns: { Label: '{{unknownVar}}' },
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))
      const sfPorts = new Map<string, SalesforcePort>([
        ['source', makeSfPort({ Id: '00Dsrc', Name: 'MyOrg' })],
        ['analytic', makeSfPort({ Id: '00Dana', Name: 'AnalyticOrg' })],
      ])

      // Act & Assert
      await expect(loadConfig('config.json', sfPorts)).rejects.toThrow(
        /unknownVar/
      )
    })

    it('given dynamic expressions, when loading, then resolves org info', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventLog: 'E1',
            interval: 'Daily',
            augmentColumns: {
              OrgId: '{{sourceOrg.Id}}',
              OrgName: '{{sourceOrg.Name}}',
              AnalyticId: '{{targetOrg.Id}}',
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
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventLog: 'E1',
            interval: 'Daily',
            augmentColumns: {
              TargetName: '{{targetOrg.Name}}',
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
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventLog: 'E1',
            interval: 'Daily',
            augmentColumns: { OrgId: '{{sourceOrg.Id}}' },
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
            sourceOrg: 'source',
            targetOrg: 'analytic',
            targetDataset: 'DS',
            eventLog: 'E1',
            interval: 'Daily',
            augmentColumns: { OrgId: '{{sourceOrg.Id}}' },
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
              csvFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
              augmentColumns: { Source: 'manual' },
            },
          ],
        })
      )
      const sfPorts = new Map([['some-org', sfPort]])

      // Act
      const sut = await loadConfig('config.json', sfPorts)

      // Assert — no SF org was queried (no sourceOrg, no {{targetOrg.*}} tokens)
      expect(sfPort.query).not.toHaveBeenCalled()
      expect(sut[0].augmentColumns).toEqual({ Source: 'manual' })
    })

    it('given CSV entry without augmentColumns, when resolving, then defaults to empty object', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login-events.csv',
              targetFile: './out/login-events.csv',
            },
          ],
        })
      )

      // Act
      const sut = await loadConfig('config.json', new Map())

      // Assert — augmentColumns ?? {} branch
      expect(sut[0].augmentColumns).toEqual({})
    })

    it('given CSV entry, when resolving, then augmentColumns are literal values (no dynamic resolution)', async () => {
      // Arrange
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              csvFile: './data/login-events.csv',
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
    it('given CSV entry without name, when getting label, then returns csv-prefixed csvFile', () => {
      // Arrange / Act
      const sut = entryLabel({
        csvFile: './data/login.csv',
        targetFile: './out/login.csv',
        operation: 'Append',
      })

      // Assert
      expect(sut).toBe('csv:./data/login.csv')
    })

    it('given CSV entry with name, when getting label, then returns name', () => {
      // Arrange / Act
      const sut = entryLabel({
        csvFile: './data/login.csv',
        targetFile: './out/login.csv',
        operation: 'Append',
        name: 'login-data',
      })

      // Assert
      expect(sut).toBe('login-data')
    })

    it('given ELF entry without name, when getting label, then returns elf-prefixed eventLog', () => {
      // Arrange / Act
      const sut = entryLabel({
        sourceOrg: 'prod',
        eventLog: 'Login',
        interval: 'Daily',
        targetOrg: 'my-org',
        targetDataset: 'LoginEvents',
        operation: 'Append',
      })

      // Assert
      expect(sut).toBe('elf:Login')
    })

    it('given SObject entry without name, when getting label, then returns sobject-prefixed sObject', () => {
      // Arrange / Act
      const sut = entryLabel({
        sourceOrg: 'prod',
        sObject: 'Account',
        fields: ['Id'],
        dateField: 'LastModifiedDate',
        targetOrg: 'my-org',
        targetDataset: 'Accounts',
        operation: 'Append',
      })

      // Assert
      expect(sut).toBe('sobject:Account')
    })
  })

  describe('augment consistency', () => {
    it('given two entries sharing DatasetKey with different augment column names, when parsing, then throws', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'src1',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Daily',
            augmentColumns: { OrgName: 'OrgA' },
          },
          {
            sourceOrg: 'src2',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Daily',
            augmentColumns: { Source: 'OrgB' },
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        /different augment column names/
      )
    })

    it('given two entries sharing DatasetKey with same augment column names but different values, when parsing, then succeeds', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'src1',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Daily',
            augmentColumns: { OrgName: 'OrgA' },
          },
          {
            sourceOrg: 'src2',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Daily',
            augmentColumns: { OrgName: 'OrgB' },
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect(sut.entries).toHaveLength(2)
    })

    it('given two sobject entries sharing DatasetKey with different fields, when parsing, then throws', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'src1',
            targetOrg: 'ana',
            targetDataset: 'DS',
            sObject: 'Account',
            fields: ['Id', 'Name'],
          },
          {
            sourceOrg: 'src2',
            targetOrg: 'ana',
            targetDataset: 'DS',
            sObject: 'Account',
            fields: ['Id', 'Industry'],
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        /different fields/
      )
    })

    it('given two sobject entries sharing DatasetKey with same fields, when parsing, then succeeds', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'src1',
            targetOrg: 'ana',
            targetDataset: 'DS',
            sObject: 'Account',
            fields: ['Id', 'Name'],
          },
          {
            sourceOrg: 'src2',
            targetOrg: 'ana',
            targetDataset: 'DS',
            sObject: 'Account',
            fields: ['Id', 'Name'],
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act
      const sut = await parseConfig('config.json')

      // Assert
      expect(sut.entries).toHaveLength(2)
    })

    it('given one entry with augment columns and peer with none targeting same DatasetKey, when parsing, then throws', async () => {
      // Arrange
      const config = {
        entries: [
          {
            sourceOrg: 'src1',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Daily',
            augmentColumns: { OrgName: 'OrgA' },
          },
          {
            sourceOrg: 'src2',
            targetOrg: 'ana',
            targetDataset: 'DS',
            eventLog: 'Login',
            interval: 'Daily',
          },
        ],
      }
      vi.mocked(fs.readFile).mockResolvedValue(JSON.stringify(config))

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        /different augment column names/i
      )
    })
  })

  describe('regex boundary validation', () => {
    it('given augmentColumns key starting with a digit, when parsing, then rejects with dataset column name error', async () => {
      // Arrange — kills L77 regex ^-anchor removal: without ^, '1abc' matches [a-zA-Z_][a-zA-Z0-9_.]*$
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              sourceOrg: 'src',
              targetOrg: 'ana',
              targetDataset: 'DS',
              eventLog: 'Login',
              interval: 'Daily',
              augmentColumns: { '1InvalidKey': 'value' },
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'Must be a valid dataset column name'
      )
    })

    it('given augmentColumns key ending with a special character, when parsing, then rejects with dataset column name error', async () => {
      // Arrange — kills L77 regex $-anchor removal: without $, 'col!' matches ^[a-zA-Z_][a-zA-Z0-9_.]*
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              sourceOrg: 'src',
              targetOrg: 'ana',
              targetDataset: 'DS',
              eventLog: 'Login',
              interval: 'Daily',
              augmentColumns: { 'col!': 'value' },
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'Must be a valid dataset column name'
      )
    })

    it('given entry name starting with a special character, when parsing, then rejects with alphanumeric error', async () => {
      // Arrange — kills L86 regex ^-anchor removal: without ^, '!valid' matches [a-zA-Z0-9_-]+$
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              name: '!valid-name',
              sourceOrg: 'src',
              targetOrg: 'ana',
              targetDataset: 'DS',
              eventLog: 'Login',
              interval: 'Daily',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(
        'Must be alphanumeric'
      )
    })
  })

  describe('validateOperationConsistency error detail', () => {
    it('given conflicting operations, when parsing, then error message contains operation names', async () => {
      // Arrange — kills L269 ArrowFunction → () => undefined (loses operation names in detail)
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              sourceOrg: 'src',
              targetOrg: 'ana',
              targetDataset: 'DS',
              eventLog: 'E1',
              interval: 'Daily',
              operation: 'Append',
            },
            {
              sourceOrg: 'src',
              targetOrg: 'ana',
              targetDataset: 'DS',
              eventLog: 'E2',
              interval: 'Daily',
              operation: 'Overwrite',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow("'Append'")
    })

    it('given conflicting operations, when parsing, then error message contains " vs " separator', async () => {
      // Arrange — kills L270 StringLiteral '' → ' vs ' (detail loses separator)
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              sourceOrg: 'src',
              targetOrg: 'ana',
              targetDataset: 'DS',
              eventLog: 'E1',
              interval: 'Daily',
              operation: 'Append',
            },
            {
              sourceOrg: 'src',
              targetOrg: 'ana',
              targetDataset: 'DS',
              eventLog: 'E2',
              interval: 'Daily',
              operation: 'Overwrite',
            },
          ],
        })
      )

      // Act & Assert
      await expect(parseConfig('config.json')).rejects.toThrow(' vs ')
    })

    it('given three entries where two share same operation, when parsing, then error lists indices with comma separator', async () => {
      // Arrange — kills L269:63 StringLiteral: join(', ') → join('')
      // Two Append entries (indices 0 and 2) conflict with one Overwrite entry (index 1)
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              sourceOrg: 'src',
              targetOrg: 'ana',
              targetDataset: 'DS',
              eventLog: 'E1',
              interval: 'Daily',
              operation: 'Append',
            },
            {
              sourceOrg: 'src',
              targetOrg: 'ana',
              targetDataset: 'DS',
              eventLog: 'E2',
              interval: 'Daily',
              operation: 'Overwrite',
            },
            {
              sourceOrg: 'src',
              targetOrg: 'ana',
              targetDataset: 'DS',
              eventLog: 'E3',
              interval: 'Daily',
              operation: 'Append',
            },
          ],
        })
      )

      // Act & Assert — indices 0 and 2 both have Append; format is "0, 2" not "02"
      await expect(parseConfig('config.json')).rejects.toThrow('0, 2')
    })
  })

  describe('augmentColumn key sort consistency', () => {
    it('given two entries with same augment keys in different insertion order, when parsing, then succeeds', async () => {
      // Arrange — kills L300/L303/L304 sort removal mutations:
      // without .sort(), keys in different insertion order would not compare equal
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              sourceOrg: 'src1',
              targetOrg: 'ana',
              targetDataset: 'DS',
              eventLog: 'Login',
              interval: 'Daily',
              augmentColumns: { B: 'orgA', A: 'extra' },
            },
            {
              sourceOrg: 'src2',
              targetOrg: 'ana',
              targetDataset: 'DS',
              eventLog: 'Login',
              interval: 'Daily',
              augmentColumns: { B: 'orgB', A: 'extra' },
            },
          ],
        })
      )

      // Act & Assert — same keys A and B, just in different order → should succeed
      const sut = await parseConfig('config.json')
      expect(sut.entries).toHaveLength(2)
    })
  })

  describe('sobject field sort consistency', () => {
    it('given two sobject entries with same fields in different order, when parsing, then succeeds', async () => {
      // Arrange — kills L326/L327/L328 sort removal mutations:
      // without .sort(), fields in different insertion order would not compare equal
      vi.mocked(fs.readFile).mockResolvedValue(
        JSON.stringify({
          entries: [
            {
              sourceOrg: 'src1',
              targetOrg: 'ana',
              targetDataset: 'DS',
              sObject: 'Account',
              fields: ['Name', 'Id'],
            },
            {
              sourceOrg: 'src2',
              targetOrg: 'ana',
              targetDataset: 'DS',
              sObject: 'Account',
              fields: ['Name', 'Id'],
            },
          ],
        })
      )

      // Act & Assert — same fields [Name, Id], just in different order → should succeed
      const sut = await parseConfig('config.json')
      expect(sut.entries).toHaveLength(2)
    })
  })
})

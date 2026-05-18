import { describe, expect, it, vi } from 'vitest'
import { SObjectBootstrapProvider } from '../../../../src/adapters/readers/sobject-bootstrap-provider.js'
import type {
  DescribeField,
  SObjectDescribe,
} from '../../../../src/ports/types.js'

interface DescribeFieldInit {
  readonly name: string
  readonly label?: string
  readonly type: string
  readonly precision?: number
  readonly scale?: number
  readonly referenceTo?: readonly string[]
  readonly relationshipName?: string | null
}

function field(init: DescribeFieldInit): DescribeField {
  return {
    name: init.name,
    label: init.label ?? init.name,
    type: init.type,
    precision: init.precision,
    scale: init.scale,
    referenceTo: init.referenceTo,
    relationshipName: init.relationshipName,
  }
}

function describePort(scripts: Record<string, SObjectDescribe>): {
  describe: ReturnType<typeof vi.fn>
} {
  return {
    describe: vi.fn(async (name: string) => {
      const out = scripts[name]
      if (!out) throw new Error(`no script for ${name}`)
      return out
    }),
  }
}

const USER_DESCRIBE: SObjectDescribe = {
  name: 'User',
  fields: [
    field({ name: 'Id', label: 'User ID', type: 'id' }),
    field({ name: 'Name', type: 'string' }),
    field({ name: 'IsActive', type: 'boolean' }),
    field({ name: 'LastModifiedDate', type: 'datetime' }),
    field({
      name: 'NumberOfFailedLogins',
      type: 'int',
      precision: 0,
      scale: 0,
    }),
    field({
      name: 'ProfileId',
      type: 'reference',
      referenceTo: ['Profile'],
      relationshipName: 'Profile',
    }),
    field({
      name: 'ManagerId',
      type: 'reference',
      referenceTo: ['User'],
      relationshipName: 'Manager',
    }),
  ],
}
const PROFILE_DESCRIBE: SObjectDescribe = {
  name: 'Profile',
  fields: [field({ name: 'Name', type: 'string' })],
}

describe('SObjectBootstrapProvider', () => {
  it('given a leaf field on the root sObject, when build, then produces a SourceField with the mapped CRMA type', async () => {
    // Arrange
    const port = describePort({ User: USER_DESCRIBE })
    const sut = new SObjectBootstrapProvider(port, {
      sobject: 'User',
      fields: ['Id'],
      datasetName: 'Users',
      augmentColumns: {},
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields).toEqual([
      { name: 'Id', label: 'Id', type: 'Text', origin: 'reader' },
    ])
  })

  it('given a one-hop relationship path, when build, then column is underscored and both root and related sObjects are described', async () => {
    // Arrange
    const port = describePort({
      User: USER_DESCRIBE,
      Profile: PROFILE_DESCRIBE,
    })
    const sut = new SObjectBootstrapProvider(port, {
      sobject: 'User',
      fields: ['Profile.Name'],
      datasetName: 'Users',
      augmentColumns: {},
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields).toEqual([
      {
        name: 'Profile_Name',
        label: 'Profile.Name',
        type: 'Text',
        origin: 'reader',
      },
    ])
    expect(port.describe).toHaveBeenCalledWith('User')
    expect(port.describe).toHaveBeenCalledWith('Profile')
  })

  it('given a two-hop relationship path (self-reference), when build, then describes each hop in order', async () => {
    // Arrange — Manager.Manager.Name walks User → User → User → leaf Name
    const port = describePort({ User: USER_DESCRIBE })
    const sut = new SObjectBootstrapProvider(port, {
      sobject: 'User',
      fields: ['Manager.Manager.Name'],
      datasetName: 'Users',
      augmentColumns: {},
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields[0]).toMatchObject({
      name: 'Manager_Manager_Name',
      label: 'Manager.Manager.Name',
      type: 'Text',
    })
    // Cache hit: User described once even though walked three times
    expect(port.describe).toHaveBeenCalledTimes(1)
  })

  it('given a missing leaf field, when build, then throws with the offending path and root sObject', async () => {
    // Arrange
    const port = describePort({ User: USER_DESCRIBE })
    const sut = new SObjectBootstrapProvider(port, {
      sobject: 'User',
      fields: ['NoSuchField'],
      datasetName: 'Users',
      augmentColumns: {},
    })

    // Act / Assert
    await expect(sut.buildSourceSchema()).rejects.toThrow(/NoSuchField/)
    await expect(sut.buildSourceSchema()).rejects.toThrow(/User/)
  })

  it('given a missing relationship name in the middle of a path, when build, then throws', async () => {
    // Arrange
    const port = describePort({ User: USER_DESCRIBE })
    const sut = new SObjectBootstrapProvider(port, {
      sobject: 'User',
      fields: ['NoSuchRel.Name'],
      datasetName: 'Users',
      augmentColumns: {},
    })

    // Act / Assert
    await expect(sut.buildSourceSchema()).rejects.toThrow(/NoSuchRel/)
  })

  it('given a boolean SF field, when build, then mapped to CRMA Text (groupable dimension)', async () => {
    // Arrange
    const port = describePort({ User: USER_DESCRIBE })
    const sut = new SObjectBootstrapProvider(port, {
      sobject: 'User',
      fields: ['IsActive'],
      datasetName: 'Users',
      augmentColumns: {},
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields[0].type).toBe('Text')
  })

  it.each<[string, string]>([
    ['string', 'Text'],
    ['id', 'Text'],
    ['reference', 'Text'],
    ['picklist', 'Text'],
    ['multipicklist', 'Text'],
    ['email', 'Text'],
    ['phone', 'Text'],
    ['url', 'Text'],
    ['textarea', 'Text'],
    ['combobox', 'Text'],
    ['encryptedstring', 'Text'],
    ['boolean', 'Text'],
    ['time', 'Text'],
    ['base64', 'Text'],
    ['anyType', 'Text'],
    ['address', 'Text'],
    ['location', 'Text'],
    ['int', 'Numeric'],
    ['long', 'Numeric'],
    ['double', 'Numeric'],
    ['currency', 'Numeric'],
    ['percent', 'Numeric'],
    ['date', 'Date'],
    ['datetime', 'Date'],
    ['unknownfuturetype', 'Text'],
  ])('given SF type %s, when build, then maps to CRMA type %s', async (sfType, crmaType) => {
    // Arrange — synthesise a single-field describe response for the input type
    const port = describePort({
      Custom__c: {
        name: 'Custom__c',
        fields: [
          field({
            name: 'F',
            type: sfType,
            precision: sfType === 'int' ? 9 : undefined,
            scale: sfType === 'int' ? 0 : undefined,
          }),
        ],
      },
    })
    const sut = new SObjectBootstrapProvider(port, {
      sobject: 'Custom__c',
      fields: ['F'],
      datasetName: 'X',
      augmentColumns: {},
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields[0].type).toBe(crmaType)
  })

  it('given SF date field, when build, then format is yyyy-MM-dd (date-only)', async () => {
    // Arrange
    const port = describePort({
      X: { name: 'X', fields: [field({ name: 'D', type: 'date' })] },
    })
    const sut = new SObjectBootstrapProvider(port, {
      sobject: 'X',
      fields: ['D'],
      datasetName: 'X',
      augmentColumns: {},
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields[0]).toMatchObject({
      type: 'Date',
      format: 'yyyy-MM-dd',
    })
  })

  it('given SF datetime field, when build, then format is the full SF-native datetime layout', async () => {
    // Arrange
    const port = describePort({
      X: {
        name: 'X',
        fields: [field({ name: 'DT', type: 'datetime' })],
      },
    })
    const sut = new SObjectBootstrapProvider(port, {
      sobject: 'X',
      fields: ['DT'],
      datasetName: 'X',
      augmentColumns: {},
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields[0]).toMatchObject({
      type: 'Date',
      format: "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    })
  })

  it('given a Numeric field with describe-supplied precision and scale, when build, then values are passed through to SourceSchema', async () => {
    // Arrange
    const port = describePort({
      X: {
        name: 'X',
        fields: [
          field({ name: 'Amount', type: 'currency', precision: 12, scale: 2 }),
        ],
      },
    })
    const sut = new SObjectBootstrapProvider(port, {
      sobject: 'X',
      fields: ['Amount'],
      datasetName: 'X',
      augmentColumns: {},
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields[0]).toMatchObject({
      type: 'Numeric',
      precision: 12,
      scale: 2,
    })
  })

  it('given augment columns, when build, then appended after reader fields with origin: augment', async () => {
    // Arrange
    const port = describePort({ User: USER_DESCRIBE })
    const sut = new SObjectBootstrapProvider(port, {
      sobject: 'User',
      fields: ['Id'],
      datasetName: 'Users',
      augmentColumns: { SourceOrgId: '00D000000000001', SourceOrgName: 'prod' },
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields).toEqual([
      { name: 'Id', label: 'Id', type: 'Text', origin: 'reader' },
      {
        name: 'SourceOrgId',
        label: 'SourceOrgId',
        type: 'Text',
        origin: 'augment',
      },
      {
        name: 'SourceOrgName',
        label: 'SourceOrgName',
        type: 'Text',
        origin: 'augment',
      },
    ])
  })

  it('given multiple fields on the same sObject, when build, then describe called exactly once', async () => {
    // Arrange — 5 fields, all on User
    const port = describePort({ User: USER_DESCRIBE })
    const sut = new SObjectBootstrapProvider(port, {
      sobject: 'User',
      fields: [
        'Id',
        'Name',
        'IsActive',
        'LastModifiedDate',
        'NumberOfFailedLogins',
      ],
      datasetName: 'Users',
      augmentColumns: {},
    })

    // Act
    await sut.buildSourceSchema()

    // Assert
    expect(port.describe).toHaveBeenCalledTimes(1)
  })
})

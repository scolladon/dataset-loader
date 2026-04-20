import { describe, expect, it } from 'vitest'
import { buildSObjectRowProjection } from '../../../src/domain/sobject-row-projection.js'
import { SkipDatasetError } from '../../../src/ports/types.js'

const baseInput = {
  datasetName: 'ALM_X',
  entryLabel: 'entry-1',
}

describe('buildSObjectRowProjection', () => {
  it('given identity mapping, when building, then outputIndex is [0..N-1] and augmentSlots is empty', () => {
    // Arrange / Act
    const sut = buildSObjectRowProjection({
      ...baseInput,
      readerFields: ['A', 'B', 'C'],
      augmentColumns: {},
      datasetFields: ['A', 'B', 'C'],
    })

    // Assert
    expect(sut.targetSize).toBe(3)
    expect([...sut.outputIndex]).toEqual([0, 1, 2])
    expect(sut.augmentSlots).toEqual([])
  })

  it('given reversed dataset order, when building, then outputIndex reverses', () => {
    // Arrange / Act
    const sut = buildSObjectRowProjection({
      ...baseInput,
      readerFields: ['A', 'B', 'C'],
      augmentColumns: {},
      datasetFields: ['C', 'B', 'A'],
    })

    // Assert
    expect([...sut.outputIndex]).toEqual([2, 1, 0])
  })

  it('given dotted reader field and underscored dataset field, when building, then matches', () => {
    // Arrange / Act
    const sut = buildSObjectRowProjection({
      ...baseInput,
      readerFields: ['UserRole.Id', 'UserRole.Name'],
      augmentColumns: {},
      datasetFields: ['UserRole_Id', 'UserRole_Name'],
    })

    // Assert
    expect([...sut.outputIndex]).toEqual([0, 1])
  })

  it('given case-only difference, when building, then matches case-insensitively', () => {
    // Arrange / Act
    const sut = buildSObjectRowProjection({
      ...baseInput,
      readerFields: ['userid', 'isactive'],
      augmentColumns: {},
      datasetFields: ['UserId', 'IsActive'],
    })

    // Assert
    expect([...sut.outputIndex]).toEqual([0, 1])
  })

  it('given a dataset field missing from input, when building, then throws SkipDatasetError naming the missing field', () => {
    // Arrange / Act / Assert
    expect(() =>
      buildSObjectRowProjection({
        ...baseInput,
        readerFields: ['A', 'B'],
        augmentColumns: {},
        datasetFields: ['A', 'B', 'C'],
      })
    ).toThrow(/missing.*\bC\b/)
  })

  it('given a reader field not in dataset, when building, then throws SkipDatasetError naming the extra field', () => {
    // Arrange / Act / Assert
    expect(() =>
      buildSObjectRowProjection({
        ...baseInput,
        readerFields: ['A', 'B', 'X'],
        augmentColumns: {},
        datasetFields: ['A', 'B'],
      })
    ).toThrow(/not in dataset.*\bX\b/)
  })

  it('given augment key also in reader fields, when building, then throws SkipDatasetError naming the overlap', () => {
    // Arrange / Act / Assert
    expect(() =>
      buildSObjectRowProjection({
        ...baseInput,
        readerFields: ['A', 'OrgId'],
        augmentColumns: { OrgId: 'value' },
        datasetFields: ['A', 'OrgId'],
      })
    ).toThrow(/overlap[\s\S]*OrgId/i)
  })

  it('given augment value with quote char, when building, then augmentSlots[i].quoted escapes them once', () => {
    // Arrange / Act
    const sut = buildSObjectRowProjection({
      ...baseInput,
      readerFields: ['A'],
      augmentColumns: { Name: 'O"Brien' },
      datasetFields: ['A', 'Name'],
    })

    // Assert
    expect(sut.augmentSlots).toEqual([{ pos: 1, quoted: '"O""Brien"' }])
  })

  it('given augment value with formula prefix, when building, then augmentSlots[i].quoted is TAB-guarded once', () => {
    // Arrange / Act
    const sut = buildSObjectRowProjection({
      ...baseInput,
      readerFields: ['A'],
      augmentColumns: { Col: '=cmd' },
      datasetFields: ['A', 'Col'],
    })

    // Assert
    expect(sut.augmentSlots).toEqual([{ pos: 1, quoted: '"\t=cmd"' }])
  })

  it('given empty augmentColumns, when building, then augmentSlots is empty', () => {
    // Arrange / Act
    const sut = buildSObjectRowProjection({
      ...baseInput,
      readerFields: ['A', 'B'],
      augmentColumns: {},
      datasetFields: ['A', 'B'],
    })

    // Assert
    expect(sut.augmentSlots).toEqual([])
  })

  it('given augment at non-trailing dataset position, when building, then augmentSlots pos reflects that position', () => {
    // Arrange / Act
    const sut = buildSObjectRowProjection({
      ...baseInput,
      readerFields: ['A', 'B'],
      augmentColumns: { Mid: 'X' },
      datasetFields: ['A', 'Mid', 'B'],
    })

    // Assert
    expect(sut.targetSize).toBe(3)
    expect([...sut.outputIndex]).toEqual([0, 2])
    expect(sut.augmentSlots).toEqual([{ pos: 1, quoted: '"X"' }])
  })

  it('given dataset name and entry label, when failing, then both appear in the error', () => {
    // Arrange / Act / Assert
    try {
      buildSObjectRowProjection({
        datasetName: 'ALM_USERS',
        entryLabel: 'users-prod',
        readerFields: ['A'],
        augmentColumns: {},
        datasetFields: ['B'],
      })
      throw new Error('expected throw')
    } catch (err) {
      expect(err).toBeInstanceOf(SkipDatasetError)
      expect((err as Error).message).toContain('ALM_USERS')
      expect((err as Error).message).toContain('users-prod')
    }
  })

  it('given reproduction of supervision.config.json userslogin scenario, when building, then outputIndex matches alphabetical dataset order', () => {
    // Arrange / Act
    const sut = buildSObjectRowProjection({
      datasetName: 'ALM_UserLogin',
      entryLabel: 'userslogin-prod',
      readerFields: [
        'UserId',
        'IsPasswordLocked',
        'IsFrozen',
        'LastModifiedDate',
      ],
      augmentColumns: { OrgId: '00D...', OrgName: 'Acme' },
      datasetFields: [
        'IsFrozen',
        'IsPasswordLocked',
        'LastModifiedDate',
        'OrgId',
        'OrgName',
        'UserId',
      ],
    })

    // Assert: UserId→5, IsPasswordLocked→1, IsFrozen→0, LastModifiedDate→2
    expect([...sut.outputIndex]).toEqual([5, 1, 0, 2])
    expect(sut.augmentSlots).toEqual([
      { pos: 3, quoted: '"00D..."' },
      { pos: 4, quoted: '"Acme"' },
    ])
  })
})

// Regression test for the dataset column alignment bug observed in
// `ExternalFileDataflow06VbF000001ZAS9UAO_digest.csv`. The user's
// `supervision.config.json` declared `userslogin-prod` with fields
// `[UserId, IsPasswordLocked, IsFrozen, LastModifiedDate]` and augment
// `{ OrgId, OrgName }`, but the dataset metadata stored columns in
// alphabetical order. Without the projector, every cell shifted by the
// permutation. With the fix, rows uploaded to InsightsExternalDataPart
// must land in dataset metadata order.

import { gunzipSync } from 'node:zlib'
import { describe, expect, it, vi } from 'vitest'
import { SObjectReader } from '../../src/adapters/readers/sobject-reader.js'
import { DatasetWriter } from '../../src/adapters/writers/dataset-writer.js'
import { DatasetKey } from '../../src/domain/dataset-key.js'
import { DateBounds } from '../../src/domain/date-bounds.js'
import { buildSObjectRowProjection } from '../../src/domain/sobject-row-projection.js'
import {
  type AlignmentSpec,
  type SalesforcePort,
} from '../../src/ports/types.js'

function makeFakePort(opts: {
  metadataJson: string
  records: Record<string, unknown>[]
  capturedParts: { dataFile: string }[]
}): SalesforcePort {
  return {
    apiVersion: '62.0',
    query: vi.fn(async (soql: string) => {
      if (soql.includes('FROM InsightsExternalData')) {
        return {
          totalSize: 1,
          done: true,
          records: [{ MetadataJson: '/blob/url' }],
          // biome-ignore lint/suspicious/noExplicitAny: fake mock
        } as any
      }
      // SObject query
      return {
        totalSize: opts.records.length,
        done: true,
        records: opts.records,
        // biome-ignore lint/suspicious/noExplicitAny: fake mock
      } as any
    }),
    queryMore: vi.fn(),
    getBlob: vi.fn(async () => opts.metadataJson),
    getBlobStream: vi.fn(),
    post: vi.fn(async (path: string, body: Record<string, unknown>) => {
      if (path.endsWith('InsightsExternalDataPart')) {
        opts.capturedParts.push({ dataFile: body.DataFile as string })
      }
      return { id: '06V0000000000A0' }
    }),
    patch: vi.fn(async () => ({})),
    del: vi.fn(),
  }
}

describe('Dataset column alignment regression (supervision.config.json)', () => {
  it('given metadata with fields in alphabetical order, when uploading SObject rows, then rows land in dataset order (not config order)', async () => {
    // Arrange
    const datasetMetadata = JSON.stringify({
      objects: [
        {
          numberOfLinesToIgnore: 1,
          fields: [
            { fullyQualifiedName: 'IsFrozen', type: 'Text' },
            { fullyQualifiedName: 'IsPasswordLocked', type: 'Text' },
            { fullyQualifiedName: 'LastModifiedDate', type: 'Text' },
            { fullyQualifiedName: 'OrgId', type: 'Text' },
            { fullyQualifiedName: 'OrgName', type: 'Text' },
            { fullyQualifiedName: 'UserId', type: 'Text' },
          ],
        },
      ],
    })
    const records = [
      {
        UserId: '005abc',
        IsPasswordLocked: false,
        IsFrozen: false,
        LastModifiedDate: '2020-11-06T14:55:51.000+0000',
      },
    ]
    const captured: { dataFile: string }[] = []
    const sfPort = makeFakePort({
      metadataJson: datasetMetadata,
      records,
      capturedParts: captured,
    })
    const alignment: AlignmentSpec = {
      readerKind: 'sobject',
      entryLabel: 'userslogin-prod',
      providedFields: [
        'UserId',
        'IsPasswordLocked',
        'IsFrozen',
        'LastModifiedDate',
      ],
      augmentColumns: { OrgId: '00D...', OrgName: 'Acme' },
    }

    const writer = new DatasetWriter(
      sfPort,
      DatasetKey.fromEntry({
        targetOrg: 'ana',
        targetDataset: 'ALM_UserLogin',
      }),
      'Append',
      undefined,
      alignment
    )
    const { chunker, datasetFields } = await writer.init()
    expect(datasetFields).toBeDefined()
    const layout = buildSObjectRowProjection({
      datasetName: 'ALM_UserLogin',
      entryLabel: alignment.entryLabel,
      readerFields: alignment.providedFields,
      augmentColumns: alignment.augmentColumns,
      datasetFields: datasetFields!,
    })

    const reader = new SObjectReader(sfPort, {
      sobject: 'UserLogin',
      fields: alignment.providedFields as string[],
      dateField: 'LastModifiedDate',
      bounds: DateBounds.none(),
    })
    reader.project(layout)

    // Act
    const fetchResult = await reader.fetch()
    for await (const batch of fetchResult.lines) {
      chunker.write(batch)
    }
    chunker.end()
    await new Promise<void>((resolve, reject) => {
      chunker.on('finish', resolve)
      chunker.on('error', reject)
    })

    // Assert
    expect(captured.length).toBeGreaterThan(0)
    const decoded = gunzipSync(
      Buffer.from(captured[0].dataFile, 'base64')
    ).toString()
    const rows = decoded.trim().split('\n')
    expect(rows).toHaveLength(1)
    const cells = rows[0].split(',')
    // Dataset order: [IsFrozen, IsPasswordLocked, LastModifiedDate, OrgId, OrgName, UserId]
    expect(cells).toEqual([
      '"false"',
      '"false"',
      '"2020-11-06T14:55:51.000+0000"',
      '"00D..."',
      '"Acme"',
      '"005abc"',
    ])
  })

  // Regression for the per-entry-augment-values bug caught in code review:
  // two config entries targeting the SAME dataset with DIFFERENT augment
  // values (e.g. different sourceOrg IDs) must each produce rows carrying
  // their own augment values, not the first entry's values for all rows.
  it('given two entries targeting same dataset with different augment values, when projecting, then each entry builds its own layout with its own values', () => {
    // Arrange
    const datasetFields = [
      'IsFrozen',
      'IsPasswordLocked',
      'LastModifiedDate',
      'OrgId',
      'OrgName',
      'UserId',
    ]
    const xrmruLayout = buildSObjectRowProjection({
      datasetName: 'ALM_UserLogin',
      entryLabel: 'userslogin-xrmru',
      readerFields: [
        'UserId',
        'IsPasswordLocked',
        'IsFrozen',
        'LastModifiedDate',
      ],
      augmentColumns: { OrgId: '00Dxrmru', OrgName: 'XRMRU' },
      datasetFields,
    })
    const prodLayout = buildSObjectRowProjection({
      datasetName: 'ALM_UserLogin',
      entryLabel: 'userslogin-prod',
      readerFields: [
        'UserId',
        'IsPasswordLocked',
        'IsFrozen',
        'LastModifiedDate',
      ],
      augmentColumns: { OrgId: '00Dprod', OrgName: 'PROD' },
      datasetFields,
    })

    // Assert: structural parts are identical; augment *values* differ.
    expect([...xrmruLayout.outputIndex]).toEqual([...prodLayout.outputIndex])
    expect(xrmruLayout.targetSize).toBe(prodLayout.targetSize)
    expect(xrmruLayout.augmentSlots).toEqual([
      { pos: 3, quoted: '"00Dxrmru"' },
      { pos: 4, quoted: '"XRMRU"' },
    ])
    expect(prodLayout.augmentSlots).toEqual([
      { pos: 3, quoted: '"00Dprod"' },
      { pos: 4, quoted: '"PROD"' },
    ])
  })
})

import { PassThrough, Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { bench, describe } from 'vitest'
import { createFanOutTransform } from '../../src/adapters/pipeline/fan-out-transform.js'
import {
  GzipChunkingWritable,
  UPLOAD_HIGH_WATER,
} from '../../src/adapters/writers/dataset-writer.js'
import type { SalesforcePort } from '../../src/ports/types.js'
import { generateCsvBatches } from './fixtures/generateFixtures.js'

const mockSfPort: SalesforcePort = {
  apiVersion: '62.0',
  query: () => Promise.resolve({ done: true, totalSize: 0, records: [] }),
  queryMore: () => Promise.resolve({ done: true, totalSize: 0, records: [] }),
  getBlob: () => Promise.resolve({}),
  getBlobStream: () => Promise.resolve(new Readable()),
  post: () => Promise.resolve({} as never),
  postMultipart: () => Promise.resolve({} as never),
  patch: () => Promise.resolve({} as never),
  del: () => Promise.resolve(),
  describe: () => Promise.resolve({ name: 'Unknown', fields: [] }),
}

const PART_PATH = '/services/data/v62.0/sobjects/InsightsExternalDataPart'
const PARENT_ID = '06V000000000001'

// Small enough that the 8 MB 'large' fixture spans 3 parts while keeping its
// ~10x compressibility, so most batches cross a part boundary.
const ROTATION_PART_MAX_BYTES = 512 * 1024

async function writeAll(
  writable: GzipChunkingWritable,
  batches: string[][]
): Promise<void> {
  for (const batch of batches) {
    await new Promise<void>((resolve, reject) => {
      writable.write(batch, err => (err ? reject(err) : resolve()))
    })
  }
  await new Promise<void>((resolve, reject) => {
    writable.end((err: Error | null) => (err ? reject(err) : resolve()))
  })
  await writable.drainUploads()
}

const sizes = ['small', 'medium', 'large'] as const

for (const size of sizes) {
  const batches = generateCsvBatches(size)

  describe(`throughput-gzip-${size}`, () => {
    bench(`throughput-gzip-chunking-${size}`, async () => {
      const writable = new GzipChunkingWritable(
        mockSfPort,
        PART_PATH,
        PARENT_ID
      )
      await writeAll(writable, batches)
    })
  })
}

describe('throughput-gzip-rotation', () => {
  const batches = generateCsvBatches('large')
  bench('throughput-gzip-chunking-rotation', async () => {
    const writable = new GzipChunkingWritable(
      mockSfPort,
      PART_PATH,
      PARENT_ID,
      undefined,
      UPLOAD_HIGH_WATER,
      ROTATION_PART_MAX_BYTES
    )
    await writeAll(writable, batches)
  })
})

for (const channelCount of [1, 3, 5]) {
  const batches = generateCsvBatches('medium')

  describe(`throughput-fanout-${channelCount}ch`, () => {
    bench(`throughput-fanout-${channelCount}-channels`, async () => {
      const channels = Array.from(
        { length: channelCount },
        () => new PassThrough({ objectMode: true })
      )
      const consumed: Promise<void>[] = channels.map(
        ch =>
          new Promise<void>(resolve => {
            ch.on('data', () => undefined)
            ch.on('end', resolve)
          })
      )
      const transform = createFanOutTransform(channels, () => undefined)
      const source = Readable.from(batches, { objectMode: true })
      await pipeline(source, transform)
      await Promise.all(consumed)
    })
  })
}

import { bench, describe } from 'vitest'
import { AsyncChannel } from '../../src/adapters/pipeline/async-channel.js'
import {
  buildAugmentHeaderSuffix,
  buildAugmentSuffix,
} from '../../src/adapters/pipeline/augment-transform.js'
import {
  generateAugmentColumns,
  generateCsvBatches,
} from './fixtures/generateFixtures.js'

const sizes = ['small', 'medium', 'large'] as const
const augmentColumns = generateAugmentColumns(5)
const headerSuffix = buildAugmentHeaderSuffix(augmentColumns)
const valueSuffix = buildAugmentSuffix(augmentColumns)

for (const size of sizes) {
  const batches = generateCsvBatches(size)

  describe(`throughput-augment-${size}`, () => {
    bench(`throughput-augment-${size}`, () => {
      for (const batch of batches) {
        for (let i = 0; i < batch.length; i++) {
          batch[i] + (i === 0 ? headerSuffix : valueSuffix)
        }
      }
    })
  })
}

describe('throughput-async-channel', () => {
  bench('throughput-channel-push-consume-1k', async () => {
    const channel = new AsyncChannel<string>(64)
    const producer = (async () => {
      for (let i = 0; i < 1000; i++) {
        await channel.push(`line-${i}`)
      }
      channel.close()
    })()
    let _count = 0
    for await (const _ of channel) {
      _count++
    }
    await producer
  })

  bench('throughput-channel-push-consume-10k', async () => {
    const channel = new AsyncChannel<string>(256)
    const producer = (async () => {
      for (let i = 0; i < 10000; i++) {
        await channel.push(`line-${i}`)
      }
      channel.close()
    })()
    let _count = 0
    for await (const _ of channel) {
      _count++
    }
    await producer
  })
})

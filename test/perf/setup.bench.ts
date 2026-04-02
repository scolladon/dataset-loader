import { bench, describe } from 'vitest'
import { Watermark } from '../../src/domain/watermark.js'
import { WatermarkKey } from '../../src/domain/watermark-key.js'
import { WatermarkStore } from '../../src/domain/watermark-store.js'
import type { EntryShape } from '../../src/ports/types.js'

const sizes = [5, 20, 50] as const

const elfEntry: EntryShape = {
  type: 'elf',
  sourceOrg: 'org-0',
  eventType: 'Login',
  interval: 'daily',
  targetOrg: 'target-0',
  dataset: 'Dataset_000000',
} as EntryShape

describe('setup-watermark-operations', () => {
  bench('watermark-parse-valid', () => {
    Watermark.fromString('2026-01-15T10:30:00.000Z')
  })

  bench('watermark-to-soql', () => {
    const wm = Watermark.fromString('2026-01-15T10:30:00.000Z')
    wm.toSoqlLiteral()
  })
})

for (const count of sizes) {
  const raw: Record<string, string> = {}
  for (let i = 0; i < count; i++) {
    raw[`org-${i}:elf:Login:daily`] = '2026-01-15T10:30:00.000Z'
  }

  describe(`setup-watermark-store-${count}`, () => {
    bench(`watermark-store-create-${count}`, () => {
      WatermarkStore.fromRecord(raw)
    })

    const store = WatermarkStore.fromRecord(raw)
    const key = WatermarkKey.fromEntry(elfEntry)
    const newWatermark = Watermark.fromString('2026-02-01T00:00:00.000Z')

    bench(`watermark-store-get-${count}`, () => {
      store.get(key)
    })

    bench(`watermark-store-set-${count}`, () => {
      store.set(key, newWatermark)
    })
  })
}

import { Watermark } from './watermark.js'
import { type WatermarkKey } from './watermark-key.js'

export class WatermarkStore {
  private constructor(
    private readonly entries: ReadonlyMap<string, Watermark>
  ) {}

  static fromRecord(raw: Record<string, string>): WatermarkStore {
    const entries = new Map<string, Watermark>()
    for (const [key, value] of Object.entries(raw)) {
      entries.set(key, Watermark.fromString(value))
    }
    return new WatermarkStore(entries)
  }

  static empty(): WatermarkStore {
    return new WatermarkStore(new Map())
  }

  get(key: WatermarkKey): Watermark | undefined {
    return this.entries.get(key.toString())
  }

  set(key: WatermarkKey, value: Watermark): WatermarkStore {
    const updated = new Map(this.entries)
    updated.set(key.toString(), value)
    return new WatermarkStore(updated)
  }

  toRecord(): Record<string, string> {
    const result: Record<string, string> = {}
    for (const [key, watermark] of this.entries) {
      result[key] = watermark.toString()
    }
    return result
  }
}

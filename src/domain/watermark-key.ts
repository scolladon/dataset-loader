import { type EntryShape } from '../ports/types.js'

export class WatermarkKey {
  private constructor(private readonly value: string) {}

  static fromEntry(entry: EntryShape): WatermarkKey {
    if (entry.name) {
      return new WatermarkKey(entry.name)
    }
    if (entry.type === 'elf') {
      return new WatermarkKey(
        `${entry.sourceOrg}:elf:${entry.eventType}:${entry.interval}`
      )
    }
    return new WatermarkKey(`${entry.sourceOrg}:sobject:${entry.sobject}`)
  }

  toString(): string {
    return this.value
  }
}

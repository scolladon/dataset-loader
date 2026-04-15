import { type EntryShape, isElf, isSObject } from '../ports/types.js'

export class WatermarkKey {
  private constructor(private readonly value: string) {}

  static fromEntry(entry: EntryShape): WatermarkKey {
    if (entry.name) {
      return new WatermarkKey(entry.name)
    }
    if (isElf(entry)) {
      return new WatermarkKey(
        `${entry.sourceOrg}:elf:${entry.eventLog}:${entry.interval}`
      )
    }
    if (isSObject(entry)) {
      return new WatermarkKey(`${entry.sourceOrg}:sobject:${entry.sObject}`)
    }
    return new WatermarkKey(`csv:${entry.csvFile}`)
  }

  toString(): string {
    return this.value
  }
}

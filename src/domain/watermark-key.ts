import {
  type EntryShape,
  isElfShape,
  isSObjectShape,
} from '../ports/types.js'

export class WatermarkKey {
  private constructor(private readonly value: string) {}

  static fromEntry(entry: EntryShape): WatermarkKey {
    if (entry.name) {
      return new WatermarkKey(entry.name)
    }
    if (isElfShape(entry)) {
      return new WatermarkKey(
        `${entry.sourceOrg}:elf:${entry.eventLog}:${entry.interval}`
      )
    }
    if (isSObjectShape(entry)) {
      return new WatermarkKey(`${entry.sourceOrg}:sobject:${entry.sObject}`)
    }
    return new WatermarkKey(`csv:${entry.csvFile}`)
  }

  toString(): string {
    return this.value
  }
}

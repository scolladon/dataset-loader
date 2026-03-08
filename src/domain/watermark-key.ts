interface ElfShape {
  readonly name?: string
  readonly type: 'elf'
  readonly sourceOrg: string
  readonly eventType: string
  readonly interval: string
}

interface SObjectShape {
  readonly name?: string
  readonly type: 'sobject'
  readonly sourceOrg: string
  readonly sobject: string
}

type EntryShape = ElfShape | SObjectShape

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

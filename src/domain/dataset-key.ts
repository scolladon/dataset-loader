export class DatasetKey {
  private constructor(
    private readonly targetOrg: string | undefined,
    private readonly target: string
  ) {}

  static fromEntry(entry: {
    targetOrg?: string
    targetDataset?: string
    targetFile?: string
  }): DatasetKey {
    // `=== ''` already excludes `undefined`; no need for a separate
    // `!== undefined` guard (would be an equivalent-mutant magnet).
    if (entry.targetOrg === '') {
      throw new Error('targetOrg must not be empty')
    }
    if (entry.targetOrg) {
      return new DatasetKey(entry.targetOrg, entry.targetDataset!)
    }
    return new DatasetKey(undefined, entry.targetFile!)
  }

  get org(): string | undefined {
    return this.targetOrg
  }

  get name(): string {
    return this.target
  }

  toString(): string {
    return this.targetOrg
      ? `org:${this.targetOrg}:${this.target}`
      : `file:${this.target}`
  }
}

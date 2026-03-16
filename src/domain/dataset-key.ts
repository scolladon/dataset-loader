export class DatasetKey {
  private constructor(
    private readonly analyticOrg: string | undefined,
    private readonly dataset: string
  ) {}

  static fromEntry(entry: {
    analyticOrg?: string
    dataset: string
  }): DatasetKey {
    if (entry.analyticOrg !== undefined && entry.analyticOrg === '') {
      throw new Error('analyticOrg must not be empty')
    }
    return new DatasetKey(entry.analyticOrg, entry.dataset)
  }

  get org(): string | undefined {
    return this.analyticOrg
  }

  get name(): string {
    return this.dataset
  }

  toString(): string {
    return this.analyticOrg
      ? `org:${this.analyticOrg}:${this.dataset}`
      : `file:${this.dataset}`
  }
}

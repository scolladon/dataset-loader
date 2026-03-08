export class DatasetKey {
  private constructor(
    private readonly analyticOrg: string,
    private readonly dataset: string
  ) {}

  static fromEntry(entry: {
    analyticOrg: string
    dataset: string
  }): DatasetKey {
    return new DatasetKey(entry.analyticOrg, entry.dataset)
  }

  get org(): string {
    return this.analyticOrg
  }

  get name(): string {
    return this.dataset
  }

  toString(): string {
    return `${this.analyticOrg}:${this.dataset}`
  }
}

const ISO_8601_PATTERN =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{4})$/

export class Watermark {
  private constructor(private readonly value: string) {}

  static fromString(value: string): Watermark {
    if (!ISO_8601_PATTERN.test(value)) {
      throw new Error(`Invalid watermark: '${value}' is not ISO 8601`)
    }
    return new Watermark(value)
  }

  toSoqlLiteral(): string {
    return this.value
  }

  toString(): string {
    return this.value
  }
}

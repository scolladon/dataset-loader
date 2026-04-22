import { type DateBounds } from './date-bounds.js'

// NUL (\0) separates key components so no component value can collide with
// the separator. SF identifiers, field lists, interval values, and
// DateBounds.toString() never contain control characters. The `where` clause
// is user-provided SOQL; SOQL itself rejects NUL bytes, so even a malicious
// `where` can't forge a collision.
const KEY_SEPARATOR = '\0'

export class ReaderKey {
  private constructor(private readonly value: string) {}

  static forElf(
    sourceOrg: string,
    eventType: string,
    interval: string,
    bounds: DateBounds
  ): ReaderKey {
    return new ReaderKey(
      ['elf', sourceOrg, eventType, interval, bounds.toString()].join(
        KEY_SEPARATOR
      )
    )
  }

  static forSObject(
    sourceOrg: string,
    sobject: string,
    fields: readonly string[],
    dateField: string,
    where: string | undefined,
    queryLimit: number | undefined,
    bounds: DateBounds
  ): ReaderKey {
    return new ReaderKey(
      [
        'sobject',
        sourceOrg,
        sobject,
        fields.join(','),
        dateField,
        where ?? '',
        String(queryLimit ?? 0),
        bounds.toString(),
      ].join(KEY_SEPARATOR)
    )
  }

  static forCsv(filePath: string): ReaderKey {
    return new ReaderKey(['csv', filePath].join(KEY_SEPARATOR))
  }

  toString(): string {
    return this.value
  }
}

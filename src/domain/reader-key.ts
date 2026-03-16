export class ReaderKey {
  private constructor(private readonly value: string) {}

  static forElf(
    sourceOrg: string,
    eventType: string,
    interval: string
  ): ReaderKey {
    return new ReaderKey(['elf', sourceOrg, eventType, interval].join('\0'))
  }

  static forSObject(
    sourceOrg: string,
    sobject: string,
    fields: readonly string[],
    dateField: string,
    where: string | undefined,
    queryLimit: number | undefined
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
      ].join('\0')
    )
  }

  static forCsv(filePath: string): ReaderKey {
    return new ReaderKey(['csv', filePath].join('\0'))
  }

  toString(): string {
    return this.value
  }
}

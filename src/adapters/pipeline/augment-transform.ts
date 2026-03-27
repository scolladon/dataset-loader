function csvQuote(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

export function buildAugmentSuffix(columns: Record<string, string>): string {
  const values = Object.values(columns)
  if (values.length === 0) return ''
  return ',' + values.map(csvQuote).join(',')
}

export function buildAugmentHeaderSuffix(
  columns: Record<string, string>
): string {
  const keys = Object.keys(columns)
  if (keys.length === 0) return ''
  return ',' + keys.join(',')
}

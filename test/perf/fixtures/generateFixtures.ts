type FixtureSize = 'small' | 'medium' | 'large'

const ROW_COUNTS: Record<FixtureSize, number> = {
  small: 1_000,
  medium: 10_000,
  large: 100_000,
}

const COLUMNS = ['Id', 'Name', 'Email', 'Status', 'CreatedDate']
const CSV_HEADER = COLUMNS.join(',')

const pad = (n: number): string => String(n).padStart(6, '0')

const generateRow = (index: number): string =>
  `"001${pad(index)}","User ${index}","user${index}@example.com","Active","2026-01-15T10:30:00.000Z"`

export const generateCsvLines = (size: FixtureSize): string[] => {
  const count = ROW_COUNTS[size]
  const lines = [CSV_HEADER]
  for (let i = 0; i < count; i++) {
    lines.push(generateRow(i))
  }
  return lines
}

export const generateCsvBatches = (
  size: FixtureSize,
  batchSize = 2000
): string[][] => {
  const lines = generateCsvLines(size)
  const header = lines[0]
  const dataLines = lines.slice(1)
  const batches: string[][] = []

  for (let i = 0; i < dataLines.length; i += batchSize) {
    const batch = dataLines.slice(i, i + batchSize)
    if (i === 0) {
      batch.unshift(header)
    }
    batches.push(batch)
  }

  return batches
}

export const generateAugmentColumns = (
  count: number
): Record<string, string> => {
  const columns: Record<string, string> = {}
  for (let i = 0; i < count; i++) {
    columns[`AugCol_${i}`] = `augmented_value_${i}`
  }
  return columns
}

interface ConfigEntry {
  readonly sourceOrg: string
  readonly type: string
  readonly targetOrg: string
  readonly dataset: string
}

export const generateConfigEntries = (count: number): ConfigEntry[] =>
  Array.from({ length: count }, (_, i) => ({
    sourceOrg: `org-${i % 3}`,
    type: i % 2 === 0 ? 'elf' : 'sobject',
    targetOrg: `target-${i % 2}`,
    dataset: `Dataset_${pad(i)}`,
  }))

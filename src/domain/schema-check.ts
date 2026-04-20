type SchemaCheckResult =
  | { readonly ok: true; readonly casingDiff: boolean }
  | { readonly ok: false; readonly reason: string }

interface SchemaCheckInput {
  readonly datasetName: string
  readonly entryLabel: string
  readonly expected: readonly string[]
  readonly provided: readonly string[]
  readonly checkOrder: boolean
}

const normalize = (name: string): string => name.toLowerCase()

export function checkSchemaAlignment(
  input: SchemaCheckInput
): SchemaCheckResult {
  const expectedNormalized = input.expected.map(normalize)
  const providedNormalized = input.provided.map(normalize)
  const expectedSet = new Set(expectedNormalized)
  const providedSet = new Set(providedNormalized)

  const missing = input.expected.filter(
    name => !providedSet.has(normalize(name))
  )
  const extra = input.provided.filter(name => !expectedSet.has(normalize(name)))

  if (missing.length > 0 || extra.length > 0) {
    return {
      ok: false,
      reason: formatSetMismatch(input, missing, extra),
    }
  }

  if (input.checkOrder) {
    const orderDiffs = collectOrderDiffs(
      input.expected,
      input.provided,
      expectedNormalized,
      providedNormalized
    )
    if (orderDiffs.length > 0) {
      return {
        ok: false,
        reason: formatOrderMismatch(input, orderDiffs),
      }
    }
  }

  // Sets already match case-insensitively. Casing diff iff any provided
  // name (with its original case) is not in the expected set (with original
  // case). Order-independent, ignores duplicates.
  const expectedExact = new Set(input.expected)
  const casingDiff = input.provided.some(name => !expectedExact.has(name))
  return { ok: true, casingDiff }
}

interface OrderDiff {
  readonly position: number
  readonly expected: string
  readonly provided: string
}

function collectOrderDiffs(
  expected: readonly string[],
  provided: readonly string[],
  expectedNormalized: readonly string[],
  providedNormalized: readonly string[]
): OrderDiff[] {
  // Set equality has already been validated, so expected.length === provided.length.
  const diffs: OrderDiff[] = []
  for (let i = 0; i < expected.length; i++) {
    if (expectedNormalized[i] !== providedNormalized[i]) {
      diffs.push({
        position: i,
        expected: expected[i],
        provided: provided[i],
      })
    }
  }
  return diffs
}

// Exported so the SObject projection builder formats identical messages.
export function formatSchemaMismatch(
  datasetName: string,
  entryLabel: string,
  missing: readonly string[],
  extra: readonly string[]
): string {
  const lines = [
    `Schema mismatch for dataset '${datasetName}' (entry '${entryLabel}'):`,
  ]
  if (missing.length > 0) {
    lines.push(
      `  expected by dataset, missing from input: [${missing.join(', ')}]`
    )
  }
  if (extra.length > 0) {
    lines.push(
      `  provided by input, not in dataset:       [${extra.join(', ')}]`
    )
  }
  return lines.join('\n')
}

function formatSetMismatch(
  input: SchemaCheckInput,
  missing: readonly string[],
  extra: readonly string[]
): string {
  return formatSchemaMismatch(
    input.datasetName,
    input.entryLabel,
    missing,
    extra
  )
}

function formatOrderMismatch(
  input: SchemaCheckInput,
  diffs: readonly OrderDiff[]
): string {
  const lines = [
    `Order mismatch for dataset '${input.datasetName}' (entry '${input.entryLabel}'):`,
  ]
  for (const d of diffs) {
    lines.push(
      `  position ${d.position}: dataset expects '${d.expected}', input provides '${d.provided}'`
    )
  }
  lines.push(
    '  hint: column-set is correct; reorder source columns or recreate the dataset to match.'
  )
  lines.push(
    '  note: augment columns (if any) must be the trailing columns of the dataset.'
  )
  return lines.join('\n')
}

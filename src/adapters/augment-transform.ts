import { Transform, type TransformCallback } from 'node:stream'

function csvQuote(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

export function buildAugmentSuffix(columns: Record<string, string>): string {
  const values = Object.values(columns)
  if (values.length === 0) return ''
  return ',' + values.map(csvQuote).join(',')
}

export function createAugmentTransform(suffix: string): Transform {
  return new Transform({
    objectMode: true,
    transform(line: string, _enc: string, cb: TransformCallback) {
      cb(null, line + suffix)
    },
  })
}

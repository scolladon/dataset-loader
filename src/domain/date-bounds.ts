import { Watermark } from './watermark.js'

// Calendar validity check via UTC-components round-trip.
//
// Why not Number.isNaN(Date.parse(iso)): V8's Date.parse is lenient on
// calendar-invalid inputs. '2026-02-30T00:00:00Z' does NOT return NaN;
// it rolls over to March 2 — a silent acceptance of an invalid date.
// We need a stricter guard.
//
// The input is already ISO_8601_PATTERN-validated by Watermark.fromString
// (caller `parseFlag` below), so the first 10 characters are always
// `YYYY-MM-DD`. We parse Y/M/D, construct a UTC Date, and compare the
// Date's round-tripped `YYYY-MM-DD` prefix against the input's.
// Feb 30 → constructed Date is March 2 → round-trip is '2026-03-02' !=
// '2026-02-30' → returns false. One equality catches any
// single-component mismatch, making every mutation killable.
function isValidCalendarDate(iso: string): boolean {
  const ymd = iso.slice(0, 10)
  const [yearStr, monthStr, dayStr] = ymd.split('-')
  const year = Number.parseInt(yearStr, 10)
  const month = Number.parseInt(monthStr, 10)
  const day = Number.parseInt(dayStr, 10)
  const date = new Date(Date.UTC(year, month - 1, day))
  return date.toISOString().slice(0, 10) === ymd
}

export class DateBounds {
  // Pre-parsed epoch-ms for startAt/endAt, cached at construction so every
  // bounds × watermark comparison parses the watermark side only (the bounds
  // side is stable per instance).
  private readonly startMs?: number
  private readonly endMs?: number

  private constructor(
    private readonly startAt?: Watermark,
    private readonly endAt?: Watermark
  ) {
    this.startMs = startAt ? Date.parse(startAt.toString()) : undefined
    this.endMs = endAt ? Date.parse(endAt.toString()) : undefined
  }

  static from(start: string | undefined, end: string | undefined): DateBounds {
    const s = DateBounds.parseFlag('--start-date', start)
    const e = DateBounds.parseFlag('--end-date', end)
    if (s && e && Date.parse(s.toString()) > Date.parse(e.toString())) {
      throw new Error(`--start-date ${s} must be <= --end-date ${e}`)
    }
    return new DateBounds(s, e)
  }

  static none(): DateBounds {
    return new DateBounds()
  }

  isEmpty(): boolean {
    return !this.startAt && !this.endAt
  }

  lowerConditionFor(
    dateField: string,
    watermark: Watermark | undefined
  ): string | undefined {
    if (this.startAt) return `${dateField} >= ${this.startAt.toSoqlLiteral()}`
    if (watermark) return `${dateField} > ${watermark.toSoqlLiteral()}`
    return undefined
  }

  upperConditionFor(dateField: string): string | undefined {
    return this.endAt
      ? `${dateField} <= ${this.endAt.toSoqlLiteral()}`
      : undefined
  }

  rewindsBelow(watermark: Watermark | undefined): boolean {
    // Stryker disable next-line ConditionalExpression: the LHS `false` mutation
    // is equivalent — when `startMs` is undefined and `watermark` is defined,
    // the mutant falls through to `undefined < number`, which JS evaluates to
    // `false` (not a TypeError), matching the guarded path's return value.
    if (this.startMs === undefined || !watermark) return false
    return this.startMs < Date.parse(watermark.toString())
  }

  leavesHoleAbove(watermark: Watermark | undefined): boolean {
    // Stryker disable next-line ConditionalExpression: equivalent — see `rewindsBelow`.
    if (this.startMs === undefined || !watermark) return false
    return this.startMs > Date.parse(watermark.toString())
  }

  matchesWatermark(watermark: Watermark | undefined): boolean {
    // Stryker disable next-line ConditionalExpression: equivalent — see `rewindsBelow`.
    if (this.startMs === undefined || !watermark) return false
    return this.startMs === Date.parse(watermark.toString())
  }

  endsBeforeWatermark(watermark: Watermark | undefined): boolean {
    // Stryker disable next-line ConditionalExpression: equivalent — see `rewindsBelow`.
    if (this.endMs === undefined || !watermark) return false
    return this.endMs < Date.parse(watermark.toString())
  }

  toString(): string {
    return `[${this.startAt?.toString() ?? '-∞'}, ${this.endAt?.toString() ?? '+∞'}]`
  }

  private static parseFlag(
    flag: string,
    value: string | undefined
  ): Watermark | undefined {
    if (value === undefined) return undefined
    const w = DateBounds.parseIsoOrThrow(flag, value)
    if (!isValidCalendarDate(w.toString())) {
      throw new Error(`${flag} is not a valid calendar date: '${value}'`)
    }
    return w
  }

  private static parseIsoOrThrow(flag: string, value: string): Watermark {
    try {
      return Watermark.fromString(value)
    } catch {
      throw new Error(`${flag} is not a valid ISO-8601 datetime: '${value}'`)
    }
  }
}

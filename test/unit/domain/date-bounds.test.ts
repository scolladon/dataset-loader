import { describe, expect, it } from 'vitest'
import { DateBounds } from '../../../src/domain/date-bounds.js'
import { Watermark } from '../../../src/domain/watermark.js'

const ISO_JAN = '2026-01-01T00:00:00.000Z'
const ISO_FEB = '2026-02-01T00:00:00.000Z'
const ISO_MAR = '2026-03-01T00:00:00.000Z'

const wm = (iso: string): Watermark => Watermark.fromString(iso)

describe('DateBounds', () => {
  describe('from', () => {
    it('given undefined start and undefined end, when creating, then isEmpty is true and toString is infinity', () => {
      // Arrange / Act
      const sut = DateBounds.from(undefined, undefined)

      // Assert
      expect(sut.isEmpty()).toBe(true)
      expect(sut.toString()).toBe('[-∞, +∞]')
    })

    it('given valid start only, when creating, then not empty and upperConditionFor is undefined', () => {
      // Arrange / Act
      const sut = DateBounds.from(ISO_JAN, undefined)

      // Assert
      expect(sut.isEmpty()).toBe(false)
      expect(sut.upperConditionFor('DF')).toBeUndefined()
    })

    it('given valid end only, when creating, then not empty and lowerConditionFor with no watermark is undefined', () => {
      // Arrange / Act
      const sut = DateBounds.from(undefined, ISO_JAN)

      // Assert
      expect(sut.isEmpty()).toBe(false)
      expect(sut.lowerConditionFor('DF', undefined)).toBeUndefined()
    })

    it('given malformed start, when creating, then throws with flag name and the bad value', () => {
      // Act & Assert — regex assertion: resilient to wording changes,
      // asserts only that flag name + bad value are surfaced.
      expect(() => DateBounds.from('not-a-date', undefined)).toThrow(
        /--start-date.*not-a-date/
      )
    })

    it('given malformed end, when creating, then throws with flag name and the bad value', () => {
      // Act & Assert
      expect(() => DateBounds.from(undefined, 'garbage')).toThrow(
        /--end-date.*garbage/
      )
    })

    it('given calendar-invalid Feb 30 start, when creating, then throws calendar error', () => {
      // Act & Assert — V8 Date.parse is lenient on Feb 30 (rolls to Mar 2),
      // so the guard is a round-trip UTC-components check.
      expect(() =>
        DateBounds.from('2026-02-30T00:00:00.000Z', undefined)
      ).toThrow(/--start-date.*calendar date.*2026-02-30T00:00:00\.000Z/)
    })

    it('given calendar-invalid month 13 start, when creating, then throws calendar error', () => {
      // Act & Assert
      expect(() =>
        DateBounds.from('2026-13-01T00:00:00.000Z', undefined)
      ).toThrow(/--start-date.*calendar date.*2026-13-01T00:00:00\.000Z/)
    })

    it('given calendar-invalid day 32 start, when creating, then throws calendar error', () => {
      // Act & Assert
      expect(() =>
        DateBounds.from('2026-01-32T00:00:00.000Z', undefined)
      ).toThrow(/--start-date.*calendar date.*2026-01-32T00:00:00\.000Z/)
    })

    it('given calendar-invalid end, when creating, then throws calendar error for end flag', () => {
      // Act & Assert
      expect(() =>
        DateBounds.from(undefined, '2026-02-30T00:00:00.000Z')
      ).toThrow(/--end-date.*calendar date.*2026-02-30T00:00:00\.000Z/)
    })

    it('given start after end, when creating, then throws with both values in message', () => {
      // Act & Assert
      expect(() => DateBounds.from(ISO_MAR, ISO_JAN)).toThrow(
        /--start-date .*2026-03-01.* must be <= --end-date .*2026-01-01/
      )
    })

    it('given start equal to end, when creating, then accepts (boundary case kills > vs >= mutation)', () => {
      // Act — same instant on both sides is a valid degenerate window
      const sut = DateBounds.from(ISO_JAN, ISO_JAN)

      // Assert
      expect(sut.isEmpty()).toBe(false)
      expect(sut.toString()).toBe(`[${ISO_JAN}, ${ISO_JAN}]`)
    })

    it('given start in Z and end in offset representing valid chronological order, when creating, then accepts and retains both bounds verbatim (epoch-ms comparison, no normalization)', () => {
      // Arrange
      const startInZ = '2026-01-01T00:00:00.000Z' // instant: 2026-01-01T00:00:00Z
      const endInOffset = '2026-01-01T10:00:00.000+02:00' // instant: 2026-01-01T08:00:00Z — chronologically after

      // Act
      const sut = DateBounds.from(startInZ, endInOffset)

      // Assert — epoch-ms comparison accepts; lex would order start > end incorrectly.
      // Also kills a mutation that drops `endAt`: both conditions must surface.
      expect(sut.isEmpty()).toBe(false)
      expect(sut.lowerConditionFor('DF', undefined)).toBe(`DF >= ${startInZ}`)
      expect(sut.upperConditionFor('DF')).toBe(`DF <= ${endInOffset}`)
    })
  })

  describe('none', () => {
    it('given no inputs, when calling none, then isEmpty is true', () => {
      // Arrange / Act
      const sut = DateBounds.none()

      // Assert
      expect(sut.isEmpty()).toBe(true)
    })
  })

  describe('hasStart / hasEnd', () => {
    it('given none, when checking flags, then both false', () => {
      // Arrange / Act
      const sut = DateBounds.none()

      // Assert
      expect(sut.hasStart()).toBe(false)
      expect(sut.hasEnd()).toBe(false)
    })

    it('given only start, when checking flags, then hasStart is true and hasEnd is false', () => {
      // Arrange / Act
      const sut = DateBounds.from(ISO_JAN, undefined)

      // Assert
      expect(sut.hasStart()).toBe(true)
      expect(sut.hasEnd()).toBe(false)
    })

    it('given only end, when checking flags, then hasStart is false and hasEnd is true', () => {
      // Arrange / Act
      const sut = DateBounds.from(undefined, ISO_MAR)

      // Assert
      expect(sut.hasStart()).toBe(false)
      expect(sut.hasEnd()).toBe(true)
    })

    it('given both, when checking flags, then both true', () => {
      // Arrange / Act
      const sut = DateBounds.from(ISO_JAN, ISO_MAR)

      // Assert
      expect(sut.hasStart()).toBe(true)
      expect(sut.hasEnd()).toBe(true)
    })
  })

  describe('lowerConditionFor', () => {
    it('given no sd and no wm, when asking for lower, then returns undefined', () => {
      // Arrange
      const sut = DateBounds.none()

      // Assert
      expect(sut.lowerConditionFor('DF', undefined)).toBeUndefined()
    })

    it('given only watermark, when asking for lower, then returns strict greater clause', () => {
      // Arrange
      const sut = DateBounds.none()

      // Assert
      expect(sut.lowerConditionFor('DF', wm(ISO_FEB))).toBe(`DF > ${ISO_FEB}`)
    })

    it('given only sd, when asking for lower, then returns inclusive geq clause', () => {
      // Arrange
      const sut = DateBounds.from(ISO_JAN, undefined)

      // Assert
      expect(sut.lowerConditionFor('DF', undefined)).toBe(`DF >= ${ISO_JAN}`)
    })

    it('given start strictly less than watermark, when asking for lower, then start-date wins with geq', () => {
      // Arrange
      const sut = DateBounds.from(ISO_JAN, undefined)

      // Assert — regression guard: --start-date always wins over watermark
      expect(sut.lowerConditionFor('DF', wm(ISO_FEB))).toBe(`DF >= ${ISO_JAN}`)
    })

    it('given start equal to watermark, when asking for lower, then start-date wins with geq', () => {
      // Arrange
      const sut = DateBounds.from(ISO_FEB, undefined)

      // Assert
      expect(sut.lowerConditionFor('DF', wm(ISO_FEB))).toBe(`DF >= ${ISO_FEB}`)
    })

    it('given start strictly greater than watermark, when asking for lower, then start-date wins with geq', () => {
      // Arrange
      const sut = DateBounds.from(ISO_MAR, undefined)

      // Assert
      expect(sut.lowerConditionFor('DF', wm(ISO_FEB))).toBe(`DF >= ${ISO_MAR}`)
    })

    it('given only end-date with watermark, when asking for lower, then watermark wins with strict gt (start absent)', () => {
      // Arrange — covers the `!startAt && watermark` branch at the unit level;
      // otherwise this path is only exercised through SObjectReader integration.
      const sut = DateBounds.from(undefined, ISO_MAR)

      // Assert
      expect(sut.lowerConditionFor('DF', wm(ISO_FEB))).toBe(`DF > ${ISO_FEB}`)
    })
  })

  describe('upperConditionFor', () => {
    it('given no ed, when asking for upper, then returns undefined', () => {
      // Arrange
      const sut = DateBounds.none()

      // Assert
      expect(sut.upperConditionFor('DF')).toBeUndefined()
    })

    it('given ed, when asking for upper, then returns inclusive leq clause', () => {
      // Arrange
      const sut = DateBounds.from(undefined, ISO_MAR)

      // Assert
      expect(sut.upperConditionFor('DF')).toBe(`DF <= ${ISO_MAR}`)
    })

    it('given start and end, when asking for upper, then returns leq on end (start does not mask end)', () => {
      // Arrange — kills a mutation that short-circuits upperConditionFor when --start-date is set.
      const sut = DateBounds.from(ISO_JAN, ISO_MAR)

      // Assert
      expect(sut.upperConditionFor('DF')).toBe(`DF <= ${ISO_MAR}`)
    })
  })

  describe('rewindsBelow', () => {
    it('given no sd with any watermark, when checking rewind, then false', () => {
      // Arrange
      const sut = DateBounds.none()

      // Assert
      expect(sut.rewindsBelow(wm(ISO_FEB))).toBe(false)
    })

    it('given sd with no watermark, when checking rewind, then false', () => {
      // Arrange
      const sut = DateBounds.from(ISO_JAN, undefined)

      // Assert
      expect(sut.rewindsBelow(undefined)).toBe(false)
    })

    it('given sd strictly less than wm, when checking rewind, then true', () => {
      // Arrange
      const sut = DateBounds.from(ISO_JAN, undefined)

      // Assert
      expect(sut.rewindsBelow(wm(ISO_FEB))).toBe(true)
    })

    it('given sd equal to wm, when checking rewind, then false', () => {
      // Arrange
      const sut = DateBounds.from(ISO_FEB, undefined)

      // Assert
      expect(sut.rewindsBelow(wm(ISO_FEB))).toBe(false)
    })

    it('given sd strictly greater than wm, when checking rewind, then false', () => {
      // Arrange
      const sut = DateBounds.from(ISO_MAR, undefined)

      // Assert
      expect(sut.rewindsBelow(wm(ISO_FEB))).toBe(false)
    })

    it('given mixed offsets representing same instant, when checking rewind, then false', () => {
      // Arrange — sd '2026-02-01T02:00:00.000+02:00' == 2026-02-01T00:00:00Z == ISO_FEB
      const sut = DateBounds.from('2026-02-01T02:00:00.000+02:00', undefined)

      // Assert
      expect(sut.rewindsBelow(wm(ISO_FEB))).toBe(false)
    })

    it('given sd exactly 1ms before wm, when checking rewind, then true (boundary case kills <= vs < mutation)', () => {
      // Arrange — sd = 2026-02-01T00:00:00.000Z - 1ms = 2026-01-31T23:59:59.999Z
      const sut = DateBounds.from('2026-01-31T23:59:59.999Z', undefined)

      // Assert
      expect(sut.rewindsBelow(wm(ISO_FEB))).toBe(true)
    })

    it('given sd exactly 1ms after wm, when checking rewind, then false (boundary case kills < vs <= mutation)', () => {
      // Arrange — sd = 2026-02-01T00:00:00.001Z
      const sut = DateBounds.from('2026-02-01T00:00:00.001Z', undefined)

      // Assert
      expect(sut.rewindsBelow(wm(ISO_FEB))).toBe(false)
    })
  })

  describe('leavesHoleAbove', () => {
    it('given no sd with any wm, when checking hole, then false', () => {
      // Arrange
      const sut = DateBounds.none()

      // Assert
      expect(sut.leavesHoleAbove(wm(ISO_FEB))).toBe(false)
    })

    it('given sd with no wm, when checking hole, then false', () => {
      // Arrange
      const sut = DateBounds.from(ISO_JAN, undefined)

      // Assert
      expect(sut.leavesHoleAbove(undefined)).toBe(false)
    })

    it('given sd strictly less than wm, when checking hole, then false', () => {
      // Arrange
      const sut = DateBounds.from(ISO_JAN, undefined)

      // Assert
      expect(sut.leavesHoleAbove(wm(ISO_FEB))).toBe(false)
    })

    it('given sd equal to wm, when checking hole, then false', () => {
      // Arrange
      const sut = DateBounds.from(ISO_FEB, undefined)

      // Assert
      expect(sut.leavesHoleAbove(wm(ISO_FEB))).toBe(false)
    })

    it('given sd strictly greater than wm, when checking hole, then true', () => {
      // Arrange
      const sut = DateBounds.from(ISO_MAR, undefined)

      // Assert
      expect(sut.leavesHoleAbove(wm(ISO_FEB))).toBe(true)
    })

    it('given mixed offsets with sd strictly after wm, when checking hole, then true', () => {
      // Arrange — sd instant = 2026-03-01T00:00:00Z, after ISO_FEB
      const sut = DateBounds.from('2026-03-01T02:00:00.000+02:00', undefined)

      // Assert
      expect(sut.leavesHoleAbove(wm(ISO_FEB))).toBe(true)
    })

    it('given sd exactly 1ms after wm, when checking hole, then true (boundary case kills >= vs > mutation)', () => {
      // Arrange — sd = 2026-02-01T00:00:00.001Z
      const sut = DateBounds.from('2026-02-01T00:00:00.001Z', undefined)

      // Assert
      expect(sut.leavesHoleAbove(wm(ISO_FEB))).toBe(true)
    })

    it('given sd exactly 1ms before wm, when checking hole, then false (boundary case kills > vs >= mutation)', () => {
      // Arrange — sd = 2026-01-31T23:59:59.999Z
      const sut = DateBounds.from('2026-01-31T23:59:59.999Z', undefined)

      // Assert
      expect(sut.leavesHoleAbove(wm(ISO_FEB))).toBe(false)
    })
  })

  describe('matchesWatermark', () => {
    it('given no sd with any wm, when checking match, then false', () => {
      // Arrange
      const sut = DateBounds.none()

      // Assert
      expect(sut.matchesWatermark(wm(ISO_FEB))).toBe(false)
    })

    it('given sd with no wm, when checking match, then false', () => {
      // Arrange
      const sut = DateBounds.from(ISO_JAN, undefined)

      // Assert
      expect(sut.matchesWatermark(undefined)).toBe(false)
    })

    it('given sd strictly less than wm, when checking match, then false', () => {
      // Arrange
      const sut = DateBounds.from(ISO_JAN, undefined)

      // Assert
      expect(sut.matchesWatermark(wm(ISO_FEB))).toBe(false)
    })

    it('given sd strictly greater than wm, when checking match, then false', () => {
      // Arrange
      const sut = DateBounds.from(ISO_MAR, undefined)

      // Assert
      expect(sut.matchesWatermark(wm(ISO_FEB))).toBe(false)
    })

    it('given sd exactly equal to wm (same ms), when checking match, then true', () => {
      // Arrange
      const sut = DateBounds.from(ISO_FEB, undefined)

      // Assert
      expect(sut.matchesWatermark(wm(ISO_FEB))).toBe(true)
    })

    it('given sd and wm representing same instant in different offsets, when checking match, then true', () => {
      // Arrange — sd instant = 2026-02-01T00:00:00Z = ISO_FEB
      const sut = DateBounds.from('2026-02-01T02:00:00.000+02:00', undefined)

      // Assert
      expect(sut.matchesWatermark(wm(ISO_FEB))).toBe(true)
    })

    it('given sd exactly 1ms before wm, when checking match, then false (boundary kills == vs <= mutation)', () => {
      // Arrange
      const sut = DateBounds.from('2026-01-31T23:59:59.999Z', undefined)

      // Assert
      expect(sut.matchesWatermark(wm(ISO_FEB))).toBe(false)
    })

    it('given sd exactly 1ms after wm, when checking match, then false (boundary kills == vs >= mutation)', () => {
      // Arrange
      const sut = DateBounds.from('2026-02-01T00:00:00.001Z', undefined)

      // Assert
      expect(sut.matchesWatermark(wm(ISO_FEB))).toBe(false)
    })
  })

  describe('endsBeforeWatermark', () => {
    it('given no ed with any wm, when checking ends-before, then false', () => {
      // Arrange
      const sut = DateBounds.none()

      // Assert
      expect(sut.endsBeforeWatermark(wm(ISO_FEB))).toBe(false)
    })

    it('given ed with no wm, when checking ends-before, then false', () => {
      // Arrange
      const sut = DateBounds.from(undefined, ISO_JAN)

      // Assert
      expect(sut.endsBeforeWatermark(undefined)).toBe(false)
    })

    it('given ed strictly less than wm, when checking ends-before, then true', () => {
      // Arrange
      const sut = DateBounds.from(undefined, ISO_JAN)

      // Assert
      expect(sut.endsBeforeWatermark(wm(ISO_FEB))).toBe(true)
    })

    it('given ed equal to wm, when checking ends-before, then false', () => {
      // Arrange
      const sut = DateBounds.from(undefined, ISO_FEB)

      // Assert
      expect(sut.endsBeforeWatermark(wm(ISO_FEB))).toBe(false)
    })

    it('given ed strictly greater than wm, when checking ends-before, then false', () => {
      // Arrange
      const sut = DateBounds.from(undefined, ISO_MAR)

      // Assert
      expect(sut.endsBeforeWatermark(wm(ISO_FEB))).toBe(false)
    })

    it('given mixed offsets with ed strictly before wm, when checking ends-before, then true', () => {
      // Arrange — ed instant = 2025-12-31T23:00:00Z, before ISO_FEB
      const sut = DateBounds.from(undefined, '2026-01-01T01:00:00.000+02:00')

      // Assert
      expect(sut.endsBeforeWatermark(wm(ISO_FEB))).toBe(true)
    })

    it('given ed exactly 1ms before wm, when checking ends-before, then true (boundary kills <= vs < mutation)', () => {
      // Arrange
      const sut = DateBounds.from(undefined, '2026-01-31T23:59:59.999Z')

      // Assert
      expect(sut.endsBeforeWatermark(wm(ISO_FEB))).toBe(true)
    })

    it('given ed exactly 1ms after wm, when checking ends-before, then false (boundary kills < vs <= mutation)', () => {
      // Arrange
      const sut = DateBounds.from(undefined, '2026-02-01T00:00:00.001Z')

      // Assert
      expect(sut.endsBeforeWatermark(wm(ISO_FEB))).toBe(false)
    })
  })

  describe('toString', () => {
    it('given empty bounds, when toString, then returns minus-inf plus-inf', () => {
      // Arrange
      const sut = DateBounds.none()

      // Assert
      expect(sut.toString()).toBe('[-∞, +∞]')
    })

    it('given only start, when toString, then iso plus-inf', () => {
      // Arrange
      const sut = DateBounds.from(ISO_JAN, undefined)

      // Assert
      expect(sut.toString()).toBe(`[${ISO_JAN}, +∞]`)
    })

    it('given only end, when toString, then minus-inf iso', () => {
      // Arrange
      const sut = DateBounds.from(undefined, ISO_MAR)

      // Assert
      expect(sut.toString()).toBe(`[-∞, ${ISO_MAR}]`)
    })

    it('given both, when toString, then iso iso', () => {
      // Arrange
      const sut = DateBounds.from(ISO_JAN, ISO_MAR)

      // Assert
      expect(sut.toString()).toBe(`[${ISO_JAN}, ${ISO_MAR}]`)
    })
  })
})

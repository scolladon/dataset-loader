import { describe, expect, it } from 'vitest'
import { ProgressReporter } from '../../../src/adapters/progress-reporter.js'

describe('ProgressReporter', () => {
  it('given zero total, when creating phase, then tick and stop are safe no-ops', () => {
    // Arrange
    const sut = new ProgressReporter()

    // Act
    const phase = sut.create('Fetching', 0)

    // Assert
    expect(() => phase.tick('detail')).not.toThrow()
    expect(() => phase.stop()).not.toThrow()
  })

  it('given positive total, when ticking and stopping, then does not throw', () => {
    // Arrange
    const sut = new ProgressReporter()

    // Act
    const phase = sut.create('Fetching', 2)

    // Assert
    expect(() => phase.tick('item 1')).not.toThrow()
    expect(() => phase.tick('item 2')).not.toThrow()
    expect(() => phase.stop()).not.toThrow()
  })

  it('given total of one, when creating phase, then creates phase without error', () => {
    // Arrange
    const sut = new ProgressReporter()

    // Act
    const phase = sut.create('Fetching', 1)

    // Assert
    expect(() => phase.tick('single')).not.toThrow()
    expect(() => phase.stop()).not.toThrow()
  })
})

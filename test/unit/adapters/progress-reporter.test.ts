import { describe, expect, it } from 'vitest'
import { ProgressReporter } from '../../../src/adapters/progress-reporter.js'

describe('ProgressReporter', () => {
  it('given zero total, when creating phase, then returns callable tick, trackGroup and stop', () => {
    // Arrange
    const sut = new ProgressReporter()

    // Act
    const phase = sut.create('Fetching', 0)

    // Assert
    expect(typeof phase.tick).toBe('function')
    expect(typeof phase.trackGroup).toBe('function')
    expect(typeof phase.stop).toBe('function')
    expect(() => phase.tick('detail')).not.toThrow()
    expect(() => phase.stop()).not.toThrow()
  })

  it('given zero total, when tracking group, then returns noop group tracker', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Fetching', 0)

    // Act
    const tracker = phase.trackGroup('MyDataset')

    // Assert
    expect(typeof tracker.updateParentId).toBe('function')
    expect(typeof tracker.incrementParts).toBe('function')
    expect(typeof tracker.addFiles).toBe('function')
    expect(typeof tracker.addRows).toBe('function')
    expect(typeof tracker.stop).toBe('function')
    expect(() => tracker.updateParentId('06Vxxx')).not.toThrow()
    expect(() => tracker.incrementParts()).not.toThrow()
    expect(() => tracker.addFiles(2)).not.toThrow()
    expect(() => tracker.addRows(100)).not.toThrow()
    expect(() => tracker.stop()).not.toThrow()
  })

  it('given positive total, when ticking and stopping, then completes without error', () => {
    // Arrange
    const sut = new ProgressReporter()

    // Act
    const phase = sut.create('Fetching', 2)

    // Assert
    expect(typeof phase.tick).toBe('function')
    expect(typeof phase.trackGroup).toBe('function')
    expect(typeof phase.stop).toBe('function')
    expect(() => phase.tick('item 1')).not.toThrow()
    expect(() => phase.tick('item 2')).not.toThrow()
    expect(() => phase.stop()).not.toThrow()
  })

  it('given positive total, when tracking group, then returns functional group tracker', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Fetching', 2)

    // Act
    const tracker = phase.trackGroup('MyDataset')

    // Assert
    expect(() => tracker.updateParentId('06V000000000001')).not.toThrow()
    expect(() => tracker.addFiles(2)).not.toThrow()
    expect(() => tracker.addRows(150)).not.toThrow()
    expect(() => tracker.incrementParts()).not.toThrow()
    expect(() => tracker.incrementParts()).not.toThrow()
    expect(() => tracker.stop()).not.toThrow()
    phase.stop()
  })

  it('given total of one, when creating phase, then uses singular unit label', () => {
    // Arrange
    const sut = new ProgressReporter()

    // Act
    const phase = sut.create('Fetching', 1)

    // Assert
    expect(typeof phase.tick).toBe('function')
    expect(typeof phase.stop).toBe('function')
    expect(() => phase.tick('single')).not.toThrow()
    expect(() => phase.stop()).not.toThrow()
  })
})

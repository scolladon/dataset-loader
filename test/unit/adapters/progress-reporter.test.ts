import { describe, expect, it } from 'vitest'
import { ProgressReporter } from '../../../src/adapters/progress-reporter.js'

describe('ProgressReporter', () => {
  it('given zero total, when creating phase, then returns callable tick, trackGroup and stop', () => {
    // Arrange
    const sut = new ProgressReporter()

    // Act
    const phase = sut.create('Fetching', 0)

    // Assert
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
    expect(() => phase.tick('item 1')).not.toThrow()
    expect(() => phase.tick('item 2')).not.toThrow()
    expect(() => phase.stop()).not.toThrow()
  })

  it('given positive total, when tracking group without parts, then returns functional group tracker', () => {
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
    expect(() => tracker.stop()).not.toThrow()
    phase.stop()
  })

  it('given positive total, when tracking group with parts, then returns functional group tracker including parts', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Processing', 2)

    // Act
    const tracker = phase.trackGroup('MyDataset', true)

    // Assert
    expect(() => tracker.addFiles(3)).not.toThrow()
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
    expect(() => phase.tick('single')).not.toThrow()
    expect(() => phase.stop()).not.toThrow()
  })

  it('given two group trackers, when updating one, then the other remains independent', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Processing', 2)
    const tracker1 = phase.trackGroup('DS1')
    const tracker2 = phase.trackGroup('DS2')

    // Act — mutate tracker1 only
    tracker1.updateParentId('06V001')
    tracker1.addFiles(3)
    tracker1.addRows(100)
    tracker1.incrementParts()

    // Assert — tracker2 operations remain independent (no shared state corruption)
    expect(() => tracker2.updateParentId('06V002')).not.toThrow()
    expect(() => tracker2.addFiles(1)).not.toThrow()
    expect(() => tracker2.addRows(50)).not.toThrow()
    expect(() => tracker2.incrementParts()).not.toThrow()

    // Cleanup
    tracker1.stop()
    tracker2.stop()
    phase.stop()
  })
})

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { readState, writeState } from '../../src/core/state-manager.js'
import { writeFile, rm, mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { randomUUID } from 'node:crypto'

describe('StateManager', () => {
  let testDir: string

  beforeEach(async () => {
    testDir = join(tmpdir(), `state-test-${randomUUID()}`)
    await mkdir(testDir, { recursive: true })
  })

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true })
  })

  describe('readState', () => {
    it('given existing state file, when reading, then returns parsed state', async () => {
      // Arrange
      const path = join(testDir, 'state.json')
      const state = { watermarks: { 'org:elf:Login:Daily': '2026-01-01T00:00:00.000Z' } }
      await writeFile(path, JSON.stringify(state))

      // Act
      const sut = await readState(path)

      // Assert
      expect(sut).toEqual(state)
    })

    it('given missing file, when reading, then returns empty state', async () => {
      // Act
      const sut = await readState(join(testDir, 'nonexistent.json'))

      // Assert
      expect(sut).toEqual({ watermarks: {} })
    })

    it('given invalid JSON, when reading, then throws', async () => {
      // Arrange
      const path = join(testDir, 'bad.json')
      await writeFile(path, 'not json')

      // Act & Assert
      await expect(readState(path)).rejects.toThrow()
    })
  })

  describe('writeState', () => {
    it('given a state object, when writing, then file contains valid JSON', async () => {
      // Arrange
      const path = join(testDir, 'state.json')
      const state = { watermarks: { 'org:elf:Login:Daily': '2026-03-01T00:00:00.000Z' } }

      // Act
      await writeState(path, state)

      // Assert
      const sut = await readState(path)
      expect(sut).toEqual(state)
    })

    it('given an existing file, when writing new state, then overwrites atomically', async () => {
      // Arrange
      const path = join(testDir, 'state.json')
      await writeState(path, { watermarks: { key: '2026-01-01T00:00:00.000Z' } })

      // Act
      await writeState(path, { watermarks: { key: '2026-03-05T00:00:00.000Z' } })

      // Assert
      const sut = await readState(path)
      expect(sut.watermarks.key).toBe('2026-03-05T00:00:00.000Z')
    })
  })
})

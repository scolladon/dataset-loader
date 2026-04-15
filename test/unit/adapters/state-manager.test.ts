import { randomUUID } from 'node:crypto'
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { FileStateManager } from '../../../src/adapters/state-manager.js'
import { Watermark } from '../../../src/domain/watermark.js'
import { WatermarkKey } from '../../../src/domain/watermark-key.js'
import { WatermarkStore } from '../../../src/domain/watermark-store.js'

describe('FileStateManager', () => {
  let testDir: string

  beforeEach(async () => {
    testDir = join(tmpdir(), `state-test-${randomUUID()}`)
    await mkdir(testDir, { recursive: true })
  })

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true })
  })

  describe('read', () => {
    it('given existing state file, when reading, then returns WatermarkStore with values', async () => {
      // Arrange
      const path = join(testDir, 'state.json')
      await writeFile(
        path,
        JSON.stringify({
          watermarks: { 'org:elf:Login:Daily': '2026-01-01T00:00:00.000Z' },
        })
      )

      // Act
      const sut = new FileStateManager(path)
      const store = await sut.read()

      // Assert
      const key = WatermarkKey.fromEntry({
        sourceOrg: 'org',
        eventLog: 'Login',
        interval: 'Daily',
      })
      expect(store.get(key)!.toString()).toBe('2026-01-01T00:00:00.000Z')
    })

    it('given missing file, when reading, then returns empty WatermarkStore', async () => {
      // Arrange
      const sut = new FileStateManager(join(testDir, 'nonexistent.json'))

      // Act
      const store = await sut.read()

      // Assert
      expect(store.toRecord()).toEqual({})
    })

    it('given state file with invalid ISO timestamp, when reading, then throws with ISO 8601 message', async () => {
      // Arrange
      const path = join(testDir, 'bad-watermark.json')
      await writeFile(
        path,
        JSON.stringify({ watermarks: { 'org:elf:Login:Daily': 'not-a-date' } })
      )
      const sut = new FileStateManager(path)

      // Act & Assert — kills 'Must be ISO 8601 datetime' string mutation
      await expect(sut.read()).rejects.toThrow('Must be ISO 8601 datetime')
    })

    it('given invalid JSON, when reading, then throws a JSON parse error', async () => {
      // Arrange
      const path = join(testDir, 'bad.json')
      await writeFile(path, 'not json')
      const sut = new FileStateManager(path)

      // Act & Assert
      await expect(sut.read()).rejects.toThrow(SyntaxError)
    })
  })

  describe('write', () => {
    it('given a WatermarkStore, when writing, then file contains valid JSON with watermarks', async () => {
      // Arrange
      const path = join(testDir, 'state.json')
      const key = WatermarkKey.fromEntry({
        sourceOrg: 'org',
        eventLog: 'Login',
        interval: 'Daily',
      })
      const store = WatermarkStore.empty().set(
        key,
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
      const sut = new FileStateManager(path)

      // Act
      await sut.write(store)

      // Assert
      const raw = JSON.parse(await readFile(path, 'utf-8'))
      expect(raw.watermarks['org:elf:Login:Daily']).toBe(
        '2026-03-01T00:00:00.000Z'
      )
    })

    it('given a WatermarkStore, when writing, then file ends with a newline', async () => {
      // Arrange
      const path = join(testDir, 'state.json')
      const store = WatermarkStore.empty()
      const sut = new FileStateManager(path)

      // Act
      await sut.write(store)

      // Assert — kills '\n' trailing newline mutation
      const raw = await readFile(path, 'utf-8')
      expect(raw.endsWith('\n')).toBe(true)
    })

    it('given rename fails, when writing, then cleans up temp file and rethrows', async () => {
      // Arrange — write to a non-existent directory so rename will fail
      const path = join(testDir, 'readonly', 'state.json')
      const key = WatermarkKey.fromEntry({
        sourceOrg: 'org',
        eventLog: 'Login',
        interval: 'Daily',
      })
      const store = WatermarkStore.empty().set(
        key,
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
      const sut = new FileStateManager(path)

      // Act & Assert
      await expect(sut.write(store)).rejects.toThrow(/ENOENT/)
    })

    it('given an existing file, when writing new state, then overwrites atomically', async () => {
      // Arrange
      const path = join(testDir, 'state.json')
      const sut = new FileStateManager(path)
      const key = WatermarkKey.fromEntry({
        sourceOrg: 'org',
        eventLog: 'Login',
        interval: 'Daily',
      })
      await sut.write(
        WatermarkStore.empty().set(
          key,
          Watermark.fromString('2026-01-01T00:00:00.000Z')
        )
      )

      // Act
      await sut.write(
        WatermarkStore.empty().set(
          key,
          Watermark.fromString('2026-03-05T00:00:00.000Z')
        )
      )

      // Assert
      const store = await sut.read()
      expect(store.get(key)!.toString()).toBe('2026-03-05T00:00:00.000Z')
    })
  })
})

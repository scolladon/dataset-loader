import { randomUUID } from 'node:crypto'
import { readFile, rename, unlink, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { z } from 'zod'
import { ISO_8601_PATTERN } from '../domain/watermark.js'
import { WatermarkStore } from '../domain/watermark-store.js'
import { type StatePort } from '../ports/types.js'

const iso8601 = z.string().regex(ISO_8601_PATTERN, 'Must be ISO 8601 datetime')

const stateFileSchema = z.object({
  watermarks: z.record(z.string(), iso8601),
})

export class FileStateManager implements StatePort {
  constructor(private readonly path: string) {}

  async read(): Promise<WatermarkStore> {
    try {
      const raw = await readFile(this.path, 'utf-8')
      const parsed = stateFileSchema.parse(JSON.parse(raw))
      return WatermarkStore.fromRecord(parsed.watermarks)
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT')
        return WatermarkStore.empty()
      throw err
    }
  }

  async write(store: WatermarkStore): Promise<void> {
    const state = { watermarks: store.toRecord() }
    // Atomic write: write to temp then rename, so a crash never leaves a truncated state file
    const tmpPath = join(dirname(this.path), `.tmp-${randomUUID()}.json`)
    try {
      await writeFile(tmpPath, JSON.stringify(state, null, 2) + '\n', {
        encoding: 'utf-8',
        mode: 0o600,
      })
      await rename(tmpPath, this.path)
    } catch (err) {
      await unlink(tmpPath).catch(() => {
        /* best-effort cleanup */
      })
      throw err
    }
  }
}

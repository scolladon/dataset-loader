import { readFile, writeFile, rename, unlink } from 'node:fs/promises'
import { randomUUID } from 'node:crypto'
import { dirname, join } from 'node:path'
import { z } from 'zod'
import { type StateFile } from '../types.js'

const iso8601 = z.string().regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/, 'Must be ISO 8601 datetime')

const stateFileSchema = z.object({
  watermarks: z.record(iso8601),
})

const EMPTY_STATE: StateFile = { watermarks: {} }

export async function readState(path: string): Promise<StateFile> {
  try {
    const raw = await readFile(path, 'utf-8')
    return stateFileSchema.parse(JSON.parse(raw))
  } catch (err: unknown) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return { ...EMPTY_STATE, watermarks: {} }
    throw err
  }
}

export async function writeState(path: string, state: StateFile): Promise<void> {
  const tmpPath = join(dirname(path), `.tmp-${randomUUID()}.json`)
  try {
    await writeFile(tmpPath, JSON.stringify(state, null, 2) + '\n', { encoding: 'utf-8', mode: 0o600 })
    await rename(tmpPath, path)
  } catch (err) {
    await unlink(tmpPath).catch(() => {})
    throw err
  }
}

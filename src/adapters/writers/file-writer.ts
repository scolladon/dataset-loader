import {
  createWriteStream,
  existsSync,
  mkdirSync,
  rmSync,
  statSync,
  type WriteStream,
} from 'node:fs'
import { dirname } from 'node:path'
import { Writable } from 'node:stream'
import { type DatasetKey } from '../../domain/dataset-key.js'
import {
  type CreateWriterPort,
  type HeaderProvider,
  type Operation,
  type ProgressListener,
  type Writer,
  type WriterResult,
} from '../../ports/types.js'

export class FileWriter implements Writer {
  private fileStream?: WriteStream
  private headerWritten = false

  constructor(
    private readonly filePath: string,
    private readonly operation: Operation,
    private readonly headerProvider: HeaderProvider,
    private readonly listener?: ProgressListener
  ) {}

  async init(): Promise<Writable> {
    mkdirSync(dirname(this.filePath), { recursive: true })

    const appendToNonEmpty =
      this.operation === 'Append' &&
      existsSync(this.filePath) &&
      statSync(this.filePath).size > 0

    this.fileStream = createWriteStream(this.filePath, {
      flags: appendToNonEmpty ? 'a' : 'w',
    })

    if (appendToNonEmpty) {
      this.headerWritten = true
    }

    return new Writable({
      objectMode: true,
      write: (
        batch: string[],
        _encoding: string,
        callback: (err?: Error | null) => void
      ) => {
        this.listener?.onRowsWritten(batch.length)
        this.doWrite(batch).then(() => callback(), callback)
      },
    })
  }

  private async doWrite(batch: string[]): Promise<void> {
    if (batch.length === 0) return
    if (!this.headerWritten) {
      this.headerWritten = true
      const header = await this.headerProvider.resolveHeader()
      await this.writeToFile(Buffer.from(header + '\n'))
    }
    await this.writeToFile(Buffer.from(batch.join('\n') + '\n'))
  }

  private writeToFile(chunk: Buffer): Promise<void> {
    return new Promise((resolve, reject) => {
      this.fileStream!.write(chunk, err => (err ? reject(err) : resolve()))
    })
  }

  async finalize(): Promise<WriterResult> {
    await this.closeStream()
    return { parentId: this.filePath, partCount: 0 }
  }

  async abort(): Promise<void> {
    await this.closeStream()
    if (existsSync(this.filePath)) {
      rmSync(this.filePath)
    }
  }

  async skip(): Promise<void> {
    await this.closeStream()
    if (this.operation === 'Overwrite' && existsSync(this.filePath)) {
      rmSync(this.filePath)
    }
  }

  private closeStream(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.fileStream) return resolve()
      this.fileStream.end((err?: NodeJS.ErrnoException | null) =>
        err ? reject(err) : resolve()
      )
    })
  }
}

export class FileWriterFactory implements CreateWriterPort {
  create(
    dataset: DatasetKey,
    operation: Operation,
    listener: ProgressListener,
    headerProvider: HeaderProvider
  ): Writer {
    return new FileWriter(dataset.name, operation, headerProvider, listener)
  }
}

import { PassThrough, Transform, type Writable } from 'node:stream'

function promiseWrite(stream: Writable, chunk: string[]): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (!stream.write(chunk)) {
      const onDrain = () => {
        stream.off('error', onError)
        resolve()
      }
      const onError = (err: Error) => {
        stream.off('drain', onDrain)
        reject(err)
      }
      stream.once('drain', onDrain)
      stream.once('error', onError)
    } else {
      resolve()
    }
  })
}

export function createFanOutTransform(
  channels: PassThrough[],
  onChannelError: (err: Error, channel: PassThrough) => void
): Transform {
  const active = new Set(channels)
  for (const ch of channels) {
    ch.once('error', (err: Error) => {
      onChannelError(err, ch)
      active.delete(ch)
    })
    ch.once('close', () => active.delete(ch))
  }
  return new Transform({
    objectMode: true,
    transform(chunk: string[], _enc, cb) {
      Promise.all(
        [...active].map(ch =>
          promiseWrite(ch, chunk).catch((err: Error) => {
            onChannelError(err, ch)
            active.delete(ch)
          })
        )
      ).then(() => cb(), cb)
    },
    flush(cb) {
      active.forEach(ch => ch.end())
      cb()
    },
  })
}

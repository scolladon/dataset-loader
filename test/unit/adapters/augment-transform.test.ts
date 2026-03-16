import { describe, expect, it } from 'vitest'
import {
  buildAugmentHeaderSuffix,
  buildAugmentSuffix,
  createAugmentTransform,
} from '../../../src/adapters/augment-transform.js'

describe('buildAugmentSuffix', () => {
  it('given empty columns, when building suffix, then returns empty string', () => {
    const sut = buildAugmentSuffix({})
    expect(sut).toBe('')
  })

  it('given single column, when building suffix, then returns comma-prefixed quoted value', () => {
    const sut = buildAugmentSuffix({ Org: 'prod' })
    expect(sut).toBe(',"prod"')
  })

  it('given value with quotes, when building suffix, then escapes quotes', () => {
    const sut = buildAugmentSuffix({ Name: 'O"Brien' })
    expect(sut).toBe(',"O""Brien"')
  })

  it('given multiple columns, when building suffix, then joins all values', () => {
    const sut = buildAugmentSuffix({ A: '1', B: '2' })
    expect(sut).toBe(',"1","2"')
  })
})

describe('buildAugmentHeaderSuffix', () => {
  it('given empty columns, when building header suffix, then returns empty string', () => {
    const sut = buildAugmentHeaderSuffix({})
    expect(sut).toBe('')
  })

  it('given columns with values, when building header suffix, then returns comma-separated column names without quoting', () => {
    const sut = buildAugmentHeaderSuffix({
      SourceOrg: '00D123',
      Interval: 'Hourly',
    })
    expect(sut).toBe(',SourceOrg,Interval')
  })
})

describe('createAugmentTransform', () => {
  it('given suffix, when line passes through, then suffix is appended', async () => {
    const sut = createAugmentTransform(',"extra"')
    const chunks: string[] = []
    sut.on('data', (chunk: string) => chunks.push(chunk))

    sut.write('"Id","Name"')
    sut.end()
    await new Promise(resolve => sut.on('end', resolve))

    expect(chunks).toEqual(['"Id","Name","extra"'])
  })

  it('given empty suffix, when line passes through, then line is unchanged', async () => {
    const sut = createAugmentTransform('')
    const chunks: string[] = []
    sut.on('data', (chunk: string) => chunks.push(chunk))

    sut.write('"value"')
    sut.end()
    await new Promise(resolve => sut.on('end', resolve))

    expect(chunks).toEqual(['"value"'])
  })
})

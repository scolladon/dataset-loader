import { describe, expect, it } from 'vitest'
import { mutateRangesFromDiff } from '../../mutation/changedRanges.mjs'

const fileHeader = (path: string): string =>
  [`diff --git a/${path} b/${path}`, `--- a/${path}`, `+++ b/${path}`].join(
    '\n'
  )

describe('mutateRangesFromDiff', () => {
  it('given a hunk adding several lines to a source file, when reading the diff, then the range covers those lines', () => {
    // Arrange
    const diff = [fileHeader('src/a.ts'), '@@ -9,2 +10,3 @@ context'].join('\n')

    // Act
    const sut = mutateRangesFromDiff(diff)

    // Assert
    expect(sut).toEqual(['src/a.ts:10-12'])
  })

  it('given a hunk without a line count, when reading the diff, then the range is that single line', () => {
    // Arrange
    const diff = [fileHeader('src/a.ts'), '@@ -5 +5 @@'].join('\n')

    // Act
    const sut = mutateRangesFromDiff(diff)

    // Assert
    expect(sut).toEqual(['src/a.ts:5-5'])
  })

  it('given a hunk that only deletes lines, when reading the diff, then it yields no range', () => {
    // Arrange
    const diff = [fileHeader('src/a.ts'), '@@ -7,3 +6,0 @@'].join('\n')

    // Act
    const sut = mutateRangesFromDiff(diff)

    // Assert
    expect(sut).toEqual([])
  })

  it('given hunks in files that are not TypeScript sources, when reading the diff, then they are left out', () => {
    // Arrange
    const diff = [
      fileHeader('test/unit/a.test.ts'),
      '@@ -1 +1,2 @@',
      fileHeader('src/schema.json'),
      '@@ -1 +1,2 @@',
      fileHeader('README.md'),
      '@@ -1 +1,2 @@',
    ].join('\n')

    // Act
    const sut = mutateRangesFromDiff(diff)

    // Assert
    expect(sut).toEqual([])
  })

  it('given a deleted file followed by a changed source file, when reading the diff, then only the source file yields a range', () => {
    // Arrange
    const diff = [
      'diff --git a/src/gone.ts b/src/gone.ts',
      '--- a/src/gone.ts',
      '+++ /dev/null',
      '@@ -1,4 +0,0 @@',
      fileHeader('src/b.ts'),
      '@@ -3 +3,2 @@',
    ].join('\n')

    // Act
    const sut = mutateRangesFromDiff(diff)

    // Assert
    expect(sut).toEqual(['src/b.ts:3-4'])
  })

  it('given several hunks across source files, when reading the diff, then ranges keep the diff order', () => {
    // Arrange
    const diff = [
      fileHeader('src/a.ts'),
      '@@ -1 +1,2 @@',
      '@@ -40,0 +42,5 @@',
      fileHeader('src/dir/b.ts'),
      '@@ -8,2 +8 @@',
    ].join('\n')

    // Act
    const sut = mutateRangesFromDiff(diff)

    // Assert
    expect(sut).toEqual(['src/a.ts:1-2', 'src/a.ts:42-46', 'src/dir/b.ts:8-8'])
  })
})

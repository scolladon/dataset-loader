export async function collectLines(
  iterable: AsyncIterable<string[]>
): Promise<string[]> {
  const lines: string[] = []
  for await (const batch of iterable) lines.push(...batch)
  return lines
}

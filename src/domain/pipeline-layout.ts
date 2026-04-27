import { type ProjectionLayout } from '../ports/types.js'

// Structural equality on projection layouts. Both undefined → equal;
// mixed → not equal. augmentSlots compared order-independently by pos.
export function layoutsEqual(
  a: ProjectionLayout | undefined,
  b: ProjectionLayout | undefined
): boolean {
  if (a === b) return true
  if (!a || !b) return false
  if (a.targetSize !== b.targetSize) return false
  if (a.outputIndex.length !== b.outputIndex.length) return false
  for (let i = 0; i < a.outputIndex.length; i++) {
    if (a.outputIndex[i] !== b.outputIndex[i]) return false
  }
  if (a.augmentSlots.length !== b.augmentSlots.length) return false
  const bByPos = new Map(b.augmentSlots.map(s => [s.pos, s.quoted]))
  for (const s of a.augmentSlots) {
    if (bByPos.get(s.pos) !== s.quoted) return false
  }
  return true
}

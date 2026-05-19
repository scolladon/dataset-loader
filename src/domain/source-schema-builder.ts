import { type SourceField } from './metadata-types.js'

// Each provider (SObject / ELF / CSV) emits its reader fields first then
// appends augment columns as Text. Centralising the augment-append here
// keeps the providers thin and eliminates the cloned loop that jscpd
// flagged across the three implementations.

export function readerTextField(name: string): SourceField {
  return { name, label: name, type: 'Text', origin: 'reader' }
}

export function appendAugmentFields(
  fields: SourceField[],
  augmentColumns: Readonly<Record<string, string>>
): void {
  for (const augKey of Object.keys(augmentColumns)) {
    fields.push({
      name: augKey,
      label: augKey,
      type: 'Text',
      origin: 'augment',
    })
  }
}

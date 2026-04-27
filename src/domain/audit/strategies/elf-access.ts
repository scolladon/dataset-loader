import { type AuditCheckStrategy, pass } from '../audit-strategy.js'

export const elfAccess: AuditCheckStrategy = {
  select: e =>
    e.readerKind === 'elf' ? [{ org: e.sourceOrg, key: 'elf' }] : [],
  label: org => `${org}: EventLogFile access (ViewEventLogFiles)`,
  evaluate: async sfPort => {
    await sfPort.query('SELECT Id FROM EventLogFile LIMIT 1')
    return pass()
  },
}

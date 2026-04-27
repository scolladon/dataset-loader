import { type AuditCheckStrategy, pass } from '../audit-strategy.js'

export const insightsAccess: AuditCheckStrategy = {
  select: e => (e.targetOrg ? [{ org: e.targetOrg, key: 'insights' }] : []),
  label: org => `${org}: InsightsExternalData access`,
  evaluate: async sfPort => {
    await sfPort.query('SELECT Id FROM InsightsExternalData LIMIT 1')
    return pass()
  },
}

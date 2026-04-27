import { type AuditCheckStrategy, pass } from '../audit-strategy.js'

export const authConnectivity: AuditCheckStrategy = {
  select: e =>
    e.targetOrg
      ? [
          { org: e.sourceOrg, key: 'auth' },
          { org: e.targetOrg, key: 'auth' },
        ]
      : [{ org: e.sourceOrg, key: 'auth' }],
  label: org => `${org}: auth and connectivity`,
  evaluate: async sfPort => {
    await sfPort.query('SELECT Id FROM Organization LIMIT 1')
    return pass()
  },
}

import { vi } from 'vitest'
import { type SalesforcePort } from '../../src/ports/types.js'

export function makeSfPort(
  overrides: Partial<SalesforcePort> = {}
): SalesforcePort {
  return {
    apiVersion: '62.0',
    query: vi.fn().mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    queryMore: vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    getBlob: vi.fn().mockResolvedValue(''),
    getBlobStream: vi.fn(),
    post: vi.fn().mockResolvedValue({ id: '06W000000000001' }),
    patch: vi.fn().mockResolvedValue(null),
    del: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  }
}

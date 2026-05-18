import { describe, expect, it, vi } from 'vitest'
import { buildAuditChecks } from '../../../../../src/domain/audit/runner.js'
import { type SalesforcePort } from '../../../../../src/ports/types.js'
import { auditEntryOf, createMockSfPort } from '../../../../fixtures/audit.js'

describe('SObject read access check', () => {
  it('given SObject entries, when building checks, then includes check per unique (org, sObject)', () => {
    // Arrange
    const entries = [
      auditEntryOf({ isElf: false, sourceOrg: 'srcA', sObject: 'Account' }),
      auditEntryOf({ isElf: false, sourceOrg: 'srcA', sObject: 'Contact' }),
      auditEntryOf({ isElf: false, sourceOrg: 'srcA', sObject: 'Account' }),
    ]
    const sfPorts = new Map([['srcA', createMockSfPort()]])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const readChecks = sut.filter(c => c.label.includes('read access'))
    expect(readChecks.length).toBe(2)
    expect(readChecks.map(c => c.label)).toEqual(
      expect.arrayContaining([
        'srcA: Account read access',
        'srcA: Contact read access',
      ])
    )
  })

  it('given no SObject entries, when building checks, then skips sObject read access check', () => {
    // Arrange
    const entries = [
      auditEntryOf({ isElf: true, sourceOrg: 'srcA', targetOrg: 'anaA' }),
    ]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const readChecks = sut.filter(c => c.label.includes('read access'))
    expect(readChecks.length).toBe(0)
  })

  it('given sObject check, when executing, then queries with FLS-enforced SOQL listing reader fields', async () => {
    // Arrange
    const sfMock = createMockSfPort()
    const entries = [
      auditEntryOf({
        isElf: false,
        sourceOrg: 'src',
        sObject: 'Account',
        readerFields: ['Id', 'Name', 'Owner.Profile.Name'],
      }),
    ]
    const sfPorts = new Map([['src', sfMock]])
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    await readCheck.execute()

    // Assert — FLS enforced + dotted relationship paths preserved
    expect(sfMock.query).toHaveBeenCalledWith(
      'SELECT Id, Name, Owner.Profile.Name FROM Account WITH SECURITY_ENFORCED LIMIT 1'
    )
  })

  it('given two SObject entries on same (org, sObject) with disjoint fields, when executing the merged check, then SOQL queries the union', async () => {
    // Arrange
    const sfMock = createMockSfPort()
    const entries = [
      auditEntryOf({
        isElf: false,
        sourceOrg: 'src',
        sObject: 'Account',
        readerFields: ['Id', 'Name'],
      }),
      auditEntryOf({
        isElf: false,
        sourceOrg: 'src',
        sObject: 'Account',
        readerFields: ['Industry', 'Name'], // overlapping `Name` is deduped
      }),
    ]
    const sfPorts = new Map([['src', sfMock]])
    const checks = buildAuditChecks(entries, sfPorts)
    const readChecks = checks.filter(c => c.label.includes('read access'))

    // Act
    await readChecks[0].execute()

    // Assert — single check covers the union (Id, Name, Industry), no duplicates
    expect(readChecks.length).toBe(1)
    expect(sfMock.query).toHaveBeenCalledWith(
      'SELECT Id, Name, Industry FROM Account WITH SECURITY_ENFORCED LIMIT 1'
    )
  })

  it('given an FLS-blocked field, when executing the check, then FAIL surfaces the SF error message naming the field', async () => {
    // Arrange — simulate INVALID_FIELD with FLS-style message
    const sfPort: SalesforcePort = {
      apiVersion: '62.0',
      query: vi.fn(async () => {
        throw new Error(
          "INVALID_FIELD: No such column 'SecretField' on entity 'Account' or you do not have access to it"
        )
      }),
      queryMore: vi.fn(),
      getBlob: vi.fn(),
      getBlobStream: vi.fn(),
      post: vi.fn(),
      patch: vi.fn(),
      del: vi.fn(),
    }
    const entries = [
      auditEntryOf({
        isElf: false,
        sourceOrg: 'src',
        sObject: 'Account',
        readerFields: ['Id', 'SecretField'],
      }),
    ]
    const sfPorts = new Map([['src', sfPort]])
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    const sut = await readCheck.execute()

    // Assert — message preserved end-to-end and names the offending field
    expect(sut.kind).toBe('fail')
    if (sut.kind === 'fail') {
      expect(sut.message).toMatch(/SecretField/)
      expect(sut.message).toMatch(/INVALID_FIELD/)
    }
  })

  it('given sObject query succeeds, when executing check, then returns true', async () => {
    // Arrange
    const entries = [
      auditEntryOf({ isElf: false, sourceOrg: 'src', sObject: 'Account' }),
    ]
    const sfPorts = new Map([['src', createMockSfPort('ok')]])
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    const sut = await readCheck.execute()

    // Assert
    expect(sut.kind).toBe('pass')
  })

  it('given sObject query fails, when executing check, then returns false', async () => {
    // Arrange
    const entries = [
      auditEntryOf({ isElf: false, sourceOrg: 'src', sObject: 'Account' }),
    ]
    const sfPorts = new Map([['src', createMockSfPort('fail')]])
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    const sut = await readCheck.execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given sObject check with missing sfPort, when executing, then returns false', async () => {
    // Arrange
    const entries = [
      auditEntryOf({ isElf: false, sourceOrg: 'src', sObject: 'Account' }),
    ]
    const sfPorts = new Map<string, SalesforcePort>()
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    const sut = await readCheck.execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given an entity that rejects WITH SECURITY_ENFORCED, when executing the check, then WARN after the FLS-less re-probe succeeds', async () => {
    // Arrange — first call rejects with the SF "not allowed in this context"
    // error; second (re-probe without FLS) succeeds.
    const query = vi
      .fn()
      .mockRejectedValueOnce(
        new Error(
          'SECURITY_ENFORCED not allowed in this context: {"errorCode":"MALFORMED_QUERY"}'
        )
      )
      .mockResolvedValueOnce({ records: [], totalSize: 0, done: true })
    const sfPort: SalesforcePort = {
      apiVersion: '62.0',
      query,
      queryMore: vi.fn(),
      getBlob: vi.fn(),
      getBlobStream: vi.fn(),
      post: vi.fn(),
      patch: vi.fn(),
      del: vi.fn(),
    }
    const entries = [
      auditEntryOf({
        isElf: false,
        sourceOrg: 'src',
        sObject: 'User',
        readerFields: ['Id', 'Username'],
      }),
    ]
    const sfPorts = new Map([['src', sfPort]])
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    const sut = await readCheck.execute()

    // Assert — both probes executed (with and without FLS), result is WARN
    expect(query).toHaveBeenNthCalledWith(
      1,
      'SELECT Id, Username FROM User WITH SECURITY_ENFORCED LIMIT 1'
    )
    expect(query).toHaveBeenNthCalledWith(
      2,
      'SELECT Id, Username FROM User LIMIT 1'
    )
    expect(sut.kind).toBe('warn')
    if (sut.kind === 'warn') {
      expect(sut.message).toMatch(/User/)
      expect(sut.message).toMatch(/FLS not enforced/)
    }
  })

  it('given a non-SECURITY_ENFORCED query error, when executing the check, then FAIL without a re-probe', async () => {
    // Arrange — any other error should propagate to FAIL on the first attempt
    const query = vi
      .fn()
      .mockRejectedValue(new Error('boom: unrelated SF error'))
    const sfPort: SalesforcePort = {
      apiVersion: '62.0',
      query,
      queryMore: vi.fn(),
      getBlob: vi.fn(),
      getBlobStream: vi.fn(),
      post: vi.fn(),
      patch: vi.fn(),
      del: vi.fn(),
    }
    const entries = [
      auditEntryOf({ isElf: false, sourceOrg: 'src', sObject: 'Account' }),
    ]
    const sfPorts = new Map([['src', sfPort]])
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    const sut = await readCheck.execute()

    // Assert
    expect(query).toHaveBeenCalledTimes(1)
    expect(sut.kind).toBe('fail')
    if (sut.kind === 'fail') {
      expect(sut.message).toMatch(/boom/)
    }
  })
})

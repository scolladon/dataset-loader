import { describe, expect, it, vi } from 'vitest'
import { buildAuditChecks, runAudit } from '../../../src/domain/auditor.js'
import {
  type LoggerPort,
  type SalesforcePort,
} from '../../../src/ports/types.js'

function createMockSfPort(queryResult: 'ok' | 'fail' = 'ok'): SalesforcePort {
  return {
    query:
      queryResult === 'ok'
        ? vi.fn(async () => ({
            totalSize: 1,
            done: true,
            records: [{ Id: '001' }],
          }))
        : vi.fn(async () => {
            throw new Error('access denied')
          }),
    queryMore: vi.fn(),
    getBlob: vi.fn(),
    getBlobStream: vi.fn(),
    post: vi.fn(),
    patch: vi.fn(),
    del: vi.fn(),
    apiVersion: '62.0',
  }
}

function createMockLogger(): LoggerPort {
  return { info: vi.fn(), warn: vi.fn(), debug: vi.fn() }
}

describe('buildAuditChecks', () => {
  it('given entries, when building checks, then includes auth check for all unique orgs', () => {
    // Arrange
    const entries = [
      { type: 'elf' as const, sourceOrg: 'srcA', targetOrg: 'anaA' },
      {
        type: 'sobject' as const,
        sourceOrg: 'srcB',
        targetOrg: 'anaA',
      },
    ]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['srcB', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const authChecks = sut.filter(c => c.label.includes('auth'))
    expect(authChecks.length).toBe(3)
  })

  it('given ELF entries, when building checks, then includes EventLogFile check for ELF source orgs', () => {
    // Arrange
    const entries = [
      { type: 'elf' as const, sourceOrg: 'srcA', targetOrg: 'anaA' },
    ]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const elfChecks = sut.filter(c => c.label.includes('EventLogFile'))
    expect(elfChecks.length).toBe(1)
    expect(elfChecks[0].org).toBe('srcA')
  })

  it('given no ELF entries, when building checks, then skips EventLogFile check', () => {
    // Arrange
    const entries = [
      {
        type: 'sobject' as const,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
      },
    ]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const elfChecks = sut.filter(c => c.label.includes('EventLogFile'))
    expect(elfChecks.length).toBe(0)
  })

  it('given sfPorts map missing an org entry, when executing check, then check returns false', async () => {
    const entries = [
      { type: 'elf' as const, sourceOrg: 'srcA', targetOrg: 'anaA' },
    ]
    const sfPorts = new Map<string, SalesforcePort>()

    const checks = buildAuditChecks(entries, sfPorts)
    const authCheck = checks.find(
      c => c.org === 'srcA' && c.label.includes('auth')
    )!

    const sut = await authCheck.execute()

    expect(sut).toBe(false)
  })

  it('given InsightsExternalData query fails, when executing check, then returns false', async () => {
    // Arrange
    const entries = [
      { type: 'sobject' as const, sourceOrg: 'srcA', targetOrg: 'anaA' },
    ]
    const sfPorts = new Map([
      ['srcA', createMockSfPort('ok')],
      ['anaA', createMockSfPort('fail')],
    ])

    const checks = buildAuditChecks(entries, sfPorts)
    const insightsCheck = checks.find(c =>
      c.label.includes('InsightsExternalData')
    )!

    // Act
    const sut = await insightsCheck.execute()

    // Assert
    expect(sut).toBe(false)
  })

  it('given EventLogFile check with missing sfPort, when executing, then returns false', async () => {
    // Arrange
    const entries = [
      { type: 'elf' as const, sourceOrg: 'srcA', targetOrg: 'anaA' },
    ]
    const sfPorts = new Map<string, SalesforcePort>()

    const checks = buildAuditChecks(entries, sfPorts)
    const elfCheck = checks.find(c => c.label.includes('EventLogFile'))!

    // Act
    const sut = await elfCheck.execute()

    // Assert
    expect(sut).toBe(false)
  })

  it('given InsightsExternalData check with missing sfPort, when executing, then returns false', async () => {
    // Arrange
    const entries = [
      { type: 'sobject' as const, sourceOrg: 'srcA', targetOrg: 'anaA' },
    ]
    const sfPorts = new Map<string, SalesforcePort>()

    const checks = buildAuditChecks(entries, sfPorts)
    const insightsCheck = checks.find(c =>
      c.label.includes('InsightsExternalData')
    )!

    // Act
    const sut = await insightsCheck.execute()

    // Assert
    expect(sut).toBe(false)
  })

  it('given analytic orgs, when building checks, then includes InsightsExternalData check', () => {
    const entries = [
      {
        type: 'sobject' as const,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
      },
    ]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    const sut = buildAuditChecks(entries, sfPorts)
    const insightsChecks = sut.filter(c =>
      c.label.includes('InsightsExternalData')
    )
    expect(insightsChecks.length).toBe(1)
    expect(insightsChecks[0].org).toBe('anaA')
  })
})

describe('runAudit', () => {
  it('given all checks pass, when running audit, then returns passed true', async () => {
    const entries = [
      { type: 'elf' as const, sourceOrg: 'src', targetOrg: 'ana' },
    ]
    const sfPorts = new Map([
      ['src', createMockSfPort('ok')],
      ['ana', createMockSfPort('ok')],
    ])
    const checks = buildAuditChecks(entries, sfPorts)
    const logger = createMockLogger()

    const sut = await runAudit(checks, logger)

    expect(sut.passed).toBe(true)
    expect(logger.info).toHaveBeenCalled()
  })

  it('given check that throws, when running audit, then logs FAIL with check label', async () => {
    // Arrange
    const checks = [
      {
        org: 'src',
        label: 'src: exploding check',
        execute: async () => {
          throw new Error('unexpected failure')
        },
      },
    ]
    const logger = createMockLogger()

    // Act
    await runAudit(checks, logger)

    // Assert
    expect(logger.info).toHaveBeenCalledWith(expect.stringContaining('[FAIL]'))
    expect(logger.info).toHaveBeenCalledWith(
      expect.stringContaining('src: exploding check')
    )
  })

  it('given check that throws, when running audit, then audit marks it as failed', async () => {
    const checks = [
      {
        org: 'src',
        label: 'src: exploding check',
        execute: async () => {
          throw new Error('unexpected failure')
        },
      },
    ]
    const logger = createMockLogger()

    const sut = await runAudit(checks, logger)

    expect(sut.passed).toBe(false)
  })

  it('given auth check fails, when running audit, then returns passed false', async () => {
    const entries = [
      { type: 'elf' as const, sourceOrg: 'src', targetOrg: 'ana' },
    ]
    const sfPorts = new Map([
      ['src', createMockSfPort('fail')],
      ['ana', createMockSfPort('ok')],
    ])
    const checks = buildAuditChecks(entries, sfPorts)
    const logger = createMockLogger()

    const sut = await runAudit(checks, logger)

    expect(sut.passed).toBe(false)
  })
})

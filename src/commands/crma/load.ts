import { SfCommand, Flags } from '@salesforce/sf-plugins-core'
import { Org } from '@salesforce/core'
import { SfClient } from '../../core/sf-client.js'
import { parseConfig, resolveConfig } from '../../core/config-loader.js'
import { readState, writeState } from '../../core/state-manager.js'
import { augment } from '../../core/augmenter.js'
import { group } from '../../core/grouper.js'
import { fetchElf } from '../../adapters/elf-fetcher.js'
import { fetchSObject } from '../../adapters/sobject-fetcher.js'
import { upload } from '../../adapters/uploader.js'
import { type ConfigEntry, type FetchResult, type GroupInput, type ResolvedEntry, watermarkKey, groupKey } from '../../types.js'

export interface CrmaLoadResult {
  entriesProcessed: number
  entriesSkipped: number
  entriesFailed: number
  groupsUploaded: number
}

export default class CrmaLoad extends SfCommand<CrmaLoadResult> {
  public static readonly summary = 'Load Event Log Files and SObject data into CRMA datasets'
  public static readonly examples = [
    '<%= config.bin %> <%= command.id %>',
    '<%= config.bin %> <%= command.id %> --config-file my-config.json --dry-run',
  ]

  public static readonly flags = {
    'config-file': Flags.file({
      char: 'c',
      summary: 'Path to config JSON',
      default: 'crma-load.config.json',
    }),
    'state-file': Flags.file({
      char: 's',
      summary: 'Path to watermark state file',
      default: '.crma-load.state.json',
    }),
    audit: Flags.boolean({
      summary: 'Pre-flight checks only (auth, connectivity, permissions)',
      default: false,
    }),
    'dry-run': Flags.boolean({
      summary: 'Show plan without executing',
      default: false,
    }),
    entry: Flags.integer({
      summary: 'Process only entry at this 0-based index',
    }),
  }

  public async run(): Promise<CrmaLoadResult> {
    const { flags } = await this.parse(CrmaLoad)
    const configPath = flags['config-file']
    const statePath = flags['state-file']
    const audit = flags['audit']
    const dryRun = flags['dry-run']
    const entryIndex = flags['entry']

    const clientsByOrg = new Map<string, SfClient>()

    const getClient = async (orgAlias: string): Promise<SfClient> => {
      if (!clientsByOrg.has(orgAlias)) {
        const org = await Org.create({ aliasOrUsername: orgAlias })
        const connection = org.getConnection()
        clientsByOrg.set(orgAlias, new SfClient(connection))
      }
      return clientsByOrg.get(orgAlias)!
    }

    let resolvedEntries: ResolvedEntry[]
    try {
      const config = await parseConfig(configPath)
      const allOrgs = new Set<string>()
      for (const entry of config.entries) {
        allOrgs.add(entry.sourceOrg)
        allOrgs.add(entry.analyticOrg)
      }
      await Promise.all([...allOrgs].map((alias) => getClient(alias)))
      resolvedEntries = await resolveConfig(config, clientsByOrg)
    } catch (error) {
      this.error(`Config loading failed: ${error instanceof Error ? error.message : error}`)
    }

    if (entryIndex !== undefined) {
      resolvedEntries = resolvedEntries.filter((e) => e.index === entryIndex)
      if (resolvedEntries.length === 0) {
        this.error(`Entry index ${entryIndex} not found`)
      }
    }

    if (audit) {
      return this.runAudit(resolvedEntries, clientsByOrg)
    }

    const state = await readState(statePath)

    if (dryRun) {
      this.log('Dry run — planned entries:')
      for (const { entry, index } of resolvedEntries) {
        const wk = watermarkKey(entry)
        const wm = state.watermarks[wk] ?? '(none)'
        this.log(`  [${index}] ${entry.type} → ${entry.analyticOrg}:${entry.dataset} (watermark: ${wm})`)
      }
      return { entriesProcessed: 0, entriesSkipped: 0, entriesFailed: 0, groupsUploaded: 0 }
    }

    // Fetch phase: all entries in parallel
    const fetchResults = await Promise.allSettled(
      resolvedEntries.map(async ({ entry, index, augmentColumns }) => {
        const client = await getClient(entry.sourceOrg)
        const wk = watermarkKey(entry)
        const wm = state.watermarks[wk]

        let result: FetchResult | null
        if (entry.type === 'elf') {
          result = await fetchElf(client, entry.eventType, entry.interval, wm)
        } else {
          result = await fetchSObject(client, entry.sobject, entry.fields, entry.dateField ?? 'LastModifiedDate', wm, entry.where)
        }

        if (!result) {
          this.log(`  [${index}] No new records, skipping`)
          return null
        }

        const augmentedCsv = augment(result.csv, augmentColumns)
        return { entry, index, csv: augmentedCsv, newWatermark: result.newWatermark }
      })
    )

    // Collect successful fetches
    const successfulFetches: { entry: ConfigEntry; index: number; csv: string; newWatermark: string }[] = []
    let entriesFailed = 0
    let entriesSkipped = 0

    for (const result of fetchResults) {
      if (result.status === 'rejected') {
        entriesFailed++
        this.warn(`Fetch failed: ${result.reason instanceof Error ? result.reason.message : 'unknown error'}`)
      } else if (result.value === null) {
        entriesSkipped++
      } else {
        successfulFetches.push(result.value)
      }
    }

    if (successfulFetches.length === 0) {
      this.log('No data to upload')
      return { entriesProcessed: 0, entriesSkipped, entriesFailed, groupsUploaded: 0 }
    }

    // Group phase
    const groupInputs: GroupInput[] = successfulFetches.map((f) => ({
      key: groupKey(f.entry),
      csv: f.csv,
      operation: f.entry.operation ?? 'Append',
    }))

    const groups = group(groupInputs)

    // Upload phase
    let groupsUploaded = 0
    const uploadResults = await Promise.allSettled(
      [...groups.entries()].map(async ([key, { csv, operation }]) => {
        const [analyticOrg] = key.split(':')
        const dataset = key.slice(analyticOrg.length + 1)
        const client = await getClient(analyticOrg)
        await upload(client, dataset, csv, operation)
        return key
      })
    )

    // Update watermarks for successfully uploaded groups
    const uploadedKeys = new Set<string>()
    for (const result of uploadResults) {
      if (result.status === 'fulfilled') {
        uploadedKeys.add(result.value)
        groupsUploaded++
      } else {
        entriesFailed++
        this.warn(`Upload failed: ${result.reason instanceof Error ? result.reason.message : 'unknown error'}`)
      }
    }

    // Only update watermarks for entries whose groups uploaded successfully
    for (const fetch of successfulFetches) {
      const gk = groupKey(fetch.entry)
      if (uploadedKeys.has(gk)) {
        state.watermarks[watermarkKey(fetch.entry)] = fetch.newWatermark
      }
    }

    await writeState(statePath, state)

    const entriesProcessed = successfulFetches.filter((f) => uploadedKeys.has(groupKey(f.entry))).length
    this.log(`Done: ${entriesProcessed} processed, ${entriesSkipped} skipped, ${entriesFailed} failed, ${groupsUploaded} groups uploaded`)

    if (entriesFailed > 0 && entriesProcessed > 0) process.exitCode = 1
    if (entriesFailed > 0 && entriesProcessed === 0) process.exitCode = 2

    return { entriesProcessed, entriesSkipped, entriesFailed, groupsUploaded }
  }

  private async runAudit(
    resolvedEntries: ResolvedEntry[],
    clientsByOrg: Map<string, SfClient>
  ): Promise<CrmaLoadResult> {
    this.log('Audit — pre-flight checks:')

    const uniqueOrgs = new Set<string>()
    for (const { entry } of resolvedEntries) {
      uniqueOrgs.add(entry.sourceOrg)
      uniqueOrgs.add(entry.analyticOrg)
    }

    const authResults = await Promise.allSettled(
      [...uniqueOrgs].map(async (orgAlias) => {
        const client = clientsByOrg.get(orgAlias)
        if (!client) return { orgAlias, passed: false, message: 'no authenticated connection' }

        try {
          await client.query('SELECT Id FROM Organization LIMIT 1')
          return { orgAlias, passed: true, message: 'auth and connectivity OK' }
        } catch (err) {
          return { orgAlias, passed: false, message: err instanceof Error ? err.message : String(err) }
        }
      })
    )

    let allPassed = true
    for (const result of authResults) {
      if (result.status === 'rejected') {
        allPassed = false
        continue
      }
      const { orgAlias, passed, message } = result.value
      this.log(`  [${passed ? 'PASS' : 'FAIL'}] ${orgAlias}: ${message}`)
      if (!passed) allPassed = false
    }

    const analyticOrgs = new Set(resolvedEntries.map(({ entry }) => entry.analyticOrg))
    const insightsResults = await Promise.allSettled(
      [...analyticOrgs].map(async (orgAlias) => {
        const client = clientsByOrg.get(orgAlias)
        if (!client) return null

        try {
          await client.query("SELECT Id FROM InsightsExternalData LIMIT 1")
          return { orgAlias, passed: true, message: 'InsightsExternalData access OK' }
        } catch (err) {
          return { orgAlias, passed: false, message: `InsightsExternalData access — ${err instanceof Error ? err.message : err}` }
        }
      })
    )

    for (const result of insightsResults) {
      if (result.status === 'rejected') {
        allPassed = false
        continue
      }
      if (!result.value) continue
      const { orgAlias, passed, message } = result.value
      this.log(`  [${passed ? 'PASS' : 'FAIL'}] ${orgAlias}: ${message}`)
      if (!passed) allPassed = false
    }

    this.log(allPassed ? 'All checks passed' : 'Some checks failed')
    if (!allPassed) process.exitCode = 2

    return { entriesProcessed: 0, entriesSkipped: 0, entriesFailed: allPassed ? 0 : 1, groupsUploaded: 0 }
  }
}

<!-- markdownlint-disable MD013 MD032 -- manual-test scenario with long command lines and inline lists -->
# Manual Test Scenario

## Prerequisites

- Two authenticated SF CLI orgs (adjust aliases to match yours):
  - `my-source` — source org with Event Log Files enabled
  - `my-analytic` — analytic org with CRM Analytics

```bash
sf org display --target-org my-source
sf org display --target-org my-analytic
```

- Plugin built and linked:

```bash
npm run build && sf plugins link .
```

## Scenario 1: ELF — Full Lifecycle

### 1.1 Create config

```bash
cat > dataset-load.config.json << 'EOF'
{
  "entries": [
    {
      "eventLog": "Login",
      "interval": "Daily",
      "sourceOrg": "my-source",
      "targetOrg": "my-analytic",
      "targetDataset": "Test_Login"
    }
  ]
}
EOF
```

### 1.2 Audit

```bash
sf dataset load --audit
```

**Expected**: Both orgs show `[PASS]` for auth and InsightsExternalData access.

### 1.3 Dry run

```bash
sf dataset load --dry-run
```

**Expected**: Shows `[0] elf → my-analytic:Test_Login (watermark: (none))`.

### 1.4 First run (no watermark)

```bash
sf dataset load
```

**Expected**:
- Fetches latest Login ELF records
- Uploads to `Test_Login` dataset
- Creates `.dataset-load.state.json` with a watermark like `my-source:elf:Login:Daily`
- Output: `Done: 1 processed, 0 skipped, 0 failed, 1 groups uploaded`

**Verify in CRM Analytics**: Open Analytics Studio > Data Manager > Datasets. `Test_Login` should appear with data rows.

### 1.5 Second run (incremental)

```bash
sf dataset load --dry-run
```

**Expected**: Shows the watermark date from the previous run instead of `(none)`.

```bash
sf dataset load
```

**Expected**: Either `No new records, skipping` (if no new Login events) or appends new records.

### 1.6 Verify state file

```bash
cat .dataset-load.state.json
```

**Expected**: Valid JSON with ISO 8601 watermark:

```json
{
  "watermarks": {
    "my-source:elf:Login:Daily": "2026-03-05T00:00:00.000Z"
  }
}
```

## Scenario 2: SObject — Overwrite

### 2.1 Add SObject entry

```bash
cat > dataset-load.config.json << 'EOF'
{
  "entries": [
    {
      "sObject": "Account",
      "fields": ["Id", "Name", "Industry"],
      "dateField": "LastModifiedDate",
      "sourceOrg": "my-source",
      "targetOrg": "my-analytic",
      "targetDataset": "Test_Accounts",
      "operation": "Overwrite"
    }
  ]
}
EOF
```

### 2.2 Run

```bash
sf dataset load
```

**Expected**: Fetches accounts, uploads to `Test_Accounts` with Overwrite operation.

**Verify in CRM Analytics**: `Test_Accounts` dataset exists with Id, Name, Industry columns.

### 2.3 Run again

```bash
sf dataset load
```

**Expected**: Fetches only accounts modified after the watermark. If none, logs `No new records, skipping`.

## Scenario 3: Augment Columns

### 3.1 Config with augmentation

```bash
cat > dataset-load.config.json << 'EOF'
{
  "entries": [
    {
      "sObject": "Account",
      "fields": ["Id", "Name"],
      "sourceOrg": "my-source",
      "targetOrg": "my-analytic",
      "targetDataset": "Test_Augmented",
      "operation": "Overwrite",
      "augmentColumns": {
        "SourceOrgId": "{{sourceOrg.Id}}",
        "Env": "Production"
      }
    }
  ]
}
EOF
```

### 3.2 Run

```bash
sf dataset load
```

**Verify in CRM Analytics**: `Test_Augmented` dataset has columns Id, Name, SourceOrgId (18-char org id), Env ("Production").

## Scenario 4: Grouping (Multiple Entries → Single Dataset)

### 4.1 Config with two sources into one dataset

```bash
cat > dataset-load.config.json << 'EOF'
{
  "entries": [
    {
      "sObject": "Account",
      "fields": ["Id", "Name"],
      "sourceOrg": "my-source",
      "targetOrg": "my-analytic",
      "targetDataset": "Test_Grouped",
      "operation": "Overwrite",
      "augmentColumns": { "Source": "org-a" }
    },
    {
      "sObject": "Account",
      "fields": ["Id", "Name"],
      "sourceOrg": "my-analytic",
      "targetOrg": "my-analytic",
      "targetDataset": "Test_Grouped",
      "operation": "Overwrite",
      "augmentColumns": { "Source": "org-b" }
    }
  ]
}
EOF
```

### 4.2 Run

```bash
sf dataset load
```

**Expected**: Both entries fetched in parallel, merged into one upload. Output shows `1 groups uploaded`.

**Verify in CRM Analytics**: `Test_Grouped` has rows from both orgs, distinguishable by the `Source` column.

## Scenario 5: Single Entry Mode

Add a `"name"` field to the first entry in the config (e.g., `"name": "login-events"`), then:

```bash
sf dataset load --entry login-events
```

**Expected**: Only the named entry is processed. Other entries are ignored. If no entries have `name` fields, the error message includes a hint.

## Scenario 6: Error Handling

### 6.1 Invalid config

```bash
echo '{"entries":[]}' > dataset-load.config.json
sf dataset load
```

**Expected**: Exit code 2, error about entries array needing at least 1 element.

### 6.2 Operation conflict

```bash
cat > dataset-load.config.json << 'EOF'
{
  "entries": [
    {
      "sObject": "Account", "fields": ["Id"],
      "sourceOrg": "my-source", "targetOrg": "my-analytic",
      "targetDataset": "Test_Conflict", "operation": "Append"
    },
    {
      "sObject": "Contact", "fields": ["Id"],
      "sourceOrg": "my-source", "targetOrg": "my-analytic",
      "targetDataset": "Test_Conflict", "operation": "Overwrite"
    }
  ]
}
EOF
sf dataset load
```

**Expected**: Exit code 2, error about conflicting operations for `Test_Conflict`.

### 6.3 Bad org alias

```bash
cat > dataset-load.config.json << 'EOF'
{
  "entries": [
    {
      "sObject": "Account", "fields": ["Id"],
      "sourceOrg": "nonexistent-org", "targetOrg": "my-analytic",
      "targetDataset": "Test_BadOrg"
    }
  ]
}
EOF
sf dataset load
```

**Expected**: Exit code 2, error about org authentication.

## Scenario 7: Watermark Reset

```bash
# Reset a single watermark
cat .dataset-load.state.json  # note current value
# Edit to remove the key or set an older date

# Reset everything
rm .dataset-load.state.json
sf dataset load
```

**Expected**: Next run fetches from scratch (latest records for ELF, all records for SObject).

## Cleanup

Delete test datasets in Analytics Studio > Data Manager > Datasets:
- `Test_Login`
- `Test_Accounts`
- `Test_Augmented`
- `Test_Grouped`

```bash
rm -f dataset-load.config.json .dataset-load.state.json
```

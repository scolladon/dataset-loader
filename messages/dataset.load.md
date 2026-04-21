# summary

Load Event Log Files and SObject data into CRM Analytics datasets

# examples

- <%= config.bin %> <%= command.id %>

- <%= config.bin %> <%= command.id %> --config-file my-config.json --dry-run

- <%= config.bin %> <%= command.id %> --start-date 2026-01-01T00:00:00.000Z --end-date 2026-01-31T23:59:59.999Z

# flags.config-file.summary

Path to config JSON

# flags.state-file.summary

Path to watermark state file

# flags.audit.summary

Pre-flight checks only (auth, connectivity, permissions)

# flags.dry-run.summary

Show plan without executing

# flags.entry.summary

Process only the entry with this name

# flags.start-date.summary

Load only records with dateField/LogDate >= this ISO-8601 datetime (ignored for CSV entries)

# flags.end-date.summary

Load only records with dateField/LogDate <= this ISO-8601 datetime (ignored for CSV entries)

<!-- markdownlint-disable MD013 -- long prose and command-line examples -->
# Contributing

## Prerequisites

- **Node.js** >= 18
- **Salesforce CLI** (`sf`)

```bash
npm install
```

## Project Structure

```text
src/
├── commands/dataset/
│   └── load.ts              # SF CLI command (composition root)
├── domain/                   # Pure business logic, value objects
│   ├── pipeline.ts          # Core orchestration engine
│   ├── auditor.ts           # Pre-flight permission checks
│   ├── watermark.ts         # Value object: ISO 8601 timestamp
│   ├── watermark-key.ts     # Value object: entry identifier
│   ├── watermark-store.ts   # Immutable watermark map
│   └── dataset-key.ts       # Value object: (targetOrg, targetDataset/targetFile)
├── ports/
│   └── types.ts             # All port interfaces
└── adapters/                 # Infrastructure implementations
    ├── sf-client.ts          # Salesforce REST client (concurrency + retry)
    ├── elf-reader.ts         # EventLogFile reader (yields CSV lines)
    ├── sobject-reader.ts     # SObject query reader (yields CSV lines)
    ├── augment-transform.ts  # Appends extra columns to CSV lines
    ├── fan-out-transform.ts  # Tees a stream to multiple writable channels
    ├── row-counter.ts        # PassThrough that counts rows for progress
    ├── dataset-writer.ts     # CRM Analytics upload lifecycle
    ├── file-writer.ts        # Local file writer (CSV output)
    ├── config-loader.ts      # Config parsing & validation (Zod)
    ├── state-manager.ts      # Atomic watermark state file
    └── progress-reporter.ts  # CLI progress bar

test/
├── unit/
│   ├── domain/              # Domain logic tests
│   ├── adapters/            # Adapter tests with mocked ports
│   └── ports/               # Shared type & utility tests
├── nut/                     # Node Unit Tests (CLI integration)
├── fixtures/                # Shared test helpers (FakeConnectionBuilder)
└── manual/                  # Manual test scenarios
```

## Building

```bash
npm run build
```

## Running Tests

```bash
# All tests
npm test

# Watch mode
npm run test:watch

# Single file
npx vitest run test/unit/domain/watermark.test.ts

# Filter by name
npx vitest run -t "watermark"
```

## Writing Tests

Tests use [Vitest](https://vitest.dev/). Coverage target is 100% (excluding `ports/types.ts`).

### Naming Convention

Follow **Given/When/Then** in test titles, with **Arrange/Act/Assert** sections in the body. The variable under test should be named `sut`:

```typescript
it('given empty store, when setting a watermark, then returns new store with value', () => {
  // Arrange
  const sut = WatermarkStore.empty()

  // Act
  const result = sut.set(key, watermark)

  // Assert
  expect(result).not.toBe(sut)
  expect(result.get(key)).toEqual(watermark)
})
```

### Test Approach

- **Domain tests** — Test behavior through public API, verify immutability and validation rules
- **Adapter tests** — Mock port dependencies, verify correct API calls and data transformation
- **Command tests** — Verify wiring between adapters and domain services
- **NUT tests** — End-to-end command execution with mocked infrastructure

### Mocking

Tests mock port interfaces via Vitest. No external processes or network calls:

```typescript
const sfPort: SalesforcePort = {
  apiVersion: '62.0',
  query: vi.fn().mockResolvedValue({ totalSize: 0, done: true, records: [] }),
  queryMore: vi.fn(),
  getBlob: vi.fn().mockResolvedValue(''),
  getBlobStream: vi.fn(),
  post: vi.fn().mockResolvedValue({ id: '06V000000000001' }),
  patch: vi.fn().mockResolvedValue(null),
  del: vi.fn().mockResolvedValue(undefined),
}
```

## Architecture

The plugin follows **Hexagonal Architecture** (Ports & Adapters):

- **Domain** (`src/domain/`) contains pure business logic and value objects. No infrastructure dependencies.
- **Ports** (`src/ports/`) define interface contracts between domain and adapters.
- **Adapters** (`src/adapters/`) implement ports with infrastructure concerns (Salesforce API, file system, CLI).
- **Command** (`src/commands/dataset/load.ts`) is the composition root — it wires adapters to domain services.

See [DESIGN.md](DESIGN.md) for the full architecture documentation.

### Key Rules

1. Domain code must never import from `adapters/` — only from `ports/types.ts`
2. Adapters implement port interfaces and receive dependencies via constructor injection
3. The command layer is the only place where adapters are instantiated and composed

## Adding a New Feature

1. Define or extend port interfaces in `ports/types.ts` if new I/O is needed
2. Add domain logic in `domain/` with tests (TDD: red → green → refactor)
3. Implement adapter in `adapters/` with tests
4. Wire in `commands/dataset/load.ts`
5. Add NUT test for CLI integration
6. Update manual test scenarios if applicable

## Adding a New Reader Type

1. Create a new class implementing `ReaderPort` in `adapters/`
2. Add the config entry type to the Zod schema in `config-loader.ts`
3. Add a discriminated union branch for the new type
4. Handle the new type in `load.ts` when building `PipelineEntry` objects
5. Add unit tests for the reader and config validation

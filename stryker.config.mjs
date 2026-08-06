/** @type {import('@stryker-mutator/core').PartialStrykerOptions} */
const config = {
  packageManager: 'npm',
  reporters: ['html', 'clear-text', 'progress', 'json'],
  testRunner: 'vitest',
  testRunner_options: {
    configFile: 'vitest.config.ts',
  },
  coverageAnalysis: 'perTest',
  mutate: ['src/**/*.ts'],
  ignorePatterns: ['lib/**', 'coverage/**', 'reports/**'],
  // Shared across the four sibling plugins: high 95 / low 90 / break 90.
  // Note the PR job mutates only the files changed against origin/main, so the
  // score it reports is scoped to that subset and is not comparable to a full
  // run. That job is advisory (continue-on-error), so break never blocks it.
  thresholds: {
    high: 95,
    low: 90,
    break: 90,
  },
}

export default config

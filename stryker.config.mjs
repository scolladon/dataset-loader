/** @type {import('@stryker-mutator/core').PartialStrykerOptions} */
const config = {
  packageManager: 'npm',
  reporters: ['html', 'clear-text', 'progress', 'json'],
  testRunner: 'vitest',
  vitest: {
    configFile: 'vitest.config.ts',
  },
  coverageAnalysis: 'perTest',
  // Stryker rewrites tsconfig paths in its sandbox through TypeScript's JS
  // API (`ts.parseConfigFileTextToJson`), which TypeScript 7 no longer ships,
  // so every run crashed before the dry run. tsconfig.json has no `extends`
  // or `references` to rewrite: naming a file that does not exist skips the
  // rewrite.
  tsconfigFile: 'tsconfig.stryker-skip.json',
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

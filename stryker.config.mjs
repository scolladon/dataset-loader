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
}

export default config

import { defineConfig } from 'vitest/config'

// Mutation runs judge the unit suites only: a mutant must be killed by a unit
// test, not by an end-to-end scenario in test/nut that happens to cross it.
export default defineConfig({
  test: {
    root: '.',
    include: ['test/unit/**/*.test.ts'],
  },
})

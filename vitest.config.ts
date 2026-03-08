import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    root: '.',
    include: ['test/unit/**/*.test.ts', 'test/nut/**/*.nut.ts'],
    coverage: {
      include: ['src/**/*.ts'],
      exclude: ['src/ports/types.ts'],
    },
  },
})

export default {
  entry: [
    'src/commands/**/*.ts',
    '**/*.{nut,test}.ts',
    'test/perf/**/*.{ts,mjs}',
    'vitest.config.perf.ts',
    'vitest.config.mutation.ts',
    '.github/**/*.yml',
  ],
  project: ['**/*.{ts,js,json,yml}'],
  ignoreDependencies: ['sinon'],
}

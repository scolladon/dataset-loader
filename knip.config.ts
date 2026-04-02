export default {
  entry: [
    'src/commands/**/*.ts',
    '**/*.{nut,test}.ts',
    'test/perf/**/*.{ts,mjs}',
    'vitest.config.perf.ts',
    '.github/**/*.yml',
  ],
  project: ['**/*.{ts,js,json,yml}'],
  ignoreDependencies: ['sinon'],
  ignoreBinaries: ['npm-check-updates'],
}

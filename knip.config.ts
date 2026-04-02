export default {
  entry: [
    'src/commands/**/*.ts',
    'bin/dev.js',
    '**/*.{nut,test}.ts',
    'test/perf/**/*.{ts,mjs}',
    '.github/**/*.yml',
  ],
  project: ['**/*.{ts,js,json,yml}'],
  ignoreDependencies: ['@commitlint/config-conventional'],
  ignoreBinaries: ['commitlint', 'npm-check-updates'],
}

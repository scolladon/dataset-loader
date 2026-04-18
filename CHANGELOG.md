<!-- markdownlint-disable MD013 MD024 -- release-please emits "Features" headings per version and long commit-message lines -->
# Changelog

## [1.2.0](https://github.com/scolladon/dataset-loader/compare/v1.1.0...v1.2.0) (2026-04-17)

### Features

* add SObject read access and dataset ready audit checks ([#22](https://github.com/scolladon/dataset-loader/issues/22)) ([2209308](https://github.com/scolladon/dataset-loader/commit/220930874d4a2e6257333c2b28ffdd2ffe1df755))

## [1.1.0](https://github.com/scolladon/dataset-loader/compare/v1.0.0...v1.1.0) (2026-04-15)

### Features

* simplify config format ([#20](https://github.com/scolladon/dataset-loader/issues/20)) ([c045462](https://github.com/scolladon/dataset-loader/commit/c045462effa1ae6bea96e0eb80144ae9015ff322))

## 1.0.0 (2026-04-01)

### Features

* add CSV file source — load local CSV files into CRMA datasets ([a3ad7a2](https://github.com/scolladon/dataset-loader/commit/a3ad7a204bb347d48846a18b73866bdfde22f4ec))
* add file-target output — write ELF/SObject data to local CSV files ([b193f25](https://github.com/scolladon/dataset-loader/commit/b193f253132d1d4244b3481bd48b14dee87661c7))
* implement CRMA data loader SF CLI plugin ([e553373](https://github.com/scolladon/dataset-loader/commit/e5533730eae3fe6b0447a198573000fafd884720))
* replace bare-token \$var syntax with mustache {{token}} interpolation in augmentColumns ([289b29c](https://github.com/scolladon/dataset-loader/commit/289b29ce93e77b54b2d2a9e419d0ba9b8bbab198))
* support SOQL relationship traversal fields in sobject entries ([de4ac9c](https://github.com/scolladon/dataset-loader/commit/de4ac9c3e3bb5364a78a4a468604504fbe4571d9))

### Bug Fixes

* correct profiling filenames in reset.sh and PROFILING.md ([4bced5f](https://github.com/scolladon/dataset-loader/commit/4bced5f0e1732b3df7eb35623e04ec032875c931))
* defer InsightsExternalData creation to first data write ([90dd9e0](https://github.com/scolladon/dataset-loader/commit/90dd9e001f1c85571c32c6207a44be4d145a5d10))
* drain uploads before Action:Process and prevent unhandledRejection crash ([fcd34fd](https://github.com/scolladon/dataset-loader/commit/fcd34fda09de7c6358918233207b44590aed2492))
* include CompletedWithWarnings in metadata query ([5912b2e](https://github.com/scolladon/dataset-loader/commit/5912b2e5904e81471d9cb358309e967d45f09465))
* propagate chunker write errors back through pipeline via forwarder ([0ac4561](https://github.com/scolladon/dataset-loader/commit/0ac4561686033625ce4e915f9acf69e7c175817f))
* restore 100% coverage after Vitest 4 upgrade ([566178b](https://github.com/scolladon/dataset-loader/commit/566178b695de353656e2b8dd0d2ecb5dbdae0308))

### Performance Improvements

* pipeline streaming optimizations — eliminate stream hops, add FanInStream, tune gzip ([8cf4e91](https://github.com/scolladon/dataset-loader/commit/8cf4e9172255efcc0370b9f0eb295910d22f5cd4))
* reduce async pipeline overhead — batch lines, AsyncChannel, upload backpressure ([8ab0224](https://github.com/scolladon/dataset-loader/commit/8ab02242f166fee629360cab8cda50160f0cc7ff))
* stream pipeline refactor — eliminate CSV roundtrip and parallelize execution ([cee3fd4](https://github.com/scolladon/dataset-loader/commit/cee3fd48f7699a2e233f271518a1203a075900b6))

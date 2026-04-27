window.BENCHMARK_DATA = {
  "lastUpdate": 1777290399719,
  "repoUrl": "https://github.com/scolladon/dataset-loader",
  "entries": {
    "Runtime Benchmark": [
      {
        "commit": {
          "author": {
            "email": "colladonsebastien@gmail.com",
            "name": "Sebastien",
            "username": "scolladon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ebd73131cf156d2dbf853b9eb892fd531a63e761",
          "message": "chore(perf): add comprehensive performance testing infrastructure (#12)",
          "timestamp": "2026-04-02T16:37:14+02:00",
          "tree_id": "4b387b47378fb97b7eb7df7f60a216cc5b90d6dd",
          "url": "https://github.com/scolladon/dataset-loader/commit/ebd73131cf156d2dbf853b9eb892fd531a63e761"
        },
        "date": 1775140809084,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 8797770,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-to-soql",
            "value": 8712764,
            "range": "±0.20%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-5",
            "value": 1079125,
            "range": "±0.23%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-5",
            "value": 17530363,
            "range": "±0.24%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-5",
            "value": 2347570,
            "range": "±1.03%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-20",
            "value": 189282,
            "range": "±1.27%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-20",
            "value": 17578841,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-20",
            "value": 759907,
            "range": "±2.06%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-50",
            "value": 77109,
            "range": "±1.94%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-50",
            "value": 17475248,
            "range": "±0.13%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-50",
            "value": 411079,
            "range": "±0.78%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 28,
            "range": "±5.39%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 3,
            "range": "±4.80%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 0,
            "range": "±2.94%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 10469,
            "range": "±5.56%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 11467,
            "range": "±5.64%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 9750,
            "range": "±5.24%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-small",
            "value": 348348,
            "range": "±0.47%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-medium",
            "value": 38197,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-large",
            "value": 3462,
            "range": "±0.16%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 7008,
            "range": "±0.66%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 691,
            "range": "±1.69%",
            "unit": "ops/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "colladonsebastien@gmail.com",
            "name": "Sebastien",
            "username": "scolladon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a513330377feeacc36b86818be73eda5f186e499",
          "message": "chore(ci): compare perf benchmarks on same runner to eliminate CI noise (#13)",
          "timestamp": "2026-04-11T00:16:58+02:00",
          "tree_id": "b8fcf20c3f26249872e6082d46468c8f3a72e84e",
          "url": "https://github.com/scolladon/dataset-loader/commit/a513330377feeacc36b86818be73eda5f186e499"
        },
        "date": 1775859593889,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 8489643,
            "range": "±0.14%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-to-soql",
            "value": 8532084,
            "range": "±0.10%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-5",
            "value": 1110841,
            "range": "±0.17%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-5",
            "value": 16971572,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-5",
            "value": 2613422,
            "range": "±0.53%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-20",
            "value": 194767,
            "range": "±0.41%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-20",
            "value": 16962053,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-20",
            "value": 832881,
            "range": "±0.89%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-50",
            "value": 81667,
            "range": "±0.55%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-50",
            "value": 16901275,
            "range": "±0.13%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-50",
            "value": 414604,
            "range": "±0.64%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 29,
            "range": "±2.67%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 3,
            "range": "±4.03%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 0,
            "range": "±2.21%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 11281,
            "range": "±4.53%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 11929,
            "range": "±4.25%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 10235,
            "range": "±3.76%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-small",
            "value": 379821,
            "range": "±0.10%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-medium",
            "value": 38815,
            "range": "±0.10%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-large",
            "value": 3739,
            "range": "±0.21%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 7199,
            "range": "±0.54%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 746,
            "range": "±0.41%",
            "unit": "ops/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "colladonsebastien@gmail.com",
            "name": "Sebastien",
            "username": "scolladon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "34b3a9d8e23449946ff620e9c85aa8f588652610",
          "message": "ci(perf): post same-runner perf comparison as PR comment (#14)",
          "timestamp": "2026-04-12T17:07:50+02:00",
          "tree_id": "a24b3a07cb06a513ac2ee29e9cb1c5448d400df4",
          "url": "https://github.com/scolladon/dataset-loader/commit/34b3a9d8e23449946ff620e9c85aa8f588652610"
        },
        "date": 1776006642950,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 8134703,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-to-soql",
            "value": 8048400,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-5",
            "value": 1115708,
            "range": "±0.14%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-5",
            "value": 17719529,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-5",
            "value": 2653862,
            "range": "±0.62%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-20",
            "value": 194072,
            "range": "±0.47%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-20",
            "value": 17692875,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-20",
            "value": 850756,
            "range": "±1.06%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-50",
            "value": 81936,
            "range": "±0.64%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-50",
            "value": 17628986,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-50",
            "value": 425314,
            "range": "±0.76%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 31,
            "range": "±6.49%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 3,
            "range": "±5.66%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 0,
            "range": "±1.77%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 10369,
            "range": "±7.89%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 11268,
            "range": "±4.56%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 10358,
            "range": "±3.83%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-small",
            "value": 381763,
            "range": "±0.10%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-medium",
            "value": 38433,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-large",
            "value": 3728,
            "range": "±0.14%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 6715,
            "range": "±0.61%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 696,
            "range": "±0.41%",
            "unit": "ops/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "colladonsebastien@gmail.com",
            "name": "Sebastien",
            "username": "scolladon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c045462effa1ae6bea96e0eb80144ae9015ff322",
          "message": "feat: simplify config format (#20)",
          "timestamp": "2026-04-15T16:46:14+02:00",
          "tree_id": "d4850d08d0c57821433a5403d19df80065f830b8",
          "url": "https://github.com/scolladon/dataset-loader/commit/c045462effa1ae6bea96e0eb80144ae9015ff322"
        },
        "date": 1776264546967,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 7838673,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-to-soql",
            "value": 7776149,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-5",
            "value": 1062351,
            "range": "±0.33%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-5",
            "value": 17070796,
            "range": "±0.25%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-5",
            "value": 2579341,
            "range": "±0.94%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-20",
            "value": 187376,
            "range": "±1.16%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-20",
            "value": 17586797,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-20",
            "value": 830620,
            "range": "±1.61%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-50",
            "value": 76542,
            "range": "±1.84%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-50",
            "value": 17438091,
            "range": "±0.45%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-50",
            "value": 403204,
            "range": "±1.29%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 33,
            "range": "±3.50%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 3,
            "range": "±3.86%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 0,
            "range": "±3.08%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 10271,
            "range": "±6.02%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 11423,
            "range": "±5.38%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 9681,
            "range": "±5.12%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-small",
            "value": 377901,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-medium",
            "value": 38290,
            "range": "±1.09%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-large",
            "value": 3675,
            "range": "±0.14%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 6898,
            "range": "±0.70%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 699,
            "range": "±0.82%",
            "unit": "ops/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "colladonsebastien@gmail.com",
            "name": "Sebastien",
            "username": "scolladon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "220930874d4a2e6257333c2b28ffdd2ffe1df755",
          "message": "feat: add SObject read access and dataset ready audit checks (#22)",
          "timestamp": "2026-04-17T13:03:35+02:00",
          "tree_id": "150639d186325fe41a223f922ceab31f3514bed8",
          "url": "https://github.com/scolladon/dataset-loader/commit/220930874d4a2e6257333c2b28ffdd2ffe1df755"
        },
        "date": 1776423982469,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 8313602,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-to-soql",
            "value": 7976076,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-5",
            "value": 1072890,
            "range": "±0.25%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-5",
            "value": 16657466,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-5",
            "value": 2538539,
            "range": "±0.99%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-20",
            "value": 193903,
            "range": "±1.19%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-20",
            "value": 16417304,
            "range": "±0.13%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-20",
            "value": 829549,
            "range": "±1.44%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-50",
            "value": 79515,
            "range": "±1.66%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-50",
            "value": 16410211,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-50",
            "value": 403868,
            "range": "±0.93%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 32,
            "range": "±4.43%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 4,
            "range": "±4.11%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 0,
            "range": "±3.06%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 10630,
            "range": "±5.40%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 11382,
            "range": "±5.13%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 9829,
            "range": "±4.77%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-small",
            "value": 379303,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-medium",
            "value": 38611,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-large",
            "value": 3640,
            "range": "±0.21%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 7159,
            "range": "±0.78%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 710,
            "range": "±0.53%",
            "unit": "ops/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "colladonsebastien@gmail.com",
            "name": "Sebastien",
            "username": "scolladon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "132432e23f4014709136d9df2082c7dc1f74394c",
          "message": "fix: performance, security and correctness issues (#24)",
          "timestamp": "2026-04-18T10:48:40+02:00",
          "tree_id": "ccdaeb410e9c3af0858db64872a168d71198a39f",
          "url": "https://github.com/scolladon/dataset-loader/commit/132432e23f4014709136d9df2082c7dc1f74394c"
        },
        "date": 1776502238463,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 8349016,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-to-soql",
            "value": 8459412,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-5",
            "value": 1007121,
            "range": "±0.29%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-5",
            "value": 17632368,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-5",
            "value": 2529171,
            "range": "±0.66%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-20",
            "value": 190040,
            "range": "±0.53%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-20",
            "value": 17655296,
            "range": "±0.15%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-20",
            "value": 850020,
            "range": "±1.06%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-50",
            "value": 79735,
            "range": "±0.84%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-50",
            "value": 17679808,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-50",
            "value": 423918,
            "range": "±0.75%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 2087,
            "range": "±1.00%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 228,
            "range": "±3.49%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 9,
            "range": "±4.53%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 9579,
            "range": "±6.41%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 11740,
            "range": "±4.78%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 10146,
            "range": "±4.45%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-small",
            "value": 377733,
            "range": "±0.23%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-medium",
            "value": 38835,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-large",
            "value": 3694,
            "range": "±0.13%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 7208,
            "range": "±0.63%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 721,
            "range": "±1.06%",
            "unit": "ops/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "colladonsebastien@gmail.com",
            "name": "Sebastien",
            "username": "scolladon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f1040c4e33af2d1609a9d8a8ef8dd6af04364c88",
          "message": "fix(align): enforce SObject dataset column order against CRMA metadata (#26)",
          "timestamp": "2026-04-20T17:49:02+02:00",
          "tree_id": "4b18b8f4b5072d92a1b7f16cdb41a5ee127ed931",
          "url": "https://github.com/scolladon/dataset-loader/commit/f1040c4e33af2d1609a9d8a8ef8dd6af04364c88"
        },
        "date": 1776700278079,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 8376061,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-to-soql",
            "value": 8230596,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-5",
            "value": 1091662,
            "range": "±0.23%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-5",
            "value": 17551400,
            "range": "±0.19%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-5",
            "value": 2605383,
            "range": "±0.68%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-20",
            "value": 189600,
            "range": "±0.72%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-20",
            "value": 17641077,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-20",
            "value": 851447,
            "range": "±1.27%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-50",
            "value": 80337,
            "range": "±1.04%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-50",
            "value": 17581792,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-50",
            "value": 421501,
            "range": "±0.80%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 2090,
            "range": "±1.09%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 236,
            "range": "±3.52%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 9,
            "range": "±2.99%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 9667,
            "range": "±6.28%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 11551,
            "range": "±4.95%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 10092,
            "range": "±4.48%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-small",
            "value": 381618,
            "range": "±0.10%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-medium",
            "value": 38184,
            "range": "±0.14%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-large",
            "value": 3692,
            "range": "±0.16%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 7211,
            "range": "±0.59%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 717,
            "range": "±1.09%",
            "unit": "ops/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "colladonsebastien@gmail.com",
            "name": "Sebastien",
            "username": "scolladon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c6154bc3203236898c33f087b1ff8b5ad990c907",
          "message": "fix(pipeline): surface writer-init errors instead of silently swallowing them (#31)",
          "timestamp": "2026-04-21T11:08:49+02:00",
          "tree_id": "026ab3dd33cd3cad1a858162ce5f564461683594",
          "url": "https://github.com/scolladon/dataset-loader/commit/c6154bc3203236898c33f087b1ff8b5ad990c907"
        },
        "date": 1776762662221,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 8543228,
            "range": "±0.09%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-to-soql",
            "value": 8474928,
            "range": "±0.09%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-5",
            "value": 967152,
            "range": "±0.28%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-5",
            "value": 19416688,
            "range": "±0.09%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-5",
            "value": 2441917,
            "range": "±0.68%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-20",
            "value": 181231,
            "range": "±0.59%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-20",
            "value": 18219885,
            "range": "±0.09%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-20",
            "value": 887051,
            "range": "±1.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-50",
            "value": 79737,
            "range": "±1.02%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-50",
            "value": 18787712,
            "range": "±0.09%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-50",
            "value": 436597,
            "range": "±0.53%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 2397,
            "range": "±1.20%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 259,
            "range": "±3.22%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 11,
            "range": "±3.07%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 13221,
            "range": "±7.23%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 13226,
            "range": "±5.50%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 10592,
            "range": "±5.08%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-small",
            "value": 406229,
            "range": "±0.26%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-medium",
            "value": 40683,
            "range": "±0.06%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-large",
            "value": 3487,
            "range": "±0.14%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 7104,
            "range": "±0.71%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 706,
            "range": "±1.13%",
            "unit": "ops/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "colladonsebastien@gmail.com",
            "name": "Sebastien",
            "username": "scolladon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "cbd82c9aae13f4fd58c9cd9dbe60f6a0dec072de",
          "message": "feat(load): add --start-date / --end-date flags with dry-run bounds UX (#33)",
          "timestamp": "2026-04-22T18:43:24+02:00",
          "tree_id": "eacb67b7de61c9fb11617e995bd4313ce4bd57f2",
          "url": "https://github.com/scolladon/dataset-loader/commit/cbd82c9aae13f4fd58c9cd9dbe60f6a0dec072de"
        },
        "date": 1776876335266,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 8079424,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-to-soql",
            "value": 7824096,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-5",
            "value": 1069195,
            "range": "±0.20%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-5",
            "value": 17396017,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-5",
            "value": 2494007,
            "range": "±0.66%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-20",
            "value": 185695,
            "range": "±0.72%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-20",
            "value": 17015645,
            "range": "±0.22%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-20",
            "value": 843500,
            "range": "±1.04%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-50",
            "value": 78942,
            "range": "±0.96%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-50",
            "value": 17248822,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-50",
            "value": 415757,
            "range": "±0.73%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 1816,
            "range": "±1.39%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 228,
            "range": "±3.95%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 8,
            "range": "±3.55%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 9534,
            "range": "±7.23%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 11429,
            "range": "±4.63%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 9948,
            "range": "±4.51%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-small",
            "value": 379799,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-medium",
            "value": 38285,
            "range": "±0.10%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-large",
            "value": 3642,
            "range": "±0.18%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 7251,
            "range": "±0.68%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 708,
            "range": "±1.12%",
            "unit": "ops/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "colladonsebastien@gmail.com",
            "name": "Sebastien",
            "username": "scolladon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "eb114b30a3f3c7a57766b3e63dddfa27fb5b8bed",
          "message": "feat(progress): show per-reader progress totals from firstPage.totalSize (#35)",
          "timestamp": "2026-04-27T12:16:59+02:00",
          "tree_id": "0187fb3dfc815e15a49e157d24d422594ebaa307",
          "url": "https://github.com/scolladon/dataset-loader/commit/eb114b30a3f3c7a57766b3e63dddfa27fb5b8bed"
        },
        "date": 1777285157452,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 8522845,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-to-soql",
            "value": 8544290,
            "range": "±0.13%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-5",
            "value": 1072432,
            "range": "±0.25%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-5",
            "value": 17081523,
            "range": "±0.13%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-5",
            "value": 2608325,
            "range": "±0.77%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-20",
            "value": 184831,
            "range": "±0.54%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-20",
            "value": 17139199,
            "range": "±0.33%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-20",
            "value": 832536,
            "range": "±1.42%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-50",
            "value": 77727,
            "range": "±1.59%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-50",
            "value": 16696191,
            "range": "±0.19%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-50",
            "value": 423401,
            "range": "±0.74%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 2022,
            "range": "±1.18%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 220,
            "range": "±3.77%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 9,
            "range": "±3.97%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 10676,
            "range": "±5.49%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 11612,
            "range": "±5.09%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 10085,
            "range": "±4.16%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-small",
            "value": 373951,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-medium",
            "value": 38176,
            "range": "±0.29%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-large",
            "value": 3504,
            "range": "±1.23%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 7251,
            "range": "±0.60%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 708,
            "range": "±1.05%",
            "unit": "ops/sec"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "colladonsebastien@gmail.com",
            "name": "Sebastien",
            "username": "scolladon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "50f2d7fb7ac2fa4472acc1e8e97477e53826d427",
          "message": "feat(audit): enforce FLS via WITH SECURITY_ENFORCED on SObject read check (#37)",
          "timestamp": "2026-04-27T13:44:21+02:00",
          "tree_id": "13c58237f29652540f5274eaa2f52e460351addc",
          "url": "https://github.com/scolladon/dataset-loader/commit/50f2d7fb7ac2fa4472acc1e8e97477e53826d427"
        },
        "date": 1777290399159,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 11595098,
            "range": "±0.12%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-to-soql",
            "value": 11525333,
            "range": "±0.17%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-5",
            "value": 1505045,
            "range": "±0.31%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-5",
            "value": 21769675,
            "range": "±0.13%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-5",
            "value": 3097049,
            "range": "±1.69%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-20",
            "value": 248103,
            "range": "±1.63%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-20",
            "value": 21760304,
            "range": "±0.15%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-20",
            "value": 996266,
            "range": "±2.21%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-create-50",
            "value": 99530,
            "range": "±2.55%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-get-50",
            "value": 21746984,
            "range": "±0.11%",
            "unit": "ops/sec"
          },
          {
            "name": "watermark-store-set-50",
            "value": 451517,
            "range": "±1.30%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 2712,
            "range": "±1.24%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 272,
            "range": "±3.80%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 12,
            "range": "±3.41%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 19619,
            "range": "±6.83%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 16911,
            "range": "±6.29%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 15471,
            "range": "±4.64%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-small",
            "value": 459016,
            "range": "±0.14%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-medium",
            "value": 48592,
            "range": "±0.19%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-augment-large",
            "value": 4755,
            "range": "±0.18%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 8796,
            "range": "±0.69%",
            "unit": "ops/sec"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 871,
            "range": "±1.56%",
            "unit": "ops/sec"
          }
        ]
      }
    ]
  }
}
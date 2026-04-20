window.BENCHMARK_DATA = {
  "lastUpdate": 1776700278774,
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
      }
    ]
  }
}
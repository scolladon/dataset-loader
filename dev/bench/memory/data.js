window.BENCHMARK_DATA = {
  "lastUpdate": 1786022541653,
  "repoUrl": "https://github.com/scolladon/dataset-loader",
  "entries": {
    "Memory Benchmark": [
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
        "date": 1775140810399,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.20%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0009,
            "range": "±0.23%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.24%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±1.03%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0053,
            "range": "±1.27%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0013,
            "range": "±2.06%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.013,
            "range": "±1.94%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.13%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0024,
            "range": "±0.78%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 35.3135,
            "range": "±5.39%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 301.9472,
            "range": "±4.80%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 3254.9734,
            "range": "±2.94%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.0955,
            "range": "±5.56%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0872,
            "range": "±5.64%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.1026,
            "range": "±5.24%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0029,
            "range": "±0.47%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0262,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2889,
            "range": "±0.16%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1427,
            "range": "±0.66%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.4465,
            "range": "±1.69%",
            "unit": "ms"
          }
        ]
      }
    ],
    "Latency Benchmark": [
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
        "date": 1775859595430,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.14%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0009,
            "range": "±0.17%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.53%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0051,
            "range": "±0.41%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0012,
            "range": "±0.89%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0122,
            "range": "±0.55%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.13%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0024,
            "range": "±0.64%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 34.3224,
            "range": "±2.67%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 309.5328,
            "range": "±4.03%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 2933.889,
            "range": "±2.21%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.0886,
            "range": "±4.53%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0838,
            "range": "±4.25%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.0977,
            "range": "±3.76%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0026,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0258,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2675,
            "range": "±0.21%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1389,
            "range": "±0.54%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.3406,
            "range": "±0.41%",
            "unit": "ms"
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
        "date": 1776006644842,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0009,
            "range": "±0.14%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.62%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0052,
            "range": "±0.47%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0012,
            "range": "±1.06%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0122,
            "range": "±0.64%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0024,
            "range": "±0.76%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 32.4963,
            "range": "±6.49%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 329.7491,
            "range": "±5.66%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 3285.027,
            "range": "±1.77%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.0964,
            "range": "±7.89%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0887,
            "range": "±4.56%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.0965,
            "range": "±3.83%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0026,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.026,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2682,
            "range": "±0.14%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1489,
            "range": "±0.61%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.4375,
            "range": "±0.41%",
            "unit": "ms"
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
        "date": 1776264548602,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0009,
            "range": "±0.33%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.25%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.94%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0053,
            "range": "±1.16%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0012,
            "range": "±1.61%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0131,
            "range": "±1.84%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.45%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0025,
            "range": "±1.29%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 30.397,
            "range": "±3.50%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 292.0704,
            "range": "±3.86%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 3134.0204,
            "range": "±3.08%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.0974,
            "range": "±6.02%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0875,
            "range": "±5.38%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.1033,
            "range": "±5.12%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0026,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0261,
            "range": "±1.09%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2721,
            "range": "±0.14%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.145,
            "range": "±0.70%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.4309,
            "range": "±0.82%",
            "unit": "ms"
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
        "date": 1776423983792,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0009,
            "range": "±0.25%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.99%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0052,
            "range": "±1.19%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.13%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0012,
            "range": "±1.44%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0126,
            "range": "±1.66%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0025,
            "range": "±0.93%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 31.6604,
            "range": "±4.43%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 280.5529,
            "range": "±4.11%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 2964.4677,
            "range": "±3.06%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.0941,
            "range": "±5.40%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0879,
            "range": "±5.13%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.1017,
            "range": "±4.77%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0026,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0259,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2747,
            "range": "±0.21%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1397,
            "range": "±0.78%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.4087,
            "range": "±0.53%",
            "unit": "ms"
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
        "date": 1776502240351,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.001,
            "range": "±0.29%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.66%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0053,
            "range": "±0.53%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.15%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0012,
            "range": "±1.06%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0125,
            "range": "±0.84%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0024,
            "range": "±0.75%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 0.4792,
            "range": "±1.00%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 4.3832,
            "range": "±3.49%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 107.0899,
            "range": "±4.53%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.1044,
            "range": "±6.41%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0852,
            "range": "±4.78%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.0986,
            "range": "±4.45%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0026,
            "range": "±0.23%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0257,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2707,
            "range": "±0.13%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1387,
            "range": "±0.63%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.3875,
            "range": "±1.06%",
            "unit": "ms"
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
        "date": 1776700280253,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0009,
            "range": "±0.23%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.19%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.68%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0053,
            "range": "±0.72%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0012,
            "range": "±1.27%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0124,
            "range": "±1.04%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0024,
            "range": "±0.80%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 0.4786,
            "range": "±1.09%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 4.2291,
            "range": "±3.52%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 109.6757,
            "range": "±2.99%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.1034,
            "range": "±6.28%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0866,
            "range": "±4.95%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.0991,
            "range": "±4.48%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0026,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0262,
            "range": "±0.14%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2708,
            "range": "±0.16%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1387,
            "range": "±0.59%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.395,
            "range": "±1.09%",
            "unit": "ms"
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
        "date": 1776762664015,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.09%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.09%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.001,
            "range": "±0.28%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.09%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.68%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0055,
            "range": "±0.59%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.09%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0011,
            "range": "±1.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0125,
            "range": "±1.02%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.09%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0023,
            "range": "±0.53%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 0.4172,
            "range": "±1.20%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 3.8592,
            "range": "±3.22%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 92.1463,
            "range": "±3.07%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.0756,
            "range": "±7.23%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0756,
            "range": "±5.50%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.0944,
            "range": "±5.08%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0025,
            "range": "±0.26%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0246,
            "range": "±0.06%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2868,
            "range": "±0.14%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1408,
            "range": "±0.71%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.4155,
            "range": "±1.13%",
            "unit": "ms"
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
        "date": 1776876336866,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0009,
            "range": "±0.20%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.66%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0054,
            "range": "±0.72%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.22%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0012,
            "range": "±1.04%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0127,
            "range": "±0.96%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0024,
            "range": "±0.73%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 0.5506,
            "range": "±1.39%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 4.3894,
            "range": "±3.95%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 118.0049,
            "range": "±3.55%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.1049,
            "range": "±7.23%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0875,
            "range": "±4.63%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.1005,
            "range": "±4.51%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0026,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0261,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2746,
            "range": "±0.18%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1379,
            "range": "±0.68%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.4121,
            "range": "±1.12%",
            "unit": "ms"
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
        "date": 1777285159017,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.13%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0009,
            "range": "±0.25%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.13%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.77%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0054,
            "range": "±0.54%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.33%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0012,
            "range": "±1.42%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0129,
            "range": "±1.59%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.19%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0024,
            "range": "±0.74%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 0.4944,
            "range": "±1.18%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 4.5489,
            "range": "±3.77%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 105.7449,
            "range": "±3.97%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.0937,
            "range": "±5.49%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0861,
            "range": "±5.09%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.0992,
            "range": "±4.16%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0027,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0262,
            "range": "±0.29%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2853,
            "range": "±1.23%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1379,
            "range": "±0.60%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.4131,
            "range": "±1.05%",
            "unit": "ms"
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
        "date": 1777290400997,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.17%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0007,
            "range": "±0.31%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0,
            "range": "±0.13%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0003,
            "range": "±1.69%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.004,
            "range": "±1.63%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0,
            "range": "±0.15%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.001,
            "range": "±2.21%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.01,
            "range": "±2.55%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0022,
            "range": "±1.30%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 0.3688,
            "range": "±1.24%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 3.6778,
            "range": "±3.80%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 84.0255,
            "range": "±3.41%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.051,
            "range": "±6.83%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0591,
            "range": "±6.29%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.0646,
            "range": "±4.64%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0022,
            "range": "±0.14%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0206,
            "range": "±0.19%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2103,
            "range": "±0.18%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1137,
            "range": "±0.69%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.1477,
            "range": "±1.56%",
            "unit": "ms"
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
          "id": "718c2cecff2842bffe168b21b98af5eb07a6eb36",
          "message": "fix(progress): live per-reader progress bar with correct unit and visual fill (#39)",
          "timestamp": "2026-05-04T12:12:57+02:00",
          "tree_id": "5ada99849e41cc32546b365a1c298ca4b9778bfc",
          "url": "https://github.com/scolladon/dataset-loader/commit/718c2cecff2842bffe168b21b98af5eb07a6eb36"
        },
        "date": 1777889715654,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.08%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.07%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0011,
            "range": "±0.19%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.64%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0054,
            "range": "±0.56%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.14%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0011,
            "range": "±1.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0123,
            "range": "±0.87%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0022,
            "range": "±0.76%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 0.4597,
            "range": "±2.76%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 3.8721,
            "range": "±2.70%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 90.5145,
            "range": "±5.99%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.074,
            "range": "±5.98%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0722,
            "range": "±5.03%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.0933,
            "range": "±4.54%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0025,
            "range": "±0.07%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0246,
            "range": "±0.07%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2673,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1332,
            "range": "±0.64%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.3677,
            "range": "±1.28%",
            "unit": "ms"
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
          "id": "508c2d5cf31695c7f1142acdfc3a0b0f82761a29",
          "message": "fix(audit): correct SOQL clause order and gracefully degrade FLS probe (#43)",
          "timestamp": "2026-05-18T14:20:32+02:00",
          "tree_id": "def6612656125e35da0459e5f3239a5c05b43bd9",
          "url": "https://github.com/scolladon/dataset-loader/commit/508c2d5cf31695c7f1142acdfc3a0b0f82761a29"
        },
        "date": 1779106967698,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0009,
            "range": "±0.34%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.80%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0049,
            "range": "±0.75%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0012,
            "range": "±1.34%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0119,
            "range": "±1.02%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0025,
            "range": "±0.91%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 0.4697,
            "range": "±1.05%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 4.5054,
            "range": "±3.78%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 102.8557,
            "range": "±3.19%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.0659,
            "range": "±6.54%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0662,
            "range": "±5.22%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.0866,
            "range": "±4.67%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0027,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0256,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.269,
            "range": "±0.15%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1515,
            "range": "±0.81%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.6334,
            "range": "±2.07%",
            "unit": "ms"
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
          "id": "76a4a71a92789b3e565030171c230d92b55c3d3c",
          "message": "feat: bootstrap upload to a dataset with no prior completed load (#45)",
          "timestamp": "2026-05-19T09:27:07+02:00",
          "tree_id": "a95b6d35b6a6522478d4e87e4480aecd1a3b9175",
          "url": "https://github.com/scolladon/dataset-loader/commit/76a4a71a92789b3e565030171c230d92b55c3d3c"
        },
        "date": 1779175764841,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0009,
            "range": "±0.20%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.16%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.58%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0052,
            "range": "±0.43%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0012,
            "range": "±0.96%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.012,
            "range": "±0.70%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0023,
            "range": "±0.72%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 0.4917,
            "range": "±1.28%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 4.3668,
            "range": "±3.73%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 103.9042,
            "range": "±3.39%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.095,
            "range": "±5.03%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0852,
            "range": "±4.53%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.0999,
            "range": "±4.18%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0026,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0262,
            "range": "±1.12%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.275,
            "range": "±0.32%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1433,
            "range": "±0.65%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.4275,
            "range": "±1.18%",
            "unit": "ms"
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
          "id": "f8557aa6a38c7ae405cde3834f0b46749ea81938",
          "message": "feat!: migrate to node 22+ with matrix testing and engine lint harness (#51)\n\nBREAKING CHANGE: requires Node.js >= 22.19.0; Node 18 and 20 are no longer supported.",
          "timestamp": "2026-07-30T15:51:58+02:00",
          "tree_id": "5ee756786a88849167848ffec72c841c124812bd",
          "url": "https://github.com/scolladon/dataset-loader/commit/f8557aa6a38c7ae405cde3834f0b46749ea81938"
        },
        "date": 1785419680815,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.14%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.10%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.001,
            "range": "±0.27%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±1.94%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0055,
            "range": "±1.59%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0012,
            "range": "±0.38%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0128,
            "range": "±0.30%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0026,
            "range": "±0.39%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 0.4836,
            "range": "±1.46%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 4.2181,
            "range": "±4.02%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 104.4075,
            "range": "±4.55%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.073,
            "range": "±13.55%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0615,
            "range": "±11.87%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.0684,
            "range": "±14.37%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0024,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0232,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2456,
            "range": "±0.14%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1792,
            "range": "±0.39%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.7531,
            "range": "±0.31%",
            "unit": "ms"
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
          "id": "c821d2926d2a30537f6442e13db4f7168168a4df",
          "message": "fix(release): replace removed npm shrinkwrap command in prepublishOnly (#53)",
          "timestamp": "2026-07-30T16:11:04+02:00",
          "tree_id": "6d2916959515574b392444a3068a7cb73b792692",
          "url": "https://github.com/scolladon/dataset-loader/commit/c821d2926d2a30537f6442e13db4f7168168a4df"
        },
        "date": 1785420783915,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.12%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.08%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.0011,
            "range": "±0.25%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.09%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.63%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0056,
            "range": "±0.38%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.08%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0013,
            "range": "±0.67%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0128,
            "range": "±0.51%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.08%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0026,
            "range": "±1.45%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 0.4017,
            "range": "±1.46%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 3.8806,
            "range": "±2.88%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 102.0561,
            "range": "±12.47%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.0698,
            "range": "±13.27%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0555,
            "range": "±13.42%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.0593,
            "range": "±11.94%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0022,
            "range": "±0.07%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0216,
            "range": "±0.06%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2564,
            "range": "±0.14%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.202,
            "range": "±0.47%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 2.0523,
            "range": "±0.50%",
            "unit": "ms"
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
          "id": "5a9bf562714801c721f332f384b9abdf279897c5",
          "message": "ci: publish pull request previews to pkg.pr.new and modernise dependency management (#55)",
          "timestamp": "2026-08-06T15:19:47+02:00",
          "tree_id": "767c32ec452f658218228df63abdd8e98a9bbfd0",
          "url": "https://github.com/scolladon/dataset-loader/commit/5a9bf562714801c721f332f384b9abdf279897c5"
        },
        "date": 1786022541625,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "watermark-parse-valid",
            "value": 0.0001,
            "range": "±0.24%",
            "unit": "ms"
          },
          {
            "name": "watermark-to-soql",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-5",
            "value": 0.001,
            "range": "±0.20%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-5",
            "value": 0.0001,
            "range": "±0.21%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-5",
            "value": 0.0004,
            "range": "±0.34%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-20",
            "value": 0.0054,
            "range": "±0.21%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-20",
            "value": 0.0001,
            "range": "±0.11%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-20",
            "value": 0.0012,
            "range": "±0.35%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-create-50",
            "value": 0.0127,
            "range": "±0.39%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-get-50",
            "value": 0.0001,
            "range": "±0.13%",
            "unit": "ms"
          },
          {
            "name": "watermark-store-set-50",
            "value": 0.0025,
            "range": "±1.72%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-small",
            "value": 0.4853,
            "range": "±1.19%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-medium",
            "value": 4.1266,
            "range": "±3.95%",
            "unit": "ms"
          },
          {
            "name": "throughput-gzip-chunking-large",
            "value": 103.744,
            "range": "±3.35%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-1-channels",
            "value": 0.0726,
            "range": "±11.49%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-3-channels",
            "value": 0.0627,
            "range": "±11.87%",
            "unit": "ms"
          },
          {
            "name": "throughput-fanout-5-channels",
            "value": 0.069,
            "range": "±13.88%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-small",
            "value": 0.0024,
            "range": "±0.13%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-medium",
            "value": 0.0239,
            "range": "±0.42%",
            "unit": "ms"
          },
          {
            "name": "throughput-augment-large",
            "value": 0.2503,
            "range": "±0.16%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-1k",
            "value": 0.1714,
            "range": "±0.37%",
            "unit": "ms"
          },
          {
            "name": "throughput-channel-push-consume-10k",
            "value": 1.7797,
            "range": "±0.50%",
            "unit": "ms"
          }
        ]
      }
    ]
  }
}
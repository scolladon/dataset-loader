window.BENCHMARK_DATA = {
  "lastUpdate": 1776423983813,
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
      }
    ]
  }
}
window.BENCHMARK_DATA = {
  "lastUpdate": 1775859594278,
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
      }
    ]
  }
}
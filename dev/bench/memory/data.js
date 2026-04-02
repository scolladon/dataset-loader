window.BENCHMARK_DATA = {
  "lastUpdate": 1775140810422,
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
    ]
  }
}
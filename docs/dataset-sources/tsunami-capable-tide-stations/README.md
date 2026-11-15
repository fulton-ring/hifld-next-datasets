# Tsunami-capable tide stations

Source/job: NOAA NCEI's tsunami-capable tide-station roster, staged by the explicit Dagster source asset. This is an archival roster; no dated release or operational status was established. Weekly refresh remains **STOPPED**. Source, rights, identifier/schema, baseline and release compatibility decisions remain unresolved; publication is gated pending maintainer approval.

Original research, raw captures, reports and replay logs are retained outside Git in `/tmp/hifld-native-reconstruction/cleanup334/artifacts/issue-334-removed-research-docs-ce0d9c477c78.tar.gz` (SHA-256 `f5ad964bd01a710a23b8ee0bfad442c896b939cc8829a8ff3137960290c9c8b8`, 927157 bytes). Restore the archived folder with `tar -xzf /tmp/hifld-native-reconstruction/cleanup334/artifacts/issue-334-removed-research-docs-ce0d9c477c78.tar.gz -C .`.

The independent transport guards run without captures:

```bash
PYTHONPATH=src:tests:. uv run python -m unittest tests.test_tsunami_tide_stations.TideTransportTests
```

The 18 complete-original-capture tests remain opt-in through `TSUNAMI_TIDE_TEST_CAPTURE_DIR`, pointed directly at an externally retained original capture directory. No replacement data is provided.

The three standalone audit/replay scripts remain available outside Git at `/workspace/work/hifld-coordinator/artifacts/issue-334-unused-audits-ce0d9c477c78.tar.gz` (SHA-256 `99418092559fa35ffe03adea3215f4cf3f86f906387c3a33d54084f0ecad4b39`). Restore them with `tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-334-unused-audits-ce0d9c477c78.tar.gz -C .`.

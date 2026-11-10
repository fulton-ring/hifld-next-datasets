# Historical Tsunami Event Locations draft

The NOAA NCEI historical tsunami event source is a reviewable draft. The weekly job `tsunami_events_draft_weekly` (`tsunami_events_draft_refresh`) remains `STOPPED`. Source authority and coverage, baseline and schema compatibility, field/ID/null/date/geometry contracts, licensing, and release approval remain unresolved human decisions; this cleanup does not accept them or change pipeline code.

Historical research, execution records, raw-capture pins and audit material were retained losslessly in `/workspace/work/hifld-coordinator/artifacts/issue-113-removed-research-docs-ed9e3b4f762c.tar.gz` (109138 bytes, SHA-256 `ec040835e13e148217d78d9a9e6297b0a19130edd37d1538693e72daa18d45a2`). Its sidecar manifest records all 50 files. Restore from the repository root with `tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-113-removed-research-docs-ed9e3b4f762c.tar.gz -C .`.

The unused standalone audit tool was retained separately in `/workspace/work/hifld-coordinator/artifacts/issue-113-unused-audit-tool.tar.gz` (4865 bytes, SHA-256 `084bb611bc24f19d9081a34f4481e48ed93efae1c27f5296e0f60b72e5fce046`). Restore it with `tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-113-unused-audit-tool.tar.gz -C .`.

The default synthetic guard suite is `PYTHONPATH=src:tests:. python -m unittest discover -s tests -p test_historical_tsunami_events.py`. The complete real-capture test is explicitly opt-in via `TSUNAMI_EVENTS_TEST_CAPTURE_DIR=/path/to/complete-capture` for that same command. No test was run for this documentation cleanup.

# Census Combined Statistical Areas draft

The draft reconstructs the inventory-linked 2024 Census national geodatabase `Combined_Statistical_Area` layer. Its monthly source-integrity job remains **STOPPED**. No release approval is implied.

Source edition and lineage (2024 layer vs. 2026 successor; linked 2023 metadata), the baseline, source-to-published IDs, field types, geometry differences, and output contract remain unresolved decisions. The original ZIP and all source, baseline, and runtime evidence remain preserved outside this archive operation.

The 30-file research/evidence bundle is `/workspace/work/hifld-coordinator/artifacts/issue-40-removed-research-docs-48bf50e0ed65.tar.gz` (46,627 bytes, SHA-256 `5c49e21c5ccdc3b98e2707296556fe29d167e5d410d803e00fa9268e58bdebf7`). Its manifest lists every member's size and SHA-256. Restore the historical docs with:

```bash
tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-40-removed-research-docs-48bf50e0ed65.tar.gz -C /workspace/hifld-worker-worktrees/40
```

The producer declaration guard uses the verbatim 4,008-byte `FIELD_1` XML from the archived evidence as a compact fixture at `tests/fixtures/csa_native_layer_definition.xml`. It runs in the default focused tests without restoring the evidence bundle:

```bash
PYTHONPATH=src:tests:. uv run python -m unittest tests.test_combined_statistical_areas_source
```

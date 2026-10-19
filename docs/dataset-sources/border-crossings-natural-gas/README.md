# Natural gas border crossings — unapproved North America draft

This adapter preserves the complete official NACEI/NRCan layer: 99 native EPSG4326 points,
24 original attributes and OBJECTIDs (US51/Canada28/Mexico20); no filter, cast or ID mapping.
It is a changed-source/coverage proposal, **not** reconstruction of the exact EIA US51 release.
Official delegation: https://open.canada.ca/data/en/dataset/e313db89-4219-4a5e-a543-772a86068710 .
The exact EIA service requires a token; source/grain/coverage/rights/schema/IDs/geometry,
baseline and contract acceptance remain human release decisions. The schedule stays STOPPED.
Set isolated `DAGSTER_HOME`, `HIFLD_STAGING_DIR` and `HIFLD_DATASETS_DIR`, then run
`uv run dagster job execute -m dagster_hifld.definitions -j nacei_natural_gas_border_crossings_draft_refresh`.
`HIFLD_NATURAL_GAS_CROSSINGS_CAPTURE_DIR` optionally replays a complete captured input directory;
`uv run python -m unittest discover tests -p test_natural_gas_border_crossings.py` runs focused tests.
Lossless raw inputs and separate complete six-format published US51 candidates are retained.
Capture/replay artifact: `source-and-published-captures.tar.gz`, 447064 bytes, 155 verified members;
SHA256 `f6e01995e83dee365cc8f44b1dfd38727ecd9cba69245a6746fbfec54c43d9e1` (coordinator artifacts).
GPKG TEXT widths remain declared in raw/Arrow metadata; OID int64/nullability are unapproved encodings.

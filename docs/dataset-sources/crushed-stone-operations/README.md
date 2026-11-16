# Crushed-stone historical candidate (draft)

Explicit input: the USGS-owned "Active Mines and Mineral Processing Plants in the United States in 2003" distribution (`https://mrdata.usgs.gov/mineplant/mineplant.zip`, metadata `https://mrdata.usgs.gov/metadata/mineplant.xml`), pinned as package `usgs-mrdata-mineplant-2003-crushed-stone-v1`; no default fetch or archive polling. The pinned USGS legacy metadata `https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/crstonx020.xml` documents the retired National Atlas "Crushed Stone Operations" layer (2,324 points, active 2002, production over 50,000 tons, published 200506); the survey subset is the producer COMMODITY class `Crushed Stone` (2,190 rows in 47 states, aggregates bound over 30,000 tons per year in 2002), a different vintage, threshold, schema and ID family, not a recreation of the retired layer.
The 2,190-row native NAD27 candidate preserves all 9 mineplant DBF fields, exact Decimal128 values, original mineplant ids (613–2802), SHP/DBF record indices and Point WKB in GeoParquet; no datum correction, uppercase conversion or ID mapping.
Published 766-row MINOP construction-minerals data remains separate comparison context; source edition/identity selection, baseline/ID/schema/coverage compatibility, rights, contract and release require external decisions.

Set `HIFLD_CRUSHED_STONE_CAPTURE_DIR` to the complete explicit capture directory and `HIFLD_CRUSHED_STONE_PACKAGE_ID` to the ID above.
Run registered job `crushed_stone_preservation_reconstruct`; schedule `crushed_stone_preservation_integrity` is STOPPED.
Real-input tests: `HIFLD_CRUSHED_STONE_TEST_CAPTURE_ARCHIVE=/path/to/captures.tar.gz PYTHONPATH=.:src:tests python -m unittest test_crushed_stone`.
Isolated complete replay: `PYTHONPATH=.:src python scripts/verify_crushed_stone_draft.py --capture-dir /path/to/explicit-package --runtime /new/isolated/runtime`.

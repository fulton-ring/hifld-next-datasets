# Crushed-stone historical candidate (draft)

Explicit input: Stanford `cs268nn2622` preservation-as-provided package; USGS historical `crstonx020.xml` identifies the active-2002 dataset family.
Package ID: `stanford-cs268nn2622-preservation-as-provided-v1`; no default fetch or archive polling.
The 2,324-row native NAD83 candidate preserves all 11 DBF fields, IDs, exact Decimal128 values and Point WKB in GeoParquet.
Published 766-row MINOP construction-minerals data remains separate; source authenticity/selection, dataset identity, baseline/ID/schema/coverage compatibility, rights, contract and release require external decisions.

Set `HIFLD_CRUSHED_STONE_CAPTURE_DIR` to the complete preserved capture directory and `HIFLD_CRUSHED_STONE_PACKAGE_ID` to the ID above.
Run registered job `crushed_stone_preservation_reconstruct`; schedule `crushed_stone_preservation_integrity` is STOPPED.
Real-input tests: `HIFLD_CRUSHED_STONE_REAL_CAPTURE_DIR=/path/to/explicit-package PYTHONPATH=.:src:tests python -m unittest test_crushed_stone test_crushed_stone_registration`.
Isolated complete replay: `PYTHONPATH=.:src python scripts/verify_crushed_stone_draft.py --capture-dir /path/to/explicit-package --runtime /new/isolated/runtime`.
Full real input artifact `coordinator48:crushed-stone-complete-real-fixture-artifact.tar.gz`: SHA256 `4ed1451fb98c24fcd710236b8550157ceccc09597cbd0d733b157ae6b5d7d343`.
Full verification artifact `coordinator48:crushed-stone-verification-artifact.tar.gz`: SHA256 `838a8b27e78a885bde016f7cd3c8e1dad42c47d8b9ad4d9781137b41d894142b`.
Artifacts are retained locally by the coordinator; full capture/manifests/reports stay outside Git. Issue #48 / PR #462 carry progress and external decisions.

# CBRS Buffer Zone — unapproved reconstruction

Official FWS publication2024-12-19: all1,145 original NAD83 polygons, three native Double fields and original FIDs retained with rawZIP/all25members. This is not a legal CBRS determination.
Pipeline: `sources/cbrs_buffer.py`, `assets/fws.py`; registered monthly integrity schedule remains STOPPED. Use explicit local staging/output and isolated DAGSTER_HOME.

Default focused behavior: `uv run --frozen python -m unittest discover -s tests -p 'test_cbrs_buffer_source.py'`. Small SYNTHETIC fixtures establish behavior only.
For complete real-input tests set `HIFLD_CBRS_CAPTURE_DIR` to the preserved `evidence/captures` directory. Reconstruct with job `fws_cbrs_buffer_draft_refresh`, ingest config `capture_dir` set to that matching directory.

Complete original Git evidence/tools remain in `original-research-captures-tools-9b137cd2.tar.gz`, SHA256 `8c8c715c4b9109331e3d9fd0422182a1bc670f949e4e5ad4113e3e6834a85cab`; its127members were hash/Gitblob/physical-permission replayed before removal. Original runtime captures and all historical proofs remain preserved externally; replay requires matching originals, not live refetch.

The1,206-row published prohibition grain differs from this1,145-row buffer source. Baseline/source selection, originalFID/proposed schema/geometry/format/rights contracts and seven authentic BUFF_DIST=0.0 values remain unapproved. Provenance and promotion gates block release; no production write or acceptance.

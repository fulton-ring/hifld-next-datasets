# Aviation Facilities draft pipeline

FAA NASR via the official BTS Aviation Facilities ArcGIS service; producer release and fetch times remain distinct.
Implementation: `src/dagster_hifld/aviation_extract.py` and `src/dagster_hifld/assets/faa.py`.
Use isolated `DAGSTER_HOME`, local staging/output, and the source asset `capture_dir` config for complete preserved inputs.
Run `faa_aviation_draft_refresh`; refresh is stopped and promotion requires independent approval.
Tests: `tests/test_aviation_extract.py`; its small original-field fixture is not an approved contract.
The complete 19,427-row/91-field input retains raw bodies/receipts, source IDs and original returned geometry.
PR #431 depends on FAA/shared implementation PR #427; shared machinery is reviewed in PR #421.
Full captures, logs and prior reports are external checksummed artifacts; detailed run results remain in issue #20/PR #431.
Removed research artifact: `issue-20-removed-research-docs-4e97fae7f755.tar.gz`, SHA256 `811b138f435eb505469332c262e4dc38193ff7eb7f1a9365cff04386bb9c750c`.
Release decisions: source/baseline/schema/IDs/types/coverage/formats and conflicting layer versus returned-page CRS metadata.

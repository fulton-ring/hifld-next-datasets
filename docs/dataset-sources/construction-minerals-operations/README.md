# Construction Minerals Operations preservation candidate

- Source package: https://purl.stanford.edu/ds401wt4784.xml, with its five master shapefile members at https://stacks.stanford.edu/file/druid:ds401wt4784/.
- Existing historical dataset-family metadata: https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop2x020.xml. This is a historical metadata document, not proof that Stanford is an authorized current USGS distributor.
- Local job: `construction_minerals_preservation_reconstruction`; its monthly schedule is STOPPED.
- Candidate mode requires explicit package selection. It does not fetch or poll by default and makes no authority, datum, schema/ID mapping, baseline-contract, rights, or release-acceptance claim.

A single bounded retrieval on 2026-10-21 captured all 13 URLs already pinned in the source module. Every response was HTTP 200 and matched its existing exact byte-count and SHA-256 pin. Stanford/USGS inputs and six separately labeled existing HIFLD output/baseline inputs are retained under `/workspace/work/42-evidence/capture-once/`; the test archive is `/workspace/work/42-evidence/construction-minerals-captures.tar.gz`. The exact per-object response URL, observed time, byte count, and digest are in each receipt. These retrieval times are not source publication dates. The Stanford members are treated only as a preservation package as provided; the HIFLD artifacts remain a comparison candidate, not an accepted baseline.

The archived workspace paths referenced by the previous draft were absent during recovery. No replacement source or baseline was substituted. The preservation inputs retain 766 source records, original DBF declarations, point coordinates and the source-provided WGS 1984 PRJ. The candidate separately retains the existing published identifiers; no cross-package mapping or datum transformation is inferred. Release, authority, compatibility and rights decisions remain pending.

The complete-capture tests can be run with:

```bash
HIFLD_CONSTRUCTION_MINERALS_TEST_CAPTURE_ARCHIVE=/workspace/work/42-evidence/construction-minerals-captures.tar.gz \
  python -m unittest tests.test_construction_minerals tests.test_construction_minerals_registration
```

No production write or release acceptance is implied.

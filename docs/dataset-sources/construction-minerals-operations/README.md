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

## Isolated verification

The final clean-main source code is commit `b3704857ee66671309eb3d9675c8a0811ea20f84`; the additive merge of current main is `758d512dacbfbc6ab09fabd643b6ea1ceaa116b0`. The no-Git runtime was made from that final tree and reported `sha256-packaged-dagster-hifld-v1`, `working_tree_status: unknown`, and `packaged-sha256:7415a0ac0cb79432f0e3ff6e86b3f58b1ce614904484a5b8483c2bc8d6f8817f`. That explicit package identity is recorded under `/workspace/work/42-evidence/no-git-processing-identity.json`; it is not presented as a Git commit.

Using the retained 13-object capture and persistent isolated Dagster home, registered source run `253ac2e7-ba22-4101-a6cd-0412ce1d3d81`, catalog run `333737c6-5a15-411f-8a7c-eecc5b36e9f0`, registered quality check `f2913515-7f71-4093-923e-61d04d7e56ba`, and post-catalog source/cache replay `a3d51737-8d40-4c88-bce6-2cb8716ec0d3` succeeded. The catalog reports 766 points, zero invalid or null geometries, and a passing technical quality check. The source and catalog results do not accept source authority, package authenticity, datum, a baseline/contract, rights, or release.

Two registered failure graphs used the retained actual candidate or an isolated copy. Quality-check run `f1e09d3c-76b1-498c-b00a-121f9c6ad5df` read a deliberately false quality flag in the copied candidate; the blocking check failed and neither verify nor promote started. Empty-registry run `254a1714-4b51-4dd8-ac22-4a73a19d8529` selected verify and promote against the actual candidate; verify refused with `missing_independent_release_approval_registry` and promote did not start. Both runs retained the 14-object synthetic previously accepted published fixture unchanged (inventory digest `b22034447d13f113f380250d3a0e47bd093ea924a111ff818a3ed95843230441`). These are isolated safety proofs, not release acceptance. Persistent run logs, databases, candidate outputs, the false-quality copy, and replay harnesses are in `/workspace/work/42-evidence/`.

The focused capture-backed source and registration tests passed (17 tests); the focused current-main registration-union check passed separately. No full repository suite or CI result is claimed. The scheduled job remains STOPPED.

# Construction Minerals Operations preservation candidate

- Source package: https://purl.stanford.edu/ds401wt4784.xml, with its five master shapefile members at https://stacks.stanford.edu/file/druid:ds401wt4784/.
- Existing historical dataset-family metadata: https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop2x020.xml. This is a historical metadata document, not proof that Stanford is an authorized current USGS distributor.
- Local job: `construction_minerals_preservation_reconstruction`; its monthly schedule is STOPPED.
- Candidate mode requires explicit package selection. It does not fetch or poll by default and makes no authority, datum, schema/ID mapping, baseline-contract, rights, or release-acceptance claim.

A single bounded retrieval on 2026-10-21 captured all 14 URLs already pinned in the source module. Every response was HTTP 200 and matched its existing exact byte-count and SHA-256 pin. Seven Stanford/USGS inputs and seven separately labeled existing HIFLD output/baseline inputs are retained under `/workspace/work/42-evidence/capture-once/`; the test archive is `/workspace/work/42-evidence/construction-minerals-captures.tar.gz`. The exact per-object response URL, observed time, byte count, and digest are in each receipt. These retrieval times are not source publication dates. The Stanford members are treated only as a preservation package as provided; the HIFLD artifacts remain a comparison candidate, not an accepted baseline.

The archived workspace paths referenced by the previous draft were absent during recovery. No replacement source or baseline was substituted. The preservation inputs retain 766 source records, original DBF declarations, point coordinates and the source-provided WGS 1984 PRJ. The candidate separately retains the existing published identifiers; no cross-package mapping or datum transformation is inferred. Release, authority, compatibility and rights decisions remain pending.

The complete-capture tests can be run with:

```bash
HIFLD_CONSTRUCTION_MINERALS_TEST_CAPTURE_ARCHIVE=/workspace/work/42-evidence/construction-minerals-captures.tar.gz \
  python -m unittest tests.test_construction_minerals tests.test_construction_minerals_registration
```

No production write or release acceptance is implied.

## Isolated verification

The final branch head is `b7d1f55188ab1b912b3e692cc6d556badd950952`, based on frozen main `4f184eaa0020a71bb355a18bd70af64e7a95aea1`. The final no-Git runtime was extracted from that exact tree and reports `sha256-packaged-dagster-hifld-v1`, `working_tree_status: unknown`, and `packaged-sha256:9bef2174ee85e474e0def340045569eeb07aadb885ed3859fe3bd50a18792798`. The identity record is `/workspace/work/42-evidence/no-git-processing-identity.json`; the package digest is not presented as a Git commit. The source code identity inputs match the packaged runtime; only this README was edited afterward.

Using the retained 14-object capture, a clean isolated staging directory, and persistent Dagster home, source run `e64934de-b801-43bc-8d91-78b7b63950e9`, catalog run `cdfd4b9d-833b-4f07-87fe-5114a21a0ba8`, and post-catalog source/cache replay `a8c76d5d-16a9-46b8-abf5-95106932f06a` succeeded. The resulting candidate is `v0.0.0-draft-stanford-preservation-f96902cd7c45e9b1-8b2fac6f8e896847`. The catalog reports all 766 points, zero invalid or null geometries, and passing technical geometry quality. This does not accept source authority, package authenticity, datum, baseline/contract equivalence, rights, or release. The scheduled job remains STOPPED.

The current publish graph now requires staged STAC metadata before verification. The registered false-quality run `aca896f7-1576-41dc-b270-dd7f674238ed` used a copied candidate with `quality_check_passed=false` and a fixture note; verify and promote were selected, the blocking quality check failed first, and neither verify nor promote started. The registered empty-approval run `12ff38ba-7bac-4c79-8938-65fbe51c5948` selected verify and promote against an isolated copy of the candidate. A schema-minimal synthetic `collection.json` was added to that copy solely to reach the approval check; it is not a valid or accepted release artifact. Verification refused for missing exact-object approval and promote did not start. Both controls left the populated 14-object synthetic published fixture unchanged (inventory digest `b22034447d13f113f380250d3a0e47bd093ea924a111ff818a3ed95843230441`). These are isolated gate proofs, not release acceptance.

Actual release-metadata preparation was attempted with the local default public root `http://localhost:8333`; no GCS bucket or `HIFLD_PORTOLAN_PUBLIC_ROOT` was configured. The generated asset hrefs therefore used HTTP, which the Portolan profile rejects because it requires HTTPS. This records a local public-root configuration prerequisite, not a source-candidate or source-adapter defect. Configure an approved HTTPS public root before attempting release metadata; source selection, rights and release acceptance remain pending. No valid release metadata or accepted release is claimed. The run logs, persistent Dagster database, clean runtime, staging copies, fixture, and harnesses are in `/workspace/work/42-evidence/`. The focused capture-backed source and registration tests passed (17 tests) before the additive frozen-main merge; no full repository suite or CI result is claimed.

# Construction Minerals Operations USGS producer-successor candidate

- Assigned dataset: USGS National Atlas *Construction Minerals Operations* layer (`minop2x020`, publication 200512, operations active 2003), identified by the pinned USGS metadata at https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop2x020.xml.
- Candidate input (USGS-owned producer successor, explicit selection only): https://mrdata.usgs.gov/mineplant/mineplant.zip — *Active Mines and Mineral Processing Plants in the United States in 2003* (USGS National Minerals Information Center, FGDC publication 2005), with its FGDC metadata at https://mrdata.usgs.gov/metadata/mineplant.xml. The pipeline never fetches or polls by default and makes no authority, datum, schema/ID mapping, baseline-contract, rights, or release-acceptance claim.
- Original-release blocker (dated 2026-11-10): the retired layer's own distribution `http://dds.cr.usgs.gov/pub/data/nationalatlas/minop2x020_nt00019.tar.gz` returns 404, and `https://dds.cr.usgs.gov/` now redirects to the ERS login. The USGS EROS EarthExplorer National Atlas collection requires an ERS account registration (a human step) for download. No third-party mirror was substituted for the input.
- Retained comparison context only (never inputs): the pinned Stanford preservation masters of the retired layer (`purl.stanford.edu/ds401wt4784`) and the existing published HIFLD v1 objects. They prove successor fidelity and preserve the published identifier roster.
- Successor fidelity (machine-checked at staging time): the USGS package contains all 766 retired-layer operations in its construction-minerals subset — 764 exact one-to-one rows (three with literal `NULL` site names where the retired layer stored empty text), one company string truncated by the successor's 50-character DBF width (retired-layer `MINOP2X020` 126), and one retired-layer duplicate (`MINOP2X020` 539/540) carried once (`mineplant id` 2928). LATITUDE/LONGITUDE are numerically identical in both USGS products; the contradictory datum claims (successor PRJ NAD 1927, retired-layer PRJ WGS 1984, retired-layer metadata NAD 1983) are retained unresolved and no transformation is applied.
- Local job: `construction_minerals_preservation_reconstruction`; its monthly schedule is STOPPED.

A single bounded anonymous retrieval on 2026-11-10 captured all 16 pinned objects at HTTP 200 with exact pinned byte counts and SHA-256 digests: three USGS inputs (`mineplant.zip` 513,191 bytes, `6dabd3bb...`; `mineplant.xml` 26,366 bytes, `419629ed...`; atlas legacy metadata 19,769 bytes, `d68776d0...`), six Stanford preservation masters, and seven existing HIFLD comparison objects. Per-object receipts record the observed URL, time, byte count and digest. Retrieval times are not source release dates.

The complete-capture tests can be run with:

```bash
HIFLD_CONSTRUCTION_MINERALS_TEST_CAPTURE_ARCHIVE=/workspace/work/42-evidence/construction-minerals-usgs-captures.tar.gz \
  python -m unittest tests.test_construction_minerals tests.test_construction_minerals_registration
```

The 16 capture-dependent tests skip by default; the registration tests remain in the default suite. No production write or release acceptance is implied. Baseline/schema/ID/coverage contract and rights acceptance remain external decisions; the source manifest and candidate metadata record `approved: false`.

## Isolated verification

Verified at code head `feaf98917fc540abd2b1eba56293446a0b874c26` (branch `codex/cloud-dataset-42`, PR #453) in an owner-scoped factory task sandbox (clone at exact head, `uv sync --frozen`, isolated `DAGSTER_HOME` and isolated local staging/published roots, no GCS). With the retained 16-object capture as the explicit local package:

- Registered source materialization run `dfd7038a-3066-49b7-90e6-85b391b9a5ee` staged candidate `v0.0.0-draft-usgs-mineplant-6a6a4e2b2716422e-d81325cbee927103` (765 rows) and retained the full raw USGS package, receipts, audit, successor comparison and separately retained published identifiers.
- Catalog run `a8b2c7fc-eed9-42bd-9529-68f3694d887d` summarized the candidate through the native geoparquet catalog path: 765 Point features, zero invalid or null geometries, bounds `[-122.816, 25.763, -68.677, 48.737]`, `quality_check_passed: true`. The staged dictionary lists the original mineplant columns (`id`, `COMMODITY`, `SITE_NAME`, `COMPANY_NA`, `STATE_LOCA`, `COUNTY`, `LATITUDE`, `LONGITUDE`, `PLANT_MIN`, native record indices, `geometry`).
- Post-catalog source refresh run `fa9cbebb-7e10-4f0e-91a5-d05cf8f219f7` verified and reused the unchanged candidate with every catalog derivative present; no second version was created.
- Native GeoParquet: 51,321 bytes, SHA-256 `ded608215b80d73414d7192675f7201a42096e68d1ade5fafc72360a000ab805`; source provenance labels the three USGS objects `producer_publication`/`candidate_input` and every retained preservation or published object `archive`/`comparison_only_retained_prior_research`.

Focused checks at the same head: 16 capture-backed construction-minerals tests and the registration tests passed against the retained capture archive (run in batches under the sandbox command cap; the real-POSIX unreadable-receipt negative was skipped as root and its unchanged code path was previously reviewed under an ordinary user). The complete repository suite was also run once at this head: 2075 tests, 270 skipped, 7 failures and 2 errors — every non-passing case is a symlink/root-alias rejection test in shared runtime modules (construction_state, arc_regions, cbrs_units_source, fdic_locations, fema_regions) that reproduces identically at base `7c7c1933` in the same root-user sandbox; none touches the construction-minerals diff. Release approval was not requested; the shared verify/promote gates and the stopped schedule are unchanged from the reviewed shared runtime, and no production or GCS write occurred. Source selection, datum resolution, schema/ID mapping, baseline/contract acceptance, rights and release remain external decisions.


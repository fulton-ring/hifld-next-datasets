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

(Sandbox verification evidence for the current head is recorded here during implementation.)

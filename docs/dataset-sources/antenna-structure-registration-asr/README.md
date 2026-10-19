# Antenna Structure Registration (ASR): source access and compatibility blocker

Issue #18 remains open. No working reconstruction is claimed. This evidence supports a concrete external blocker: the complete official FCC ASR bulk records and their schema cannot currently be retrieved from this runtime. `data.fcc.gov/download/pub/uls/complete/r_tower.zip` and `a_tower.zip`, FCC's ASR landing/download pages, and a schema-document discovery URL returned HTTP 403 Akamai **Access Denied** bodies. Runtime policy was unrestricted/enforced, revision 8. These are origin HTTP failures, not CONNECT policy denials. The separate historical `webgis3.fcc.gov` service did return a CONNECT 403; do not generalize that failure to FCC's other hosts. Nothing changed network policy or credentials.

The exact inventory row attributes FCC and links DataLumos project 238655. The original DHS crosswalk DOI redirected to DataLumos, which returned a provider HTTP 403 challenge. The archive also returned 403. The workbook match is **unverified**, rather than proven unmatched. The preceding issue40 discussion was read; it supplies no fresh FCC extraction audit or acceptance decision.

The reachable FCC Socrata catalog does not establish a current ASR replacement: ASR search returned zero results, while antenna/structure searches returned distinct FSS earth-station and ULS 3650 populations. These are not silently substituted. ArcGIS discovery found FCC-associated organization `YnOQrIGdN9JGtBh4`; its matching records were a wildfire map and Feature/Map services explicitly titled December 2017, described as January 7, 2018. The historical service item says `FCC Internal Use`, despite public item access. Its title, age and license prevent treating it as a maintained, unrestricted ASR continuation. Other regional/HIFLD copies were discovery leads only. Bounded searches do not prove that no alternative feed exists.

## Complete published candidate audit

The current public HIFLD catalog resolves the exact inventory slug and file to v1.0.0. The complete 2,099,257-byte GeoParquet candidate is retained, SHA-256 `fc43f476d4181538ad139d0e6504122cf4f258e0dd1d4817981377c62b73258d`, GCS generation `1791506349781360`. A second generation-specific read returned identical complete bytes. It remains **unapproved**; the `derived_from` archive link establishes the catalog's attribution, not an independent original-source match.

Every one of 17,666 rows and all 31 physical columns were audited. All geometries are valid, nonempty Point geometries with the recorded CRS metadata and extent. `RegNum` and `UniqSysID` each contain 17,666 distinct, non-null int64 values. `id` has only 11 distinct values and 17,655 duplicate rows. No identifier is replaced or discarded.

The complete candidate exhibits material name/value inconsistencies: 17,656 rows contain values outside N/S in `LatDir`, outside E/W in `LonDir`, and non-HTTP numeric text in `url`. None of the 7,465 non-null declared `latdec`/`londec` pairs exactly matches Point(y,x). The first row declares `LatDir="51.2"`, `LonDir="33.7"`, `LocState="MARS HILL"`, `latdec=70`, `londec=46.5475555556`, `url="-67.8093611111"`, while its geometry is Point(-67.8093611108266,46.5475555551448). This supports investigation of positional/schema corruption; it does not establish an approved corrective mapping or actual FCC raw field meanings. No field shift, ID substitution, geometry repair or coordinate derivation is applied.

`published-analysis.json` preserves physical Arrow types/nullability, schema metadata, all-column ordered value digests, full identifier cardinality and geometry/coverage diagnostics. The original Parquet preserves every value and identifier. No complete upstream comparison is possible without complete FCC source bytes and a verified dictionary. No national completeness, currency, filtering policy or source refresh cadence is inferred from a historical candidate's count or a portal edit timestamp.

## Required external inputs and next implementation

Supply an accessible complete official FCC ASR release (or a faithfully preserved complete official capture with observed request/final URLs, receipts, schema dictionary and release attribution), or establish an explicitly delegated maintained distributor. That will permit a draft parser and full join/coordinate accounting without guessing table layouts or selection rules. Then implement lossless relational staging, exact identifiers, deterministic local candidate construction, explicit Dagster registration, completeness/drift quarantine, proposed stopped refresh, and isolated graph/promotion-preservation tests using #417. Pending real contract/baseline acceptance **permits** this draft implementation and local testing; it only blocks release. The actual current blocker is missing verified complete upstream data/schema, not a blanket documentation-only restriction.

Separately, a maintainer must decide the compatibility treatment of the observed published schema/identifier inconsistencies. Preserving the original candidate and raw future source is required. No accepted baseline, source, contract, mapping, filter, current release date or production approval is invented here.

## Reproduce offline

From this worktree root:

```sh
UV_CACHE_DIR=/workspace/.cache/uv uv sync --frozen
PYTHONPATH=src uv run --frozen python docs/dataset-sources/antenna-structure-registration-asr/validate_evidence.py
PYTHONPATH=src uv run --frozen python docs/dataset-sources/antenna-structure-registration-asr/analyze_candidate.py
```

Evidence replay checks every retained file's SHA-256 and size, all HTTP body receipts, the generation-specific baseline pin, and the complete row audit. Original response bodies and observed final URLs are retained; no missing redirect evidence is fabricated. Live discovery URLs can change. No FCC data pipeline was materialized, no CI/test-suite pass is claimed, and no production data changed. The unrelated missing sibling `hifld-next-iac/environments/prod/dagster.tf` remains an explicit full-suite limitation.

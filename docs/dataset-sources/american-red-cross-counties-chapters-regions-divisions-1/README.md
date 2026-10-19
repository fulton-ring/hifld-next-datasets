# Red Cross county assignments: producer and release decision

The exact #13 inventory has a coherent county-assignment candidate, independently audited here. The complete published candidate has **3,162 rows**; current ARC master county layer 5 has **3,163 rows**. This is distinct from #12’s Chapter/State identity problem. A missing approved contract permits draft implementation, but the current evidence does not independently establish the publisher’s delegation or the actual effective release. No dataset pipeline is claimed complete.

The public master item `8c6753c3c5814d05be65723cd4615d7c` is owned by `NHQRedCrossGIS`, organization `pGfbNJoYypmNq86F`. Its own metadata attributes production to American Red Cross Humanitarian Services Operations / Disaster Data and Systems Operations. The organization identifies itself as American Red Cross National Headquarters, but those are same-portal claims. Fresh anonymous official Red Cross SSO reads link `https://rcview.redcross.org/`; that hostname actually redirects to a Volunteer Connection login. No observed path connects it to this ArcGIS account or county feed. The official humanitarian-mapping and fire-risk pages describe other mapping products and do not resolve the county-feed delegation. Search results are discovery leads, not authority evidence.

The item/service title says FY27, effective July 2026. The same service description prominently says it is temporarily reverted to FY26 January 2026 and will update about July 14. A September layer edit timestamp and the October fetch date cannot resolve which effective release the actual rows represent. DataLumos project 238572 and the DHS DOI crosswalk separately returned provider-side HTTP 403 pages; their contents remain **unverified**, not an unmatched crosswalk. Current HIFLD catalog, GCS, official Red Cross and ArcGIS requests succeeded under configured/observed unrestricted, enforced runtime revision 9.

## Complete evidence and compatibility differences

Four ordered original ESRI JSON pages contain 1,000 / 1,000 / 1,000 / 163 records with native geometry, all 61 fields and unchanged original identifiers. Counts, full OBJECTID sets, layer schema/edit metadata agree before and after extraction; the terminal page is empty. Transfer-limit flags are true on the first three pages and false/absent only on the last. All 3,163 GeoIDs are unique, while 3,153 FIPS values repeat across assignment pieces. FIPS is therefore not a row key.

The complete comparison joins **original GeoID values for diagnostics only**:

- 3,150 GeoIDs overlap; 12 historical GeoIDs disappear and 13 current GeoIDs appear. Alaska changes from 29 to 30 assignment rows; county/chapter assignments also change elsewhere.
- Among shared GeoIDs, 145 original OBJECTIDs differ. Historical published `id` equals historical OBJECTID on every row. No replacement IDs or cross-version remapping is approved.
- 3,143 shared geometries are topologically equal; seven differ topologically. Only 228 are ordered/normalized exact, because the historical candidate contains only MultiPolygons while the source contains 2,927 Polygons and 236 MultiPolygons. Native CRS is EPSG:3857 on both sides.
- The historical candidate has 23 invalid geometries and the current candidate 16. Invalid coordinates are preserved; no repair or simplification is authorized.
- The historical schema truncates field names to ten characters. The report explicitly labels positional/truncation correspondences as diagnostic hypotheses. They are not approved output mappings. Current `Acres` / `SQ_MI` are declared Double although the historical output uses int64. Source population/income strings retain commas, dollar signs, nulls and leading-zero identifiers without inferred conversion.

`evidence/full-comparison.json` records every shared-GeoID attribute difference and geometry comparison, complete schemas, source-only/baseline-only rows, IDs, coverage and validity. `evidence/public-reads.json` records dated request URLs, status, wire/decoded sizes, response metadata, redirects and SHA-256 pins. Raw captures are preserved at `/workspace/work/hifld-coordinator/13`, with their complete roster in `evidence/capture-manifest.json`. The complete 14,701,784-byte published GeoParquet candidate is pinned independently; it remains unapproved.

## Concrete external decision

1. Supply an official Red Cross publication/delegation link or producer confirmation tying the public master item, account and county layer to the maintained Red Cross feed.
2. Confirm the actual effective county release and resolve the simultaneous FY27 and FY26 claims.

Once that source prerequisite is established, the complete extraction is technically suitable for draft adapter implementation. Baseline and compatibility acceptance still gate release: every source/published identifier, row-grain/coverage change, native type, field name and geometry must remain reviewable. An old HIFLD/PASDA/DataLumos snapshot must not be relabeled as a maintained archive-refresh source. The user’s draft-implementation authorization is recorded in the issue and source record; this blocker concerns unresolved producer lineage and vintage, not an absent schema approval alone.

## Replay

From this branch, with the preserved complete captures and repository dependencies:

```bash
PYTHONPATH=src /workspace/hifld-worker-worktrees/19/.venv/bin/python \
  docs/dataset-sources/american-red-cross-counties-chapters-regions-divisions-1/audit_captures.py \
  /workspace/work/hifld-coordinator/13 --output /tmp/red-cross-counties-comparison.json
```

The replay verifies every raw-file SHA-256/size before parsing. It checks complete pagination, original IDs, exact schemas, before/after metadata, source/published coverage and every shared geometry without a tolerance or repair. It passed on the actual full captures. This is source-feasibility evidence; no Dagster materialization, integration, CI or full-suite success is implied. No source, dependency, workflow, contract, accepted release or production object was changed.

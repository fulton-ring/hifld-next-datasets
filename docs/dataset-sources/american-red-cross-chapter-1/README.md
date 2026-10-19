# American Red Cross Chapter: dataset identity decision

The published object under `american-red-cross-chapter-1` contains **56 state polygons**, while the inventory describes Chapter Regions. A complete candidate feed has **226 Chapter polygons**. Replacing one with the other would silently change the dataset. This is an unresolved dataset identity and producer authority problem, beyond a pending schema contract.

The complete published GeoParquet candidate (5,815,573 bytes, generation `1791506347645440`, SHA-256 `12dae3c2f686c1e3bead863a9a8884c230131c5424c3c1d04ba288e93697c3f4`) was pinned anonymously on 2026-10-19. It is not an accepted baseline. Its 56 `ST_FIPS` values, original `OBJECTID` values and all nine shared attributes match the current master service's **States layer 4**, not its Chapters layer 3. Two truncated published shape attributes also numerically match the corresponding source fields; this diagnostic comparison does not approve renaming. Published `Acres`/`SQ_MI` are int64 versus source Double. Published `id` equals `OBJECTID` in all 56 rows. Ordered and normalized zero-tolerance geometry comparisons match 30/56; topological equality matches 53/56. All feature comparisons, types and coverage are in [full-comparison.json](evidence/full-comparison.json). No geometry tolerance, repair or identifier substitution was applied.

The candidate master item `8c6753c3c5814d05be65723cd4615d7c` is owned by `NHQRedCrossGIS` in organization `pGfbNJoYypmNq86F`, whose public portal identifies itself as American Red Cross National Headquarters and lists a redcross.org contact. This is promising first-party attribution, but the current audit has **not independently anchored that ArcGIS organization to an official Red Cross link or explicit delegation**. The official `www.redcross.org/sso/success.html` links `https://rcview.redcross.org/`; the actual observed 301 redirects to Volunteer Connection's login at `https://volunteerconnection.redcross.org/?nd=vms_page&page_id=8233`. It does not establish an observed redirect to ArcGIS. The ArcGIS homepage itself says it replaced RCView in May 2024, which remains a self-assertion until corroborated. The official chapter finder is a location finder, not a Chapter polygon release.

The item's title and description identify FY27 effective July 2026, while the service description still warns that it was temporarily reverted to FY26 January 2026. The layer names identify 2027 and `dataLastEditDate` is 2026-07-15; these observations do not resolve the contradictory release text. Portal edit time and fetch time are not release dates. All these responses are preserved.

Anonymous complete layer 3 extraction retained 24,213,613 native Esri JSON bytes and a separate 24,226,236-byte GeoJSON diagnostic. All 226 original OBJECTIDs, ECODEs and Chapter_ECODEs are unique and nonnull; all 19 source columns are present. The independent count/ID census, empty terminal page and sequential after-extraction metadata/count/ID witnesses reconcile. Three native Chapter geometries are invalid under GDAL/Fiona/Shapely interpretation; they were retained without repair. The published state candidate also has three invalid geometries. A future adapter must quarantine these issues, not silently fix or drop them.

DataLumos project 238571 and the DOI crosswalk resolved to provider-side HTTP 403 Cloudflare pages. Their content is **unverified**, not an unmatched crosswalk. Public HIFLD catalog, GCS and the ArcGIS reads succeeded under unrestricted/enforced runtime revision 8. `maps.redcross.org` separately returned CONNECT 403; no environment changes or bypasses were attempted. Guessed Red Cross GIS article paths returned 404; those are unsuccessful probes, not authority evidence.

## Concrete decision required

1. Identify the intended dataset: preserve the existing 56-state product under an accurately named identity, or prepare a separately versioned 226-Chapter successor under this inventory entry. Confirm the crosswalk/historical archive mapping and preserve the mislabeled historical release.
2. Supply an official Red Cross link/delegation tying the public master service/account to the producer, and clarify the actual release represented by the conflicting FY26/FY27 metadata.
3. For the selected identity, review all source/published columns, IDs, physical types and invalid geometries. No destructive mapping, geometry repair or replacement is approved here.

A faithful draft pipeline remains authorized once its producer and dataset identity are established. No asset was added to falsely identify States as Chapters or install a third-party archive refresh. No Dagster materialization was run for #12: the exact execution blocker is unresolved source authority and conflicting dataset identity, not an absent approved contract. Shared verifier/promotion code is inherited from #417; this PR makes no source, release or production change.

## Replay and preserved inputs

```bash
UV_CACHE_DIR=/workspace/work/uv-cache uv sync --frozen
PYTHONPATH=src .venv/bin/python docs/dataset-sources/american-red-cross-chapter-1/audit_captures.py \
  /workspace/work/hifld-coordinator/12 --output /tmp/red-cross-comparison.json
```

The replay validates every pinned complete capture and performs full-row attribute, ID, schema, geometry and state-coverage comparisons. Raw bodies remain under `/workspace/work/hifld-coordinator/12/`; `evidence/capture-manifest.json` pins their names, byte counts and SHA-256. Preserve that directory for later acceptance review. Bounded metadata, complete feature comparison results and exact requested/observed final URL receipts are committed. The complete capture bundle is also preserved at `/workspace/work/hifld-coordinator/12-captures.tar.gz`; its SHA-256 is in `source.json`. Replay used the isolated worktree's frozen environment. Full suite and CI were not run for this evidence-only change; missing sibling IaC remains a known repository suite limitation. Independent review is required on the exact commit.

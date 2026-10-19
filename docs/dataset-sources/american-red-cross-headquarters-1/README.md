# American Red Cross Headquarters: exact source decisions required

The complete live headquarters candidate contains 226 records and 19 original fields, with 225 native EPSG:3857 Points and one record whose geometry member is absent. This is a bounded, complete source audit, not a working reconstruction pipeline or an accepted source. The external prerequisites are the exact publisher's distribution authority and the effective source edition. The user permits draft implementations before contract acceptance; missing contract approval alone is not the reason implementation stops here.

## Exact source and official paths

The public ArcGIS item `8c6753c3c5814d05be65723cd4615d7c`, owned by `NHQRedCrossGIS` in organization `pGfbNJoYypmNq86F`, claims American Red Cross production. Its machine URL is:

https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Master_ARC_Geography_2022/FeatureServer/0

Layer 0 is named `Master_RC_Geo_HQ_2027`. The item and service description claim FY27/July2026, but the service's prominent active notice says it has temporarily reverted to FY26/January2026. Portal edit times and this 2026-10-19 extraction do not resolve the actual edition of the returned 226 rows. Account and organization branding, copyright and the item's “No constraints, public” statement remain publisher claims, not independently established authority or rights approval.

The official Red Cross chapter finder returns200 and documents its own lookup API in captured HTML/JavaScript. A real lookup for ZIP96950 returns the Pacific Islands Region; the resulting official regional API/page and locations page return200. Those pages publish textual regional/chapter addresses and Google address-search links. They do not establish delegation to this exact master account/item or a complete native geospatial export with the source identifiers. The official SSO page links RC View, which redirects to a Volunteer Connection login; that observed path also does not bind this exact HQ publication. These are bounded observed paths, not proof that an official publication cannot exist.

The documented official Pacific Islands locations page lists Northern Mariana Islands at “P.O. Box 500814, Saipan, MP96950”. The candidate instead has Airport Rd and no geometry for OBJECTID226 / ECODE`55004`, with Status`U`, Score0 and empty Match_addr. Do not fill this point from the old release, geocode the address, or discard the record.

The exact DHS crosswalk DOI returns302 to DataLumos project241367; that destination and exact HQ archive238613 return provider HTTP403 with retained response bodies. The exact crosswalk match is **unverified/inaccessible**, not unmatched. The prior issue40 findings were read as historical context and do not establish this row's match. Four Google200 search responses are JavaScript redirect stubs and are not credited as search results or evidence of absence.

One lead, https://maps.redcross.org/, fails with ProxyError403 before an origin HTTP response. It was tested with explicitly enabled tool network access; the current runtime metadata reports configured/observed revision11, unrestricted and current. Exact URL/time/exception chains are retained in `evidence/official-maps-network-context.json`. This single-host CONNECT failure is distinct from DataLumos provider403 and does not justify a global network diagnosis or an environment change. Catalog, GCS, official redcross.org and ArcGIS reads worked.

## Complete extraction and historical comparison

Before and after extraction, the count is226, all typed OBJECTIDs1–226 match, schema/edit metadata match, and three explicit100/100/26-ID pages return every requested row without transfer-limit indication. All original response bytes, declarations, nulls, identifiers and native coordinates are retained. All226 ECODEs are unique;36 retain leading zeros. There are179 chapter and47 regional HQ records.

All five complete historical published assets were fetched from the pinned release `1704edb3-322d-469c-91da-92902fe76a2d` and checked against collection sizes/checksums; SHA256 pins are retained. They remain baseline candidates. The full native GeoPackage and Parquet comparison, without sampling or a coordinate tolerance, finds:

- Both historical tables contain226 rows. All shared attributes and native geometries agree between those two formats; their original extra IDs remain preserved.
- All226 literal ECODE values occur in both source and baseline. This is a diagnostic alignment, **not an approved source↔published identifier join**. Source OBJECTID differs for207 ECODEs; original published fid/OBJECTID/id values and every occurrence remain separately recorded.
- All226 source/baseline geometries differ. The225 supplied source Points differ from historical coordinates; native Euclidean distances range from0.00005087958324723102 to85,532.24784042698 CRS units, with223 above1 unit. No tolerance is approved. The remaining Saipan source record has no geometry while the historical baseline has a real Point, not EMPTY.
- Chapter names differ for11 records, main addresses for9, ZIPs for5, RCODEs for4 and Region_Type for1. The full comparison records all field differences, not only these examples.
- Source fields retain their full names; historical names include DBF truncations. Source Score is declared Double while the historical published Score is string. Full original source declarations, actual value types/nulls, original GeoPackage SQL types, Arrow types and DBF declarations are preserved. No destructive renaming, widening, narrowing or replacement IDs are proposed as accepted.

The complete GeoJSON and Shapefile have226 features/records; their original bytes/checksums and DBF declarations are preserved. PMTiles is fully captured/pinned; it is not decoded as an independent authoritative feature table.

## Replay and required decisions

Using the repository's managed Python dependencies:

```bash
uv run python docs/dataset-sources/american-red-cross-headquarters-1/audit_captures.py --output /tmp/headquarters-comparison.json
cmp /tmp/headquarters-comparison.json docs/dataset-sources/american-red-cross-headquarters-1/evidence/full-native-comparison.json
uv run python docs/dataset-sources/american-red-cross-headquarters-1/verify_evidence.py
```

Replay uses the complete committed original captures and verifies the collection checksums. A later live refetch is not a replacement for the preserved historical evidence. Response cookie header values are omitted only in committed receipt copies; their original receipt hashes and runtime originals remain preserved.

A maintainer or producer must establish:

1. The official Red Cross publication/delegation for this exact account, item and HQ layer, or explicitly choose a named preserved historical candidate with its unresolved lineage stated. No such choice is assumed.
2. The actual effective HQ edition, resolving simultaneous FY26/FY27 claims.
3. After source/edition resolution, an explicit geometry/coverage policy preserving the Saipan row without invented coordinates, plus an approved versioned baseline/ID/schema contract addressing the complete differences.
4. Accessible exact DHS crosswalk evidence, or an explicit decision accepting that unresolved prerequisite.

No source, baseline, rights, contract or release is approved. No source/Dagster asset or production data was changed. No materialization, full-suite or CI success is claimed for this audit; its validation is complete local evidence replay and checksum verification. The requested `tools/pipeline_workers/README.md` and `docs/agency-source-recovery/README.md` do not exist at this reviewed base; PR418 is untouched.

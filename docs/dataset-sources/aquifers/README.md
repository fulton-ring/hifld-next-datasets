# Aquifers: faithful local reconstruction draft

The official [USGS principal-aquifers page](https://www.usgs.gov/mission-areas/water-resources/science/principal-aquifers-united-states) still distributes the national **2003** producer publication. Its 2015 archive packaging date and our 2026 fetch do not represent a new aquifer assessment. The inventory scope includes CONUS, Hawaii, Puerto Rico and USVI, and depicts shallowest outcrop/subcrop extent. The 2023 version2 prototype covers three aquifer systems and is not a national substitute.

The source asset preserves all4,637 Polygons, all seven original DBF attributes/types, the exact producer ZIP and all seven members, native NAD83 coordinates, and source `OBJECTID_1`. Typed native GeoPackage and ArrowParquet plus deterministic canonical records are built in a discoverable `v0.0.0-draft-usgs2003-…` namespace. Source data has20 self-intersections: these remain intact and **fail the normal catalog quality check**. Full-row shared verification confirms zero changes but fails the same invalid source geometries; it grants no quality exception. No repair, split, deletion, reprojection or replacement identifier occurs.

The complete unapproved published Parquet has4,637 valid geometries, but source and published numeric IDs are not equivalent. A unique shared attribute-tuple diagnostic finds3,956 renumberings; this diagnostic is not an accepted crosswalk. Published data also fills1,627 source `ROCK_NAME`nulls, omits `Shape_Area`, widens integers and changes CRS. Historical published releases are retained. These differences require a reviewed versioned mapping and geometry decision before release.

Live fetches use bounded retries, complete HTTP200 and exact requested/observed URL checks. Failed or partial responses retain bytes and receipts outside discoverable candidates. Unknown archive bytes, members, CRC, schema, ID, type, null or geometry populations quarantine. Raw archive pins describe fidelity to one producer release, not acceptance. Captured replays require their actual original receipt; a local file never impersonates a fresh fetch. Immutable candidate identities include all installed pipeline source files, lock/project hashes, Python/distribution versions and native GDAL/GEOS/PROJ. Every reused object and its complete expected inventory is checked. The proposed monthly integrity schedule is stopped and all source/helper/schedule routes reject remote and PublishedStorageResource before network or writes.

Reproduce with a frozen environment and isolated local directories:

```bash
UV_CACHE_DIR=/workspace/work/uv-cache uv sync --frozen
uv run python docs/dataset-sources/aquifers/capture.py --directory /workspace/work/aquifers-capture
PYTHONPATH=src uv run python docs/dataset-sources/aquifers/audit.py --captures /workspace/work/aquifers-capture --output /workspace/work/aquifers-comparison.json
PYTHONPATH=src uv run python scripts/verification/aquifers_normal_graph.py --archive /workspace/work/aquifers-capture/zip.body --receipt /workspace/work/aquifers-capture/zip.json --workdir /workspace/work/aquifers-dagster
PYTHONPATH=src uv run python -m unittest tests.test_aquifers_source -v
```

The graph runner uses the actual registered source job, normal catalog/check/publish graph, dynamic partitions and isolated durable Dagster state. It records run IDs and full produced-data assertions; tests both real failed quality and the existing missing-approval verification asset; proves all raw/candidate bytes and prior local accepted-data fixture remain unchanged. No synthetic acceptance registry is created. Full-suite limitations include the unresolved sibling `hifld-next-iac/environments/prod/dagster.tf`; no CI or full-suite pass is inferred.

[`source.json`](source.json) and [`evidence/complete-comparison.json`](evidence/complete-comparison.json) record the producer, static release, baseline generation/checksum, complete population/schema/ID comparison and exact acceptance prerequisites. The DHS crosswalk is provider403, not a successful match. Raw full archives remain in the execution evidence directory and can be recaptured only if their pinned bytes remain available; Git retains bounded receipts, complete audit output and official landing capture.

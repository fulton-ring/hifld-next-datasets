# Aquifers: faithful local reconstruction draft

The official [USGS principal-aquifers page](https://www.usgs.gov/mission-areas/water-resources/science/principal-aquifers-united-states) still distributes the national **2003** producer publication. Its 2015 archive packaging date and our 2026 fetch do not represent a new aquifer assessment. The inventory scope includes CONUS, Hawaii, Puerto Rico and USVI, and depicts shallowest outcrop/subcrop extent. The 2023 version 2 prototype covers three aquifer systems and is not a national substitute.

The source asset preserves all 4,637 Polygons, all seven original DBF attributes/types, the exact producer ZIP and all seven members, native NAD83 coordinates, and source `OBJECTID_1`. Typed native GeoPackage and ArrowParquet plus deterministic canonical records are built in a discoverable `v0.0.0-draft-usgs2003-…` namespace. Source data has 20 self-intersections: these remain intact and **fail the normal catalog quality check**. Full-row shared verification confirms zero changes but fails the same invalid source geometries; it grants no quality exception. No repair, split, deletion, reprojection or replacement identifier occurs.

The complete unapproved published Parquet has 4,637 valid geometries, but source and published numeric IDs are not equivalent. A unique shared attribute-tuple diagnostic finds 3,956 renumberings; this diagnostic is not an accepted crosswalk. Published data also fills 1,627 source `ROCK_NAME` nulls, omits `Shape_Area`, widens integers, represents all source Polygons as MultiPolygons and changes CRS. Historical published releases are retained. These differences require a reviewed versioned mapping and geometry decision before release.

Live fetches use bounded retries, complete HTTP200 and exact requested/observed URL checks. Failed or partial responses retain bytes and receipts outside discoverable candidates. Unknown archive bytes, members, CRC, schema, ID, type, null or geometry populations quarantine. Raw archive pins describe fidelity to one producer release, not acceptance. Captured replays require their actual original receipt; a local file never impersonates a fresh fetch. Immutable candidate identities include all installed pipeline source files, lock/project hashes, Python/distribution versions and native GDAL/GEOS/PROJ. Every reused object and its complete expected inventory is checked. The proposed monthly integrity schedule is stopped and all source/helper/schedule routes reject remote and PublishedStorageResource before network or writes.

Reproduce with a frozen environment and isolated local directories:

```bash
export UV_CACHE_DIR=/workspace/work/uv-cache
uv sync --frozen
uv run python docs/dataset-sources/aquifers/capture.py --directory /workspace/work/aquifers-capture
PYTHONPATH=src uv run python docs/dataset-sources/aquifers/audit.py --captures /workspace/work/aquifers-capture --output /workspace/work/aquifers-comparison.json
PYTHONPATH=src uv run python scripts/verification/aquifers_normal_graph.py --archive /workspace/work/aquifers-capture/zip.body --receipt /workspace/work/aquifers-capture/zip.json --workdir /workspace/work/aquifers-dagster
PYTHONPATH=src uv run python -m unittest tests.test_aquifers_source -v
```

The graph runner uses the actual registered source job, normal catalog/check/publish graph, dynamic partitions and isolated durable Dagster state. It records run IDs and full produced-data assertions; tests both real failed quality and the existing missing-approval verification asset; proves all raw/candidate bytes and prior local accepted-data fixture remain unchanged. No synthetic acceptance registry is created. The earlier missing sibling IaC input is retained as historical evidence; a separate final suite used the restored real pinned sibling source and passed with one production-fixture skip. No CI or release acceptance is inferred.

[`source.json`](source.json) and [`evidence/complete-comparison.json`](evidence/complete-comparison.json) record the producer, static release, baseline generation/checksum, complete population/schema/ID comparison and exact acceptance prerequisites. The DHS crosswalk is provider403, not a successful match. Raw full archives remain in the execution evidence directory and can be recaptured only if their pinned bytes remain available; Git retains bounded receipts, complete audit output and official landing capture.

Actual materialization at source code `7a2f603c553bab56da2f463f1d521c4ee00d5a89`:

| Run | Result |
| --- | --- |
| `6b1e3169-f6f2-4e37-a93c-d556c161210b` | Registered source success; all 4,637 native rows and physical types verified |
| `567a8065-a812-4bf1-a80b-7946ce8e8c80` | Idempotent rerun; immutable source/candidate bytes unchanged |
| `62a6d120-426e-4605-9780-0169475a1ff0` | Normal full graph denied by blocking quality check: 20 native invalid polygons |
| `b78f3368-2533-46dd-b36f-8a5110622a69` | Existing verify asset denied: independent release registry absent |

All original raw/candidate objects and the prior local accepted-data fixture survived both failures. Catalog inputs equal the full structurally validated source provenance. Six final focused tests passed, including retained complete comparator sidecars and changed runtime dependency preservation. The earlier full suite ran 479 tests: 477 passed, one skipped, one errored on missing sibling IaC. That full suite preceded the final sidecar retention follow-up; it is not represented as a passing suite or a final-head CI result. See [`evidence/validation.json`](evidence/validation.json), full run report, logs and complete comparator artifacts. Independent exact-head review remains pending.

The final separate full-suite run at `4cf2c4e90907b74081a4873d7b3cd8423098304f` completed **479 tests: 478 passed, one production-fixture skip, zero errors**. The coordinator restored the complete unmodified real `fulton-ring/hifld-next-iac` source at `56aa10757129c457c530d707d9a40bae13e0907f` (archive SHA256 `ee36600c45e5bb71d5b96736a34ec4391a31dc1502c7c01ff8b2413eeadebc00`). No infrastructure command, IaC edit, fake fixture or suppression was used. `evidence/real-iac-test-input.json` pins the actual test input, and `evidence/full-suite-real-iac.log.gz` records this distinct passing run. Historical missing-input results above remain unchanged. Source, test and graph-runner code is identical to the real materialization source head; only bounded evidence/documentation followed. This local result grants no source, contract, baseline, migration, release or CI approval.

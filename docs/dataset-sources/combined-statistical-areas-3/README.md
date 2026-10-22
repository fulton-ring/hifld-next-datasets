# Census Combined Statistical Areas draft

This draft reconstructs the inventory-linked `Combined_Statistical_Area` layer from the Census 2024 national geodatabase. The monthly source-integrity schedule is stopped. Each live run makes one bounded request to the pinned official archive URL, retains its full response and receipt, and fails closed if the source bytes differ from the recorded pin.

The source output preserves all 184 original rows, IDs, attributes and native EPSG:4269 MultiPolygon WKB. The producer declares `OBJECTID` as a four-byte OID while GDAL Arrow exposes its unchanged values as int64; the candidate records this representation change as unapproved. No reprojection, ID mapping, area conversion or geometry repair is performed.

The candidate identity uses the shared packaged SHA256 scheme when running without Git metadata. It records `working_tree_status: unknown`, hashes the packaged Dagster source and lock/project files, includes installed distribution versions, and records the GDAL version used to decode the GDB.

The 2024 edition is historical, and its lineage against the linked 2023 metadata and the separate 2026 successor remains unresolved. Baseline compatibility, source/published ID mapping, types, rights and release acceptance are pending. No production publication or schedule activation is authorized.

Focused guard tests:

```bash
PYTHONPATH=src:tests:. uv run python -m unittest tests.test_combined_statistical_areas_source
```

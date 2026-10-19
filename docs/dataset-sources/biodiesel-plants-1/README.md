# Biodiesel Plants: current spatial feed requires an access decision

Issue #22 remains blocked. EIA's authoritative ArcGIS service, layer 113, and count query return **HTTP 200 with application error 499 `Token Required`**. This is a source access failure, not an unrestricted-network CONNECT denial. The complete public EIA atlas catalog contains 76 items and no biodiesel/biofuels title. This bounded audit does not prove no other official publication exists.

The responding EIA static ZIP is a historical **January 2021** extract: 75 points and eight CRC-valid sidecars. It cannot refresh the complete published candidate of **56 January 2024** points. The inventory text describes January 2023, another unresolved relationship. The DHS DOI and archive page return provider Cloudflare 403; their exact crosswalk match remains unverified.

EIA's current combined report was released **September 24, 2026**, covers **January 1, 2026**, and names 45 biodiesel plants among 253 total facilities. Its complete workbook has entity/facility/state/PADD/capacity/facility-type columns, but no geometry or source identifiers. Geocoding those names, carrying forward older coordinates by guessed name joins, or ingesting the old 75-point ZIP as current would invent a source relationship.

The full local comparison retains all rows and source OBJECTIDs. Fifty-three old/new points share exact numeric Longitude/Latitude witnesses; 22 historical points and three published points do not. Those witnesses are diagnostics, not an accepted ID join. On all 53 pairs, capacity changes physical type (DBF string versus published int64) and Period changes; 28 Company values differ. The historical source has `OBJECTID` and `Site_Name`, whereas the published candidate lacks `OBJECTID` and has `Site`. Native CRS differs (4326 versus 3857); raw coordinates and published WKB remain preserved and no geometry tolerance is accepted.

A faithful current reconstruction requires an accessible complete official GIS publication (or authorized source read access), or an explicit maintainer decision to reconstruct a historical/capacity-only successor and its location/ID lineage. Pending contract approval permits draft code and local testing, but does not supply missing source coordinates. No archive-refresh pipeline or materialization is claimed here.

Replay the exact retained inputs without network or publication:

```bash
UV_CACHE_DIR=/workspace/work/uv-cache uv sync --frozen
PYTHONPATH=src uv run --frozen python docs/dataset-sources/biodiesel-plants-1/replay.py
```

`replay.py` verifies capture byte lengths/SHA-256, full ZIP CRC/member inventory, reads every historical feature and published candidate row, compares every overlapping coordinate witness, parses the complete workbooks, and checks the three captured application-error responses. `comparison.json` contains the complete result. GCS candidate generation `1791506351389207`, SHA-256 `0d52e6312eefd3ebcb45879eb29e3ccf6841b93224d26fc3c8c544aac0a2a653`, remains **unapproved**. The 2026 current workbook and source ZIP are preserved in full.

Replay passed locally. No dataset assets or runtime code changed, no Dagster materialization was run, and no CI/full-suite pass is claimed. Missing sibling `hifld-next-iac/environments/prod/dagster.tf` remains a known repository-suite limitation. This evidence handoff does not approve a source, contract, baseline, migration, production write, merge or closure.

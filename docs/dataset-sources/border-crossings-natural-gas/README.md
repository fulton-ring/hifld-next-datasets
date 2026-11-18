# Natural gas border crossings — unapproved North America draft

Source: NACEI MapServer layer 2, Natural Gas Pipeline border crossings,
<https://geoappext.nrcan.gc.ca/arcgis/rest/services/NACEI/energy_infrastructure_of_north_america_en/MapServer/2>.
Publisher: Natural Resources Canada for the North American Cooperation on Energy
Information (NACEI), a joint publication of the US Department of Energy/EIA,
Natural Resources Canada and Mexico's Secretaría de Energía. Delegated record:
<https://open.canada.ca/data/en/dataset/e313db89-4219-4a5e-a543-772a86068710>.

Licence: Open Government Licence - Canada
(<https://open.canada.ca/en/open-government-licence-canada>). The Open Canada
record lists the service metadata as last modified 2021-05-19 with maintenance
frequency "As Needed". The layer copyright remains "North American Cooperation
on Energy Information (NACEI), August 2017".

This adapter preserves the complete official NACEI/NRCan layer: 99 native EPSG4326 points,
24 original attributes and OBJECTIDs (US51/Canada28/Mexico20); no filter, cast or ID mapping.
It is a changed-source/coverage proposal, **not** reconstruction of the exact EIA US51 release.
The exact EIA service requires a token; source/grain/coverage/rights/schema/IDs/geometry,
baseline and contract acceptance remain human release decisions. The schedule stays STOPPED.
Set isolated `DAGSTER_HOME`, `HIFLD_STAGING_DIR` and `HIFLD_DATASETS_DIR`, then run
`uv run dagster job execute -m dagster_hifld.definitions -j nacei_natural_gas_border_crossings_draft_refresh`.
`HIFLD_NATURAL_GAS_CROSSINGS_CAPTURE_DIR` optionally replays a complete captured input directory;
`uv run python -m unittest discover tests -p test_natural_gas_border_crossings.py` runs focused tests.
The GeoPackage check compares SQLite schema, spatial reference, index and every
feature/attribute row against a fresh canonical write; it does not compare
container bytes, because SQLite records transaction history in its header.
Lossless raw inputs and separate complete six-format published US51 candidates are retained.
Capture/replay artifact: `source-and-published-captures.tar.gz`, 447064 bytes, 155 verified members;
SHA256 `f6e01995e83dee365cc8f44b1dfd38727ecd9cba69245a6746fbfec54c43d9e1` (coordinator artifacts).
GPKG TEXT widths remain declared in raw/Arrow metadata; OID int64/nullability are unapproved encodings.

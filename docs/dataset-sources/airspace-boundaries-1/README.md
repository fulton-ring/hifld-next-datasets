# FAA Airspace Boundaries — unapproved draft

Source: FAA Aeronautical Information Services, `Boundary_Airspace/FeatureServer/0` (item `67885972e4e940b2aa6d74024901c561`); exact requests and provenance checks are in `faa_airspace.py`.
Pipeline: `assets/faa.py`; daily06:00 America/New_York refresh remains STOPPED. Complete native477-feature extraction retains every source identifier, field and native4269 polygon; separate served GeoJSON is diagnostic.

Run: `uv run --frozen python -m unittest tests.test_faa_airspace tests.test_refresh_registration_10`.
Use isolated local staging/output and DAGSTER_HOME for the registered `faa_airspace_draft_refresh` job. Supported draft outputs are GeoPackage/Parquet; incompatible derivatives remain disabled.

Full captures, logs, baseline audits and run evidence remain external artifacts. Removed Git research/audit tools: `issue-10-removed-research-docs-fb5ea7c1db3d.tar.gz`, SHA256 `80e30d2ed7c7b41d71df052ef11dee9f5c238e7b975add01a7afc80a7431128e`.

Source/crosswalk,375→477scope/IDs, baseline, all-column types/nullability/CRS/output mappings and release need independent acceptance. Actual runs/review decisions are recorded on issue10/PR427; promotion remains gated.

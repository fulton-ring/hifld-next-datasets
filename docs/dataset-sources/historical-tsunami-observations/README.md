# Historical Tsunami Observations — #114 / PR449

Unapproved NOAA NCEI → Time Lapse webmap → delegated observations layer: 34,495 rows × 50 native attributes, EPSG:3857. Preserve original IDs/types/WKB and BCE/unknown date strings. Registered job `noaa_tsunami_observations_draft_refresh`, weekly schedule STOPPED; isolated local storage/Dagster only. Configure its ingest op's `capture_dir` with preserved `source_http.zip` and `authority_http.zip`; explicit instance/storage setup and full-graph drivers are in the private replay-tools archive.

```sh
uv run python -m unittest discover -s tests -p test_tsunami_observations.py
HIFLD_TSUNAMI_OBSERVATIONS_CAPTURE_DIR=/private/captures uv run python -m unittest discover -s tests -p test_tsunami_observations.py
```

Complete local archives (SHA256; full member inventories/replay tools preserved alongside them):
- `reviewed-cc3acce-complete-evidence.tar.gz`: `e3c9a60b0f2a58ab9545223103999b0fdb8b4a6bbbb2a35e8ad772fa11226cc3` — all 82 original HTTP inputs and six complete historical formats.
- `cleanup-validation-ac8.tar.gz`: `829a035bba0dfd6c1cd9d2776aa5feccd315c836678fa1531a29ff14ccfb7046` — full current execution/test evidence.
- `cleanup-replay-tools-ac8.tar.gz`: `648b8cd55a63d7501231357001a6fddee86a40fd0f0b4dcb9e4d18bb62473836` — verify this hash before extracting its standalone verifiers/drivers/inventories.

Processing `ac8a8285bcc4f61a8788ed6690f6321bbc7c0348`: 564 local tests passed/one production opt-in skipped, complete-source test enabled; seven complete-source graph runs and 17 synthetic marker jobs. Strict first-marker rejection is the only source-function change; native gold, prior output and original state remain intact. Issue114/PR449 hold detailed scopes and independent review; no Git history purge or CI claim.

Release remains blocked on source/baseline/rights, ID/schema/type/nullability/geometry/coverage/output-policy/contract decisions and independent exact-artifact approval. Historical32,614 versus current34,495 rows: diagnostic32,595 common/1,900 new/19 old IDs; no join accepted. No source/baseline/release acceptance or production publication.

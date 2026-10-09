# Publisher legacy retirement

Portolan publication discovers datasets through STAC and the SQLite catalog.
Publishing no longer requires the historical Dataset API registration service.

Removed interfaces:

- `DatasetApiResource`, its HTTP registration client, and `DATASET_API_URL`,
  `DATASET_API_COLLECTION_ID`, `DATASET_API_TOKEN`, and
  `DATASET_API_AUTH_HEADER` configuration.
- `register_published_outputs`, the API-only `api_resource` and
  `storage_location_name` arguments to `run_local_version_pipeline`, and its
  `api_payload` result. The pipeline still returns its `outputs` records.
- The historical `seed-source-manifests` CLI, its implementation and tests.
  This importer only read Dataset API JSONL exports. Authored source manifests,
  their inheritance and snapshots, and inventory metadata remain active.
- The empty `dagster_hifld.publish` compatibility module. Publish assets and
  helpers remain in `dagster_hifld.assets.publish`.

No GeoServer integration exists in the publisher source, dependencies, image,
Helm values, or workflows. Historical conversion provenance is retained in the
conversion module docstring. Retired source and CLI code remain recoverable
from Git history.

Existing source compatibility remains supported: readable Shapefiles under
`unknown/`, historical compressed archives and source metadata, version parsing,
and original source ID columns. These paths operate on existing dataset bytes;
they do not call the retired services. Source manifests, quality manifests,
data dictionaries, schema validation, derived formats, immutable promotion,
SeaweedFS/S3 and GCS/local storage, release pointers, and durable Dagster state
retain their current behavior and storage paths.

Verification uses local temporary storage: conversion, catalog, source-manifest,
storage, publish, Portolan workflow, and full unit suites, plus compilation and
native Ruff comparison with the existing baseline. The GCS smoke helper now
checks published objects and catalog metadata directly; real-cloud runs remain
behind both explicit environment gates and were not run for this retirement.
No production database, bucket, release pointer, or deployment was changed.

Results: `uv run python -m unittest discover tests` passed 443 tests with one
real-cloud test skipped. Targeted publish, pipeline, Portolan workflow,
source-manifest, resource and smoke-helper suites passed; compilation and
`git diff --check` passed. Native Ruff found 23 existing findings across the
retirement runtime/test set, down from 25 before retirement. The GCS helper and
its tests pass both Ruff checks and formatting. Existing formatting failures
remain in publish, conversion, resources, Portolan workflow and pipeline tests;
they are not presented as clean gates.

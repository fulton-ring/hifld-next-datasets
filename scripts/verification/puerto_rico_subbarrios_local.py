"""Isolated local end-to-end materialization for the puerto-rico-subbarrios candidate.

Runs the actual registered ingest asset, then catalog, formats, and the release
verifier against isolated local staging/published roots and a fresh Dagster
instance. Release refusal (empty approval registry) is expected and recorded.
"""
import json
import os
import sys
import tempfile
from pathlib import Path

from dagster import DagsterInstance, Definitions, AssetSelection, asset, define_asset_job

from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.assets.publish import (
    publish_geopackage, publish_geoparquet, publish_pmtiles, publish_shapefile_zip,
    publish_version, verify_release,
)
from dagster_hifld.assets.puerto_rico_subbarrios import (
    PuertoRicoSubbarriosConfig, puerto_rico_subbarrios_census,
    puerto_rico_subbarrios_source_complete,
)
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.puerto_rico_subbarrios import (
    DATASET_SLUG, FILE_SLUG, candidate_version,
)

work = Path(tempfile.mkdtemp(prefix="pr282-e2e-"))
staging = StagingStorageResource(use_local=True, local_dir=str(work / "staging"))
published = PublishedStorageResource(use_local=True, local_dir=str(work / "published"))
version = candidate_version()
partition = build_publish_partition_key(DATASET_SLUG, FILE_SLUG, version)


@asset(key=publish_catalog.key, partitions_def=publish_catalog.partitions_def,
       deps=[puerto_rico_subbarrios_census.key])
def catalog(context, staging_storage: StagingStorageResource):
    return publish_catalog.op.compute_fn.decorated_fn(context, staging_storage)


for fmt_asset in (publish_geopackage, publish_geoparquet, publish_pmtiles, publish_shapefile_zip):
    fmt_asset  # registered via Definitions below with original keys

defs = Definitions(
    assets=[puerto_rico_subbarrios_census, catalog, publish_geopackage, publish_geoparquet,
            publish_pmtiles, publish_shapefile_zip, verify_release, publish_version],
    asset_checks=[puerto_rico_subbarrios_source_complete],
    resources={"staging_storage": staging, "published_storage": published},
    jobs=[define_asset_job("pr282_ingest", selection=AssetSelection.assets(puerto_rico_subbarrios_census)),
          define_asset_job("pr282_downstream", selection=AssetSelection.assets(
              publish_catalog, publish_geopackage, publish_geoparquet, publish_pmtiles,
              publish_shapefile_zip, verify_release))],
)

report = {"work": str(work), "version": version, "partition": partition}
with DagsterInstance.get() as instance:
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition])
    ingest = defs.resolve_job_def("pr282_ingest").execute_in_process(
        instance=instance,         raise_on_error=False)
    ingest_ok = ingest.success
    result = [e for e in ingest.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"]
    report["ingest_success"] = ingest_ok
    report["check_results"] = [
        {"check": e.event_specific_data.check_name, "passed": e.event_specific_data.passed}
        for e in result]
    keys = sorted(staging.list_prefix(f"{DATASET_SLUG}/{FILE_SLUG}/{version}"))
    report["staged_keys"] = keys
    archive_key = f"{DATASET_SLUG}/{FILE_SLUG}/{version}/shapefile/tl_2023_72_subbarrio.zip"
    report["staged_archive_sha256"] = staging.sha256_key(archive_key)
    downstream = defs.resolve_job_def("pr282_downstream").execute_in_process(
        instance=instance, partition_key=partition, raise_on_error=False)
    report["downstream_success"] = downstream.success
    verify_events = [e for e in downstream.all_events if e.event_type_value == "RUN_FAILURE"
                     or e.event_type_value == "STEP_FAILURE"]
    failures = []
    for e in downstream.all_events:
        if e.event_type_value == "STEP_FAILURE":
            data = e.event_specific_data
            err = getattr(data, "error", None)
            inner = getattr(err, "original_excpt", None) or getattr(err, "cause", None)
            text = str(inner or err or e.message or "")
            if inner is not None:
                text = type(inner).__name__ + ": " + str(inner)
            failures.append({"step": e.step_key, "error": text[:400]})
    report["step_failures"] = failures
    verify_refused = any("ReleaseApprovalError" in (f.get("error") or "") or "release_approval" in (f.get("error") or "")
                         for f in failures)
    report["release_refused_by_verifier"] = verify_refused
    report["published_prefix_empty"] = not published.list_prefix(DATASET_SLUG)
    report["staged_object_checksums"] = {
        key: staging.sha256_key(key)
        for key in sorted(staging.list_prefix(f"{DATASET_SLUG}/{FILE_SLUG}/{version}"))
    }
    report["downstream_events"] = {
        "catalog_materialized": any(e.is_step_materialization and e.step_key == "publish__catalog" for e in downstream.all_events),
        "geoparquet_materialized": any(e.is_step_materialization and e.step_key == "publish__formats__geoparquet" for e in downstream.all_events),
        "verify_failed_as_expected": any(e.is_step_failure and e.step_key == "publish__verify" for e in downstream.all_events),
        "promotion_not_attempted": not any(e.is_step_failure and e.step_key == "publish__promote" for e in downstream.all_events),
    }
    report["source_release_accepted"] = False
    report["production_publication_performed"] = False

out = Path(sys.argv[1]) if len(sys.argv) > 1 else work / "local-materialization.json"
out.write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report, indent=2))

"""Isolated local end-to-end materialization for the puerto-rico-subbarrios candidate.

Runs the actual registered ingest asset and check, then catalog, formats, the
release verifier and promotion against isolated local staging/published roots
and a fresh Dagster instance. The published inventory is seeded with historical
and unrelated objects before the run; a release refusal (empty approval
registry) must leave that entire inventory byte-for-byte unchanged and must
prevent promotion. Any other outcome fails this verification script.
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
    DATASET_SLUG, FILE_SLUG, candidate_version, processing_code_identity,
)

HISTORICAL_KEY = f"{DATASET_SLUG}/{FILE_SLUG}/v20200101/shapefile/historical.txt"
UNRELATED_KEY = "other-dataset/other-file/v1/geoparquet/data.parquet"
HISTORICAL_BYTES = b"historical approved inventory sentinel\n"
UNRELATED_BYTES = b"unrelated published dataset sentinel\n"


def _seed_published(published: PublishedStorageResource) -> None:
    """Populate the published inventory before the candidate run."""
    root = Path(published.local_dir).resolve()
    for key, body in ((HISTORICAL_KEY, HISTORICAL_BYTES), (UNRELATED_KEY, UNRELATED_BYTES)):
        path = root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)


def _inventory(published: PublishedStorageResource) -> dict[str, str]:
    return {key: published.sha256_key(key) for key in published.list_prefix("")}


def main() -> int:
    os.environ.pop("HIFLD_RELEASE_APPROVALS", None)
    work = Path(tempfile.mkdtemp(prefix="pr282-e2e-"))
    home = work / "dagster"
    home.mkdir()
    (home / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
    os.environ["DAGSTER_HOME"] = str(home)
    staging = StagingStorageResource(use_local=True, local_dir=str(work / "staging"))
    published = PublishedStorageResource(use_local=True, local_dir=str(work / "published"))
    _seed_published(published)
    seeded_inventory = _inventory(published)
    version = candidate_version()
    partition = build_publish_partition_key(DATASET_SLUG, FILE_SLUG, version)

    @asset(key=publish_catalog.key, partitions_def=publish_catalog.partitions_def,
           deps=[puerto_rico_subbarrios_census.key])
    def catalog(context, staging_storage: StagingStorageResource):
        return publish_catalog.op.compute_fn.decorated_fn(context, staging_storage)

    defs = Definitions(
        assets=[puerto_rico_subbarrios_census, catalog, publish_geopackage, publish_geoparquet,
                publish_pmtiles, publish_shapefile_zip, verify_release, publish_version],
        asset_checks=[puerto_rico_subbarrios_source_complete],
        resources={"staging_storage": staging, "published_storage": published},
        jobs=[
            define_asset_job("pr282_ingest", selection=AssetSelection.assets(puerto_rico_subbarrios_census)),
            define_asset_job("pr282_downstream", selection=AssetSelection.assets(
                publish_catalog, publish_geopackage, publish_geoparquet, publish_pmtiles,
                publish_shapefile_zip, verify_release, publish_version)),
        ],
    )

    report = {
        "work": str(work), "version": version, "partition": partition,
        "processing_code_sha256": processing_code_identity(),
        "seeded_published_inventory": seeded_inventory,
        "production_publication_performed": False,
    }
    with DagsterInstance.get() as instance:
        instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition])
        ingest = defs.resolve_job_def("pr282_ingest").execute_in_process(
            instance=instance, raise_on_error=False)
        report["ingest_success"] = ingest.success
        report["check_results"] = [
            {"check": e.event_specific_data.check_name, "passed": e.event_specific_data.passed}
            for e in ingest.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"]
        keys = sorted(staging.list_prefix(f"{DATASET_SLUG}/{FILE_SLUG}/{version}"))
        report["staged_keys"] = keys
        archive_key = f"{DATASET_SLUG}/{FILE_SLUG}/{version}/shapefile/tl_2023_72_subbarrio.zip"
        report["staged_archive_sha256"] = staging.sha256_key(archive_key)
        report["staged_object_checksums"] = {
            key: staging.sha256_key(key)
            for key in sorted(staging.list_prefix(f"{DATASET_SLUG}/{FILE_SLUG}/{version}"))}

        downstream = defs.resolve_job_def("pr282_downstream").execute_in_process(
            instance=instance, partition_key=partition, raise_on_error=False)
        report["downstream_success"] = downstream.success
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
        report["release_refused_by_verifier"] = any(
            "ReleaseApprovalError" in (f.get("error") or "") or "release_approval" in (f.get("error") or "")
            for f in failures)
        report["verify_failed_with_release_refusal"] = any(
            f["step"] == "publish__verify"
            and ("ReleaseApprovalError" in (f.get("error") or "")
                 or "missing_independent_release_approval_registry" in (f.get("error") or ""))
            for f in failures)
        report["promotion_step_failed"] = any(f["step"] == "publish__promote" for f in failures)
        report["promotion_materialized"] = any(
            e.is_step_materialization and e.step_key == "publish__promote"
            for e in downstream.all_events)
        report["downstream_events"] = {
            "catalog_materialized": any(e.is_step_materialization and e.step_key == "publish__catalog" for e in downstream.all_events),
            "geoparquet_materialized": any(e.is_step_materialization and e.step_key == "publish__formats__geoparquet" for e in downstream.all_events),
        }

        final_inventory = _inventory(published)
        report["published_inventory_unchanged"] = final_inventory == seeded_inventory
        report["final_published_inventory"] = final_inventory

    checks = {
        "ingest_success": report["ingest_success"] is True,
        "source_check_passed": all(item["passed"] for item in report["check_results"]) and bool(report["check_results"]),
        "staged_archive_pinned": report["staged_archive_sha256"] == "5358e982d49bc6a5c099e12d32220e756e05aff848f24ba89d862dd89d7b8d8a",
        "verify_failed_with_release_refusal": report["verify_failed_with_release_refusal"] is True,
        "promotion_prevented": report["promotion_materialized"] is False,
        "published_inventory_unchanged": report["published_inventory_unchanged"] is True,
    }
    report["verification_checks"] = checks
    report["verification_passed"] = all(checks.values())
    report["release_accepted"] = False
    report["schema_contract_accepted"] = False
    report["id_contract_accepted"] = False

    out = Path(sys.argv[1]) if len(sys.argv) > 1 else work / "local-materialization.json"
    out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["verification_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())

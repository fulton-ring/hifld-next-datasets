"""Ingest USACE IENC Pipeline Areas with source drift checks."""

from __future__ import annotations

import json

from dagster import AssetKey, MetadataValue, asset

from dagster_hifld.download import build_version_id
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.usace_pipeline_areas import extract_pipeline_areas

_DATASET_SLUG = "pipeline-areas-usace-ienc"
_FILE_SLUG = "pipeline-areas-usace-ienc"


@asset(
    key=AssetKey(["ingest", _DATASET_SLUG, _FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "USACE Inland Electronic Navigational Chart Pipeline Areas. Captures the "
        "complete reviewed 112-feature PIPE_AREA layer from the USACE AGC IENC "
        "master service, retaining native OBJECTID, attributes, and polygon "
        "geometry. HIFLD snapshot equivalence and source survey currency remain "
        "unverified; source drift is gated."
    ),
)
def usace_pipeline_areas_usace_ienc(context, staging_storage: StagingStorageResource) -> dict:
    geojson_bytes, receipt = extract_pipeline_areas()
    version = build_version_id(context)
    prefix = staging_storage.build_target_location(
        _DATASET_SLUG, _FILE_SLUG, version, ""
    ).rstrip("/")
    raw_key = f"{prefix}/raw/source.geojson"
    geojson_key = f"{prefix}/geojson/source.geojson"
    receipt_key = f"{prefix}/metadata/source_receipt.json"
    staging_storage.write_key(raw_key, geojson_bytes)
    staging_storage.write_key(geojson_key, geojson_bytes)
    receipt["raw_object"] = raw_key
    receipt["geojson_object"] = geojson_key
    staging_storage.write_key(
        receipt_key,
        json.dumps(receipt, indent=2, sort_keys=True).encode("utf-8"),
    )
    context.add_output_metadata(
        {
            "source_item_id": MetadataValue.text(str(receipt["source_item_id"])),
            "feature_count": MetadataValue.int(int(receipt["feature_count"])),
            "objectids_sha256": MetadataValue.text(str(receipt["objectids_sha256"])),
            "geojson_sha256": MetadataValue.text(str(receipt["geojson_sha256"])),
            "raw_object": MetadataValue.text(raw_key),
            "receipt_object": MetadataValue.text(receipt_key),
        }
    )
    return {
        "dataset_slug": _DATASET_SLUG,
        "file_slug": _FILE_SLUG,
        "version": version,
        "feature_count": receipt["feature_count"],
        "geojson_sha256": receipt["geojson_sha256"],
        "geojson_object": geojson_key,
        "raw_object": raw_key,
        "receipt_object": receipt_key,
    }

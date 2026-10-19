"""NOAA dataset assets."""

import hashlib
import json
from pathlib import Path

from dagster import AssetKey, Field, asset

from dagster_hifld.download import build_version_id, download_convert_and_stage
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.noaa_extract import fetch_noaa_layer, stage_noaa_candidate

_NOAA_MAPSERVER_BASE = (
    "https://maritimeboundaries.noaa.gov/arcgis/rest/services/"
    "MaritimeBoundaries/US_Maritime_Limits_Boundaries/MapServer"
)
_TERRITORIAL_SEA_URL = (
    f"{_NOAA_MAPSERVER_BASE}/1/query?where=1%3D1&outFields=*&f=geojson"
)
_TERRITORIAL_SEA_NAME = "12nm-territorial-sea.geojson"
_TERRITORIAL_SEA_DATASET_SLUG = "12nm-territorial-sea"
_TERRITORIAL_SEA_FILE_SLUG = "12nm-territorial-sea"
_EEZ_URL = f"{_NOAA_MAPSERVER_BASE}/3/query?where=1%3D1&outFields=*&f=geojson"
_EEZ_NAME = "200nm-eez-and-maritime-boundaries.geojson"
_EEZ_DATASET_SLUG = "200nm-eez-and-maritime-boundaries"
_EEZ_FILE_SLUG = "200nm-eez-and-maritime-boundaries"
_CONTIGUOUS_ZONE_URL = (
    f"{_NOAA_MAPSERVER_BASE}/2/query?where=1%3D1&outFields=*&f=geojson"
)
_CONTIGUOUS_ZONE_NAME = "24nm-contiguous-zone.geojson"
_CONTIGUOUS_ZONE_DATASET_SLUG = "24nm-contiguous-zone"
_CONTIGUOUS_ZONE_FILE_SLUG = "24nm-contiguous-zone"


@asset(
    key=AssetKey(["ingest", _TERRITORIAL_SEA_DATASET_SLUG, _TERRITORIAL_SEA_FILE_SLUG]),
    group_name="noaa",
    compute_kind="http",
    description="12NM Territorial Sea (HIFLD inventory). Publisher: National Oceanic and Atmospheric Administration. NOAA depicts on nautical charts the limits of the 12 NM Territorial Sea, 24 NM Contiguous Zone, and 200 NM EEZ from the U.S. normal baseline. Source: maritimeboundaries.noaa.gov.",
)
def noaa_12nm_territorial_sea(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [(_TERRITORIAL_SEA_URL, _TERRITORIAL_SEA_NAME)],
        _TERRITORIAL_SEA_DATASET_SLUG,
        _TERRITORIAL_SEA_FILE_SLUG,
        version,
        staging_storage,
    )


# Proposed source snapshot pins are review boundaries, not accepted contracts.
_EEZ_EXPECTED_FIELDS = {'OBJECTID': 'esriFieldTypeOID', 'BOUND_ID': 'esriFieldTypeString', 'REGION': 'esriFieldTypeString', 'FEAT_TYPE': 'esriFieldTypeString', 'PUB_DATE': 'esriFieldTypeDate', 'APPRV_DATE': 'esriFieldTypeDate', 'LEGAL_AUTH': 'esriFieldTypeString', 'AOR': 'esriFieldTypeString', 'NOTE': 'esriFieldTypeString', 'SUPP_INFO': 'esriFieldTypeString', 'UNILATERAL': 'esriFieldTypeDouble', 'RULEID': 'esriFieldTypeInteger', 'SHAPE': 'esriFieldTypeGeometry', 'SHAPE.LEN': 'esriFieldTypeDouble'}
_EEZ_EXPECTED_FINGERPRINTS = {'schema': 'd4f1d796ea2d69d492403cb45c7adf19824491b58ba8a56dba5f4b36f6dd8fb0', 'identifiers': '81dd15337b23b553cf4fbc8329bb0a48f3acb7b6ecb7fa145c59f07be9350fc2', 'geometry': '9c9d57961dd37aa9fc1bd164d4bddb698b299f4381c16b74a6f79703dee3c79d', 'coverage': '1909b57827f10c92e0ce0b9aaf5ab0037eed0f1cc2781faf42dd38d324176081'}


@asset(
    key=AssetKey(["ingest", _EEZ_DATASET_SLUG, _EEZ_FILE_SLUG]),
    group_name="noaa",
    compute_kind="http",
    config_schema={
        "allow_unapproved_local_draft": Field(bool, default_value=False),
        "capture_directory": Field(str, default_value=""),
    },
    description="Draft NOAA EEZ adapter: bounded raw extraction, native typed candidate and local catalog source; real source/contract acceptance and production promotion remain blocked.",
)
def noaa_200nm_eez_and_maritime_boundaries(context, staging_storage: StagingStorageResource) -> dict:
    if not context.op_config["allow_unapproved_local_draft"]:
        raise ValueError("EEZ draft adapter requires explicit allow_unapproved_local_draft; this is not acceptance")
    directory = context.op_config["capture_directory"]
    if directory:
        root = Path(directory)
        receipts = json.loads((root / "receipts.json").read_text())
        captures = {}
        for receipt in receipts:
            name = receipt["capture"]
            if name not in {"layer", "count_before", "features", "ids_before", "ids_after", "count_after", "geojson"}:
                raise ValueError("Unknown NOAA receipt capture")
            raw = (root / (name + ".json")).read_bytes()
            if receipt["sha256"] != hashlib.sha256(raw).hexdigest() or receipt["bytes"] != len(raw) or receipt["http_status"] != 200:
                raise ValueError("NOAA replay receipt checksum/length/status mismatch")
            if name in captures:
                raise ValueError("Duplicate NOAA replay receipt")
            captures[name] = raw
    else:
        captures, receipts = fetch_noaa_layer(_NOAA_MAPSERVER_BASE + "/3")
    result = stage_noaa_candidate(
        staging=staging_storage,
        dataset_slug=_EEZ_DATASET_SLUG,
        file_slug=_EEZ_FILE_SLUG,
        captures=captures,
        receipts=receipts,
        expected_fields=_EEZ_EXPECTED_FIELDS,
        expected_fingerprints=_EEZ_EXPECTED_FINGERPRINTS,
        expected_count=96,
        code_commit=context.run_tags.get("hifld/source_code_commit"),
    )
    context.add_output_metadata({"version": result["version"], "feature_count": result["feature_count"], "candidate_sha256": result["candidate_sha256"], "promotion_approved": False})
    return result


@asset(
    key=AssetKey(["ingest", _CONTIGUOUS_ZONE_DATASET_SLUG, _CONTIGUOUS_ZONE_FILE_SLUG]),
    group_name="noaa",
    compute_kind="http",
    description="24NM Contiguous Zone (HIFLD inventory). Publisher: NOAA. Maritime limits from the U.S. normal baseline; part of NOAA OCS U.S. Maritime Limits & Boundaries. Source: maritimeboundaries.noaa.gov.",
)
def noaa_24nm_contiguous_zone(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [(_CONTIGUOUS_ZONE_URL, _CONTIGUOUS_ZONE_NAME)],
        _CONTIGUOUS_ZONE_DATASET_SLUG,
        _CONTIGUOUS_ZONE_FILE_SLUG,
        version,
        staging_storage,
    )

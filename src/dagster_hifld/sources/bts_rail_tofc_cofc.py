"""Draft native capture for the BTS NTAD rail TOFC/COFC facilities layer.

The frozen per-layer pins live in ``bts_intermodal_family.RAIL_TOFC_COFC_SPEC``;
capture, ordered page planning, source validation and unapproved candidate
staging are shared in that module. The module-level pin constants stay the
live surface the asset check and offline tests patch.
"""
from __future__ import annotations

from dataclasses import replace

import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.bts_intermodal_family import (
    RAIL_TOFC_COFC_SPEC, capture_bts_intermodal,
)

SPEC = RAIL_TOFC_COFC_SPEC
ITEM_ID = SPEC.item_id
SERVICE_URL = SPEC.service_url
LAYER_ID = SPEC.layer_id
LAYER_URL = SPEC.layer_url
LAYER_NAME = SPEC.layer_name
DATASET_SLUG = SPEC.dataset_slug
FILE_SLUG = SPEC.file_slug
PUBLISHER = SPEC.publisher
PAGE_SIZE = SPEC.page_size
MAX_BODY_BYTES = SPEC.max_body_bytes
MAX_CAPTURE_TOTAL_BYTES = SPEC.max_capture_total_bytes
MAX_OBJECT_ID_URL_LENGTH = SPEC.max_object_id_url_length
EXPECTED_SOURCE_SCHEMA_SHA256 = SPEC.expected_source_schema_sha256
EXPECTED_SOURCE_ID_SHA256 = SPEC.expected_source_id_sha256
EXPECTED_SOURCE_COUNT = SPEC.expected_source_count
EXPECTED_FIELD_NAMES = list(SPEC.expected_field_names)
EXPECTED_FIELD_TYPES = list(SPEC.expected_field_types)


def capture_bts_rail_tofc_cofc(storage: StagingStorageResource, *,
                               capture_directory: str | None = None) -> dict:
    """Acquire, validate and stage a complete unapproved BTS candidate."""
    return capture_bts_intermodal(
        replace(SPEC, expected_source_count=EXPECTED_SOURCE_COUNT,
                expected_source_id_sha256=EXPECTED_SOURCE_ID_SHA256,
                expected_source_schema_sha256=EXPECTED_SOURCE_SCHEMA_SHA256),
        storage,
        capture_directory=capture_directory,
    )

"""Gated real-GCS end-to-end publishing smoke tests."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TypedDict

from pydantic import JsonValue, TypeAdapter

from dagster_hifld.assets.publish import (
    PublishedFormatOutput,
    run_local_version_pipeline,
)
from dagster_hifld.resources import (
    PublishedStorageResource,
    StagingStorageResource,
)


@dataclass(frozen=True)
class RealGcsE2ECase:
    dataset_slug: str
    file_slug: str
    version: str = "v1.0.0"


REAL_GCS_E2E_CASES = (
    RealGcsE2ECase("12nm-territorial-sea", "12nm-territorial-sea"),
    RealGcsE2ECase("amtrak-stations", "amtrak-stations"),
    RealGcsE2ECase("nfhl", "alluvial-fans"),
    RealGcsE2ECase("2020-census-blocks-1", "tl_2024_01_tabblock20"),
)


class RealGcsE2EResult(TypedDict):
    outputs: list[PublishedFormatOutput]
    quality_manifest: dict[str, JsonValue]
    data_dictionary: dict[str, JsonValue]
    published_keys: list[str]


def run_real_gcs_e2e_case(case: RealGcsE2ECase) -> RealGcsE2EResult:
    if os.environ.get("HIFLD_RUN_GCS_E2E") != "1":
        raise RuntimeError("Set HIFLD_RUN_GCS_E2E=1 to run real GCS E2E cases.")
    if os.environ.get("HIFLD_E2E_CONFIRM_PROD_WRITE") != "1":
        raise RuntimeError(
            "Set HIFLD_E2E_CONFIRM_PROD_WRITE=1 to confirm writes to the published bucket."
        )

    staging = StagingStorageResource(
        bucket=os.environ.get("HIFLD_STAGING_BUCKET", "hifld-next-staging-prod"),
        use_local=False,
        local_dir="",
    )
    published = PublishedStorageResource(
        bucket=os.environ.get("HIFLD_DATASETS_BUCKET", "hifld-next-datasets-prod"),
        use_local=False,
        local_dir="",
    )

    staged_keys = staging.list_keys(case.dataset_slug, case.file_slug, case.version)
    if not staged_keys:
        raise ValueError(
            f"No staged source keys found for {case.dataset_slug}/{case.file_slug}/{case.version}."
        )

    result = run_local_version_pipeline(
        staging_storage=staging,
        published_storage=published,
        dataset_slug=case.dataset_slug,
        file_slug=case.file_slug,
        version=case.version,
    )

    json_object = TypeAdapter(dict[str, JsonValue])
    quality_manifest = json_object.validate_json(
        staging.read_bytes(
            case.dataset_slug,
            case.file_slug,
            case.version,
            "metadata/quality_manifest.json",
        )
    )
    data_dictionary = json_object.validate_json(
        staging.read_bytes(
            case.dataset_slug,
            case.file_slug,
            case.version,
            "metadata/data_dictionary.json",
        )
    )
    published_keys = published.list_keys(
        case.dataset_slug, case.file_slug, case.version
    )
    if not published_keys:
        raise ValueError(
            f"No published keys found for {case.dataset_slug}/{case.file_slug}/{case.version}."
        )
    return {
        "outputs": result["outputs"],
        "quality_manifest": quality_manifest,
        "data_dictionary": data_dictionary,
        "published_keys": published_keys,
    }

"""Terminal catalog-publication Dagster asset."""
from __future__ import annotations

import os
from pathlib import Path

from dagster import AssetKey, Field, Output, asset

from dagster_hifld.partitions import PUBLISH_PARTITIONS, parse_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.portolan.release import read_staged_release
from dagster_hifld.release_approval import require_generated_object_approval


@asset(
    key=AssetKey(["publish", "release_metadata"]),
    partitions_def=PUBLISH_PARTITIONS,
    group_name="publish",
    deps=[AssetKey(["publish", "formats", name]) for name in ("geopackage", "geoparquet", "pmtiles", "shapefile_zip")],
    config_schema={"version": Field(str, description="Explicit semantic release label, e.g. v1.5.0. Never the candidate partition ID.")},
    description="Prepare release-ready STAC in staging; independent approval is still required.",
)
def prepare_release_metadata(context, staging_storage: StagingStorageResource,
                             published_storage: PublishedStorageResource) -> Output[dict]:
    from dagster_hifld.portolan.workflow import PortolanPublishRequest, prepare_staged_release, DEFAULT_PUBLIC_ROOT
    dataset, file, candidate = parse_publish_partition_key(context.partition_key)
    request = PortolanPublishRequest(
        os.environ.get("HIFLD_PORTOLAN_COLLECTION", "hifld"), dataset, file,
        context.op_config["version"], "", "", "",
        public_root=os.environ.get("HIFLD_PORTOLAN_PUBLIC_ROOT") or
                    (f"https://storage.googleapis.com/{published_storage.bucket}" if published_storage.bucket and published_storage.backend == "gcs" else DEFAULT_PUBLIC_ROOT),
        storage_slug=os.environ.get("HIFLD_PORTOLAN_STORAGE_SLUG") or default_storage_slug(published_storage.backend, published_storage.bucket),
    )
    document = prepare_staged_release(request, staging_storage, published_storage, staged_version=candidate)
    return Output({"version": document["version"], "collection": f"{dataset}/{file}/{candidate}/collection.json"})


@asset(
    key=AssetKey(["publish", "portolan_catalog"]),
    partitions_def=PUBLISH_PARTITIONS,
    group_name="publish",
    deps=[AssetKey(["publish", "promote"])],
    description="Explicitly prepare or apply a persistent reviewed catalog bundle for the selected promoted partition.",
    config_schema={"mode": Field(str, default_value="unconfigured"),
                   "plan_dir": Field(str, default_value="")},
)
def publish_portolan_catalog(
    context,
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
) -> Output[dict[str, str]]:
    if os.environ.get("HIFLD_PORTOLAN_ENABLED") != "1":
        raise RuntimeError("Portolan publication is disabled for this Dagster deployment.")
    from dagster_hifld.portolan.workflow import (
        DEFAULT_PUBLIC_ROOT,
        PortolanPublishRequest,
        apply_selected_portolan_catalog,
        prepare_promoted_portolan_catalog,
    )

    dataset_slug, file_slug, version = parse_publish_partition_key(context.partition_key)
    prefix = f"{dataset_slug}/{file_slug}/{version}"
    document, destination_prefix = read_staged_release(staging_storage, prefix)
    if document["id"].split("/")[0] != os.environ.get("HIFLD_PORTOLAN_COLLECTION", "hifld"):
        raise ValueError("Staged STAC collection differs from configured catalog")
    contents = staging_storage.read_key(f"{prefix}/collection.json")
    if published_storage.read_key(f"{destination_prefix}/collection.json") != contents:
        raise ValueError("Staged release Collection changed after promotion")
    require_generated_object_approval(published_storage, f"{destination_prefix}/collection.json", contents)
    public_root = os.environ.get("HIFLD_PORTOLAN_PUBLIC_ROOT")
    if not public_root and published_storage.bucket and published_storage.backend == "gcs":
        public_root = f"https://storage.googleapis.com/{published_storage.bucket}"
    if not public_root:
        public_root = DEFAULT_PUBLIC_ROOT
    storage_slug = os.environ.get("HIFLD_PORTOLAN_STORAGE_SLUG") or default_storage_slug(
        published_storage.backend, published_storage.bucket
    )
    request = PortolanPublishRequest(
        collection_slug=os.environ.get("HIFLD_PORTOLAN_COLLECTION", "hifld"),
        dataset_slug=dataset_slug,
        file_slug=file_slug,
        version=destination_prefix.rsplit("/", 1)[1],
        title="",
        description="",
        provider="",
        public_root=public_root.rstrip("/"),
        storage_slug=storage_slug,
        collection_title=os.environ.get("HIFLD_PORTOLAN_COLLECTION_TITLE"),
        archive_public_domain=(
            os.environ.get("HIFLD_PORTOLAN_ARCHIVE_PUBLIC_DOMAIN", "0") == "1"
        ),
    )
    mode = context.op_config.get("mode", "unconfigured")
    plan_value = context.op_config.get("plan_dir", "")
    if mode not in {"prepare", "apply"} or not isinstance(plan_value, str) or not plan_value.strip():
        raise ValueError("Configure mode=prepare or mode=apply and an external persistent plan_dir; implicit catalog publication is disabled")
    if os.environ.get("HIFLD_PORTOLAN_RELEASE_POINTER", "1") == "0":
        raise ValueError("Legacy in-place catalog publication is disabled; a reviewed immutable release plan is required")
    directory = Path(plan_value)
    if mode == "prepare":
        plan = prepare_promoted_portolan_catalog(request, staging_storage, published_storage, directory)
        generation = plan["generation"]
    else:
        generation = apply_selected_portolan_catalog(request, staging_storage, published_storage, directory)
    return Output({"catalog_generation": generation, "mode": mode, "plan_dir": str(directory)},
                  metadata={"catalog_generation": generation, "catalog_mode": mode, "plan_dir": str(directory)})


def default_storage_slug(backend: str, bucket: str | None) -> str:
    """Storage slug used when HIFLD_PORTOLAN_STORAGE_SLUG is unset.

    Must match the slug the readers' storage registries use for the bucket
    (gcp-portolan-published in production), or published assets will be
    recorded under a name no reader resolves.
    """
    if bucket and backend == "gcs":
        return f"gcp-{bucket.removeprefix('hifld-next-')}"
    if bucket and backend == "s3":
        return f"s3-{bucket}"
    return "seaweedfs-local-published"

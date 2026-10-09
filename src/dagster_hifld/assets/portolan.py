"""Terminal catalog-publication Dagster asset."""
from __future__ import annotations

import os

from dagster import AssetKey, Output, asset

from dagster_hifld.partitions import PUBLISH_PARTITIONS, parse_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


@asset(
    key=AssetKey(["publish", "portolan_catalog"]),
    partitions_def=PUBLISH_PARTITIONS,
    group_name="publish",
    deps=[AssetKey(["publish", "promote"])],
    description="Publishes the selected promoted version to STAC and SQLite last.",
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
        publish_portolan_record,
    )

    dataset_slug, file_slug, version = parse_publish_partition_key(context.partition_key)
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
        version=version,
        title="",
        description="",
        provider="",
        public_root=public_root.rstrip("/"),
        storage_slug=storage_slug,
        collection_title=os.environ.get("HIFLD_PORTOLAN_COLLECTION_TITLE"),
        archive_public_domain=(
            os.environ.get("HIFLD_PORTOLAN_ARCHIVE_PUBLIC_DOMAIN", "1") != "0"
        ),
    )
    generation = publish_portolan_record(
        request,
        staging=staging_storage,
        published=published_storage,
        catalog_only=True,
        use_release_pointer=(
            os.environ.get("HIFLD_PORTOLAN_RELEASE_POINTER", "1") != "0"
        ),
    )
    return Output({"catalog_generation": generation}, metadata={"catalog_generation": generation})


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

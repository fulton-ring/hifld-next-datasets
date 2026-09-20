"""Metadata-only CLI for building a Portolan tree from production inventories."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path

from dagster import DagsterInstance, Field, Out, job, op

from .catalog import build_catalog_sqlite, render_portolan_tree
from .inventory import (
    GenerationPinnedMetadataLoader,
    build_catalog_from_inventories,
    inventory_from_copy_report,
)


@dataclass(frozen=True)
class InventoryCatalogBuildConfig:
    source_inventory: Path
    published_copy_report: Path
    parquet_footer: Path
    source_bucket: str
    published_root: str
    output_dir: Path
    metadata_cache: Path
    production_collections: Path
    production_datasets: Path | None = None


def build_inventory_catalog_artifact(
    config: InventoryCatalogBuildConfig,
) -> dict[str, int]:
    """Build local STAC and SQLite from metadata inventories only."""
    collections = json.loads(config.production_collections.read_text(encoding="utf-8"))
    if not isinstance(collections, list):
        raise TypeError("Production collections export must be an array.")
    hifld = next(
        (
            item
            for item in collections
            if isinstance(item, dict) and item.get("slug") == "hifld"
        ),
        None,
    )
    if hifld is None:
        raise ValueError("Production collections export lacks hifld.")
    collection_description = hifld.get("description")
    if not isinstance(collection_description, str):
        raise TypeError("Production collection description must be a string.")
    collection_created_at = _timestamp(hifld, "created_at")
    collection_updated_at = _timestamp(hifld, "updated_at")
    dataset_timestamps = (
        _production_dataset_timestamps(config.production_datasets, hifld.get("id"))
        if config.production_datasets is not None
        else {}
    )
    target_inventory = (
        config.output_dir.parent / f"{config.output_dir.name}.target-inventory.json"
    )
    target_inventory.parent.mkdir(parents=True, exist_ok=True)
    target_inventory.write_text(
        json.dumps(
            [
                {
                    "name": item.name,
                    "generation": item.generation,
                    "size": item.size,
                    "md5Hash": item.md5_hash,
                    "contentType": item.content_type,
                    "updated": item.updated,
                }
                for item in inventory_from_copy_report(config.published_copy_report)
            ]
        ),
        encoding="utf-8",
    )
    loader = GenerationPinnedMetadataLoader(config.source_bucket, config.metadata_cache)
    try:
        catalog = build_catalog_from_inventories(
            config.source_inventory,
            target_inventory,
            config.parquet_footer,
            source_metadata=loader,
            published_root=config.published_root,
            collection_description=collection_description,
            collection_created_at=collection_created_at,
            collection_updated_at=collection_updated_at,
            dataset_timestamps=dataset_timestamps,
        )
    finally:
        loader.close()
    render_portolan_tree(
        config.output_dir, catalog.records, public_root=config.published_root
    )
    build_catalog_sqlite(
        config.output_dir / "_catalog" / "catalog.sqlite",
        catalog.records,
        root_href=f"{config.published_root.rstrip('/')}/catalog.json",
    )
    report = {
        "version_count": catalog.report.version_count,
        "asset_count": catalog.report.asset_count,
        "nonspatial_version_count": catalog.report.nonspatial_version_count,
        "multipart_geoparquet_version_count": catalog.report.multipart_geoparquet_version_count,
    }
    (
        config.output_dir.parent
        / f"{config.output_dir.name}.inventory-build-report.json"
    ).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


_INVENTORY_JOB_CONFIG = {
    "source_inventory": Field(str),
    "published_copy_report": Field(str),
    "parquet_footer": Field(str),
    "source_bucket": Field(str),
    "published_root": Field(str),
    "output_dir": Field(str),
    "metadata_cache": Field(str),
    "production_collections": Field(str),
    "production_datasets": Field(str, is_required=False),
}


@op(config_schema=_INVENTORY_JOB_CONFIG, out=Out(dict[str, int]))
def build_inventory_catalog(context) -> dict[str, int]:
    values = context.op_config
    return build_inventory_catalog_artifact(
        InventoryCatalogBuildConfig(
            source_inventory=Path(values["source_inventory"]),
            published_copy_report=Path(values["published_copy_report"]),
            parquet_footer=Path(values["parquet_footer"]),
            source_bucket=values["source_bucket"],
            published_root=values["published_root"],
            output_dir=Path(values["output_dir"]),
            metadata_cache=Path(values["metadata_cache"]),
            production_collections=Path(values["production_collections"]),
            production_datasets=(
                Path(values["production_datasets"])
                if "production_datasets" in values
                else None
            ),
        )
    )


@job(
    description="Build a Portolan catalog from inventories without reading data assets."
)
def inventory_catalog_job() -> None:
    build_inventory_catalog()


def execute_inventory_catalog(config: InventoryCatalogBuildConfig, dagster_home: Path):
    """Run the metadata-only inventory build with persistent local Dagster state."""
    dagster_home.mkdir(parents=True, exist_ok=True)
    dagster_yaml = dagster_home / "dagster.yaml"
    if not dagster_yaml.exists():
        dagster_yaml.write_text("telemetry:\n  enabled: false\n", encoding="utf-8")
    os.environ["DAGSTER_HOME"] = str(dagster_home.resolve())
    instance = DagsterInstance.get()
    return inventory_catalog_job.execute_in_process(
        instance=instance,
        run_config={
            "ops": {
                "build_inventory_catalog": {
                    "config": {
                        "source_inventory": str(config.source_inventory.resolve()),
                        "published_copy_report": str(
                            config.published_copy_report.resolve()
                        ),
                        "parquet_footer": str(config.parquet_footer.resolve()),
                        "source_bucket": config.source_bucket,
                        "published_root": config.published_root,
                        "output_dir": str(config.output_dir.resolve()),
                        "metadata_cache": str(config.metadata_cache.resolve()),
                        "production_collections": str(
                            config.production_collections.resolve()
                        ),
                        **(
                            {"production_datasets": str(config.production_datasets.resolve())}
                            if config.production_datasets is not None
                            else {}
                        ),
                    }
                }
            }
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-inventory", type=Path, required=True)
    parser.add_argument("--published-copy-report", type=Path, required=True)
    parser.add_argument("--parquet-footer", type=Path, required=True)
    parser.add_argument("--source-bucket", required=True)
    parser.add_argument("--published-root", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--metadata-cache", type=Path, required=True)
    parser.add_argument("--production-collections", type=Path, required=True)
    parser.add_argument("--production-datasets", type=Path)
    arguments = parser.parse_args()
    build_inventory_catalog_artifact(
        InventoryCatalogBuildConfig(
            source_inventory=arguments.source_inventory,
            published_copy_report=arguments.published_copy_report,
            parquet_footer=arguments.parquet_footer,
            source_bucket=arguments.source_bucket,
            published_root=arguments.published_root,
            output_dir=arguments.output_dir,
            metadata_cache=arguments.metadata_cache,
            production_collections=arguments.production_collections,
            production_datasets=arguments.production_datasets,
        )
    )


def _timestamp(value: dict[str, object], key: str) -> str | None:
    candidate = value.get(key)
    return candidate if isinstance(candidate, str) and candidate else None


def _production_dataset_timestamps(
    path: Path, collection_id: object
) -> dict[str, tuple[str | None, str | None]]:
    if not isinstance(collection_id, int):
        raise TypeError("Production collection id must be an integer.")
    datasets = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(datasets, list):
        raise TypeError("Production datasets export must be an array.")
    timestamps: dict[str, tuple[str | None, str | None]] = {}
    for dataset in datasets:
        if not isinstance(dataset, dict) or dataset.get("collection_id") != collection_id:
            continue
        slug = dataset.get("slug")
        if not isinstance(slug, str) or not slug:
            raise TypeError("Production datasets require non-empty string slugs.")
        timestamps[slug] = (
            _timestamp(dataset, "created_at"),
            _timestamp(dataset, "updated_at"),
        )
    return timestamps


if __name__ == "__main__":
    main()

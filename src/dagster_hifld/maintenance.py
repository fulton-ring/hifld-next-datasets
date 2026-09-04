"""Dry-run-first inventory and staging restoration for published datasets."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Sequence

from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.source_formats import (
    CANONICAL_SOURCE_FORMAT_PRECEDENCE,
    SHAPEFILE_DATASET_SUFFIXES,
    SOURCE_FORMAT_EXTENSIONS,
    discover_legacy_unknown_shapefile_keys,
)

_IGNORED_ROOTS = frozenset({"_temporary", "_rollback"})
_REQUIRED_SHAPEFILE_SUFFIXES = frozenset({".shp", ".shx", ".dbf"})


@dataclass(frozen=True, order=True)
class VersionIdentity:
    dataset: str
    file: str
    version: str

    @property
    def prefix(self) -> str:
        return f"{self.dataset}/{self.file}/{self.version}"


@dataclass(frozen=True)
class SelectedSource:
    format_name: str
    source_keys: tuple[str, ...]
    destination_keys: tuple[str, ...]


@dataclass(frozen=True)
class VersionMaintenanceResult:
    dataset: str
    file: str
    version: str
    status: str
    selected_format: str | None = None
    source_keys: tuple[str, ...] = ()
    destination_keys: tuple[str, ...] = ()
    metadata_keys: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "file": self.file,
            "version": self.version,
            "status": self.status,
            "selected_format": self.selected_format,
            "source_keys": list(self.source_keys),
            "destination_keys": list(self.destination_keys),
            "metadata_keys": list(self.metadata_keys),
            "errors": list(self.errors),
        }


@dataclass(frozen=True)
class MaintenanceReport:
    action: str
    apply: bool
    overwrite: bool
    versions: tuple[VersionMaintenanceResult, ...]

    @property
    def has_failures(self) -> bool:
        return any(item.status in {"blocked", "failed"} for item in self.versions)

    def to_dict(self) -> dict[str, object]:
        return {
            "action": self.action,
            "apply": self.apply,
            "overwrite": self.overwrite,
            "versions": [item.to_dict() for item in self.versions],
        }


def inventory_published(
    published: PublishedStorageResource,
    *,
    dataset: str | None = None,
    file: str | None = None,
    version: str | None = None,
) -> MaintenanceReport:
    """Inventory published object names and select one coherent source per version."""
    all_keys = published.list_prefix()
    logical_by_key = {key: _logical_key(published, key) for key in all_keys}
    identities = sorted(
        {
            identity
            for logical_key in logical_by_key.values()
            if (identity := _version_identity(logical_key)) is not None
            and (dataset is None or identity.dataset == dataset)
            and (file is None or identity.file == file)
            and (version is None or identity.version == version)
        }
    )
    results: list[VersionMaintenanceResult] = []
    for identity in identities:
        version_keys = tuple(
            sorted(
                key
                for key, logical_key in logical_by_key.items()
                if logical_key.startswith(f"{identity.prefix}/")
            )
        )
        selected, error = _select_source(published, identity, version_keys)
        metadata_keys = _metadata_keys(published, identity, all_keys)
        if error is not None:
            results.append(
                VersionMaintenanceResult(
                    identity.dataset,
                    identity.file,
                    identity.version,
                    "blocked",
                    metadata_keys=metadata_keys,
                    errors=(error,),
                )
            )
            continue
        if selected is None:
            results.append(
                VersionMaintenanceResult(
                    identity.dataset,
                    identity.file,
                    identity.version,
                    "blocked",
                    metadata_keys=metadata_keys,
                    errors=(
                        "No allowed processing source exists; derived data is not eligible.",
                    ),
                )
            )
            continue
        results.append(
            VersionMaintenanceResult(
                identity.dataset,
                identity.file,
                identity.version,
                "ready",
                selected.format_name,
                selected.source_keys,
                selected.destination_keys,
                metadata_keys,
            )
        )
    return MaintenanceReport("inventory", False, False, tuple(results))


def restore_staging(
    published: PublishedStorageResource,
    staging: StagingStorageResource,
    *,
    dataset: str | None = None,
    file: str | None = None,
    version: str | None = None,
    apply: bool = False,
    overwrite: bool = False,
) -> MaintenanceReport:
    """Restore selected published sources to staging; dry-run unless ``apply``."""
    inventory = inventory_published(
        published,
        dataset=dataset,
        file=file,
        version=version,
    )
    results: list[VersionMaintenanceResult] = []
    for item in inventory.versions:
        if item.status == "blocked":
            results.append(item)
            continue
        try:
            conflict_formats = _conflicting_canonical_formats(
                published,
                staging,
                item,
            )
            if conflict_formats and not overwrite:
                joined = ", ".join(sorted(conflict_formats))
                raise ValueError(
                    f"Staging contains conflicting canonical source objects in: {joined}. "
                    "Use --overwrite-existing-sources to replace only those format paths."
                )
            if not apply:
                results.append(
                    VersionMaintenanceResult(
                        item.dataset,
                        item.file,
                        item.version,
                        "planned",
                        item.selected_format,
                        item.source_keys,
                        item.destination_keys,
                        item.metadata_keys,
                    )
                )
                continue
            _apply_restore(
                published,
                staging,
                item,
                conflict_formats=conflict_formats,
                overwrite=overwrite,
            )
            results.append(
                VersionMaintenanceResult(
                    item.dataset,
                    item.file,
                    item.version,
                    "restored",
                    item.selected_format,
                    item.source_keys,
                    item.destination_keys,
                    item.metadata_keys,
                )
            )
        except Exception as exc:
            results.append(
                VersionMaintenanceResult(
                    item.dataset,
                    item.file,
                    item.version,
                    "failed" if apply else "blocked",
                    item.selected_format,
                    item.source_keys,
                    item.destination_keys,
                    item.metadata_keys,
                    (f"{type(exc).__name__}: {exc}",),
                )
            )
    return MaintenanceReport(
        "restore-staging",
        apply,
        overwrite,
        tuple(results),
    )


def _apply_restore(
    published: PublishedStorageResource,
    staging: StagingStorageResource,
    item: VersionMaintenanceResult,
    *,
    conflict_formats: set[str],
    overwrite: bool,
) -> None:
    if overwrite:
        for format_name in sorted(conflict_formats):
            staging.delete_prefix(
                staging.build_target_location(
                    item.dataset,
                    item.file,
                    item.version,
                    format_name,
                )
            )

    _copy_missing_or_changed(
        published,
        staging,
        item.source_keys,
        item.destination_keys,
    )
    metadata_destinations = tuple(
        _logical_key(published, key) for key in item.metadata_keys
    )
    _copy_missing_or_changed(
        published,
        staging,
        item.metadata_keys,
        metadata_destinations,
    )

    version_manifest_key = (
        f"{item.dataset}/{item.file}/{item.version}/metadata/source_manifest.json"
    )
    published_has_version_manifest = any(
        _logical_key(published, key) == version_manifest_key
        for key in item.metadata_keys
    )
    if not published_has_version_manifest and not staging.object_exists(
        version_manifest_key
    ):
        resolved = load_resolved_source_manifest(
            staging,
            item.dataset,
            item.file,
            item.version,
        )
        staging.write_key(
            version_manifest_key,
            json.dumps(resolved.metadata, sort_keys=True, indent=2).encode("utf-8"),
        )

    resolved = load_resolved_source_manifest(
        staging,
        item.dataset,
        item.file,
        item.version,
    )
    summary = summarize_staged_catalog(
        staging,
        item.dataset,
        item.file,
        item.version,
        item.dataset,
        source_metadata=resolved.metadata,
    )
    write_catalog_metadata(
        staging,
        item.dataset,
        item.file,
        item.version,
        summary.quality_manifest,
        summary.data_dictionary,
    )


def _copy_missing_or_changed(
    source: PublishedStorageResource,
    destination: StagingStorageResource,
    source_keys: tuple[str, ...],
    destination_keys: tuple[str, ...],
) -> None:
    pending = [
        (source_key, destination_key)
        for source_key, destination_key in zip(
            source_keys,
            destination_keys,
            strict=True,
        )
        if not source.object_content_matches(
            destination,
            source_key,
            destination_key,
        )
    ]
    source.copy_keys_to(
        destination,
        [source_key for source_key, _destination_key in pending],
        destination_keys=[destination_key for _source_key, destination_key in pending],
    )


def _conflicting_canonical_formats(
    published: PublishedStorageResource,
    staging: StagingStorageResource,
    item: VersionMaintenanceResult,
) -> set[str]:
    expected_by_destination = {
        staging._ensure_prefixed(destination_key): source_key
        for source_key, destination_key in zip(
            item.source_keys,
            item.destination_keys,
            strict=True,
        )
    }
    conflicts: set[str] = set()
    for existing_key in staging.list_keys(item.dataset, item.file, item.version):
        logical = _logical_key(staging, existing_key)
        relative = logical.removeprefix(f"{item.dataset}/{item.file}/{item.version}/")
        format_name = PurePosixPath(relative).parts[0]
        if format_name not in CANONICAL_SOURCE_FORMAT_PRECEDENCE:
            continue
        source_key = expected_by_destination.get(existing_key)
        if source_key is None or not published.object_content_matches(
            staging,
            source_key,
            existing_key,
        ):
            conflicts.add(format_name)
    return conflicts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inventory published processing sources or restore them to staging."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("inventory", "restore-staging"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("--dataset")
        command_parser.add_argument("--file")
        command_parser.add_argument("--version")
        if command == "restore-staging":
            command_parser.add_argument("--apply", action="store_true")
            command_parser.add_argument(
                "--overwrite-existing-sources",
                action="store_true",
                help="Replace only conflicting canonical staging source format paths.",
            )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    published = PublishedStorageResource.from_env()
    if args.command == "inventory":
        report = inventory_published(
            published,
            dataset=args.dataset,
            file=args.file,
            version=args.version,
        )
    else:
        report = restore_staging(
            published,
            StagingStorageResource.from_env(),
            dataset=args.dataset,
            file=args.file,
            version=args.version,
            apply=args.apply,
            overwrite=args.overwrite_existing_sources,
        )
    print(json.dumps(report.to_dict(), sort_keys=True, indent=2))
    return 1 if report.has_failures else 0


def _logical_key(storage: StagingStorageResource, key: str) -> str:
    prefix = storage.prefix.strip("/")
    normalized = key.lstrip("/")
    if not prefix:
        return normalized
    return normalized.removeprefix(f"{prefix}/")


def _version_identity(logical_key: str) -> VersionIdentity | None:
    parts = PurePosixPath(logical_key).parts
    if len(parts) < 5 or parts[0] in _IGNORED_ROOTS:
        return None
    return VersionIdentity(parts[0], parts[1], parts[2])


def _metadata_keys(
    storage: PublishedStorageResource,
    identity: VersionIdentity,
    keys: list[str],
) -> tuple[str, ...]:
    expected = {
        f"{identity.dataset}/metadata/source_manifest.json",
        f"{identity.dataset}/{identity.file}/metadata/source_manifest.json",
        f"{identity.prefix}/metadata/source_manifest.json",
    }
    return tuple(sorted(key for key in keys if _logical_key(storage, key) in expected))


def _select_source(
    storage: PublishedStorageResource,
    identity: VersionIdentity,
    keys: tuple[str, ...],
) -> tuple[SelectedSource | None, str | None]:
    relative_by_key = {
        key: _logical_key(storage, key).removeprefix(f"{identity.prefix}/")
        for key in keys
    }
    for format_name in CANONICAL_SOURCE_FORMAT_PRECEDENCE:
        format_keys = tuple(
            sorted(
                key
                for key, relative in relative_by_key.items()
                if relative.startswith(f"{format_name}/")
            )
        )
        if format_keys:
            selected_keys, error = _coherent_format_keys(
                format_name,
                format_keys,
                relative_by_key,
            )
            if error is not None:
                return None, error
            return _selected_source(
                identity,
                format_name,
                selected_keys,
                relative_by_key,
            ), None
        if format_name == "shapefile":
            legacy_keys = tuple(
                sorted(
                    key
                    for key, relative in relative_by_key.items()
                    if relative.startswith("unknown/")
                )
            )
            if legacy_keys:
                try:
                    selected_keys = discover_legacy_unknown_shapefile_keys(legacy_keys)
                except ValueError as exc:
                    return None, str(exc)
                if not selected_keys:
                    return None, "Legacy unknown/ does not contain a Shapefile source."
                return _selected_source(
                    identity,
                    "shapefile",
                    selected_keys,
                    relative_by_key,
                    legacy=True,
                ), None
    return None, None


def _coherent_format_keys(
    format_name: str,
    keys: tuple[str, ...],
    relative_by_key: dict[str, str],
) -> tuple[tuple[str, ...], str | None]:
    if format_name in {"geopackage", "geojson"}:
        extensions = SOURCE_FORMAT_EXTENSIONS[format_name]
        candidates = tuple(
            key for key in keys if PurePosixPath(key).suffix.casefold() in extensions
        )
        if len(candidates) != 1:
            qualifier = "multiple" if len(candidates) > 1 else "no"
            return (), (
                f"Found {qualifier} ({len(candidates)}) {format_name} source files; "
                "exactly one is required."
            )
        return candidates, None
    if format_name == "file_geodatabase":
        groups: dict[str, list[str]] = {}
        for key in keys:
            relative = relative_by_key[key].removeprefix("file_geodatabase/")
            parts = PurePosixPath(relative).parts
            archive = relative if relative.casefold().endswith(".gdb.zip") else None
            tree = next(
                (
                    "/".join(parts[: index + 1])
                    for index, part in enumerate(parts)
                    if part.casefold().endswith(".gdb")
                ),
                None,
            )
            group = archive or tree
            if group is not None:
                groups.setdefault(group, []).append(key)
        if len(groups) != 1:
            return (), (
                f"Found {len(groups)} FileGDB trees/archives; exactly one is required."
            )
        return tuple(sorted(next(iter(groups.values())))), None
    try:
        return _canonical_shapefile_keys(keys), None
    except ValueError as exc:
        return (), str(exc)


def _canonical_shapefile_keys(keys: tuple[str, ...]) -> tuple[str, ...]:
    shapefiles = tuple(
        key for key in keys if PurePosixPath(key).suffix.casefold() == ".shp"
    )
    if len(shapefiles) != 1:
        raise ValueError(
            f"Found {len(shapefiles)} Shapefile datasets; exactly one is required."
        )
    shapefile = PurePosixPath(shapefiles[0])
    selected = tuple(
        key
        for key in keys
        if PurePosixPath(key).parent == shapefile.parent
        and _is_shapefile_dataset_filename(PurePosixPath(key).name, shapefile.stem)
    )
    suffixes = {PurePosixPath(key).suffix.casefold() for key in selected}
    if not _REQUIRED_SHAPEFILE_SUFFIXES <= suffixes:
        raise ValueError(
            f"Shapefile {shapefile.name} requires .shp, .shx, and .dbf sidecars."
        )
    return selected


def _is_shapefile_dataset_filename(filename: str, stem: str) -> bool:
    normalized_name = filename.casefold()
    normalized_stem = stem.casefold()
    if normalized_name.startswith(f"{normalized_stem}.") and normalized_name.endswith(
        ".atx"
    ):
        return True
    return any(
        normalized_name == f"{normalized_stem}{suffix}"
        for suffix in SHAPEFILE_DATASET_SUFFIXES
    )


def _selected_source(
    identity: VersionIdentity,
    format_name: str,
    source_keys: tuple[str, ...],
    relative_by_key: dict[str, str],
    *,
    legacy: bool = False,
) -> SelectedSource:
    destination_keys: list[str] = []
    for key in source_keys:
        relative = relative_by_key[key]
        if legacy:
            relative = f"shapefile/{relative.removeprefix('unknown/')}"
        destination_keys.append(f"{identity.prefix}/{relative}")
    return SelectedSource(
        format_name,
        tuple(sorted(source_keys)),
        tuple(sorted(destination_keys)),
    )

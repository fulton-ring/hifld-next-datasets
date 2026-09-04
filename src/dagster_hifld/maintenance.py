"""Dry-run-first inventory and staging restoration for published datasets."""

from __future__ import annotations

import argparse
import json
import re
import tempfile
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

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


@dataclass(frozen=True)
class PromotionPlan:
    candidate_pairs: tuple[tuple[str, str], ...]
    stale_destination_keys: tuple[str, ...]
    conflicting_destination_keys: tuple[str, ...]


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
        candidate: StagingStorageResource | None = None
        try:
            with _candidate_storage(staging, apply=apply) as candidate:
                candidate_logical_keys = _build_candidate(published, candidate, item)
                plan = _plan_promotion(
                    candidate,
                    staging,
                    item,
                    candidate_logical_keys,
                )
                if plan.conflicting_destination_keys and not overwrite:
                    joined = ", ".join(plan.conflicting_destination_keys)
                    raise ValueError(
                        f"Staging contains conflicting managed destination objects: {joined}. "
                        "Use --overwrite-existing-sources to replace only the selected "
                        "source and its managed manifests."
                    )
                if apply:
                    _promote_candidate(candidate, staging, plan)
            results.append(
                VersionMaintenanceResult(
                    item.dataset,
                    item.file,
                    item.version,
                    "restored" if apply else "planned",
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
                    (_stable_restore_error(exc, candidate),),
                )
            )
    return MaintenanceReport(
        "restore-staging",
        apply,
        overwrite,
        tuple(results),
    )


def _stable_restore_error(
    error: Exception,
    candidate: StagingStorageResource | None,
) -> str:
    message = f"{type(error).__name__}: {error}"
    if candidate is not None:
        candidate_roots: list[str] = []
        if candidate.use_local or not candidate.bucket:
            candidate_root = Path(candidate.local_dir).resolve()
            if candidate.prefix:
                candidate_root /= candidate.prefix
            candidate_roots.append(str(candidate_root))
        elif candidate.bucket and candidate.prefix:
            candidate_roots.extend(
                (
                    f"gs://{candidate.bucket}/{candidate.prefix}",
                    f"{candidate.bucket}/{candidate.prefix}",
                )
            )
        if candidate.prefix:
            candidate_roots.append(candidate.prefix)
        for root in sorted(candidate_roots, key=len, reverse=True):
            message = message.replace(root.rstrip("/"), "<candidate>")

    system_temp_root = re.escape(str(Path(tempfile.gettempdir()).resolve()))
    return re.sub(
        rf"{system_temp_root}/hifld_staging_[^/\s:;'\"]+",
        "<candidate>",
        message,
    )


@contextmanager
def _candidate_storage(
    staging: StagingStorageResource,
    *,
    apply: bool,
) -> Iterator[StagingStorageResource]:
    if not apply:
        with tempfile.TemporaryDirectory(prefix="hifld_restore_candidate_") as tmpdir:
            yield StagingStorageResource(local_dir=tmpdir, use_local=True)
        return

    operation_prefix = _storage_key(
        staging,
        f"_temporary/restore-{uuid.uuid4().hex}",
    )
    candidate = StagingStorageResource(
        bucket=staging.bucket,
        prefix=f"{operation_prefix}/candidate",
        use_local=staging.use_local,
        local_dir=staging.local_dir,
    )
    try:
        yield candidate
    finally:
        staging.delete_prefix(operation_prefix)


def _build_candidate(
    published: PublishedStorageResource,
    candidate: StagingStorageResource,
    item: VersionMaintenanceResult,
) -> tuple[str, ...]:
    _copy_missing_or_changed(
        published,
        candidate,
        item.source_keys,
        item.destination_keys,
    )
    metadata_destinations = tuple(
        _candidate_metadata_destination(published, item, key)
        for key in item.metadata_keys
    )
    _copy_missing_or_changed(
        published,
        candidate,
        item.metadata_keys,
        metadata_destinations,
    )

    version_manifest_key = (
        f"{item.dataset}/{item.file}/{item.version}/metadata/source_manifest.json"
    )
    upstream_manifest_key = (
        f"{item.dataset}/{item.file}/{item.version}/metadata/"
        "upstream_source_manifest.json"
    )
    if candidate.object_exists(upstream_manifest_key):
        raw_version_manifest = candidate.read_bytes(
            item.dataset,
            item.file,
            item.version,
            "metadata/upstream_source_manifest.json",
        )
        candidate.write_key(version_manifest_key, raw_version_manifest)

    resolved = load_resolved_source_manifest(
        candidate,
        item.dataset,
        item.file,
        item.version,
    )
    resolved_metadata = dict(resolved.metadata)
    resolved_metadata["manifest_keys"] = list(metadata_destinations)
    resolved_metadata["manifest_role"] = "resolved_version"
    resolved_metadata["schema_version"] = "v1"
    candidate.write_key(
        version_manifest_key,
        json.dumps(resolved_metadata, sort_keys=True, indent=2).encode("utf-8"),
    )

    summary = summarize_staged_catalog(
        candidate,
        item.dataset,
        item.file,
        item.version,
        item.dataset,
        source_metadata=resolved_metadata,
    )
    write_catalog_metadata(
        candidate,
        item.dataset,
        item.file,
        item.version,
        summary.quality_manifest,
        summary.data_dictionary,
    )
    return tuple(
        sorted(
            set(item.destination_keys)
            | set(metadata_destinations)
            | {
                version_manifest_key,
                f"{item.dataset}/{item.file}/{item.version}/metadata/quality_manifest.json",
                f"{item.dataset}/{item.file}/{item.version}/metadata/data_dictionary.json",
            }
        )
    )


def _candidate_metadata_destination(
    published: PublishedStorageResource,
    item: VersionMaintenanceResult,
    source_key: str,
) -> str:
    logical_key = _logical_key(published, source_key)
    version_manifest_key = (
        f"{item.dataset}/{item.file}/{item.version}/metadata/source_manifest.json"
    )
    if logical_key == version_manifest_key:
        return (
            f"{item.dataset}/{item.file}/{item.version}/metadata/"
            "upstream_source_manifest.json"
        )
    return logical_key


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


def _plan_promotion(
    candidate: StagingStorageResource,
    staging: StagingStorageResource,
    item: VersionMaintenanceResult,
    candidate_logical_keys: tuple[str, ...],
) -> PromotionPlan:
    candidate_pairs = tuple(
        sorted(
            (
                _storage_key(candidate, logical_key),
                _storage_key(staging, logical_key),
            )
            for logical_key in candidate_logical_keys
        )
    )
    candidate_by_destination = {
        destination_key: candidate_key
        for candidate_key, destination_key in candidate_pairs
    }
    existing_keys = _managed_destination_keys(staging, item)
    stale_keys = tuple(
        sorted(key for key in existing_keys if key not in candidate_by_destination)
    )
    conflicting_keys = set(stale_keys)
    for destination_key, candidate_key in candidate_by_destination.items():
        if staging.object_exists(
            destination_key
        ) and not candidate.object_content_matches(
            staging,
            candidate_key,
            destination_key,
        ):
            conflicting_keys.add(destination_key)
    return PromotionPlan(
        candidate_pairs,
        stale_keys,
        tuple(sorted(conflicting_keys)),
    )


def _managed_destination_keys(
    staging: StagingStorageResource,
    item: VersionMaintenanceResult,
) -> tuple[str, ...]:
    version_prefix = f"{item.dataset}/{item.file}/{item.version}"
    managed_metadata = {
        f"{item.dataset}/metadata/source_manifest.json",
        f"{item.dataset}/{item.file}/metadata/source_manifest.json",
        f"{version_prefix}/metadata/source_manifest.json",
        f"{version_prefix}/metadata/upstream_source_manifest.json",
        f"{version_prefix}/metadata/quality_manifest.json",
        f"{version_prefix}/metadata/data_dictionary.json",
    }
    keys = {
        key
        for key in staging.list_keys(item.dataset, item.file, item.version)
        if (
            _logical_key(staging, key)
            .removeprefix(f"{version_prefix}/")
            .split("/", 1)[0]
            in CANONICAL_SOURCE_FORMAT_PRECEDENCE
            or _logical_key(staging, key) in managed_metadata
        )
    }
    for logical_key in managed_metadata:
        storage_key = _storage_key(staging, logical_key)
        if staging.object_exists(storage_key):
            keys.add(storage_key)
    return tuple(sorted(keys))


def _promote_candidate(
    candidate: StagingStorageResource,
    staging: StagingStorageResource,
    plan: PromotionPlan,
) -> None:
    candidate_by_destination = {
        destination_key: candidate_key
        for candidate_key, destination_key in plan.candidate_pairs
    }
    promote_pairs = tuple(
        (candidate_key, destination_key)
        for destination_key, candidate_key in sorted(candidate_by_destination.items())
        if not candidate.object_content_matches(
            staging,
            candidate_key,
            destination_key,
        )
    )
    affected_existing = tuple(
        sorted(
            {
                destination_key
                for _candidate_key, destination_key in promote_pairs
                if staging.object_exists(destination_key)
            }
            | set(plan.stale_destination_keys)
        )
    )
    operation_prefix = candidate.prefix.rsplit("/candidate", 1)[0]
    backup = StagingStorageResource(
        bucket=staging.bucket,
        prefix=f"{operation_prefix}/backup",
        use_local=staging.use_local,
        local_dir=staging.local_dir,
    )
    backup_logical_keys = tuple(_logical_key(staging, key) for key in affected_existing)
    staging.copy_keys_to(
        backup,
        list(affected_existing),
        destination_keys=list(backup_logical_keys),
        max_workers=1,
    )
    mutated_destination_keys = tuple(
        sorted(
            set(plan.stale_destination_keys)
            | {destination_key for _candidate_key, destination_key in promote_pairs}
        )
    )
    try:
        for stale_key in plan.stale_destination_keys:
            staging.delete_prefix(stale_key)
        candidate.copy_keys_to(
            staging,
            [candidate_key for candidate_key, _destination_key in promote_pairs],
            destination_keys=[
                _logical_key(staging, destination_key)
                for _candidate_key, destination_key in promote_pairs
            ],
            max_workers=1,
        )
    except Exception as promotion_error:
        try:
            for destination_key in mutated_destination_keys:
                staging.delete_prefix(destination_key)
            backup_keys = backup.list_prefix()
            backup.copy_keys_to(
                staging,
                backup_keys,
                destination_keys=[
                    _logical_key(backup, backup_key) for backup_key in backup_keys
                ],
                max_workers=1,
            )
        except Exception as rollback_error:
            raise RuntimeError(
                f"Final promotion failed ({promotion_error}); rollback also failed "
                f"({rollback_error})."
            ) from promotion_error
        raise


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
                help=(
                    "Replace only the selected versions' conflicting managed source, "
                    "provenance, and metadata objects."
                ),
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


def _storage_key(storage: StagingStorageResource, logical_key: str) -> str:
    prefix = storage.prefix.strip("/")
    normalized = logical_key.lstrip("/")
    return f"{prefix}/{normalized}" if prefix else normalized


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

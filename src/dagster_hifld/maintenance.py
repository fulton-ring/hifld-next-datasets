"""Dry-run-first inventory and staging restoration for published datasets."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import statistics
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol, cast

from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.resources import (
    PublishedStorageResource,
    StagingStorageResource,
    StorageObjectSnapshot,
    snapshots_content_match,
)
from dagster_hifld.source_formats import (
    CANONICAL_SOURCE_FORMAT_PRECEDENCE,
    SHAPEFILE_DATASET_SUFFIXES,
    SOURCE_FORMAT_EXTENSIONS,
    discover_legacy_unknown_shapefile_keys,
)
from dagster_hifld.source_manifest import load_resolved_source_manifest

_IGNORED_ROOTS = frozenset({"_temporary", "_rollback"})
_REQUIRED_SHAPEFILE_SUFFIXES = frozenset({".shp", ".shx", ".dbf"})
_DEFAULT_ROW_GROUP_LIMIT = 128 * 1024 * 1024
_DEFAULT_S2_LIMIT = 1024 * 1024 * 1024


class _TileResponse(Protocol):
    status_code: int
    content: bytes


class _TileClient(Protocol):
    def get(
        self, url: str, *, headers: Mapping[str, str], timeout: float
    ) -> _TileResponse: ...


class _ParquetRowGroup(Protocol):
    num_rows: int
    total_byte_size: int


class _ParquetMetadata(Protocol):
    num_row_groups: int

    def row_group(self, index: int) -> _ParquetRowGroup: ...


class _ParquetFile(Protocol):
    metadata: _ParquetMetadata
    schema_arrow: object


class MaintenanceJsonReport(dict[str, object]):
    """Mapping report with the same ``to_dict`` convenience as legacy reports."""

    def to_dict(self) -> dict[str, object]:
        return dict(self)

    @property
    def has_failures(self) -> bool:
        return self.get("status") != "compliant"


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
    source_snapshots: tuple[StorageObjectSnapshot, ...] = ()
    metadata_snapshots: tuple[StorageObjectSnapshot, ...] = ()

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
            "source_snapshots": [
                snapshot.to_dict() for snapshot in self.source_snapshots
            ],
            "metadata_snapshots": [
                snapshot.to_dict() for snapshot in self.metadata_snapshots
            ],
        }


@dataclass(frozen=True)
class MaintenanceReport:
    action: str
    apply: bool
    overwrite: bool
    versions: tuple[VersionMaintenanceResult, ...]
    errors: tuple[str, ...] = ()

    @property
    def has_failures(self) -> bool:
        return bool(self.errors) or any(
            item.status in {"blocked", "failed"} for item in self.versions
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "action": self.action,
            "apply": self.apply,
            "overwrite": self.overwrite,
            "versions": [item.to_dict() for item in self.versions],
            "errors": list(self.errors),
        }


@dataclass(frozen=True)
class PromotionCopy:
    source_snapshot: StorageObjectSnapshot
    destination_key: str
    destination_snapshot: StorageObjectSnapshot | None


@dataclass(frozen=True)
class PromotionPlan:
    copies: tuple[PromotionCopy, ...]
    stale_destination_snapshots: tuple[StorageObjectSnapshot, ...]
    conflicting_destination_keys: tuple[str, ...]


@dataclass(frozen=True)
class PromotionMutation:
    destination_key: str
    previous_snapshot: StorageObjectSnapshot | None
    promoted_snapshot: StorageObjectSnapshot | None


class IncompleteRollbackError(RuntimeError):
    """Raised when canonical state could not be fully restored from backup."""


def inventory_published(
    published: PublishedStorageResource,
    *,
    dataset: str | None = None,
    file: str | None = None,
    version: str | None = None,
) -> MaintenanceReport:
    """Inventory published object names and select one coherent source per version."""
    listing_parts: list[str] = []
    if dataset is not None:
        listing_parts.append(dataset)
        if file is not None:
            listing_parts.append(file)
            if version is not None:
                listing_parts.append(version)
    listing_prefix = "/".join(listing_parts)
    snapshots_by_key = {
        snapshot.key: snapshot
        for snapshot in published.list_object_snapshots(listing_prefix)
    }
    ancestor_manifest_keys: list[str] = []
    if dataset is not None and file is not None:
        ancestor_manifest_keys.extend(
            (
                f"{dataset}/metadata/source_manifest.json",
                f"{dataset}/{file}/metadata/source_manifest.json",
            )
        )
    for logical_key in ancestor_manifest_keys:
        storage_key = _storage_key(published, logical_key)
        if storage_key not in snapshots_by_key:
            snapshot = published.object_snapshot(storage_key)
            if snapshot is not None:
                snapshots_by_key[storage_key] = snapshot
    all_snapshots = tuple(snapshots_by_key[key] for key in sorted(snapshots_by_key))
    all_keys = [snapshot.key for snapshot in all_snapshots]
    snapshot_by_key = snapshots_by_key
    logical_by_key = {key: _logical_key(published, key) for key in all_keys}
    version_keys_by_identity: dict[VersionIdentity, list[str]] = {}
    metadata_key_by_logical: dict[str, str] = {}
    for key, logical_key in logical_by_key.items():
        if logical_key.endswith("/metadata/source_manifest.json"):
            metadata_key_by_logical[logical_key] = key
        identity = _version_identity(logical_key)
        if (
            identity is not None
            and (dataset is None or identity.dataset == dataset)
            and (file is None or identity.file == file)
            and (version is None or identity.version == version)
        ):
            version_keys_by_identity.setdefault(identity, []).append(key)

    identities = sorted(version_keys_by_identity)
    results: list[VersionMaintenanceResult] = []
    for identity in identities:
        version_keys = tuple(sorted(version_keys_by_identity[identity]))
        selected, error = _select_source(
            published,
            identity,
            version_keys,
            logical_by_key=logical_by_key,
        )
        metadata_keys = tuple(
            key
            for logical_key in (
                f"{identity.dataset}/metadata/source_manifest.json",
                f"{identity.dataset}/{identity.file}/metadata/source_manifest.json",
                f"{identity.prefix}/metadata/source_manifest.json",
            )
            if (key := metadata_key_by_logical.get(logical_key)) is not None
        )
        if error is not None:
            results.append(
                VersionMaintenanceResult(
                    identity.dataset,
                    identity.file,
                    identity.version,
                    "blocked",
                    metadata_keys=metadata_keys,
                    errors=(error,),
                    metadata_snapshots=tuple(
                        snapshot_by_key[key] for key in metadata_keys
                    ),
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
                    metadata_snapshots=tuple(
                        snapshot_by_key[key] for key in metadata_keys
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
                source_snapshots=tuple(
                    snapshot_by_key[key] for key in selected.source_keys
                ),
                metadata_snapshots=tuple(snapshot_by_key[key] for key in metadata_keys),
            )
        )
    errors: tuple[str, ...] = ()
    if not results and any(
        selector is not None for selector in (dataset, file, version)
    ):
        selected_filters = ", ".join(
            f"{name}={value}"
            for name, value in (
                ("dataset", dataset),
                ("file", file),
                ("version", version),
            )
            if value is not None
        )
        errors = (
            f"Explicit filters matched no published versions: {selected_filters}.",
        )
    return MaintenanceReport("inventory", False, False, tuple(results), errors)


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
    namespace_error = _storage_namespace_error(published, staging)
    if namespace_error is not None:
        return MaintenanceReport(
            "restore-staging",
            apply,
            overwrite,
            (),
            (namespace_error,),
        )
    inventory = inventory_published(
        published,
        dataset=dataset,
        file=file,
        version=version,
    )
    if inventory.errors:
        return MaintenanceReport(
            "restore-staging",
            apply,
            overwrite,
            (),
            inventory.errors,
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
                    source_snapshots=item.source_snapshots,
                    metadata_snapshots=item.metadata_snapshots,
                )
            )
        except Exception as exc:  # noqa: BLE001
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
                    item.source_snapshots,
                    item.metadata_snapshots,
                )
            )
    return MaintenanceReport(
        "restore-staging",
        apply,
        overwrite,
        tuple(results),
    )


def _storage_namespace_error(
    published: PublishedStorageResource,
    staging: StagingStorageResource,
) -> str | None:
    published_is_local = published.use_local or not published.bucket
    staging_is_local = staging.use_local or not staging.bucket
    if published_is_local != staging_is_local:
        return None

    if published_is_local:
        published_root = (
            Path(published.local_dir).resolve() / published.prefix
        ).resolve()
        staging_root = (Path(staging.local_dir).resolve() / staging.prefix).resolve()
        overlaps = (
            published_root == staging_root
            or published_root in staging_root.parents
            or staging_root in published_root.parents
        )
    else:
        if published.bucket != staging.bucket:
            return None
        published_parts = PurePosixPath(published.prefix.strip("/")).parts
        staging_parts = PurePosixPath(staging.prefix.strip("/")).parts
        common_length = min(len(published_parts), len(staging_parts))
        overlaps = published_parts[:common_length] == staging_parts[:common_length]

    if not overlaps:
        return None
    return (
        "Published and staging storage namespaces overlap; refusing inventory or "
        "restoration to protect published objects."
    )


def _stable_restore_error(
    error: Exception,
    candidate: StagingStorageResource | None,
) -> str:
    message = f"{type(error).__name__}: {error}"
    if candidate is not None:
        operation_prefix = candidate.prefix.removesuffix("/candidate")
        if operation_prefix != candidate.prefix:
            operation_roots = [operation_prefix]
            if candidate.use_local or not candidate.bucket:
                operation_roots.append(
                    str(Path(candidate.local_dir).resolve() / operation_prefix)
                )
            elif candidate.bucket:
                operation_roots.extend(
                    (
                        f"gs://{candidate.bucket}/{operation_prefix}",
                        f"{candidate.bucket}/{operation_prefix}",
                    )
                )
            for root in sorted(operation_roots, key=len, reverse=True):
                message = message.replace(root.rstrip("/"), "<operation>")
        elif candidate.use_local or not candidate.bucket:
            message = message.replace(
                str(Path(candidate.local_dir).resolve()).rstrip("/"),
                "<candidate>",
            )

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
    except IncompleteRollbackError as rollback_error:
        try:
            staging.delete_prefix(candidate.prefix)
        except Exception as cleanup_error:  # noqa: BLE001
            raise IncompleteRollbackError(
                f"{rollback_error} Candidate cleanup also failed ({cleanup_error})."
            ) from rollback_error
        raise
    except Exception:
        staging.delete_prefix(operation_prefix)
        raise
    else:
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
        item.source_snapshots,
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
        item.metadata_snapshots,
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
    source_snapshots: tuple[StorageObjectSnapshot, ...],
) -> None:
    for source_key, destination_key, source_snapshot in zip(
        source_keys,
        destination_keys,
        source_snapshots,
        strict=True,
    ):
        source.copy_key_to_if_unchanged(
            destination,
            source_key,
            destination_key,
            source_snapshot=source_snapshot,
            destination_snapshot=None,
        )


def _plan_promotion(
    candidate: StagingStorageResource,
    staging: StagingStorageResource,
    item: VersionMaintenanceResult,
    candidate_logical_keys: tuple[str, ...],
) -> PromotionPlan:
    existing_snapshots = _managed_destination_snapshots(staging, item)
    existing_by_key = {snapshot.key: snapshot for snapshot in existing_snapshots}
    copies: list[PromotionCopy] = []
    candidate_destination_keys: set[str] = set()
    conflicting_keys: set[str] = set()
    for logical_key in sorted(candidate_logical_keys):
        candidate_key = _storage_key(candidate, logical_key)
        destination_key = _storage_key(staging, logical_key)
        candidate_snapshot = candidate.object_snapshot(candidate_key)
        if candidate_snapshot is None:
            raise RuntimeError(
                f"Candidate object disappeared before preflight: {logical_key}"
            )
        destination_snapshot = existing_by_key.get(destination_key)
        copies.append(
            PromotionCopy(
                candidate_snapshot,
                destination_key,
                destination_snapshot,
            )
        )
        candidate_destination_keys.add(destination_key)
        if destination_snapshot is not None and not snapshots_content_match(
            candidate_snapshot,
            destination_snapshot,
        ):
            conflicting_keys.add(destination_key)
    stale_snapshots = tuple(
        snapshot
        for snapshot in existing_snapshots
        if snapshot.key not in candidate_destination_keys
    )
    conflicting_keys.update(snapshot.key for snapshot in stale_snapshots)
    return PromotionPlan(
        tuple(copies),
        stale_snapshots,
        tuple(sorted(conflicting_keys)),
    )


def _managed_destination_snapshots(
    staging: StagingStorageResource,
    item: VersionMaintenanceResult,
) -> tuple[StorageObjectSnapshot, ...]:
    version_prefix = f"{item.dataset}/{item.file}/{item.version}"
    managed_metadata = {
        f"{item.dataset}/metadata/source_manifest.json",
        f"{item.dataset}/{item.file}/metadata/source_manifest.json",
        f"{version_prefix}/metadata/source_manifest.json",
        f"{version_prefix}/metadata/upstream_source_manifest.json",
        f"{version_prefix}/metadata/quality_manifest.json",
        f"{version_prefix}/metadata/data_dictionary.json",
    }
    snapshots_by_key = {
        snapshot.key: snapshot
        for snapshot in staging.list_object_snapshots(version_prefix)
        if (
            _logical_key(staging, snapshot.key)
            .removeprefix(f"{version_prefix}/")
            .split("/", 1)[0]
            in CANONICAL_SOURCE_FORMAT_PRECEDENCE
            or _logical_key(staging, snapshot.key) in managed_metadata
        )
    }
    for logical_key in managed_metadata:
        storage_key = _storage_key(staging, logical_key)
        snapshot = staging.object_snapshot(storage_key)
        if snapshot is not None:
            snapshots_by_key[storage_key] = snapshot
    return tuple(snapshots_by_key[key] for key in sorted(snapshots_by_key))


def _promote_candidate(
    candidate: StagingStorageResource,
    staging: StagingStorageResource,
    plan: PromotionPlan,
) -> None:
    for copy in plan.copies:
        if staging.object_snapshot(copy.destination_key) != copy.destination_snapshot:
            raise RuntimeError(
                f"Destination changed after conflict preflight: {copy.destination_key}"
            )
    for stale_snapshot in plan.stale_destination_snapshots:
        if staging.object_snapshot(stale_snapshot.key) != stale_snapshot:
            raise RuntimeError(
                f"Destination changed after conflict preflight: {stale_snapshot.key}"
            )

    promotion_copies = tuple(
        copy
        for copy in plan.copies
        if copy.destination_snapshot is None
        or not snapshots_content_match(
            copy.source_snapshot,
            copy.destination_snapshot,
        )
    )
    affected_existing = tuple(
        sorted(
            {
                copy.destination_snapshot
                for copy in promotion_copies
                if copy.destination_snapshot is not None
            }
            | set(plan.stale_destination_snapshots),
            key=lambda snapshot: snapshot.key,
        )
    )
    operation_prefix = candidate.prefix.rsplit("/candidate", 1)[0]
    backup = StagingStorageResource(
        bucket=staging.bucket,
        prefix=f"{operation_prefix}/backup",
        use_local=staging.use_local,
        local_dir=staging.local_dir,
    )
    backup_snapshots_by_destination: dict[str, StorageObjectSnapshot] = {}
    for existing_snapshot in affected_existing:
        backup_key = _logical_key(staging, existing_snapshot.key)
        backup_snapshot = staging.copy_key_to_if_unchanged(
            backup,
            existing_snapshot.key,
            backup_key,
            source_snapshot=existing_snapshot,
            destination_snapshot=None,
        )
        backup_snapshots_by_destination[existing_snapshot.key] = backup_snapshot

    mutations: list[PromotionMutation] = []
    try:
        for stale_snapshot in plan.stale_destination_snapshots:
            try:
                staging.delete_key_if_unchanged(stale_snapshot.key, stale_snapshot)
            except Exception as delete_error:
                current = staging.object_snapshot(stale_snapshot.key)
                if current != stale_snapshot:
                    raise IncompleteRollbackError(
                        "Conditional delete failed and the destination no longer matches "
                        f"preflight: {stale_snapshot.key}. Recovery backup retained at "
                        f"{operation_prefix}/backup."
                    ) from delete_error
                raise
            mutations.append(
                PromotionMutation(stale_snapshot.key, stale_snapshot, None)
            )
        for copy in promotion_copies:
            try:
                promoted_snapshot = candidate.copy_key_to_if_unchanged(
                    staging,
                    copy.source_snapshot.key,
                    _logical_key(staging, copy.destination_key),
                    source_snapshot=copy.source_snapshot,
                    destination_snapshot=copy.destination_snapshot,
                )
            except Exception as copy_error:
                current = staging.object_snapshot(copy.destination_key)
                if current != copy.destination_snapshot:
                    raise IncompleteRollbackError(
                        "Conditional copy failed and the destination no longer matches "
                        f"preflight: {copy.destination_key}. Recovery backup retained at "
                        f"{operation_prefix}/backup."
                    ) from copy_error
                raise
            mutations.append(
                PromotionMutation(
                    copy.destination_key,
                    copy.destination_snapshot,
                    promoted_snapshot,
                )
            )
    except Exception as promotion_error:
        try:
            _rollback_promotions(
                backup,
                staging,
                mutations,
                backup_snapshots_by_destination,
            )
        except Exception as rollback_error:  # noqa: BLE001
            raise IncompleteRollbackError(
                f"Final promotion failed ({promotion_error}); rollback also failed "
                f"({rollback_error}). Recovery backup retained at "
                f"{operation_prefix}/backup."
            ) from promotion_error
        raise


def _rollback_promotions(
    backup: StagingStorageResource,
    staging: StagingStorageResource,
    mutations: list[PromotionMutation],
    backup_snapshots_by_destination: dict[str, StorageObjectSnapshot],
) -> None:
    for mutation in reversed(mutations):
        current = staging.object_snapshot(mutation.destination_key)
        if mutation.promoted_snapshot is None:
            if current is not None:
                raise RuntimeError(
                    f"Destination recreated during rollback: {mutation.destination_key}"
                )
        else:
            if current is None or current != mutation.promoted_snapshot:
                raise RuntimeError(
                    f"Destination changed before rollback: {mutation.destination_key}"
                )
            staging.delete_key_if_unchanged(mutation.destination_key, current)

        if mutation.previous_snapshot is None:
            continue
        backup_snapshot = backup_snapshots_by_destination[mutation.destination_key]
        backup.copy_key_to_if_unchanged(
            staging,
            backup_snapshot.key,
            _logical_key(staging, mutation.destination_key),
            source_snapshot=backup_snapshot,
            destination_snapshot=None,
        )


def _mapping(value: object) -> Mapping[str, object] | None:
    return value if isinstance(value, Mapping) else None


def _string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _integer(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _nonnegative_integer(value: object) -> bool:
    integer = _integer(value)
    return integer is not None and integer >= 0


def _layout_relative_path(path: str, identity: VersionIdentity) -> str | None:
    prefix = f"{identity.prefix}/"
    relative = path.removeprefix(prefix)
    if relative == path or not relative.startswith("geoparquet/"):
        return None
    return relative


def _declared_output_relative_path(
    path: str,
    identity: VersionIdentity,
    storage: StagingStorageResource,
    *,
    allow_storage_prefixed_path: bool,
) -> str | None:
    expected_prefix = f"{identity.prefix}/"
    if path.startswith(expected_prefix):
        relative = path.removeprefix(expected_prefix)
    elif allow_storage_prefixed_path and storage.prefix:
        candidate_prefix = f"{storage.prefix.strip('/')}/{expected_prefix}"
        if not path.startswith(candidate_prefix):
            return None
        relative = path.removeprefix(f"{storage.prefix.strip('/')}/")
        relative = relative.removeprefix(expected_prefix)
    else:
        return None
    return relative if relative.startswith("geoparquet/") else None


def _read_snapshot_bytes(
    storage: StagingStorageResource, snapshot: StorageObjectSnapshot
) -> bytes:
    logical = _logical_key(storage, snapshot.key)
    parts = PurePosixPath(logical).parts
    if len(parts) >= 4:
        data = storage.read_bytes(parts[0], parts[1], parts[2], "/".join(parts[3:]))
    elif storage.use_local or not storage.bucket:
        data = (Path(storage.local_dir).resolve() / snapshot.key).read_bytes()
    else:
        import gcsfs

        data = gcsfs.GCSFileSystem().read_bytes(f"{storage.bucket}/{snapshot.key}")
    if not isinstance(data, bytes):
        raise TypeError(f"Storage returned non-bytes content: {logical}")
    return data


def _footer_for_snapshot(
    storage: StagingStorageResource,
    snapshot: StorageObjectSnapshot,
) -> tuple[_ParquetFile, bytes]:
    """Read one footer, materializing at most one object per call.

    The returned ParquetFile is intentionally typed as object because pyarrow's
    stubs do not expose a stable common protocol across supported versions.
    """
    from pyarrow import parquet

    data = _read_snapshot_bytes(storage, snapshot)
    return cast(_ParquetFile, cast(object, parquet.ParquetFile(io.BytesIO(data)))), data


@contextmanager
def _inspect_parquet_snapshot(
    storage: StagingStorageResource,
    snapshot: StorageObjectSnapshot,
) -> Iterator[tuple[_ParquetFile, str]]:
    """Open a footer while hashing in bounded chunks.

    Local objects are opened in place. Remote objects are streamed once into a
    temporary file so Parquet never requires a whole-object ``read_bytes`` call.
    """
    digest = hashlib.sha256()
    if storage.use_local or not storage.bucket:
        path = Path(storage.local_dir).resolve() / snapshot.key
        with path.open("rb") as source:
            while chunk := source.read(8 * 1024 * 1024):
                digest.update(chunk)
        from pyarrow import parquet

        yield (
            cast(_ParquetFile, cast(object, parquet.ParquetFile(path))),
            digest.hexdigest(),
        )
        return

    import gcsfs

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix="hifld_parquet_", suffix=".parquet", delete=False
        ) as target:
            temporary_path = Path(target.name)
            fs = gcsfs.GCSFileSystem()
            with fs.open(f"{storage.bucket}/{snapshot.key}", "rb") as source:
                while chunk := source.read(8 * 1024 * 1024):
                    if not isinstance(chunk, bytes):
                        raise TypeError("GCS returned a non-byte Parquet chunk")
                    digest.update(chunk)
                    target.write(chunk)
        from pyarrow import parquet

        yield (
            cast(
                _ParquetFile,
                cast(object, parquet.ParquetFile(temporary_path)),
            ),
            digest.hexdigest(),
        )
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _schema_fingerprint(parquet_file: _ParquetFile) -> str:
    schema_arrow = getattr(parquet_file, "schema_arrow", None)
    if schema_arrow is not None:
        metadata = getattr(schema_arrow, "metadata", None)
        if metadata is not None:
            schema_arrow = schema_arrow.remove_metadata()
        return str(schema_arrow)
    schema = getattr(parquet_file, "schema", None)
    return str(schema)


def _geo_crs_fingerprint(parquet_file: _ParquetFile) -> str | None:
    schema_arrow = getattr(parquet_file, "schema_arrow", None)
    metadata = getattr(schema_arrow, "metadata", None)
    if not isinstance(metadata, Mapping):
        return None
    raw = metadata.get(b"geo")
    if not isinstance(raw, bytes):
        return None
    try:
        geo = _mapping(json.loads(raw))
    except (TypeError, json.JSONDecodeError):
        return None
    if geo is None:
        return None
    primary = _string(geo.get("primary_column"))
    columns = _mapping(geo.get("columns"))
    if primary is None or columns is None:
        return None
    primary_meta = _mapping(columns.get(primary))
    if primary_meta is None:
        return None
    crs = primary_meta.get("crs")
    return json.dumps(crs, sort_keys=True, default=str)


def _audit_version(
    storage: StagingStorageResource,
    identity: VersionIdentity,
    snapshots: tuple[StorageObjectSnapshot, ...],
    *,
    row_group_limit_bytes: int,
    s2_limit_bytes: int,
    allow_storage_prefixed_paths: bool = False,
) -> dict[str, object]:
    reasons: list[str] = []
    version_prefix = f"{identity.prefix}/"
    logical = {
        snapshot.key: _logical_key(storage, snapshot.key) for snapshot in snapshots
    }
    parquet_snapshots = tuple(
        sorted(
            (
                snapshot
                for snapshot in snapshots
                if (
                    relative := logical[snapshot.key].removeprefix(version_prefix)
                ).startswith("geoparquet/")
                and relative.endswith(".parquet")
            ),
            key=lambda snapshot: logical[snapshot.key],
        )
    )
    parquet_relative = tuple(
        logical[snapshot.key].removeprefix(version_prefix)
        for snapshot in parquet_snapshots
    )
    manifest_key = _storage_key(
        storage, f"{identity.prefix}/metadata/geoparquet_layout.json"
    )
    manifest_snapshot = storage.object_snapshot(manifest_key)
    manifest: Mapping[str, object] | None = None
    if manifest_snapshot is None:
        reasons.append("missing layout manifest")
    else:
        try:
            manifest = _mapping(
                json.loads(_read_snapshot_bytes(storage, manifest_snapshot))
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            reasons.append("unreadable layout manifest")
        if manifest is None:
            reasons.append("layout manifest is not an object")

    layers_value = manifest.get("layers") if manifest is not None else None
    layers = (
        tuple(layer for layer in layers_value if _mapping(layer) is not None)
        if isinstance(layers_value, list)
        else ()
    )
    if manifest is not None and (
        manifest.get("schema_version") != 1
        or manifest.get("validation_status") != "valid"
        or not isinstance(layers_value, list)
        or not layers
    ):
        reasons.append("invalid layout manifest schema or status")

    declared_relative: list[str] = []
    declared_by_layer: list[tuple[Mapping[str, object], tuple[str, ...]]] = []
    for raw_layer in layers:
        layer = _mapping(raw_layer)
        if layer is None or layer.get("validation_status") != "valid":
            reasons.append("invalid layer layout status")
            continue
        outputs_value = layer.get("outputs")
        if not isinstance(outputs_value, list):
            reasons.append("layer outputs are missing or invalid")
            continue
        layer_paths: list[str] = []
        for raw_output in outputs_value:
            output = _mapping(raw_output)
            path = _string(output.get("path")) if output is not None else None
            relative = (
                _declared_output_relative_path(
                    path,
                    identity,
                    storage,
                    allow_storage_prefixed_path=allow_storage_prefixed_paths,
                )
                if path is not None
                else None
            )
            if relative is None:
                reasons.append("layout output path is invalid")
                continue
            declared_relative.append(relative)
            layer_paths.append(relative)
        declared_by_layer.append((layer, tuple(layer_paths)))
    if len(declared_relative) != len(set(declared_relative)):
        reasons.append("duplicate declared parquet output paths")
    if set(declared_relative) != set(parquet_relative) or len(declared_relative) != len(
        parquet_relative
    ):
        reasons.append("layout output set does not match current parquet objects")
    flat = [path for path in parquet_relative if len(PurePosixPath(path).parts) == 2]
    nested = [path for path in parquet_relative if len(PurePosixPath(path).parts) > 2]
    if flat and nested:
        reasons.append("mixed monolith and nested GeoParquet representations")

    total_uncompressed = 0
    total_row_groups = 0
    total_features = 0
    partition_bytes: dict[str, int] = {}
    footer_by_relative: dict[
        str, tuple[_ParquetFile, StorageObjectSnapshot, list[int]]
    ] = {}
    hash_by_relative: dict[str, str] = {}
    for snapshot, relative in zip(parquet_snapshots, parquet_relative, strict=True):
        try:
            with _inspect_parquet_snapshot(storage, snapshot) as (footer, content_hash):
                metadata = footer.metadata
                row_group_sizes: list[int] = []
                row_count = 0
                for index in range(metadata.num_row_groups):
                    row_group = metadata.row_group(index)
                    size = int(row_group.total_byte_size)
                    row_group_sizes.append(size)
                    total_uncompressed += size
                    total_row_groups += 1
                    row_count += int(row_group.num_rows)
                    if size > row_group_limit_bytes:
                        reasons.append(
                            f"row group exceeds {row_group_limit_bytes} bytes: {relative}"
                        )
                total_features += row_count
                partition = "/".join(
                    part
                    for part in PurePosixPath(relative).parts[1:-1]
                    if "=" in part
                    and not part.split("=", 1)[0].endswith("s2_parent_cell")
                )
                partition_bytes[partition] = partition_bytes.get(partition, 0) + sum(
                    row_group_sizes
                )
                footer_by_relative[relative] = (footer, snapshot, row_group_sizes)
                hash_by_relative[relative] = content_hash
        except (OSError, ValueError, TypeError, RuntimeError) as exc:
            reasons.append(
                f"unreadable parquet footer: {relative} ({type(exc).__name__})"
            )

    if total_uncompressed > s2_limit_bytes:
        strategy = None
        hive_s2 = None
        if declared_by_layer:
            layer = declared_by_layer[0][0]
            strategy = _string(layer.get("partition_strategy"))
            hive = _mapping(layer.get("hive_partition_columns"))
            hive_s2 = _string(hive.get("s2_parent_cell")) if hive else None
        if strategy not in {"s2", "admin_s2"}:
            reasons.append("S2 partition required for dataset")
        elif not hive_s2:
            reasons.append("S2 partition declares no S2 Hive key")
        elif not all(f"{hive_s2}=" in path for path in parquet_relative):
            reasons.append(f"declared S2 Hive key missing: {hive_s2}=")
    for partition, measured in partition_bytes.items():
        if measured <= s2_limit_bytes:
            continue
        strategy = None
        hive_s2 = None
        for layer, paths in declared_by_layer:
            if partition and not any(
                path.startswith(f"geoparquet/{partition}") for path in paths
            ):
                continue
            strategy = _string(layer.get("partition_strategy"))
            hive = _mapping(layer.get("hive_partition_columns"))
            hive_s2 = _string(hive.get("s2_parent_cell")) if hive else None
            break
        if strategy not in {"s2", "admin_s2"}:
            reasons.append(f"S2 partition required for {partition or 'dataset'}")
        elif not hive_s2:
            reasons.append(
                f"S2 partition declares no S2 Hive key: {partition or 'dataset'}"
            )
        elif not all(
            f"{hive_s2}=" in path
            for path in parquet_relative
            if not partition or path.startswith(f"geoparquet/{partition}")
        ):
            reasons.append(f"declared S2 Hive key missing: {hive_s2}=")

    expected_feature_count = sum(
        _integer(layer.get("feature_count")) or 0 for layer, _paths in declared_by_layer
    )
    if declared_by_layer and expected_feature_count != total_features:
        reasons.append(
            f"feature count mismatch: manifest={expected_feature_count}, current={total_features}"
        )
    expected_file_count = sum(len(paths) for _layer, paths in declared_by_layer)
    if declared_by_layer and expected_file_count != len(parquet_relative):
        reasons.append(
            f"file count mismatch: manifest={expected_file_count}, current={len(parquet_relative)}"
        )
    expected_row_groups = 0
    for footer, _snapshot, _sizes in footer_by_relative.values():
        expected_row_groups += int(footer.metadata.num_row_groups)
    for field_name, actual_count, label in (
        ("feature_count", total_features, "feature"),
        ("file_count", len(parquet_relative), "file"),
        ("row_group_count", expected_row_groups, "row-group"),
    ):
        declared_count = (
            _integer(manifest.get(field_name)) if manifest is not None else None
        )
        if declared_count is not None and declared_count != actual_count:
            reasons.append(
                f"{label} count mismatch: manifest={declared_count}, current={actual_count}"
            )

    for layer, paths in declared_by_layer:
        declared_files = _integer(layer.get("file_count"))
        if declared_files is not None and declared_files != len(paths):
            reasons.append(
                f"layer file count mismatch: manifest={declared_files}, current={len(paths)}"
            )
        declared_groups = _integer(layer.get("row_group_count"))
        current_groups = sum(
            len(footer_by_relative[path][2])
            for path in paths
            if path in footer_by_relative
        )
        if declared_groups is not None and declared_groups != current_groups:
            reasons.append(
                f"layer row-group count mismatch: manifest={declared_groups}, current={current_groups}"
            )

    for layer, paths in declared_by_layer:
        fingerprints: set[tuple[str, str | None]] = set()
        for path in paths:
            entry = footer_by_relative.get(path)
            if entry is not None:
                fingerprints.add(
                    (_schema_fingerprint(entry[0]), _geo_crs_fingerprint(entry[0]))
                )
        if len(fingerprints) > 1:
            reasons.append(
                f"schema/CRS incompatibility in layer {_string(layer.get('layer')) or ''}"
            )
        outputs_value = layer.get("outputs")
        if isinstance(outputs_value, list):
            for raw_output in outputs_value:
                output = _mapping(raw_output)
                if output is None:
                    continue
                path = _string(output.get("path"))
                if path is None:
                    continue
                rel = path.removeprefix(f"{identity.prefix}/")
                declared_size = output.get("file_size_bytes")
                if not _nonnegative_integer(declared_size):
                    reasons.append(f"invalid file_size_bytes: {rel}")
                declared_hash = output.get("sha256")
                if (
                    not isinstance(declared_hash, str)
                    or re.fullmatch(r"[0-9a-f]{64}", declared_hash) is None
                ):
                    reasons.append(f"invalid sha256: {rel}")
                declared_rows = output.get("row_counts")
                if (
                    not isinstance(declared_rows, list)
                    or not declared_rows
                    or any(
                        not _nonnegative_integer(row_count)
                        for row_count in declared_rows
                    )
                ):
                    reasons.append(f"invalid row_counts: {rel}")
                declared_sizes = output.get("row_group_uncompressed_sizes")
                if (
                    not isinstance(declared_sizes, list)
                    or not declared_sizes
                    or any(not _nonnegative_integer(size) for size in declared_sizes)
                ):
                    reasons.append(f"invalid row_group_uncompressed_sizes: {rel}")
                entry = footer_by_relative.get(rel)
                if entry is None:
                    continue
                snapshot = entry[1]
                if (
                    _integer(declared_size) is not None
                    and declared_size != snapshot.size
                ):
                    reasons.append(f"size mismatch: {rel}")
                if (
                    isinstance(declared_hash, str)
                    and re.fullmatch(r"[0-9a-f]{64}", declared_hash) is not None
                ):
                    actual_hash = snapshot.sha256 or hash_by_relative[rel]
                    if declared_hash != actual_hash:
                        reasons.append(f"hash mismatch: {rel}")
                actual_rows = [
                    int(entry[0].metadata.row_group(index).num_rows)
                    for index in range(int(entry[0].metadata.num_row_groups))
                ]
                if (
                    isinstance(declared_rows, list)
                    and declared_rows
                    and declared_rows != actual_rows
                ):
                    reasons.append(f"row-group count mismatch: {rel}")
                if (
                    isinstance(declared_sizes, list)
                    and declared_sizes
                    and declared_sizes != entry[2]
                ):
                    reasons.append(f"row-group byte-size mismatch: {rel}")

    return {
        "dataset": identity.dataset,
        "file": identity.file,
        "version": identity.version,
        "status": "compliant" if not reasons else "violation",
        "reasons": sorted(set(reasons)),
        "parquet_keys": list(parquet_relative),
        "feature_count": total_features,
        "file_count": len(parquet_relative),
        "row_group_count": total_row_groups,
        "uncompressed_bytes": total_uncompressed,
    }


def audit_geoparquet(
    published: StagingStorageResource,
    *,
    dataset: str | None = None,
    file: str | None = None,
    version: str | None = None,
    row_group_limit_bytes: int = _DEFAULT_ROW_GROUP_LIMIT,
    s2_limit_bytes: int = _DEFAULT_S2_LIMIT,
    allow_storage_prefixed_paths: bool = False,
) -> dict[str, object]:
    """Audit canonical GeoParquet without changing storage."""
    selector_parts = [part for part in (dataset, file, version) if part is not None]
    listing_prefix = (
        dataset
        if dataset is not None and file is None
        else f"{dataset}/{file}"
        if dataset is not None and file is not None and version is None
        else f"{dataset}/{file}/{version}"
        if dataset is not None and file is not None and version is not None
        else ""
    )
    snapshots = published.list_object_snapshots(listing_prefix)
    grouped: dict[VersionIdentity, list[StorageObjectSnapshot]] = {}
    for snapshot in snapshots:
        logical = _logical_key(published, snapshot.key)
        identity = _version_identity(logical)
        if (
            identity is None
            or (dataset and identity.dataset != dataset)
            or (file and identity.file != file)
            or (version and identity.version != version)
        ):
            continue
        relative = logical.removeprefix(f"{identity.prefix}/")
        if (
            relative.startswith("geoparquet/")
            or relative == "metadata/geoparquet_layout.json"
        ):
            grouped.setdefault(identity, []).append(snapshot)
    versions = tuple(
        _audit_version(
            published,
            identity,
            tuple(items),
            row_group_limit_bytes=row_group_limit_bytes,
            s2_limit_bytes=s2_limit_bytes,
            allow_storage_prefixed_paths=allow_storage_prefixed_paths,
        )
        for identity, items in sorted(grouped.items())
    )
    errors: list[str] = []
    if not versions and selector_parts:
        errors.append("Explicit filters matched no GeoParquet versions.")
    has_violation = bool(errors) or any(
        item["status"] != "compliant" for item in versions
    )
    return MaintenanceJsonReport(
        {
            "action": "audit-geoparquet",
            "status": "violation" if has_violation else "compliant",
            "filters": {"dataset": dataset, "file": file, "version": version},
            "versions": list(versions),
            "errors": errors,
        }
    )


def _candidate_storage_for_repack(
    staging: StagingStorageResource, run_id: str
) -> StagingStorageResource:
    prefix = f"{staging.prefix.strip('/')}/_temporary/repack/{run_id}".strip("/")
    return StagingStorageResource(
        bucket=staging.bucket,
        prefix=prefix,
        use_local=staging.use_local,
        local_dir=staging.local_dir,
    )


def replace_geoparquet(
    published: PublishedStorageResource,
    staging: StagingStorageResource,
    dataset: str,
    file: str,
    version: str,
    run_id: str,
    *,
    apply: bool = False,
    row_group_limit_bytes: int = _DEFAULT_ROW_GROUP_LIMIT,
    s2_limit_bytes: int = _DEFAULT_S2_LIMIT,
) -> dict[str, object]:
    """Validate and optionally atomically promote one staged GeoParquet repack."""
    if any(
        not value or "/" in value or value in {".", ".."}
        for value in (dataset, file, version, run_id)
    ):
        return MaintenanceJsonReport(
            {
                "action": "replace-geoparquet",
                "status": "blocked",
                "errors": [
                    "dataset, file, version, and run_id must be exact path components"
                ],
            }
        )
    namespace_error = _storage_namespace_error(published, staging)
    if namespace_error is not None:
        return MaintenanceJsonReport(
            {
                "action": "replace-geoparquet",
                "status": "blocked",
                "errors": [namespace_error],
            }
        )
    identity = VersionIdentity(dataset, file, version)
    candidate = _candidate_storage_for_repack(staging, run_id)
    candidate_version_prefix = f"{dataset}/{file}/{version}/"
    stray_candidate_parquet = tuple(
        sorted(
            _logical_key(candidate, snapshot.key)
            for snapshot in candidate.list_object_snapshots(identity.prefix)
            if (logical := _logical_key(candidate, snapshot.key)).startswith(
                candidate_version_prefix
            )
            and logical.endswith(".parquet")
            and not logical.startswith(f"{candidate_version_prefix}geoparquet/")
        )
    )
    if stray_candidate_parquet:
        return MaintenanceJsonReport(
            {
                "action": "replace-geoparquet",
                "status": "blocked",
                "errors": [
                    "Candidate Parquet object outside geoparquet/: "
                    + ", ".join(stray_candidate_parquet)
                ],
            }
        )
    candidate_report = audit_geoparquet(
        candidate,
        dataset=dataset,
        file=file,
        version=version,
        row_group_limit_bytes=row_group_limit_bytes,
        s2_limit_bytes=s2_limit_bytes,
        allow_storage_prefixed_paths=True,
    )
    if candidate_report["status"] != "compliant":
        return MaintenanceJsonReport(
            {
                "action": "replace-geoparquet",
                "status": "blocked",
                "candidate": candidate_report,
                "errors": ["Candidate GeoParquet failed audit."],
            }
        )
    production_snapshots = {
        _logical_key(published, snapshot.key): snapshot
        for snapshot in published.list_object_snapshots(identity.prefix)
        if (
            _logical_key(published, snapshot.key).startswith(
                f"{identity.prefix}/geoparquet/"
            )
            and _logical_key(published, snapshot.key).endswith(".parquet")
        )
    }
    manifest_logical = f"{identity.prefix}/metadata/geoparquet_layout.json"
    manifest_snapshot = published.object_snapshot(
        _storage_key(published, manifest_logical)
    )
    if manifest_snapshot is not None:
        production_snapshots[manifest_logical] = manifest_snapshot
    candidate_snapshots = {
        _logical_key(candidate, snapshot.key): snapshot
        for snapshot in candidate.list_object_snapshots(identity.prefix)
        if _logical_key(candidate, snapshot.key).startswith(f"{identity.prefix}/")
        and (
            _logical_key(candidate, snapshot.key).endswith(".parquet")
            or _logical_key(candidate, snapshot.key) == manifest_logical
        )
    }
    candidate_paths = tuple(sorted(candidate_snapshots))
    canonical_paths = tuple(sorted(production_snapshots))
    backup_prefix = f"_rollback/geoparquet/{run_id}"
    backup = PublishedStorageResource(
        bucket=published.bucket,
        prefix=f"{published.prefix.rstrip('/')}/{backup_prefix}".strip("/"),
        use_local=published.use_local,
        local_dir=published.local_dir,
    )
    backup_keys = tuple(f"{backup_prefix}/{logical}" for logical in canonical_paths)
    promoted_keys = candidate_paths
    result: MaintenanceJsonReport = MaintenanceJsonReport(
        {
            "action": "replace-geoparquet",
            "status": "planned" if not apply else "promoted",
            "dataset": dataset,
            "file": file,
            "version": version,
            "run_id": run_id,
            "discovery_prefix": dataset,
            "intentional_deletion_gap": "Production GeoParquet is deleted before candidate copy; CAS rollback restores it on failure.",
            "backup_keys": list(backup_keys),
            "promoted_keys": list(promoted_keys),
            "snapshots": [
                production_snapshots[key].to_dict()
                for key in sorted(production_snapshots)
            ],
        }
    )
    if apply and (
        set(canonical_paths) == set(candidate_paths)
        and all(
            snapshots_content_match(
                production_snapshots[path], candidate_snapshots[path]
            )
            for path in canonical_paths
        )
    ):
        return MaintenanceJsonReport({**result, "status": "already-applied"})
    if not apply:
        return result
    # Re-check all production identities immediately before the mutation.
    for logical, snapshot in production_snapshots.items():
        if published.object_snapshot(_storage_key(published, logical)) != snapshot:
            return MaintenanceJsonReport(
                {
                    **result,
                    "status": "blocked",
                    "errors": [f"Generation race: {logical}"],
                }
            )
    backup_snapshots: dict[str, StorageObjectSnapshot] = {}
    promoted_snapshots: dict[str, StorageObjectSnapshot] = {}
    try:
        for logical, snapshot in sorted(production_snapshots.items()):
            backup_logical = f"{backup_prefix}/{logical}"
            existing_backup = backup.object_snapshot(backup_logical)
            if existing_backup is not None and not snapshots_content_match(
                snapshot, existing_backup
            ):
                raise RuntimeError(f"Conflicting rollback backup: {backup_logical}")
            if existing_backup is None:
                backup_snapshots[logical] = published.copy_key_to_if_unchanged(
                    backup,
                    snapshot.key,
                    backup_logical,
                    source_snapshot=snapshot,
                    destination_snapshot=None,
                )
            else:
                backup_snapshots[logical] = existing_backup
        for logical, snapshot in production_snapshots.items():
            published.delete_key_if_unchanged(snapshot.key, snapshot)
        for logical in candidate_paths:
            source_snapshot = candidate_snapshots[logical]
            promoted_snapshots[logical] = candidate.copy_key_to_if_unchanged(
                published,
                source_snapshot.key,
                logical,
                source_snapshot=source_snapshot,
                destination_snapshot=None,
            )
        result["promoted_snapshots"] = [
            promoted_snapshots[key].to_dict() for key in sorted(promoted_snapshots)
        ]
        return result
    except (OSError, ValueError, RuntimeError) as promotion_error:
        rollback_error: Exception | None = None
        try:
            for logical in candidate_paths:
                current = published.object_snapshot(_storage_key(published, logical))
                promoted = promoted_snapshots.get(logical)
                if current is not None and promoted is not None and current == promoted:
                    published.delete_key_if_unchanged(current.key, current)
            for logical, backup_snapshot in backup_snapshots.items():
                backup.copy_key_to_if_unchanged(
                    published,
                    backup_snapshot.key,
                    logical,
                    source_snapshot=backup_snapshot,
                    destination_snapshot=None,
                )
        except (OSError, ValueError, RuntimeError) as exc:
            rollback_error = exc
        if rollback_error is not None:
            return MaintenanceJsonReport(
                {
                    **result,
                    "status": "blocked",
                    "errors": [
                        f"Promotion failed: {promotion_error}",
                        f"Rollback failed; backup retained: {rollback_error}",
                    ],
                    "backup_retained": True,
                }
            )
        return MaintenanceJsonReport(
            {
                **result,
                "status": "blocked",
                "errors": [f"Promotion failed and was rolled back: {promotion_error}"],
            }
        )


def benchmark_tiles(
    base_url: str,
    query_id: str,
    token_env: str,
    tiles: Sequence[tuple[int, int, int]],
    *,
    repetitions: int = 3,
    client: _TileClient | None = None,
    clock: Callable[[], float] = time.monotonic,
    median_target_seconds: float = 8.0,
    hard_timeout_seconds: float = 10.0,
) -> dict[str, object]:
    """Benchmark query tile endpoints; token values never enter the report."""
    import httpx

    token = __import__("os").environ.get(token_env)
    if not token:
        return MaintenanceJsonReport(
            {
                "action": "benchmark-tiles",
                "status": "violation",
                "errors": [f"Missing token environment variable: {token_env}"],
            }
        )
    if repetitions < 1:
        return MaintenanceJsonReport(
            {
                "action": "benchmark-tiles",
                "status": "violation",
                "errors": ["repetitions must be positive"],
            }
        )
    owned_client = client is None
    actual_client: _TileClient = client or cast(
        _TileClient, cast(object, httpx.Client())
    )
    cases: list[dict[str, object]] = []
    errors: list[str] = []
    try:
        for z, x, y in tiles:
            durations: list[float] = []
            url = f"{base_url.rstrip('/')}/api/queries/{query_id}/tiles/{z}/{x}/{y}.mvt"
            for _index in range(repetitions):
                started = clock()
                try:
                    response = actual_client.get(
                        url,
                        headers={"X-HIFLD-Query-Token": token},
                        timeout=hard_timeout_seconds,
                    )
                except (httpx.HTTPError, OSError, ValueError, RuntimeError) as exc:
                    errors.append(f"HTTP error for {z}/{x}/{y}: {type(exc).__name__}")
                    continue
                duration = round(clock() - started, 3)
                durations.append(duration)
                if response.status_code not in {200, 204} or (
                    response.status_code == 200 and not response.content
                ):
                    errors.append(
                        f"HTTP failure for {z}/{x}/{y}: status {response.status_code}"
                    )
                if response.status_code == 200:
                    response_headers = getattr(response, "headers", {})
                    content_type = (
                        str(response_headers.get("content-type", ""))
                        .split(";", 1)[0]
                        .strip()
                        .lower()
                    )
                    if content_type != "application/vnd.mapbox-vector-tile":
                        errors.append(
                            f"Invalid MVT content type for {z}/{x}/{y}: {content_type or 'missing'}"
                        )
                if duration >= hard_timeout_seconds:
                    errors.append(f"Hard timeout exceeded for {z}/{x}/{y}")
            median = round(statistics.median(durations), 3) if durations else None
            maximum = round(max(durations), 3) if durations else None
            cases.append(
                {
                    "tile": f"{z}/{x}/{y}",
                    "durations_seconds": durations,
                    "median_seconds": median,
                    "max_seconds": maximum,
                }
            )
            if median is None or median > median_target_seconds:
                errors.append(f"Median target missed for {z}/{x}/{y}")
    finally:
        if owned_client:
            close = getattr(actual_client, "close", None)
            if callable(close):
                close()
    return MaintenanceJsonReport(
        {
            "action": "benchmark-tiles",
            "status": "compliant" if not errors else "violation",
            "query_id": query_id,
            "repetitions": repetitions,
            "median_target_seconds": median_target_seconds,
            "hard_timeout_seconds": hard_timeout_seconds,
            "cases": cases,
            "errors": sorted(set(errors)),
        }
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inventory published processing sources or restore them to staging."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("inventory", "restore-staging", "audit-geoparquet"):
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
    replace_parser = subparsers.add_parser("replace-geoparquet")
    replace_parser.add_argument("--dataset", required=True)
    replace_parser.add_argument("--file", required=True)
    replace_parser.add_argument("--version", required=True)
    replace_parser.add_argument("--run-id", required=True)
    replace_parser.add_argument("--apply", action="store_true")
    benchmark_parser = subparsers.add_parser("benchmark-tiles")
    benchmark_parser.add_argument("--base-url", required=True)
    benchmark_parser.add_argument("--query-id", required=True)
    benchmark_parser.add_argument(
        "--token-env",
        "--token-env-var",
        dest="token_env",
        required=True,
    )
    benchmark_parser.add_argument(
        "--tile", action="append", required=True, metavar="Z/X/Y"
    )
    benchmark_parser.add_argument("--repetitions", type=int, default=3)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    cli_parser = build_parser()
    args = cli_parser.parse_args(argv)
    published = PublishedStorageResource.from_env()
    if args.command == "inventory":
        report = inventory_published(
            published,
            dataset=args.dataset,
            file=args.file,
            version=args.version,
        )
    elif args.command == "restore-staging":
        report = restore_staging(
            published,
            StagingStorageResource.from_env(),
            dataset=args.dataset,
            file=args.file,
            version=args.version,
            apply=args.apply,
            overwrite=args.overwrite_existing_sources,
        )
    elif args.command == "audit-geoparquet":
        report = audit_geoparquet(
            published,
            dataset=args.dataset,
            file=args.file,
            version=args.version,
        )
    elif args.command == "replace-geoparquet":
        report = replace_geoparquet(
            published,
            StagingStorageResource.from_env(),
            args.dataset,
            args.file,
            args.version,
            args.run_id,
            apply=args.apply,
        )
    else:
        try:
            tiles = tuple(
                tuple(int(part) for part in tile.split("/")) for tile in args.tile
            )
            if any(len(tile) != 3 for tile in tiles):
                raise ValueError
        except ValueError:
            cli_parser.error("--tile must be Z/X/Y")
        report = benchmark_tiles(
            args.base_url,
            args.query_id,
            args.token_env,
            cast(Sequence[tuple[int, int, int]], tiles),
            repetitions=args.repetitions,
        )
    payload = report.to_dict() if isinstance(report, MaintenanceReport) else report
    print(json.dumps(payload, sort_keys=True, indent=2))
    failed = (
        report.has_failures
        if isinstance(report, MaintenanceReport)
        else payload.get("status") != "compliant"
    )
    return 1 if failed else 0


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


def _select_source(
    storage: PublishedStorageResource,
    identity: VersionIdentity,
    keys: tuple[str, ...],
    *,
    logical_by_key: dict[str, str] | None = None,
) -> tuple[SelectedSource | None, str | None]:
    if logical_by_key is None:
        logical_by_key = {key: _logical_key(storage, key) for key in keys}
    relative_by_key = {
        key: logical_by_key[key].removeprefix(f"{identity.prefix}/") for key in keys
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

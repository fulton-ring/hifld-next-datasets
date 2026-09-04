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
from dagster_hifld.resources import (
    PublishedStorageResource,
    StagingStorageResource,
    StorageObjectSnapshot,
    snapshots_content_match,
)
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
        except Exception as cleanup_error:
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
        except Exception as rollback_error:
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

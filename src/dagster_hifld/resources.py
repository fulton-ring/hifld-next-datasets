"""Dagster resources for staging/published storage and optional API utilities."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, replace
from functools import cache
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Protocol, cast

import google_crc32c
import httpx
from dagster import ConfigurableResource
from google.cloud import storage
from google.cloud.storage.blob import Blob

logger = logging.getLogger(__name__)

_GCS_DOWNLOAD_TIMEOUT_SECONDS = 60


class _GCSWritable(Protocol):
    generation: str | int | None

    def write(self, data: bytes) -> int: ...

    def __enter__(self) -> "_GCSWritable": ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...


class _GCSJsonClient(Protocol):
    _location: str

    def call(
        self,
        method: str,
        path: str,
        *args: str,
        **kwargs: object,
    ) -> dict[str, object]: ...


@dataclass(frozen=True)
class StorageObjectSnapshot:
    key: str
    size: int
    generation: str | None
    md5: str | None
    crc32c: str | None
    sha256: str | None = None

    def to_dict(self) -> dict[str, str | int | None]:
        return {
            "key": self.key,
            "size": self.size,
            "generation": self.generation,
            "md5": self.md5,
            "crc32c": self.crc32c,
            "sha256": self.sha256,
        }


class StagingStorageResource(ConfigurableResource):
    """Staging storage: writes to GCS bucket or local path.

    - If HIFLD_STAGING_BUCKET is set: use GCS (requires gcsfs and credentials).
    - Else: use local directory HIFLD_STAGING_DIR or ./data/staging.
    """

    bucket: str | None = None
    prefix: str = ""
    use_local: bool = True
    local_dir: str = ""
    backend: str = "gcs"
    s3_endpoint_url: str | None = None
    s3_access_key_id: str | None = None
    s3_secret_access_key: str | None = None
    s3_region: str = "us-east-1"
    s3_use_ssl: bool = False

    @classmethod
    def from_env(cls) -> "StagingStorageResource":
        bucket = os.environ.get("HIFLD_STAGING_BUCKET") or None
        local_dir = os.environ.get("HIFLD_STAGING_DIR", "data/staging")
        backend = os.environ.get("HIFLD_STAGING_BACKEND", "gcs")
        return cls(bucket=bucket, prefix=os.environ.get("HIFLD_STAGING_PREFIX", "").strip("/"), use_local=not bool(bucket), local_dir=local_dir, backend=backend, s3_endpoint_url=os.environ.get("HIFLD_STAGING_S3_ENDPOINT"), s3_access_key_id=os.environ.get("HIFLD_STAGING_S3_ACCESS_KEY"), s3_secret_access_key=os.environ.get("HIFLD_STAGING_S3_SECRET_KEY"), s3_region=os.environ.get("HIFLD_STAGING_S3_REGION", "us-east-1"), s3_use_ssl=os.environ.get("HIFLD_STAGING_S3_USE_SSL", "false").lower() == "true")

    def _uses_s3(self) -> bool:
        return not self.use_local and self.backend == "s3"

    def _s3(self):
        if not self.bucket:
            raise RuntimeError("S3 storage requires a bucket.")
        import boto3
        from botocore.config import Config
        return boto3.client("s3", endpoint_url=self.s3_endpoint_url, aws_access_key_id=self.s3_access_key_id, aws_secret_access_key=self.s3_secret_access_key, region_name=self.s3_region, use_ssl=self.s3_use_ssl, config=Config(s3={"addressing_style": "path"}))

    def _apply_prefix(self, key: str) -> str:
        if self.prefix:
            return f"{self.prefix.rstrip('/')}/{key.lstrip('/')}"
        return key

    def _ensure_prefixed(self, key: str) -> str:
        normalized = key.lstrip("/")
        if not self.prefix:
            return normalized
        prefix = self.prefix.rstrip("/")
        if normalized == prefix or normalized.startswith(f"{prefix}/"):
            return normalized
        return f"{prefix}/{normalized}"

    def build_target_location(
        self,
        dataset_slug: str,
        file_slug: str,
        version: str,
        filename: str,
    ) -> str:
        return self._apply_prefix(f"{dataset_slug}/{file_slug}/{version}/{filename}")

    def write_key(self, key: str, data: bytes) -> str:
        key = self._ensure_prefixed(key)
        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            full = root / key
            full.parent.mkdir(parents=True, exist_ok=True)
            full.write_bytes(data)
            return key

        if self._uses_s3():
            content_types = {
                ".json": "application/json",
                ".sqlite": "application/vnd.sqlite3",
                ".parquet": "application/vnd.apache.parquet",
                ".pmtiles": "application/vnd.pmtiles",
            }
            self._s3().put_object(Bucket=self.bucket, Key=key, Body=data,
                                  Metadata={"sha256": hashlib.sha256(data).hexdigest()},
                                  CacheControl="no-cache",
                                  ContentType=content_types.get(Path(key).suffix.lower(), "application/octet-stream"))
            return key

        import gcsfs

        fs = gcsfs.GCSFileSystem()
        with cast(
            BinaryIO,
            cast(
                object,
                fs.open(
                    f"{self.bucket}/{key}",
                    "wb",
                    fixed_key_metadata={"cache_control": "no-cache"},
                    metadata={"sha256": hashlib.sha256(data).hexdigest()},
                ),
            ),
        ) as output:
            _ = output.write(data)
        return key

    def write(
        self,
        dataset_slug: str,
        file_slug: str,
        version: str,
        filename: str,
        data: bytes,
    ) -> str:
        key = self.build_target_location(dataset_slug, file_slug, version, filename)
        return self.write_key(key, data)

    def list_keys(self, dataset_slug: str, file_slug: str, version: str) -> list[str]:
        key_prefix = self.build_target_location(
            dataset_slug, file_slug, version, ""
        ).rstrip("/")

        return self.list_prefix(key_prefix)

    def list_prefix(self, key_prefix: str = "") -> list[str]:
        """List object keys below a logical prefix without reading object contents."""
        key_prefix = self._ensure_prefixed(key_prefix).rstrip("/")

        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            dir_path = root / key_prefix if key_prefix else root
            if dir_path.is_file():
                return [str(dir_path.relative_to(root))]
            if not dir_path.is_dir():
                return []
            return sorted(
                str(path.relative_to(root))
                for path in dir_path.rglob("*")
                if path.is_file()
            )
        if self._uses_s3():
            client = self._s3()
            paginator = client.get_paginator("list_objects_v2")
            return sorted(item["Key"] for page in paginator.paginate(Bucket=self.bucket, Prefix=key_prefix) for item in page.get("Contents", []))

        import gcsfs

        fs = gcsfs.GCSFileSystem()
        path = f"{self.bucket}/{key_prefix}" if key_prefix else self.bucket
        found = fs.find(path)
        return sorted(p.removeprefix(f"{self.bucket}/") for p in found)

    def list_object_snapshots(
        self,
        key_prefix: str = "",
    ) -> tuple[StorageObjectSnapshot, ...]:
        """List immutable object identities below a prefix in one traversal."""
        key_prefix = self._ensure_prefixed(key_prefix).rstrip("/")
        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            target = root / key_prefix if key_prefix else root
            if target.is_file():
                return (_local_snapshot(root, target),)
            if not target.is_dir():
                return ()
            return tuple(
                _local_snapshot(root, path)
                for path in sorted(target.rglob("*"))
                if path.is_file()
            )
        if self._uses_s3():
            return tuple(snapshot for key in self.list_prefix(key_prefix) if (snapshot := self.object_snapshot(key)) is not None)

        import gcsfs

        fs = gcsfs.GCSFileSystem()
        path = f"{self.bucket}/{key_prefix}" if key_prefix else self.bucket
        details = fs.find(path, detail=True)
        if not isinstance(details, dict):
            raise RuntimeError("GCS detailed listing did not return object metadata.")
        return tuple(
            _gcs_snapshot(self.bucket, object_path, info)
            for object_path, info in sorted(details.items())
        )

    def object_snapshot(self, key: str) -> StorageObjectSnapshot | None:
        key = self._ensure_prefixed(key)
        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            path = root / key
            return _local_snapshot(root, path) if path.is_file() else None
        if self._uses_s3():
            from botocore.exceptions import ClientError
            try:
                info = self._s3().head_object(Bucket=self.bucket, Key=key)
            except ClientError as error:
                if error.response["Error"].get("Code") in {"404", "NoSuchKey", "NotFound"}:
                    return None
                raise
            metadata = info.get("Metadata", {})
            return StorageObjectSnapshot(key, int(info["ContentLength"]), str(info.get("VersionId") or info.get("ETag", "").strip('"')), info.get("ETag", "").strip('"'), None, metadata.get("sha256"))

        from google.api_core.exceptions import NotFound
        blob = _gcs_client().bucket(self.bucket).blob(key)
        try:
            blob.reload(timeout=_GCS_DOWNLOAD_TIMEOUT_SECONDS)
        except NotFound:
            return None
        return _native_gcs_snapshot(key, blob)

    def read_bytes(
        self,
        dataset_slug: str,
        file_slug: str,
        version: str,
        key_or_filename: str,
    ) -> bytes:
        key_prefix = self.build_target_location(
            dataset_slug, file_slug, version, ""
        ).rstrip("/")
        if key_or_filename.startswith(f"{key_prefix}/"):
            key = key_or_filename
        else:
            key = f"{key_prefix}/{key_or_filename.lstrip('/')}"

        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            full = root / key
            return full.read_bytes()
        if self._uses_s3():
            return self._s3().get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return self.read_key(key)

    def download_key_to(self, key: str, destination: Path) -> None:
        """Materialize an exact object with bounded memory, including S3."""
        key = self._ensure_prefixed(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if self.use_local or not self.bucket:
            shutil.copy2(Path(self.local_dir).resolve() / key, destination)
        elif self._uses_s3():
            self._s3().download_file(self.bucket, key, str(destination))
        else:
            _gcs_client().bucket(self.bucket).blob(key).download_to_filename(
                str(destination), timeout=_GCS_DOWNLOAD_TIMEOUT_SECONDS
            )

    def upload_local_file(self, source: Path, key: str) -> str:
        """Upload bytes without buffering the full file and persist its checksum."""
        key = self._ensure_prefixed(key)
        if self.use_local or not self.bucket:
            destination = Path(self.local_dir).resolve() / key
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        elif self._uses_s3():
            with source.open("rb") as body:
                self._s3().put_object(Bucket=self.bucket, Key=key, Body=body,
                                      Metadata={"sha256": _file_sha256(source)},
                                      CacheControl="no-cache")
        else:
            import gcsfs
            gcsfs.GCSFileSystem().put(str(source), f"{self.bucket}/{key}")
        return key

    def read_key(self, key: str) -> bytes:
        """Read one exact catalog-relative key without dataset path inference."""
        key = self._ensure_prefixed(key)
        if self.use_local or not self.bucket:
            return (Path(self.local_dir).resolve() / key).read_bytes()
        if self._uses_s3():
            return self._s3().get_object(Bucket=self.bucket, Key=key)["Body"].read()
        data = _gcs_client().bucket(self.bucket).blob(key).download_as_bytes(
            timeout=_GCS_DOWNLOAD_TIMEOUT_SECONDS
        )
        if not isinstance(data, bytes):
            raise RuntimeError(f"GCS returned non-bytes content for {key}.")
        return data

    def read_key_range(
        self,
        key: str,
        offset: int,
        length: int,
        expected_snapshot: StorageObjectSnapshot | None = None,
    ) -> bytes:
        """Read a fixed object range without materializing the full object."""
        if offset < 0 or length < 0:
            raise ValueError("Range offset and length must be non-negative.")
        key = self._ensure_prefixed(key)
        before = self.object_snapshot(key)
        if before is None:
            raise FileNotFoundError(f"Storage object does not exist: {key}")
        if expected_snapshot is not None and before != expected_snapshot:
            raise RuntimeError(f"Storage object changed before range read: {key}")
        if offset + length > before.size:
            raise ValueError(f"Requested range exceeds storage object size: {key}")
        if self.use_local or not self.bucket:
            with (Path(self.local_dir).resolve() / key).open("rb") as source:
                source.seek(offset)
                data = source.read(length)
        elif self._uses_s3():
            end = offset + length - 1
            response = self._s3().get_object(
                Bucket=self.bucket,
                Key=key,
                Range=f"bytes={offset}-{end}",
            )
            body = cast(BinaryIO, response["Body"])
            with body:
                data = body.read()
        else:
            import gcsfs

            with cast(
                BinaryIO,
                gcsfs.GCSFileSystem().open(f"{self.bucket}/{key}", "rb"),
            ) as source:
                source.seek(offset)
                data = source.read(length)
        if not isinstance(data, bytes) or len(data) != length:
            raise RuntimeError(f"Storage range read returned incomplete data: {key}")
        after = self.object_snapshot(key)
        if after != before:
            raise RuntimeError(f"Storage object changed during range read: {key}")
        return data

    def sha256_key(self, key: str) -> str:
        """Hash an exact object in bounded memory when storage lacks SHA metadata."""
        key = self._ensure_prefixed(key)
        if not (self.use_local or not self.bucket) and not self._uses_s3():
            with tempfile.TemporaryDirectory(prefix="hifld-gcs-sha256-") as temporary:
                local_path = Path(temporary) / "object"
                self.download_key_to(key, local_path)
                return _file_sha256(local_path)
        if self.use_local or not self.bucket:
            stream = (Path(self.local_dir).resolve() / key).open("rb")
        else:
            stream = cast(
                BinaryIO,
                self._s3().get_object(Bucket=self.bucket, Key=key)["Body"],
            )
        digest = hashlib.sha256()
        with stream:
            while chunk := stream.read(8 * 1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()

    def object_exists(self, key: str) -> bool:
        key = self._ensure_prefixed(key)
        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            return (root / key).exists()
        if self._uses_s3():
            return self.object_snapshot(key) is not None

        return self.object_snapshot(key) is not None

    def object_content_matches(
        self,
        destination: "StagingStorageResource",
        key: str,
        destination_key: str,
    ) -> bool:
        """Return whether two existing objects contain the same bytes."""
        key = self._ensure_prefixed(key)
        destination_key = destination._ensure_prefixed(destination_key)
        if not self.object_exists(key) or not destination.object_exists(
            destination_key
        ):
            return False

        if self._uses_s3() or destination._uses_s3():
            source_snapshot = self.object_snapshot(key)
            target_snapshot = destination.object_snapshot(destination_key)
            if source_snapshot is None or target_snapshot is None or source_snapshot.size != target_snapshot.size:
                return False
            if source_snapshot.sha256 and target_snapshot.sha256:
                return source_snapshot.sha256 == target_snapshot.sha256
            with tempfile.TemporaryDirectory(prefix="hifld-compare-") as temporary:
                source_path = Path(temporary) / "source"
                target_path = Path(temporary) / "target"
                self.download_key_to(key, source_path)
                destination.download_key_to(destination_key, target_path)
                return _file_sha256(source_path) == _file_sha256(target_path)

        source_is_local = self.use_local or not self.bucket
        destination_is_local = destination.use_local or not destination.bucket
        if source_is_local and destination_is_local:
            source_path = Path(self.local_dir).resolve() / key
            destination_path = Path(destination.local_dir).resolve() / destination_key
            if source_path.stat().st_size != destination_path.stat().st_size:
                return False
            return _file_sha256(source_path) == _file_sha256(destination_path)

        if not source_is_local and not destination_is_local:
            import gcsfs

            fs = gcsfs.GCSFileSystem()
            source_info = fs.info(f"{self.bucket}/{key}")
            destination_info = fs.info(f"{destination.bucket}/{destination_key}")
            if source_info.get("size") != destination_info.get("size"):
                return False
            for checksum_key in ("md5Hash", "md5", "crc32c"):
                source_checksum = source_info.get(checksum_key)
                destination_checksum = destination_info.get(checksum_key)
                if source_checksum is not None and destination_checksum is not None:
                    return source_checksum == destination_checksum
            return False

        local_storage = self if source_is_local else destination
        remote_storage = destination if source_is_local else self
        local_key = key if source_is_local else destination_key
        remote_key = destination_key if source_is_local else key
        local_path = Path(local_storage.local_dir).resolve() / local_key

        import gcsfs

        fs = gcsfs.GCSFileSystem()
        remote_info = fs.info(f"{remote_storage.bucket}/{remote_key}")
        if local_path.stat().st_size != remote_info.get("size"):
            return False
        local_md5_base64, local_md5_hex = _file_md5(local_path)
        if any(
            checksum in {local_md5_base64, local_md5_hex}
            for checksum_key in ("md5Hash", "md5")
            if (checksum := remote_info.get(checksum_key)) is not None
        ):
            return True
        remote_crc32c = remote_info.get("crc32c")
        return remote_crc32c is not None and remote_crc32c in _file_crc32c(local_path)

    def write_key_if_unchanged(
        self,
        key: str,
        data: bytes,
        expected_snapshot: StorageObjectSnapshot | None,
    ) -> StorageObjectSnapshot:
        """Atomically create or replace one key only at the expected identity."""
        key = self._ensure_prefixed(key)
        if expected_snapshot is not None and expected_snapshot.key != key:
            raise ValueError("Expected snapshot key does not match write key.")
        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            destination = root / key
            return _atomic_local_write(destination, data, expected_snapshot, root)
        if self._uses_s3():
            client = self._s3()
            request: dict[str, object] = {"Bucket": self.bucket, "Key": key, "Body": data, "Metadata": {"sha256": hashlib.sha256(data).hexdigest()}, "ContentLength": len(data), "ContentType": "application/vnd.sqlite3" if key.endswith(".sqlite") else "application/octet-stream", "CacheControl": "no-cache"}
            if expected_snapshot is None:
                request["IfNoneMatch"] = "*"
            elif expected_snapshot.md5:
                request["IfMatch"] = expected_snapshot.md5
            else:
                raise RuntimeError(f"S3 snapshot has no ETag for conditional write: {key}")
            response = client.put_object(**request)
            return StorageObjectSnapshot(key, len(data), str(response.get("VersionId") or response.get("ETag", "").strip('"')), response.get("ETag", "").strip('"'), None, hashlib.sha256(data).hexdigest())

        import gcsfs

        fs = gcsfs.GCSFileSystem()
        expected_generation = "0"
        if expected_snapshot is not None:
            if expected_snapshot.generation is None:
                raise RuntimeError(f"GCS snapshot has no generation: {key}")
            expected_generation = expected_snapshot.generation
        result = _gcs_conditional_write(
            cast(_GCSJsonClient, cast(object, fs)),
            self.bucket,
            key,
            data,
            expected_generation,
        )
        return _gcs_snapshot(self.bucket, f"{self.bucket}/{key}", result)

    def delete_key_if_unchanged(
        self,
        key: str,
        expected_snapshot: StorageObjectSnapshot,
    ) -> None:
        """Delete one exact object only if its identity is unchanged."""
        key = self._ensure_prefixed(key)
        if expected_snapshot.key != key:
            raise ValueError("Expected snapshot key does not match delete key.")
        if self._uses_s3():
            if not expected_snapshot.md5:
                raise ValueError("S3 conditional delete requires ETag")
            self._s3().delete_object(Bucket=self.bucket, Key=key, IfMatch=expected_snapshot.md5)
            return
        if self.use_local or not self.bucket:
            current = self.object_snapshot(key)
            if current != expected_snapshot:
                raise RuntimeError(f"Destination changed before delete: {key}")
            (Path(self.local_dir).resolve() / key).unlink()
            return

        if expected_snapshot.generation is None:
            raise RuntimeError(f"GCS snapshot has no generation: {key}")
        import gcsfs

        fs = gcsfs.GCSFileSystem()
        fs.call(
            "DELETE",
            "b/{}/o/{}",
            self.bucket,
            key,
            ifGenerationMatch=expected_snapshot.generation,
        )

    def delete_prefix(self, key_prefix: str) -> None:
        key_prefix = self._ensure_prefixed(key_prefix).rstrip("/")
        if not key_prefix:
            raise ValueError("Refusing to delete an empty storage prefix.")
        if self._uses_s3():
            # Delimit the directory: `format` must not also match `format-other`.
            for key in self.list_prefix(key_prefix + "/"):
                if key.startswith(key_prefix + "/"):
                    snapshot = self.object_snapshot(key)
                    if snapshot is not None:
                        self.delete_key_if_unchanged(key, snapshot)
            return

        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            target = root / key_prefix
            if target.is_dir():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()
            return

        import gcsfs

        fs = gcsfs.GCSFileSystem()
        path = f"{self.bucket}/{key_prefix}"
        if fs.exists(path):
            fs.rm(path, recursive=True)

    def copy_key_to(
        self,
        destination: "StagingStorageResource",
        key: str,
        destination_key: str | None = None,
    ) -> str:
        """Copy one fully-qualified relative key to another storage resource."""
        key = self._ensure_prefixed(key)
        destination_key = destination._ensure_prefixed(destination_key or key)
        if self._uses_s3() or destination._uses_s3():
            with tempfile.TemporaryDirectory(prefix="hifld-copy-") as temporary:
                path = Path(temporary) / "object"
                self.download_key_to(key, path)
                return destination.upload_local_file(path, destination_key)

        if (self.use_local or not self.bucket) and (
            destination.use_local or not destination.bucket
        ):
            src = Path(self.local_dir).resolve() / key
            dst = Path(destination.local_dir).resolve() / destination_key
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            return destination_key

        import gcsfs

        if not (self.use_local or not self.bucket) and not (
            destination.use_local or not destination.bucket
        ):
            fs = gcsfs.GCSFileSystem()
            fs.copy(f"{self.bucket}/{key}", f"{destination.bucket}/{destination_key}")
            return destination_key

        if self.use_local or not self.bucket:
            fs = gcsfs.GCSFileSystem()
            src = Path(self.local_dir).resolve() / key
            fs.put(str(src), f"{destination.bucket}/{destination_key}")
            return destination_key

        fs = gcsfs.GCSFileSystem()
        dst = Path(destination.local_dir).resolve() / destination_key
        dst.parent.mkdir(parents=True, exist_ok=True)
        fs.get(f"{self.bucket}/{key}", str(dst))
        return destination_key

    def copy_key_to_if_unchanged(
        self,
        destination: "StagingStorageResource",
        key: str,
        destination_key: str,
        *,
        source_snapshot: StorageObjectSnapshot,
        destination_snapshot: StorageObjectSnapshot | None,
    ) -> StorageObjectSnapshot:
        """Copy one object with source and destination identity preconditions."""
        key = self._ensure_prefixed(key)
        destination_key = destination._ensure_prefixed(destination_key)
        if source_snapshot.key != key:
            raise ValueError("Source snapshot key does not match copy key.")
        if (
            destination_snapshot is not None
            and destination_snapshot.key != destination_key
        ):
            raise ValueError("Destination snapshot key does not match copy key.")

        source_is_local = self.use_local or not self.bucket
        destination_is_local = destination.use_local or not destination.bucket
        if self._uses_s3() or destination._uses_s3():
            if self.object_snapshot(key) != source_snapshot:
                raise RuntimeError("Source changed before conditional copy")
            data = self.read_key(key)
            if self.object_snapshot(key) != source_snapshot:
                raise RuntimeError("Source changed during conditional copy")
            return destination.write_key_if_unchanged(destination_key, data, destination_snapshot)
        if source_is_local:
            source_path = Path(self.local_dir).resolve() / key
            if self.object_snapshot(key) != source_snapshot:
                raise RuntimeError(f"source changed before copy: {key}")
            if destination_is_local:
                destination_root = Path(destination.local_dir).resolve()
                destination_path = destination_root / destination_key
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path = _copy_local_source_to_temp(source_path, destination_path)
                try:
                    if self.object_snapshot(key) != source_snapshot:
                        raise RuntimeError(f"source changed during copy: {key}")
                    return _commit_local_temp(
                        temp_path,
                        destination_path,
                        destination_snapshot,
                        destination_root,
                    )
                finally:
                    temp_path.unlink(missing_ok=True)

            if destination_snapshot is not None:
                raise RuntimeError(
                    "Conditional replacement from local storage to GCS is unsupported."
                )
            import gcsfs

            fs = gcsfs.GCSFileSystem()
            created_snapshot: StorageObjectSnapshot | None = None
            output_ref: _GCSWritable | None = None
            try:
                with (
                    source_path.open("rb") as source_file,
                    cast(
                        _GCSWritable,
                        cast(
                            object,
                            fs.open(f"{destination.bucket}/{destination_key}", "xb"),
                        ),
                    ) as output,
                ):
                    output_ref = output
                    shutil.copyfileobj(source_file, output, length=1024 * 1024)
                if output_ref is not None and isinstance(
                    output_ref.generation, (str, int)
                ):
                    created_snapshot = replace(
                        source_snapshot,
                        key=destination_key,
                        generation=str(output_ref.generation),
                    )
                if created_snapshot is None:
                    raise RuntimeError(
                        f"GCS create returned no generation: {destination_key}"
                    )
                if self.object_snapshot(key) != source_snapshot:
                    raise RuntimeError(f"source changed during copy: {key}")
                return created_snapshot
            except Exception as copy_error:
                if created_snapshot is None:
                    raise
                try:
                    destination.delete_key_if_unchanged(
                        destination_key, created_snapshot
                    )
                except Exception as cleanup_error:
                    raise RuntimeError(
                        f"Source changed after GCS copy and cleanup failed for "
                        f"{destination_key}: {cleanup_error}"
                    ) from copy_error
                raise

        if source_snapshot.generation is None:
            raise RuntimeError(f"GCS snapshot has no generation: {key}")
        source_bucket = self.bucket
        if source_bucket is None:
            raise RuntimeError("GCS source bucket is not configured.")
        import gcsfs

        if not destination_is_local:
            fs = gcsfs.GCSFileSystem()
            destination_bucket = destination.bucket
            if destination_bucket is None:
                raise RuntimeError("GCS destination bucket is not configured.")
            destination_generation = "0"
            if destination_snapshot is not None:
                if destination_snapshot.generation is None:
                    raise RuntimeError(
                        f"GCS snapshot has no generation: {destination_key}"
                    )
                destination_generation = destination_snapshot.generation
            result = _gcs_conditional_copy(
                cast(_GCSJsonClient, cast(object, fs)),
                source_bucket,
                key,
                destination_bucket,
                destination_key,
                source_snapshot.generation,
                destination_generation,
            )
            return _gcs_snapshot(
                destination_bucket,
                f"{destination_bucket}/{destination_key}",
                result,
            )

        destination_root = Path(destination.local_dir).resolve()
        fs = gcsfs.GCSFileSystem(version_aware=True)
        destination_path = destination_root / destination_key
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(dir=destination_path.parent)
        os.close(fd)
        temp_path = Path(temp_name)
        try:
            with cast(
                BinaryIO,
                cast(
                    object,
                    fs.open(
                        f"{source_bucket}/{key}#{source_snapshot.generation}",
                        "rb",
                    ),
                ),
            ) as source_file:
                with temp_path.open("wb") as output:
                    shutil.copyfileobj(source_file, output, length=1024 * 1024)
            return _commit_local_temp(
                temp_path,
                destination_path,
                destination_snapshot,
                destination_root,
            )
        finally:
            temp_path.unlink(missing_ok=True)

    def copy_keys_to(
        self,
        destination: "StagingStorageResource",
        keys: list[str],
        *,
        destination_keys: list[str] | None = None,
        max_workers: int | None = None,
    ) -> list[str]:
        """Copy many relative keys, using concurrent server-side object copies for GCS."""
        if not keys:
            return []
        if destination_keys is None:
            destination_keys = keys
        if len(destination_keys) != len(keys):
            raise ValueError("Source and destination key counts must match.")
        key_pairs = list(zip(keys, destination_keys, strict=True))

        if max_workers is None:
            max_workers = int(os.environ.get("HIFLD_PROMOTE_COPY_WORKERS", "32"))
        max_workers = max(1, min(max_workers, len(keys)))

        if max_workers == 1:
            return [
                self.copy_key_to(destination, key, destination_key)
                for key, destination_key in key_pairs
            ]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(
                executor.map(
                    lambda pair: self.copy_key_to(destination, pair[0], pair[1]),
                    key_pairs,
                )
            )

    def key_size(self, key: str) -> int:
        if self._uses_s3():
            snapshot = self.object_snapshot(key)
            return snapshot.size if snapshot is not None else 0
        key = self._ensure_prefixed(key)
        if self.use_local or not self.bucket:
            path = Path(self.local_dir).resolve() / key
            return path.stat().st_size if path.exists() else 0

        import gcsfs

        fs = gcsfs.GCSFileSystem()
        try:
            info = fs.info(f"{self.bucket}/{key}")
        except FileNotFoundError:
            return 0
        return int(info.get("size", 0) or 0)

    def list_versions(self, dataset_slug: str, file_slug: str) -> list[str]:
        dataset_prefix = self._apply_prefix(f"{dataset_slug}/{file_slug}").rstrip("/")
        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            dir_path = root / dataset_prefix
            if not dir_path.is_dir():
                return []
            return sorted(p.name for p in dir_path.iterdir() if p.is_dir())

        import gcsfs

        fs = gcsfs.GCSFileSystem()
        path = f"{self.bucket}/{dataset_prefix}"
        if not fs.exists(path):
            return []
        entries = fs.ls(path, detail=True)

        versions: list[str] = []
        for entry in entries:
            entry_name = entry.get("name", "").rstrip("/")
            if not entry_name:
                continue
            if entry.get("type") == "directory":
                versions.append(entry_name.rsplit("/", 1)[-1])
                continue
            suffix = entry_name.removeprefix(f"{self.bucket}/{dataset_prefix}/")
            if "/" in suffix and suffix:
                versions.append(suffix.split("/", 1)[0])
        return sorted(set(versions))

    @contextmanager
    def get_local_version_dir(
        self,
        dataset_slug: str,
        file_slug: str,
        version: str,
        *,
        include_directories: tuple[str, ...] | None = None,
    ):
        """Yield a local directory path containing the version's files (for fiona/chunked read).
        For local storage returns the actual path; for GCS materializes to a temp dir and cleans up on exit.
        ``include_directories`` limits remote materialization to named first-level
        version directories while retaining their relative layout.
        """
        key_prefix = self.build_target_location(
            dataset_slug, file_slug, version, ""
        ).rstrip("/")
        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            yield root / key_prefix
            return
        tmp = Path(tempfile.mkdtemp(prefix="hifld_staging_"))
        try:
            if self._uses_s3():
                prefix_with_slash = key_prefix + "/"
                client = self._s3()
                for key in self.list_keys(dataset_slug, file_slug, version):
                    rel = key.removeprefix(prefix_with_slash)
                    if not rel:
                        continue
                    if not _included_version_directory(rel, include_directories):
                        continue
                    dest = tmp / rel
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    client.download_file(self.bucket, key, str(dest))
                yield tmp
                return
            key_prefix = self.build_target_location(
                dataset_slug, file_slug, version, ""
            ).rstrip("/")
            prefix_with_slash = key_prefix + "/"
            keys = self.list_keys(dataset_slug, file_slug, version)
            for key in keys:
                # Preserve subdir structure (e.g. shapefile/, metadata/, pmtiles/)
                rel = key.removeprefix(prefix_with_slash)
                if not rel:
                    continue
                if not _included_version_directory(rel, include_directories):
                    continue
                dest = tmp / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                self.download_key_to(key, dest)
            yield tmp
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class GreatExpectationsResource(ConfigurableResource):
    """Optional GX helper for ad hoc validation experiments; not wired into the live graph."""

    def get_validator(self, df, expectation_suite_name: str = "asset_check_suite"):
        """Return a GX Validator for the given DataFrame (e.g. GeoDataFrame or first chunk)."""
        import great_expectations as gx

        ctx = gx.get_context(mode="ephemeral")
        ds = ctx.data_sources.add_pandas("pandas_ds")
        batch = ds.read_dataframe(df, asset_name="df_asset")
        return ctx.get_validator(
            batch_list=[batch],
            create_expectation_suite_with_name=expectation_suite_name,
        )


class PublishedStorageResource(StagingStorageResource):
    """Published storage: destination bucket/path for promoted dataset files."""

    @classmethod
    def from_env(cls) -> "PublishedStorageResource":
        bucket = os.environ.get("HIFLD_DATASETS_BUCKET") or None
        local_dir = os.environ.get("HIFLD_DATASETS_DIR", "data/published")
        backend = os.environ.get("HIFLD_DATASETS_BACKEND", "gcs")
        return cls(bucket=bucket, prefix=os.environ.get("HIFLD_DATASETS_PREFIX", "").strip("/"), use_local=not bool(bucket), local_dir=local_dir, backend=backend, s3_endpoint_url=os.environ.get("HIFLD_DATASETS_S3_ENDPOINT"), s3_access_key_id=os.environ.get("HIFLD_DATASETS_S3_ACCESS_KEY"), s3_secret_access_key=os.environ.get("HIFLD_DATASETS_S3_SECRET_KEY"), s3_region=os.environ.get("HIFLD_DATASETS_S3_REGION", "us-east-1"), s3_use_ssl=os.environ.get("HIFLD_DATASETS_S3_USE_SSL", "false").lower() == "true")


def _included_version_directory(
    relative_key: str,
    include_directories: tuple[str, ...] | None,
) -> bool:
    if include_directories is None:
        return True
    return relative_key.split("/", 1)[0] in include_directories


def _local_snapshot(root: Path, path: Path) -> StorageObjectSnapshot:
    stat = path.stat()
    sha256, md5, crc32c = _file_checksums(path)
    return StorageObjectSnapshot(
        key=str(path.relative_to(root)),
        size=stat.st_size,
        generation=(f"local:{stat.st_dev}:{stat.st_ino}:{stat.st_mtime_ns}"),
        md5=md5,
        crc32c=crc32c,
        sha256=sha256,
    )


def _gcs_snapshot(
    bucket: str,
    object_path: str,
    info: dict[str, object],
) -> StorageObjectSnapshot:
    key = object_path.removeprefix(f"{bucket}/")
    size_value = info.get("size", 0)
    if not isinstance(size_value, (str, int)):
        raise RuntimeError(f"GCS object has invalid size metadata: {key}")
    generation = info.get("generation")
    md5 = info.get("md5Hash", info.get("md5"))
    crc32c = info.get("crc32c")
    return StorageObjectSnapshot(
        key=key,
        size=int(size_value or 0),
        generation=str(generation) if generation is not None else None,
        md5=str(md5) if md5 is not None else None,
        crc32c=str(crc32c) if crc32c is not None else None,
    )


@cache
def _gcs_client() -> storage.Client:
    """Construct an ADC client without repeatedly probing the Cloud SDK project."""
    return storage.Client(project=os.environ.get("GOOGLE_CLOUD_PROJECT"))


def _native_gcs_snapshot(key: str, blob: Blob) -> StorageObjectSnapshot:
    """Normalize native client metadata into the storage identity contract."""
    if not isinstance(blob.size, int):
        raise RuntimeError(f"GCS object has invalid size metadata: {key}")
    metadata = blob.metadata or {}
    return StorageObjectSnapshot(
        key=key,
        size=blob.size,
        generation=str(blob.generation) if blob.generation is not None else None,
        md5=blob.md5_hash,
        crc32c=blob.crc32c,
        sha256=metadata.get("sha256"),
    )


def _copy_local_source_to_temp(source: Path, destination: Path) -> Path:
    fd, temp_name = tempfile.mkstemp(dir=destination.parent)
    os.close(fd)
    temp_path = Path(temp_name)
    with source.open("rb") as source_file:
        with temp_path.open("wb") as output:
            shutil.copyfileobj(source_file, output, length=1024 * 1024)
    return temp_path


def _atomic_local_write(
    destination: Path,
    data: bytes,
    expected_snapshot: StorageObjectSnapshot | None,
    root: Path,
) -> StorageObjectSnapshot:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(dir=destination.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        return _commit_local_temp(temp_path, destination, expected_snapshot, root)
    finally:
        temp_path.unlink(missing_ok=True)


def _commit_local_temp(
    temp_path: Path,
    destination: Path,
    expected_snapshot: StorageObjectSnapshot | None,
    root: Path,
) -> StorageObjectSnapshot:
    current = _local_snapshot(root, destination) if destination.is_file() else None
    if current != expected_snapshot:
        state = "created" if expected_snapshot is None else "changed"
        raise RuntimeError(
            f"destination {state} before copy: {destination.relative_to(root)}"
        )
    committed = replace(
        _local_snapshot(root, temp_path),
        key=str(destination.relative_to(root)),
    )
    if expected_snapshot is None:
        try:
            os.link(temp_path, destination)
        except FileExistsError as exc:
            raise RuntimeError(
                f"destination created during copy: {destination.relative_to(root)}"
            ) from exc
        return committed
    os.replace(temp_path, destination)
    return committed


def _gcs_conditional_copy(
    fs: _GCSJsonClient,
    source_bucket: str,
    source_key: str,
    destination_bucket: str,
    destination_key: str,
    source_generation: str,
    destination_generation: str,
) -> dict[str, object]:
    request = {
        "headers": {"Content-Type": "application/json"},
        "ifSourceGenerationMatch": source_generation,
        "ifGenerationMatch": destination_generation,
        "json_out": True,
    }
    result = fs.call(
        "POST",
        "b/{}/o/{}/rewriteTo/b/{}/o/{}",
        source_bucket,
        source_key,
        destination_bucket,
        destination_key,
        **request,
    )
    while result["done"] is not True:
        result = fs.call(
            "POST",
            "b/{}/o/{}/rewriteTo/b/{}/o/{}",
            source_bucket,
            source_key,
            destination_bucket,
            destination_key,
            rewriteToken=result["rewriteToken"],
            **request,
        )
    resource = result.get("resource")
    return _require_string_object_mapping(resource, "GCS rewrite resource")


def _gcs_conditional_write(
    fs: _GCSJsonClient,
    bucket: str,
    key: str,
    data: bytes,
    expected_generation: str,
) -> dict[str, object]:
    boundary = "hifld-conditional-upload"
    metadata = json.dumps(
        {
            "name": key,
            "cacheControl": "no-cache",
            "metadata": {"sha256": hashlib.sha256(data).hexdigest()},
        },
        separators=(",", ":"),
    )
    payload = (
        (
            f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
            f"{metadata}\r\n--{boundary}\r\n"
            "Content-Type: application/octet-stream\r\n\r\n"
        ).encode()
        + data
        + f"\r\n--{boundary}--".encode()
    )
    location = fs._location
    result = fs.call(
        "POST",
        f"{location}/upload/storage/v1/b/{{}}/o",
        bucket,
        uploadType="multipart",
        ifGenerationMatch=expected_generation,
        headers={"Content-Type": f'multipart/related; boundary="{boundary}"'},
        data=payload,
        json_out=True,
    )
    return _require_string_object_mapping(result, "GCS upload resource")


def _require_string_object_mapping(
    value: object,
    context: str,
) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise RuntimeError(f"{context} was not an object.")
    return {key: item for key, item in value.items() if isinstance(key, str)}


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_checksums(path: Path) -> tuple[str, str, str]:
    sha256 = hashlib.sha256()
    md5 = hashlib.md5(usedforsecurity=False)
    crc32c = google_crc32c.Checksum()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            sha256.update(chunk)
            md5.update(chunk)
            crc32c.update(chunk)
    return (
        sha256.hexdigest(),
        base64.b64encode(md5.digest()).decode("ascii"),
        base64.b64encode(crc32c.digest()).decode("ascii"),
    )


def _file_md5(path: Path) -> tuple[str, str]:
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return base64.b64encode(digest.digest()).decode("ascii"), digest.hexdigest()


def _file_crc32c(path: Path) -> tuple[str, str]:
    checksum = google_crc32c.Checksum()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            checksum.update(chunk)
    digest = checksum.digest()
    return base64.b64encode(digest).decode("ascii"), digest.hex()


def snapshots_content_match(
    source: StorageObjectSnapshot,
    destination: StorageObjectSnapshot,
) -> bool:
    if source.size != destination.size:
        return False
    for source_checksum, destination_checksum in (
        (source.sha256, destination.sha256),
        (source.md5, destination.md5),
        (source.crc32c, destination.crc32c),
    ):
        if source_checksum is not None and destination_checksum is not None:
            return source_checksum == destination_checksum
    return False


class DatasetApiResource(ConfigurableResource):
    """Dataset API client for promotion and quality comparison."""

    base_url: str = ""
    collection_id: int = 1
    timeout_seconds: float = 120.0
    api_token: str = ""
    auth_header_name: str = "Authorization"

    @classmethod
    def from_env(cls) -> "DatasetApiResource":
        base_url = os.environ.get("DATASET_API_URL", "").rstrip("/")
        collection_id = int(os.environ.get("DATASET_API_COLLECTION_ID", "1"))
        return cls(
            base_url=base_url,
            collection_id=collection_id,
            api_token=os.environ.get("DATASET_API_TOKEN", ""),
            auth_header_name=os.environ.get("DATASET_API_AUTH_HEADER", "Authorization"),
        )

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    def _headers(self) -> dict[str, str]:
        if not self.api_token:
            return {}
        if self.auth_header_name.lower() == "authorization":
            return {"Authorization": f"Bearer {self.api_token}"}
        return {self.auth_header_name: self.api_token}

    def upsert_dataset_version(
        self,
        dataset_slug: str,
        version: str,
        storage_location_name: str,
        files: list[dict],
        overwrite_existing: bool = False,
    ) -> dict | None:
        if not self.enabled:
            return None
        url = (
            f"{self.base_url}/api/collections/{self.collection_id}/datasets/"
            f"by-slug/{dataset_slug}/versions"
        )
        payload = {
            "version": version,
            "storage_location_name": storage_location_name,
            "files": files,
            "overwrite_existing": overwrite_existing,
        }
        with httpx.Client(
            timeout=self.timeout_seconds, headers=self._headers()
        ) as client:
            resp = client.post(url, json=payload)
            resp.raise_for_status()
            return resp.json()

    def get_dataset_quality(
        self,
        dataset_slug: str,
        file_slug: str | None = None,
        compute_if_missing: bool = True,
    ) -> dict | None:
        if not self.enabled:
            return None
        url = (
            f"{self.base_url}/api/collections/{self.collection_id}/datasets/"
            f"by-slug/{dataset_slug}/quality"
        )
        params: dict[str, str] = {
            "compute_if_missing": "true" if compute_if_missing else "false",
        }
        if file_slug:
            params["file_slug"] = file_slug
        with httpx.Client(
            timeout=self.timeout_seconds, headers=self._headers()
        ) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()

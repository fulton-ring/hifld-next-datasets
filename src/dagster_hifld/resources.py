"""Dagster resources for staging/published storage and optional API utilities."""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import httpx
from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


class StagingStorageResource(ConfigurableResource):
    """Staging storage: writes to GCS bucket or local path.

    - If HIFLD_STAGING_BUCKET is set: use GCS (requires gcsfs and credentials).
    - Else: use local directory HIFLD_STAGING_DIR or ./data/staging.
    """

    bucket: str | None = None
    prefix: str = ""
    use_local: bool = True
    local_dir: str = ""

    @classmethod
    def from_env(cls) -> "StagingStorageResource":
        bucket = os.environ.get("HIFLD_STAGING_BUCKET") or None
        local_dir = os.environ.get("HIFLD_STAGING_DIR", "data/staging")
        return cls(bucket=bucket, use_local=not bool(bucket), local_dir=local_dir)

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

        import gcsfs
        fs = gcsfs.GCSFileSystem()
        path = f"{self.bucket}/{key}"
        fs.write_bytes(path, data)
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
        key_prefix = self.build_target_location(dataset_slug, file_slug, version, "").rstrip("/")

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

        import gcsfs
        fs = gcsfs.GCSFileSystem()
        path = f"{self.bucket}/{key_prefix}" if key_prefix else self.bucket
        found = fs.find(path)
        return sorted(p.removeprefix(f"{self.bucket}/") for p in found)

    def read_bytes(
        self,
        dataset_slug: str,
        file_slug: str,
        version: str,
        key_or_filename: str,
    ) -> bytes:
        key_prefix = self.build_target_location(dataset_slug, file_slug, version, "").rstrip("/")
        if key_or_filename.startswith(f"{key_prefix}/"):
            key = key_or_filename
        else:
            key = f"{key_prefix}/{key_or_filename.lstrip('/')}"

        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            full = root / key
            return full.read_bytes()

        import gcsfs
        fs = gcsfs.GCSFileSystem()
        path = f"{self.bucket}/{key}"
        return fs.read_bytes(path)

    def object_exists(self, key: str) -> bool:
        key = self._ensure_prefixed(key)
        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            return (root / key).exists()

        import gcsfs

        fs = gcsfs.GCSFileSystem()
        return bool(fs.exists(f"{self.bucket}/{key}"))

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

        return False

    def delete_prefix(self, key_prefix: str) -> None:
        key_prefix = self._ensure_prefixed(key_prefix).rstrip("/")
        if not key_prefix:
            raise ValueError("Refusing to delete an empty storage prefix.")

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

        if (self.use_local or not self.bucket) and (destination.use_local or not destination.bucket):
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
    def get_local_version_dir(self, dataset_slug: str, file_slug: str, version: str):
        """Yield a local directory path containing the version's files (for fiona/chunked read).
        For local storage returns the actual path; for GCS materializes to a temp dir and cleans up on exit.
        """
        key_prefix = self.build_target_location(dataset_slug, file_slug, version, "").rstrip("/")
        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            yield root / key_prefix
            return
        tmp = Path(tempfile.mkdtemp(prefix="hifld_staging_"))
        try:
            import gcsfs
            fs = gcsfs.GCSFileSystem()
            key_prefix = self.build_target_location(dataset_slug, file_slug, version, "").rstrip("/")
            prefix_with_slash = key_prefix + "/"
            keys = self.list_keys(dataset_slug, file_slug, version)
            for key in keys:
                # Preserve subdir structure (e.g. shapefile/, metadata/, pmtiles/)
                rel = key.removeprefix(prefix_with_slash)
                if not rel:
                    continue
                dest = tmp / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                self._copy_gcs_key_to_path(fs, key, dest)
            yield tmp
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def _copy_gcs_key_to_path(self, fs, key: str, dest: Path) -> None:
        """Stream a GCS object to local disk without loading it all into memory."""
        with fs.open(f"{self.bucket}/{key}", "rb") as source:
            with dest.open("wb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)


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
        return cls(bucket=bucket, use_local=not bool(bucket), local_dir=local_dir)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
        with httpx.Client(timeout=self.timeout_seconds, headers=self._headers()) as client:
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
        with httpx.Client(timeout=self.timeout_seconds, headers=self._headers()) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()

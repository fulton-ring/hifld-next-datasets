"""Dagster resources for staging/published storage and optional API utilities."""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
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

        if self.use_local or not self.bucket:
            root = Path(self.local_dir).resolve()
            dir_path = root / key_prefix
            if not dir_path.is_dir():
                return []
            # rglob to include files in format subdirectories.
            return [str(p.relative_to(root)) for p in dir_path.rglob("*") if p.is_file()]

        import gcsfs
        fs = gcsfs.GCSFileSystem()
        path = f"{self.bucket}/{key_prefix}"
        if not fs.exists(path):
            return []
        # fs.find() recurses into all subdirectories
        found = fs.find(path)
        return [p.replace(f"{self.bucket}/", "") for p in found]

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
                data = fs.read_bytes(f"{self.bucket}/{key}")
                dest.write_bytes(data)
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
        return cls(bucket=bucket, use_local=not bool(bucket), local_dir=local_dir)


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

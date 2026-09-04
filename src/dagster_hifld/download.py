"""Download and stage raw geospatial datasets."""

from __future__ import annotations

import io
import logging
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx

from dagster_hifld.resources import StagingStorageResource

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

_GEO_PRIORITY = [".shp", ".gpkg", ".gdb", ".geojson", ".json", ".fgb", ".parquet"]

_FORMAT_NAMES: dict[str, str] = {
    ".shp": "shapefile",
    ".gpkg": "geopackage",
    ".gdb": "file_geodatabase",
    ".geojson": "geojson",
    ".json": "geojson",
    ".fgb": "flatgeobuf",
    ".parquet": "geoparquet",
}


def get_run_id(context) -> str | None:
    """Get run_id from OpExecutionContext or AssetCheckExecutionContext."""
    if hasattr(context, "op_execution_context"):
        return getattr(context.op_execution_context, "run_id", None)
    return getattr(context, "run_id", None)


def _format_version_id(timestamp: datetime) -> str:
    return timestamp.astimezone(timezone.utc).strftime("v%Y%m%dT%H%M%SZ")


def build_version_id(context=None) -> str:
    """Build a stable, time-based version ID for this materialization."""
    run_id = get_run_id(context) if context is not None and not isinstance(context, str) else None
    instance = getattr(context, "instance", None) if context is not None and not isinstance(context, str) else None

    if run_id:
        if instance is None:
            raise ValueError("Dagster context must provide an instance to build a stable version ID.")
        run_record = instance.get_run_record_by_id(run_id)
        if run_record is None:
            raise ValueError(f"Could not resolve Dagster run record for run_id={run_id}.")
        return _format_version_id(run_record.create_timestamp)

    if isinstance(context, datetime):
        return _format_version_id(context)

    return _format_version_id(datetime.now(timezone.utc))


def _key_prefix(
    staging: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> str:
    return staging.build_target_location(dataset_slug, file_slug, version, "").rstrip("/")


def _filename_from_response(url: str, response: httpx.Response, suggested: str | None) -> str:
    cd = response.headers.get("content-disposition")
    if cd and "filename=" in cd:
        part = cd.split("filename=")[-1].strip().strip("\"'")
        if part:
            return part
    path = urlparse(url).path
    if path and path.rstrip("/"):
        return path.rstrip("/").rsplit("/", 1)[-1]
    return suggested or "download"


def _extract_zip(content: bytes, out_dir: Path) -> list[Path]:
    extracted: list[Path] = []
    with zipfile.ZipFile(io.BytesIO(content), "r") as zf:
        for info in zf.infolist():
            member_path = Path(info.filename)
            if member_path.is_absolute() or ".." in member_path.parts or info.filename.startswith(("/", "\\")):
                raise ValueError(f"Unsafe zip member path: {info.filename}")
            if info.is_dir():
                (out_dir / member_path).mkdir(parents=True, exist_ok=True)
                continue
            dest = out_dir / member_path
            dest.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, "r") as src, dest.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            extracted.append(dest)
    return extracted


def _pick_primary_file(candidates: list[Path]) -> Path | None:
    """Pick the best primary geospatial source from extracted files."""
    # .gdb is a directory; find the deepest one
    gdb_dirs: set[Path] = set()
    for p in candidates:
        for parent in p.parents:
            if parent.suffix.lower() == ".gdb":
                gdb_dirs.add(parent)
    if gdb_dirs:
        return min(gdb_dirs, key=lambda d: len(d.parts))
    for ext in _GEO_PRIORITY:
        for p in candidates:
            if p.suffix.lower() == ext and p.is_file():
                return p
    return None


def _collect_staging_files(primary: Path) -> list[tuple[Path, str]]:
    """Return (local_path, relative_key) pairs for all files belonging to this format.

    The relative_key is relative to the format subdirectory, so callers prepend
    ``{format_name}/`` to get the full staging key suffix.

    - GDB (directory): every file inside, preserving the directory name.
    - Shapefile: the .shp plus all standard sidecar files.
    - Any other single file: just the file itself.
    """
    if primary.is_dir():
        result = []
        for p in sorted(primary.rglob("*")):
            if p.is_file():
                rel = p.relative_to(primary.parent)  # e.g. Stations.gdb/a00000001.gdbtable
                result.append((p, str(rel)))
        return result

    files: list[tuple[Path, str]] = [(primary, primary.name)]
    if primary.suffix.lower() == ".shp":
        for ext in [".shx", ".dbf", ".prj", ".cpg", ".sbn", ".sbx", ".qpj"]:
            sib = primary.parent / f"{primary.stem}{ext}"
            if sib.exists():
                files.append((sib, sib.name))
    return files


def _safe_layer_name(name: str) -> str:
    """Sanitise a layer name for use in file names (matches process_gcs_datasets._safe_layer_suffix)."""
    return name.replace("/", "-").replace("\\", "-").replace(" ", "_")


def download_convert_and_stage(
    urls_and_names: list[tuple[str, str | None]],
    dataset_slug: str,
    file_slug: str,
    version: str,
    staging_storage: StagingStorageResource,
) -> dict:
    """Download all available source formats and write only extracted source files to staging."""
    prefix = _key_prefix(staging_storage, dataset_slug, file_slug, version)

    with tempfile.TemporaryDirectory(prefix="hifld_dl_") as _tmp:
        tmp = Path(_tmp)
        raw_dir = tmp / "raw"
        raw_dir.mkdir()

        written: list[str] = []

        with httpx.Client(follow_redirects=True, timeout=120.0) as client:
            for i, (url, name) in enumerate(urls_and_names):
                resp = client.get(url)
                resp.raise_for_status()
                content = resp.content
                filename = name or _filename_from_response(url, resp, "download")

                dl_dir = raw_dir / f"dl_{i}"
                dl_dir.mkdir()

                if len(content) >= 4 and content[:4] == b"PK\x03\x04":
                    extracted = _extract_zip(content, dl_dir)
                else:
                    raw_path = dl_dir / filename
                    raw_path.write_bytes(content)
                    extracted = [raw_path]

                dl_primary = _pick_primary_file(extracted)
                if dl_primary is None:
                    logger.warning("No geospatial file found in download: %s", filename)
                    continue

                ext = dl_primary.suffix.lower() if dl_primary.is_file() else ".gdb"
                format_name = _FORMAT_NAMES.get(ext, "source")

                # Stage extracted files under {format_name}/ (mirrors bucket layout)
                for local_path, rel_key in _collect_staging_files(dl_primary):
                    key = f"{prefix}/{format_name}/{rel_key}"
                    staging_storage.write_key(key, local_path.read_bytes())
                    written.append(key)
                    logger.info("Staged %s", key)

        if not written:
            raise ValueError(f"No readable geospatial file found for {dataset_slug}/{file_slug}.")

        return {
            "dataset_slug": dataset_slug,
            "file_slug": file_slug,
            "version": version,
            "written_keys": written,
        }

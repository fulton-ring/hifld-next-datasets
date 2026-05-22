"""One-off source_manifest.json seeding from the legacy Dataset API JSONL."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

SOURCE_FORMAT_DIRS = {
    "file_geodatabase",
    "geojson",
    "geopackage",
    "unknown",
}
SEMVER_VERSION_RE = re.compile(r"^v\d+\.\d+\.\d+$")


@dataclass
class SeedSourceManifestResult:
    discovered_files: int = 0
    to_write: int = 0
    written: int = 0
    skipped_existing: int = 0
    missing_dataset_metadata: int = 0
    entries: list[dict[str, Any]] = field(default_factory=list)


def seed_source_manifests(
    staging_root: str,
    datasets_jsonl: str | Path,
    *,
    dry_run: bool = True,
    overwrite: bool = False,
) -> SeedSourceManifestResult:
    """Seed file-level source manifests for every staged dataset/file pair."""
    dataset_index = load_datasets_jsonl(datasets_jsonl)
    storage = _ManifestStorage(staging_root)
    existing_manifest_keys = set(storage.iter_existing_manifest_keys())
    result = SeedSourceManifestResult()

    for dataset_slug, file_slug in sorted(_iter_staged_files(storage)):
        result.discovered_files += 1
        manifest = build_manifest_for_staged_file(dataset_slug, file_slug, dataset_index)
        if not manifest:
            result.missing_dataset_metadata += 1
            continue

        key = f"{dataset_slug}/{file_slug}/metadata/source_manifest.json"
        entry = {
            "dataset_slug": dataset_slug,
            "file_slug": file_slug,
            "key": key,
            "manifest": manifest,
        }
        if key in existing_manifest_keys and not overwrite:
            result.skipped_existing += 1
            entry["action"] = "skip_existing"
            result.entries.append(entry)
            continue

        result.to_write += 1
        entry["action"] = "write" if not dry_run else "dry_run"
        result.entries.append(entry)
        if dry_run:
            continue

        storage.write_json(key, manifest)
        existing_manifest_keys.add(key)
        result.written += 1

    return result


def seed_dataset_source_manifests(
    staging_root: str,
    datasets_jsonl: str | Path,
    *,
    dry_run: bool = True,
    overwrite: bool = False,
) -> SeedSourceManifestResult:
    """Seed dataset-level source manifests for every staged top-level dataset."""
    dataset_index = load_datasets_jsonl(datasets_jsonl)
    storage = _ManifestStorage(staging_root)
    existing_manifest_keys = set(storage.iter_existing_manifest_keys())
    result = SeedSourceManifestResult()

    staged_files_by_dataset: dict[str, list[str]] = {}
    for dataset_slug, file_slug in _iter_staged_files(storage):
        staged_files_by_dataset.setdefault(dataset_slug, []).append(file_slug)

    for dataset_slug, file_slugs in sorted(staged_files_by_dataset.items()):
        result.discovered_files += 1
        manifest = build_manifest_for_staged_dataset(
            dataset_slug,
            sorted(set(file_slugs)),
            dataset_index,
        )
        if not manifest:
            result.missing_dataset_metadata += 1
            continue

        key = f"{dataset_slug}/metadata/source_manifest.json"
        entry = {
            "dataset_slug": dataset_slug,
            "key": key,
            "manifest": manifest,
        }
        if key in existing_manifest_keys and not overwrite:
            result.skipped_existing += 1
            entry["action"] = "skip_existing"
            result.entries.append(entry)
            continue

        result.to_write += 1
        entry["action"] = "write" if not dry_run else "dry_run"
        result.entries.append(entry)
        if dry_run:
            continue

        storage.write_json(key, manifest)
        existing_manifest_keys.add(key)
        result.written += 1

    return result


def load_datasets_jsonl(path: str | Path) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    with Path(path).open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            slug = _slug(row.get("slug", ""))
            if slug:
                index[slug] = row
    return index


def build_manifest_for_staged_dataset(
    dataset_slug: str,
    file_slugs: Iterable[str],
    dataset_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    exact_row = dataset_index.get(_slug(dataset_slug))
    if exact_row:
        return _manifest_from_dataset_row(exact_row, fallback_title=dataset_slug)

    rows = [
        row
        for file_slug in file_slugs
        if (row := _match_dataset_row(dataset_slug, file_slug, dataset_index))
    ]
    if not rows:
        return {}

    categories: list[str] = []
    for row in rows:
        tags = row.get("tags") or {}
        for category in tags.get("categories") or []:
            if category not in categories:
                categories.append(category)

    description = _most_common_non_empty(row.get("description") for row in rows)
    tags = {"inventory_name": dataset_slug}
    if categories:
        tags["categories"] = categories

    return _compact_manifest(
        {
            "title": _title_from_slug(dataset_slug),
            "description": description,
            "tags": tags,
        }
    )


def build_manifest_for_staged_file(
    dataset_slug: str,
    file_slug: str,
    dataset_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    row = _match_dataset_row(dataset_slug, file_slug, dataset_index)
    if not row:
        return {}

    file_row = _match_file_row(file_slug, row.get("files") or [])
    if file_row:
        title = file_row.get("name") or row.get("name") or file_slug
        description = file_row.get("description") or row.get("description")
    else:
        title = row.get("name") if _slug(file_slug) == _slug(row.get("slug", "")) else file_slug
        description = row.get("description")

    return _compact_manifest(
        {
            "title": title,
            "description": description,
            "tags": row.get("tags") or {},
        }
    )


def _manifest_from_dataset_row(
    row: dict[str, Any],
    *,
    fallback_title: str,
) -> dict[str, Any]:
    return _compact_manifest(
        {
            "title": row.get("name") or fallback_title,
            "description": row.get("description"),
            "tags": row.get("tags") or {},
        }
    )


def _match_dataset_row(
    dataset_slug: str,
    file_slug: str,
    dataset_index: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    for candidate in (_slug(dataset_slug), _slug(file_slug)):
        row = dataset_index.get(candidate)
        if row:
            return row

    normalized_file_slug = _slug(file_slug)
    stripped_file_slug = _strip_format_words(normalized_file_slug)
    for row in dataset_index.values():
        if _match_file_row(normalized_file_slug, row.get("files") or []):
            return row
        row_slug = _strip_format_words(_slug(row.get("slug", "")))
        if row_slug == stripped_file_slug:
            return row
    return None


def _match_file_row(file_slug: str, files: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    normalized_file_slug = _slug(file_slug)
    stripped_file_slug = _strip_format_words(normalized_file_slug)
    for file_row in files:
        candidates = {
            _slug(str(file_row.get("slug") or "")),
            _slug(str(file_row.get("name") or "")),
        }
        candidates |= {_strip_format_words(candidate) for candidate in candidates if candidate}
        if normalized_file_slug in candidates or stripped_file_slug in candidates:
            return file_row
    return None


def _iter_staged_files(storage: "_ManifestStorage") -> Iterable[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    for key in storage.iter_object_keys():
        parts = [part for part in key.split("/") if part]
        if len(parts) < 5:
            continue
        dataset_slug, file_slug, version, format_dir = parts[:4]
        if not SEMVER_VERSION_RE.match(version):
            continue
        if format_dir not in SOURCE_FORMAT_DIRS:
            continue
        pair = (dataset_slug, file_slug)
        if pair in seen:
            continue
        seen.add(pair)
        yield pair


def _compact_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in manifest.items()
        if value not in (None, "", [], {})
    }


def _most_common_non_empty(values: Iterable[Any]) -> Any:
    counts: dict[Any, int] = {}
    for value in values:
        if value in (None, "", [], {}):
            continue
        counts[value] = counts.get(value, 0) + 1
    if not counts:
        return None
    return sorted(counts.items(), key=lambda item: (-item[1], str(item[0])))[0][0]


def _title_from_slug(value: str) -> str:
    upper_acronyms = {"nfhl", "nhd", "nmg", "wbd"}
    slug = _slug(value)
    if slug in upper_acronyms:
        return slug.upper()
    return " ".join(part.upper() if part in upper_acronyms else part for part in slug.split("-"))


def _slug(value: str) -> str:
    return value.strip().lower().replace("_", "-").replace(" ", "-")


def _strip_format_words(value: str) -> str:
    return re.sub(
        r"-(geopackage|shapefile|geojson|file-geodatabase|file_geodatabase)(-.*)?$",
        "",
        value,
    )


class _ManifestStorage:
    def __init__(self, root: str) -> None:
        self.root = root.rstrip("/")
        self.is_gcs = self.root.startswith("gs://")

    def iter_object_keys(self) -> Iterable[str]:
        if self.is_gcs:
            yield from self._iter_gcs_object_keys()
            return

        root = Path(self.root).resolve()
        if not root.exists():
            return
        for path in root.rglob("*"):
            if path.is_file():
                yield path.relative_to(root).as_posix()

    def exists(self, key: str) -> bool:
        if self.is_gcs:
            return (
                subprocess.run(
                    ["gcloud", "storage", "ls", f"{self.root}/{key}"],
                    check=False,
                    capture_output=True,
                    text=True,
                ).returncode
                == 0
            )
        return (Path(self.root).resolve() / key).exists()

    def iter_existing_manifest_keys(self) -> Iterable[str]:
        if self.is_gcs:
            bucket, prefix = _split_gcs_root(self.root)
            token = _gcloud_access_token()
            yield from _iter_gcs_json_api_object_keys_with_token(
                bucket,
                prefix,
                f"{prefix}*/*/metadata/source_manifest.json",
                token,
            )
            yield from _iter_gcs_json_api_object_keys_with_token(
                bucket,
                prefix,
                f"{prefix}*/*/*/metadata/source_manifest.json",
                token,
            )
            return

        root = Path(self.root).resolve()
        if not root.exists():
            return
        for path in root.rglob("source_manifest.json"):
            if path.is_file():
                yield path.relative_to(root).as_posix()

    def write_json(self, key: str, data: dict[str, Any]) -> None:
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True, indent=2).encode(
            "utf-8"
        )
        if self.is_gcs:
            with tempfile.NamedTemporaryFile(suffix=".json") as tmp:
                tmp.write(payload)
                tmp.flush()
                _run_gcloud(
                    ["gcloud", "storage", "cp", tmp.name, f"{self.root}/{key}"],
                )
            return

        path = Path(self.root).resolve() / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)

    def _iter_gcs_object_keys(self) -> Iterable[str]:
        try:
            for format_dir in sorted(SOURCE_FORMAT_DIRS):
                yield from _iter_gcs_json_api_object_keys(self.root, format_dir)
            return
        except Exception:
            yield from self._iter_gcs_object_keys_with_gcloud()

    def _iter_gcs_object_keys_with_gcloud(self) -> Iterable[str]:
        for format_dir in sorted(SOURCE_FORMAT_DIRS):
            completed = _run_gcloud(
                [
                    "gcloud",
                    "storage",
                    "ls",
                    "--recursive",
                    f"{self.root}/*/*/v*.*.*/{format_dir}/**",
                ],
                allow_no_matches=True,
                capture_output=True,
                text=True,
            )
            yield from parse_gcloud_ls_output(completed.stdout, self.root)


def _iter_gcs_json_api_object_keys(root: str, format_dir: str) -> Iterable[str]:
    bucket, prefix = _split_gcs_root(root)
    token = _gcloud_access_token()
    match_glob = f"{prefix}*/*/v*.*.*/{format_dir}/**"
    yield from _iter_gcs_json_api_object_keys_with_token(bucket, prefix, match_glob, token)


def _iter_gcs_json_api_object_keys_with_token(
    bucket: str,
    prefix: str,
    match_glob: str,
    token: str,
) -> Iterable[str]:
    page_token: str | None = None
    while True:
        query = {
            "matchGlob": match_glob,
            "fields": "items/name,nextPageToken",
        }
        if prefix:
            query["prefix"] = prefix
        if page_token:
            query["pageToken"] = page_token
        url = (
            f"https://storage.googleapis.com/storage/v1/b/{bucket}/o?"
            f"{urllib.parse.urlencode(query)}"
        )
        request = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(request) as response:
            payload = json.loads(response.read().decode("utf-8"))

        for item in payload.get("items", []):
            name = item.get("name", "")
            if prefix and name.startswith(prefix):
                name = name[len(prefix):]
            if name:
                yield name.strip("/")

        page_token = payload.get("nextPageToken")
        if not page_token:
            break


def _split_gcs_root(root: str) -> tuple[str, str]:
    value = root.removeprefix("gs://").strip("/")
    bucket, _, prefix = value.partition("/")
    return bucket, f"{prefix.strip('/')}/" if prefix else ""


def _gcloud_access_token() -> str:
    completed = _run_gcloud(
        ["gcloud", "auth", "print-access-token"],
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def parse_gcloud_ls_output(output: str, root: str) -> list[str]:
    root = root.rstrip("/")
    keys: list[str] = []
    for line in output.splitlines():
        value = line.strip()
        if not value or not value.startswith("gs://"):
            continue
        if value.endswith(":"):
            continue
        if not value.startswith(f"{root}/"):
            continue
        keys.append(value.removeprefix(f"{root}/").strip("/"))
    return keys


def _run_gcloud(
    args: list[str],
    *,
    allow_no_matches: bool = False,
    **kwargs,
) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(args, check=True, **kwargs)
    except subprocess.CalledProcessError as exc:
        stderr = ""
        if isinstance(exc.stderr, str):
            stderr = exc.stderr.strip()
        elif exc.stderr:
            stderr = exc.stderr.decode("utf-8", errors="replace").strip()
        if allow_no_matches and _is_gcloud_no_match_error(stderr):
            return subprocess.CompletedProcess(exc.args, exc.returncode, stdout="", stderr=stderr)
        raise RuntimeError(
            f"gcloud command failed: {' '.join(args)}"
            + (f"\n{stderr}" if stderr else "")
        ) from exc


def _is_gcloud_no_match_error(stderr: str) -> bool:
    normalized = stderr.lower()
    return "no urls matched" in normalized or "matched no objects" in normalized


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Seed source_manifest.json files for staged HIFLD dataset files."
    )
    parser.add_argument(
        "--staging-root",
        default="gs://hifld-next-staging-prod",
        help="Local staging root or gs:// bucket/prefix.",
    )
    parser.add_argument(
        "--datasets-jsonl",
        required=True,
        help="Path to legacy Dataset API datasets.jsonl.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Actually write manifests. Without this flag, runs as a dry run.",
    )
    parser.add_argument(
        "--level",
        choices=("file", "dataset"),
        default="file",
        help="Manifest level to seed. File level writes dataset/file/metadata; dataset level writes dataset/metadata.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing source_manifest.json files.",
    )
    parser.add_argument(
        "--summary-json",
        help="Optional local path to write the dry-run/write summary JSON.",
    )
    args = parser.parse_args(argv)

    seed_fn = (
        seed_dataset_source_manifests
        if args.level == "dataset"
        else seed_source_manifests
    )
    result = seed_fn(
        args.staging_root,
        args.datasets_jsonl,
        dry_run=not args.write,
        overwrite=args.overwrite,
    )
    summary = {
        "discovered_files": result.discovered_files,
        "to_write": result.to_write,
        "written": result.written,
        "skipped_existing": result.skipped_existing,
        "missing_dataset_metadata": result.missing_dataset_metadata,
        "entries": result.entries,
    }
    rendered = json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2)
    if args.summary_json:
        Path(args.summary_json).write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

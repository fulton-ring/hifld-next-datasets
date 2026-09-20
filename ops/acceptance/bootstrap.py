"""Bootstrap pinned public fixtures into the local SeaweedFS S3 endpoint.

The source is deliberately restricted to public, generation-addressed GCS
objects.  The destination guard prevents an accidental write to production.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol
from urllib.parse import quote
from urllib.request import Request, urlopen

import pyogrio
from pyproj import Transformer

MANIFEST_PATH = Path(__file__).with_name("manifest.json")
DEFAULT_CACHE_DIR = Path("/private/tmp/hifld-acceptance-cache")
DEFAULT_ENDPOINT = "http://localhost:8333"
DEFAULT_BUCKET = "hifld-local-staging"

CONTENT_TYPES = {
    "file_geodatabase": "application/zip",
    "geojson": "application/geo+json",
    "geopackage": "application/geopackage+sqlite3",
    "geoparquet": "application/vnd.apache.parquet",
    "pmtiles": "application/vnd.pmtiles",
    "shapefile": "application/zip",
}


@dataclass(frozen=True)
class Fixture:
    name: str
    wave: str
    collection_slug: str
    source_bucket: str
    source_key: str
    generation: str
    size: int
    md5: str
    destination_key: str
    dataset_slug: str = "tiny"
    file_slug: str = "tiny"
    version: str = "v1.0.0"
    format: str = "geopackage"
    geometry: str = "point"

    @property
    def cache_name(self) -> str:
        suffixes = "".join(Path(self.source_key).suffixes)
        return f"{self.name}-{self.generation}{suffixes}"

    @property
    def content_type(self) -> str:
        return CONTENT_TYPES[self.format]


@dataclass(frozen=True)
class MetadataSource:
    name: str
    source_bucket: str
    source_key: str
    generation: str
    size: int
    md5: str
    scope: Literal["dataset", "file", "version"]
    dataset_slug: str
    file_slug: str | None
    version: str | None
    filename: str

    @property
    def cache_name(self) -> str:
        return f"metadata-{self.name}-{self.generation}.json"

    @property
    def destination_key(self) -> str:
        if self.scope == "dataset":
            return f"hifld/{self.dataset_slug}/metadata/source/{self.filename}"
        if self.file_slug is None:
            raise ValueError(f"{self.scope} metadata requires file_slug")
        if self.scope == "file":
            return f"hifld/{self.dataset_slug}/{self.file_slug}/metadata/source/{self.filename}"
        if self.version is None:
            raise ValueError("version metadata requires version")
        return f"hifld/{self.dataset_slug}/{self.file_slug}/{self.version}/metadata/source/{self.filename}"


class ObjectClient(Protocol):
    def head_bucket(self, *, Bucket: str) -> Mapping[str, object]: ...

    def create_bucket(self, *, Bucket: str) -> Mapping[str, object]: ...

    def head_object(self, *, Bucket: str, Key: str) -> Mapping[str, object]: ...

    def put_object(self, **kwargs: object) -> Mapping[str, object]: ...


def load_manifest(path: Path = MANIFEST_PATH) -> tuple[Fixture, ...]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("manifest must be an object")
    records = raw.get("records", raw.get("fixtures"))
    if not isinstance(records, list):
        raise TypeError("manifest must contain a records list")
    source_bucket = raw.get("source_bucket")
    if not isinstance(source_bucket, str) or not source_bucket:
        raise ValueError("manifest source_bucket must be a non-empty string")
    fixtures: list[Fixture] = []
    rich_records = "records" in raw
    for item in records:
        if not isinstance(item, dict):
            raise TypeError("fixture entries must be objects")
        fixture = Fixture(
            name=_string(item, "name"),
            wave=_string(item, "wave"),
            collection_slug=_string(item, "collection_slug"),
            source_bucket=source_bucket,
            source_key=_string(item, "source_key"),
            generation=_string(item, "generation"),
            size=_integer(item, "size"),
            md5=_string(item, "md5"),
            destination_key=_string(item, "destination_key"),
            dataset_slug=_string(item, "dataset_slug") if rich_records else "tiny",
            file_slug=_string(item, "file_slug") if rich_records else "tiny",
            version=_string(item, "version") if rich_records else "v1.0.0",
            format=_string(item, "format") if rich_records else "geopackage",
            geometry=_string(item, "geometry") if rich_records else "point",
        )
        if fixture.format not in CONTENT_TYPES:
            raise ValueError(f"unsupported source format {fixture.format!r}")
        prefix = (
            f"{fixture.collection_slug}/{fixture.dataset_slug}/{fixture.file_slug}/"
            f"{fixture.version}/{fixture.format}/"
        )
        if rich_records and not fixture.destination_key.startswith(prefix):
            raise ValueError(
                f"destination_key for {fixture.name!r} must use canonical collection prefix {prefix!r}"
            )
        fixtures.append(fixture)
    return tuple(fixtures)


def load_metadata_sources(path: Path = MANIFEST_PATH) -> tuple[MetadataSource, ...]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    source_bucket = raw.get("source_bucket")
    entries = raw.get("metadata_sources")
    if not isinstance(source_bucket, str) or not isinstance(entries, list):
        raise TypeError("manifest must contain source_bucket and metadata_sources")
    return tuple(
        MetadataSource(
            name=_string(item, "name"),
            source_bucket=source_bucket,
            source_key=_string(item, "source_key"),
            generation=_string(item, "generation"),
            size=_integer(item, "size"),
            md5=_string(item, "md5"),
            scope=_metadata_scope(item),
            dataset_slug=_string(item, "dataset_slug"),
            file_slug=_optional_string(item, "file_slug"),
            version=_optional_string(item, "version"),
            filename=_string(item, "filename"),
        )
        for item in entries
        if isinstance(item, dict)
    )


def _metadata_scope(item: dict[str, object]) -> Literal["dataset", "file", "version"]:
    value = item.get("scope", "version")
    if value == "dataset":
        return "dataset"
    if value == "file":
        return "file"
    if value == "version":
        return "version"
    raise ValueError("metadata scope must be dataset, file, or version")


def _string(item: dict[str, object], key: str) -> str:
    value = item.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"manifest field {key!r} must be a non-empty string")
    return value


def _integer(item: dict[str, object], key: str) -> int:
    value = item.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"manifest field {key!r} must be a non-negative integer")
    return value


def _optional_string(item: dict[str, object], key: str) -> str | None:
    value = item.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"manifest field {key!r} must be a non-empty string when present"
        )
    return value


def verify_bytes(path: Path, fixture: Fixture | MetadataSource) -> None:
    digest = hashlib.md5()
    size = 0
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    actual = base64.b64encode(digest.digest()).decode("ascii")
    if size != fixture.size or actual != fixture.md5:
        raise ValueError(
            f"integrity mismatch for {fixture.name}: "
            f"expected size/md5 {fixture.size}/{fixture.md5}, got {size}/{actual}"
        )


def download_fixture(fixture: Fixture | MetadataSource, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    target = cache_dir / fixture.cache_name
    if target.is_file():
        try:
            verify_bytes(target, fixture)
            return target
        except ValueError:
            target.unlink()

    url = (
        "https://storage.googleapis.com/storage/v1/b/"
        f"{fixture.source_bucket}/o/{quote(fixture.source_key, safe='')}"
        f"?generation={quote(fixture.generation)}&alt=media"
    )
    request = Request(url, headers={"Accept": "application/octet-stream"})
    with (
        urlopen(request, timeout=120) as response,
        tempfile.NamedTemporaryFile(
            mode="wb", dir=cache_dir, prefix=f".{fixture.name}-", delete=False
        ) as temporary,
    ):
        temporary_path = Path(temporary.name)
        while chunk := response.read(1024 * 1024):
            temporary.write(chunk)
    try:
        verify_bytes(temporary_path, fixture)
        temporary_path.replace(target)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise
    return target


def _client(endpoint: str) -> ObjectClient:
    from urllib.parse import urlparse

    host = urlparse(endpoint).hostname
    if host not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("destination endpoint must be local SeaweedFS")
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "bootstrap requires boto3 for the SeaweedFS S3 endpoint"
        ) from exc
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "dummy"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "dummy"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def ensure_bucket(client: ObjectClient, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:  # noqa: BLE001 - S3 clients use backend-specific exceptions.
        client.create_bucket(Bucket=bucket)


def upload_fixture(
    client: ObjectClient, bucket: str, fixture: Fixture, path: Path
) -> str:
    try:
        existing = client.head_object(Bucket=bucket, Key=fixture.destination_key)
        metadata = existing.get("Metadata")
        if (
            isinstance(metadata, Mapping)
            and metadata.get("md5") == fixture.md5
            and metadata.get("size") == str(fixture.size)
            and existing.get("ContentLength") == fixture.size
        ):
            return "cached"
    except Exception:  # noqa: BLE001, S110 - a missing object is a cache miss.
        pass

    body = path.open("rb")
    try:
        response = client.put_object(
            Bucket=bucket,
            Key=fixture.destination_key,
            Body=body,
            ContentLength=fixture.size,
            ContentMD5=fixture.md5,
            Metadata={
                "md5": fixture.md5,
                "size": str(fixture.size),
                "generation": fixture.generation,
                "source-bucket": fixture.source_bucket,
                "source-key": fixture.source_key,
            },
            ContentType=fixture.content_type,
        )
    finally:
        body.close()
    if not isinstance(response, Mapping):
        raise TypeError(f"SeaweedFS upload returned no response for {fixture.name}")
    uploaded = client.head_object(Bucket=bucket, Key=fixture.destination_key)
    metadata = uploaded.get("Metadata")
    if (
        uploaded.get("ContentLength") != fixture.size
        or not isinstance(metadata, Mapping)
        or metadata.get("md5") != fixture.md5
        or metadata.get("size") != str(fixture.size)
    ):
        raise RuntimeError(
            f"SeaweedFS checksum metadata verification failed for {fixture.name}"
        )
    return "uploaded"


def upload_metadata_source(
    client: ObjectClient, bucket: str, source: MetadataSource, path: Path
) -> str:
    try:
        existing = client.head_object(Bucket=bucket, Key=source.destination_key)
        if existing.get("ContentLength") == source.size:
            return "cached"
    except Exception:  # noqa: BLE001, S110 - a missing object is a cache miss.
        pass
    with path.open("rb") as body:
        client.put_object(
            Bucket=bucket,
            Key=source.destination_key,
            Body=body,
            ContentLength=source.size,
            ContentMD5=source.md5,
            ContentType="application/json",
            Metadata={
                "md5": source.md5,
                "size": str(source.size),
                "generation": source.generation,
                "source-bucket": source.source_bucket,
                "source-key": source.source_key,
            },
        )
    return "uploaded"


def upload_collection_source(
    client: ObjectClient, bucket: str, manifest_path: Path
) -> str:
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    source = raw.get("collection_source")
    if not isinstance(source, dict):
        raise TypeError("manifest must contain collection_source")
    relative_path = _string(source, "path")
    destination_key = _string(source, "destination_key")
    expected_sha256 = _string(source, "sha256")
    path = manifest_path.parent / relative_path
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest() != expected_sha256:
        raise ValueError("collection source integrity mismatch")
    try:
        existing = client.head_object(Bucket=bucket, Key=destination_key)
        if existing.get("ContentLength") == len(body):
            return "cached"
    except Exception:  # noqa: BLE001, S110 - a missing object is a cache miss.
        pass
    client.put_object(
        Bucket=bucket,
        Key=destination_key,
        Body=io.BytesIO(body),
        ContentLength=len(body),
        ContentType="application/json",
        Metadata={"sha256": expected_sha256, "source-url": _string(source, "url")},
    )
    return "uploaded"


def _put_json(
    client: ObjectClient, bucket: str, key: str, value: dict[str, object]
) -> None:
    body = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=io.BytesIO(body),
        ContentLength=len(body),
        ContentType="application/json",
    )


def _asset_href(bucket: str, fixture: Fixture, endpoint: str = DEFAULT_ENDPOINT) -> str:
    return f"{endpoint.rstrip('/')}/{bucket}/{quote(fixture.destination_key, safe='/')}"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return f"1220{digest.hexdigest()}"


def upload_staging_catalog(
    client: ObjectClient,
    bucket: str,
    fixtures: tuple[Fixture, ...],
    paths: Mapping[str, Path],
    metadata_paths: Mapping[str, Path],
    endpoint: str = DEFAULT_ENDPOINT,
) -> None:
    """Publish a source-only STAC tree; verified derived data is cataloged separately."""
    groups: dict[tuple[str, str, str, str], list[Fixture]] = {}
    for fixture in fixtures:
        groups.setdefault(
            (
                fixture.collection_slug,
                fixture.dataset_slug,
                fixture.file_slug,
                fixture.version,
            ),
            [],
        ).append(fixture)
    _put_json(
        client,
        bucket,
        "catalog.json",
        {
            "stac_version": "1.1.0",
            "type": "Catalog",
            "id": "hifld-local-staging",
            "title": "HIFLD local staging sources",
            "description": "Generation-pinned original source formats for local acceptance; not the verified derived-data catalog.",
            "links": [
                {
                    "rel": "child",
                    "href": "hifld/catalog.json",
                    "type": "application/json",
                }
            ],
        },
    )
    datasets = sorted({key[1] for key in groups})
    _put_json(
        client,
        bucket,
        "hifld/catalog.json",
        {
            "stac_version": "1.1.0",
            "type": "Catalog",
            "id": "hifld",
            "title": "HIFLD staging source catalog",
            "description": "Original source assets staged for local conversion.",
            "links": [
                {
                    "rel": "child",
                    "href": f"{slug}/catalog.json",
                    "type": "application/json",
                }
                for slug in datasets
            ],
        },
    )
    for dataset in datasets:
        identities = sorted(key for key in groups if key[1] == dataset)
        files = sorted({key[2] for key in identities})
        dataset_doc = json.loads(
            metadata_paths[
                f"hifld/{dataset}/metadata/source/source_manifest.json"
            ].read_text(encoding="utf-8")
        )
        _put_json(
            client,
            bucket,
            f"hifld/{dataset}/catalog.json",
            {
                "stac_version": "1.1.0",
                "type": "Catalog",
                "id": f"hifld/{dataset}",
                "title": _string(dataset_doc, "title"),
                "description": _string(dataset_doc, "description"),
                "hifld:tags": dataset_doc.get("tags", {}),
                "links": [
                    {
                        "rel": "child",
                        "href": f"{slug}/catalog.json",
                        "type": "application/json",
                    }
                    for slug in files
                ],
            },
        )
        for file_slug in files:
            identity = next(key for key in identities if key[2] == file_slug)
            sources = groups[identity]
            first = sources[0]
            version_prefix = (
                f"hifld/{dataset}/{file_slug}/{first.version}/metadata/source"
            )
            source_doc = json.loads(
                metadata_paths[f"{version_prefix}/source_manifest.json"].read_text(
                    encoding="utf-8"
                )
            )
            dictionary = json.loads(
                metadata_paths[f"{version_prefix}/data_dictionary.json"].read_text(
                    encoding="utf-8"
                )
            )
            file_doc = json.loads(
                metadata_paths[
                    f"hifld/{dataset}/{file_slug}/metadata/source/source_manifest.json"
                ].read_text(encoding="utf-8")
            )
            title = dictionary.get("title", source_doc.get("title"))
            description = dictionary.get("description", source_doc.get("description"))
            if not isinstance(title, str) or not isinstance(description, str):
                raise TypeError(
                    f"authoritative title/description missing for {dataset}/{file_slug}"
                )
            publisher = dictionary.get("publisher", source_doc.get("publisher"))
            _put_json(
                client,
                bucket,
                f"hifld/{dataset}/{file_slug}/catalog.json",
                {
                    "stac_version": "1.1.0",
                    "type": "Catalog",
                    "id": f"hifld/{dataset}/{file_slug}",
                    "title": _string(file_doc, "title"),
                    "description": _string(file_doc, "description"),
                    "hifld:tags": file_doc.get("tags", {}),
                    "links": [
                        {
                            "rel": "child",
                            "href": f"{first.version}/collection.json",
                            "type": "application/json",
                        }
                    ],
                },
            )
            inspect = next(
                (item for item in sources if item.format == "geopackage"), sources[0]
            )
            info = pyogrio.read_info(paths[inspect.name])
            bounds = tuple(float(value) for value in info["total_bounds"])
            crs = info["crs"]
            if isinstance(crs, str) and crs != "EPSG:4326":
                transform = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
                west, south = transform.transform(bounds[0], bounds[1])
                east, north = transform.transform(bounds[2], bounds[3])
                bounds = (west, south, east, north)
            assets = {
                item.format: {
                    "href": _asset_href(bucket, item, endpoint),
                    "type": item.content_type,
                    "title": f"Original {item.format.replace('_', ' ')} source",
                    "roles": ["data"],
                    "file:size": item.size,
                    "file:checksum": _sha256(paths[item.name]),
                    "hifld:source_md5": item.md5,
                    "hifld:source_generation": item.generation,
                }
                for item in sources
            }
            collection = {
                "stac_version": "1.1.0",
                "stac_extensions": [
                    "https://stac-extensions.github.io/file/v2.1.0/schema.json"
                ],
                "type": "Collection",
                "id": f"hifld/{dataset}/{file_slug}/{first.version}",
                "title": title,
                "description": description,
                "license": "other",
                "providers": [{"name": publisher}]
                if isinstance(publisher, str)
                else [],
                "keywords": [
                    item
                    for item in dictionary.get("keywords", [])
                    if isinstance(item, str)
                ],
                "hifld:tags": source_doc.get("tags", {}),
                "extent": {
                    "spatial": {"bbox": [list(bounds)]},
                    "temporal": {"interval": [[None, None]]},
                },
                "links": [{"rel": "root", "href": "../../../../catalog.json"}],
                "assets": assets,
                "hifld:catalog_scope": "unverified-staging-sources",
                "hifld:feature_count": int(info["features"]),
                "hifld:geometry_type": str(info["geometry_type"]),
                "hifld:source_crs": str(crs),
                "hifld:source_fields": [str(field) for field in info["fields"]],
            }
            _put_json(
                client,
                bucket,
                f"hifld/{dataset}/{file_slug}/{first.version}/collection.json",
                collection,
            )


def bootstrap(
    *,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    endpoint: str = DEFAULT_ENDPOINT,
    bucket: str = DEFAULT_BUCKET,
    manifest: Path = MANIFEST_PATH,
    client: ObjectClient | None = None,
    downloader: Callable[[Fixture, Path], Path] = download_fixture,
) -> tuple[str, ...]:
    if bucket != DEFAULT_BUCKET:
        raise ValueError(f"refusing non-local-staging bucket {bucket!r}")
    fixtures = load_manifest(manifest)
    rich_records = "records" in json.loads(manifest.read_text(encoding="utf-8"))
    metadata_sources = load_metadata_sources(manifest) if rich_records else ()
    destination = client or _client(endpoint)
    ensure_bucket(destination, bucket)
    statuses: list[str] = []
    paths: dict[str, Path] = {}
    metadata_paths: dict[str, Path] = {}
    for fixture in fixtures:
        path = downloader(fixture, cache_dir)
        verify_bytes(path, fixture)
        paths[fixture.name] = path
        statuses.append(
            f"{fixture.name}:{upload_fixture(destination, bucket, fixture, path)}"
        )
    for source in metadata_sources:
        path = download_fixture(source, cache_dir)
        verify_bytes(path, source)
        metadata_paths[source.destination_key] = path
        statuses.append(
            f"metadata-{source.name}:{upload_metadata_source(destination, bucket, source, path)}"
        )
    if rich_records:
        statuses.append(
            f"collection-source:{upload_collection_source(destination, bucket, manifest)}"
        )
        upload_staging_catalog(
            destination, bucket, fixtures, paths, metadata_paths, endpoint
        )
    return tuple(statuses)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    args = parser.parse_args(argv)
    for status in bootstrap(**vars(args)):
        print(status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

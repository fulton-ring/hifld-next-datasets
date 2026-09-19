from __future__ import annotations

import base64
import hashlib
import tempfile
import unittest
from pathlib import Path

from ops.acceptance.bootstrap import Fixture, bootstrap, verify_bytes


def _fixture(size: int, data: bytes) -> Fixture:
    return Fixture(
        name="tiny",
        wave="initial",
        collection_slug="hifld",
        source_bucket="hifld-next-datasets-prod",
        source_key="tiny/tiny/v1.0.0/geopackage/tiny.gpkg",
        generation="123",
        size=size,
        md5=base64.b64encode(hashlib.md5(data).digest()).decode("ascii"),
        destination_key="tiny.gpkg",
    )


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}
        self.puts = 0

    def head_bucket(self, *, Bucket: str) -> dict[str, str]:
        return {"Bucket": Bucket}

    def create_bucket(self, *, Bucket: str) -> dict[str, str]:
        return {"Bucket": Bucket}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        if Key not in self.objects:
            raise KeyError(Key)
        body, metadata = self.objects[Key]
        return {"ContentLength": len(body), "Metadata": metadata}

    def put_object(self, **kwargs: object) -> dict[str, str]:
        body = kwargs["Body"]
        assert hasattr(body, "read")
        content = body.read()
        metadata = kwargs["Metadata"]
        assert isinstance(metadata, dict)
        self.objects[str(kwargs["Key"])] = (content, metadata)
        self.puts += 1
        return {"ETag": "verified"}


def test_pinned_fixture_integrity_rejects_changed_bytes(tmp_path: Path) -> None:
    data = b"fixture bytes"
    fixture = _fixture(len(data), data)
    path = tmp_path / "fixture.gpkg"
    path.write_bytes(data)
    verify_bytes(path, fixture)

    path.write_bytes(b"changed bytes")
    with unittest.TestCase().assertRaisesRegex(ValueError, "integrity mismatch"):
        verify_bytes(path, fixture)


def test_bootstrap_reuses_cache_and_uploaded_object(tmp_path: Path) -> None:
    data = b"fixture bytes"
    fixture = _fixture(len(data), data)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        '{"source_bucket": "hifld-next-datasets-prod", "fixtures": '
        '[{"name": "tiny", "wave": "initial", "collection_slug": "hifld", '
        '"source_key": "tiny", '
        f'"generation": "123", "size": {len(data)}, "md5": "{fixture.md5}", '
        '"destination_key": "tiny.gpkg"}]}',
        encoding="utf-8",
    )
    cache = tmp_path / "cache"
    calls = 0

    def download(current: Fixture, target: Path) -> Path:
        nonlocal calls
        calls += 1
        target.mkdir(parents=True, exist_ok=True)
        result = target / current.cache_name
        result.write_bytes(data)
        return result

    client = FakeS3()
    first = bootstrap(
        cache_dir=cache, manifest=manifest, client=client, downloader=download
    )
    second = bootstrap(
        cache_dir=cache, manifest=manifest, client=client, downloader=download
    )

    assert first == ("tiny:uploaded",)
    assert second == ("tiny:cached",)
    assert calls == 2
    assert client.puts == 1


class AcceptanceBootstrapTests(unittest.TestCase):
    def test_integrity(self):
        with tempfile.TemporaryDirectory() as directory:
            test_pinned_fixture_integrity_rejects_changed_bytes(Path(directory))

    def test_idempotence(self):
        with tempfile.TemporaryDirectory() as directory:
            test_bootstrap_reuses_cache_and_uploaded_object(Path(directory))

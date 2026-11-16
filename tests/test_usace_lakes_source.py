"""Focused native type and geometry tests for the official IENC lake class."""
from __future__ import annotations

import io
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.sources.usace_lakes import (
    EXPECTED_COUNT,
    EXPECTED_ID_SHA256,
    EXPECTED_IDS,
    EXPECTED_FIELD_DECLARATIONS,
    _arrow_type,
    _parquet_bytes,
    _fetch_retry,
    _fetch,
    _configured_capture_budget,
    _sha,
    _canonical,
)


class UsaceLakesSourceTest(unittest.TestCase):
    def test_pinned_native_roster_and_count(self) -> None:
        self.assertEqual(EXPECTED_COUNT, 4940)
        self.assertEqual((EXPECTED_IDS[0], EXPECTED_IDS[-1]), (1, 4940))
        self.assertEqual(_sha(_canonical(EXPECTED_IDS)), EXPECTED_ID_SHA256)

    def test_scale_minimum_remains_nullable_int32(self) -> None:
        field = next(field for field in EXPECTED_FIELD_DECLARATIONS if field["name"] == "Scale_Minimum")
        self.assertEqual(_arrow_type(field), __import__("pyarrow").int32())
        attrs = {field["name"]: None for field in EXPECTED_FIELD_DECLARATIONS if field["name"] != "SHAPE"}
        attrs.update({"OBJECTID": 17, "Scale_Minimum": None, "Object_Name": "00107", "Feature_ID": "U3 1393266544 00001"})
        feature = {"attributes": attrs, "geometry": {"rings": [[[-90, 38], [-90, 39], [-89, 39], [-89, 38], [-90, 38]]]}}
        parquet = _parquet_bytes(EXPECTED_FIELD_DECLARATIONS, [feature], 4326)
        table = pq.ParquetFile(io.BytesIO(parquet)).read(use_threads=False)
        self.assertEqual(str(table.schema.field("Scale_Minimum").type), "int32")
        self.assertEqual(table.column("Scale_Minimum").null_count, 1)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [17])
        self.assertEqual(table.column("Object_Name").to_pylist(), ["00107"])
        geometry = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertEqual(list(geometry.exterior.coords), [(-90.0, 38.0), (-90.0, 39.0), (-89.0, 39.0), (-89.0, 38.0), (-90.0, 38.0)])
        _primary, _types, crs = geometry_metadata(table.schema)
        self.assertEqual(crs.to_epsg(), 4326)

    def test_attribute_schema_is_native_and_ordered(self) -> None:
        names = [field["name"] for field in EXPECTED_FIELD_DECLARATIONS]
        self.assertEqual(names, ["OBJECTID", "SHAPE", "Object_Name", "Information", "Source_Date", "Source_Indication", "Scale_Minimum", "Source_Dataset", "Feature_ID"])

    def test_local_capture_budget_failure_is_not_retried(self) -> None:
        with patch("dagster_hifld.sources.usace_lakes._fetch", side_effect=ValueError("source_capture_budget_exceeded")) as fetch:
            with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded"):
                _fetch_retry(None, "https://example.invalid", __import__("pathlib").Path("/tmp"), "page_000", [0])
        fetch.assert_called_once()

    def test_configured_capture_budget_is_bounded_and_enforced(self) -> None:
        self.assertEqual(_configured_capture_budget(12_000_000), 12_000_000)
        for invalid in (True, 999_999, 96_000_001):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "capture_budget_out_of_bounds"):
                _configured_capture_budget(invalid)

        class Response:
            url = "https://example.invalid/page"
            status_code = 200
            def __enter__(self): return self
            def __exit__(self, *_args): return False
        class Client:
            def stream(self, *_args, **_kwargs): return Response()
        progress = SimpleNamespace(wire_bytes_retained=6_000_000, decoded_bytes_retained=5_000_000,
                                   reserve_bytes=1_000_001)
        response_limits = []
        def over_budget(_response, **kwargs):
            response_limits.append((kwargs["max_wire_bytes"], kwargs["max_decoded_bytes"]))
            kwargs["retain_wire"](b"123456")
            kwargs["retain_decoded"](b"12345")
            try:
                kwargs["reserve"](progress)
            except ValueError:
                return SimpleNamespace(error_code="reserve_error", error_type="ValueError", status_code=200, complete=False,
                                       content_encoding=None, content_length=None)
            return SimpleNamespace(error_code=None, error_type=None, status_code=200, complete=True,
                                   content_encoding=None, content_length=None)
        with patch("dagster_hifld.sources.usace_lakes.capture_http_response", side_effect=over_budget):
            with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded"):
                _fetch(Client(), "https://example.invalid/page", __import__("pathlib").Path("/tmp"), "configured-cap-test", [0], 12_000_000)
        self.assertEqual(response_limits, [(12_000_000, 12_000_000)])

"""Fail-closed native schema and geometry checks for NCES school locations."""

from __future__ import annotations

import json
import tempfile
import unittest
from unittest.mock import patch

import geopandas as gpd
import httpx
import pandas as pd
from dagster import DagsterInstance, materialize
from shapely.geometry import Point

from dagster_hifld.assets.nces_public_schools import nces_public_schools
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import nces_public_schools as source
from dagster_hifld.sources.nces_public_schools import (
    EXPECTED_FIELDS,
    SOURCE_VINTAGE,
    _canonical,
    _parquet_bytes,
    _sha,
    _validate_frame,
)


def _frame() -> gpd.GeoDataFrame:
    rows = []
    for school_id, state, x, y in (
        ("000000000001", "AK", -149.9, 61.2),
        ("000000000002", "HI", -157.8, 21.3),
    ):
        row = {name: f"{name}-{school_id}" for name in EXPECTED_FIELDS}
        row.update({
            "NCESSCH": school_id,
            "STATE": state,
            "LAT": y,
            "LON": x,
            "SCHOOLYEAR": SOURCE_VINTAGE,
        })
        rows.append(row)
    return gpd.GeoDataFrame(pd.DataFrame(rows, columns=EXPECTED_FIELDS),
                            geometry=[Point(-149.9, 61.2), Point(-157.8, 21.3)],
                            crs="EPSG:4326")


class NcesPublicSchoolsSourceTest(unittest.TestCase):
    def test_live_invalid_archive_retains_evidence_and_original_refusal(self) -> None:
        body = b"invalid zip from publisher"
        client = httpx.Client(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=httpx.ByteStream(body), headers={"Content-Length": str(len(body))})))
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            with (patch.object(source.httpx, "Client", return_value=client),
                  patch.object(source, "EXPECTED_BYTES", len(body)),
                  patch.object(source, "EXPECTED_SHA256", _sha(body))):
                with self.assertRaisesRegex(ValueError, "nces_public_schools_archive_invalid"):
                    source.capture_nces_public_schools(storage)
            self._assert_quarantine(storage, body, 200)

    def test_live_http_failure_retains_response_without_candidate(self) -> None:
        body = b"publisher temporarily unavailable"
        client = httpx.Client(transport=httpx.MockTransport(
            lambda request: httpx.Response(503, stream=httpx.ByteStream(body), headers={"Content-Length": str(len(body))})))
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            with patch.object(source.httpx, "Client", return_value=client):
                with self.assertRaisesRegex(ValueError, "nces_public_schools_source_capture_failed"):
                    source.capture_nces_public_schools(storage)
            self._assert_quarantine(storage, body, 503)

    def _assert_quarantine(self, storage, body, status) -> None:
        keys = storage.list_prefix("_quarantine/public-schools")
        self.assertTrue(any(key.endswith("source.body") for key in keys), keys)
        raw = next(key.removesuffix("source.body") for key in keys if key.endswith("source.body"))
        self.assertEqual(storage.read_key(raw + "source.body"), body)
        self.assertEqual(storage.read_key(raw + "source.wire"), body)
        receipt = json.loads(storage.read_key(raw + "source.receipt.json"))
        original = json.loads(storage.read_key(raw + "source.original_receipt.json"))
        self.assertEqual(original["status"], status)
        self.assertEqual(receipt["status"], status)
        self.assertEqual(receipt["sha256"], _sha(body))
        self.assertFalse(storage.list_prefix("public-schools/public-schools"))
        self.assertFalse(storage.list_prefix("_construction-state/public-schools"))

    def test_dagster_http_refusal_retains_evidence_without_materialization(self) -> None:
        body = b"publisher temporarily unavailable"
        client = httpx.Client(transport=httpx.MockTransport(
            lambda request: httpx.Response(503, stream=httpx.ByteStream(body))))
        with tempfile.TemporaryDirectory() as directory, DagsterInstance.ephemeral() as instance:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            with patch.object(source.httpx, "Client", return_value=client):
                result = materialize([nces_public_schools], instance=instance,
                                     resources={"staging_storage": storage}, raise_on_error=False)
            self.assertFalse(result.success)
            self.assertFalse(result.get_asset_materialization_events())
            self._assert_quarantine(storage, body, 503)

    def test_changed_live_distribution_is_quarantined_without_adoption(self) -> None:
        body = b"a legitimately changed publisher edition still needs reviewed pins"
        client = httpx.Client(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=httpx.ByteStream(body))))
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            with patch.object(source.httpx, "Client", return_value=client):
                with self.assertRaisesRegex(ValueError, "archive_identity_or_release_drift"):
                    source.capture_nces_public_schools(storage)
            self._assert_quarantine(storage, body, 200)

    def test_row_additions_deletions_and_schema_drift_need_reviewed_pins(self) -> None:
        frame = _frame()
        ids_sha = _sha(_canonical(frame["NCESSCH"].tolist()))
        for count in (1, 3):
            with self.subTest(expected_count=count), self.assertRaisesRegex(
                    ValueError, "population_or_attribute_null_drift"):
                _validate_frame(frame, expected_count=count, expected_id_sha256=ids_sha)
        changed = frame.copy()
        changed.loc[1, "NCESSCH"] = "000000000003"
        with self.assertRaisesRegex(ValueError, "id_roster_drift"):
            _validate_frame(changed, expected_count=2, expected_id_sha256=ids_sha)
        with self.assertRaisesRegex(ValueError, "field_roster_drift"):
            _validate_frame(frame.assign(new_field="native-schema-change"),
                            expected_count=2, expected_id_sha256=ids_sha)

    def test_native_attributes_and_point_coordinates_roundtrip_as_geoparquet(self) -> None:
        frame = _frame()
        body = _parquet_bytes(frame)
        restored = gpd.read_parquet(__import__("io").BytesIO(body))
        self.assertEqual(list(restored.columns), [*EXPECTED_FIELDS, "geometry"])
        self.assertEqual(restored["NCESSCH"].tolist(), ["000000000001", "000000000002"])
        self.assertEqual(restored.geometry.to_wkt().tolist(), frame.geometry.to_wkt().tolist())
        self.assertEqual(restored.crs.to_epsg(), 4326)

    def test_duplicate_or_unsorted_school_ids_fail_closed(self) -> None:
        frame = _frame().iloc[::-1].reset_index(drop=True)
        with self.assertRaisesRegex(ValueError, "id_roster_drift"):
            _validate_frame(frame, expected_count=2, expected_id_sha256="0" * 64)
        duplicate = _frame()
        duplicate.loc[1, "NCESSCH"] = duplicate.loc[0, "NCESSCH"]
        with self.assertRaisesRegex(ValueError, "id_roster_drift"):
            _validate_frame(duplicate, expected_count=2, expected_id_sha256="0" * 64)

    def test_string_identifier_and_geometry_type_are_not_coerced(self) -> None:
        frame = _frame()
        frame["ZIP"] = pd.Series([12345, 96701], dtype="int64")
        with self.assertRaisesRegex(ValueError, "string_field_type_drift"):
            _validate_frame(frame, expected_count=2, expected_id_sha256="0" * 64)
        frame = _frame()
        frame.geometry = [Point(-149.9, 61.2), Point(-157.8, 21.3, 7)]
        with self.assertRaisesRegex(ValueError, "geometry_drift"):
            _validate_frame(frame, expected_count=2,
                            expected_id_sha256=_sha(_canonical(frame["NCESSCH"].tolist())))


if __name__ == "__main__":
    unittest.main()

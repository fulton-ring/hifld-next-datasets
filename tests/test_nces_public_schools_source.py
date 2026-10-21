"""Fail-closed native schema and geometry checks for NCES school locations."""

from __future__ import annotations

import unittest

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

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

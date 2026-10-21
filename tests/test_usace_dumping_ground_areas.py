"""Source fidelity and drift tests for USACE IENC dumping ground areas."""

from __future__ import annotations

import copy
import io
import json
import unittest
from urllib.parse import parse_qs, urlparse

import pyarrow.parquet as pq
import shapely

from dagster_hifld.assets.usace_dumping_ground_areas import (
    usace_dumping_ground_areas_draft_schedule,
    usace_dumping_ground_areas_usace_ienc,
)
from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.sources import usace_dumping_ground_areas as source


def _docs() -> dict[str, dict]:
    spatial_reference = {"wkid": 4326, "latestWkid": 4326}
    service = {
        "mapName": "DUMPING_GROUND_AREA",
        "copyrightText": "USACE IENC",
        "layers": [{"id": 95, "name": "DUMPING_GROUND_AREA", "geometryType": "esriGeometryPolygon"}],
    }
    layer = {
        "id": 95,
        "name": "DUMPING_GROUND_AREA",
        "geometryType": "esriGeometryPolygon",
        "spatialReference": spatial_reference,
        "sourceSpatialReference": spatial_reference,
        "extent": {"xmin": -1, "ymin": -1, "xmax": 2, "ymax": 2,
                   "spatialReference": spatial_reference},
        "hasZ": None,
        "hasM": None,
        "maxRecordCount": 2000,
        "fields": source.EXPECTED_FIELD_DECLARATIONS,
    }
    attrs = {
        "OBJECTID": 1,
        "Category_of_Dumping_Ground": "Dredged material",
        "Object_Name": "Example Ground",
        "Restriction": "No entry",
        "Information": "literal source text",
        "Scale_Minimum": 12000,
        "Source_Date": "2024-01",
        "Source_Indication": "survey",
        "Source_Dataset": "chart source",
        "Feature_ID": "U3 1393087507 00001",
    }
    feature = {
        "attributes": attrs,
        "geometry": {"rings": [[[-90.0, 38.0], [-90.0, 38.1], [-89.9, 38.1], [-89.9, 38.0], [-90.0, 38.0]]]},
    }
    directory = {"services": [{"name": "IENC_Feature_Classes/DUMPING_GROUND_AREA", "type": "MapServer"}]}
    return {
        "service_directory_before": directory,
        "service_directory_after": copy.deepcopy(directory),
        "service_before": service,
        "service_after": copy.deepcopy(service),
        "layer_before": layer,
        "layer_after": copy.deepcopy(layer),
        "count_before": {"count": 1},
        "count_after": {"count": 1},
        "ids_before": {"objectIds": [1]},
        "ids_after": {"objectIds": [1]},
        "page_000": {"geometryType": "esriGeometryPolygon", "spatialReference": spatial_reference,
                     "features": [feature]},
    }


def _captures(docs: dict[str, dict]) -> dict[str, tuple[bytes, dict]]:
    return {name: (json.dumps(doc).encode("utf-8"), {}) for name, doc in docs.items()}


class UsaceDumpingGroundAreasSourceTest(unittest.TestCase):
    def test_complete_single_feature_roster_is_validated(self) -> None:
        fields, features, audit = source._validate(_captures(_docs()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual([feature["attributes"]["OBJECTID"] for feature in features], [1])
        self.assertEqual(audit["source_feature_count"], 1)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["observed_coordinate_dimensions"], [2])
        self.assertEqual(audit["source_vertex_count"], 5)
        self.assertEqual(audit["source_coverage_scope"],
                         "Complete observed DUMPING_GROUND_AREA layer in the official USACE IENC chart service; not a nationwide or all-waterways dumping ground inventory.")

    def test_population_or_page_geometry_drift_fails_closed(self) -> None:
        docs = _docs()
        docs["ids_after"] = {"objectIds": [2]}
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            source._validate(_captures(docs))
        docs = _docs()
        docs["page_000"]["geometryType"] = "esriGeometryPoint"
        with self.assertRaisesRegex(ValueError, "source_page_geometry_type_drift"):
            source._validate(_captures(docs))

    def test_native_attributes_and_polygon_wkb_survive_geoparquet(self) -> None:
        docs = _docs()
        feature = docs["page_000"]["features"][0]
        fields = docs["layer_before"]["fields"]
        table = pq.ParquetFile(io.BytesIO(source._parquet_bytes(fields, [feature], 4326))).read()
        for name, value in feature["attributes"].items():
            self.assertEqual(table.column(name).to_pylist(), [value])
        primary, _, crs = geometry_metadata(table.schema)
        self.assertEqual(primary, "geometry")
        self.assertEqual(crs.to_epsg(), 4326)
        geometry = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertEqual(geometry.geom_type, "Polygon")
        self.assertEqual(shapely.to_wkb(geometry), source._esri_polygon(feature["geometry"]))
        self.assertEqual(table.column("OBJECTID").to_pylist(), [1])
        self.assertEqual(table.column("Scale_Minimum").to_pylist(), [12000])

    def test_z_or_m_dimensions_are_rejected_and_refresh_stays_stopped(self) -> None:
        docs = _docs()
        for point in docs["page_000"]["features"][0]["geometry"]["rings"][0]:
            point.append(3.5)
        with self.assertRaisesRegex(ValueError, "source_polygon_zm_flags_do_not_match_coordinates"):
            source._validate(_captures(docs))
        self.assertEqual(usace_dumping_ground_areas_usace_ienc.key.path,
                         ["ingest", source.DATASET_SLUG, source.FILE_SLUG])
        self.assertEqual(usace_dumping_ground_areas_draft_schedule.default_status.value, "STOPPED")

    def test_true_curves_are_requested_and_curve_rings_fail_closed(self) -> None:
        page_url = dict(source._capture_roster([1]))["page_000"]
        query = parse_qs(urlparse(page_url).query)
        self.assertEqual(query.get("returnTrueCurves"), ["true"])
        docs = _docs()
        geometry = docs["page_000"]["features"][0]["geometry"]
        geometry["curveRings"] = [[{"c": [[-90.0, 38.0], [-89.9, 38.1]]}]]
        with self.assertRaisesRegex(ValueError, "unsupported_true_curve_geometry"):
            source._validate(_captures(docs))


if __name__ == "__main__":
    unittest.main()

"""Focused contract checks for the FEMA NFHL Flood Hazard Zones source adapter."""
from __future__ import annotations

import hashlib
import json
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq

from dagster_hifld.sources import nfhl_flood_hazard_zones as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")

# Verbatim producer geometry: FEMA NFHL MapServer layer 4 (Base Index) OBJECTID
# 49181 (BASE_ID 12007C_421), captured anonymously from the producer's own
# service on 2026-11-12. Its second ring is a ~5.6e-8 by ~2.9e-8 degree sliver
# whose global-origin shoelace sum cancels to exactly 0.0; the true signed area
# is about -1.5128447588e-15 (50-digit decimal ground truth), so it is a
# clockwise exterior and the feature is a two-shell MultiPolygon, not one
# polygon with a hole lying outside its shell. This layer shares the service's
# global lon/lat coordinate scale and ring conventions.
TINY_SHELL_FEATURE_RINGS = [[[-82.03333484264579, 29.75000298614185], [-82.0371347907523, 29.750002987451523],
                             [-82.03713477634591, 29.75000358073339], [-82.03687580553589, 29.76046269741437],
                             [-82.03653537699432, 29.77420835336011], [-82.03619485546596, 29.787954001447872],
                             [-82.03585423833147, 29.801699642987217], [-82.03558646782858, 29.812502860532845],
                             [-82.03333490027143, 29.81250284612645], [-82.02916823717652, 29.812502850055466],
                             [-82.02500157146233, 29.81250285136514], [-82.02083490967715, 29.812502855294156],
                             [-82.0166682439629, 29.812502856603828], [-82.01250157955837, 29.812502860532845],
                             [-82.00833491646353, 29.81250286315219], [-82.00416825336862, 29.812502864461862],
                             [-82.00000159420279, 29.81250286839088], [-82.00000153002878, 29.750003017573988],
                             [-82.00416819050434, 29.75000301364497], [-82.0083348535992, 29.750003009715954],
                             [-82.01250151931339, 29.750003005786937], [-82.01666818371797, 29.75000300185792],
                             [-82.02083484943216, 29.750002997928902], [-82.02500151383668, 29.750002993999885],
                             [-82.02916817824126, 29.750002988761196], [-82.03333484264579, 29.75000298614185]],
                            [[-82.06250143978099, 29.75000295863873], [-82.06250149478728, 29.75000295732906],
                             [-82.06250149609696, 29.75000298614185], [-82.06250144109066, 29.75000298483218],
                             [-82.06250143978099, 29.75000295863873]]]


def _fixture():
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
        {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
        {"name": "FLD_ZONE", "type": "esriFieldTypeString", "alias": "FLD_ZONE", "length": 17, "domain": None},
        {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
        {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
        {"name": "SHAPE.STArea()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STArea()", "domain": None},
        {"name": "SHAPE.STLength()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STLength()", "domain": None},
    ]
    layer = {"id": 28, "name": "Flood Hazard Zones", "type": "Feature Layer",
             "geometryType": "esriGeometryPolygon", "maxRecordCount": 2000,
             "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"xmin": -100.0, "ymin": 20.0, "xmax": -60.0, "ymax": 45.0,
                        "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"mapName": "NFHL",
               "layers": [{"id": 28, "name": "Flood Hazard Zones", "type": "Feature Layer"}]}

    def attrs(oid):
        return {"OBJECTID": oid, "GFID": "ab1bd93a-570b-4cea-8ac3-782f01eaf93f", "FLD_ZONE": "AE",
                "GlobalID": "{9585E72B-E15E-4949-8B7A-6D7D29569B64}", "SHAPE.STArea()": 12.5,
                "SHAPE.STLength()": 30.25}

    exterior = [[-86.0, 34.0], [-86.0, 34.1], [-85.9, 34.1], [-85.9, 34.0], [-86.0, 34.0]]
    hole = [[-85.98, 34.04], [-85.96, 34.04], [-85.96, 34.06], [-85.98, 34.06], [-85.98, 34.04]]
    exterior2 = [[-86.5, 34.0], [-86.5, 34.05], [-86.4, 34.05], [-86.4, 34.0], [-86.5, 34.0]]
    features = [
        {"attributes": attrs(10), "geometry": {"rings": [exterior, hole]}},
        {"attributes": attrs(11), "geometry": None},
        {"attributes": attrs(12), "geometry": {"rings": [exterior2, [["NaN", "NaN"]]]}},
    ]
    ids = [10, 11, 12]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f}
              for f in fields]
    geom = [[10, [exterior, hole]], [11, None], [12, [exterior2, [["NaN", "NaN"]]]]]
    crs = {"wkid": 4269, "latestWkid": 4269}
    page0 = {"features": features[:2], "spatialReference": crs, "exceededTransferLimit": True}
    page1 = {"features": features[2:], "spatialReference": crs}
    docs = {"service_before": service, "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 3}, "count_after": {"count": 3},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "page_0000000_0002": page0, "page_0000002_0001": page1}
    return fields, features, docs, schema, geom


def _pins(features, schema, geom):
    return dict(EXPECTED_COUNT=3, SEGMENT_START_RECORDS=2,
                EXPECTED_NULL_GEOMETRY_COUNT=1, EXPECTED_NAN_GEOMETRY_COUNT=1,
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12])).hexdigest(),
                EXPECTED_RECORD_SHA256=hashlib.sha256(_canonical(features)).hexdigest(),
                EXPECTED_GEOMETRY_SHA256=hashlib.sha256(_canonical(geom)).hexdigest(),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(28, "Flood Hazard Zones", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -100.0, "ymin": 20.0, "xmax": -60.0, "ymax": 45.0},
                EXPECTED_OBSERVED_BOUNDS={"xmin": -86.5, "ymin": 34.0, "xmax": -85.9, "ymax": 34.1})


def _split(docs):
    """Return (page_bodies, complete_docs) for validation calls."""
    pages = {name: json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode()
             for name, doc in docs.items() if name.startswith("page_")}
    complete = dict(docs)
    complete["service_doc"] = SERVICE_DOC_BODY
    return pages, complete


class NfhlFloodHazardZonesValidationTests(unittest.TestCase):
    def test_validation_and_streaming_parquet_preserve_native_values(self):
        fields, features, docs, schema, geom = _fixture()
        import tempfile
        with tempfile.TemporaryDirectory() as temporary, patch.multiple(source, **_pins(features, schema, geom)):
            parquet_path = source.Path(temporary) / "candidate.parquet"
            _, original_fields, extracted = source._validate_captures(
                *_split(docs), [10, 11, 12], parquet_path=parquet_path)
            self.assertEqual(extracted["audit"]["source_feature_count"], 3)
            self.assertEqual(extracted["audit"]["null_geometry_count"], 1)
            self.assertEqual(extracted["audit"]["nan_string_geometry_count"], 1)
            table = pq.read_table(parquet_path)
            self.assertEqual(table.num_rows, 3)
            self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12])
            self.assertEqual(table.column("FLD_ZONE").to_pylist(), ["AE", "AE", "AE"])
            self.assertEqual(table.column("GlobalID").type.__str__(), "string")
            self.assertEqual(table.column("SHAPE").null_count, 2)
            geo = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)
            self.assertEqual(geo["columns"]["SHAPE"]["geometry_types"], ["Polygon", "MultiPolygon"])
            wkb = table.column("SHAPE").to_pylist()[0]
            import shapely
            polygon = shapely.from_wkb(wkb)
            self.assertEqual(polygon.geom_type, "Polygon")
            self.assertEqual(len(polygon.interiors), 1)

    def test_live_mode_observed_bounds_use_bounded_running_box(self):
        # The ~5.8M-feature population carries order 10^9 vertices; the observed
        # coverage box must come from a bounded running min/max, not from
        # collecting every ordinate, so validation memory stays bounded.
        _, _, docs, _, _ = _fixture()
        with patch.multiple(source, EXPECTED_SERVICE_LAYER_ROSTER=[(28, "Flood Hazard Zones", "Feature Layer")]):
            _, _, extracted = source._validate_captures(*_split(docs), [10, 11, 12])
        self.assertEqual(extracted["contract"]["expected_observed_bounds"],
                         {"xmin": -86.5, "ymin": 34.0, "xmax": -85.9, "ymax": 34.1})

    def test_hole_joins_smallest_containing_exterior_as_multipolygon(self):
        exterior_a = [[0.0, 0.0], [0.0, 10.0], [10.0, 10.0], [10.0, 0.0], [0.0, 0.0]]
        exterior_b = [[20.0, 0.0], [20.0, 10.0], [30.0, 10.0], [30.0, 0.0], [20.0, 0.0]]
        hole_b = [[21.0, 1.0], [22.0, 1.0], [22.0, 2.0], [21.0, 2.0], [21.0, 1.0]]
        wkb = source._polygon_wkb({"rings": [exterior_a, exterior_b, hole_b]})
        import shapely
        multipolygon = shapely.from_wkb(wkb)
        self.assertEqual(multipolygon.geom_type, "MultiPolygon")
        self.assertEqual(len(multipolygon.geoms), 2)
        self.assertEqual(len(multipolygon.geoms[0].interiors), 0)
        self.assertEqual(len(multipolygon.geoms[1].interiors), 1)

    def test_tiny_producer_exterior_ring_is_not_lost_to_cancellation(self):
        rings = TINY_SHELL_FEATURE_RINGS

        def naive_global_shoelace(ring):
            area = 0.0
            for (x1, y1), (x2, y2) in zip(ring, ring[1:] + [ring[0]]):
                area += x1 * y2 - x2 * y1
            return area / 2.0

        self.assertEqual(naive_global_shoelace(rings[1]), 0.0)
        self.assertLess(source._signed_area(rings[1]), 0.0)
        self.assertLess(source._signed_area(rings[0]), 0.0)
        import shapely
        geometry = shapely.from_wkb(source._polygon_wkb({"rings": rings}))
        self.assertEqual(geometry.geom_type, "MultiPolygon")
        self.assertEqual(len(geometry.geoms), 2)
        self.assertTrue(all(part.is_valid for part in geometry.geoms))
        self.assertTrue(geometry.is_valid)
        self.assertEqual(len(geometry.geoms[0].interiors), 0)
        self.assertEqual(len(geometry.geoms[1].interiors), 0)
        self.assertEqual(list(geometry.geoms[1].exterior.coords), [tuple(v) for v in rings[1]])
        self.assertEqual(list(geometry.geoms[0].exterior.coords), [tuple(v) for v in rings[0]])

    def test_signed_area_matches_decimal_ground_truth_at_global_scale(self):
        from decimal import Decimal, getcontext

        getcontext().prec = 60
        ring = TINY_SHELL_FEATURE_RINGS[1]
        total = Decimal(0)
        closed = ring + [ring[0]]
        for (x1, y1), (x2, y2) in zip(closed, closed[1:]):
            total += Decimal(x1) * Decimal(y2) - Decimal(x2) * Decimal(y1)
        exact = total / 2
        self.assertLess(float(exact), 0.0)
        self.assertLess(abs(source._signed_area(ring) - float(exact)), 1e-30)

    def test_nan_rings_never_become_geometry(self):
        self.assertIsNone(source._polygon_wkb({"rings": [[["NaN", "NaN"]]]}))

    def test_id_roster_response_order_is_not_population_drift(self):
        _, features, docs, schema, geom = _fixture()
        for name, ids in (("ids_before", [11, 12, 10]), ("ids_after", [12, 10, 11])):
            doc = json.loads(json.dumps(docs[name]))
            doc["objectIds"] = ids
            docs[name] = doc
        with patch.multiple(source, **_pins(features, schema, geom)):
            _, _, extracted = source._validate_captures(*_split(docs), [10, 11, 12])
        self.assertEqual(extracted["ids"], [10, 11, 12])

    def test_missing_service_documentation_link_is_rejected(self):
        _, features, docs, schema, geom = _fixture()
        complete = dict(docs)
        complete["service_doc"] = b"<html>unrelated</html>"
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "fema_service_documentation_link_drift"):
                source._validate_captures({name: doc for name, doc in complete.items()
                                           if name.startswith("page_")}, complete, [10, 11, 12])

    def test_crs_drift_is_rejected(self):
        _, features, docs, schema, geom = _fixture()
        docs["page_0000000_0002"] = json.loads(json.dumps(docs["page_0000000_0002"]))
        docs["page_0000000_0002"]["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_page_0000000_crs_drift"):
                source._validate_captures(*_split(docs), [10, 11, 12])

    def test_layer_geometry_type_drift_is_rejected(self):
        _, features, docs, schema, geom = _fixture()
        docs["layer_before"] = json.loads(json.dumps(docs["layer_before"]))
        docs["layer_before"]["geometryType"] = "esriGeometryPolyline"
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "exact_nfhl_flood_hazard_zones_layer_metadata_drift"):
                source._validate_captures(*_split(docs), [10, 11, 12])

    def test_unexpected_field_is_rejected_by_contract(self):
        _, features, docs, schema, geom = _fixture()
        docs["layer_before"] = json.loads(json.dumps(docs["layer_before"]))
        docs["layer_before"]["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_captures(*_split(docs), [10, 11, 12])

    def test_missing_page_id_is_not_accepted(self):
        _, features, docs, schema, geom = _fixture()
        docs["page_0000000_0002"] = json.loads(json.dumps(docs["page_0000000_0002"]))
        docs["page_0000000_0002"]["features"] = docs["page_0000000_0002"]["features"][:1]
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                source._validate_captures(*_split(docs), [10, 11, 12])

    def test_final_page_cannot_claim_transfer_truncation(self):
        _, features, docs, schema, geom = _fixture()
        docs["page_0000002_0001"] = json.loads(json.dumps(docs["page_0000002_0001"]))
        docs["page_0000002_0001"]["exceededTransferLimit"] = True
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
                source._validate_captures(*_split(docs), [10, 11, 12])

    def test_record_value_change_is_detected(self):
        _, features, docs, schema, geom = _fixture()
        docs["page_0000000_0002"] = json.loads(json.dumps(docs["page_0000000_0002"]))
        docs["page_0000000_0002"]["features"][0]["attributes"]["FLD_ZONE"] = "X"
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(*_split(docs), [10, 11, 12])

    def test_vertex_change_is_detected(self):
        _, features, docs, schema, geom = _fixture()
        docs["page_0000000_0002"] = json.loads(json.dumps(docs["page_0000000_0002"]))
        docs["page_0000000_0002"]["features"][0]["geometry"]["rings"][0][1][0] = -85.8
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_geometry_or_coverage_drift"):
                source._validate_captures(*_split(docs), [10, 11, 12])

    def test_observed_coverage_drift_is_detected(self):
        _, features, docs, schema, geom = _fixture()
        pins = _pins(features, schema, geom)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -86.5, "ymin": 34.0, "xmax": -85.9, "ymax": 34.2}
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
                source._validate_captures(*_split(docs), [10, 11, 12])

    def _docs_with_rotating_colorramp(self, docs, rotate: bool):
        """Mirror the live layer-28 renderer: both bracket docs carry the full
        drawingInfo; only authoringInfo.colorRamp.colorRamps palette colors
        rotate between fetches."""
        renderer = {"type": "uniqueValue", "field1": "FLD_ZONE", "field2": "ZONE_SUBTY",
                    "fieldDelimiter": ";", "uniqueValueInfos": [{"symbol": {"color": [9, 9, 9, 255]}}],
                    "authoringInfo": {"colorRamp": {"type": "multipart", "colorRamps": [
                        {"fromColor": [252, 242, 204, 255], "toColor": [211, 228, 252, 255],
                         "algorithm": "esriCIELabAlgorithm", "type": "algorithmic"}]}}}
        docs["layer_before"] = json.loads(json.dumps(dict(docs["layer_before"], drawingInfo={"renderer": renderer})))
        rotated = json.loads(json.dumps(renderer))
        if rotate:
            rotated["authoringInfo"]["colorRamp"]["colorRamps"] = [
                {"fromColor": [207, 220, 252, 255], "toColor": [207, 220, 252, 255],
                 "algorithm": "esriCIELabAlgorithm", "type": "algorithmic"}]
        docs["layer_after"] = json.loads(json.dumps(dict(docs["layer_after"], drawingInfo={"renderer": rotated})))
        return docs

    def test_rotating_colorramp_palette_between_layer_fetches_is_not_metadata_drift(self):
        # Regression for the 2026-11-12 live probe of layer 28: every layer
        # fetch rotates drawingInfo.renderer.authoringInfo.colorRamp palette
        # colors; only that palette may differ between the before/after
        # brackets (the merged sibling LOMRs module observed the same quirk).
        _, features, docs, schema, geom = _fixture()
        self._docs_with_rotating_colorramp(docs, rotate=True)
        with patch.multiple(source, **_pins(features, schema, geom)):
            _, _, extracted = source._validate_captures(*_split(docs), [10, 11, 12])
            self.assertEqual(extracted["audit"]["source_feature_count"], 3)

    def test_other_layer_metadata_change_is_still_rejected(self):
        _, features, docs, schema, geom = _fixture()
        self._docs_with_rotating_colorramp(docs, rotate=True)
        docs["layer_after"]["drawingInfo"]["renderer"]["type"] = "classBreaks"
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_layer_metadata_changed_during_capture"):
                source._validate_captures(*_split(docs), [10, 11, 12])

    def test_descending_objectid_order_is_rejected(self):
        _, features, docs, schema, geom = _fixture()
        docs["page_0000000_0002"] = json.loads(json.dumps(docs["page_0000000_0002"]))
        docs["page_0000000_0002"]["features"] = list(reversed(docs["page_0000000_0002"]["features"]))
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                source._validate_captures(*_split(docs), [10, 11, 12])

    def test_tiling_gap_is_rejected(self):
        _, features, docs, schema, geom = _fixture()
        del docs["page_0000002_0001"]
        with patch.multiple(source, **_pins(features, schema, geom)):
            with self.assertRaisesRegex(ValueError, "source_page_tiling_incomplete_or_overlapped"):
                source._validate_captures(*_split(docs), [10, 11, 12])


def _fake_capture_get(bodies):
    requested = []

    def fake(_client, url, capture_root, name, *, max_bytes):
        requested.append(name)
        body = bodies[name]
        digest = hashlib.sha256(body).hexdigest()
        receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-12T00:00:00+00:00",
                   "status": 200, "complete_body": True, "wire_bytes": len(body), "wire_sha256": digest,
                   "bytes": len(body), "sha256": digest, "content_encoding": None,
                   "content_length": str(len(body)), "error_code": None}
        (capture_root / f"{name}.body").write_bytes(body)
        (capture_root / f"{name}.wire").write_bytes(body)
        (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        return body, receipt

    return fake, requested


def _bodies_from_fixture():
    fields, features, docs, schema, geom = _fixture()
    pages, complete = _split(docs)
    bodies = {name: json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode()
              for name, doc in complete.items() if not isinstance(doc, bytes)}
    bodies.update(pages)
    bodies["service_doc"] = SERVICE_DOC_BODY
    # The producer refuses a whole-layer returnIdsOnly request (observed
    # 2026-11-12), so the capture assembles the roster from OBJECTID-range
    # chunks; the fixture roster [10, 11, 12] spans one window [10, 13).
    bodies["ids_stats"] = json.dumps({"features": [{"attributes": {"MIN_OBJECTID": 10, "MAX_OBJECTID": 12}}]},
                                     separators=(",", ":")).encode()
    range_body = json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": [10, 11, 12]},
                            separators=(",", ":")).encode()
    for prefix in ("ids_seed", "ids_before", "ids_after"):
        bodies[f"{prefix}_r000"] = range_body
    return fields, features, docs, schema, geom, bodies


class EmptyClient:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class NfhlFloodHazardZonesRosterTests(unittest.TestCase):
    """The producer refuses whole-layer returnIdsOnly; the roster is range-chunked."""

    def _roster(self, bodies, min_oid, max_oid, **extra_pins):
        import tempfile
        from pathlib import Path
        fake, requested = _fake_capture_get(bodies)
        pins = {"ROSTER_WINDOW": 5, "DOC_CAPTURE_BASE_DELAY_SECONDS": 0.01,
                "DOC_CAPTURE_ATTEMPTS": 1, **extra_pins}
        with tempfile.TemporaryDirectory(prefix="fhz-roster-") as temporary, \
                patch.multiple(source, **pins), \
                patch.object(source, "capture_get", side_effect=fake):
            return source._capture_roster(None, Path(temporary), "ids_seed", min_oid, max_oid), requested

    def test_roster_assembles_from_disjoint_windows_sorted(self):
        bodies = {
            "ids_seed_r000": json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": [14, 10, 11, 12]}).encode(),
            "ids_seed_r001": json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": [16, 15]}).encode(),
        }
        ids, requested = self._roster(bodies, 10, 16)
        self.assertEqual(ids, [10, 11, 12, 14, 15, 16])
        self.assertEqual(requested, ["ids_seed_r000", "ids_seed_r001"])

    def test_roster_range_with_wrong_id_field_is_rejected(self):
        bodies = {"ids_seed_r000": json.dumps({"objectIdFieldName": "FID", "objectIds": [10]}).encode()}
        with self.assertRaisesRegex(ValueError, "source_id_field_changed"):
            self._roster(bodies, 10, 12)

    def test_roster_range_with_non_integer_ids_is_rejected(self):
        bodies = {"ids_seed_r000": json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": [10, "11"]}).encode()}
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            self._roster(bodies, 10, 12)

    def test_roster_range_with_duplicate_ids_is_rejected(self):
        bodies = {"ids_seed_r000": json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": [10, 10]}).encode()}
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            self._roster(bodies, 10, 12)


class NfhlFloodHazardZonesCaptureTests(unittest.TestCase):
    def test_pins_pending_completion_writes_contract_and_stages_nothing(self):
        import os
        import tempfile
        fields, features, docs, schema, geom, bodies = _bodies_from_fixture()
        fake, _ = _fake_capture_get(bodies)
        pins = {"SEGMENT_START_RECORDS": 2,
                "EXPECTED_SERVICE_LAYER_ROSTER": [(28, "Flood Hazard Zones", "Feature Layer")],
                "DOC_CAPTURE_BASE_DELAY_SECONDS": 0.01, "SEGMENT_PROBE_BASE_DELAY_SECONDS": 0.01}
        with tempfile.TemporaryDirectory(prefix="fhz-pins-pending-") as temporary, \
                patch.multiple(source, **pins), \
                patch.object(source, "capture_get", side_effect=fake), \
                patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
            from pathlib import Path
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with self.assertRaisesRegex(source._PinsPendingError, "source_population_pins_not_established"):
                source.capture_nfhl_flood_hazard_zones(storage, version="vfixture",
                                                       capture_directory=str(root / "captures"))
            self.assertEqual(set(storage.list_prefix()), set())
            attempt = next((root / "captures").iterdir())
            contract = json.loads((attempt / "population_pins.json").read_bytes())
            self.assertEqual(contract["expected_count"], 3)
            self.assertEqual(contract["expected_null_geometry_count"], 1)
            self.assertEqual(contract["expected_nan_geometry_count"], 1)
            self.assertIn("expected_record_sha256", contract)
            manifest = json.loads((attempt / "capture_manifest.json").read_bytes())
            self.assertTrue(manifest["complete"])
            self.assertFalse(manifest["staged"])

    def test_oid_stats_drift_fails_closed(self):
        import tempfile
        from pathlib import Path
        fields, features, docs, schema, geom, bodies = _bodies_from_fixture()
        bodies["ids_stats"] = json.dumps({"features": [{"attributes": {"MIN_OBJECTID": 12, "MAX_OBJECTID": 10}}]},
                                         separators=(",", ":")).encode()
        fake, _ = _fake_capture_get(bodies)
        pins = {"SEGMENT_START_RECORDS": 2,
                "EXPECTED_SERVICE_LAYER_ROSTER": [(28, "Flood Hazard Zones", "Feature Layer")],
                "DOC_CAPTURE_BASE_DELAY_SECONDS": 0.01, "SEGMENT_PROBE_BASE_DELAY_SECONDS": 0.01}
        with tempfile.TemporaryDirectory(prefix="fhz-stats-") as temporary, \
                patch.multiple(source, **pins), \
                patch.object(source, "capture_get", side_effect=fake), \
                patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with self.assertRaisesRegex(ValueError, "source_oid_stats_drift"):
                source.capture_nfhl_flood_hazard_zones(storage, version="vfixture",
                                                       capture_directory=str(root / "captures"))
            self.assertEqual(set(storage.list_prefix()), set())

    def test_oversized_or_refused_wide_segment_is_bisected(self):
        # Regression for the 2026-11-12 live session: this layer's dense head
        # region makes wide pages exceed the module's capture cap, and the
        # producer refuses them, so a wide segment whose capture is refused
        # must bisect instead of aborting the session.
        import tempfile
        from pathlib import Path
        fields, features, docs, schema, geom, bodies = _bodies_from_fixture()
        wide_bodies = dict(bodies)

        def single(feature):
            return json.dumps({"features": [feature],
                               "spatialReference": {"wkid": 4269, "latestWkid": 4269},
                               "exceededTransferLimit": True},
                              separators=(",", ":"), ensure_ascii=False).encode()

        wide_bodies["page_0000000_0001"] = single(features[0])
        wide_bodies["page_0000001_0001"] = single(features[1])

        def refused_wide(_client, url, capture_root, name, *, max_bytes):
            if name == "page_0000000_0002":
                raise ValueError("incomplete_source_capture:" + name)
            return _fake_capture_get(wide_bodies)[0](_client, url, capture_root, name, max_bytes=max_bytes)

        pins = dict(_pins(features, schema, geom), SEGMENT_START_RECORDS=2,
                    SEGMENT_PROBE_ATTEMPTS=1, SEGMENT_PROBE_BASE_DELAY_SECONDS=0.01,
                    DOC_CAPTURE_BASE_DELAY_SECONDS=0.01)
        with tempfile.TemporaryDirectory(prefix="fhz-bisect-") as temporary, \
                patch.multiple(source, **pins), \
                patch.object(source, "capture_get", side_effect=refused_wide), \
                patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            candidate = source.capture_nfhl_flood_hazard_zones(storage, version="vfixture",
                                                               capture_directory=str(root / "captures"))
            attempt = next(path for path in (root / "captures").iterdir()
                           if path.name.startswith("vfixture-"))
            plan = json.loads((attempt / "segments.json").read_bytes())
            self.assertEqual([list(pair) for pair in plan["accepted"]],
                             [[0, 1], [1, 1], [2, 1]])
            self.assertEqual(candidate["feature_count"], 3)

    def test_resume_reuses_cached_segment_bodies(self):
        import tempfile
        from pathlib import Path
        fields, features, docs, schema, geom, bodies = _bodies_from_fixture()
        fake, _ = _fake_capture_get(bodies)
        pins = dict(_pins(features, schema, geom), SEGMENT_PROBE_BASE_DELAY_SECONDS=0.01,
                    DOC_CAPTURE_BASE_DELAY_SECONDS=0.01)
        with tempfile.TemporaryDirectory(prefix="fhz-resume-") as temporary, \
                patch.multiple(source, **pins), \
                patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.object(source, "capture_get", side_effect=fake):
                first = source.capture_nfhl_flood_hazard_zones(storage, version="vfixture",
                                                               capture_directory=str(root / "captures"))
            self.assertFalse(first.get("reused"))
            attempt = next(path for path in (root / "captures").iterdir()
                           if path.name.startswith("vfixture-"))
            # Seed a second, resumable attempt from the first one's page evidence.
            resume_root = root / "captures" / "vfixture2-resume"
            resume_root.mkdir()
            for name in ("page_0000000_0002",):
                for suffix in (".body", ".wire", ".receipt.json"):
                    (resume_root / f"{name}{suffix}").write_bytes((attempt / f"{name}{suffix}").read_bytes())
            (resume_root / "segments.json").write_bytes(_canonical({"accepted": [[0, 2]]}))
            fake2, requested2 = _fake_capture_get(bodies)
            with patch.object(source, "capture_get", side_effect=fake2):
                second = source.capture_nfhl_flood_hazard_zones(storage, version="vfixture2",
                                                                capture_directory=str(root / "captures"))
            self.assertEqual(second["feature_count"], 3)
            self.assertNotIn("page_0000000_0002", requested2)
            self.assertIn("page_0000002_0001", requested2)

    def test_ambiguous_resume_attempts_fail_closed(self):
        import tempfile
        from pathlib import Path
        fields, features, docs, schema, geom, bodies = _bodies_from_fixture()
        fake, requested = _fake_capture_get(bodies)
        pins = _pins(features, schema, geom)
        with tempfile.TemporaryDirectory(prefix="fhz-ambiguous-") as temporary, \
                patch.multiple(source, **pins), \
                patch.object(source, "capture_get", side_effect=fake), \
                patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            captures = root / "captures"
            (captures / "vfixture-attempt-a").mkdir(parents=True)
            (captures / "vfixture-attempt-b").mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "ambiguous_resume_capture_attempts"):
                source.capture_nfhl_flood_hazard_zones(storage, version="vfixture",
                                                       capture_directory=str(captures))
            self.assertEqual(requested, [])

    def test_failure_manifest_is_written_and_capture_root_is_resumable(self):
        import tempfile
        from pathlib import Path
        fields, features, docs, schema, geom, bodies = _bodies_from_fixture()
        fake, requested = _fake_capture_get(bodies)

        def failing(_client, url, capture_root, name, *, max_bytes):
            if name == "page_0000002_0001":
                raise ValueError("incomplete_source_capture:stream_truncated")
            return fake(_client, url, capture_root, name, max_bytes=max_bytes)

        pins = dict(_pins(features, schema, geom), SEGMENT_PROBE_ATTEMPTS=1,
                    SEGMENT_PROBE_BASE_DELAY_SECONDS=0.01, DOC_CAPTURE_BASE_DELAY_SECONDS=0.01)
        with tempfile.TemporaryDirectory(prefix="fhz-failure-") as temporary, \
                patch.multiple(source, **pins), \
                patch.object(source, "capture_get", side_effect=failing), \
                patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with self.assertRaisesRegex(ValueError, "incomplete_source_capture"):
                source.capture_nfhl_flood_hazard_zones(storage, version="vfixture",
                                                       capture_directory=str(root / "captures"))
            self.assertEqual(set(storage.list_prefix()), set())
            attempt = next((root / "captures").iterdir())
            manifest = json.loads((attempt / "capture_manifest.json").read_bytes())
            self.assertFalse(manifest["complete"])
            self.assertTrue(manifest["resumable"])

    def test_tiny_capture_seal_then_cache_reuse(self):
        import tempfile
        from pathlib import Path
        fields, features, docs, schema, geom, bodies = _bodies_from_fixture()
        fake, _ = _fake_capture_get(bodies)
        pins = dict(_pins(features, schema, geom), SEGMENT_PROBE_BASE_DELAY_SECONDS=0.01,
                    DOC_CAPTURE_BASE_DELAY_SECONDS=0.01)
        with tempfile.TemporaryDirectory(prefix="fhz-e2e-") as temporary, \
                patch.multiple(source, **pins), \
                patch.object(source, "capture_get", side_effect=fake), \
                patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
            from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
            from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            candidate = source.capture_nfhl_flood_hazard_zones(storage, version="vfixture",
                                                               capture_directory=str(root / "captures"))
            self.assertEqual(candidate["feature_count"], 3)
            self.assertEqual(candidate["source_null_geometry_count"], 1)
            # Local staging consumes each raw evidence file as it is staged
            # (verified against its descriptor first): the sealed candidate
            # holds the durable copies, and this layer's tens-of-GB raw
            # staging stays within one disk.
            capture_dirs = list((root / "captures").glob("vfixture-*"))
            self.assertEqual(len(capture_dirs), 1)
            self.assertFalse(list(capture_dirs[0].glob("page_*.body")))
            self.assertFalse(list(capture_dirs[0].glob("page_*.wire")))
            self.assertTrue(list(capture_dirs[0].glob("page_*.receipt.json")))
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
            snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG, candidate["version"])
            manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                     candidate["version"]).metadata
            summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                               candidate["version"], source.DATASET_SLUG,
                                               source_metadata=manifest)
            write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG, file_slug=source.FILE_SLUG,
                                   version=candidate["version"], quality_dict=summary.quality_manifest,
                                   dictionary_dict=summary.data_dictionary)
            table = pq.read_table(storage.download_key_to(f"{prefix}/geoparquet/{source.DATASET_SLUG}.parquet",
                                                          root / "candidate.parquet") or
                                  (root / "candidate.parquet"))
            self.assertEqual(table.num_rows, 3)
            reused = source.capture_nfhl_flood_hazard_zones(storage, version="vfixture",
                                                            capture_directory=str(root / "captures"))
            self.assertTrue(reused["reused"])
            self.assertEqual(reused["version"], candidate["version"])

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        import os
        import tempfile

        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          f"geoparquet/{source.DATASET_SLUG}.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           f"geoparquet/{source.DATASET_SLUG}.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError,
                                            "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, source.DATASET_SLUG,
                                        source.FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)


class NfhlFloodHazardZonesWiringTests(unittest.TestCase):
    def test_module_lists_and_definitions_are_registered(self):
        from dagster_hifld.assets import nfhl_flood_hazard_zones as wiring

        self.assertEqual([asset.key.to_user_string() for asset in wiring.INGEST_ASSETS],
                         [f"ingest/{source.DATASET_SLUG}/{source.FILE_SLUG}"])
        self.assertEqual(len(wiring.ASSET_CHECKS), 1)
        self.assertEqual(wiring.nfhl_flood_hazard_zones_draft_schedule.default_status,
                         wiring.DefaultScheduleStatus.STOPPED)

    def test_discovery_collects_the_dataset(self):
        from dagster_hifld.assets.discovery import discovered

        names = [asset.key.to_user_string() for asset in discovered("INGEST_ASSETS")]
        self.assertIn(f"ingest/{source.DATASET_SLUG}/{source.FILE_SLUG}", names)


if __name__ == "__main__":
    unittest.main()

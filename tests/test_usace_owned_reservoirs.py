import contextlib
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from shapely import from_wkb

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_owned_reservoirs import (
    usace_owned_reservoirs, usace_owned_reservoirs_complete,
    usace_owned_reservoirs_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import usace_owned_reservoirs as source
from dagster_hifld.sources.epa_frs_common import _canonical


@contextlib.contextmanager
def relaxed_pins(*extra):
    with contextlib.ExitStack() as stack:
        for item in (
            patch.object(source, "EXPECTED_RECORD_SHA256", ""),
            patch.object(source, "EXPECTED_GEOMETRY_SHA256", ""),
            patch.object(source, "EXPECTED_NULL_GEOMETRY_COUNT", None),
            patch.object(source, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS", None),
            *extra,
        ):
            stack.enter_context(item)
        yield


class UsaceOwnedReservoirsTests(unittest.TestCase):
    def _shell(self, oid):
        x0 = -105.0 + (oid % 40) * 0.7
        y0 = 30.0 + (oid // 40) * 0.4
        return [[x0, y0], [x0, y0 + 0.3], [x0 + 0.2, y0 + 0.3], [x0 + 0.2, y0], [x0, y0]]

    def _hole(self, shell):
        inset = 0.05
        x0, y0 = shell[0]
        x1, y1 = shell[2]
        return [[x0 + inset, y0 + inset], [x1 - inset, y0 + inset],
                [x1 - inset, y1 - inset], [x0 + inset, y1 - inset],
                [x0 + inset, y0 + inset]]

    def _bowtie(self, oid):
        x0 = -105.0 + (oid % 40) * 0.7
        y0 = 30.0 + (oid // 40) * 0.4
        return [[x0, y0], [x0 + 0.1, y0], [x0, y0 + 0.2], [x0 + 0.3, y0 + 0.2], [x0, y0]]

    def _overlapping_shells(self, oid):
        x0 = -105.0 + (oid % 40) * 0.7
        y0 = 30.0 + (oid // 40) * 0.4
        first = [[x0, y0], [x0, y0 + 0.4], [x0 + 0.3, y0 + 0.4], [x0 + 0.3, y0], [x0, y0]]
        second = [[x0 + 0.2, y0 + 0.1], [x0 + 0.2, y0 + 0.5], [x0 + 0.5, y0 + 0.5],
                  [x0 + 0.5, y0 + 0.1], [x0 + 0.2, y0 + 0.1]]
        return [first, second]

    def _gate_features(self):
        return {oid: mode for oid, mode in (
            (11, "source_polygon_ring_degenerate"), (13, "source_polygon_ring_degenerate"),
            (15, "source_polygon_ring_degenerate"), (17, "source_polygon_ring_degenerate"),
            (19, "source_polygon_ring_degenerate"), (21, "source_polygon_ring_degenerate"),
            (23, "source_polygon_ring_self_intersecting"), (25, "source_polygon_ring_self_intersecting"),
            (27, "source_polygon_ring_self_intersecting"), (29, "source_polygon_ring_self_intersecting"),
            (31, "source_polygon_ring_self_intersecting"), (33, "source_polygon_ring_self_intersecting"),
            (35, "source_polygon_ring_self_intersecting"), (37, "source_polygon_ring_self_intersecting"),
            (39, "source_polygon_ring_self_intersecting"),
            (41, "source_polygon_assembly_invalid"))}

    def _feature(self, oid):
        gates = self._gate_features()
        attrs = {}
        for field in source.ATTRIBUTE_FIELDS:
            name, kind = field["name"], field["type"]
            if name == "OBJECTID":
                attrs[name] = oid
            elif kind == "esriFieldTypeString":
                attrs[name] = f"{name}-{oid}"
            else:
                attrs[name] = None
        rings = [self._shell(oid)]
        if oid % 7 == 3 and oid not in gates:
            rings.append(self._hole(rings[0]))
        if gates.get(oid) == "source_polygon_ring_degenerate":
            rings.append([[-100.0, 33.0]] * 4)
        if gates.get(oid) == "source_polygon_ring_self_intersecting":
            rings = [self._bowtie(oid)]
        if gates.get(oid) == "source_polygon_assembly_invalid":
            rings = self._overlapping_shells(oid)
        if oid not in gates:
            decoded = from_wkb(source._assemble_geometry({"rings": rings}))
            attrs["Shape__Area"] = decoded.area
            attrs["Shape__Length"] = decoded.length
        else:
            attrs["Shape__Area"] = 0.5
            attrs["Shape__Length"] = 3.0
        return {"attributes": attrs, "geometry": {"rings": rings}}

    def _documents(self):
        sr = {"wkid": 4326, "latestWkid": 4326}
        service = {"serviceItemId": source.CATALOG_ITEM_ID,
                   "description": source.ITEM_DESCRIPTION,
                   "serviceDescription": source.SERVICE_DESCRIPTION,
                   "capabilities": "Query,Extract", "maxRecordCount": 1000,
                   "hasVersionedData": False, "tables": [], "spatialReference": sr,
                   "fullExtent": {**source.EXPECTED_EXTENT, "spatialReference": sr},
                   "layers": [{"id": 0, "name": source.LAYER_NAME, "parentLayerId": -1,
                               "subLayerIds": None, "type": "Feature Layer",
                               "geometryType": "esriGeometryPolygon"}]}
        layer = {"id": 0, "name": source.LAYER_NAME, "geometryType": "esriGeometryPolygon",
                 "maxRecordCount": 1000, "objectIdField": "OBJECTID", "globalIdField": "",
                 "typeIdField": "", "hasZ": False, "hasM": False,
                 "editingInfo": dict(source.EXPECTED_EDITING_INFO),
                 "spatialReference": sr,
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": sr},
                 "advancedQueryCapabilities": {"supportsPagination": True},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "catalog_item": {"id": source.CATALOG_ITEM_ID, "owner": source.CATALOG_ITEM_OWNER,
                             "type": "Feature Service", "title": source.LAYER_NAME,
                             "description": source.ITEM_DESCRIPTION,
                             "licenseInfo": source.ITEM_LICENSE,
                             "spatialReference": "4326",
                             "extent": [[source.EXPECTED_EXTENT["xmin"], source.EXPECTED_EXTENT["ymin"]],
                                        [source.EXPECTED_EXTENT["xmax"], source.EXPECTED_EXTENT["ymax"]]]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            "service_after": json.loads(json.dumps(service)), "layer_after": json.loads(json.dumps(layer)),
            "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS},
        }
        page_fields = [{key: value for key, value in field.items()
                        if key not in ("nullable", "editable")}
                       for field in source.EXPECTED_FIELD_DECLARATIONS]
        features_by_oid = {oid: self._feature(oid) for oid in source.EXPECTED_IDS}
        for page_index in range((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
            page_ids = source.EXPECTED_IDS[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]
            docs[f"page_{page_index:03d}"] = {"objectIdFieldName": "OBJECTID",
                                              "geometryType": "esriGeometryPolygon",
                                              "geometryProperties": dict(source.EXPECTED_GEOMETRY_PROPERTIES),
                                              "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                                              "fields": page_fields,
                                              "features": [features_by_oid[oid] for oid in page_ids]}
        return docs

    def _captures(self, docs):
        return {name: (json.dumps(value, ensure_ascii=False, allow_nan=False).encode(), {"status": 200})
                for name, value in docs.items()}

    def test_pinned_source_constants_match_verified_evidence(self):
        self.assertEqual(source.DATASET_SLUG, "usace-owned-reservoirs")
        self.assertEqual(source.CATALOG_ITEM_ID, "03e322d7e89b48a9b48e9c3f4bcaf29e")
        self.assertEqual(source.CATALOG_ITEM_OWNER, "usace_erd_aep")
        self.assertEqual(source.LAYER_URL, "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/"
                         "arcgis/rest/services/usace_rez/FeatureServer/0")
        self.assertEqual(source.LANDING_URL, "https://geospatial-usace.opendata.arcgis.com/"
                         "datasets/03e322d7e89b48a9b48e9c3f4bcaf29e_0/about")
        self.assertEqual(source.EXPECTED_COUNT, 418)
        self.assertNotIn(10, source.EXPECTED_IDS)
        self.assertEqual(len(source.EXPECTED_IDS), 418)
        self.assertEqual(source.EXPECTED_ID_SHA256,
                         "9da9b23698cc6db17475b2218ddef9da4fbb3aff0daa9fa1e6007644fe5c362f")
        self.assertEqual(source.EXPECTED_SCHEMA_SHA256,
                         "319daded87c7314f171a5229ff3fb163ddd17832f3bd96ac83fec3d9d9b57569")
        self.assertEqual(source.EXPECTED_RECORD_SHA256,
                         "39d8c519a236ab6543b4c96d831196f3eb2a4bc3ce79c86d8cb95192dac0aa45")
        self.assertEqual(source.EXPECTED_GEOMETRY_SHA256,
                         "2be036dbdd8ef51c9b6a1ff5d7c9d0bd827d6a2f52f6de3538380fd2bc09ed03")
        self.assertEqual(source.EXPECTED_NULL_GEOMETRY_COUNT, 16)
        self.assertEqual(source.EXPECTED_UNUSABLE_GEOMETRY_COUNTS, {
            "source_polygon_ring_degenerate": 6,
            "source_polygon_ring_self_intersecting": 9,
            "source_polygon_assembly_invalid": 1})
        self.assertEqual(source.OBSERVED_VERTEX_COUNT, 2134520)
        self.assertEqual(source.OBSERVED_WINDING_COUNTS, {"cw_shells": 3971, "ccw_holes": 8461})
        self.assertEqual(source.EXPECTED_LAST_DATA_EDIT_UTC, "2024-04-02T15:55:29.726000Z")
        self.assertEqual(source.EXPECTED_EDITING_INFO["dataLastEditDate"], 1712070929726)
        self.assertEqual(source._sha(_canonical(source.EXPECTED_FIELD_DECLARATIONS)),
                         source.EXPECTED_SCHEMA_SHA256)
        self.assertEqual(source._sha(_canonical(source.EXPECTED_IDS)), source.EXPECTED_ID_SHA256)

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(usace_owned_reservoirs, hifld_ingest_assets)
        self.assertEqual(usace_owned_reservoirs_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = usace_owned_reservoirs_complete({"feature_count": 418, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = usace_owned_reservoirs_complete({"feature_count": 417, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        page_names = [name for name in sorted(urls) if name.startswith("page_")]
        self.assertEqual(page_names, ["page_000", "page_001", "page_002", "page_003", "page_004"])
        covered = []
        for name in page_names:
            query = urls[name]
            self.assertIn("outSR=4326", query)
            self.assertIn("returnZ=false", query)
            self.assertIn("returnM=false", query)
            self.assertIn("orderByFields=OBJECTID+ASC", query)
            payload = query.split("objectIds=")[1].split("&")[0]
            covered.extend(int(value) for value in payload.replace("%2C", ",").split(","))
        self.assertEqual(sorted(covered), source.EXPECTED_IDS)
        self.assertEqual(len(covered), len(set(covered)))
        self.assertEqual(source.CATALOG_ITEM_URL, "https://www.arcgis.com/sharing/rest/content/items/"
                         "03e322d7e89b48a9b48e9c3f4bcaf29e?f=json")

    def test_complete_native_population_and_geometry_are_validated(self):
        with relaxed_pins():
            fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 418)
        self.assertNotIn(10, [feature["attributes"]["OBJECTID"] for feature in features])
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["dhs_crosswalk_row"]["status"], "Deprecate")
        self.assertEqual(audit["dhs_crosswalk_row"]["old_hosting_item_id"],
                         "e8abf1e31e2644458a9cd62145f9048c")
        self.assertIn("USACE-hosted services7 FeatureServer", audit["source_coverage_scope"])
        self.assertEqual(audit["source_last_data_edit_utc"], "2024-04-02T15:55:29.726000Z")
        gated = [feature for feature in features if "_gate" in feature]
        self.assertEqual(len(gated), 16)
        self.assertEqual(sorted(feature["attributes"]["OBJECTID"] for feature in gated),
                         list(self._gate_features()))
        self.assertEqual(audit["null_geometry_count"], 16)
        fixture_counts = {}
        for mode in self._gate_features().values():
            fixture_counts[mode] = fixture_counts.get(mode, 0) + 1
        self.assertEqual(audit["unusable_geometry_counts"], dict(sorted(fixture_counts.items())))
        self.assertEqual(audit["geometry_valid_count"], 402)

    def test_gate_pins_are_enforced_once_pinned(self):
        fixture_counts = {}
        for mode in self._gate_features().values():
            fixture_counts[mode] = fixture_counts.get(mode, 0) + 1
        with relaxed_pins():
            audit = source._validate(self._captures(self._documents()))[2]
            self.assertEqual(audit["unusable_geometry_counts"], fixture_counts)
            self.assertEqual(audit["null_geometry_count"], 16)
        with relaxed_pins(patch.object(source, "EXPECTED_NULL_GEOMETRY_COUNT", 16),
                          patch.object(source, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS", fixture_counts)):
            source._validate(self._captures(self._documents()))
            with self.assertRaisesRegex(ValueError, "source_null_geometry_count_drift"):
                with patch.object(source, "EXPECTED_NULL_GEOMETRY_COUNT", 15):
                    source._validate(self._captures(self._documents()))
            with self.assertRaisesRegex(ValueError, "source_unusable_geometry_counts_drift"):
                with patch.object(source, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS",
                                  {**fixture_counts, "source_polygon_ring_degenerate": 5}):
                    source._validate(self._captures(self._documents()))

    def test_polygon_wkb_is_xy_only_and_preserves_ring_structure(self):
        shell = self._shell(1)
        single = source._assemble_geometry({"rings": [shell]})
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 3))
        disjoint = source._assemble_geometry({"rings": [shell, self._shell(2)]})
        self.assertEqual(struct.unpack("<BI", disjoint[:5]), (1, 6))
        with_hole = source._assemble_geometry({"rings": [shell, self._hole(shell)]})
        self.assertEqual(struct.unpack("<BI", with_hole[:5]), (1, 3))
        decoded = from_wkb(with_hole)
        self.assertEqual(len(decoded.interiors), 1)
        for flags in ({"hasZ": True}, {"hasM": True}, {"hasZ": True, "hasM": True}):
            with relaxed_pins():
                docs = self._documents()
                docs["page_000"]["features"][0]["geometry"].update(flags)
                with self.assertRaisesRegex(ValueError, "source_geometry_dimensional_drift"):
                    source._validate(self._captures(docs))
        unflagged = [[0.0, 0.0, 1.0], [0.0, 1.0, 1.0], [1.0, 1.0, 1.0], [1.0, 0.0, 1.0], [0.0, 0.0, 1.0]]
        with self.assertRaisesRegex(Exception, "source_polygon_ordinate_not_xy"):
            source._assemble_geometry({"rings": [unflagged]})
        for mode in ("source_polygon_ring_degenerate", "source_polygon_ring_self_intersecting",
                     "source_polygon_assembly_invalid", "source_polygon_exterior_missing"):
            self.assertIn(mode, source.GATE_MODES)

    def test_assembly_gate_modes_fail_closed_per_row(self):
        shell = self._shell(1)
        for rings, mode in (
                ([[-100.0, 33.0]] * 4, "source_polygon_ring_degenerate"),
                ([self._bowtie(1)], "source_polygon_ring_self_intersecting"),
                (self._overlapping_shells(1), "source_polygon_assembly_invalid"),
                ([self._hole(shell)], "source_polygon_exterior_missing"),
                ([[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0]]], "source_polygon_ring_not_closed")):
            with self.assertRaisesRegex(Exception, mode):
                source._assemble_geometry({"rings": rings})

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population after", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_id_population_drift:after"),
            ("count before", lambda d: d["count_before"].update(count=417),
             "source_count_population_drift:before"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
            ("transfer", lambda d: d["page_000"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_000"].update(spatialReference={"wkid": 3857}),
             "source_page_000_page_crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                OBJECTID=d["page_000"]["features"][0]["attributes"]["OBJECTID"]),
             "source_page_id_population_drift"),
            ("layer dimensional", lambda d: d["layer_before"].update(hasZ=True),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer freshness", lambda d: d["layer_before"]["editingInfo"].update(dataLastEditDate=1712070929727),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer extent", lambda d: d["layer_before"]["extent"].update(xmax=0.0),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer type field", lambda d: d["layer_before"].update(typeIdField="DRY"),
             "source_layer_identity_extent_or_freshness_drift"),
            ("page dimensional", lambda d: d["page_000"].update(hasZ=True),
             "source_page_dimensional_drift"),
            ("page identity", lambda d: d["page_000"].update(objectIdFieldName="FID"),
             "source_page_identity_drift"),
            ("page geometry properties", lambda d: d["page_000"].update(geometryProperties={"units": "esriMeters"}),
             "source_page_geometry_properties_drift"),
            ("page field drift", lambda d: d["page_000"]["fields"][2].update(name="renamed"),
             "source_page_physical_schema_changed"),
            ("geometry dimensional", lambda d: d["page_000"]["features"][0]["geometry"].update(hasZ=True),
             "source_geometry_dimensional_drift"),
            ("open ring", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].pop(),
             "source_polygon_ring_not_closed"),
            ("outside coverage", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].__setitem__(
                2, [-185.0, 35.5]),
             "source_geometry_outside_declared_coverage"),
            ("item identity", lambda d: d["catalog_item"].update(owner="someone_else"),
             "usace_catalog_item_identity_drift"),
            ("item license text", lambda d: d["catalog_item"].update(licenseInfo="changed"),
             "usace_catalog_item_text_drift"),
            ("item extent", lambda d: d["catalog_item"].update(extent=[[0.0, 0.0], [1.0, 1.0]]),
             "usace_catalog_item_extent_drift"),
            ("service identity", lambda d: d["service_before"].update(serviceItemId="other"),
             "service_before_service_identity_drift"),
            ("service tables", lambda d: d["service_before"].update(tables=[{"id": 1}]),
             "service_before_service_identity_drift"),
            ("service roster", lambda d: d["service_before"]["layers"].append(
                {"id": 1, "name": "Extra", "parentLayerId": -1, "subLayerIds": None,
                 "type": "Feature Layer", "geometryType": "esriGeometryPolygon"}),
             "service_before_service_layer_roster_drift"),
            ("service changed during capture", lambda d: d["service_after"].update(maxRecordCount=2000),
             "source_service_or_layer_changed_during_capture"),
            ("string value type", lambda d: d["page_000"]["features"][0]["attributes"].update(NAME=5),
             "source_string_value_type_drift:NAME"),
            ("double value type", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Area="x"),
             "source_double_value_type_drift:Shape__Area"),
            ("area parity", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Area=9.0),
             "source_shape_area_parity_drift"),
            ("length parity", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Length=9.0),
             "source_shape_length_parity_drift"),
            ("attribute order", lambda d: d["page_000"]["features"][0]["attributes"].pop("NAME"),
             "source_attribute_roster_or_order_drift"),
            ("oid value", lambda d: d["page_000"]["features"][0]["attributes"].update(OBJECTID=0),
             "source_oid_value_type_drift"),
            ("oid roster", lambda d: d["page_000"]["features"][0]["attributes"].update(OBJECTID=419),
             "source_feature_id_roster_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with relaxed_pins():
                    with self.assertRaisesRegex(ValueError, expected):
                        source._validate(self._captures(docs))

    def test_pinned_baseline_drift_is_refused(self):
        with relaxed_pins():
            audit = source._validate(self._captures(self._documents()))[2]
        with relaxed_pins(patch.object(source, "EXPECTED_RECORD_SHA256", audit["source_record_sha256"]),
                          patch.object(source, "EXPECTED_GEOMETRY_SHA256", audit["source_geometry_sha256"]),
                          patch.object(source, "EXPECTED_NULL_GEOMETRY_COUNT", audit["null_geometry_count"]),
                          patch.object(source, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS",
                                       audit["unusable_geometry_counts"])):
            source._validate(self._captures(self._documents()))
            docs = self._documents()
            docs["page_000"]["features"][0]["attributes"]["NAME"] = "Renamed Reservoir"
            with self.assertRaisesRegex(ValueError, "source_record_baseline_drift"):
                source._validate(self._captures(docs))
            docs = self._documents()
            ring = docs["page_000"]["features"][0]["geometry"]["rings"][0]
            ring.insert(2, [ring[1][0] + (ring[2][0] - ring[1][0]) / 2, ring[1][1]])
            with self.assertRaisesRegex(ValueError, "source_geometry_baseline_drift"):
                source._validate(self._captures(docs))

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_usace_owned_reservoirs(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

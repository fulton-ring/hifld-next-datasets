import unittest
import json

from dagster_hifld.assets.nces_private_schools import nces_private_schools_draft_schedule
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import CATALOG_METADATA_FIELDS
from dagster_hifld.sources.nces_private_schools import (
    EXPECTED_COUNT,
    EXPECTED_FIELDS,
    EXPECTED_MEMBERS,
    EXPECTED_SERVICE_COUNT,
    EXPECTED_SERVICE_FIELDS,
    EXPECTED_SERVICE_IDS_SHA256,
    EXPECTED_SERVICE_WKID,
    SOURCE_RELEASE,
    SOURCE_URL,
    _point_wkb,
    _service_evidence,
)


def _service_bodies():
    service = {"mapName": "EDGE_GEOCODE_PRIVATESCH_2324", "layers": [{"id": 0, "name": "Private School Locations 2023-24"}]}
    layer = {
        "name": "Private School Locations 2023-24",
        "geometryType": "esriGeometryPoint",
        "maxRecordCount": 2000,
        "fields": [{"name": name} for name in EXPECTED_SERVICE_FIELDS],
        "extent": {"spatialReference": {"wkid": EXPECTED_SERVICE_WKID, "latestWkid": EXPECTED_SERVICE_WKID}},
    }
    ids = list(range(1, EXPECTED_SERVICE_COUNT + 1))
    return {
        "service_before": json.dumps(service).encode(),
        "service_after": json.dumps(service).encode(),
        "layer_before": json.dumps(layer).encode(),
        "layer_after": json.dumps(layer).encode(),
        "count_before": json.dumps({"count": EXPECTED_SERVICE_COUNT}).encode(),
        "count_after": json.dumps({"count": EXPECTED_SERVICE_COUNT}).encode(),
        "ids_before": json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": ids}).encode(),
        "ids_after": json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": ids}).encode(),
        "page_first_probe": json.dumps({
            "features": [{"attributes": {"OBJECTID": object_id}} for object_id in range(1, 2001)],
            "exceededTransferLimit": True,
        }).encode(),
        "page_last_probe": json.dumps({
            "features": [{"attributes": {"OBJECTID": object_id}} for object_id in range(22501, 22511)],
            "exceededTransferLimit": False,
        }).encode(),
    }


class NcesPrivateSchoolsSourceContractTest(unittest.TestCase):
    def test_pins_official_complete_successor_and_native_archive_contract(self):
        self.assertEqual(SOURCE_URL, "https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PRIVATESCH_2324.zip")
        self.assertEqual(SOURCE_RELEASE, "2023-2024")
        self.assertEqual(EXPECTED_COUNT, 22_510)
        self.assertEqual(len(EXPECTED_FIELDS), 21)
        self.assertEqual(EXPECTED_FIELDS[0], ("PPIN", "C", 8, 0))
        self.assertEqual(EXPECTED_FIELDS[-1], ("SCHOOLYEAR", "C", 10, 0))
        self.assertIn("EDGE_GEOCODE_PRIVATESCH_2324.shp", EXPECTED_MEMBERS)

    def test_service_roster_and_page_edges_are_checked(self):
        evidence = _service_evidence(_service_bodies())
        self.assertEqual(evidence["service_count"], EXPECTED_SERVICE_COUNT)
        self.assertEqual(evidence["service_object_id_sha256"], EXPECTED_SERVICE_IDS_SHA256)
        self.assertEqual(evidence["service_native_wkid"], EXPECTED_SERVICE_WKID)

        drifted = _service_bodies()
        drifted["count_after"] = json.dumps({"count": EXPECTED_SERVICE_COUNT - 1}).encode()
        with self.assertRaisesRegex(ValueError, "source_service_count_drift"):
            _service_evidence(drifted)

    def test_geometry_and_refresh_policy_preserve_pending_state(self):
        self.assertEqual(_point_wkb(-71.25, 42.5).hex(), "01010000000000000000d051c00000000000404540")
        self.assertEqual(nces_private_schools_draft_schedule.default_status.value, "STOPPED")

    def test_versioned_processing_identity_survives_provenance_projection(self):
        scheme = "sha256-packaged-dagster-hifld-v1"
        manifest = {
            "schema_version": 1,
            "processor": "HIFLD Next",
            "code_commit": "packaged-sha256:" + "0" * 64,
            "code_identity_scheme": scheme,
            "inputs": [{
                "input_id": "nces-private-schools",
                "producer": "National Center for Education Statistics",
                "host": "nces.ed.gov",
                "role": "producer_publication",
                "canonical_url": SOURCE_URL,
                "retrieval_url": SOURCE_URL,
                "retrieved_at": "2026-10-20T03:44:19Z",
                "sha256": "a" * 64,
                "bytes": 1,
                "license": "review pending",
                "attribution": "review pending",
                "release_date": None,
                "release_date_unknown_reason": "Source collection period does not establish a producer release date.",
            }],
            "transformations": [{"operation": "NCES native fields and points to GeoParquet"}],
            "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
                "reason": "native fields retained",
                "objects": [{"path": "geoparquet/private-schools.parquet", "bytes": 1, "sha256": "b" * 64}],
            }}},
        }
        self.assertIn("code_identity_scheme", CATALOG_METADATA_FIELDS)
        self.assertEqual(provenance_projection(manifest)["code_identity_scheme"], scheme)


if __name__ == "__main__":
    unittest.main()

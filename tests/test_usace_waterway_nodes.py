"""Focused contract checks for the native BTS/USACE node candidate."""
from __future__ import annotations

import unittest

from dagster_hifld.sources.usace_waterway_nodes import (
    BTS_ITEM_ID,
    EXPECTED_COUNT,
    EXPECTED_FIELDS,
    EXPECTED_FIELD_TYPES,
    EXPECTED_EXTENT,
    EXPECTED_SOURCE_IDS,
    LAYER_URL,
    SERVICE_URL,
    USACE_SERVICE_ITEM_ID,
    _validate_source,
)
from dagster_hifld.assets.usace_waterway_nodes import (
    usace_waterway_network_nodes_candidate_complete,
)


def _snapshot():
    fields = [
        {"name": name, "type": kind, "length": length, "nullable": nullable}
        for name, kind, length, nullable in EXPECTED_FIELD_TYPES
    ]
    return (
        {"id": BTS_ITEM_ID, "owner": "USDOT_BTS", "url": LAYER_URL},
        {"id": USACE_SERVICE_ITEM_ID, "owner": "usace_iwr_cac", "url": SERVICE_URL},
        {"serviceItemId": USACE_SERVICE_ITEM_ID, "capabilities": "Query,Extract",
         "supportedExportFormats": "csv,shapefile,sqlite,geoPackage,filegdb"},
        {"id": 0, "name": "Waterway_Network_Node", "geometryType": "esriGeometryPoint",
         "hasZ": False, "hasM": False,
         "extent": {"xmin": EXPECTED_EXTENT[0], "ymin": EXPECTED_EXTENT[1],
                    "xmax": EXPECTED_EXTENT[2], "ymax": EXPECTED_EXTENT[3],
                    "spatialReference": {"latestWkid": 4269}}, "fields": fields},
        {"count": EXPECTED_COUNT},
        {"objectIds": list(EXPECTED_SOURCE_IDS)},
    )


class WaterwayNodeContractTests(unittest.TestCase):
    def test_exact_node_layer_identity_types_and_ids_pass(self):
        fields, ids = _validate_source(*_snapshot())
        self.assertEqual([field["name"] for field in fields], list(EXPECTED_FIELDS))
        self.assertEqual(ids, list(EXPECTED_SOURCE_IDS))

    def test_missing_or_renamed_identifier_is_quarantined(self):
        snapshot = list(_snapshot())
        snapshot[3] = dict(snapshot[3])
        snapshot[3]["fields"] = list(snapshot[3]["fields"][:-1])
        with self.assertRaisesRegex(ValueError, "field_roster_drift"):
            _validate_source(*snapshot)

    def test_native_type_change_is_quarantined(self):
        snapshot = list(_snapshot())
        snapshot[3] = dict(snapshot[3])
        snapshot[3]["fields"] = [dict(field) for field in snapshot[3]["fields"]]
        snapshot[3]["fields"][1]["type"] = "esriFieldTypeInteger"
        with self.assertRaisesRegex(ValueError, "field_type_or_nullability_drift"):
            _validate_source(*snapshot)

    def test_population_or_identifier_drift_is_quarantined(self):
        snapshot = list(_snapshot())
        snapshot[5] = {"objectIds": list(EXPECTED_SOURCE_IDS[:-1]) + [99999]}
        with self.assertRaisesRegex(ValueError, "ID_baseline_drift"):
            _validate_source(*snapshot)

    def test_geometry_coverage_drift_is_quarantined(self):
        snapshot = list(_snapshot())
        snapshot[3] = dict(snapshot[3])
        snapshot[3]["extent"] = dict(snapshot[3]["extent"])
        snapshot[3]["extent"]["xmin"] -= 1
        with self.assertRaisesRegex(ValueError, "coverage_extent_drift"):
            _validate_source(*snapshot)

    def test_complete_candidate_passes_research_check_with_acceptance_false(self):
        result = usace_waterway_network_nodes_candidate_complete({
            "candidate_ready": True,
            "feature_count": EXPECTED_COUNT,
            "native_filegdb_preserved": True,
            "source_release_accepted": False,
            "freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "historical_published_ids_reconciled": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        })
        self.assertTrue(result.passed)
        self.assertTrue(result.metadata["candidate_ready"])

    def test_incomplete_candidate_fails_research_check(self):
        result = usace_waterway_network_nodes_candidate_complete({
            "candidate_ready": True,
            "feature_count": EXPECTED_COUNT - 1,
            "native_filegdb_preserved": True,
        })
        self.assertFalse(result.passed)

if __name__ == "__main__":
    unittest.main()

import json
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.portolan.catalog import (
    HIFLD_NEXT_HOST_NAME,
    PORTOLAN_STAC_EXTENSION,
    CatalogRecord,
    render_portolan_tree,
)
from dagster_hifld.portolan.validation import (
    PortolanValidationError,
    normalize_candidate_tree,
    validate_candidate_tree,
)


class PortolanValidationTests(unittest.TestCase):
    def test_retained_out_of_range_extent_uses_conservative_world_bbox(self):
        record = CatalogRecord(
            "hifld",
            "forests",
            "forests",
            "v1.0.0",
            "Forests",
            "Data",
            "spatial",
            1,
            (),
            provider="Source agency",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            render_portolan_tree(root, (record,))
            path = root / "hifld/forests/forests/v1.0.0/collection.json"
            document = json.loads(path.read_text())
            document["extent"]["spatial"]["bbox"] = [
                [-16698780.0, 2064632.0, -7313653.0, 8745977.0]
            ]
            document["hifld:native_bbox"] = [
                -16698780.0,
                2064632.0,
                -7313653.0,
                8745977.0,
            ]
            path.write_text(json.dumps(document))

            self.assertEqual(normalize_candidate_tree(root), [])

            repaired = json.loads(path.read_text())
            self.assertEqual(
                repaired["extent"]["spatial"]["bbox"],
                [[-180.0, -90.0, 180.0, 90.0]],
            )
            self.assertEqual(
                repaired["hifld:native_bbox"],
                [-16698780.0, 2064632.0, -7313653.0, 8745977.0],
            )
            self.assertEqual(
                repaired["hifld:spatial_extent_status"], "unknown_source_bbox"
            )

    def test_validation_rejects_reversed_temporal_coverage(self):
        record = CatalogRecord(
            "hifld", "roads", "roads", "v1.0.0", "Roads", "Data", "spatial", 1, (),
            provider="Agency",
            temporal_start="2024-06-25T00:00:00Z",
            temporal_end="2020-10-21T00:00:00Z",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            render_portolan_tree(root, (record,))
            normalize_candidate_tree(root)
            with self.assertRaisesRegex(PortolanValidationError, "temporal interval"):
                validate_candidate_tree(root)

    def test_validation_rejects_invalid_one_sided_temporal_coverage(self):
        record = CatalogRecord(
            "hifld", "roads", "roads", "v1.0.0", "Roads", "Data", "spatial", 1, (),
            provider="Agency", temporal_start="not-a-date",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            render_portolan_tree(root, (record,))
            normalize_candidate_tree(root)
            with self.assertRaisesRegex(PortolanValidationError, "not-a-date"):
                validate_candidate_tree(root)

    def test_host_only_collection_is_an_explicit_missing_producer_exception(self):
        record = CatalogRecord(
            "hifld",
            "hospitals",
            "hospitals",
            "v1.0.0",
            "Hospitals",
            "Data",
            "spatial",
            1,
            (),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            render_portolan_tree(root, (record,))

            exceptions = normalize_candidate_tree(root)

            collection_path = root / "hifld/hospitals/hospitals/v1.0.0/collection.json"
            collection = json.loads(collection_path.read_text())
            self.assertEqual(
                collection["providers"],
                [
                    {
                        "name": HIFLD_NEXT_HOST_NAME,
                        "roles": ["host"],
                        "url": "https://hifld.publicenvirodata.org",
                    }
                ],
            )
            self.assertNotIn(PORTOLAN_STAC_EXTENSION, collection["stac_extensions"])
            self.assertEqual(
                exceptions,
                [
                    {
                        "version_path": "hifld/hospitals/hospitals/v1.0.0",
                        "reason": "missing_source_producer",
                        "source_evidence": "not_present",
                    }
                ],
            )
            self.assertEqual(
                json.loads((root / "_catalog/validation-exceptions.json").read_text()),
                {"exceptions": exceptions},
            )

    def test_retained_publisher_provider_is_normalized_to_producer(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "legacy/collection.json"
            path.parent.mkdir()
            path.write_text(
                json.dumps(
                    {
                        "stac_version": "1.1.0",
                        "stac_extensions": [PORTOLAN_STAC_EXTENSION],
                        "type": "Collection",
                        "id": "legacy",
                        "description": "Legacy",
                        "license": "other",
                        "extent": {
                            "spatial": {"bbox": [[-180, -90, 180, 90]]},
                            "temporal": {"interval": [[None, None]]},
                        },
                        "links": [],
                        "providers": [
                            {"name": "Census Bureau", "roles": ["publisher"]},
                            {"name": HIFLD_NEXT_HOST_NAME, "roles": ["host"]},
                        ],
                    }
                )
            )

            self.assertEqual(normalize_candidate_tree(root), [])

            document = json.loads(path.read_text())
            self.assertEqual(document["providers"][0]["roles"], ["producer"])

    def test_retained_name_only_source_provider_gets_producer_and_host_roles(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "hifld/districts/districts/v1.0.0/collection.json"
            path.parent.mkdir(parents=True)
            path.write_text(
                json.dumps(
                    {
                        "stac_version": "1.1.0",
                        "stac_extensions": [PORTOLAN_STAC_EXTENSION],
                        "type": "Collection",
                        "id": "hifld/districts/districts/v1.0.0",
                        "description": "Districts",
                        "license": "CC-PDM-1.0",
                        "extent": {
                            "spatial": {"bbox": [[-180, -90, 180, 90]]},
                            "temporal": {"interval": [[None, None]]},
                        },
                        "links": [],
                        "hifld:agency": "Census Bureau",
                        "providers": [{"name": "Census Bureau"}],
                    }
                )
            )

            self.assertEqual(normalize_candidate_tree(root), [])

            document = json.loads(path.read_text())
            self.assertEqual(
                document["providers"],
                [
                    {"name": "Census Bureau", "roles": ["producer"]},
                    {
                        "name": HIFLD_NEXT_HOST_NAME,
                        "roles": ["host"],
                        "url": "https://hifld.publicenvirodata.org",
                    },
                ],
            )

    def test_retained_publisher_is_verified_against_source_dictionary(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "hifld/address-ranges/address-ranges/v1.0.0/collection.json"
            path.parent.mkdir(parents=True)
            path.write_text(
                json.dumps(
                    {
                        "stac_version": "1.1.0",
                        "stac_extensions": [PORTOLAN_STAC_EXTENSION],
                        "type": "Collection",
                        "id": "hifld/address-ranges/address-ranges/v1.0.0",
                        "description": "Address ranges",
                        "license": "CC-PDM-1.0",
                        "extent": {
                            "spatial": {"bbox": [[-180, -90, 180, 90]]},
                            "temporal": {"interval": [[None, None]]},
                        },
                        "links": [],
                        "hifld:agency": "Census Bureau",
                        "providers": [{"name": "United States Census Bureau"}],
                    }
                )
            )

            exceptions = normalize_candidate_tree(
                root,
                source_publisher=lambda version_path: (
                    "United States Census Bureau"
                    if version_path == "hifld/address-ranges/address-ranges/v1.0.0"
                    else None
                ),
            )

            self.assertEqual(exceptions, [])
            document = json.loads(path.read_text())
            self.assertEqual(document["providers"][0]["roles"], ["producer"])
            self.assertEqual(document["providers"][-1]["roles"], ["host"])

    def test_validation_rejects_a_broken_local_child_link(self):
        record = CatalogRecord(
            "hifld",
            "roads",
            "roads",
            "v1.0.0",
            "Roads",
            "Data",
            "spatial",
            1,
            (),
            provider="Agency",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            render_portolan_tree(root, (record,))
            document_path = root / "catalog.json"
            document = json.loads(document_path.read_text())
            document["links"][-1]["href"] = "missing/catalog.json"
            document_path.write_text(json.dumps(document))

            with self.assertRaisesRegex(
                PortolanValidationError, "catalog.json.*missing"
            ):
                validate_candidate_tree(root)

    def test_validation_rejects_a_missing_companion_document(self):
        record = CatalogRecord(
            "hifld",
            "roads",
            "roads",
            "v1.0.0",
            "Roads",
            "Data",
            "spatial",
            1,
            (),
            provider="Agency",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            render_portolan_tree(root, (record,))
            (root / "hifld/roads/roads/v1.0.0/AGENTS.md").unlink()

            with self.assertRaisesRegex(PortolanValidationError, "AGENTS.md"):
                validate_candidate_tree(root)

    def test_profile_failure_reports_a_bounded_provider_reason(self):
        record = CatalogRecord(
            "hifld",
            "roads",
            "roads",
            "v1.0.0",
            "Roads",
            "Data",
            "spatial",
            1,
            (),
            provider="Agency",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            render_portolan_tree(root, (record,))
            path = root / "hifld/roads/roads/v1.0.0/collection.json"
            document = json.loads(path.read_text())
            document["providers"] = [
                {
                    "name": HIFLD_NEXT_HOST_NAME,
                    "roles": ["host"],
                    "url": "https://hifld.publicenvirodata.org",
                }
            ]
            path.write_text(json.dumps(document))

            with self.assertRaises(PortolanValidationError) as captured:
                validate_candidate_tree(root)

            message = str(captured.exception)
            self.assertIn("providers", message)
            self.assertLess(len(message), 1200)

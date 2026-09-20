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

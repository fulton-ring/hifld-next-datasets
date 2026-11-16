import json
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.sources import epa_frs_common
from dagster_hifld.sources.epa_frs_common import validate_staged_release_derivatives


PREFIX = "candidate-dataset/candidate-file/vabc123"
NAMES = {"collection.json", "README.md", "AGENTS.md"}


class MemoryStorage:
    def __init__(self, objects):
        self.objects = objects

    def read_key(self, key):
        return self.objects[key]

    def list_prefix(self, prefix):
        return [key for key in self.objects if key.startswith(prefix + "/")]


def complete_release():
    collection = {
        "type": "Collection",
        "id": "hifld/candidate-dataset/candidate-file/1.2.3",
        "version": "1.2.3",
    }
    objects = {
        f"{PREFIX}/collection.json": json.dumps(collection).encode(),
        f"{PREFIX}/README.md": b"# Candidate release\n",
        f"{PREFIX}/AGENTS.md": b"Use catalog links.\n",
    }
    return objects


class StagedReleaseCacheDerivativeTests(unittest.TestCase):
    def test_accepts_only_complete_semver_bound_release_derivative_trio(self):
        objects = complete_release()
        keys = set(objects)
        self.assertEqual(validate_staged_release_derivatives(MemoryStorage(objects), PREFIX, keys), keys)

    def test_rejects_partial_release_derivative_trio(self):
        objects = complete_release()
        objects.pop(f"{PREFIX}/AGENTS.md")
        with self.assertRaisesRegex(ValueError, "incomplete_staged_release_derivatives"):
            validate_staged_release_derivatives(MemoryStorage(objects), PREFIX, set(objects))

    def test_rejects_release_metadata_for_another_candidate(self):
        for field, value in (("id", "hifld/other/file/1.2.3"), ("version", "v1.2.3")):
            with self.subTest(field=field):
                objects = complete_release()
                document = json.loads(objects[f"{PREFIX}/collection.json"])
                document[field] = value
                objects[f"{PREFIX}/collection.json"] = json.dumps(document).encode()
                with self.assertRaises(ValueError):
                    validate_staged_release_derivatives(MemoryStorage(objects), PREFIX, set(objects))

    def test_rejects_empty_undecodable_or_oversized_release_docs(self):
        for document in (b"", b"\xff", b"x" * (2 * 1024 * 1024 + 1)):
            with self.subTest(size=len(document)):
                objects = complete_release()
                objects[f"{PREFIX}/README.md"] = document
                with self.assertRaises(ValueError):
                    validate_staged_release_derivatives(MemoryStorage(objects), PREFIX, set(objects))

    def test_shared_cache_verifier_accepts_release_derivatives_but_still_rejects_unknown_files(self):
        storage_objects = complete_release()
        storage = MemoryStorage(storage_objects)
        identity = {"code_commit": "packaged-sha256:test", "files": {}}
        payload = {
            "processing_identity": identity,
            "source_record_sha256": "record",
            "source_schema_sha256": "schema",
            "capture_receipts": {},
            "objects": {},
        }
        with patch.object(epa_frs_common, "load_first_construction", return_value=json.dumps({"payload": payload})), \
             patch.object(epa_frs_common, "validate_construction_code_identity"):
            result = epa_frs_common.verify_cached_candidate(
                storage, Path("."), "state", PREFIX, "1.2.3", "fingerprint",
                identity, "record", "schema", {}, {},
            )
        self.assertEqual(result, {"version": "1.2.3", "reused": True})

        storage_objects[f"{PREFIX}/unknown.json"] = b"unrostered"
        with patch.object(epa_frs_common, "load_first_construction", return_value=json.dumps({"payload": payload})), \
             patch.object(epa_frs_common, "validate_construction_code_identity"):
            with self.assertRaisesRegex(ValueError, "cached_candidate_storage_roster_changed"):
                epa_frs_common.verify_cached_candidate(
                    storage, Path("."), "state", PREFIX, "1.2.3", "fingerprint",
                    identity, "record", "schema", {}, {},
                )

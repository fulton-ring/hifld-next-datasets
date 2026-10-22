"""Semantic cache validation for catalog derivatives of native FRS candidates."""
from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.catalog import summarize_staged_catalog
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.sources.epa_frs_common import _validate_cached_catalog_sidecars
from tests.test_native_geoparquet_catalog import PREFIX, write_fixture


class CachedCatalogSemanticValidationTests(unittest.TestCase):
    def _candidate(self, root: Path):
        write_fixture(root)
        storage = StagingStorageResource(local_dir=str(root))
        resolved = load_resolved_source_manifest(storage, *PREFIX.split("/")).metadata
        summary = summarize_staged_catalog(
            storage, *PREFIX.split("/"), PREFIX.split("/")[0], source_metadata=resolved
        )
        quality_key = f"{PREFIX}/metadata/quality_manifest.json"
        dictionary_key = f"{PREFIX}/metadata/data_dictionary.json"
        storage.write_key(quality_key, json.dumps(summary.quality_manifest).encode())
        storage.write_key(dictionary_key, json.dumps(summary.data_dictionary).encode())
        resolved = load_resolved_source_manifest(storage, *PREFIX.split("/")).metadata
        projection = provenance_projection(resolved)
        if projection is not None:
            storage.write_key(
                f"{PREFIX}/metadata/resolved_provenance.json",
                json.dumps(projection).encode(),
            )
        return storage, summary, quality_key, dictionary_key

    def test_accepts_exact_catalog_derivatives_and_rejects_tampered_semantics(self):
        with tempfile.TemporaryDirectory() as temporary:
            storage, summary, quality_key, dictionary_key = self._candidate(Path(temporary))
            _validate_cached_catalog_sidecars(storage, PREFIX, set(storage.list_prefix(PREFIX)))

            tamper_cases = (
                (quality_key, "quality_check_passed", not summary.quality_manifest["quality_check_passed"]),
                (quality_key, "feature_count", summary.quality_manifest["feature_count"] + 1),
                (quality_key, "native_crs", {"name": "EPSG:4326"}),
                (dictionary_key, "schema_version", "tampered"),
            )
            for key, field, value in tamper_cases:
                with self.subTest(field=field):
                    original = storage.read_key(key)
                    changed = json.loads(original)
                    changed[field] = value
                    storage.write_key(key, json.dumps(changed).encode())
                    with self.assertRaisesRegex(ValueError, "cached_catalog_metadata_mismatch"):
                        _validate_cached_catalog_sidecars(
                            storage, PREFIX, set(storage.list_prefix(PREFIX))
                        )
                    storage.write_key(key, original)

            changed_dictionary = copy.deepcopy(summary.data_dictionary)
            changed_dictionary["columns"][0]["name"] = "altered"
            storage.write_key(dictionary_key, json.dumps(changed_dictionary).encode())
            with self.assertRaisesRegex(ValueError, "cached_catalog_metadata_mismatch"):
                _validate_cached_catalog_sidecars(storage, PREFIX, set(storage.list_prefix(PREFIX)))


if __name__ == "__main__":
    unittest.main()

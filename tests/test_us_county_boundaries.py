import io
import unittest
import zipfile

from dagster_hifld.assets.us_county_boundaries import us_county_boundaries_source_complete
from dagster_hifld.sources.us_county_boundaries import (
    EXPECTED_BYTES,
    EXPECTED_FEATURE_COUNT,
    EXPECTED_GEOID_SHA256,
    EXPECTED_SHA256,
    candidate_version,
    _inspect_archive,
)


class UsCountyBoundariesTests(unittest.TestCase):
    def _archive(self, names):
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w") as archive:
            for name in names:
                archive.writestr(name, b"fixture")
        return sink.getvalue()

    def test_candidate_version_is_bound_to_packaged_code_identity(self):
        identity = {
            "code_commit": "packaged-sha256:" + "a" * 64,
            "code_identity_scheme": "sha256-packaged-dagster-hifld-v1",
            "files": {"src/dagster_hifld/example.py": "b" * 64},
            "working_tree_status": "unknown",
            "installed_distributions": [{"name": "dagster", "version": "1.11.0"}],
            "python": "3.12.14", "pyarrow": "23.0.1", "proj": "9.5.1", "geos": "3.13.1",
        }
        first = candidate_version(identity)
        self.assertEqual(first, candidate_version(identity))
        identity = {**identity, "installed_distributions": [{"name": "dagster", "version": "1.11.1"}]}
        self.assertNotEqual(first, candidate_version(identity))

    def test_rejects_incomplete_tiger_archive_member_roster(self):
        body = self._archive(["tl_2023_us_county.shp"])
        with self.assertRaisesRegex(ValueError, "tiger_archive_member_roster_drift"):
            _inspect_archive(body)

    def test_rejects_archive_path_traversal_before_extraction(self):
        names = [
            "../tl_2023_us_county.cpg", "tl_2023_us_county.dbf", "tl_2023_us_county.prj",
            "tl_2023_us_county.shp", "tl_2023_us_county.shp.ea.iso.xml",
            "tl_2023_us_county.shp.iso.xml", "tl_2023_us_county.shx",
        ]
        with self.assertRaisesRegex(ValueError, "tiger_archive_unsafe_member"):
            _inspect_archive(self._archive(names))

    def test_complete_source_gate_keeps_release_acceptance_false(self):
        result = us_county_boundaries_source_complete({
            "feature_count": EXPECTED_FEATURE_COUNT,
            "source_population_complete": True,
            "source_archive_bytes": EXPECTED_BYTES,
            "source_archive_sha256": EXPECTED_SHA256,
            "source_id_sha256": EXPECTED_GEOID_SHA256,
        })
        self.assertTrue(result.passed)
        self.assertFalse(result.metadata["release_accepted"].value)
        self.assertFalse(result.metadata["rights_accepted"].value)


if __name__ == "__main__":
    unittest.main()

import copy
import unittest

from dagster_hifld.provenance import validate_provenance
from dagster_hifld.release_approval import ReleaseApprovalError, validate_release_provenance


def provenance_fixture():
    return {"schema_version": 1, "processor": "HIFLD Next", "code_commit": "synthetic-fixture-only",
            "transformations": [{"name": "identity archival conversion", "parameters": {"repair": False}}],
            "inputs": [{"input_id": "producer", "producer": "Original Synthetic Agency", "host": "example.invalid",
                        "canonical_url": "https://example.invalid/release", "retrieval_url": "https://example.invalid/archive.zip",
                        "requested_url": "https://example.invalid/latest", "observed_final_url": "https://example.invalid/archive.zip",
                        "role": "producer_publication", "retrieved_at": "2026-10-19T01:02:03.123456Z",
                        "release_date_unknown_reason": "The publisher supplies no separately versioned release date",
                        "sha256": "a" * 64, "bytes": 123, "license": "Captured source notice; synthetic fixture",
                        "attribution": "Original Synthetic Agency; original wording retained"}]}


class ProvenanceTests(unittest.TestCase):
    def test_versioned_packaged_processing_identity_is_valid_provenance(self):
        value = provenance_fixture()
        value["code_commit"] = "packaged-sha256:" + "a" * 64
        value["code_identity_scheme"] = "sha256-packaged-dagster-hifld-v1"
        validate_provenance(value)
        validate_release_provenance(value)

    def test_timezone_aware_retrieval_and_separate_release_dates_remain_unmodified(self):
        for timestamp in ["2026-10-19T01:02:03Z", "2026-10-19T01:02:03.123456789+02:30", "2026-10-19T01:02:03-05:00"]:
            for release in [None, "2025-10-19", "2025-10-19T05:00:00+00:00"]:
                value = provenance_fixture()
                value["inputs"][0].update(retrieved_at=timestamp, release_date=release)
                original = copy.deepcopy(value)
                validate_provenance(value)
                validate_release_provenance(value)
                self.assertEqual(value, original)

    def test_malformed_values_fail_as_explicit_validation_errors(self):
        changes = [
            ("retrieved_at", "not-a-date"), ("retrieved_at", "2026-10-19"), ("retrieved_at", "2026-10-19T01:02:03"),
            ("retrieved_at", "2026-02-30T01:02:03Z"), ("retrieved_at", "2026-10-19T01:02:03+24:00"),
            ("retrieved_at", "2026-10-19T01:02:03+00:99"), ("retrieved_at", "2026-10-19T01:02:03-01:60"),
            ("retrieved_at", True), ("bytes", True), ("bytes", -1), ("bytes", "123"), ("sha256", 123),
            ("license", {}), ("license", " "), ("attribution", []), ("producer", "HIFLD Next"),
            ("producer", {}), ("input_id", " producer "), ("role", []), ("role", "official"),
            ("host", "wrong.invalid"), ("host", "https://example.invalid"), ("host", ["example.invalid"]),
            ("retrieval_url", "https://user:pass@example.invalid/data"), ("canonical_url", "ftp://example.invalid/data"),
            ("retrieval_url", "https://example.invalid/data#not-retrieved"), ("retrieval_url", "https://example.invalid:bad/data"),
            ("canonical_url", "https://example.invalid/data?api_key=redacted"), ("observed_final_url", "https://example.invalid/data?token=redacted"),
            ("requested_url", "https://example.invalid/has space"),
            ("request_url", "https://user:pass@example.invalid/data"), ("request_url", "https://example.invalid/data?api_key=redacted"),
            ("final_url", "https://example.invalid/data?token=redacted"), ("http_status", 403), ("http_status", "200"), ("canonical_url", "https://example.invalid\\other/data"),
            ("release_date", "unknown"), ("release_date", "2026-02-30"), ("release_date_unknown_reason", []),
        ]
        for field, replacement in changes:
            with self.subTest(field=field, replacement=replacement):
                value = provenance_fixture()
                value["inputs"][0][field] = replacement
                with self.assertRaises(ValueError):
                    validate_provenance(value)
                with self.assertRaises(ReleaseApprovalError):
                    validate_release_provenance(value)

    def test_duplicate_identity_and_invalid_transformations_block(self):
        values = []
        duplicate = provenance_fixture(); duplicate["inputs"].append(copy.deepcopy(duplicate["inputs"][0])); values.append(duplicate)
        for field, replacement in [("schema_version", True), ("processor", {"name": "HIFLD Next"}),
                                   ("inputs", {}), ("inputs", [None]), ("transformations", {}), ("transformations", [{}]),
                                   ("transformations", [{"name": "convert", "parameters": []}]), ("code_commit", [])]:
            value = provenance_fixture(); value[field] = replacement; values.append(value)
        for value in values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_provenance(value)

    def test_failed_diagnostics_remain_separate_from_successful_source_inputs(self):
        value = provenance_fixture()
        value["diagnostic_attempts"] = [{"http_status": 403, "retrieved_at": None, "classification": "failed bytes; not source inputs"}]
        validate_provenance(value)
        for timestamp in ["2025-01-01T00:00:00+00:99", "2025-01-01T00:00:00-12:61"]:
            value["inputs"][0]["release_date"] = timestamp
            with self.assertRaisesRegex(ValueError, "invalid_producer_release_date"):
                validate_provenance(value)

    def test_distributor_delegation_is_captured_without_authority_acceptance(self):
        value = provenance_fixture()
        value["inputs"][0].update(role="official_distributor", delegation_evidence={"url": "https://example.invalid/delegation", "acceptance": "pending"})
        validate_provenance(value)
        self.assertEqual(value["inputs"][0]["delegation_evidence"]["acceptance"], "pending")
        for evidence in [None, {}, True, " ", {"url": "https://user:pass@example.invalid/delegation"}]:
            value["inputs"][0]["delegation_evidence"] = evidence
            with self.subTest(evidence=evidence), self.assertRaises(ValueError):
                validate_provenance(value)


if __name__ == "__main__":
    unittest.main()

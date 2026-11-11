import gzip
import hashlib
import json
import tempfile
import tarfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import DagsterInstance, build_asset_context

from dagster_hifld.assets.epa_frs_interest_contact import (
    DATASET_SLUG, EXPECTED_FIELDS, FILE_SLUG,
    epa_frs_interest_contact, _reuse_existing_contact_candidate,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import processing_identity, write_immutable
from dagster_hifld.sources.epa_frs_contact_stream import (
    FIELDS, TABLE_URL, canonical, compact_capture_receipt_index,
)


def _roster_digest(object_ids):
    digest = hashlib.sha256()
    for object_id in object_ids:
        digest.update(f"{object_id}\n".encode())
    return digest.hexdigest()


def _identity(commit):
    return {"code_identity_scheme": "git-commit-blobs-v1", "working_tree_status": "clean",
            "code_commit": commit, "files": {"src/dagster_hifld/assets/epa_frs_interest_contact.py":
                                             hashlib.sha256(commit.encode()).hexdigest()},
            "python": "3.12.3", "pyarrow": "16.1.0", "proj": "9.4.1", "geos": "3.12.2"}


def _write_parquet(parquet_path: Path, object_ids) -> None:
    schema = pa.schema([pa.field(name, pa.int64() if name == "OBJECTID" else pa.string(),
                                 nullable=nullable)
                        for name, _esri, nullable, _length in FIELDS])
    def row(object_id):
        value = {"OBJECTID": object_id}
        for name, _esri, nullable, _length in FIELDS[1:]:
            value[name] = None if nullable else "fixture"
        return value
    pq.write_table(pa.Table.from_pylist([row(object_id) for object_id in object_ids],
                                        schema=schema), parquet_path)


def _catalog_documents(rows: int):
    quality = {"version": "v1", "feature_count": rows, "bounds": None, "geometry_type": None,
               "invalid_geometry_count": 0, "null_geometry_count": 0,
               "spatial_status": "non_spatial_source", "quality_check_passed": True,
               "columns_hash": hashlib.sha256(json.dumps(list(EXPECTED_FIELDS)).encode()).hexdigest()}
    dictionary = {"columns": [{"name": name, "type": "integer" if name == "OBJECTID" else "string"}
                              for name in EXPECTED_FIELDS]}
    return quality, dictionary


class EpaFrsContactRecoveryTests(unittest.TestCase):
    def _build_retained_fixture(self, directory: str, *, object_ids=(1, 3),
                                maximum_object_id=3, source_ids_sha=None,
                                capture_overrides=None, include_maximum_id=True) -> tuple:
        """Write one complete sealed retained candidate with the eight source objects."""
        root = Path(directory)
        storage = StagingStorageResource(local_dir=directory, use_local=True, backend="local")
        repo = Path(__file__).resolve().parents[1]
        identity = processing_identity(repo)
        old_prefix = f"{DATASET_SLUG}/{FILE_SLUG}/vretained-fixture"
        captured_at = "2026-10-19T12:34:56.000000+00:00"
        rows = len(object_ids)
        source_record_sha = hashlib.sha256(b"fixture source records").hexdigest()
        source_ids_sha = source_ids_sha or _roster_digest(object_ids)
        source_schema_sha = hashlib.sha256(b"fixture schema").hexdigest()

        wire = gzip.compress(b"fixture source response")
        receipt = {"name": "page_00000", "method": "GET", "url": TABLE_URL + "/query",
                   "final_url": TABLE_URL + "/query", "query": {"resultOffset": 0},
                   "status": 200, "content_encoding": "gzip", "wire_bytes": len(wire),
                   "wire_sha256": hashlib.sha256(wire).hexdigest(), "decoded_bytes": 23,
                   "decoded_sha256": hashlib.sha256(b"fixture source response").hexdigest(),
                   "complete_body": True}
        archive_path = root / "source-wire-capture.tar"
        with tarfile.open(archive_path, "w") as archive:
            for suffix, body in ((".wire.gz", wire), (".receipt.json", canonical(receipt) + b"\n")):
                path = root / (receipt["name"] + suffix)
                path.write_bytes(body)
                archive.add(path, arcname=path.name, recursive=False)

        fields = tuple({"name": name, "type": esri, "nullable": nullable, "length": length}
                       for name, esri, nullable, length in FIELDS)
        parquet_path = root / f"{FILE_SLUG}.parquet"
        _write_parquet(parquet_path, object_ids)
        parquet_descriptor = {"bytes": parquet_path.stat().st_size,
                              "sha256": hashlib.sha256(parquet_path.read_bytes()).hexdigest()}
        capture = {"captured_at": captured_at, "row_count": rows,
                   "source_record_sha256": source_record_sha,
                   "source_ids_sha256": source_ids_sha,
                   "source_schema_sha256": source_schema_sha,
                   "fields": list(fields), "source_count_before": rows,
                   "source_count_after": rows, "id_count_before": rows, "id_count_after": rows,
                   "id_roster_equal_before_after": True, "captured_ids_match_roster": True,
                   "source_release_date": None, "response_count": 1, "page_count": 1}
        if include_maximum_id:
            capture["maximum_object_id"] = maximum_object_id
        if capture_overrides:
            capture.update(capture_overrides)
        metadata = {
            "metadata/source_capture.json": canonical(capture) + b"\n",
            "metadata/source_manifest.json": b'{"capture_observed_at":"' + captured_at.encode() + b'"}\n',
            "metadata/source_audit.json": b'{"source_complete":true}\n',
            "metadata/processing_identity.json": canonical(identity) + b"\n",
            "metadata/source_id_roster_summary.json": canonical(
                {"count": rows, "sha256": source_ids_sha, "complete": True}) + b"\n",
            "metadata/candidate_ready.json": canonical(
                {"approved": False, "version": "vretained-fixture",
                 "source_record_sha256": source_record_sha,
                 "source_schema_sha256": source_schema_sha,
                 "objects": {f"unknown/{FILE_SLUG}.parquet": parquet_descriptor}}) + b"\n",
        }
        bodies = {"raw/source-wire-capture.tar": archive_path.read_bytes(),
                  f"unknown/{FILE_SLUG}.parquet": parquet_path.read_bytes(), **metadata}
        for name, body in bodies.items():
            write_immutable(storage, f"{old_prefix}/{name}", body)
        return storage, old_prefix, capture, identity, parquet_descriptor

    def _add_catalog_documents(self, storage, old_prefix: str, capture: dict) -> None:
        """Simulate the exact documents catalog and release preparation write."""
        quality, dictionary = _catalog_documents(capture["row_count"])
        manifest_body = storage.read_key(f"{old_prefix}/metadata/source_manifest.json")
        manifest_sha = hashlib.sha256(manifest_body).hexdigest()
        for name, body in (
            ("metadata/quality_manifest.json", json.dumps(quality, sort_keys=True, indent=2).encode()),
            ("metadata/data_dictionary.json", json.dumps(dictionary, sort_keys=True, indent=2).encode()),
            ("metadata/source/quality_manifest.json", json.dumps(quality, sort_keys=True, indent=2).encode()),
            ("metadata/source/data_dictionary.json", json.dumps(dictionary, sort_keys=True, indent=2).encode()),
            ("metadata/source/source_manifest.json", manifest_body),
            (f"metadata/source/version_{manifest_sha}_source_manifest.json", manifest_body),
        ):
            write_immutable(storage, f"{old_prefix}/{name}", body)

    def test_explicit_recovery_preserves_observation_and_rejects_tamper(self):
        with tempfile.TemporaryDirectory() as directory:
            storage, old_prefix, capture, identity, _parquet = self._build_retained_fixture(directory)
            captured_at = capture["captured_at"]
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(
                    asset_config={"resume_candidate": old_prefix}, instance=instance)
                result = epa_frs_interest_contact(context, storage)
            self.assertTrue(result["resumed_offline"])
            self.assertFalse(result["reused"])
            self.assertEqual(result["captured_at"], captured_at)
            self.assertEqual(result["source_id_bounds"],
                             {"count": 2, "minimum_object_id": 1, "maximum_object_id": 3,
                              "sha256": capture["source_ids_sha256"]})
            self.assertEqual(storage.read_key(result["candidate_ready"].replace("metadata/candidate_ready.json",
                                                                                 "metadata/source_capture.json")),
                             canonical(capture) + b"\n")

            with tarfile.open(Path(directory) / "source-wire-capture.tar", "r:") as archive:
                receipts = [json.loads(archive.extractfile(member).read())
                            for member in archive.getmembers() if member.name.endswith(".receipt.json")]
            capture_index = compact_capture_receipt_index(
                receipts, Path(directory) / "source-wire-capture.tar")
            current_observation = {"row_count": 2, "source_record_sha256": capture["source_record_sha256"],
                                   "source_schema_sha256": capture["source_schema_sha256"],
                                   "source_ids_sha256": capture["source_ids_sha256"]}
            with patch("dagster_hifld.construction_provenance.validate_construction_code_identity"):
                reused = _reuse_existing_contact_candidate(
                    storage, Path(__file__).resolve().parents[1],
                    result["candidate_ready"].removesuffix("/metadata/candidate_ready.json"),
                    result["version"], identity, current_observation, capture_index)
            self.assertTrue(reused["reused"])
            self.assertEqual(reused["captured_at"], captured_at)

            # Resuming from the now-sealed candidate after catalog uses its
            # first construction directly and does not decode any retained page.
            current_prefix = result["candidate_ready"].removesuffix("/metadata/candidate_ready.json")
            self._add_catalog_documents(storage, current_prefix, capture)
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    patch("dagster_hifld.assets.epa_frs_interest_contact.compact_capture_receipt_index",
                          side_effect=AssertionError("sealed cache should not re-decode raw pages")), \
                    patch("dagster_hifld.construction_provenance.validate_construction_code_identity"), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": current_prefix},
                                              instance=instance)
                cache_run = epa_frs_interest_contact(context, storage)
            self.assertTrue(cache_run["reused"])
            self.assertTrue(cache_run["resumed_offline"])
            self.assertEqual(cache_run["captured_at"], captured_at)

            source_capture_key = result["candidate_ready"].replace("candidate_ready.json", "source_capture.json")
            Path(directory, source_capture_key).write_bytes(b'{"tampered":true}')
            with patch("dagster_hifld.construction_provenance.validate_construction_code_identity"):
                with self.assertRaisesRegex(ValueError, "cached_contact_candidate_object_changed"):
                    _reuse_existing_contact_candidate(
                        storage, Path(__file__).resolve().parents[1],
                        result["candidate_ready"].removesuffix("/metadata/candidate_ready.json"),
                        result["version"], identity, current_observation, capture_index)

    def test_recovery_after_catalog_with_changed_code_identity(self):
        """A cataloged retained candidate recovers across a changed code identity."""
        with tempfile.TemporaryDirectory() as directory:
            storage, old_prefix, capture, _real_identity, _parquet = self._build_retained_fixture(directory)
            old_code = _identity("0" * 40)
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    patch("dagster_hifld.assets.epa_frs_interest_contact.processing_identity",
                          return_value=old_code), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": old_prefix},
                                              instance=instance)
                sealed = epa_frs_interest_contact(context, storage)
            sealed_prefix = sealed["candidate_ready"].removesuffix("/metadata/candidate_ready.json")

            # Catalog the sealed version: derived documents and manifest snapshots
            # are added to the sealed prefix, exactly as the catalog run does.
            self._add_catalog_documents(storage, sealed_prefix, capture)

            # A changed code identity derives a different version, so recovery
            # from the cataloged prefix must take the cross-version path and
            # still succeed without altering the sealed observation.
            new_code = _identity("1" * 40)
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    patch("dagster_hifld.assets.epa_frs_interest_contact.processing_identity",
                          return_value=new_code), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": sealed_prefix},
                                              instance=instance)
                recovered = epa_frs_interest_contact(context, storage)
            self.assertTrue(recovered["resumed_offline"])
            self.assertFalse(recovered["reused"])
            self.assertNotEqual(recovered["version"], sealed["version"])
            self.assertEqual(recovered["captured_at"], capture["captured_at"])
            self.assertEqual(recovered["source_id_bounds"]["sha256"], capture["source_ids_sha256"])
            recovered_prefix = recovered["candidate_ready"].removesuffix("/metadata/candidate_ready.json")
            recovered_keys = set(storage.list_prefix(recovered_prefix))
            self.assertEqual(recovered_keys, {f"{recovered_prefix}/{name}" for name in (
                "raw/source-wire-capture.tar", f"unknown/{FILE_SLUG}.parquet",
                "metadata/source_id_roster_summary.json", "metadata/source_capture.json",
                "metadata/source_manifest.json", "metadata/source_audit.json",
                "metadata/processing_identity.json", "metadata/candidate_ready.json")})
            self.assertEqual(json.loads(storage.read_key(
                f"{recovered_prefix}/metadata/processing_identity.json")), new_code)

    def test_recovery_accepts_cataloged_pair_after_code_identity_change(self):
        """The prior review's reproduction: the catalog pair alone must not block recovery."""
        with tempfile.TemporaryDirectory() as directory:
            storage, old_prefix, capture, _real_identity, _parquet = self._build_retained_fixture(directory)
            quality, dictionary = _catalog_documents(2)
            write_immutable(storage, f"{old_prefix}/metadata/quality_manifest.json",
                            json.dumps(quality, sort_keys=True, indent=2).encode())
            write_immutable(storage, f"{old_prefix}/metadata/data_dictionary.json",
                            json.dumps(dictionary, sort_keys=True, indent=2).encode())
            new_code = _identity("2" * 40)
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    patch("dagster_hifld.assets.epa_frs_interest_contact.processing_identity",
                          return_value=new_code), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": old_prefix},
                                              instance=instance)
                result = epa_frs_interest_contact(context, storage)
            self.assertTrue(result["resumed_offline"])
            self.assertEqual(result["captured_at"], capture["captured_at"])

    def test_recovery_refuses_unknown_retained_object(self):
        with tempfile.TemporaryDirectory() as directory:
            storage, old_prefix, capture, _identity, _parquet = self._build_retained_fixture(directory)
            write_immutable(storage, f"{old_prefix}/metadata/unexpected_document.json", b"{}\n")
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": old_prefix},
                                              instance=instance)
                with self.assertRaisesRegex(ValueError, "resume_candidate_storage_roster_changed"):
                    epa_frs_interest_contact(context, storage)

    def test_recovery_refuses_incomplete_catalog_pair(self):
        with tempfile.TemporaryDirectory() as directory:
            storage, old_prefix, capture, _identity, _parquet = self._build_retained_fixture(directory)
            quality, _dictionary = _catalog_documents(2)
            write_immutable(storage, f"{old_prefix}/metadata/quality_manifest.json",
                            json.dumps(quality, sort_keys=True, indent=2).encode())
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": old_prefix},
                                              instance=instance)
                with self.assertRaisesRegex(ValueError, "resume_catalog_metadata_incomplete"):
                    epa_frs_interest_contact(context, storage)

    def test_recovery_refuses_changed_catalog_manifest_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            storage, old_prefix, capture, _identity, _parquet = self._build_retained_fixture(directory)
            self._add_catalog_documents(storage, old_prefix, capture)
            manifest_key = f"{old_prefix}/metadata/source/source_manifest.json"
            Path(directory, manifest_key).write_bytes(b'{"tampered":true}\n')
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": old_prefix},
                                              instance=instance)
                with self.assertRaisesRegex(ValueError, "resume_catalog_manifest_snapshot_changed"):
                    epa_frs_interest_contact(context, storage)

    def test_recovery_refuses_catalog_quality_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            storage, old_prefix, capture, _identity, _parquet = self._build_retained_fixture(directory)
            self._add_catalog_documents(storage, old_prefix, capture)
            quality_key = f"{old_prefix}/metadata/quality_manifest.json"
            Path(directory, quality_key).write_bytes(b'{"feature_count":3}\n')
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": old_prefix},
                                              instance=instance)
                with self.assertRaisesRegex(ValueError, "resume_catalog_document_semantics_changed"):
                    epa_frs_interest_contact(context, storage)

    def test_recovery_refuses_duplicate_retained_object_id(self):
        with tempfile.TemporaryDirectory() as directory:
            storage, old_prefix, capture, _identity, _parquet = self._build_retained_fixture(
                directory, object_ids=(1, 1), source_ids_sha=_roster_digest((1, 1)))
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": old_prefix},
                                              instance=instance)
                with self.assertRaisesRegex(ValueError,
                                            "resume_candidate_duplicate_or_unordered_object_id"):
                    epa_frs_interest_contact(context, storage)

    def test_recovery_refuses_retained_object_id_out_of_source_bounds(self):
        with tempfile.TemporaryDirectory() as directory:
            storage, old_prefix, capture, _identity, _parquet = self._build_retained_fixture(
                directory, object_ids=(1, 4), maximum_object_id=3,
                source_ids_sha=_roster_digest((1, 4)))
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": old_prefix},
                                              instance=instance)
                with self.assertRaisesRegex(ValueError, "resume_candidate_object_id_out_of_source_bounds"):
                    epa_frs_interest_contact(context, storage)

    def test_recovery_refuses_retained_roster_drift(self):
        """An in-range unique payload missing a producer ID fails the roster digest."""
        with tempfile.TemporaryDirectory() as directory:
            storage, old_prefix, capture, _identity, _parquet = self._build_retained_fixture(
                directory, object_ids=(1, 2), maximum_object_id=3,
                source_ids_sha=_roster_digest((1, 3)))
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": old_prefix},
                                              instance=instance)
                with self.assertRaisesRegex(ValueError, "resume_candidate_source_id_roster_changed"):
                    epa_frs_interest_contact(context, storage)

    def test_recovery_refuses_missing_producer_id_bounds(self):
        with tempfile.TemporaryDirectory() as directory:
            storage, old_prefix, capture, _identity, _parquet = self._build_retained_fixture(
                directory, include_maximum_id=False)
            with patch("dagster_hifld.assets.epa_frs_interest_contact.EXPECTED_ROWS", 2), \
                    DagsterInstance.ephemeral() as instance:
                context = build_asset_context(asset_config={"resume_candidate": old_prefix},
                                              instance=instance)
                with self.assertRaisesRegex(ValueError, "resume_candidate_source_id_bounds_missing"):
                    epa_frs_interest_contact(context, storage)


if __name__ == "__main__":
    unittest.main()

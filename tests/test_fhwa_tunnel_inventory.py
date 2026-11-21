"""Bounded tests for the FHWA NTI annual XML source contract and release blocking."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from xml.etree import ElementTree as ET

from dagster import AssetCheckSeverity

from dagster_hifld.assets.fhwa_tunnel_inventory import (
    fhwa_national_tunnel_inventory_complete,
    fhwa_national_tunnel_inventory_drift,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import fhwa_tunnel_inventory as source

INDEX = (
    b"<html><body><h1>Download NTI Data</h1>"
    b"<p>National Tunnel Inventory</p>"
    b'<a href="download/2025NTI.xml">2025 XML</a>'
    b'<a href="download/2026NTI.xml">2026 XML</a>'
    b"</body></html>"
)
DATA_URL = ("https://www.fhwa.dot.gov/bridge/inspection/tunnel/inventory/"
            "download/2026NTI.xml")


class _EmptyClient:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _xml(records: list[dict[str, str]]) -> bytes:
    root = ET.Element("TunnelRoot")
    for record in records:
        instance = ET.SubElement(root, "TunnelInstance")
        for name, _kind in source.ELEMENT_FIELDS:
            child = ET.SubElement(instance, name)
            child.text = record.get(name, "")
        ET.SubElement(instance, "FHWAElements")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


class FhwaTunnelInventoryTests(unittest.TestCase):
    def _records(self) -> list[dict[str, str]]:
        return [
            {"I1": "00003", "I3": "40", "I13": "36.12018100", "I14": "-95.98526700"},
            {"I1": "00003", "I3": "72", "I13": "18.15209900", "I14": "-66.10073600"},
        ]

    def _validate(self, records=None):
        return source._validate(INDEX, _xml(records or self._records()), 2026, DATA_URL)

    def test_release_index_selects_newest_xml_edition(self):
        year, url = source.newest_release(INDEX)
        self.assertEqual(year, 2026)
        self.assertEqual(url, DATA_URL)

    def test_source_contract_accepts_complete_native_population(self):
        features, audit = self._validate()
        self.assertEqual(audit["source_feature_count"], 2)
        self.assertTrue(audit["source_population_complete"])
        self.assertEqual(audit["source_id_fields"], ["I3", "I1"])
        self.assertEqual([row["attributes"]["I1"] for row in features], ["00003", "00003"])
        self.assertEqual([row["attributes"]["I3"] for row in features], [40, 72])
        self.assertAlmostEqual(features[0]["geometry"]["x"], -95.985267)
        self.assertAlmostEqual(features[0]["geometry"]["y"], 36.120181)
        self.assertEqual(audit["source_crs_epsg"], 4326)

    def test_population_count_is_not_pinned(self):
        extra = {"I1": "00004", "I3": "40", "I13": "35.0", "I14": "-96.0"}
        _features, two = self._validate()
        _features, three = self._validate(self._records() + [extra])
        self.assertEqual(two["source_feature_count"], 2)
        self.assertEqual(three["source_feature_count"], 3)
        self.assertTrue(two["source_population_complete"])
        self.assertTrue(three["source_population_complete"])

    def test_duplicate_native_key_is_rejected(self):
        records = self._records() + [
            {"I1": "00003", "I3": "40", "I13": "35.0", "I14": "-96.0"}
        ]
        with self.assertRaisesRegex(ValueError, "source_native_key_duplicate"):
            self._validate(records)

    def test_missing_coordinate_is_rejected(self):
        records = self._records()
        records[0]["I13"] = ""
        with self.assertRaisesRegex(ValueError, "source_coordinate_missing_or_out_of_range"):
            self._validate(records)

    def test_unknown_element_is_rejected(self):
        root = ET.fromstring(_xml(self._records()))
        ET.SubElement(root[0], "UNEXPECTED")
        bad = ET.tostring(root, encoding="utf-8", xml_declaration=True)
        with self.assertRaisesRegex(ValueError, "source_xml_element_roster_drift"):
            source._validate(INDEX, bad, 2026, DATA_URL)

    def test_completeness_passes_while_release_flags_stay_closed(self):
        result = fhwa_national_tunnel_inventory_complete({
            "feature_count": 579, "source_population_complete": True,
            "source_id_sha256": "a" * 64, "source_schema_sha256": "b" * 64,
            "source_release_year": 2026, "source_data_url": DATA_URL,
        })
        self.assertTrue(result.passed)
        self.assertFalse(result.metadata["source_release_accepted"].value)
        self.assertFalse(result.metadata["release_accepted"].value)

    def test_drift_check_reports_no_approved_baseline(self):
        result = fhwa_national_tunnel_inventory_drift({
            "dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
            "feature_count": 579, "source_id_sha256": "a" * 64,
        })
        self.assertFalse(result.passed)
        self.assertEqual(result.severity, AssetCheckSeverity.WARN)
        self.assertEqual(result.metadata["comparison_status"].value,
                         "skipped_no_approved_baseline")

    def _fake_capture_get(self, body_for):
        observed, calls = [], []

        def fake(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = body_for(name)
            digest = source._sha(body)
            receipt = {
                "method": "GET", "url": url, "final_url": url,
                "observed_at": f"2026-01-01T00:{len(calls):02d}:00+00:00",
                "status": 200, "complete_body": True, "wire_bytes": len(body),
                "wire_sha256": digest, "bytes": len(body), "sha256": digest,
                "content_encoding": None, "content_length": str(len(body)),
                "error_code": None,
            }
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(source._canonical(receipt) + b"\n")
            observed.append(receipt["observed_at"])
            return body, receipt

        return fake, observed

    def _capture(self, storage, root, captures_dir, fake):
        with patch.object(source, "capture_get", side_effect=fake), \
             patch.object(source, "validate_construction_code_identity"), \
             patch("dagster_hifld.sources.capture_common.validate_construction_code_identity"), \
             patch.object(source.httpx, "Client",
                          side_effect=lambda *args, **kwargs: _EmptyClient()):
            return source.capture_fhwa_tunnel_inventory(
                storage, version="vfixture", capture_directory=str(root / captures_dir))

    def test_second_fresh_acquisition_with_new_receipts_reuses_candidate(self):
        bodies = {"release_index": INDEX, "data": _xml(self._records())}
        fake, observed = self._fake_capture_get(lambda name: bodies[name])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            first = self._capture(storage, root, "captures-a", fake)
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], 2)
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            original_receipt = storage.read_key(f"{prefix}/raw/release_index.receipt.json")
            original_manifest = storage.read_key(f"{prefix}/metadata/source_manifest.json")
            original_parquet = storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet")
            second = self._capture(storage, root, "captures-b", fake)
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])
            self.assertEqual(second["feature_count"], first["feature_count"])
            self.assertNotEqual(observed[0], observed[2])
            self.assertEqual(json.loads(original_receipt)["observed_at"], observed[0])
            # The original immutable provenance survives; fresh receipts are not restamped.
            self.assertEqual(storage.read_key(f"{prefix}/raw/release_index.receipt.json"),
                             original_receipt)
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_manifest.json"),
                             original_manifest)
            self.assertEqual(storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet"),
                             original_parquet)
            self.assertEqual(sorted(observed), sorted(set(observed)),
                             "every receipt carries a distinct timestamp")

    def test_reacquisition_with_changed_raw_capture_evidence_fails_closed(self):
        bodies = {"release_index": INDEX, "data": _xml(self._records())}
        restamped = {name: body + b"\n" for name, body in bodies.items()}
        holder = {"bodies": bodies}
        fake, _observed = self._fake_capture_get(lambda name: holder["bodies"][name])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            first = self._capture(storage, root, "captures-a", fake)
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            original_receipt = storage.read_key(f"{prefix}/raw/release_index.receipt.json")
            holder["bodies"] = restamped
            with self.assertRaisesRegex(ValueError, "cached_source_evidence_changed"):
                self._capture(storage, root, "captures-b", fake)
            self.assertEqual(storage.read_key(f"{prefix}/raw/release_index.receipt.json"),
                             original_receipt)
            self.assertEqual(storage.read_key(f"{prefix}/raw/data.body"), bodies["data"])
            self.assertTrue(storage.object_snapshot(
                f"_construction-state/{source.DATASET_SLUG}/{first['version']}.json"))


if __name__ == "__main__":
    unittest.main()

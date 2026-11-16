"""Shared retained-capture replay validator: manifest, roster, digest and request-identity gates."""
import json
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.http_capture import (
    capture_roster_digests,
    validate_retained_capture_replay,
)
from dagster_hifld.sources.epa_frs_common import validate_capture_request_identity
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
from dagster_hifld.sources.arcgis_layer import _load_capture

NAMES = {"alpha", "beta"}
URLS = {"alpha": "https://producer.example.invalid/service/alpha",
        "beta": "https://producer.example.invalid/service/beta"}


def _receipt(body: bytes, wire: bytes, url: str) -> dict:
    return {
        "method": "GET", "url": url, "final_url": url,
        "observed_at": "2026-11-14T00:00:00Z", "status": 200, "complete_body": True,
        "bytes": len(body), "sha256": _sha(body),
        "wire_bytes": len(wire), "wire_sha256": _sha(wire),
        "content_encoding": "identity", "content_length": len(body), "error_code": None,
    }


def _write_capture(root: Path, *, bodies=None, wires=None, manifest=None, receipts=None) -> None:
    root.mkdir(parents=True, exist_ok=True)
    bodies = bodies or {name: f"{name} archive bytes".encode() for name in sorted(NAMES)}
    wires = wires or {name: f"{name} wire bytes".encode() for name in sorted(NAMES)}
    receipts = receipts or {name: _receipt(bodies[name], wires[name], URLS[name]) for name in sorted(NAMES)}
    for name in sorted(NAMES):
        (root / f"{name}.body").write_bytes(bodies[name])
        (root / f"{name}.wire").write_bytes(wires[name])
        (root / f"{name}.receipt.json").write_bytes(_canonical(receipts[name]) + b"\n")
    manifest = manifest if manifest is not None else {
        "complete": True,
        "receipts": receipts,
        "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"}),
    }
    (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")


class ValidateRetainedCaptureReplayTests(unittest.TestCase):
    def replay(self, root: Path, *, capture_names=NAMES, expected_urls=URLS):
        return validate_retained_capture_replay(
            root,
            capture_names=capture_names,
            expected_urls=expected_urls,
            load_capture=_load_capture,
            validate_request_identity=validate_capture_request_identity,
        )

    def test_valid_capture_replays_with_same_bodies_and_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            _write_capture(root)
            captures = self.replay(root)
            self.assertEqual(set(captures), NAMES)
            for name in sorted(NAMES):
                body, receipt = captures[name]
                self.assertEqual(body, f"{name} archive bytes".encode())
                self.assertEqual(receipt["url"], URLS[name])
                self.assertEqual(receipt["sha256"], _sha(f"{name} archive bytes".encode()))

    def test_symlinked_manifest_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            _write_capture(root)
            real = Path(tmp) / "real_manifest.json"
            real.write_bytes((root / "capture_manifest.json").read_bytes())
            (root / "capture_manifest.json").unlink()
            (root / "capture_manifest.json").symlink_to(real)
            with self.assertRaisesRegex(ValueError, "retained_capture_manifest_symlink_rejected"):
                self.replay(root)

    def test_symlinked_artifact_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            _write_capture(root)
            target = Path(tmp) / "elsewhere.body"
            target.write_bytes(b"elsewhere")
            (root / "alpha.body").unlink()
            (root / "alpha.body").symlink_to(target)
            with self.assertRaisesRegex(ValueError, "capture_roster_nonregular_path:alpha.body"):
                self.replay(root)

    def test_missing_artifact_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            _write_capture(root)
            (root / "beta.body").unlink()
            with self.assertRaisesRegex(ValueError, "capture_roster_mismatch"):
                self.replay(root)

    def test_extra_artifact_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            _write_capture(root)
            (root / "gamma.body").write_bytes(b"unbound artifact")
            with self.assertRaisesRegex(ValueError, "capture_roster_mismatch"):
                self.replay(root)

    def test_incomplete_manifest_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            _write_capture(root)
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            manifest["complete"] = False
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
            with self.assertRaisesRegex(ValueError, "retained_capture_manifest_invalid"):
                self.replay(root)
            manifest["complete"] = True
            manifest["receipts"] = "not a mapping"
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
            with self.assertRaisesRegex(ValueError, "retained_capture_manifest_invalid"):
                self.replay(root)

    def test_receipt_names_beyond_capture_names_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            _write_capture(root)
            with self.assertRaisesRegex(ValueError, "retained_capture_roster_or_names_invalid"):
                self.replay(root, capture_names={"alpha", "beta", "gamma"})

    def test_changed_body_hash_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            _write_capture(root)
            (root / "alpha.body").write_bytes(b"tampered archive bytes")
            with self.assertRaisesRegex(ValueError, "retained_capture_artifact_digest_mismatch"):
                self.replay(root)

    def test_changed_wire_hash_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            _write_capture(root)
            (root / "beta.wire").write_bytes(b"tampered wire bytes")
            with self.assertRaisesRegex(ValueError, "retained_capture_artifact_digest_mismatch"):
                self.replay(root)

    def test_receipt_diverging_from_manifest_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            _write_capture(root)
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            manifest["receipts"]["alpha"] = {**manifest["receipts"]["alpha"], "observed_at": "2000-01-01T00:00:00Z"}
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
            with self.assertRaisesRegex(ValueError, "retained_capture_manifest_receipt_mismatch:alpha"):
                self.replay(root)

    def test_wrong_requested_url_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            _write_capture(root, receipts={
                "alpha": _receipt(b"alpha archive bytes", b"alpha wire bytes", URLS["alpha"]),
                "beta": _receipt(b"beta archive bytes", b"beta wire bytes",
                                 "https://attacker.example.invalid/service/beta"),
            })
            with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch:beta"):
                self.replay(root)

    def test_expected_urls_are_explicit_not_inherited(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            _write_capture(root)
            other_urls = {name: url.replace("producer", "other-dataset") for name, url in URLS.items()}
            with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch:alpha"):
                self.replay(root, expected_urls=other_urls)


if __name__ == "__main__":
    unittest.main()

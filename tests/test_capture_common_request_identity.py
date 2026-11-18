"""Request-identity binding for retained captures in capture_common."""
from __future__ import annotations

import unittest

from dagster_hifld.sources.capture_common import validate_capture_request_identity


class ValidateCaptureRequestIdentityTests(unittest.TestCase):
    URL = "https://www.arcgis.com/sharing/rest/portals/test?f=json"

    def test_matching_get_receipt_passes(self):
        validate_capture_request_identity(
            "test-layer", {"method": "GET", "url": self.URL, "final_url": self.URL}, self.URL
        )

    def test_non_get_method_is_rejected(self):
        receipt = {"method": "POST", "url": self.URL, "final_url": self.URL}
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch:test-layer"):
            validate_capture_request_identity("test-layer", receipt, self.URL)

    def test_url_mismatch_is_rejected(self):
        receipt = {"method": "GET", "url": "https://example.invalid/other", "final_url": self.URL}
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch:test-layer"):
            validate_capture_request_identity("test-layer", receipt, self.URL)

    def test_final_url_redirect_is_rejected(self):
        receipt = {"method": "GET", "url": self.URL, "final_url": "https://example.invalid/wrong"}
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch:test-layer"):
            validate_capture_request_identity("test-layer", receipt, self.URL)

    def test_error_names_the_capture(self):
        receipt = {"method": "GET", "url": self.URL, "final_url": "https://example.invalid/wrong"}
        try:
            validate_capture_request_identity("wbd-6-digit-hu", receipt, self.URL)
        except ValueError as exc:
            self.assertEqual(str(exc), "retained_capture_request_identity_mismatch:wbd-6-digit-hu")
        else:
            self.fail("expected ValueError")


if __name__ == "__main__":
    unittest.main()

"""Boundary isolation for pre-existing conversion/catalog mechanics unit tests.

These tests deliberately start *after* independent acceptance. Real fail-closed
boundary and Dagster integration behavior is exercised without mocks in
test_full_verification.py; this mixin must not be used by acceptance tests.
"""

from unittest.mock import patch


class PublicationMechanicsMixin:
    def setUp(self):
        super().setUp()
        for target, value in (
            ("dagster_hifld.promotion.require_release_approval", {}),
            ("dagster_hifld.promotion.require_generated_object_approval", None),
            ("dagster_hifld.portolan.workflow.require_generated_object_approval", None),
        ):
            boundary = patch(target, return_value=value)
            boundary.start()
            self.addCleanup(boundary.stop)

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
            ("dagster_hifld.assets.publish.load_release_approvals", {}),
        ):
            boundary = patch(target, return_value=value)
            boundary.start()
            self.addCleanup(boundary.stop)
        # These older tests exercise conversion/copy mechanics after the release
        # identity boundary. Real version/approval tests do not use this mixin.
        def selected(storage, prefix):
            return {"id": "hifld/" + prefix, "version": prefix.rsplit("/", 1)[1].removeprefix("v")}, prefix
        for target in ("dagster_hifld.assets.publish.read_staged_release", "dagster_hifld.assets.portolan.read_staged_release"):
            boundary = patch(target, side_effect=selected)
            boundary.start()
            self.addCleanup(boundary.stop)
        boundary = patch("dagster_hifld.assets.portolan.require_generated_object_approval", return_value=None)
        boundary.start()
        self.addCleanup(boundary.stop)

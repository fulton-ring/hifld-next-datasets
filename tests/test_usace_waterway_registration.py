"""Waterway network ingest assets self-register; draft schedules stay stopped."""
import unittest

from dagster import DefaultScheduleStatus, build_schedule_context, validate_run_config


class WaterwayRegistrationTests(unittest.TestCase):
    def test_module_lists_register_ingest_checks_jobs_and_stopped_schedules(self):
        from dagster_hifld.definitions import defs
        from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
        from dagster_hifld.assets.usace_waterway_lines import (
            usace_navigable_waterway_network_lines,
            usace_navigable_waterway_network_lines_complete,
            usace_waterway_lines_draft_job,
            usace_waterway_lines_draft_schedule,
        )
        from dagster_hifld.assets.usace_waterway_nodes import (
            usace_waterway_network_nodes,
            usace_waterway_network_nodes_candidate_complete,
            usace_waterway_nodes_draft_job,
            usace_waterway_nodes_draft_schedule,
        )

        graph = defs.resolve_asset_graph()
        keys = {tuple(k.path) for k in graph.get_all_asset_keys()}
        node_key = ("ingest", "navigable-waterway-network-node-locations-1",
                    "navigable-waterway-network-node-locations-1")
        lines_key = ("ingest", "navigable-waterway-network-lines",
                     "navigable-waterway-network-lines")
        for expected in (node_key, lines_key, ("publish", "catalog"), ("publish", "verify"),
                         ("publish", "promote")):
            self.assertIn(expected, keys)

        registry_keys = [tuple(k.path) for a in hifld_ingest_assets for k in a.keys]
        for expected in (node_key, lines_key):
            self.assertEqual(registry_keys.count(expected), 1)

        registered_checks = set(graph.asset_check_keys)
        self.assertTrue(set(usace_waterway_network_nodes_candidate_complete.check_keys)
                        <= registered_checks)
        self.assertTrue(set(usace_navigable_waterway_network_lines_complete.check_keys)
                        <= registered_checks)
        self.assertTrue(next(iter(usace_waterway_network_nodes_candidate_complete.check_specs)).blocking)
        self.assertTrue(next(iter(usace_navigable_waterway_network_lines_complete.check_specs)).blocking)

        for schedule, job in (
            (usace_waterway_nodes_draft_schedule, usace_waterway_nodes_draft_job),
            (usace_waterway_lines_draft_schedule, usace_waterway_lines_draft_job),
        ):
            job_def = defs.resolve_job_def(job.name)
            request = schedule.evaluate_tick(build_schedule_context()).run_requests[0]
            validate_run_config(job_def, request.run_config)
            self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()

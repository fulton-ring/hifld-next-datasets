"""Register the Airport Area source-only refresh as a stopped draft."""
import unittest

from dagster import AssetKey, DefaultScheduleStatus

from dagster_hifld.definitions import defs


class AirportAreaRefreshRegistrationTests(unittest.TestCase):
    def test_registered_stopped_source_only_refresh(self):
        repository = defs.get_repository_def()
        job = repository.get_job("usace_airport_area_draft_refresh")
        self.assertEqual(set(job.asset_layer.executable_asset_keys), {
            AssetKey(["ingest", "airport-area-usace-ienc", "airport-area-usace-ienc"])
        })
        check_nodes = set(job.graph.node_names()) - {
            "ingest__airport_area_usace_ienc__airport_area_usace_ienc"
        }
        self.assertEqual(len(check_nodes), 3)
        self.assertTrue(all("usace_airport_area_" in node for node in check_nodes))
        schedule = repository.get_schedule_def("usace_airport_area_draft_refresh_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.cron_schedule, "0 8 1 * *")
        checks = {
            spec.name: spec
            for definition in defs.asset_checks
            for spec in definition.check_specs
        }
        self.assertTrue(checks["usace_airport_area_complete"].blocking)
        self.assertTrue(checks["usace_airport_area_quality"].blocking)
        self.assertFalse(checks["usace_airport_area_release"].blocking)


if __name__ == "__main__":
    unittest.main()

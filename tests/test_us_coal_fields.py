"""Parked-record fixtures grant no source/contract acceptance."""
import json
import os
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import us_coal_fields as s


def fgdc_xml(progress=None, update=None, spacing=""):
    body = (
        "<metadata><idinfo><citation><citeinfo><title>" + s.OFR_TITLE + "</title></citeinfo></citation>"
        "<status><progress>" + (progress if progress is not None else "Complete") + "</progress>"
        "<update>" + (update if update is not None else "None planned") + "</update></status>"
        "</idinfo></metadata>"
    )
    if spacing:
        body = body.replace("><", ">\n" + spacing + "<")
    return body.encode("utf-8")


def layer_json(**overrides):
    doc = {
        "id": 0, "name": s.COALBYRANK_LAYER_NAME, "type": "Feature Layer",
        "geometryType": "esriGeometryPolygon", "parentLayer": None, "subLayers": [],
    }
    doc.update(overrides)
    return json.dumps(doc).encode("utf-8")


class StructuralUsCoalFieldsTests(unittest.TestCase):
    def test_ofr_release_status_pins_the_one_time_release(self):
        status = s.parse_ofr_release_status(fgdc_xml())
        self.assertFalse(status["approved"])
        self.assertTrue(status["one_time_release"])
        self.assertEqual(status["progress"], "Complete")
        self.assertEqual(status["update_frequency"], "None planned")
        self.assertIn("not a pipeline input", status["eligibility"])
        self.assertIn("None planned", Path(s.__file__).read_text())
        with self.subTest(label="whitespace_tolerant"):
            status = s.parse_ofr_release_status(fgdc_xml(spacing="\n  "))
            self.assertEqual(status["progress"], "Complete")
        for label, kwargs, pattern in (
            ("progress", {"progress": "Ongoing"}, "ofr_release_status_changed"),
            ("progress_missing", {"progress": ""}, "ofr_release_status_changed"),
            ("update", {"update": "As needed"}, "ofr_maintenance_frequency_changed"),
            ("update_missing", {"update": ""}, "ofr_maintenance_frequency_changed"),
        ):
            with self.subTest(label=label), self.assertRaisesRegex(
                    ValueError, pattern):
                s.parse_ofr_release_status(fgdc_xml(**kwargs))

    def test_layer_maintenance_records_absence_and_fails_on_edit_metadata(self):
        status = s.parse_layer_maintenance(layer_json())
        self.assertFalse(status["approved"])
        self.assertTrue(status["live_service"])
        self.assertEqual(status["layer_name"], s.COALBYRANK_LAYER_NAME)
        self.assertIsNone(status["editing_info"])
        self.assertIsNone(status["last_edit_date"])
        self.assertIsNone(status["edit_fields_info"])
        self.assertIn("not established as a maintained publication", status["eligibility"])
        for label, override in (
            ("editingInfo", {"editingInfo": {"lastEditDate": 1700000000000}}),
            ("lastEditDate", {"lastEditDate": 1700000000000}),
            ("editFieldsInfo", {"editFieldsInfo": {"dateFieldsTimeReference": None,
                                                   "lastEditDateField": "LastEdited"}}),
        ):
            with self.subTest(label=label), self.assertRaisesRegex(
                    ValueError, "coalbyrank_maintenance_metadata_published_re_evaluate_source_eligibility:"
                                + label):
                s.parse_layer_maintenance(layer_json(**override))
        with self.assertRaisesRegex(ValueError, "coalbyrank_layer_document_unparseable"):
            s.parse_layer_maintenance(b"<html>error</html>")

    def test_staging_fails_closed_without_reading_or_writing_anything(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            for kwargs in ({}, {"capture_dir": tmp}, {"capture_dir": tmp, "version": "v1"}):
                with self.subTest(kwargs=kwargs), self.assertRaisesRegex(
                        ValueError, "no_maintained_producer_publication_park_or_retire"):
                    s.stage_us_coal_fields(storage, **kwargs)
            self.assertEqual(os.listdir(tmp), [])
        with tempfile.TemporaryDirectory() as tmp:
            for resource in (PublishedStorageResource(local_dir=tmp),
                             StagingStorageResource(bucket="forbidden", use_local=False), None):
                with self.subTest(resource=type(resource).__name__), self.assertRaisesRegex(
                        ValueError, "no_maintained_producer_publication"):
                    s.stage_us_coal_fields(resource)
            self.assertEqual(os.listdir(tmp), [])

    def test_park_evidence_names_the_dated_blockers(self):
        evidence = s.PARK_EVIDENCE
        self.assertIn("park or retire", evidence["decision"])
        self.assertIn("Deprecate", evidence["retired_hifld_item"]["crosswalk"])
        self.assertIn("DataLumos", evidence["retired_hifld_item"]["banned_archive"])
        one_time = evidence["one_time_origin_publication"]
        self.assertEqual(one_time["first_posted"], "2013-04-02")
        self.assertIn("None planned", one_time["producer_status"])
        self.assertEqual(one_time["fgdc_metadata"]["sha256"],
                         "17299068e32ab1bffce6391fd803ce70a6dd08fea91de49772e432fbb2833ba5")
        self.assertIn("2026-11-14", one_time["verified"])
        self.assertIn("not maintained", one_time["eligibility"])
        live = evidence["live_service_candidate_checked"]
        self.assertIn("2026-11-14", live["content_fidelity_verified"])
        self.assertIn("no editingInfo, lastEditDate or editFieldsInfo",
                      " ".join(live["maintenance_evidence_checked_and_absent"]))
        self.assertIn("availability preservation, not refresh",
                      " ".join(live["maintenance_evidence_checked_and_absent"]))
        self.assertIn("not a maintained publication", live["eligibility"])
        alternatives = {item["product"]: item for item in evidence["current_alternatives_checked"]}
        self.assertEqual({item["check_date"] for item in alternatives.values()}, {"2026-11-14"})
        self.assertIn("not separate maintained products",
                      alternatives["NCRDS CoalByAge and CoalProvinces MapServices"]["finding"])
        self.assertIn("availability preservation", alternatives["EUSEF Data Management task"]["finding"])
        self.assertIn("no national US coal-fields layer",
                      alternatives["Maps of Energy Occurrence task"]["finding"])
        self.assertIn("no national coal-fields polygon layer",
                      alternatives["U.S. Coal Resources and Reserves Assessments (EUSEF task)"]["finding"])

    def test_pins_are_producer_owned_research_records(self):
        self.assertEqual(s.producer_hosts(), {"pubs.usgs.gov", "energy.usgs.gov", "www.usgs.gov"})
        for name, pin in s.PINS.items():
            self.assertTrue(pin["url"].startswith("https://"), name)
            self.assertGreater(pin["bytes"], 0, name)
            self.assertEqual(len(pin["sha256"]), 64, name)
            self.assertEqual(pin["observed"], "2026-11-14", name)
            self.assertTrue(pin["role"].startswith("research_record"), name)
        self.assertEqual(s.PINS["usgs_ofr_fgdc_metadata"]["sha256"],
                         "17299068e32ab1bffce6391fd803ce70a6dd08fea91de49772e432fbb2833ba5")
        self.assertEqual(s.PINS["usgs_ofr_gis_zip"]["sha256"],
                         "e491215d0a937cae48cc5e9a2763ea43c85a4d11c183ec7c5b1ac559d2a585a5")

    def test_module_is_network_free_and_leaves_no_reconstruction(self):
        source = Path(s.__file__).read_text()
        for forbidden in ("httpx", "requests", "urllib.request", "http.client", "socket",
                          "import zipfile", "import struct", "import tarfile", "import pyarrow",
                          "import shapely", "import pyproj", "acquire", "manifest", "parquet",
                          "storage.googleapis.com", "hifld-next-datasets-prod"):
            self.assertNotIn(forbidden, source, forbidden)
        self.assertIn("PARK_REASON", source)
        self.assertEqual(sorted(name for name in dir(s) if name.startswith("stage_")),
                         ["stage_us_coal_fields"])


class RegistrationTests(unittest.TestCase):
    def test_asset_job_and_schedule_are_parked_registered_and_stopped(self):
        from dagster import DefaultScheduleStatus
        from dagster_hifld.assets import us_coal_fields as module
        from dagster_hifld.assets.discovery import discovered
        self.assertEqual(module.us_coal_fields.key.to_user_string(),
                         "ingest/us-coal-fields-1/us-coal-fields-1")
        self.assertIn(module.us_coal_fields, discovered("INGEST_ASSETS"))
        self.assertIn(module.us_coal_fields_parked_job, discovered("JOBS"))
        self.assertIn(module.us_coal_fields_parked_schedule, discovered("SCHEDULES"))
        self.assertIs(module.us_coal_fields_parked_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(module.us_coal_fields_parked_schedule.cron_schedule, "0 7 1 * *")
        self.assertEqual(module.us_coal_fields_parked_schedule.job_name, "us_coal_fields_parked_refusal")
        self.assertEqual(s.SLUG, "us-coal-fields-1")
        self.assertEqual(s.PARK_REASON, "no_maintained_producer_publication_park_or_retire")

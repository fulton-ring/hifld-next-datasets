"""Tests for the newest-edition NCFO ferry terminals adapter."""
import copy
import json
import tempfile
from pathlib import Path
from unittest import TestCase

import httpx
import pyarrow.parquet as pq
from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import ferry_terminals as s

COLUMNS = [
    ("terminal_id", "number"), ("terminal_name", "text"), ("term_city", "text"),
    ("term_state", "text"), ("term_country", "text"), ("latitude", "number"),
    ("longitude", "number"), ("in_operation", "number"), ("intercity_bus", "number"),
    ("intercity_rail", "number"), ("local_bus", "number"), ("local_rail", "number"),
    ("bike_share", "number"), ("parking", "number"), ("terminal_operation", "text"),
    ("terminal_ownership", "text"), ("number_of_operators", "number"), ("census_year", "number"),
]


def rows_fixture():
    base = {
        "terminal_name": "Victoria Inner Harbor", "term_city": "Victoria",
        "term_state": "BC", "term_country": "Canada", "in_operation": "1",
        "intercity_bus": "1", "intercity_rail": "0", "local_bus": "1",
        "local_rail": "1", "bike_share": "0", "parking": "1",
        "terminal_operation": "PRIVATE", "terminal_ownership": "BOTH",
        "number_of_operators": "2", "census_year": "2024",
    }
    return [
        {**base, "terminal_id": "4", "latitude": "48.42169854", "longitude": "-123.3722943"},
        {**base, "terminal_id": "19", "latitude": "18.41911192", "longitude": "-64.61785082",
         "terminal_name": "Road Town, Tortola", "term_state": "VI"},
        {**base, "terminal_id": "1123", "latitude": "33.935116", "longitude": "-77.99295",
         "terminal_name": "Deep Point", "term_state": "NC"},
    ]


def view_fixture(year=2024, dataset_id="difu-6bgs"):
    return {
        "id": dataset_id,
        "name": f"{year} NCFO Terminals File",
        "attribution": "USDOT/Bureau of Transportation Statistics",
        "description": f"This is information about ferry terminals that are included in the {year} NCFO dataset.",
        "provenance": "official",
        "publicationDate": 1781099951,
        "rowsUpdatedAt": 1781099940,
        "columns": [{"fieldName": name, "dataTypeName": kind} for name, kind in COLUMNS],
    }


def catalog_fixture(extra=()):
    results = [
        {"resource": {"name": "2020 NCFO Terminals File", "id": "ca7h-i9yt", "type": "dataset"},
         "metadata": {"domain": "data.bts.gov"}},
        {"resource": {"name": "2024 NCFO Terminals File", "id": "difu-6bgs", "type": "dataset"},
         "metadata": {"domain": "data.bts.gov"}},
        {"resource": {"name": "2024 NCFO Terminals Supplemental", "id": "wmvq-by24", "type": "dataset"},
         "metadata": {"domain": "data.bts.gov"}},
        {"resource": {"name": "2024 NCFO Terminals and Segments", "id": "rb4b-te24", "type": "story"},
         "metadata": {"domain": "data.bts.gov"}},
        *extra,
    ]
    return {"results": results}


def response(status, payload):
    body = json.dumps(payload).encode()
    return httpx.Response(status, headers={"content-type": "application/json"},
                          stream=httpx.ByteStream(body))


def handler(catalog, view, rows, count=3):
    def respond(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.startswith(s.CATALOG_URL):
            return response(200, catalog)
        if url == s._view_url("difu-6bgs"):
            return response(200, view)
        if url == s._count_url("difu-6bgs"):
            return response(200, [{"count": str(count)}])
        if url == s._page_url("difu-6bgs", 0):
            return response(200, rows)
        return response(404, {"error": True})
    return respond


class FerryTerminalsSourceTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.storage = StagingStorageResource(local_dir=self.tmp.name, use_local=True)

    def client(self, catalog, view=None, rows=None, count=3):
        return httpx.Client(transport=httpx.MockTransport(
            handler(catalog, view or view_fixture(), rows or rows_fixture(), count)))

    def stage(self, client):
        return s.stage_ferry_terminals_candidate(
            self.storage, capture_dir=Path(self.tmp.name) / "captures", client=client)

    def test_discovers_newest_exact_edition_and_ignores_supplements_and_stories(self):
        edition = s.select_newest_edition(catalog_fixture())
        self.assertEqual(edition, {"dataset_id": "difu-6bgs", "name": "2024 NCFO Terminals File", "year": 2024})
        newest = catalog_fixture([{"resource": {"name": "2026 NCFO Terminals File", "id": "abcd-efgh",
                                                "type": "dataset"},
                                   "metadata": {"domain": "data.bts.gov"}}])
        self.assertEqual(s.select_newest_edition(newest)["year"], 2026)

    def test_duplicate_edition_and_missing_dataset_fail_closed(self):
        duplicate = catalog_fixture([{"resource": {"name": "2024 NCFO Terminals File", "id": "dupe-dupe",
                                                   "type": "dataset"},
                                      "metadata": {"domain": "data.bts.gov"}}])
        with self.assertRaisesRegex(ValueError, "ambiguous_ncfo_terminals_edition"):
            s.select_newest_edition(duplicate)
        with self.assertRaisesRegex(ValueError, "no_official_ncfo_terminals_dataset"):
            s.select_newest_edition({"results": []})

    def test_view_must_be_the_official_bts_edition(self):
        view = view_fixture()
        self.assertEqual(s.validate_view(view, {"dataset_id": "difu-6bgs",
                                                "name": "2024 NCFO Terminals File", "year": 2024}),
                         [name for name, _ in COLUMNS])
        for mutate, reason in (
            (lambda v: v.update(provenance="community"), "source_is_not_official_bts_publication"),
            (lambda v: v.update(attribution="A mirror"), "source_is_not_official_bts_publication"),
            (lambda v: v.update(name="2022 NCFO Terminals File"), "source_dataset_identity_changed"),
            (lambda v: v["columns"].pop(), "source_column_roster_drift"),
            (lambda v: v["columns"][0].update(dataTypeName="text"), "source_column_physical_type_drift"),
        ):
            changed = copy.deepcopy(view)
            mutate(changed)
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(ValueError, reason):
                    s.validate_view(changed, {"dataset_id": "difu-6bgs",
                                              "name": "2024 NCFO Terminals File", "year": 2024})

    def test_row_typing_rejects_edition_identifier_and_value_drift(self):
        columns = [name for name, _ in COLUMNS]
        edition = {"year": 2024}
        rows = s.normalize_rows([rows_fixture()], columns, edition, 3)
        self.assertEqual([row["terminal_id"] for row in rows], [4, 19, 1123])
        self.assertEqual((rows[0]["latitude"], rows[0]["longitude"]),
                         (48.42169854, -123.3722943))
        cases = (
            (lambda r: r[0].update(census_year="2022"), "source_edition_year_mismatch"),
            (lambda r: r[1].update(terminal_id="4"), "source_terminal_identifier_duplicate"),
            (lambda r: r[0].update(latitude="91"), "source_coordinate_out_of_range"),
            (lambda r: r[0].update(parking="2"), "source_flag_value_drift"),
            (lambda r: r[0].pop("terminal_name"), "source_record_column_roster_drift"),
            (lambda r: r[0].update(terminal_id="0004"), "source_integer_value_drift"),
            (lambda r: r[0].update(latitude="nan"), "source_number_value_drift"),
            (lambda r: r[0].update(terminal_name=7), "source_text_value_drift"),
        )
        for mutate, reason in cases:
            changed = copy.deepcopy(rows_fixture())
            mutate(changed)
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(ValueError, reason):
                    s.normalize_rows([changed], columns, edition, 3)
        with self.assertRaisesRegex(ValueError, "source_population_incomplete"):
            s.normalize_rows([rows_fixture()], columns, edition, 4)
        reordered = list(reversed(rows_fixture()))
        with self.assertRaisesRegex(ValueError, "source_terminal_identifier_order_changed"):
            s.normalize_rows([reordered], columns, edition, 3)

    def test_stages_native_parquet_and_2024_provenance(self):
        client = self.client(catalog_fixture())
        try:
            result = self.stage(client)
        finally:
            client.close()
        self.assertEqual(result["edition_year"], 2024)
        self.assertEqual(result["feature_count"], 3)
        self.assertEqual(result["source_release_date"], "2026-06-10")
        root = Path(self.tmp.name) / s.DATASET_SLUG / s.FILE_SLUG / result["version"]
        table = pq.read_table(root / "geoparquet" / f"{s.FILE_SLUG}.parquet")
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(str(table.schema.field("terminal_id").type), "int64")
        self.assertEqual(str(table.schema.field("latitude").type), "double")
        self.assertEqual(table.column("census_year").to_pylist(), [2024, 2024, 2024])
        self.assertEqual(table.column("geometry").to_pylist()[0][:1], b"\x01")
        manifest = json.loads((root / "metadata/source_manifest.json").read_bytes())
        self.assertFalse(manifest["approved"])
        self.assertIn("2024", manifest["title"])
        self.assertEqual(manifest["date_issued"], "2026-06-10")
        self.assertTrue(manifest["inputs"])
        self.assertEqual({input_["host"] for input_ in manifest["inputs"]}, {"data.bts.gov"})
        audit = json.loads((root / "metadata/source_audit.json").read_bytes())
        self.assertEqual(audit["edition_year"], 2024)
        self.assertEqual(audit["census_year_values"], [2024])
        self.assertFalse(audit["release_accepted"])

    def test_rerun_is_idempotent_and_keeps_one_version(self):
        client = self.client(catalog_fixture())
        try:
            first = self.stage(client)
            second = self.stage(client)
        finally:
            client.close()
        self.assertEqual(first, second)
        versions = list((Path(self.tmp.name) / s.DATASET_SLUG / s.FILE_SLUG).iterdir())
        self.assertEqual(len(versions), 1)

    def test_rerun_reuses_candidate_after_catalog_metadata_is_written(self):
        client = self.client(catalog_fixture())
        try:
            first = self.stage(client)
            snapshot_source_metadata(self.storage, s.DATASET_SLUG, s.FILE_SLUG, first["version"])
            resolved = load_resolved_source_manifest(
                self.storage, s.DATASET_SLUG, s.FILE_SLUG, first["version"])
            summary = summarize_staged_catalog(
                self.storage, s.DATASET_SLUG, s.FILE_SLUG, first["version"],
                s.DATASET_SLUG, source_metadata=resolved.metadata)
            write_catalog_metadata(
                self.storage, s.DATASET_SLUG, s.FILE_SLUG, first["version"],
                summary.quality_manifest, summary.data_dictionary)
            self.assertEqual(self.stage(client), first)
        finally:
            client.close()

    def test_count_change_during_capture_quarantines_evidence(self):
        calls = {"count": 0}

        def respond(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if url == s._count_url("difu-6bgs"):
                calls["count"] += 1
                return response(200, [{"count": "3" if calls["count"] == 1 else "4"}])
            return handler(catalog_fixture(), view_fixture(), rows_fixture())(request)

        client = httpx.Client(transport=httpx.MockTransport(respond))
        try:
            with self.assertRaisesRegex(ValueError, "source_population_changed_during_capture"):
                self.stage(client)
        finally:
            client.close()
        quarantined = list((Path(self.tmp.name) / "_quarantine" / s.DATASET_SLUG).iterdir())
        self.assertEqual(len(quarantined), 1)

    def test_requires_unprefixed_local_staging(self):
        with self.assertRaisesRegex(ValueError, "draft_requires_explicit_isolated_local_staging"):
            s.require_local(StagingStorageResource(local_dir=self.tmp.name, use_local=True, prefix="x"))
        with self.assertRaisesRegex(ValueError, "draft_requires_explicit_isolated_local_staging"):
            s.require_local(StagingStorageResource(local_dir=self.tmp.name, use_local=False,
                                                   bucket="a-bucket"))

import hashlib
import gzip
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs

import httpx
import pyarrow.parquet as pq
import shapely

from dagster import DefaultScheduleStatus

from dagster_hifld.sources import national_transit_map_agencies as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def fixture(oids=(1,)):
    fields = [
        {"name":"OBJECTID","type":"esriFieldTypeOID","alias":"OBJECTID","nullable":False,"domain":None},
        {"name":"NTD_ID","type":"esriFieldTypeString","alias":"NTD_ID","length":254,"nullable":True,"domain":None},
        {"name":"NTAD_NTD_ID","type":"esriFieldTypeString","alias":"NTAD_NTD_ID","length":254,"nullable":True,"domain":None},
        {"name":"Agency_Name","type":"esriFieldTypeString","alias":"Agency_Name","length":254,"nullable":True,"domain":None},
        {"name":"Common_Name","type":"esriFieldTypeString","alias":"Common_Name","length":254,"nullable":True,"domain":None},
        {"name":"City","type":"esriFieldTypeString","alias":"City","length":254,"nullable":True,"domain":None},
        {"name":"State","type":"esriFieldTypeString","alias":"State","length":254,"nullable":True,"domain":None},
        {"name":"Lon","type":"esriFieldTypeDouble","alias":"Lon","nullable":True,"domain":None},
        {"name":"Lat","type":"esriFieldTypeDouble","alias":"Lat","nullable":True,"domain":None},
        {"name":"In_Latest_NTAD_Upload","type":"esriFieldTypeString","alias":"In_Latest_NTAD_Upload","length":254,"nullable":True,"domain":None},
        {"name":"License","type":"esriFieldTypeString","alias":"License","length":254,"nullable":True,"domain":None},
    ]
    layer = {"id":0,"name":"National Transit Map Agencies","geometryType":"esriGeometryPoint","objectIdField":"OBJECTID","maxRecordCount":2000,"hasZ":True,"hasM":False,
        "extent":{"spatialReference":{"latestWkid":4326}},"fields":fields}
    features = [
        {"attributes":{"OBJECTID":oid,"NTD_ID":f"{oid:04d}","NTAD_NTD_ID":f"{oid:04d}","Agency_Name":f"Example Transit {oid}","Common_Name":"Example","City":"Example","State":"CA","Lon":-120.0+(oid % 50)/100,"Lat":37.0,"In_Latest_NTAD_Upload":"Yes","License":"Public Domain"},"geometry":{"x":-120.0+(oid % 50)/100,"y":37.0,"z":0}}
        for oid in oids
    ]
    service={"serviceItemId":source.ITEM_ID,"layers":[{"id":0}]}
    layer["copyrightText"] = source.LICENSE
    item={"id":source.ITEM_ID,"owner":source.OWNER,"access":"public","url":source.SERVICE_URL,
          "description":"compiled on September 23, 2026","orgId":"xOi1kZaI0eWDREZv",
          "title":"National Transit Map Agencies","type":"Feature Service",
          "licenseInfo":source.LICENSE,
          "accessInformation":"USDOT; BTS; City of Lawndale; Via Mobility Services"}
    ids=sorted(oids)
    docs={"item":item,"service_before":service,"layer_before":layer,"count_before":{"count":len(ids)},"ids_before":{"objectIds":ids},"ids_seed":{"objectIds":ids},
        "count_after":{"count":len(ids)},"ids_after":{"objectIds":ids},"layer_after":layer,"service_after":service}
    for i, start in enumerate(range(0, len(features), source.PAGE_SIZE)):
        docs[f"page_{i:03}"]={"spatialReference":{"latestWkid":4326},
                             "features":features[start:start+source.PAGE_SIZE]}
    fields_profile,_=source._field_profile(fields)
    schema=hashlib.sha256(canonical(fields_profile)).hexdigest()
    urls = dict(source._capture_roster(ids))
    captures={k:(canonical(v),{"method":"GET","url":urls[k],"final_url":urls[k],
                              "complete_body":True,"status":200}) for k,v in docs.items()}
    return captures, schema


def write_retained(root, captures, mtime=0):
    records = {}
    for name, (body, receipt) in captures.items():
        wire = gzip.compress(body, mtime=mtime)
        receipt = {**receipt, "observed_at": "2026-10-19T20:00:00+00:00",
                   "bytes": len(body), "sha256": source._sha(body),
                   "wire_bytes": len(wire), "wire_sha256": source._sha(wire),
                   "content_encoding": "gzip", "content_length": len(wire)}
        (root / f"{name}.body").write_bytes(body)
        (root / f"{name}.wire").write_bytes(wire)
        (root / f"{name}.receipt.json").write_bytes(canonical(receipt) + b"\n")
        records[name] = (body, receipt)
    count = json.loads(captures["count_before"][0])["count"]
    (root / "capture_manifest.json").write_bytes(canonical({
        "complete": True, "feature_count": count,
        "receipts": {name: receipt for name, (_, receipt) in records.items()}}))
    return records


def fixture_transport(captures, mtime=0, fail=None):
    seed = json.loads(captures["ids_seed"][0])["objectIds"]
    plan = iter(source._capture_roster(seed))

    def respond(request):
        name, url = next(plan)
        if str(request.url) != url:
            raise AssertionError(f"unexpected request: {request.url}")
        if name == fail:
            return httpx.Response(503, stream=httpx.ByteStream(b'{"error":"unavailable"}'))
        body = captures[name][0]
        wire = gzip.compress(body, mtime=mtime)
        return httpx.Response(200, headers={"Content-Encoding": "gzip",
                                          "Content-Length": str(len(wire))},
                              stream=httpx.ByteStream(wire))

    return httpx.MockTransport(respond)


class NationalTransitMapAgenciesCaptureTests(unittest.TestCase):
    def setUp(self):
        self.captures, schema=fixture()
        self.patch=patch.object(source, "EXPECTED_SCHEMA_SHA256", schema)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()

    def test_accepts_complete_exact_population(self):
        fields,features=source.validate_captures(self.captures)
        self.assertEqual(len(fields),11)
        self.assertEqual(features[0]["attributes"]["NTD_ID"],"0001")
        self.assertFalse(hasattr(source, "EXPECTED_COUNT"))

    def test_accepts_truthful_page_count_transitions_and_changed_compilation(self):
        for count in (1, 250, 251, 1250, 1251):
            with self.subTest(count):
                captures, schema = fixture(oids=range(1, count + 1))
                item = json.loads(captures["item"][0])
                item["description"] = "compiled on October 24, 2026"
                captures["item"] = (canonical(item), captures["item"][1])
                with patch.object(source, "EXPECTED_SCHEMA_SHA256", schema):
                    fields, features = source.validate_captures(captures)
                self.assertEqual(len(features), count)
                self.assertEqual(len(fields), 11)

    def test_exact_id_requests_keep_native_query_parameters_and_observational_dates(self):
        roster = source._capture_roster([7, 2, 91])
        page_url = dict(roster)["page_000"]
        params = parse_qs(httpx.URL(page_url).query.decode())
        self.assertEqual(params, {"objectIds": ["7,2,91"], "outFields": ["*"],
                                 "returnGeometry": ["true"], "returnZ": ["true"],
                                 "returnM": ["true"], "outSR": ["4326"],
                                 "orderByFields": ["OBJECTID ASC"], "f": ["json"]})
        self.assertEqual(source._compilation_date({"description": "compiled on October 24, 2026"}),
                         "2026-10-24")
        self.assertIsNone(source._compilation_date({"description": "current agency compilation"}))
        self.assertIsNone(source._compilation_date({"description": "compiled on February 31, 2026"}))

    def test_rejects_missing_or_changed_ids(self):
        docs=json.loads(self.captures["ids_after"][0]);docs["objectIds"]=[]
        self.captures["ids_after"]=(canonical(docs),self.captures["ids_after"][1])
        with self.assertRaisesRegex(ValueError,"source_count_or_id_roster_drift"):
            source.validate_captures(self.captures)

    def test_rejects_unexpected_column_and_type_drift(self):
        docs=json.loads(self.captures["layer_before"][0]);docs["fields"].append({"name":"NEW_FIELD","type":"esriFieldTypeString"})
        self.captures["layer_before"]=(canonical(docs),self.captures["layer_before"][1])
        self.captures["layer_after"]=(canonical(docs),self.captures["layer_after"][1])
        with self.assertRaisesRegex(ValueError,"source_schema_drift"):
            source.validate_captures(self.captures)

    def test_accepts_value_edits_and_rejects_source_type_change(self):
        page=json.loads(self.captures["page_000"][0]);page["features"][0]["attributes"]["NTD_ID"]="1"
        self.captures["page_000"]=(canonical(page),self.captures["page_000"][1])
        fields,features=source.validate_captures(self.captures)
        self.assertEqual(features[0]["attributes"]["NTD_ID"],"1")

        self.captures,_=fixture()
        layer=json.loads(self.captures["layer_before"][0]);layer["fields"][0]["type"]="esriFieldTypeInteger"
        self.captures["layer_before"]=(canonical(layer),self.captures["layer_before"][1]);self.captures["layer_after"]=(canonical(layer),self.captures["layer_after"][1])
        with self.assertRaisesRegex(ValueError,"source_schema_drift"):
            source.validate_captures(self.captures)

    def test_rejects_geometry_and_truncated_pages(self):
        page=json.loads(self.captures["page_000"][0]);page["features"][0]["geometry"]={"x":-120.0}
        self.captures["page_000"]=(canonical(page),self.captures["page_000"][1])
        with self.assertRaisesRegex(ValueError,"source_record_or_geometry_schema_drift"):
            source.validate_captures(self.captures)

        self.captures,_=fixture()
        page=json.loads(self.captures["page_000"][0]);page["exceededTransferLimit"]=True
        self.captures["page_000"]=(canonical(page),self.captures["page_000"][1])
        with self.assertRaisesRegex(ValueError,"source_page_error_or_truncation"):
            source.validate_captures(self.captures)

    def test_accepts_an_upstream_added_feature_and_warns_on_unapproved_drift(self):
        from dagster_hifld.assets.national_transit_map_agencies import (
            bts_national_transit_map_agencies_approved_baseline_drift,
        )

        self.captures,_=fixture(oids=(1,2))
        fields,features=source.validate_captures(self.captures)
        self.assertEqual([f["attributes"]["OBJECTID"] for f in features],[1,2])
        candidate={"dataset_slug":source.DATASET_SLUG,"file_slug":source.FILE_SLUG,
            "feature_count":2,"source_id_sha256":source._sha(source._canonical([1,2])),
            "release_accepted":False}
        drift=bts_national_transit_map_agencies_approved_baseline_drift(candidate)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value,"skipped_no_approved_baseline")

    def test_replays_retained_body_wire_and_original_receipt_without_fetching(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture = root / "capture"
            capture.mkdir()
            destination = root / "copy"
            destination.mkdir()
            captured = write_retained(capture, self.captures)
            with patch.object(httpx.Client, "send", side_effect=AssertionError("HTTP")):
                replay = source._load_retained_capture(capture, destination)
            self.assertEqual(replay, captured)
            for name in captured:
                self.assertEqual((destination / f"{name}.wire").read_bytes(),
                                 (capture / f"{name}.wire").read_bytes())

    def test_admits_retained_artifacts_before_content_reads_or_quarantine(self):
        from dagster_hifld.resources import StagingStorageResource

        for case in ("total", "body_limit", "manifest_limit", "fifo", "symlink",
                     "directory", "unexpected", "parent_symlink"):
            with self.subTest(case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                capture = root / "capture"
                capture.mkdir()
                write_retained(capture, self.captures)
                total = sum(path.stat().st_size for path in capture.iterdir())
                target = capture / "ids_seed.body"
                if case in {"fifo", "symlink", "directory"}:
                    target.unlink()
                    if case == "fifo":
                        os.mkfifo(target)
                    elif case == "symlink":
                        target.symlink_to(capture / "item.body")
                    else:
                        target.mkdir()
                elif case == "unexpected":
                    (capture / "unbound.body").write_bytes(b"unbound")
                elif case == "parent_symlink":
                    (root / "alias").symlink_to(root, target_is_directory=True)
                    capture = root / "alias" / "capture"
                with (
                    patch.object(source, "MAX_CAPTURE_BYTES",
                                 total - 1 if case == "total" else source.MAX_CAPTURE_BYTES),
                    patch.object(source, "MAX_BODY_BYTES",
                                 1 if case == "body_limit" else source.MAX_BODY_BYTES),
                    patch.object(source, "MAX_MANIFEST_BYTES",
                                 1 if case == "manifest_limit" else source.MAX_MANIFEST_BYTES),
                    patch.object(Path, "read_bytes", side_effect=AssertionError("content read")),
                    patch.object(source, "quarantine_incomplete_capture",
                                 side_effect=AssertionError("quarantine")),
                    patch.object(httpx.Client, "send", side_effect=AssertionError("HTTP")),
                ):
                    with self.assertRaises(ValueError):
                        source.capture_national_transit_map_agencies(
                            StagingStorageResource(local_dir=str(root / "stage")), version="admission",
                            capture_directory=str(root / "output"),
                            replay_capture_directory=str(capture))

    def test_replay_binds_decoded_wire_and_exact_requests_after_repinned_hashes(self):
        for case in ("decoded_body", "malformed_gzip", "different_gzip_body", "request",
                     "receipt", "missing_wire", "extra_page"):
            with self.subTest(case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                capture = root / "capture"
                capture.mkdir()
                destination = root / "copy"
                destination.mkdir()
                records = write_retained(capture, self.captures)
                name = "page_000"
                body, receipt = records[name]
                receipt = dict(receipt)
                if case == "decoded_body":
                    page = json.loads(body)
                    page["features"][0]["attributes"]["Agency_Name"] = "forged"
                    body = canonical(page)
                    (capture / f"{name}.body").write_bytes(body)
                    receipt.update(bytes=len(body), sha256=source._sha(body))
                elif case in {"malformed_gzip", "different_gzip_body"}:
                    wire = (b"malformed" if case == "malformed_gzip" else
                            gzip.compress(b"different decoded body", mtime=0))
                    (capture / f"{name}.wire").write_bytes(wire)
                    receipt.update(wire_bytes=len(wire), wire_sha256=source._sha(wire),
                                   content_length=len(wire))
                elif case == "request":
                    receipt["url"] = receipt["final_url"] = source.LAYER_URL + "/query?objectIds=999"
                elif case == "receipt":
                    receipt["method"] = "POST"
                elif case == "missing_wire":
                    (capture / f"{name}.wire").unlink()
                else:
                    for suffix in ("body", "wire", "receipt.json"):
                        (capture / f"page_001.{suffix}").write_bytes(
                            (capture / f"{name}.{suffix}").read_bytes())
                (capture / f"{name}.receipt.json").write_bytes(canonical(receipt) + b"\n")
                manifest = json.loads((capture / "capture_manifest.json").read_bytes())
                manifest["receipts"][name] = receipt
                (capture / "capture_manifest.json").write_bytes(canonical(manifest))
                with patch.object(httpx.Client, "send", side_effect=AssertionError("HTTP")):
                    with self.assertRaises(ValueError):
                        source._load_retained_capture(capture, destination)
                self.assertFalse(list(destination.iterdir()))

    def test_rejects_omitted_duplicated_or_cross_page_ids_and_wrong_native_contract(self):
        captures, _ = fixture(range(1, 252))
        for case in ("cross_page", "duplicate", "missing", "owner", "org", "license", "z"):
            with self.subTest(case):
                changed = dict(captures)
                if case in {"owner", "org", "license"}:
                    item = json.loads(changed["item"][0])
                    item[{"owner": "owner", "org": "orgId", "license": "licenseInfo"}[case]] = "wrong"
                    changed["item"] = (canonical(item), changed["item"][1])
                elif case == "z":
                    layer = json.loads(changed["layer_before"][0])
                    layer["hasZ"] = False
                    changed["layer_before"] = (canonical(layer), changed["layer_before"][1])
                else:
                    page = json.loads(changed["page_000"][0])
                    if case == "cross_page":
                        page["features"][-1] = json.loads(changed["page_001"][0])["features"][0]
                    elif case == "duplicate":
                        page["features"][-1] = page["features"][0]
                    else:
                        page["features"].pop()
                    changed["page_000"] = (canonical(page), changed["page_000"][1])
                with self.assertRaises(ValueError):
                    source.validate_captures(changed)


class NationalTransitMapAgenciesPipelineTests(unittest.TestCase):
    def test_real_capture_size_transitions_replay_and_immutable_refresh(self):
        from dagster_hifld.resources import StagingStorageResource

        real_client = httpx.Client
        for count in (1, 251, 1251):
            with self.subTest(count), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                captures, schema = fixture(range(1, count + 1))
                self.assertEqual(schema, source.EXPECTED_SCHEMA_SHA256)
                storage = StagingStorageResource(local_dir=str(root / "stage"))
                transport = fixture_transport(captures)
                with patch.object(source.httpx, "Client",
                                  side_effect=lambda **kwargs: real_client(transport=transport, **kwargs)):
                    first = source.capture_national_transit_map_agencies(
                        storage, version="fixture", capture_directory=str(root / "capture"))
                captured = next((root / "capture").iterdir())
                prefix = first["candidate_ready"].removesuffix("/metadata/candidate_ready.json")
                original = {key: storage.read_key(key) for key in storage.list_prefix(prefix)}
                self.assertEqual(first["feature_count"], count)
                self.assertEqual(first["source_reported_count"], count)
                self.assertTrue(first["source_population_complete"])
                self.assertEqual(first["producer_stated_compilation_date"], "2026-09-23")
                self.assertFalse(first["release_accepted"])
                data = storage.read_key(prefix + f"/geoparquet/{source.DATASET_SLUG}.parquet")
                table = pq.read_table(io.BytesIO(data))
                fields = json.loads(captures["layer_before"][0])["fields"]
                self.assertEqual(table.schema.names, [field["name"] for field in fields] + ["geometry"])
                expected = [row for name, (body, _) in captures.items() if name.startswith("page_")
                            for row in json.loads(body)["features"]]
                self.assertEqual(table.drop(["geometry"]).to_pylist(),
                                 [row["attributes"] for row in expected])
                geometry = shapely.from_wkb(table.column("geometry")[0].as_py())
                self.assertTrue(geometry.has_z)
                self.assertEqual(list(geometry.coords)[0], tuple(expected[0]["geometry"][axis]
                                                               for axis in ("x", "y", "z")))
                manifest = json.loads(storage.read_key(prefix + "/metadata/source_manifest.json"))
                self.assertIsNone(manifest["date_issued"])
                for input_record in manifest["inputs"]:
                    self.assertIsNone(input_record["release_date"])
                    self.assertIn("City of Lawndale", input_record["attribution"])
                    self.assertEqual(input_record["license"], source.LICENSE)
                with patch.object(real_client, "send", side_effect=AssertionError("offline HTTP")):
                    warm = source.capture_national_transit_map_agencies(
                        storage, version="warm", capture_directory=str(root / "warm"),
                        replay_capture_directory=str(captured))
                    cold = source.capture_national_transit_map_agencies(
                        StagingStorageResource(local_dir=str(root / "cold-stage")), version="cold",
                        capture_directory=str(root / "cold"), replay_capture_directory=str(captured))
                self.assertTrue(warm["reused"])
                self.assertEqual(cold["version"], first["version"])
                item = json.loads(captures["item"][0])
                item["description"] = "compiled on October 24, 2026"
                captures["item"] = (canonical(item), captures["item"][1])
                transport = fixture_transport(captures, mtime=1)
                with patch.object(source.httpx, "Client",
                                  side_effect=lambda **kwargs: real_client(transport=transport, **kwargs)):
                    fresh = source.capture_national_transit_map_agencies(
                        storage, version="fresh", capture_directory=str(root / "fresh"))
                self.assertTrue(fresh["reused"])
                self.assertEqual(fresh["version"], first["version"])
                self.assertEqual({key: storage.read_key(key) for key in storage.list_prefix(prefix)},
                                 original)
                refreshed = next((root / "fresh").iterdir())
                self.assertEqual(source._compilation_date(json.loads(
                    (refreshed / "item.body").read_bytes())), "2026-10-24")
                self.assertNotEqual((refreshed / "page_000.wire").read_bytes(),
                                    (captured / "page_000.wire").read_bytes())
                if count in (1, 251):
                    relative = ("metadata/candidate_ready.json" if count == 1 else
                                "raw/page_000.body")
                    staged = root / "stage" / prefix / relative
                    if count == 1:
                        staged.unlink()
                        expected_error = "cached_candidate_storage_roster_changed"
                    else:
                        staged.write_bytes(staged.read_bytes() + b"tampered")
                        expected_error = "cached_candidate_object_integrity_failed"
                    with patch.object(real_client, "send", side_effect=AssertionError("offline HTTP")):
                        with self.assertRaisesRegex(ValueError, expected_error):
                            source.capture_national_transit_map_agencies(
                                storage, version="corrupt", capture_directory=str(root / "corrupt"),
                                replay_capture_directory=str(captured))
                    if count == 1:
                        self.assertFalse(staged.exists())
                    else:
                        self.assertTrue(staged.read_bytes().endswith(b"tampered"))

    def test_failed_source_response_is_retained_and_quarantined(self):
        from dagster_hifld.resources import StagingStorageResource

        captures, _ = fixture()
        transport = fixture_transport(captures, fail="page_000")
        real_client = httpx.Client
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.object(source.httpx, "Client",
                              side_effect=lambda **kwargs: real_client(transport=transport, **kwargs)):
                with self.assertRaisesRegex(ValueError, "incomplete_source_capture"):
                    source.capture_national_transit_map_agencies(
                        StagingStorageResource(local_dir=str(root / "stage")), version="refused",
                        capture_directory=str(root / "capture"))
            self.assertTrue(list((root / "stage" / "_quarantine").rglob("failure.json")))
            self.assertFalse(list((root / "stage").rglob("candidate_ready.json")))
            self.assertTrue(list((root / "capture").rglob("page_000.receipt.json")))


class NationalTransitMapAgenciesRegistrationTests(unittest.TestCase):
    def test_registered_checks_job_and_stopped_schedule(self):
        from dagster_hifld.assets.national_transit_map_agencies import (
            bts_national_transit_map_agencies,
            bts_national_transit_map_agencies_candidate_complete,
            bts_national_transit_map_agencies_release_gate,
        )
        from dagster_hifld.definitions import defs

        repo = defs.get_repository_def()
        job = repo.get_job("bts_national_transit_map_agencies_draft_refresh")
        self.assertEqual(job.asset_layer.executable_asset_keys, bts_national_transit_map_agencies.keys)
        graph = defs.resolve_asset_graph()
        self.assertTrue(set(bts_national_transit_map_agencies_candidate_complete.check_keys) <= graph.asset_check_keys)
        self.assertTrue(set(bts_national_transit_map_agencies_release_gate.check_keys) <= graph.asset_check_keys)
        for check in (bts_national_transit_map_agencies_candidate_complete, bts_national_transit_map_agencies_release_gate):
            self.assertTrue(next(iter(check.check_specs)).blocking)
        schedule = repo.get_schedule_def("bts_national_transit_map_agencies_draft_refresh_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.cron_schedule, "0 8 1 * *")
        self.assertEqual(schedule.execution_timezone, "America/New_York")


if __name__ == "__main__":
    unittest.main()

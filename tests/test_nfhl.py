"""Offline contract tests for the FEMA NFHL family against a fake MapServer."""
from __future__ import annotations

import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.nfhl import LAYERS, LAYERS_BY_SLUG, CaptureSettings, capture_nfhl_layer
from dagster_hifld.sources.nfhl import candidate as nfhl_candidate
from dagster_hifld.sources.nfhl.capture import SERVICE_URL, EvidenceDirectory

CRS = {"wkid": 4269, "latestWkid": 4269}
GEOMETRY_TYPES = {"point": "esriGeometryPoint", "line": "esriGeometryPolyline", "polygon": "esriGeometryPolygon"}
SQUARE = [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]
FAR_SQUARE = [[5, 5], [5, 6], [6, 6], [6, 5], [5, 5]]
SHELL = [[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]]
HOLE = [[2, 2], [4, 2], [4, 4], [2, 4], [2, 2]]
BOWTIE = [[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]
OUTSIDE = [[-305, -125], [-305, -120], [-300, -120], [-305, -125]]
PATH_A = [[-86.07310378995749, 31.423340278536557], [-86.07258210005335, 31.423605648408852]]
PATH_B = [[-86.10, 31.50], [-86.09, 31.51], [-86.08, 31.52]]

# (Esri geometry served, expected candidate geometry) per OBJECTID; ``None`` expects null.
GEOMETRIES = {
    "point": {3: ({"x": -86.5, "y": 31.25}, Point(-86.5, 31.25)), 4: ({"x": "NaN", "y": "NaN"}, None),
              10: (None, None), 11: ({"x": 145.75, "y": 15.125}, Point(145.75, 15.125)),
              40: ({"x": -66.0, "y": 18.5}, Point(-66.0, 18.5))},
    "line": {3: ({"paths": [PATH_A]}, LineString(PATH_A)), 4: ({"paths": [PATH_A, PATH_B]},
                                                                MultiLineString([PATH_A, PATH_B])),
             10: (None, None), 40: ({"paths": [PATH_B]}, LineString(PATH_B))},
    "polygon": {3: ({"rings": [SQUARE]}, Polygon(SQUARE)),
                4: ({"rings": [SQUARE, FAR_SQUARE]}, MultiPolygon([Polygon(SQUARE), Polygon(FAR_SQUARE)])),
                10: ({"rings": [SHELL, HOLE]}, Polygon(SHELL, [HOLE])), 11: (None, None),
                12: ({"rings": [BOWTIE]}, Polygon(BOWTIE)), 40: ({"rings": []}, Polygon()),
                41: ({"rings": [OUTSIDE]}, Polygon(OUTSIDE)), 100: ({"rings": [FAR_SQUARE]}, Polygon(FAR_SQUARE))},
    "none": {oid: (None, None) for oid in (3, 4, 10, 11, 12, 40, 41, 100)},
}
FAMILY_SLUGS = {
    "nfhl-alluvial-fans", "nfhl-availability", "nfhl-base-flood-elevations", "nfhl-base-index",
    "nfhl-coastal-gages", "nfhl-coastal-transects", "nfhl-cross-sections", "nfhl-flood-hazard-boundaries",
    "nfhl-firm-panels", "nfhl-gages", "nfhl-general-structures", "nfhl-high-water-marks",
    "nfhl-hydrologic-reaches", "nfhl-levees", "nfhl-limit-moderate-wave-action", "nfhl-lomrs", "nfhl-nodes",
    "nfhl-plss", "nfhl-political-jurisdictions", "nfhl-primary-frontal-dunes", "nfhl-profile-baselines",
    "nfhl-river-mile-markers", "nfhl-seclusion-boundaries", "nfhl-station-start-points", "nfhl-study-info",
    "nfhl-subbasins", "nfhl-topographic-low-confidence-areas", "nfhl-transect-baselines", "nfhl-water-areas",
    "nfhl-water-lines",
}


class FakeMapServer:
    """In-process NFHL MapServer: service/layer documents, counts, statistics and OBJECTID windows."""

    def __init__(self, layer, *, oids=None, max_record_count=2000):
        self.layer = layer
        self.fields = [{"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID"}]
        for name in layer.id_fields:
            if name != "OBJECTID":
                self.fields.append({"name": name, "type": "esriFieldTypeGlobalID" if name == "GlobalID"
                                    else "esriFieldTypeString", "alias": name, "length": 38})
        self.fields += [{"name": "EFF_DATE", "type": "esriFieldTypeDate", "alias": "EFF_DATE", "length": 8},
                        {"name": "LEN", "type": "esriFieldTypeDouble", "alias": "LEN"},
                        {"name": "CODE", "type": "esriFieldTypeSmallInteger", "alias": "CODE"}]
        if not layer.is_table:
            self.fields.insert(3, {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE"})
        geometries = GEOMETRIES[layer.geometry]
        self.features = {oid: self.feature(oid, geometries[oid][0])
                         for oid in (geometries if oids is None else oids)}
        self.max_record_count = max_record_count
        self.refuse = lambda lo, hi: None
        # Z/M flags served in the layer document; defaults to the table entry's contract.
        self.zm_doc: tuple[bool, bool] | None = layer.source_zm
        self.hidden: set[int] = set()
        self.after_pages = None
        self.requests: list[str] = []
        self.page_requests: list[tuple[int, int]] = []
        self.palette = 0

    def feature(self, oid, geometry):
        attrs = {}
        for field in self.fields:
            name, kind = field["name"], field["type"]
            if kind == "esriFieldTypeGeometry":
                continue
            attrs[name] = (oid if kind == "esriFieldTypeOID" else 1700000000000 + oid if kind == "esriFieldTypeDate"
                           else oid * 1.5 if kind == "esriFieldTypeDouble" else oid % 7
                           if kind == "esriFieldTypeSmallInteger" else None if oid == 11 else f"{name}-{oid:04d}")
        feature = {"attributes": attrs}
        if not self.layer.is_table:
            feature["geometry"] = geometry
        return feature

    @property
    def attr_fields(self):
        return [f for f in self.fields if f["type"] != "esriFieldTypeGeometry"]

    def layer_doc(self):
        self.palette += 1
        doc = {"id": self.layer.layer_id, "name": self.layer.name,
               "type": "Table" if self.layer.is_table else "Feature Layer",
               "maxRecordCount": self.max_record_count, "fields": self.fields,
               "drawingInfo": {"renderer": {"type": "simple",
                                            "authoringInfo": {"colorRamp": {"color": [self.palette, 0, 0]}}}}}
        if not self.layer.is_table:
            doc.update(geometryType=GEOMETRY_TYPES[self.layer.geometry], sourceSpatialReference=CRS,
                       geometryField={"name": "SHAPE", "type": "esriFieldTypeGeometry"})
            if self.zm_doc is not None:
                doc.update(hasZ=self.zm_doc[0], hasM=self.zm_doc[1])
        return doc

    def service_doc(self):
        entry = {"id": self.layer.layer_id, "name": self.layer.name,
                 "type": "Table" if self.layer.is_table else "Feature Layer"}
        return {"mapName": "NFHL", "layers": [] if self.layer.is_table else [entry],
                "tables": [entry] if self.layer.is_table else []}

    def handle(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.requests.append(url)
        parts = urlsplit(url)
        query = {key: values[0] for key, values in parse_qs(parts.query).items()}
        if self.page_requests and self.after_pages is not None:
            self.after_pages(self)
            self.after_pages = None
        if parts.path == "/femaportal/resources/flood_map_svc.htm":
            html = f"<html>NFHL REST service: {SERVICE_URL}</html>".encode()
            return httpx.Response(200, stream=httpx.ByteStream(html))
        if parts.netloc == "www.arcgis.com":
            return self.json({"id": "ae38b6f94eaf4abf97f986fa01921e13", "owner": "FEMA_MAC",
                              "orgId": "XG15cJAlne2vxtgt", "type": "Map Service", "url": SERVICE_URL,
                              "name": "FEMA AGOL"} if "/items/" in parts.path
                             else {"id": "XG15cJAlne2vxtgt", "name": "FEMA AGOL"})
        if url.startswith(SERVICE_URL + "?"):
            return self.json(self.service_doc())
        if parts.path.endswith(f"/MapServer/{self.layer.layer_id}"):
            return self.json(self.layer_doc())
        oids = sorted(self.features)
        if query.get("returnCountOnly") == "true":
            return self.json({"count": len(oids)})
        if "outStatistics" in query:
            values = {"count_oid": len(oids), "min_oid": oids[0] if oids else None,
                      "max_oid": oids[-1] if oids else None}
            return self.json({"fields": [], "features": [{"attributes": values}]})
        lo, hi = map(int, re.fullmatch(r"OBJECTID>=(\d+) AND OBJECTID<=(\d+)", query["where"]).groups())
        self.page_requests.append((lo, hi))
        refusal = self.refuse(lo, hi)
        if refusal == "reset":
            raise httpx.ReadError("connection reset", request=request)
        if refusal == "error":
            return self.json({"error": {"code": 500, "message": "Error performing query operation"}})
        rows = [self.features[oid] for oid in oids if lo <= oid <= hi and oid not in self.hidden]
        page = {"displayFieldName": "", "fields": self.attr_fields, "features": rows[:self.max_record_count]}
        if not self.layer.is_table:
            page.update(geometryType=GEOMETRY_TYPES[self.layer.geometry], spatialReference=CRS)
        if len(rows) > self.max_record_count:
            page["exceededTransferLimit"] = True
        return self.json(page)

    @staticmethod
    def json(doc) -> httpx.Response:
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(doc).encode()),
                              headers={"content-type": "application/json"})


class NfhlCaptureCase(unittest.TestCase):
    verify_code_identity = False

    def setUp(self):
        self.temp = Path(tempfile.mkdtemp(prefix="nfhl-test-")).resolve()
        self.addCleanup(__import__("shutil").rmtree, self.temp, ignore_errors=True)
        self.storage = StagingStorageResource(use_local=True, local_dir=str(self.temp / "staging"), prefix="")
        if not self.verify_code_identity:
            # Git-blob code identity is validated by the shared construction tests and by
            # test_candidates_are_deterministic_and_reruns_reuse_the_sealed_candidate here;
            # it costs one git subprocess per source file, so other captures skip it.
            identity = patch.object(nfhl_candidate, "validate_construction_code_identity")
            identity.start()
            self.addCleanup(identity.stop)

    def run_capture(self, server, *, capture_directory=None, storage=None):
        settings = CaptureSettings(doc_delay=0, page_delay=0, transport=httpx.MockTransport(server.handle))
        return capture_nfhl_layer(storage or self.storage, server.layer, settings=settings,
                                  capture_directory=None if capture_directory is None else str(capture_directory))

    def staged(self, result, name, storage=None):
        storage = storage or self.storage
        return storage.read_key(f"{result['dataset_slug']}/{result['file_slug']}/{result['version']}/{name}")

    def parquet(self, result, layer, storage=None):
        folder = "unknown" if layer.is_table else "geoparquet"
        path = Path((storage or self.storage).local_dir) / result["dataset_slug"] / result["file_slug"] \
            / result["version"] / folder / f"{layer.slug}.parquet"
        return pq.read_table(path)

    def quarantines(self, layer):
        return [key for key in self.storage.list_prefix(f"_quarantine/{layer.slug}/")]


class CandidateTests(NfhlCaptureCase):
    def test_point_line_polygon_and_table_layers_stage_complete_candidates(self):
        for slug in ("nfhl-gages", "nfhl-levees", "nfhl-lomrs", "nfhl-study-info", "nfhl-seclusion-boundaries",
                     "nfhl-profile-baselines"):
            with self.subTest(slug=slug):
                layer = LAYERS_BY_SLUG[slug]
                server = FakeMapServer(layer, oids=[] if slug == "nfhl-seclusion-boundaries" else None,
                                       max_record_count=3)
                result = self.run_capture(server)
                self.assertFalse(result["reused"])
                self.assertEqual(result["feature_count"], len(server.features))
                self.assertEqual(result["source_count"], len(server.features))
                table = self.parquet(result, layer)
                self.assertEqual(table.column("OBJECTID").to_pylist(), sorted(server.features))
                self.assertEqual(table.schema.field("EFF_DATE").type, pa.int64())
                self.assertEqual(table.schema.field("CODE").type, pa.int16())
                for oid, row in zip(table.column("OBJECTID").to_pylist(), table.to_pylist()):
                    source = server.features[oid]["attributes"]
                    self.assertEqual({k: row[k] for k in source}, source)
                if layer.is_table:
                    self.assertNotIn(b"geo", table.schema.metadata or {})
                    continue
                geo = json.loads(table.schema.metadata[b"geo"])
                self.assertEqual(geo["primary_column"], "SHAPE")
                self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"], {"authority": "EPSG", "code": 4269})
                for oid, wkb in zip(table.column("OBJECTID").to_pylist(), table.column("SHAPE").to_pylist()):
                    expected = GEOMETRIES[layer.geometry][oid][1]
                    if expected is None:
                        self.assertIsNone(wkb)
                    else:
                        self.assertTrue(shapely.from_wkb(wkb).equals_exact(expected, 0), (slug, oid))
                ready = json.loads(self.staged(result, "metadata/candidate_ready.json"))
                self.assertFalse(ready["approved"])
                self.assertFalse(any(v for k, v in ready.items() if k.endswith("_accepted")))

    def test_problem_geometry_is_reported_not_rejected_or_repaired(self):
        polygon = self.run_capture(FakeMapServer(LAYERS_BY_SLUG["nfhl-lomrs"]))
        report = json.loads(self.staged(polygon, "metadata/source_audit.json"))["geometry"]
        self.assertEqual((report["null_geometry_count"], report["empty_geometry_count"],
                          report["invalid_geometry_count"], report["out_of_domain_geometry_count"]), (1, 1, 1, 1))
        self.assertEqual(report["sample_object_ids"]["invalid_geometry"], [12])
        self.assertEqual(report["sample_object_ids"]["out_of_domain_geometry"], [41])
        point = self.run_capture(FakeMapServer(LAYERS_BY_SLUG["nfhl-gages"]))
        self.assertEqual((point["unconvertible_geometry_count"], point["null_geometry_count"]), (1, 1))


class ZmContractTests(NfhlCaptureCase):
    """The producer's Z/M declaration is accepted only as the entry declares it,
    and served Z/M vertices always fail closed because GDAL decodes them."""

    def test_zm_declaring_document_without_entry_contract_fails(self):
        server = FakeMapServer(LAYERS_BY_SLUG["nfhl-levees"])
        server.zm_doc = (True, True)
        with self.assertRaisesRegex(ValueError, "source_z_or_m_geometry_unsupported"):
            self.run_capture(server)
        failures = [k for k in self.quarantines(LAYERS_BY_SLUG["nfhl-levees"]) if k.endswith("failure.json")]
        self.assertEqual(json.loads(self.storage.read_key(failures[0]))["reason_code"],
                         "source_z_or_m_geometry_unsupported")

    def test_zm_declaration_mismatch_fails(self):
        for doc_zm in ((False, False), (True, False), (False, True)):
            with self.subTest(doc_zm=doc_zm):
                server = FakeMapServer(LAYERS_BY_SLUG["nfhl-profile-baselines"])
                server.zm_doc = doc_zm
                with self.assertRaisesRegex(ValueError, "nfhl_layer_zm_declaration_drift"):
                    self.run_capture(server)

    def test_declared_zm_layer_records_the_producer_declaration(self):
        layer = LAYERS_BY_SLUG["nfhl-profile-baselines"]
        result = self.run_capture(FakeMapServer(layer, max_record_count=3))
        audit = json.loads(self.staged(result, "metadata/source_audit.json"))
        self.assertEqual(audit["source_declared_zm"], {"hasZ": True, "hasM": True})
        geo = json.loads(self.parquet(result, layer).schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["SHAPE"]["geometry_types"], ["LineString"])

    def test_served_zm_vertices_fail_closed_and_quarantine(self):
        layer = LAYERS_BY_SLUG["nfhl-levees"]
        server = FakeMapServer(layer, max_record_count=3)
        zm_paths = [[[-86.0, 31.0, 1.5, 2.5], [-86.1, 31.1, 3.5, 4.5]]]
        for oid, geometry in server.features.items():
            if geometry.get("geometry") is not None:
                geometry["geometry"]["paths"] = zm_paths
        with self.assertRaisesRegex(ValueError, "source_z_or_m_geometry_unsupported"):
            self.run_capture(server)
        failures = [k for k in self.quarantines(layer) if k.endswith("failure.json")]
        self.assertEqual(json.loads(self.storage.read_key(failures[0]))["reason_code"],
                         "source_z_or_m_geometry_unsupported")
        keys = self.quarantines(layer)
        self.assertTrue(any("/raw/page_" in k for k in keys))


class InventoryAliasTests(NfhlCaptureCase):
    def test_consolidated_layer_names_every_served_inventory_entry(self):
        layer = LAYERS_BY_SLUG["nfhl-flood-hazard-boundaries"]
        result = self.run_capture(FakeMapServer(layer, max_record_count=3))
        manifest = json.loads(self.staged(result, "metadata/source_manifest.json"))
        self.assertIn("nfhl/flood-hazard-boundaries", manifest["lineage"])
        self.assertIn("nfhl/national-flood-hazard-layer-line-nfhl-1", manifest["lineage"])
        self.assertIn("240863", manifest["lineage"])

class SealedCandidateTests(NfhlCaptureCase):
    verify_code_identity = True

    def test_candidates_are_deterministic_and_reruns_reuse_the_sealed_candidate(self):
        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        layer = LAYERS_BY_SLUG["nfhl-levees"]
        first = self.run_capture(FakeMapServer(layer, max_record_count=2))
        other = StagingStorageResource(use_local=True, local_dir=str(self.temp / "other"), prefix="")
        second = self.run_capture(FakeMapServer(layer, max_record_count=2), storage=other)
        self.assertEqual(first["version"], second["version"])
        self.assertEqual(self.staged(first, "geoparquet/nfhl-levees.parquet"),
                         self.staged(second, "geoparquet/nfhl-levees.parquet", other))
        version = first["version"]
        snapshot_source_metadata(self.storage, layer.slug, layer.slug, version)
        manifest = load_resolved_source_manifest(self.storage, layer.slug, layer.slug, version).metadata
        summary = summarize_staged_catalog(self.storage, layer.slug, layer.slug, version, layer.slug,
                                           source_metadata=manifest)
        write_catalog_metadata(self.storage, dataset_slug=layer.slug, file_slug=layer.slug, version=version,
                               quality_dict=summary.quality_manifest, dictionary_dict=summary.data_dictionary)
        reused = self.run_capture(FakeMapServer(layer))
        self.assertTrue(reused["reused"])
        self.assertEqual(reused["version"], version)


class CompletenessTests(NfhlCaptureCase):
    layer = LAYERS_BY_SLUG["nfhl-lomrs"]

    def assert_quarantined(self, reason):
        keys = self.quarantines(self.layer)
        failures = [k for k in keys if k.endswith("metadata/failure.json")]
        self.assertEqual(len(failures), 1)
        self.assertEqual(json.loads(self.storage.read_key(failures[0]))["reason_code"], reason)
        self.assertTrue(any("/raw/count_before." in k for k in keys))
        self.assertEqual(self.storage.list_prefix(f"{self.layer.slug}/"), [])

    def test_count_change_between_witnesses_fails_and_quarantines_evidence(self):
        server = FakeMapServer(self.layer, max_record_count=3)
        server.after_pages = lambda s: s.features.update({500: s.feature(500, {"rings": [SQUARE]})})
        with self.assertRaisesRegex(ValueError, "source_count_changed_during_capture"):
            self.run_capture(server)
        self.assert_quarantined("source_count_changed_during_capture")

    def test_captured_ids_must_equal_source_count(self):
        server = FakeMapServer(self.layer)
        server.hidden = {10}
        with self.assertRaisesRegex(ValueError, "source_capture_incomplete"):
            self.run_capture(server)
        self.assert_quarantined("source_capture_incomplete")

    def test_layer_metadata_change_during_capture_fails(self):
        server = FakeMapServer(self.layer)
        server.after_pages = lambda s: s.fields[1].update(length=99)
        with self.assertRaisesRegex(ValueError, "source_layer_metadata_changed_during_capture"):
            self.run_capture(server)

    def test_record_schema_drift_fails(self):
        server = FakeMapServer(self.layer)
        server.features[10]["attributes"]["EXTRA"] = 1
        with self.assertRaisesRegex(ValueError, "source_record_attribute_schema_drift"):
            self.run_capture(server)
        self.assert_quarantined("source_record_attribute_schema_drift")

    def test_missing_layer_in_service_roster_fails(self):
        server = FakeMapServer(self.layer)
        server.service_doc = lambda: {"mapName": "NFHL", "layers": [], "tables": []}
        with self.assertRaisesRegex(ValueError, "nfhl_service_roster_missing_layer"):
            self.run_capture(server)


class WindowTests(NfhlCaptureCase):
    layer = LAYERS_BY_SLUG["nfhl-lomrs"]

    def manifest(self, captures):
        return json.loads((captures / self.layer.slug / "capture_manifest.json").read_bytes())

    def test_refused_and_reset_windows_bisect_until_served(self):
        for mode in ("error", "reset"):
            with self.subTest(mode=mode):
                server = FakeMapServer(self.layer)
                server.refuse = lambda lo, hi, mode=mode: mode if hi - lo + 1 > 8 else None
                captures = self.temp / f"captures-{mode}"
                result = self.run_capture(server, capture_directory=captures)
                self.assertEqual(result["feature_count"], len(server.features))
                manifest = self.manifest(captures)
                self.assertTrue(manifest["set_aside"])
                accepted = [n for n in manifest["accepted"] if n.startswith("page_")]
                self.assertTrue(all(int(n[16:]) - int(n[5:15]) + 1 <= 8 for n in accepted))
                label = "refused" if mode == "error" else "incomplete"
                self.assertTrue(list((captures / self.layer.slug).glob(f"page_*.{label}.receipt.json")))
                self.assertTrue(list((captures / self.layer.slug).glob("page_*.attempt-01.receipt.json")))

    def test_truncated_window_bisects(self):
        server = FakeMapServer(self.layer, max_record_count=2)
        captures = self.temp / "captures"
        result = self.run_capture(server, capture_directory=captures)
        self.assertEqual(result["feature_count"], len(server.features))
        self.assertTrue(any(name.endswith(".truncated") for name in self.manifest(captures)["set_aside"]))

    def test_single_object_window_refused_fails_and_quarantines(self):
        server = FakeMapServer(self.layer)
        server.refuse = lambda lo, hi: "error" if lo <= 41 <= hi else None
        with self.assertRaisesRegex(ValueError, "source_window_unservable"):
            self.run_capture(server)
        failures = [k for k in self.quarantines(self.layer) if k.endswith("failure.json")]
        self.assertEqual(json.loads(self.storage.read_key(failures[0]))["reason_code"], "source_window_unservable")

    def test_capture_directory_is_exclusive(self):
        evidence = EvidenceDirectory(self.temp / "captures" / self.layer.slug)
        self.addCleanup(evidence.close)
        with self.assertRaisesRegex(ValueError, "capture_directory_in_use"):
            self.run_capture(FakeMapServer(self.layer), capture_directory=self.temp / "captures")


class ResumeTests(NfhlCaptureCase):
    layer = LAYERS_BY_SLUG["nfhl-lomrs"]

    def setUp(self):
        super().setUp()
        self.captures = self.temp / "captures"

    def test_rerun_reuses_verified_pages_after_failure(self):
        server = FakeMapServer(self.layer, max_record_count=3)
        server.refuse = lambda lo, hi: "error" if lo <= 100 <= hi else None
        with self.assertRaisesRegex(ValueError, "source_window_unservable"):
            self.run_capture(server, capture_directory=self.captures)
        failed = json.loads((self.captures / self.layer.slug / "capture_manifest.json").read_bytes())
        served = {(int(n[5:15]), int(n[16:])) for n in failed["accepted"] if n.startswith("page_")}
        self.assertTrue(served)
        server.refuse, server.page_requests = (lambda lo, hi: None), []
        result = self.run_capture(server, capture_directory=self.captures)
        self.assertEqual(result["feature_count"], len(server.features))
        self.assertEqual(result["reused_page_count"], len(served))
        self.assertFalse(served & set(server.page_requests))
        self.assertEqual(result["page_count"], len(served) + len(server.page_requests))

    def test_mismatched_page_evidence_is_retired_not_reused(self):
        server = FakeMapServer(self.layer, max_record_count=3)
        first = self.run_capture(server, capture_directory=self.captures)
        root = self.captures / self.layer.slug
        body = min(root.glob("page_*.body"))
        body.write_bytes(body.read_bytes().replace(b"LOMR_ID", b"LOMR_XX", 1))
        server.page_requests = []
        second = self.run_capture(server, capture_directory=self.captures)
        self.assertEqual(second["version"], first["version"])
        self.assertEqual(second["reused_page_count"], first["page_count"] - 1)
        self.assertEqual(len(server.page_requests), 1)
        retired = list((root / "previous").glob(f"*/{body.name}"))
        self.assertEqual(len(retired), 1)
        self.assertIn(b"LOMR_XX", retired[0].read_bytes())

    def test_changed_producer_witness_retires_all_pages(self):
        server = FakeMapServer(self.layer, max_record_count=3)
        first = self.run_capture(server, capture_directory=self.captures)
        server.features[500] = server.feature(500, {"rings": [SQUARE]})
        second = self.run_capture(server, capture_directory=self.captures)
        self.assertEqual(second["reused_page_count"], 0)
        self.assertEqual(second["feature_count"], first["feature_count"] + 1)
        self.assertTrue(list((self.captures / self.layer.slug / "previous").glob("*/page_*.body")))


class RegistrationTests(unittest.TestCase):
    def test_remaining_layer_entries_match_the_producer_roster(self):
        profile = LAYERS_BY_SLUG["nfhl-profile-baselines"]
        self.assertEqual((profile.layer_id, profile.name, profile.geometry), (17, "Profile Baselines", "line"))
        self.assertEqual(profile.source_zm, (True, True))
        self.assertEqual(LAYERS_BY_SLUG["nfhl-water-lines"].layer_id, 20)
        boundaries = LAYERS_BY_SLUG["nfhl-flood-hazard-boundaries"]
        self.assertEqual((boundaries.layer_id, boundaries.name, boundaries.geometry),
                         (27, "Flood Hazard Boundaries", "line"))
        self.assertEqual(boundaries.inventory_aliases,
                         (("nfhl/national-flood-hazard-layer-line-nfhl-1", 240863),))

    def test_every_layer_keeps_its_asset_key_checks_job_and_stopped_schedule(self):
        from dagster import AssetKey, DefaultScheduleStatus

        from dagster_hifld.assets import nfhl as nfhl_assets
        from dagster_hifld.definitions import defs

        self.assertEqual({layer.slug for layer in LAYERS}, FAMILY_SLUGS)
        graph = defs.resolve_asset_graph()
        keys = set(graph.get_all_asset_keys())
        checks = {(c.asset_key, c.name) for c in graph.asset_check_keys}
        for layer in LAYERS:
            key = AssetKey(["ingest", layer.slug, layer.slug])
            self.assertIn(key, keys)
            self.assertIn((key, f"fema_{layer.stem}_complete"), checks)
            self.assertIn((key, f"fema_{layer.stem}_count_drift"), checks)
            self.assertEqual(defs.get_schedule_def(f"{layer.stem}_draft_refresh_schedule").default_status,
                             DefaultScheduleStatus.STOPPED)
            defs.get_job_def(f"{layer.stem}_draft_refresh")
        self.assertEqual(len(nfhl_assets.INGEST_ASSETS), len(LAYERS))

    def test_asset_materializes_with_blocking_completeness_and_warn_drift_checks(self):
        from dagster import AssetCheckSeverity, DagsterInstance, materialize

        from dagster_hifld.assets import nfhl as nfhl_assets

        layer = LAYERS_BY_SLUG["nfhl-gages"]
        index = [a.key.path[1] for a in nfhl_assets.INGEST_ASSETS].index(layer.slug)
        selection = [nfhl_assets.INGEST_ASSETS[index], *nfhl_assets.ASSET_CHECKS[2 * index:2 * index + 2]]
        server = FakeMapServer(layer)
        settings = CaptureSettings(doc_delay=0, page_delay=0, transport=httpx.MockTransport(server.handle))
        with tempfile.TemporaryDirectory() as temp, DagsterInstance.ephemeral() as instance, \
                patch.object(nfhl_candidate, "DEFAULT_SETTINGS", settings), \
                patch.object(nfhl_candidate, "validate_construction_code_identity"):
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(temp).resolve()), prefix="")
            evaluations = []
            for _ in range(2):
                result = materialize(selection, resources={"staging_storage": storage}, instance=instance)
                self.assertTrue(result.success)
                evaluations.append({e.check_name: e for e in result.get_asset_check_evaluations()})
                server.features[500] = server.feature(500, {"x": 1.0, "y": 2.0})
            for run in evaluations:
                self.assertTrue(run[f"fema_{layer.stem}_complete"].passed)
            self.assertTrue(evaluations[0][f"fema_{layer.stem}_count_drift"].passed)
            drift = evaluations[1][f"fema_{layer.stem}_count_drift"]
            self.assertFalse(drift.passed)
            self.assertEqual(drift.severity, AssetCheckSeverity.WARN)


if __name__ == "__main__":
    unittest.main()

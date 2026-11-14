"""Focused tests for the PAD-US Protection Mechanism Category draft pipeline."""
import hashlib
import io
import json
import struct
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import httpx
import pyarrow.parquet as pq
import shapely

from dagster_hifld.assets.pad_us_protection_mechanism_category import (
    pad_us_protection_mechanism_category, pad_us_protection_mechanism_category_complete,
    pad_us_protection_mechanism_category_complete_contract)
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_frs_common
from dagster_hifld.sources import pad_us_protection_mechanism_category as src

FIXTURE_COUNT = 5
FIXTURE_OBJECTIDS = (1, 2, 3, 5, 8)
# Clockwise closed exteriors and a counter-clockwise closed hole, verbatim
# Esri JSON polygon rings with two-ordinate vertices (the layer has no Z/M).
RING_A = [[-122.69, 42.20], [-122.68, 42.20], [-122.68, 42.19], [-122.69, 42.19],
          [-122.69, 42.20]]
HOLE_A = [[-122.687, 42.196], [-122.687, 42.195], [-122.684, 42.195],
          [-122.684, 42.196], [-122.687, 42.196]]
RING_B = [[-89.96, 31.90], [-89.95, 31.90], [-89.95, 31.89], [-89.96, 31.89],
          [-89.96, 31.90]]


def _fixture_attrs(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    return {
        "OBJECTID": oid,
        "FeatClass": ["Fee", "Designation", "Easement", "Marine", "Proclamation"][index],
        "Category": ["Conservation", "Conservation", "Conservation", "Conservation",
                     "Proclamation"][index],
        "Own_Type": ["Federal", "Federal", "State", "Local-Government", "Federal"][index],
        "Own_Name": "U.S. Forest Service" if index == 0 else "Fixture Owner",
        "Loc_Own": None,
        "Mang_Type": "Federal" if index != 3 else "State",
        "Mang_Name": "U.S. Forest Service" if index != 3 else "Fixture Manager",
        "Loc_Mang": None,
        "Des_Tp": "Declared Wilderness" if index == 1 else None,
        "Loc_Ds": None,
        "Unit_Nm": "Fixture Unit",
        "Loc_Nm": None,
        "State_Nm": "Oregon" if index < 2 else "Fixture State",
        "Agg_Src": "USGS GAP",
        "GIS_Src": "Fixture Source",
        "Src_Date": "2024" if index != 2 else "2023",
        "GIS_Acres": 123 + index,
        "Source_PAID": f"FIXTURE-{oid}",
        "WDPA_Cd": 5555 + index if index != 1 else None,
        "Pub_Access": "Open Access" if index != 4 else "Unknown",
        "Access_Src": None,
        "Access_Dt": "2024" if index != 4 else None,
        "GAP_Sts": "1" if index == 0 else "2",
        "GAPCdSrc": "Fixture GAP Source",
        "GAPCdDt": "2024",
        "IUCN_Cat": "Ib" if index == 0 else "V",
        "IUCNCtSrc": "Fixture IUCN Source",
        "IUCNCtDt": "2024",
        "Date_Est": "1908",
        "Comments": None,
        "EsmtHldr": "Fixture Holder" if index == 2 else None,
        "EHoldTyp": "NGO" if index == 2 else None,
        "WebMercAc": 456 + index,
        "Shape__Area": 9564422.8 + index,
        "Shape__Length": 61.22714663080581 + index,
        "GlobalID": f"{{00000000-0000-0000-0000-{oid:012d}}}",
    }


def _fixture_geometry(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    if oid == FIXTURE_OBJECTIDS[2]:
        return {"rings": [RING_A, HOLE_A]}
    if oid == FIXTURE_OBJECTIDS[4]:
        return {"rings": [RING_A, RING_B]}
    return {"rings": [RING_A] if index % 2 == 0 else [RING_B]}


def _fixture_rows(count: int = FIXTURE_COUNT) -> list[dict]:
    return [{"attributes": _fixture_attrs(oid),
             "geometry": _fixture_geometry(oid)} for oid in FIXTURE_OBJECTIDS[:count]]


def _fixture_layer_fields() -> list[dict]:
    fields = []
    for name, esri_type, nullable, length in src.NATIVE_FIELDS:
        field = {"name": name, "alias": name if name != "Shape__Length" else "shape_Length",
                 "type": esri_type, "nullable": nullable, "domain": None}
        if length is not None:
            field["length"] = length
        fields.append(field)
    return fields


def _fixture_layer_doc(fields: list[dict]) -> dict:
    return {"id": src.LAYER_ID, "name": src.LAYER_NAME, "type": "Feature Layer",
            "geometryType": src.GEOMETRY_TYPE, "hasZ": False, "hasM": False,
            "maxRecordCount": 2000, "objectIdField": "OBJECTID", "globalIdField": "GlobalID",
            "advancedQueryCapabilities": {"supportsPagination": True},
            "spatialReference": dict(src.NATIVE_LAYER_SR),
            "extent": {**src.EXPECTED_LAYER_EXTENT,
                       "spatialReference": dict(src.NATIVE_LAYER_SR)},
            "fields": fields}


def _fixture_service_doc() -> dict:
    return {"copyrightText": src.EXPECTED_COPYRIGHT_TEXT,
            "serviceDescription": src.EXPECTED_SERVICE_DESCRIPTION,
            "spatialReference": {"wkid": 102100, "latestWkid": 3857},
            "layers": [{"id": layer_id, "name": name}
                       for layer_id, name in src.EXPECTED_SERVICE_LAYERS]}


def _fixture_item_doc(item_id: str, owner: str) -> dict:
    return {"id": item_id, "owner": owner, "orgId": "v01gqwM5QqNysAAi",
            "type": "Feature Service", "title": "PAD-US Fixture"}


def _ids_roster(rows: list[dict]) -> list[int]:
    return sorted(row["attributes"]["OBJECTID"] for row in rows
                  if isinstance(row["attributes"]["OBJECTID"], int))


def _ids_sha(rows: list[dict]) -> str:
    return hashlib.sha256(json.dumps(_ids_roster(rows), sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


class JSONStream(httpx.SyncByteStream):
    def __init__(self, payload):
        if isinstance(payload, bytes):
            self.body = payload
        else:
            self.body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()

    def __iter__(self):
        yield self.body

    def close(self):
        return None


def make_responder(rows: list[dict], *, layer_fields=None, count=None, service=None,
                   layer=None, geometry=None, doc_page=None, item_owner=None,
                   data_rows=None, page_has_z: bool | None = False) -> httpx.MockTransport:
    count = count if count is not None else len(rows)
    layer_fields = layer_fields if layer_fields is not None else _fixture_layer_fields()
    data_rows = data_rows if data_rows is not None else rows

    def layer_doc():
        if callable(layer):
            return layer()
        if layer is not None:
            return layer
        return _fixture_layer_doc(layer_fields)

    service_doc = service if service is not None else _fixture_service_doc()

    def respond(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path
        query = parse_qs(urlparse(str(request.url)).query)
        if path.startswith("/programs/gap-analysis-project/"):
            return httpx.Response(200, stream=JSONStream(doc_page or
                b"<html>PAD-US Web Services: "
                + src.SERVICE_URL.encode() + b" item " + src.VIEW_ITEM_ID.encode()
                + b"</html>"),
                headers={"content-type": "text/html"})
        if path.startswith("/sharing/rest/content/items/"):
            item_id = path.rsplit("/", 1)[-1]
            owner = (item_owner if item_owner is not None and item_id == src.VIEW_ITEM_ID
                     else "mcroft@usgs.gov_USGS" if item_id == src.VIEW_ITEM_ID
                     else "sboyer@usgs.gov_USGS")
            return httpx.Response(200, stream=JSONStream(_fixture_item_doc(item_id, owner)),
                                  headers={"content-type": "application/json"})
        if path.rstrip("/").endswith("FeatureServer"):
            return httpx.Response(200, stream=JSONStream(service_doc),
                                  headers={"content-type": "application/json"})
        if path.rstrip("/").endswith(f"/{src.LAYER_ID}"):
            return httpx.Response(200, stream=JSONStream(layer_doc()),
                                  headers={"content-type": "application/json"})
        if query.get("returnCountOnly") == ["true"]:
            return httpx.Response(200, stream=JSONStream({"count": count}),
                                  headers={"content-type": "application/json"})
        if query.get("returnIdsOnly") == ["true"]:
            return httpx.Response(200, stream=JSONStream(
                {"objectIdFieldName": "OBJECTID", "objectIds": _ids_roster(rows)}),
                headers={"content-type": "application/json"})
        if query.get("where") == ["1=1"]:
            return httpx.Response(200, stream=JSONStream({"count": count}),
                                  headers={"content-type": "application/json"})
        last = int(query["where"][0].split(">")[1])
        page_rows = [row for row in data_rows
                     if (isinstance(row["attributes"]["OBJECTID"], int)
                         and row["attributes"]["OBJECTID"] > last)
                     or not isinstance(row["attributes"]["OBJECTID"], int)][:src.PAGE_SIZE]
        if geometry is not None:
            page_rows = [dict(row, geometry=geometry(row)) for row in page_rows]
        more = False
        if page_rows:
            page_oids = [row["attributes"]["OBJECTID"] for row in page_rows
                         if isinstance(row["attributes"]["OBJECTID"], int)]
            tail_oids = [row["attributes"]["OBJECTID"] for row in data_rows
                         if isinstance(row["attributes"]["OBJECTID"], int)]
            more = bool(page_oids) and max(page_oids) < max(tail_oids)
        page_doc = {"objectIdFieldName": "OBJECTID",
                    "spatialReference": {"wkid": src.SOURCE_CRS_WKID,
                                         "latestWkid": src.SOURCE_CRS_WKID},
                    "features": page_rows, "exceededTransferLimit": more}
        if page_has_z is not None:
            page_doc["hasZ"] = page_has_z
            page_doc["hasM"] = False
        return httpx.Response(200, stream=JSONStream(page_doc),
                              headers={"content-type": "application/json"})

    return httpx.MockTransport(respond)


@contextmanager
def fixture_mode(schema_sha: str, transport: httpx.MockTransport, *, page_size=None,
                 real_identity: bool = False):
    import dagster_hifld.assets.pad_us_protection_mechanism_category as asset_module
    contexts = [
        patch.object(src, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(src, "EXPECTED_RECORD_SHA256", ""),
        patch.object(src, "EXPECTED_GEOMETRY_SHA256", ""),
        patch.object(src, "EXPECTED_IDS_SHA256", _FIXTURE_IDS_SHA),
        patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", 0),
        patch.object(src, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS", {}),
        patch.object(asset_module, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(asset_module, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS", {}),
        patch.object(src, "_default_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_doc_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        # CI runners hold far less than the pinned capture floor, so the floor
        # gate is evaluated against a stub that reports exactly the pinned
        # requirement; the refusal itself is exercised by the dedicated floor
        # test below.
        patch.object(src.shutil, "disk_usage",
                     return_value=SimpleNamespace(free=src.MIN_FREE_BYTES)),
    ]
    if not real_identity:
        contexts.extend([
            patch.object(src, "_code_identity",
                         return_value={"code_identity_scheme": "test", "code_commit": "synthetic",
                                       "files": [], "working_tree_status": "unknown"}),
            patch.object(src, "validate_construction_code_identity"),
            patch.object(epa_frs_common, "validate_construction_code_identity"),
        ])
    if page_size is not None:
        contexts.append(patch.object(src, "PAGE_SIZE", page_size))
    for context in contexts:
        context.start()
    try:
        yield
    finally:
        for context in reversed(contexts):
            context.stop()


_FIXTURE_IDS_SHA = hashlib.sha256(json.dumps(sorted(FIXTURE_OBJECTIDS), sort_keys=True,
                                            separators=(",", ":")).encode()).hexdigest()


def _staging(root: Path) -> StagingStorageResource:
    return StagingStorageResource(use_local=True, local_dir=str(root / "staging"),
                                  prefix="", backend="local")


class PadUsProtectionMechanismCategoryTests(unittest.TestCase):
    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "pad-us-protection-mechanism-category")
        self.assertEqual(
            src.SERVICE_URL,
            "https://services.arcgis.com/v01gqwM5QqNysAAi/ArcGIS/rest/services"
            "/Protection_Mechanism_Category/FeatureServer")
        self.assertEqual(src.LAYER_ID, 0)
        self.assertEqual(src.LAYER_NAME,
                         "Protected Areas Database of the United States (PAD-US) v3.0")
        self.assertEqual(src.GEOMETRY_TYPE, "esriGeometryPolygon")
        self.assertEqual(src.EXPECTED_COUNT, 441_609)
        self.assertEqual(src.PAGE_SIZE, 1000)
        self.assertEqual(len(src.NATIVE_FIELDS), 37)
        self.assertEqual(src.MIN_FREE_BYTES, 25_000_000_000)
        self.assertEqual(src.ACCEPT_ENCODING, "gzip")
        self.assertNotEqual(src.EXPECTED_RECORD_SHA256, "")
        self.assertNotEqual(src.EXPECTED_GEOMETRY_SHA256, "")
        self.assertNotEqual(src.EXPECTED_IDS_SHA256, "")
        self.assertEqual(src.EXPECTED_UNUSABLE_GEOMETRY_COUNTS,
                         {"source_polygon_ring_degenerate": 302,
                          "source_polygon_hole_unassigned": 40})
        self.assertTrue(src.BASELINE_PENDING_NOTE)
        keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertTrue(any(key.path == ["ingest", src.DATASET_SLUG, src.DATASET_SLUG]
                            for key in keys))

    def test_blocking_completeness_check_gates_the_pinned_null_contract(self):
        # Every producer null geometry in the complete capture is an
        # unusable-ring gate, so the blocking check must accept exactly the
        # pinned gated-null contract and reject any drift from it.
        contract = (src.EXPECTED_NULL_GEOMETRY_COUNT
                    + sum(src.EXPECTED_UNUSABLE_GEOMETRY_COUNTS.values()))
        self.assertEqual(contract, 342)
        self.assertTrue(pad_us_protection_mechanism_category_complete_contract(
            {"feature_count": src.EXPECTED_COUNT, "source_null_geometry_count": contract}))
        self.assertFalse(pad_us_protection_mechanism_category_complete_contract(
            {"feature_count": src.EXPECTED_COUNT, "source_null_geometry_count": contract + 1}))
        self.assertFalse(pad_us_protection_mechanism_category_complete_contract(
            {"feature_count": src.EXPECTED_COUNT - 1, "source_null_geometry_count": contract}))

    def test_keyset_pagination_contract_is_ordered_and_bounded(self):
        url = src._page_url(15000000)
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["where"], ["OBJECTID>15000000"])
        self.assertEqual(query["orderByFields"], ["OBJECTID ASC"])
        self.assertEqual(query["resultRecordCount"], [str(src.PAGE_SIZE)])
        self.assertEqual(query["outSR"], [str(src.SOURCE_CRS_WKID)])
        self.assertEqual(query["returnGeometry"], ["true"])
        self.assertNotIn("returnZ", query)
        self.assertNotIn("returnM", query)
        self.assertLess(len(url), 400)

    def test_polygon_wkb_keeps_ring_order_and_hole_assignment(self):
        wkb, unusable = src._polygon_wkb(_fixture_geometry(FIXTURE_OBJECTIDS[2]))
        self.assertIsNone(unusable)
        order, kind, ring_count = struct.unpack_from("<BII", wkb, 0)
        self.assertEqual((order, kind, ring_count), (1, 3, 2))
        offset = 9
        for ring in (RING_A, HOLE_A):
            (vertices,) = struct.unpack_from("<I", wkb, offset)
            self.assertEqual(vertices, len(ring))
            offset += 4
            for vertex in ring:
                self.assertEqual(struct.unpack_from("<2d", wkb, offset), tuple(vertex))
                offset += 16
        self.assertEqual(offset, len(wkb))
        single, unusable = src._polygon_wkb(_fixture_geometry(FIXTURE_OBJECTIDS[0]))
        self.assertIsNone(unusable)
        self.assertEqual(struct.unpack_from("<BI", single, 0), (1, 3))
        multi, unusable = src._polygon_wkb(_fixture_geometry(FIXTURE_OBJECTIDS[4]))
        self.assertIsNone(unusable)
        self.assertEqual(struct.unpack_from("<BII", multi, 0), (1, 6, 2))

    def test_polygon_wkb_round_trips_through_independent_readers(self):
        """WKB counts are uint32; independent decoders must read every ordinate.

        Writing vertex, ring or polygon counts as uint64 makes every
        independent reader (shapely/GEOS, GDAL) decode garbage coordinates or
        the wrong geometry, even when a byte-layout test repeats the same
        wrong layout.
        """

        def _read_rings(wkb: bytes, offset: int, ring_count: int):
            rings = []
            for _ring in range(ring_count):
                (vertices,) = struct.unpack_from("<I", wkb, offset)
                offset += 4
                rings.append([struct.unpack_from("<2d", wkb, offset + 16 * index)
                              for index in range(vertices)])
                offset += 16 * vertices
            return rings, offset

        def independent_decode(wkb: bytes):
            order, kind = struct.unpack_from("<BI", wkb, 0)
            self.assertEqual(order, 1)
            self.assertIn(kind, (3, 6))
            if kind == 3:
                (ring_count,) = struct.unpack_from("<I", wkb, 5)
                rings, offset = _read_rings(wkb, 9, ring_count)
                self.assertEqual(offset, len(wkb))
                return [rings]
            (polys,) = struct.unpack_from("<I", wkb, 5)
            offset = 9
            decoded = []
            for _poly in range(polys):
                part_order, part_kind, ring_count = struct.unpack_from("<BII", wkb, offset)
                self.assertEqual((part_order, part_kind), (1, 3))
                offset += 9
                rings, offset = _read_rings(wkb, offset, ring_count)
                decoded.append(rings)
            self.assertEqual(offset, len(wkb))
            return decoded

        expected_by_oid = {}
        for oid in FIXTURE_OBJECTIDS:
            geometry = _fixture_geometry(oid)
            expected = [[tuple(vertex) for vertex in ring] for ring in geometry["rings"]]
            expected_by_oid[oid] = expected
            wkb, unusable = src._polygon_wkb(geometry)
            self.assertIsNone(unusable)
            decoded = independent_decode(wkb)
            if oid == FIXTURE_OBJECTIDS[4]:
                self.assertEqual(decoded, [[[tuple(vertex) for vertex in ring]]
                                           for ring in geometry["rings"]])
            else:
                self.assertEqual(decoded, [expected])
            shapely_geometry = shapely.from_wkb(wkb)
            if oid == FIXTURE_OBJECTIDS[4]:
                self.assertIsInstance(shapely_geometry, shapely.MultiPolygon)
                self.assertEqual(shapely.get_num_geometries(shapely_geometry), 2)
            else:
                self.assertIsInstance(shapely_geometry, shapely.Polygon)
            coordinates = shapely.get_coordinates(shapely_geometry).tolist()
            self.assertEqual(sorted(map(tuple, coordinates)),
                             sorted(tuple(vertex) for ring in expected for vertex in ring))

    def test_disk_floor_refuses_before_any_fetch(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        fetches = {"count": 0}
        real_send = httpx.Client.send

        def counting_send(client, request, **kwargs):
            fetches["count"] += 1
            return real_send(client, request, **kwargs)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport):
                with patch.object(src.shutil, "disk_usage",
                                  return_value=SimpleNamespace(free=src.MIN_FREE_BYTES - 1)), \
                        patch.object(httpx.Client, "send", counting_send):
                    with self.assertRaisesRegex(src.PadUsProtectionError,
                                                "disk_floor_before_acquisition"):
                        src.capture_pad_us_protection_mechanism_category(_staging(root), version="floor-run",
                                                 capture_directory=str(root / "capture"))
            self.assertEqual(fetches["count"], 0)
            self.assertFalse((root / "capture").exists())
            self.assertFalse(any((root / "staging").iterdir()))

    def test_complete_fixture_capture_materializes_and_repeats(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                first = src.capture_pad_us_protection_mechanism_category(storage, version="fixture-run",
                                                 capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                self.assertFalse(first["source_release_accepted"])
                second = src.capture_pad_us_protection_mechanism_category(storage, version="fixture-run-2",
                                                  capture_directory=str(capture_root))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertTrue((capture_root / "computed_baseline.json").exists())
            parquet_path = capture_root / f"{src.FILE_SLUG}.parquet"
            table = pq.read_table(parquet_path)
            self.assertEqual(table.num_rows, FIXTURE_COUNT)
            self.assertEqual(table.schema.names[-1], "geometry")
            self.assertEqual(table.column("OBJECTID").to_pylist(),
                             list(FIXTURE_OBJECTIDS))
            self.assertEqual(table.column("FeatClass").to_pylist(),
                             [row["attributes"]["FeatClass"] for row in rows])
            geo_metadata = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                             ["Polygon", "MultiPolygon"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["crs"]["baseCRS"]["name"]
                             if "baseCRS" in geo_metadata["columns"]["geometry"]["crs"]
                             else geo_metadata["columns"]["geometry"]["crs"].get("name", ""),
                             "WGS 84")

            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            from dagster import DagsterInstance, materialize
            with fixture_mode(schema_sha, transport, page_size=2):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    result = materialize(
                        [pad_us_protection_mechanism_category, pad_us_protection_mechanism_category_complete],
                        resources={"staging_storage": _staging(root)},
                        instance=instance,
                        run_config={"ops": {"ingest__pad_us_protection_mechanism_category__pad_us_protection_mechanism_category": {"config": {
                            "capture_directory": str(capture_root)}}}},
                    )
                    self.assertTrue(result.success)
                    checks = {check.check_name: check
                              for check in result.get_asset_check_evaluations()}
                    check = checks["pad_us_protection_mechanism_category_complete"]
                    self.assertTrue(check.passed)
                    candidate = result.output_for_node("ingest__pad_us_protection_mechanism_category__pad_us_protection_mechanism_category")
                    self.assertEqual(candidate["feature_count"], FIXTURE_COUNT)
                    self.assertTrue(candidate["reused"])

    def test_producer_pages_may_omit_the_optional_z_and_m_flags(self):
        # The live producer's query responses omit hasZ/hasM entirely
        # (probed repeatedly 2026-11-14); the layer document keeps pinning the
        # non-Z/M shape, so an absent flag must not block the seal while an
        # explicit true stays drift (covered by the quarantine cases above).
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, page_has_z=None)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                result = src.capture_pad_us_protection_mechanism_category(
                    storage, version="flags-omitted", capture_directory=str(capture_root))
                self.assertEqual(result["feature_count"], FIXTURE_COUNT)
                self.assertFalse(result["reused"])

    def test_retained_geoparquet_seals_into_staging(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                result = src.capture_pad_us_protection_mechanism_category(_staging(root), version="seal-run",
                                                  capture_directory=str(capture_root))
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            sealed = json.loads((root / "staging" / prefix / "metadata"
                                 / "source_audit.json").read_bytes())
            self.assertEqual(sealed["source_feature_count"], FIXTURE_COUNT)
            self.assertTrue(sealed["baseline_pending_complete_capture"])
            self.assertEqual(sealed["pagination"]["mode"], "objectid_keyset_asc")
            self.assertTrue(sealed["pagination"]["resumable"])
            self.assertFalse(sealed["release_accepted"])
            manifest = json.loads((root / "staging" / prefix / "metadata"
                                   / "source_manifest.json").read_bytes())
            self.assertFalse(manifest["approved"])
            self.assertEqual(manifest["agency"], "U.S. Geological Survey")
            hosts = {entry["host"] for entry in manifest["inputs"]}
            self.assertTrue(all(host in ("services.arcgis.com", "www.arcgis.com", "www.usgs.gov")
                                for host in hosts))
            self.assertEqual([entry["input_id"] for entry in manifest["inputs"]
                              if "page_" in entry["input_id"]], [])
            aggregate = [entry for entry in manifest["inputs"]
                         if entry["input_id"] == src.PAGED_INPUT_ID]
            self.assertEqual(len(aggregate), 1)
            self.assertEqual(aggregate[0]["page_query_count"],
                             -(-FIXTURE_COUNT // 2) + 1)
            parquet_bytes = (root / "staging" / prefix / "geoparquet"
                             / f"{src.DATASET_SLUG}.parquet").read_bytes()
            self.assertEqual(pq.read_table(io.BytesIO(parquet_bytes)).num_rows, FIXTURE_COUNT)

    def test_seal_and_reuse_stream_parquet_without_reading_it_into_memory(self):
        """The national parquet is sealed and verified as a streamed file.

        Reading the candidate parquet through read_bytes would buffer the
        complete national geometry population (tens of GB) in memory during the
        seal, contradicting this module's bounded streaming contract.
        """
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                first = src.capture_pad_us_protection_mechanism_category(storage, version="stream-seal-run",
                                                 capture_directory=str(capture_root))
                parquet_path = capture_root / f"{src.FILE_SLUG}.parquet"
                real_read_bytes = Path.read_bytes

                def refuse_parquet_read_bytes(path: Path):
                    if path == parquet_path:
                        raise AssertionError("candidate parquet was read into memory")
                    return real_read_bytes(path)

                with patch.object(Path, "read_bytes", refuse_parquet_read_bytes):
                    reused = src.capture_pad_us_protection_mechanism_category(storage, version="stream-seal-run-2",
                                                      capture_directory=str(capture_root))
                    fresh = src.capture_pad_us_protection_mechanism_category(_staging(root / "b"),
                                                     version="stream-seal-run",
                                                     capture_directory=str(capture_root))
            self.assertTrue(reused["reused"])
            self.assertFalse(fresh["reused"])
            self.assertEqual(fresh["version"], first["version"])
            self.assertEqual(fresh["feature_count"], FIXTURE_COUNT)
            sealed = (f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{first['version']}"
                      f"/geoparquet/{src.DATASET_SLUG}.parquet")
            self.assertEqual((root / "staging" / sealed).read_bytes(),
                             parquet_path.read_bytes())
            self.assertEqual((root / "b" / "staging" / sealed).read_bytes(),
                             parquet_path.read_bytes())

    def test_stream_validate_extrema_are_running_and_memory_bounded(self):
        """Bounds stay four running extrema; per-vertex retention is refused.

        Holding both ordinate floats for every vertex of the 441,609-feature
        population would retain gigabytes during the streaming validation, so
        this regression walks many vertex-heavy pages and requires the traced
        peak to stay below the accumulation the old list layout needed.
        """
        import tracemalloc

        pages = 150
        features_per_page = 2
        vertices_per_feature = 600
        count = pages * features_per_page

        def geometry(row: dict) -> dict:
            index = row["attributes"]["OBJECTID"] - 1
            # A large clockwise ring in the far northwest (top edge west->east,
            # bottom edge east->west) plus one tiny clockwise exterior in the
            # southeast so the extrema span both axes without hole gating.
            top = [[-170.0 + index * 0.1 + offset * 1e-6, 80.0 - index * 0.1]
                   for offset in range(vertices_per_feature)]
            bottom = [[-170.0 + index * 0.1 + (vertices_per_feature - offset) * 1e-6,
                       79.9 - index * 0.1] for offset in range(vertices_per_feature)]
            ring = top + bottom
            ring.append(ring[0])
            return {"rings": [ring, [[-80.0, 25.1], [-79.9, 25.1], [-79.9, 25.0],
                                     [-80.0, 25.0], [-80.0, 25.1]]]}

        rows = []
        for oid in range(1, count + 1):
            attrs = dict(_fixture_attrs(FIXTURE_OBJECTIDS[0]))
            attrs["OBJECTID"] = oid
            rows.append({"attributes": attrs, "geometry": None})
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, geometry=geometry)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2), \
                    patch.object(src, "EXPECTED_COUNT", count), \
                    patch.object(src, "EXPECTED_IDS_SHA256", _ids_sha(rows)):
                src.fetch_pad_us_protection_mechanism_category_pages(capture_root)
                tracemalloc.start()
                try:
                    extracted = src._stream_validate(capture_root, parquet_sink=None)
                finally:
                    _current, peak = tracemalloc.get_traced_memory()
                    tracemalloc.stop()
            last = count - 1
            self.assertEqual(extracted["observed_bounds"], {
                "xmin": -170.0,
                "ymin": 25.0,
                "xmax": -79.9,
                "ymax": 80.0,
            })
            accumulation_bytes = count * vertices_per_feature * 2 * 32
            self.assertLess(peak, accumulation_bytes)

    def test_packaged_identity_first_seal_and_reuse_validate_for_real(self):
        """A Git-free installed tree must seal and reuse with real validation.

        The shipped runtime has no .git, so processing_identity returns a
        packaged-sha256 identity; validate_construction_code_identity must
        receive the full identity on both the first-seal and the reuse path, or
        it rejects the packaged commit as construction_invalid_commit.
        """
        import shutil as shutil_module

        from dagster_hifld.construction_provenance import validate_construction_code_identity

        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tree = root / "installed"
            (tree / "src").mkdir(parents=True)
            shutil_module.copytree(Path(src.__file__).parents[1], tree / "src" / "dagster_hifld",
                                   ignore=shutil_module.ignore_patterns("__pycache__"))
            for name in ("pyproject.toml", "uv.lock"):
                shutil_module.copy2(Path(src.__file__).parents[3] / name, tree / name)
            with fixture_mode(schema_sha, transport, page_size=2, real_identity=True), \
                    patch.object(src, "__file__", str(tree / "src" / "dagster_hifld"
                                                      / "sources" / "pad_us_protection_mechanism_category.py")):
                storage = _staging(root)
                first = src.capture_pad_us_protection_mechanism_category(storage, version="packaged-run",
                                                 capture_directory=str(root / "capture"))
                identity = src._code_identity()
                self.assertEqual(identity["code_identity_scheme"],
                                 "sha256-packaged-dagster-hifld-v1")
                self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                second = src.capture_pad_us_protection_mechanism_category(storage, version="packaged-run-2",
                                                  capture_directory=str(root / "capture"))
                self.assertTrue(second["reused"])
                self.assertEqual(first["version"], second["version"])
                validate_construction_code_identity(tree, identity["code_commit"],
                                                    identity["files"], identity=identity)

    def test_transient_error_page_is_witnessed_and_resume_refetches(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        base = make_responder(rows)
        injected = {"error_sent": False}

        def responder(request: httpx.Request) -> httpx.Response:
            query = parse_qs(urlparse(str(request.url)).query)
            if ("where" in query and query["where"][0].startswith("OBJECTID>")
                    and "returnCountOnly" not in query and not injected["error_sent"]):
                injected["error_sent"] = True
                return httpx.Response(200, stream=JSONStream(
                    {"error": {"code": 429, "message": "busy"}}),
                    headers={"content-type": "application/json"})
            return base.handler(request)

        transport = httpx.MockTransport(responder)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                with self.assertRaisesRegex(src.PadUsProtectionError, "source_page_error"):
                    src.fetch_pad_us_protection_mechanism_category_pages(capture_root)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
                self.assertFalse((capture_root / "page_000000.body").exists())
                result = src.fetch_pad_us_protection_mechanism_category_pages(capture_root)
                self.assertTrue(result["complete"])
                storage = _staging(root)
                sealed = src.capture_pad_us_protection_mechanism_category(storage, version="witness-run",
                                                  capture_directory=str(capture_root))
                self.assertEqual(sealed["feature_count"], FIXTURE_COUNT)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))

    def test_resume_reuses_retained_pages_and_discards_torn_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                src.capture_pad_us_protection_mechanism_category(storage, version="resume-run",
                                         capture_directory=str(capture_root))
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                self.assertEqual(len(state["data_pages"]), 3)
                # Simulate an interruption after the final data page: drop the tail witness.
                for suffix in ("body", "wire", "receipt.json"):
                    (capture_root / f"page_tail.{suffix}").unlink()
                state["tail"] = None
                (capture_root / "capture_state.json").write_bytes(src._canonical(state) + b"\n")
                fetches = {"count": 0}
                real_send = httpx.Client.send

                def counting_send(client, request, **kwargs):
                    fetches["count"] += 1
                    return real_send(client, request, **kwargs)

                with patch.object(httpx.Client, "send", counting_send):
                    result = src.fetch_pad_us_protection_mechanism_category_pages(capture_root)
                self.assertTrue(result["complete"])
                self.assertLessEqual(fetches["count"], 6)
                self.assertTrue((capture_root / "page_tail.body").exists())
                # A torn retained response is never trusted without a fetch client.
                (capture_root / "count_after.body").write_bytes(b'{"count":999999999}')
                with self.assertRaisesRegex(src.PadUsProtectionError,
                                            "retained_capture_receipt_invalid"):
                    src._retained_or_fetch(None, capture_root, "count_after", src._count_url())
                # With a fetch client the torn response is discarded and refetched.
                with patch.object(httpx.Client, "send", counting_send):
                    src.fetch_pad_us_protection_mechanism_category_pages(capture_root)
                self.assertTrue((capture_root / "page_tail.body").exists())

    def test_drift_and_provenance_failures_are_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        cases = {
            "source_count_drift": dict(count=FIXTURE_COUNT + 1),
            "source_schema_drift": dict(layer_fields=_fixture_layer_fields()
                                        + [{"name": "EXTRA", "type": "esriFieldTypeString",
                                            "alias": "EXTRA", "nullable": True, "length": 4,
                                            "domain": None}]),
            "source_schema_baseline_drift": dict(
                layer_fields=[{**field, "length": (field.get("length") or 0) + 1}
                              if field.get("length") else field
                              for field in _fixture_layer_fields()]),
            "usgs_padus_service_identity_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "copyrightText": "Data refreshed October, 2026."}),
            "usgs_padus_service_layer_roster_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "layers": _fixture_service_doc()["layers"][:-1]}),
            "usgs_padus_service_description_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "serviceDescription": "legacy NHD service."}),
            "exact_padus_protection_mechanism_layer_metadata_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()),
                       "geometryType": "esriGeometryPolyline"}),
            "source_page_z_or_m_flag_drift:page_000000": dict(page_has_z=True),
            "source_layer_z_or_m_contract_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "hasZ": True}),
            "source_record_limit_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "maxRecordCount": 500}),
            "source_layer_extent_drift": dict(
                layer=lambda: {**_fixture_layer_doc(_fixture_layer_fields()),
                               "extent": {**src.EXPECTED_LAYER_EXTENT,
                                          "xmax": 1.0,
                                          "spatialReference": dict(src.NATIVE_LAYER_SR)}}),
            "source_id_roster_drift:ids_before": dict(
                count=FIXTURE_COUNT, corrupt_ids=True),
            "source_page_transfer_truncated": dict(
                geometry=lambda row: _fixture_geometry(row["attributes"]["OBJECTID"]),
                truncate_short_page=True),
            "unexpected_source_geometry_members": dict(
                geometry=lambda row: {**_fixture_geometry(row["attributes"]["OBJECTID"]),
                                      "curves": []}),
            "source_coordinate_range_drift": dict(
                geometry=lambda row: {"rings": [
                    [[200.0, vertex[1]] for vertex in
                     _fixture_geometry(row["attributes"]["OBJECTID"])["rings"][0]]]}),
            "source_object_ids_not_strictly_increasing": dict(
                data_rows=[dict(rows[0]), dict(rows[0]), *rows[1:]], count=FIXTURE_COUNT),
            "source_object_id_type_changed": dict(
                data_rows=[{**rows[0], "attributes": {**rows[0]["attributes"], "OBJECTID": None}},
                           *rows[1:]]),
            "source_native_type_changed:GIS_Acres": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "GIS_Acres": "123"}},
                *rows[1:]]),
            "source_native_type_changed:Shape__Area": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "Shape__Area": "big"}},
                *rows[1:]]),
            "source_nonnullable_value_is_null:GlobalID": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "GlobalID": None}},
                *rows[1:]]),
            "source_record_schema_or_geometry_drift": dict(rows=[
                {**rows[0], "attributes": {"OBJECTID": 1, "GlobalID": "x"}},
                *rows[1:]]),
            "usgs_service_documentation_link_drift": dict(
                doc_page=b"<html>an unrelated page without the service host</html>"),
            "usgs_arcgis_item_owner_drift:view_item": dict(
                item_owner="someone@elsewhere.com_Editorial"),
        }
        for expected_error, kwargs in cases.items():
            with self.subTest(expected_error):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    capture_root = root / "capture"
                    truncate_short_page = kwargs.pop("truncate_short_page", False)
                    corrupt_ids = kwargs.pop("corrupt_ids", False)
                    case_rows = kwargs.pop("rows", rows)
                    base_transport = make_responder(case_rows, **kwargs)

                    def responder(request: httpx.Request, _base=base_transport,
                                  _truncate=truncate_short_page,
                                  _corrupt=corrupt_ids) -> httpx.Response:
                        query = parse_qs(urlparse(str(request.url)).query)
                        response = _base.handler(request)
                        if _corrupt and query.get("returnIdsOnly") == ["true"]:
                            payload = json.loads(response.read())
                            payload["objectIds"] = payload["objectIds"][:-1] + [999999]
                            return httpx.Response(200, stream=JSONStream(payload),
                                                  headers={"content-type": "application/json"})
                        if _truncate and "where" in query \
                                and query["where"][0].startswith("OBJECTID>") \
                                and "returnCountOnly" not in query:
                            payload = json.loads(response.read())
                            if len(payload["features"]) < src.PAGE_SIZE:
                                payload["exceededTransferLimit"] = True
                            return httpx.Response(200, stream=JSONStream(payload),
                                                  headers={"content-type": "application/json"})
                        return response

                    transport = httpx.MockTransport(responder)
                    with fixture_mode(schema_sha, transport, page_size=2):
                        storage = _staging(root)
                        with self.assertRaisesRegex(src.PadUsProtectionError, expected_error):
                            src.capture_pad_us_protection_mechanism_category(storage, version="drift-run",
                                                     capture_directory=str(capture_root))
                    quarantined = sorted(
                        (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
                    self.assertTrue(quarantined, f"no quarantine for {expected_error}")
                    failure = json.loads((quarantined[0] / "metadata" / "failure.json")
                                         .read_bytes())
                    self.assertTrue(failure["reason_code"])

    def test_pinned_baseline_drift_is_refused_and_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                src.capture_pad_us_protection_mechanism_category(storage, version="pin-run",
                                         capture_directory=str(capture_root))
                baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
                with patch.object(src, "EXPECTED_RECORD_SHA256",
                                  baseline["source_record_sha256"]):
                    again = src.capture_pad_us_protection_mechanism_category(storage, version="pin-run-verify",
                                                     capture_directory=str(capture_root))
                    self.assertTrue(again["reused"])
                with patch.object(src, "EXPECTED_IDS_SHA256", "0" * 64):
                    with self.assertRaisesRegex(src.PadUsProtectionError,
                                                "source_id_roster_drift"):
                        src.capture_pad_us_protection_mechanism_category(storage, version="pin-run-drift",
                                                 capture_directory=str(capture_root))
                with patch.object(src, "EXPECTED_UNUSABLE_GEOMETRY_COUNTS",
                                  {"source_polygon_ring_degenerate": 2}):
                    fresh_root = root / "capture-unusable"
                    with self.assertRaisesRegex(src.PadUsProtectionError,
                                                "source_unusable_geometry_count_drift"):
                        src.capture_pad_us_protection_mechanism_category(
                            storage, version="pin-run-unusable",
                            capture_directory=str(fresh_root))
                quarantined = sorted((root / "staging" / "_quarantine" / src.DATASET_SLUG)
                                     .glob("*"))
                self.assertTrue(quarantined)
                failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
                self.assertEqual(failure["reason_code"], "source_id_roster_drift:ids_before")


if __name__ == "__main__":
    unittest.main()

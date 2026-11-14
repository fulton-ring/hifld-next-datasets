"""Focused tests for the USGS 3DHP_all Waterbody draft pipeline."""
import io
import json
import struct
import tempfile
import unittest
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import httpx
import pyarrow.parquet as pq

from dagster_hifld.assets.nhd_waterbody import nhd_waterbody, nhd_waterbody_complete
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_frs_common
from dagster_hifld.sources import nhd_waterbody as src

FIXTURE_COUNT = 5
FIXTURE_OBJECTIDS = (1, 2, 3, 5, 8)
REFUSED_FIXTURE_OID = 3


def _exterior(offset: float) -> list:
    """Clockwise (negative signed area) closed ring: an Esri JSON exterior."""
    points = [[-89.0 + offset, 31.0 + offset, 100.0 + offset],
              [-89.0 + offset, 32.0 + offset, 100.0 + offset],
              [-90.0 + offset, 32.0 + offset, 100.0 + offset],
              [-90.0 + offset, 31.0 + offset, 100.0 + offset]]
    return [points[0], points[3], points[2], points[1], points[0]]


def _hole(x_min: float, y_min: float, z: float) -> list:
    """Counter-clockwise closed ring strictly inside the given exterior bounds."""
    points = [[x_min + 0.2, y_min + 0.2, z], [x_min + 0.8, y_min + 0.2, z],
              [x_min + 0.8, y_min + 0.8, z], [x_min + 0.2, y_min + 0.8, z]]
    return [*points, points[0]]


def _fixture_attrs(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    return {
        "OBJECTID": oid,
        "id3dhp": f"{index + 1:06X}",
        "featuredate": 1695340800000 + index,
        "mainstemid": f"https://geoconnex.us/usgs/mainstems/{oid}" if index != 1 else None,
        "gnisid": 694899 + index if index != 2 else None,
        "gnisidlabel": "Fixture Lake" if index != 2 else None,
        "featuretype": [1, 2, 3, 4, 1][index],
        "featuretypelabel": ["River", "Canal", "Lake", "Ocean or Great Lake", "River"][index],
        "areasqkm": 0.24430194 + index,
        "workunitid": "19050401",
        "Shape__Length": 61.22714663080581 + index,
        "Shape__Area": 13819.496815191216 + index,
    }


def _fixture_geometry(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    if index == 2:
        # MultiPolygon Z: two exteriors, the second carrying one hole.
        rings = [_exterior(0.0), _exterior(5.0), _hole(-84.9, 36.1, 105.0)]
    elif index == 3:
        # Polygon Z with one hole (exterior spans x [-88, -87], y [32, 33]).
        rings = [_exterior(1.0), _hole(-88.9, 32.1, 101.0)]
    else:
        rings = [_exterior(float(index))]
    return {"hasZ": True, "rings": rings}


def _fixture_rows() -> list[dict]:
    return [{"attributes": _fixture_attrs(oid),
             "geometry": _fixture_geometry(oid)} for oid in FIXTURE_OBJECTIDS]


# Real producer-served degenerate ring (OBJECTID 279258 in the complete
# 2026-11-13 capture): a closed four-vertex spike whose shoelace area
# collapses to exactly 0.0 in float64 under the module's own per-term
# computation. The 13 rings observed in that capture all have this shape.
REAL_DEGENERATE_SPIKE = [
    [-87.28423147967823, 39.571322553363785, 0],
    [-87.284229168313, 39.57132196270366, 0],
    [-87.28422989505007, 39.57132238856065, 0],
    [-87.28423147967823, 39.571322553363785, 0],
]


def _fixture_degenerate_spike() -> list:
    """Closed zero-area spike strictly inside the fixture oid-8 exterior.

    Every ordinate pair is dyadic, so the shoelace terms telescope exactly to
    0.0 in float64.
    """
    return [[-85.75, 35.5, 105.0], [-85.5, 35.5, 105.0],
            [-85.25, 35.5, 105.0], [-85.75, 35.5, 105.0]]


def _real_spike_exterior() -> list:
    """Clockwise closed exterior that contains the real observed spike."""
    points = [[-87.0, 39.5, 100.0], [-87.5, 39.5, 100.0],
              [-87.5, 39.7, 100.0], [-87.0, 39.7, 100.0]]
    return [*points, points[0]]


def _fixture_layer_fields() -> list[dict]:
    fields = []
    for name, esri_type, nullable, length in src.NATIVE_FIELDS:
        field = {"name": name, "alias": name if name not in ("Shape__Length", "Shape__Area")
                 else name.replace("__", "_").lower(),
                 "type": esri_type, "nullable": nullable, "domain": None}
        if length is not None:
            field["length"] = length
        fields.append(field)
    fields[-2]["alias"] = "shape_Length"
    fields[-1]["alias"] = "shape_Area"
    return fields


def _fixture_layer_doc(fields: list[dict]) -> dict:
    return {"id": src.LAYER_ID, "name": src.LAYER_NAME, "type": "Feature Layer",
            "geometryType": src.GEOMETRY_TYPE, "hasZ": True, "hasM": False,
            "maxRecordCount": src.PAGE_SIZE,
            "advancedQueryCapabilities": {"supportsPagination": True},
            "sourceSpatialReference": dict(src.NATIVE_LAYER_SR),
            "extent": {**src.EXPECTED_LAYER_EXTENT,
                       "spatialReference": dict(src.NATIVE_LAYER_SR)},
            "fields": fields}


def _fixture_service_doc() -> dict:
    return {"copyrightText": src.EXPECTED_COPYRIGHT_TEXT,
            "serviceDescription": "The USGS 3D Hydrography Program (3DHP) 3DHP_all service.",
            "spatialReference": {"wkid": 102100, "latestWkid": 3857},
            "layers": [{"id": layer_id, "name": name}
                       for layer_id, name in src.EXPECTED_SERVICE_LAYERS]}


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
                   layer=None, doc_page=None, geometry=None, max_served_size=None,
                   refuse_geometry_oids=frozenset()):
    count = count if count is not None else len(rows)
    layer_fields = layer_fields if layer_fields is not None else _fixture_layer_fields()

    def layer_doc():
        if callable(layer):
            return layer()
        if layer is not None:
            return layer
        return _fixture_layer_doc(layer_fields)

    service_doc = service if service is not None else _fixture_service_doc()
    state = {"page_errors": 0}

    def respond(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path
        query = parse_qs(urlparse(str(request.url)).query)
        if path.startswith("/3d-hydrography-program/"):
            return httpx.Response(200, stream=JSONStream(doc_page or
                b"<html>3DHP access: https://3dhp.nationalmap.gov/arcgis/rest/services"
                b"/usgs_3dhp_all/FeatureServer</html>"),
                headers={"content-type": "text/html"})
        if path.rstrip("/").endswith("FeatureServer"):
            return httpx.Response(200, stream=JSONStream(service_doc),
                                  headers={"content-type": "application/json"})
        if path.rstrip("/").endswith(f"/{src.LAYER_ID}"):
            return httpx.Response(200, stream=JSONStream(layer_doc()),
                                  headers={"content-type": "application/json"})
        if query.get("returnCountOnly") == ["true"]:
            return httpx.Response(200, stream=JSONStream({"count": count}),
                                  headers={"content-type": "application/json"})
        if "objectIds" in query:
            wanted = int(query["objectIds"][0].split(",")[0])
            matches = [row for row in rows if row["attributes"]["OBJECTID"] == wanted]
            if wanted in refuse_geometry_oids and query.get("returnGeometry") == ["true"]:
                return httpx.Response(200, stream=JSONStream(
                    {"error": {"code": 500, "message": "Error performing query operation",
                               "details": []}}),
                    headers={"content-type": "application/json"})
            payload_rows = [dict(row) for row in matches]
            if query.get("returnGeometry") != ["true"]:
                payload_rows = [{"attributes": row["attributes"]} for row in payload_rows]
            elif geometry is not None:
                payload_rows = [dict(row, geometry=geometry(row)) for row in payload_rows]
            else:
                payload = {"objectIdFieldName": "OBJECTID",
                           "spatialReference": {"wkid": src.SOURCE_CRS_WKID,
                                                "latestWkid": src.SOURCE_CRS_WKID},
                           "hasZ": True, "features": payload_rows}
                return httpx.Response(200, stream=JSONStream(payload),
                                      headers={"content-type": "application/json"})
            return httpx.Response(200, stream=JSONStream(
                {"objectIdFieldName": "OBJECTID", "features": payload_rows}),
                headers={"content-type": "application/json"})
        last = int(query["where"][0].split(">")[1])
        requested = int(query["resultRecordCount"][0])
        oids = sorted(row["attributes"]["OBJECTID"] for row in rows
                      if isinstance(row["attributes"]["OBJECTID"], int))
        remaining = [oid for oid in oids if oid > last]
        page_oids = remaining[:requested]
        if max_served_size is not None and requested > max_served_size and page_oids:
            state["page_errors"] += 1
            return httpx.Response(200, stream=JSONStream(
                {"error": {"code": 500, "message": "Error performing query operation",
                           "details": []}}),
                headers={"content-type": "application/json"})
        if any(oid in refuse_geometry_oids for oid in page_oids) \
                and query.get("returnGeometry") == ["true"]:
            # The producer refuses any geometry window that contains a refused
            # record; its attributes-only channel keeps serving those records.
            return httpx.Response(200, stream=JSONStream(
                {"error": {"code": 500, "message": "Error performing query operation",
                           "details": []}}),
                headers={"content-type": "application/json"})
        if query.get("returnGeometry") != ["true"]:
            payload_rows = [{"attributes": row["attributes"]}
                            for row in (next(row for row in rows
                                             if row["attributes"]["OBJECTID"] == oid)
                                        for oid in page_oids)]
            return httpx.Response(200, stream=JSONStream(
                {"objectIdFieldName": "OBJECTID", "features": payload_rows}),
                headers={"content-type": "application/json"})
        abnormal = [row for row in rows
                    if not isinstance(row["attributes"].get("OBJECTID"), int)]
        page_rows = [*abnormal,
                     *(next(row for row in rows if row["attributes"]["OBJECTID"] == oid)
                       for oid in page_oids)]
        if geometry is not None:
            page_rows = [dict(row, geometry=geometry(row)) if geometry else row
                         for row in page_rows]
        more = bool(page_oids) and max(page_oids) < max(oids)
        return httpx.Response(200, stream=JSONStream(
            {"objectIdFieldName": "OBJECTID",
             "spatialReference": {"wkid": src.SOURCE_CRS_WKID, "latestWkid": src.SOURCE_CRS_WKID},
             "hasZ": True, "features": page_rows,
             "exceededTransferLimit": more}),
            headers={"content-type": "application/json"})

    return httpx.MockTransport(respond)


@contextmanager
def fixture_mode(schema_sha: str, transport: httpx.MockTransport, *, page_size=None,
                 count=FIXTURE_COUNT, max_objectid=max(FIXTURE_OBJECTIDS),
                 refused_ids=None, null_geometry_count=None, distribution=None,
                 degenerate_count=None, degenerate_ids=None):
    import dagster_hifld.assets.nhd_waterbody as asset_module
    rows = _fixture_rows()
    if distribution is None:
        distribution = dict(Counter(row["attributes"]["featuretype"] for row in rows))
    contexts = [
        patch.object(src, "EXPECTED_COUNT", count),
        patch.object(src, "EXPECTED_MAX_OBJECTID", max_objectid),
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(src, "EXPECTED_RECORD_SHA256", ""),
        patch.object(src, "EXPECTED_GEOMETRY_SHA256", ""),
        patch.object(src, "EXPECTED_IDS_SHA256", ""),
        patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", null_geometry_count),
        patch.object(src, "EXPECTED_REFUSED_IDS", refused_ids),
        patch.object(src, "EXPECTED_DEGENERATE_RING_COUNT", degenerate_count),
        patch.object(src, "EXPECTED_DEGENERATE_RING_IDS", degenerate_ids),
        patch.object(src, "EXPECTED_FEATURETYPE_DISTRIBUTION", distribution),
        patch.object(asset_module, "EXPECTED_COUNT", count),
        patch.object(src, "_default_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_doc_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_code_identity",
                     return_value={"code_identity_scheme": "test", "code_commit": "synthetic",
                                   "files": [], "working_tree_status": "unknown"}),
        patch.object(src, "validate_construction_code_identity"),
        patch.object(epa_frs_common, "validate_construction_code_identity"),
        # CI runners hold far less than the pinned capture floor, so the floor
        # gate is evaluated against a stub that reports exactly the pinned
        # requirement; the refusal itself is exercised by the dedicated floor
        # test below.
        patch.object(src.shutil, "disk_usage",
                     return_value=SimpleNamespace(free=src.MIN_FREE_BYTES)),
    ]
    if page_size is not None:
        contexts.append(patch.object(src, "PAGE_SIZE", page_size))
    for context in contexts:
        context.start()
    try:
        yield
    finally:
        for context in reversed(contexts):
            context.stop()


def _staging(root: Path) -> StagingStorageResource:
    return StagingStorageResource(use_local=True, local_dir=str(root / "staging"),
                                  prefix="", backend="local")


class NhdWaterbodyTests(unittest.TestCase):
    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "nhd-waterbody")
        self.assertEqual(src.SERVICE_URL,
                         "https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all"
                         "/FeatureServer")
        self.assertEqual(src.LAYER_ID, 60)
        self.assertEqual(src.LAYER_NAME, "Waterbody")
        self.assertEqual(src.GEOMETRY_TYPE, "esriGeometryPolygon")
        self.assertEqual(src.EXPECTED_COUNT, 7_158_943)
        self.assertEqual(src.EXPECTED_MAX_OBJECTID, 7_158_943)
        self.assertEqual(src.PAGE_SIZE, 2500)
        self.assertEqual(len(src.NATIVE_FIELDS), 12)
        self.assertEqual(src.MIN_FREE_BYTES, 55_000_000_000)
        # Baselines pinned from the first complete capture (2026-11-13):
        # digests non-empty, the refusal and degenerate-ring contracts are
        # exact tuples, and any later drift fails closed.
        self.assertRegex(src.EXPECTED_RECORD_SHA256, "^[0-9a-f]{64}$")
        self.assertRegex(src.EXPECTED_GEOMETRY_SHA256, "^[0-9a-f]{64}$")
        self.assertRegex(src.EXPECTED_IDS_SHA256, "^[0-9a-f]{64}$")
        self.assertEqual(src.EXPECTED_NULL_GEOMETRY_COUNT, 9)
        self.assertEqual(src.EXPECTED_REFUSED_IDS,
                         (1293173, 2520496, 4245000, 4694427, 7059540, 7059541,
                          7098625, 7135496, 7157221))
        self.assertEqual(src.EXPECTED_DEGENERATE_RING_COUNT, 13)
        self.assertEqual(src.EXPECTED_DEGENERATE_RING_IDS,
                         (279258, 2727549, 2954106, 3590176, 3601265, 4575241,
                          5019081, 5144328, 5802262, 7072361, 7074673,
                          7147977, 7151432))
        self.assertEqual(src.EXPECTED_FEATURETYPE_DISTRIBUTION,
                         {1: 85220, 2: 9235, 3: 7063520, 4: 968})
        self.assertEqual(src.EXPECTED_FEATURETYPE_LABELS,
                         {1: "River", 2: "Canal", 3: "Lake", 4: "Ocean or Great Lake"})
        self.assertTrue(src.BASELINE_PENDING_NOTE)
        keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertTrue(any(key.path == ["ingest", src.DATASET_SLUG, src.DATASET_SLUG]
                            for key in keys))

    def test_keyset_pagination_contract_is_ordered_and_bounded(self):
        url = src._page_url(15000000, src.PAGE_SIZE)
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["where"], ["OBJECTID>15000000"])
        self.assertEqual(query["orderByFields"], ["OBJECTID ASC"])
        self.assertEqual(query["resultRecordCount"], [str(src.PAGE_SIZE)])
        self.assertEqual(query["outSR"], [str(src.SOURCE_CRS_WKID)])
        self.assertEqual(query["returnGeometry"], ["true"])
        self.assertEqual(query["returnZ"], ["true"])
        self.assertLess(len(url), 400)
        self.assertEqual(src._bisect(src.PAGE_SIZE), src.PAGE_SIZE // 2)
        self.assertEqual(src._bisect(2), 1)

    def test_page_name_roundtrip(self):
        name = src._page_name(123456789, 1250)
        self.assertEqual(name, "page_123456789_rec1250")
        self.assertEqual(src._parse_page_name(name), (123456789, 1250))
        with self.assertRaisesRegex(src.NhdWaterbodyError, "source_page_name_invalid"):
            src._parse_page_name("page_1234_rec2")
        with self.assertRaisesRegex(src.NhdWaterbodyError, "source_page_name_invalid"):
            src._parse_page_name("page_123456789_recX")

    def test_polygon_wkb_keeps_z_and_ring_semantics(self):
        geometry = _fixture_geometry(FIXTURE_OBJECTIDS[2])
        wkb = src._polygon_wkb(geometry)
        order, kind = struct.unpack_from("<BI", wkb, 0)
        self.assertEqual((order, kind), (1, 0x80000006))
        (parts,) = struct.unpack_from("<I", wkb, 5)
        self.assertEqual(parts, 2)
        single = src._polygon_wkb(_fixture_geometry(FIXTURE_OBJECTIDS[0]))
        self.assertEqual(struct.unpack_from("<BI", single, 0), (1, 0x80000003))
        # The producer's closed-ring, CW-exterior/CCW-hole semantics are preserved:
        # every ring re-reads as the same vertex sequence with Z.
        polygon = _fixture_geometry(FIXTURE_OBJECTIDS[3])
        wkb_with_hole = src._polygon_wkb(polygon)
        self.assertEqual(struct.unpack_from("<BI", wkb_with_hole, 0), (1, 0x80000003))
        (rings,) = struct.unpack_from("<I", wkb_with_hole, 5)
        self.assertEqual(rings, 2)

    def test_degenerate_zero_area_ring_is_retained_verbatim(self):
        from shapely import from_wkb
        collected: list = []
        geometry = {"hasZ": True, "rings": [_real_spike_exterior(),
                                            [list(vertex) for vertex in REAL_DEGENERATE_SPIKE]]}
        wkb = src._polygon_wkb(geometry, degenerate_rings=collected)
        self.assertEqual(collected, [REAL_DEGENERATE_SPIKE])
        polygon = from_wkb(wkb)
        self.assertEqual(polygon.geom_type, "Polygon")
        self.assertEqual(len(polygon.interiors), 1)
        self.assertEqual([tuple(vertex) for vertex in polygon.exterior.coords],
                         [tuple(vertex) for vertex in _real_spike_exterior()])
        self.assertEqual([tuple(vertex) for vertex in polygon.interiors[0].coords],
                         [tuple(vertex) for vertex in REAL_DEGENERATE_SPIKE])
        self.assertAlmostEqual(polygon.area, 0.1)

    def test_degenerate_only_feature_stands_as_zero_area_shell(self):
        from shapely import from_wkb
        collected: list = []
        geometry = {"hasZ": True, "rings": [[list(vertex) for vertex in REAL_DEGENERATE_SPIKE]]}
        wkb = src._polygon_wkb(geometry, degenerate_rings=collected)
        self.assertEqual(collected, [REAL_DEGENERATE_SPIKE])
        self.assertEqual(struct.unpack_from("<BI", wkb, 0), (1, 0x80000003))
        polygon = from_wkb(wkb)
        self.assertEqual(polygon.geom_type, "Polygon")
        self.assertEqual([tuple(vertex) for vertex in polygon.exterior.coords],
                         [tuple(vertex) for vertex in REAL_DEGENERATE_SPIKE])
        # The module's own shoelace loop collapses this ring to exactly 0.0;
        # GEOS's independent summation leaves a ~1e-13 sliver, which is why the
        # degeneracy contract is defined by the pinned module computation.
        self.assertLess(polygon.area, 1e-9)

    def test_uncovered_degenerate_ring_becomes_standalone_shell(self):
        from shapely import from_wkb
        collected: list = []
        # The fixture spike sits far from the fixture exterior (two real
        # producer features serve exactly this shape: a spike no exterior of
        # the feature covers).
        geometry = {"hasZ": True, "rings": [_exterior(50.0), _fixture_degenerate_spike()]}
        wkb = src._polygon_wkb(geometry, degenerate_rings=collected)
        self.assertEqual(collected, [_fixture_degenerate_spike()])
        self.assertEqual(struct.unpack_from("<BI", wkb, 0), (1, 0x80000006))
        (parts,) = struct.unpack_from("<I", wkb, 5)
        self.assertEqual(parts, 2)
        multipolygon = from_wkb(wkb)
        self.assertEqual(multipolygon.geom_type, "MultiPolygon")
        self.assertEqual(multipolygon.geoms[0].area, 1.0)
        self.assertEqual([tuple(vertex) for vertex in multipolygon.geoms[1].exterior.coords],
                         [tuple(vertex) for vertex in _fixture_degenerate_spike()])
        self.assertLess(multipolygon.geoms[1].area, 1e-9)

    def test_degenerate_pin_contract_fails_closed_after_transcription(self):
        rows = _fixture_rows()
        rows[4]["geometry"]["rings"].append(_fixture_degenerate_spike())
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            storage = _staging(root)
            with fixture_mode(schema_sha, transport):
                first = src.capture_nhd_waterbody(storage, version="degen-run",
                                                  capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                baseline = src.build_nhd_waterbody_baseline(str(capture_root))
                self.assertEqual(baseline["source_degenerate_ring_count"], 1)
                self.assertEqual(baseline["source_degenerate_ring_ids"], [8])
                audit = json.loads((root / "staging" / src.DATASET_SLUG / src.FILE_SLUG
                                    / first["version"] / "metadata"
                                    / "source_audit.json").read_bytes())
                self.assertEqual(audit["source_degenerate_ring_count"], 1)
                self.assertEqual(audit["source_degenerate_ring_ids"], [8])
                self.assertTrue(audit["baseline_pending_complete_capture"])
                self.assertIn("coverage-neutral", audit["producer_degenerate_ring_note"])
                table = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet")
                self.assertEqual(table.num_rows, FIXTURE_COUNT)
            with fixture_mode(schema_sha, transport, degenerate_count=1,
                              degenerate_ids=(8,)):
                second = src.capture_nhd_waterbody(storage, version="degen-run-2",
                                                   capture_directory=str(capture_root))
                self.assertEqual(second["version"], first["version"])
                self.assertTrue(second["reused"])
            with fixture_mode(schema_sha, transport, degenerate_ids=(9,)):
                with self.assertRaisesRegex(src.NhdWaterbodyError,
                                            "source_degenerate_ring_ids_drift"):
                    src.build_nhd_waterbody_baseline(str(capture_root))
            with fixture_mode(schema_sha, transport, degenerate_count=2):
                with self.assertRaisesRegex(src.NhdWaterbodyError,
                                            "source_degenerate_ring_count_drift"):
                    src.build_nhd_waterbody_baseline(str(capture_root))

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
                    with self.assertRaisesRegex(src.NhdWaterbodyError,
                                                "disk_floor_before_acquisition"):
                        src.capture_nhd_waterbody(_staging(root), version="floor-run",
                                                  capture_directory=str(root / "capture"))
            self.assertEqual(fetches["count"], 0)
            self.assertFalse((root / "capture").exists())
            self.assertFalse(any((root / "staging").iterdir()))

    def test_complete_capture_resume_skips_the_acquisition_floor(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                first = src.capture_nhd_waterbody(storage, version="floor-resume-1",
                                                  capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                # The retained tail page makes the resumed walk pure-local, so
                # the acquisition floor must not gate a sealed reuse that only
                # revalidates and rewrites the candidate parquet in place.
                with patch.object(src.shutil, "disk_usage",
                                  return_value=SimpleNamespace(free=src.MIN_FREE_BYTES - 1)):
                    second = src.capture_nhd_waterbody(storage, version="floor-resume-2",
                                                       capture_directory=str(capture_root))
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])

    def test_complete_fixture_capture_materializes_and_repeats(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                first = src.capture_nhd_waterbody(storage, version="fixture-run",
                                                  capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                self.assertFalse(first["source_release_accepted"])
                second = src.capture_nhd_waterbody(storage, version="fixture-run-2",
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
            self.assertEqual(table.column("id3dhp").to_pylist(),
                             [row["attributes"]["id3dhp"] for row in rows])
            geo_metadata = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                             ["Polygon Z", "MultiPolygon Z"])

            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            from dagster import DagsterInstance, materialize
            with fixture_mode(schema_sha, transport, page_size=2):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    result = materialize(
                        [nhd_waterbody, nhd_waterbody_complete],
                        resources={"staging_storage": _staging(root)},
                        instance=instance,
                        run_config={"ops": {"ingest__nhd_waterbody__nhd_waterbody": {"config": {
                            "capture_directory": str(capture_root)}}}},
                    )
                    self.assertTrue(result.success)
                    checks = {check.check_name: check
                              for check in result.get_asset_check_evaluations()}
                    check = checks["nhd_waterbody_complete"]
                    self.assertTrue(check.passed)
                    candidate = result.output_for_node("ingest__nhd_waterbody__nhd_waterbody")
                    self.assertEqual(candidate["feature_count"], FIXTURE_COUNT)
                    self.assertTrue(candidate["reused"])

    def test_retained_geoparquet_seals_into_staging(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                result = src.capture_nhd_waterbody(_staging(root), version="seal-run",
                                                   capture_directory=str(capture_root))
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            sealed = json.loads((root / "staging" / prefix / "metadata"
                                 / "source_audit.json").read_bytes())
            self.assertEqual(sealed["source_feature_count"], FIXTURE_COUNT)
            self.assertTrue(sealed["baseline_pending_complete_capture"])
            self.assertEqual(sealed["pagination"]["mode"],
                             "objectid_keyset_asc_with_bisection")
            self.assertTrue(sealed["pagination"]["resumable"])
            self.assertFalse(sealed["release_accepted"])
            manifest = json.loads((root / "staging" / prefix / "metadata"
                                   / "source_manifest.json").read_bytes())
            self.assertFalse(manifest["approved"])
            self.assertEqual(manifest["agency"], "U.S. Geological Survey")
            hosts = {entry["host"] for entry in manifest["inputs"]}
            self.assertTrue(all(host in ("3dhp.nationalmap.gov",
                                         "hydro.nationalmap.gov", "www.usgs.gov")
                                for host in hosts))
            # Per-page feature-query receipts stay staged verbatim as raw/
            # objects and are represented in the manifest by exactly one
            # aggregate input (the catalog reads the manifest under a bounded
            # layer limit), while every document and refused-attributes
            # receipt keeps its own individual provenance entry.
            aggregate = [entry for entry in manifest["inputs"]
                         if entry["input_id"] == "3dhp_paged_feature_queries"]
            self.assertEqual(len(aggregate), 1)
            page_receipts = sorted(capture_root.glob("page_*.receipt.json"))
            self.assertGreater(len(page_receipts), 1)
            self.assertEqual(aggregate[0]["page_query_count"], len(page_receipts))
            roster = [json.loads(path.read_bytes())
                      for path in page_receipts]
            self.assertEqual(aggregate[0]["sha256"], src._sha(src._canonical(roster)))
            self.assertEqual(aggregate[0]["bytes"],
                             sum(entry["bytes"] for entry in roster))
            self.assertFalse(any(
                entry["input_id"].startswith("page_") for entry in manifest["inputs"]
                if entry["input_id"] != "3dhp_paged_feature_queries"))
            self.assertIn("service_doc",
                          {entry["input_id"] for entry in manifest["inputs"]})
            self.assertTrue(all(
                (root / "staging" / prefix / "raw" / f"{path.name.removesuffix('.receipt.json')}.receipt.json").is_file()
                for path in page_receipts))
            parquet_bytes = (root / "staging" / prefix / "geoparquet"
                             / f"{src.DATASET_SLUG}.parquet").read_bytes()
            self.assertEqual(pq.read_table(io.BytesIO(parquet_bytes)).num_rows, FIXTURE_COUNT)

    def test_dense_head_bisects_to_served_sizes(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        # The producer refuses every page larger than one record (the dense-head
        # behaviour observed on the real layer at 2,500..50 records); the walk
        # must bisect 2 -> 1 instead of retrying or aborting.
        transport = make_responder(rows, max_served_size=1)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                result = src.capture_nhd_waterbody(_staging(root), version="bisect-run",
                                                   capture_directory=str(capture_root))
                self.assertEqual(result["feature_count"], FIXTURE_COUNT)
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                self.assertTrue(all(entry["size"] == 1 for entry in state["pages"]))
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
                self.assertFalse(any(
                    entry["name"].startswith("page_tail") for entry in state["pages"]))

    def test_transient_error_page_bisects_and_witnesses_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        base = make_responder(rows)
        injected = {"error_sent": False}

        def responder(request: httpx.Request) -> httpx.Response:
            query = parse_qs(urlparse(str(request.url)).query)
            if ("where" in query and query["where"][0].startswith("OBJECTID>")
                    and query.get("returnGeometry") == ["true"]
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
                storage = _staging(root)
                sealed = src.capture_nhd_waterbody(storage, version="witness-run",
                                                   capture_directory=str(capture_root))
                self.assertEqual(sealed["feature_count"], FIXTURE_COUNT)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                # The witnessed refusal bisected the first position to a single
                # record; the remaining positions kept the producer page size.
                self.assertEqual([entry["size"] for entry in state["pages"]], [1, 2, 2])
                self.assertFalse(state["refused"])
                # A resumed fetch is pure-local and recomputes the same baseline.
                baseline_before = json.loads(
                    (capture_root / "computed_baseline.json").read_bytes())
                again = src.fetch_nhd_waterbody_pages(capture_root)
                self.assertTrue(again["complete"])
                self.assertEqual(again["source_record_sha256"],
                                 baseline_before["source_record_sha256"])

    def test_producer_refused_record_is_gated_to_null_geometry(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_geometry_oids={5})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                result = src.capture_nhd_waterbody(storage, version="refused-run",
                                                   capture_directory=str(capture_root))
                self.assertEqual(result["feature_count"], FIXTURE_COUNT)
                self.assertEqual(result["source_null_geometry_count"], 1)
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                self.assertEqual(sorted(map(int, state["refused"])), [5])
                self.assertTrue((capture_root / "refused_attrs_000000005.body").exists())
                self.assertTrue(sorted(capture_root.glob("refused_probe_000000004_geo_*")))
                baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
                self.assertEqual(baseline["source_refused_ids"], [5])
                parquet = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet")
                self.assertEqual(parquet.column("OBJECTID").to_pylist(),
                                 list(FIXTURE_OBJECTIDS))
                self.assertEqual(parquet.column("geometry").to_pylist()[3], None)
                # Pin the refusal set and the null count from the baseline: the
                # capture then re-verifies the pinned refusal evidence.
                with patch.object(src, "EXPECTED_REFUSED_IDS", (5,)), \
                        patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", 1):
                    pinned = src.capture_nhd_waterbody(storage, version="refused-pin",
                                                       capture_directory=str(capture_root))
                    self.assertTrue(pinned["reused"])
                with patch.object(src, "EXPECTED_REFUSED_IDS", (6,)):
                    with self.assertRaisesRegex(src.NhdWaterbodyError,
                                                "source_refused_ids_drift"):
                        src.capture_nhd_waterbody(storage, version="refused-drift",
                                                  capture_directory=str(capture_root))
                with patch.object(src, "EXPECTED_REFUSED_IDS", (5,)), \
                        patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", 0):
                    with self.assertRaisesRegex(src.NhdWaterbodyError,
                                                "source_null_geometry_count_drift"):
                        src.capture_nhd_waterbody(storage, version="refused-null-drift",
                                                  capture_directory=str(capture_root))
                quarantined = sorted((root / "staging" / "_quarantine" / src.DATASET_SLUG)
                                     .glob("*"))
                self.assertTrue(quarantined)
                failure = json.loads((quarantined[-1] / "metadata" / "failure.json").read_bytes())
                self.assertIn(failure["reason_code"],
                              ("source_refused_ids_drift", "source_null_geometry_count_drift"))

    def test_alias_host_rescue_keeps_record_as_served_page(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        primary_transport = make_responder(rows, refuse_geometry_oids={5})
        alias_transport = make_responder(rows)
        clients = [httpx.Client(transport=primary_transport),
                   httpx.Client(transport=alias_transport)]
        rotation = {"index": 0}

        def rotating_client():
            client = clients[rotation["index"] % 2]
            rotation["index"] += 1
            return client

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, primary_transport, page_size=2):
                with patch.object(src, "_default_client", side_effect=rotating_client):
                    storage = _staging(root)
                    result = src.capture_nhd_waterbody(storage, version="rescue-run",
                                                       capture_directory=str(capture_root))
                    self.assertEqual(result["feature_count"], FIXTURE_COUNT)
                    self.assertEqual(result["source_null_geometry_count"], 0)
                    state = json.loads((capture_root / "capture_state.json").read_bytes())
                    self.assertFalse(state["refused"])
                    rescued = [entry for entry in state["pages"]
                               if entry.get("rescued_host")]
                    self.assertEqual(len(rescued), 1)
                    self.assertEqual(rescued[0]["rescued_host"], "alias")
                    parquet = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet")
                    self.assertIsNotNone(parquet.column("geometry").to_pylist()[3])

    def test_refused_identity_and_attrs_drift_are_refused(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        # The producer refuses even the attributes-only identity probe for the
        # record after the refused window: the walk cannot name the record.
        base = make_responder(rows, refuse_geometry_oids={5})

        def identity_blackhole(request: httpx.Request) -> httpx.Response:
            query = parse_qs(urlparse(str(request.url)).query)
            if ("where" in query and query["where"][0].startswith("OBJECTID>")
                    and query.get("returnGeometry") != ["true"]
                    and query.get("resultRecordCount") == ["1"]
                    and "returnCountOnly" not in query and query["where"][0] == "OBJECTID>3"):
                return httpx.Response(200, stream=JSONStream(
                    {"objectIdFieldName": "OBJECTID", "features": []}),
                    headers={"content-type": "application/json"})
            return base.handler(request)

        mismatched = make_responder(rows, refuse_geometry_oids={5})

        def attrs_mismatch(request: httpx.Request) -> httpx.Response:
            query = parse_qs(urlparse(str(request.url)).query)
            if query.get("objectIds") == ["5"] and query.get("returnGeometry") != ["true"]:
                return httpx.Response(200, stream=JSONStream(
                    {"objectIdFieldName": "OBJECTID", "features": [
                        {"attributes": dict(rows[0]["attributes"], OBJECTID=6)}]}),
                    headers={"content-type": "application/json"})
            return mismatched.handler(request)

        cases = {
            "source_refused_record_identity_unavailable":
                httpx.MockTransport(identity_blackhole),
            "source_refused_attrs_record_drift": httpx.MockTransport(attrs_mismatch),
        }
        for expected_error, transport in cases.items():
            with self.subTest(expected_error):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    with fixture_mode(schema_sha, transport, page_size=2):
                        with self.assertRaisesRegex(src.NhdWaterbodyError, expected_error):
                            src.capture_nhd_waterbody(
                                _staging(root), version="refusal-drift-run",
                                capture_directory=str(root / "capture"))

    def test_resume_reuses_retained_pages_and_discards_torn_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                src.capture_nhd_waterbody(storage, version="resume-run",
                                          capture_directory=str(capture_root))
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                self.assertEqual(len(state["pages"]), 3)
                # Simulate an interruption after the final data page: drop the tail witness.
                for suffix in ("body", "wire", "receipt.json"):
                    (capture_root / f"page_tail.{suffix}").unlink()
                state["tail"] = None
                state["complete"] = False
                (capture_root / "capture_state.json").write_bytes(src._canonical(state) + b"\n")
                fetches = {"count": 0}
                real_send = httpx.Client.send

                def counting_send(client, request, **kwargs):
                    fetches["count"] += 1
                    return real_send(client, request, **kwargs)

                with patch.object(httpx.Client, "send", counting_send):
                    result = src.fetch_nhd_waterbody_pages(capture_root)
                self.assertTrue(result["complete"])
                self.assertLessEqual(fetches["count"], 6)
                self.assertTrue((capture_root / "page_tail.body").exists())
                # A torn retained response is never trusted without a fetch client.
                (capture_root / "count_after.body").write_bytes(b'{"count":999999999}')
                with self.assertRaisesRegex(src.NhdWaterbodyError,
                                            "retained_capture_receipt_invalid"):
                    src._retained_or_fetch(None, capture_root, "count_after", src._count_url())
                # With a fetch client the torn response is discarded and refetched.
                with patch.object(httpx.Client, "send", counting_send):
                    src.fetch_nhd_waterbody_pages(capture_root)
                self.assertTrue((capture_root / "page_tail.body").exists())

    def test_pinned_baseline_drift_is_refused_and_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                src.capture_nhd_waterbody(storage, version="pin-run",
                                          capture_directory=str(capture_root))
                baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
                with patch.object(src, "EXPECTED_RECORD_SHA256",
                                  baseline["source_record_sha256"]), \
                        patch.object(src, "EXPECTED_GEOMETRY_SHA256",
                                     baseline["source_geometry_sha256"]), \
                        patch.object(src, "EXPECTED_IDS_SHA256",
                                     baseline["source_ids_sha256"]):
                    again = src.capture_nhd_waterbody(storage, version="pin-run-verify",
                                                      capture_directory=str(capture_root))
                    self.assertTrue(again["reused"])
                with patch.object(src, "EXPECTED_IDS_SHA256", "0" * 64):
                    with self.assertRaisesRegex(src.NhdWaterbodyError,
                                                "source_id_roster_drift"):
                        src.capture_nhd_waterbody(storage, version="pin-run-drift",
                                                  capture_directory=str(capture_root))
                quarantined = sorted((root / "staging" / "_quarantine" / src.DATASET_SLUG)
                                     .glob("*"))
                self.assertTrue(quarantined)
                failure = json.loads((quarantined[-1] / "metadata" / "failure.json").read_bytes())
                self.assertEqual(failure["reason_code"], "source_id_roster_drift")

    def test_null_geometry_contract_is_enforced(self):
        rows = [{**row, "geometry": None} for row in _fixture_rows()]
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport, page_size=2):
                with patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", 0):
                    with self.assertRaisesRegex(src.NhdWaterbodyError,
                                                "source_null_geometry_count_drift"):
                        src.capture_nhd_waterbody(_staging(root), version="null-geometry-run",
                                                  capture_directory=str(root / "capture"))
                # When the served null count matches its pin the capture passes.
                with patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", FIXTURE_COUNT):
                    pinned = src.capture_nhd_waterbody(
                        _staging(root), version="null-geometry-pinned",
                        capture_directory=str(root / "capture"))
                self.assertEqual(pinned["feature_count"], FIXTURE_COUNT)


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
            "usgs_3dhp_service_identity_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "copyrightText": "Data refreshed October, 2026."}),
            "usgs_3dhp_service_layer_roster_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "layers": _fixture_service_doc()["layers"][:-1]}),
            "usgs_3dhp_service_description_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "serviceDescription": "legacy NHD service."}),
            "exact_3dhp_waterbody_layer_metadata_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()),
                       "geometryType": "esriGeometryPolyline"}),
            "source_layer_z_or_m_contract_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "hasM": True}),
            "source_record_limit_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "maxRecordCount": 2000}),
            "source_layer_extent_drift": dict(
                layer=lambda: {**_fixture_layer_doc(_fixture_layer_fields()),
                               "extent": {**src.EXPECTED_LAYER_EXTENT,
                                          "xmax": 1.0,
                                          "spatialReference": dict(src.NATIVE_LAYER_SR)}}),
            "source_page_transfer_truncated": dict(
                truncate_short_page=True),
            "unexpected_source_geometry_members": dict(
                geometry=lambda row: {**_fixture_geometry(row["attributes"]["OBJECTID"]),
                                      "curves": []}),
            "source_geometry_z_flag_missing": dict(
                geometry=lambda row: {"hasZ": False,
                                      "rings": _fixture_geometry(
                                          row["attributes"]["OBJECTID"])["rings"]}),
            "source_polygon_ring_not_closed": dict(
                geometry=lambda row: {"hasZ": True, "rings": [
                    ring[:-1] for ring
                    in _fixture_geometry(row["attributes"]["OBJECTID"])["rings"]]}),
            "source_polygon_ordinate_contract_drift": dict(
                geometry=lambda row: {"hasZ": True, "rings": [
                    [vertex[:2] for vertex in ring] for ring
                    in _fixture_geometry(row["attributes"]["OBJECTID"])["rings"]]}),
            "source_coordinate_range_drift": dict(
                geometry=lambda row: {"hasZ": True, "rings": [
                    [[200.0, vertex[1], vertex[2]] for vertex in ring] for ring
                    in _fixture_geometry(row["attributes"]["OBJECTID"])["rings"]]}),
            "source_object_ids_not_strictly_increasing": dict(rows=[
                dict(rows[0]), dict(rows[0])], count=FIXTURE_COUNT),
            "source_object_id_type_changed": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "OBJECTID": None}},
                *rows[1:]]),
            "source_native_type_changed:gnisid": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "gnisid": "694899"}},
                *rows[1:]]),
            "source_native_type_changed:areasqkm": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"],
                                           "areasqkm": float("nan")}},
                *rows[1:]]),
            "source_id3dhp_value_drift": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "id3dhp": "not base36!"}},
                *rows[1:]]),
            "source_record_schema_or_geometry_drift": dict(rows=[
                {**rows[0], "attributes": {"OBJECTID": 1, "id3dhp": "1"}},
                *rows[1:]]),
            "source_featuretype_label_drift:2": dict(rows=[
                {**rows[1], "attributes": {**rows[1]["attributes"],
                                           "featuretypelabel": "Pond"}},
                *rows[:1], *rows[2:]]),
            "usgs_service_documentation_link_drift": dict(
                doc_page=b"<html>an unrelated page without the service host</html>"),
        }
        for expected_error, kwargs in cases.items():
            with self.subTest(expected_error):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    capture_root = root / "capture"
                    truncate_short_page = kwargs.pop("truncate_short_page", False)
                    case_rows = kwargs.pop("rows", rows)
                    base_transport = make_responder(case_rows, **kwargs)

                    def responder(request: httpx.Request, _base=base_transport,
                                  _truncate=truncate_short_page) -> httpx.Response:
                        query = parse_qs(urlparse(str(request.url)).query)
                        response = _base.handler(request)
                        if _truncate and "where" in query \
                                and query["where"][0].startswith("OBJECTID>") \
                                and query.get("returnGeometry") == ["true"] \
                                and "returnCountOnly" not in query:
                            payload = json.loads(response.read())
                            if 0 < len(payload["features"]) < src.PAGE_SIZE:
                                payload["exceededTransferLimit"] = True
                            return httpx.Response(200, stream=JSONStream(payload),
                                                  headers={"content-type": "application/json"})
                        return response

                    transport = httpx.MockTransport(responder)
                    with fixture_mode(schema_sha, transport, page_size=2):
                        storage = _staging(root)
                        with self.assertRaisesRegex(src.NhdWaterbodyError, expected_error):
                            src.capture_nhd_waterbody(storage, version="drift-run",
                                                      capture_directory=str(capture_root))
                    quarantined = sorted(
                        (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
                    self.assertTrue(quarantined, f"no quarantine for {expected_error}")
                    failure = json.loads((quarantined[-1] / "metadata" / "failure.json")
                                         .read_bytes())
                    self.assertTrue(failure["reason_code"])

    def test_short_population_is_incomplete(self):
        rows = _fixture_rows()[:4]
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, count=FIXTURE_COUNT)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport, page_size=2):
                with self.assertRaisesRegex(src.NhdWaterbodyError,
                                            "source_page_population_incomplete"):
                    src.capture_nhd_waterbody(_staging(root), version="short-run",
                                              capture_directory=str(root / "capture"))


if __name__ == "__main__":
    unittest.main()

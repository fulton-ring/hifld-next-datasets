"""Focused contract checks for the FEMA NFHL Flood Hazard Zones bulk-database source adapter."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import httpx
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import nfhl_flood_hazard_zones as source

STATIC_BASE = "https://hazards.fema.gov/nfhlv2/output/County/"

FIELDS = ["DFIRM_ID", "VERSION_ID", "FLD_AR_ID", "STUDY_TYP", "FLD_ZONE", "ZONE_SUBTY",
          "SFHA_TF", "STATIC_BFE", "V_DATUM", "DEPTH", "LEN_UNIT", "VELOCITY", "VEL_UNIT",
          "AR_REVERT", "AR_SUBTRV", "BFE_REVERT", "DEP_REVERT", "DUAL_ZONE", "SOURCE_CIT"]


def _attrs(dfirm_id, fld_ar_id, fld_zone="AE", zone_subty=None, bfe=-9999.0):
    return {"DFIRM_ID": dfirm_id, "VERSION_ID": "1.1.1.0", "FLD_AR_ID": fld_ar_id,
            "STUDY_TYP": "NT", "FLD_ZONE": fld_zone, "ZONE_SUBTY": zone_subty,
            "SFHA_TF": "T" if fld_zone in ("A", "AE", "AH", "AO", "VE") else "N",
            "STATIC_BFE": bfe, "V_DATUM": "NAVD88" if bfe != -9999.0 else None,
            "DEPTH": -9999.0, "LEN_UNIT": None, "VELOCITY": -9999.0, "VEL_UNIT": None,
            "AR_REVERT": None, "AR_SUBTRV": None, "BFE_REVERT": -9999.0,
            "DEP_REVERT": -9999.0, "DUAL_ZONE": None, "SOURCE_CIT": fld_ar_id + "_CIT"}


def _center(text: str) -> str:
    return f'<center class="txt12">{text}</center>'


def _row(item_id: str, county: str, state: str, java_date: str, size: str, file_name: str) -> str:
    link = (f"Download/ProductsDownLoadServlet?DFIRMID={item_id[:-5]}&state={state}"
            f"&county={county}&fileName={file_name}")
    return (
        '<tr>\n'
        f'<td scope="row">{_center(item_id)}</td>\n'
        '<td>\n'
        f'{_center(f"<span style=\"display:none;\">{county} SORT</span>\n{county}")}\n'
        '</td>\n'
        f'<td>{_center(state)}</td>\n'
        '<td>\n'
        f'{_center(f"<span style=\"display:none;\">{java_date}</span>\n{java_date}")}\n'
        '</td>\n'
        f'<td>{_center(size)}</td>\n'
        '<td>\n<center>\n'
        f'<a href="{link}" tabindex="1"><img src="/femaportal/NFHL/img/ZIPicon.png" '
        'width="32" height="32" alt="Download ZIP File" /></a>\n</center>\n</td>\n</tr>')


PRODUCT_P1 = ("01001C-NFHL", "AUTAUGA COUNTY", "ALABAMA",
              "Mon Dec 25 19:32:41 EST 2023", "94MB", "01001C_20231225.zip")
PRODUCT_P2 = ("01003C-NFHL", "BALDWIN COUNTY", "ALABAMA",
              "Sun Sep 27 08:00:00 CDT 2020", "150MB", "01003C_20200927.zip")
PRODUCT_P3 = ("560077-NFHL", "TOWN OF MOORCROFT", "WYOMING",
              "Fri Feb  2 10:15:30 MST 2007", "20KB", "560077_20070202.zip")
PRODUCT_P4 = ("47167C-NFHL", "LYMAN COUNTY", "SOUTH DAKOTA",
              "Fri Oct 21 00:00:00 EDT 2011", "12MB", "47167C_20190828.zip")
PRODUCTS = [PRODUCT_P1, PRODUCT_P2, PRODUCT_P3, PRODUCT_P4]


def _listing_html(rows) -> bytes:
    body = (f'<html><head><title>FEMA Map Service Center - National Flood Hazard Layer '
            f'(NFHL) Database Search Results</title></head><body>'
            f'<table id="itemTable2">{"".join(rows)}</table></body></html>')
    return body.encode()


def _default_listing() -> bytes:
    return _listing_html([_row(*product) for product in PRODUCTS])


def _exterior(x, y, dx, dy):
    return shapely.geometry.Polygon([(x, y), (x + dx, y), (x + dx, y + dy), (x, y + dy), (x, y)])


def _polygon_with_hole():
    exterior = _exterior(-86.5, 32.5, 0.4, 0.3)
    hole = shapely.geometry.Polygon([(-86.35, 32.6), (-86.3, 32.6), (-86.3, 32.65),
                                     (-86.35, 32.65), (-86.35, 32.6)]).exterior
    return shapely.geometry.Polygon(exterior.exterior, [hole])


def _multipolygon():
    return shapely.geometry.MultiPolygon([_exterior(-87.5, 32.5, 0.2, 0.2),
                                          _exterior(-87.0, 32.8, 0.15, 0.15)])


def _geodataframe(dfirm_id, geometries, ids):
    import geopandas as gpd
    rows = []
    for index, geometry in enumerate(geometries):
        values = _attrs(dfirm_id, ids[index])
        rows.append(values)
    frame = gpd.GeoDataFrame(rows, geometry=geometries, crs="EPSG:4269")
    return frame[list(FIELDS) + ["geometry"]]


def _zip_bytes(directory: Path, file_name: str, frame) -> bytes:
    work = directory / file_name.replace(".zip", "")
    work.mkdir(parents=True, exist_ok=True)
    frame.to_file(work / "S_FLD_HAZ_AR.shp", driver="ESRI Shapefile")
    zip_path = directory / file_name
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for member in sorted(work.iterdir()):
            archive.write(member, arcname=member.name)
    return zip_path.read_bytes()


def _line_frame(dfirm_id):
    import geopandas as gpd
    geometry = shapely.geometry.LineString([(-100.5, 44.5), (-100.4, 44.6)])
    frame = gpd.GeoDataFrame([_attrs(dfirm_id, "LINE_1")], geometry=[geometry], crs="EPSG:4269")
    return frame[list(FIELDS) + ["geometry"]]


def _build_fixture_zips(root: Path) -> dict[str, bytes]:
    root.mkdir(parents=True, exist_ok=True)
    zips = {}
    zips[PRODUCT_P1[5]] = _zip_bytes(root, PRODUCT_P1[5],
                                     _geodataframe("01001C",
                                                   [_polygon_with_hole(), _multipolygon()],
                                                   ["11916A3_2001", "11916A3_2002"]))
    frame = _geodataframe("01003C", [_exterior(-87.9, 30.5, 0.3, 0.2), None], ["11916C3_2011", "11916C3_2012"])
    zips[PRODUCT_P2[5]] = _zip_bytes(root, PRODUCT_P2[5], frame)
    zips[PRODUCT_P3[5]] = _zip_bytes(root, PRODUCT_P3[5],
                                     _geodataframe("560077", [_exterior(-104.95, 44.6, 0.05, 0.04)],
                                                   ["12007B3_1990"]))
    work = root / PRODUCT_P4[5].replace(".zip", "")
    work.mkdir(parents=True, exist_ok=True)
    _line_frame("47167C").to_file(work / "S_FLD_HAZ_LN.shp", driver="ESRI Shapefile")
    zip_path = root / PRODUCT_P4[5]
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for member in sorted(work.iterdir()):
            archive.write(member, arcname=member.name)
    zips[PRODUCT_P4[5]] = zip_path.read_bytes()
    return zips


class _Router:
    """Mock transport routing the listing, the servlet redirects and the static zips."""

    def __init__(self, listing_body: bytes, zips: dict[str, bytes], *,
                 static_failures: int = 0, succeed_max: int | None = None,
                 listing_bodies=None, redirect_host: str | None = None,
                 serve_404_for=None):
        self.listing_body = listing_body
        self.listing_bodies = listing_bodies
        self.zips = zips
        self.static_failures = static_failures
        self.succeed_max = succeed_max
        self.static_downloads = 0
        self.redirect_host = redirect_host
        self.serve_404_for = serve_404_for or set()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == source.LISTING_URL:
            if self.listing_bodies:
                return httpx.Response(200, content=self.listing_bodies.pop(0))
            return httpx.Response(200, content=self.listing_body)
        if url.startswith(source.SERVLET_URL):
            params = dict(request.url.params)
            if self.redirect_host is not None:
                location = f"{self.redirect_host}{params['fileName']}"
            else:
                location = STATIC_BASE + params["fileName"]
            return httpx.Response(302, headers={"Location": location})
        if url.startswith(STATIC_BASE):
            file_name = url.rsplit("/", 1)[-1]
            if file_name in self.serve_404_for:
                return httpx.Response(404, content=b"not found")
            self.static_downloads += 1
            if self.static_downloads <= self.static_failures:
                raise httpx.ConnectError("connection reset by peer", request=request)
            if self.succeed_max is not None and self.static_downloads > self.succeed_max:
                raise httpx.ConnectError("connection reset by peer", request=request)
            body = self.zips[file_name]
            return httpx.Response(200, content=body, headers={"Content-Length": str(len(body))})
        return httpx.Response(404, content=b"unrouted")


def _client_factory(router: _Router):
    original = httpx.Client

    def factory(*args, **kwargs):
        return original(*args, transport=httpx.MockTransport(router), **kwargs)

    return factory


def _fake_capture_get(router: _Router):
    """Emulate the shared capture_get for the listing brackets only.

    The shared helper captures the raw wire stream, which MockTransport
    responses do not expose; the redirect/zip flow uses plain client streams
    and stays on the real client. The fake writes the same body/wire/receipt
    evidence files the real helper writes.
    """

    def fake(client, url, capture_root, name, *, max_bytes=source.LISTING_MAX_BYTES, **kwargs):
        if url != source.LISTING_URL:
            raise AssertionError("unexpected capture_get url: " + url)
        body = router.listing_bodies.pop(0) if router.listing_bodies else router.listing_body
        receipt = {"method": "GET", "url": url, "final_url": url,
                   "observed_at": "2026-11-12T00:00:00Z", "status": 200,
                   "complete_body": True, "wire_bytes": len(body),
                   "wire_sha256": hashlib.sha256(body).hexdigest(),
                   "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                   "content_encoding": "identity", "content_length": None,
                   "error_code": None, "error_type": None}
        capture_root = Path(capture_root)
        (capture_root / f"{name}.body").write_bytes(body)
        (capture_root / f"{name}.wire").write_bytes(body)
        (capture_root / f"{name}.receipt.json").write_bytes(
            json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        return body, receipt

    return fake


def _speed_pins():
    return {"LISTING_BASE_DELAY_SECONDS": 0.0, "ZIP_BASE_DELAY_SECONDS": 0.0}


def _storage(root: Path) -> source.StagingStorageResource:
    return source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")


def _run_capture(storage, root: Path, router: _Router, *, pins=None, version="vfixture"):
    patchers = [patch.object(source.httpx, "Client", side_effect=_client_factory(router)),
                patch.object(source, "capture_get", side_effect=_fake_capture_get(router)),
                patch.multiple(source, **_speed_pins())]
    if pins:
        patchers.append(patch.multiple(source, **pins))
    for patcher in patchers:
        patcher.start()
    try:
        return source.capture_nfhl_flood_hazard_zones(
            storage, version=version, capture_directory=str(root / "captures"))
    finally:
        for patcher in reversed(patchers):
            patcher.stop()


def _attempt(root: Path, version="vfixture") -> Path:
    return next(path for path in (root / "captures").iterdir()
                if path.name.startswith(version + "-"))


def _pins_from_contract(contract: dict) -> dict:
    return {"EXPECTED_PRODUCT_COUNT": contract["expected_product_count"],
            "EXPECTED_PRODUCT_ROSTER_SHA256": contract["expected_product_roster_sha256"],
            "EXPECTED_COUNT": contract["expected_count"],
            "EXPECTED_ID_SHA256": contract["expected_id_sha256"],
            "EXPECTED_SCHEMA_SHA256": contract["expected_schema_sha256"],
            "EXPECTED_RECORD_SHA256": contract["expected_record_sha256"],
            "EXPECTED_GEOMETRY_SHA256": contract["expected_geometry_sha256"],
            "EXPECTED_OBSERVED_BOUNDS": contract["expected_observed_bounds"],
            "EXPECTED_NULL_GEOMETRY_COUNT": contract["expected_null_geometry_count"],
            "EXPECTED_NAN_GEOMETRY_COUNT": contract["expected_nan_geometry_count"]}


class NfhlFloodHazardZonesListingTests(unittest.TestCase):
    def test_parse_listing_builds_the_canonical_roster(self):
        products = source._parse_listing(_default_listing())
        self.assertEqual([product["item_id"] for product in products],
                         sorted(product[0] for product in PRODUCTS))
        self.assertEqual(products[0]["file_name"], PRODUCT_P1[5])
        self.assertEqual(products[0]["state"], "ALABAMA")
        self.assertEqual(products[0]["county"], "AUTAUGA COUNTY")
        # Roster order is canonical (sorted), independent of page order.
        self.assertEqual(source.product_roster_sha256(products),
                         source.product_roster_sha256(source._parse_listing(_default_listing())))

    def test_hidden_sort_name_and_display_county_are_distinct_fields(self):
        rows = [_row(*PRODUCT_P1)]
        rows[0] = rows[0].replace("AUTAUGA COUNTY SORT", "SORT: AUTAUGA")
        products = source._parse_listing(_listing_html(rows))
        self.assertEqual(products[0]["county"], "AUTAUGA COUNTY")
        self.assertEqual(source._servlet_url(products[0]).split("county=")[1].split("&")[0],
                         "AUTAUGA%20COUNTY")

    def test_listing_update_date_and_product_file_date_are_distinct(self):
        # 47167C observed live 2026-11-12: listing update 2011-10-21, product
        # zip published 2019-08-28. Both are producer fields; both are kept.
        products = source._parse_listing(_default_listing())
        lyman = next(product for product in products if product["item_id"] == "47167C-NFHL")
        self.assertEqual(lyman["update_date_java"], PRODUCT_P4[3])
        self.assertEqual(source._update_date_iso(lyman), "2019-08-28")

    def test_java_day_padding_and_timezones_parse(self):
        products = source._parse_listing(_default_listing())
        moorcroft = next(product for product in products if product["item_id"] == "560077-NFHL")
        self.assertEqual(moorcroft["update_date_java"], "Fri Feb  2 10:15:30 MST 2007")

    def test_structural_surprises_fail_closed(self):
        rows = [_row(*product) for product in PRODUCTS]
        cases = [
            ("missing_marker", _listing_html(rows).replace(b"Database Search Results", b"other")),
            ("malformed_row", _listing_html(rows + [
                '<tr><td scope="row"><center class="txt12">01011C-NFHL</center></td>'
                '<td>Download/ProductsDownLoadServlet</td></tr>'])),
            ("duplicate_item", _listing_html(rows + [_row(*PRODUCT_P1)])),
            ("item_id_mismatch", _listing_html(
                [_row("01009X-NFHL", "AUTAUGA COUNTY", "ALABAMA", PRODUCT_P1[3], PRODUCT_P1[4], PRODUCT_P1[5])] + rows[1:])),
            ("date_span_missing", _listing_html(
                [_row(PRODUCT_P1[0], PRODUCT_P1[1], PRODUCT_P1[2], PRODUCT_P1[3], PRODUCT_P1[4], PRODUCT_P1[5])]
                + rows[1:]).replace(b'<span style="display:none;">Mon Dec 25 19:32:41 EST 2023</span>', b'')),
            ("bad_size", _listing_html(
                [_row(PRODUCT_P1[0], PRODUCT_P1[1], PRODUCT_P1[2], PRODUCT_P1[3], "94", PRODUCT_P1[5])] + rows[1:])),
            ("bad_file_name", _listing_html(
                [_row(PRODUCT_P1[0], PRODUCT_P1[1], PRODUCT_P1[2], PRODUCT_P1[3], PRODUCT_P1[4], "01001C_other.zip")] + rows[1:])),
            ("state_link_mismatch", _listing_html(
                [_row(*PRODUCT_P1)] + rows[1:]).replace(
                    b"state=ALABAMA&county=AUTAUGA", b"state=GEORGIA&county=AUTAUGA", 1)),
            ("county_link_mismatch", _listing_html(
                [_row(*PRODUCT_P1)] + rows[1:]).replace(
                    b"county=AUTAUGA COUNTY&fileName", b"county=ELMORE COUNTY&fileName", 1)),
            ("empty", _listing_html([])),
        ]
        for name, body in cases:
            with self.assertRaisesRegex(
                    ValueError, "source_listing_|source_product_roster_drift|official_fema_nfhl", msg=name):
                source._parse_listing(body)


class NfhlFloodHazardZonesCaptureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._fixture_dir = tempfile.TemporaryDirectory(prefix="fhz-zips-")
        cls.zips = _build_fixture_zips(Path(cls._fixture_dir.name))

    @classmethod
    def tearDownClass(cls):
        cls._fixture_dir.cleanup()

    def test_live_mode_capture_writes_pins_and_stages_nothing(self):
        with tempfile.TemporaryDirectory(prefix="fhz-live-") as temporary:
            root = Path(temporary)
            router = _Router(_default_listing(), self.zips)
            with self.assertRaisesRegex(source._PinsPendingError, "source_population_pins_not_established"):
                _run_capture(root, router)
            self.assertEqual(set(), _root_keys(root))
            attempt = _attempt(root)
            contract = json.loads((attempt / "population_pins.json").read_bytes())
            self.assertEqual(contract["expected_product_count"], 4)
            self.assertEqual(contract["expected_count"], 5)
            self.assertEqual(contract["expected_null_geometry_count"], 1)
            self.assertEqual(contract["expected_nan_geometry_count"], 0)
            for key in ("expected_product_roster_sha256", "expected_id_sha256",
                        "expected_schema_sha256", "expected_record_sha256",
                        "expected_geometry_sha256", "expected_observed_bounds"):
                self.assertIn(key, contract)
            manifest = json.loads((attempt / "capture_manifest.json").read_bytes())
            self.assertTrue(manifest["complete"])
            self.assertFalse(manifest["staged"])
            self.assertEqual(manifest["reason_code"], "source_population_pins_not_established")

    def test_pinned_capture_stages_candidate_and_preserves_values(self):
        with tempfile.TemporaryDirectory(prefix="fhz-pinned-") as temporary:
            root = Path(temporary)
            with self.assertRaises(source._PinsPendingError):
                _run_capture(root, _Router(_default_listing(), self.zips))
            contract = json.loads((_attempt(root) / "population_pins.json").read_bytes())
            router = _Router(_default_listing(), self.zips)
            candidate, storage = _run_capture(root, _Router(_default_listing(), self.zips),
                                              pins=_pins_from_contract(contract))
            self.assertFalse(candidate["reused"])
            self.assertEqual(candidate["feature_count"], 5)
            self.assertEqual(candidate["source_null_geometry_count"], 1)
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
            staged = set(storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/metadata/candidate_ready.json", staged)
            self.assertIn(f"{prefix}/geoparquet/{source.DATASET_SLUG}.parquet", staged)
            self.assertIn(f"{prefix}/raw/{PRODUCT_P1[0]}.zip", staged)
            self.assertIn(f"{prefix}/raw/{PRODUCT_P1[0]}.receipt.json", staged)
            self.assertIn(f"{prefix}/raw/listing_before.body", staged)
            self.assertIn(f"{prefix}/raw/listing_before.wire", staged)
            ready = json.loads(storage.read_key(f"{prefix}/metadata/candidate_ready.json"))
            self.assertFalse(ready["approved"])
            self.assertFalse(ready["release_accepted"])
            self.assertEqual(ready["source_record_sha256"], contract["expected_record_sha256"])
            manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
            inputs = {entry["input_id"]: entry for entry in manifest["inputs"]}
            self.assertEqual(len(inputs), 6)
            self.assertEqual(inputs[PRODUCT_P1[0]]["release_date"], "2023-12-25")
            self.assertEqual(inputs[PRODUCT_P1[0]]["http_status"], 200)
            self.assertNotIn(" ", inputs[PRODUCT_P1[0]]["retrieval_url"])
            parquet_path = root / "readback.parquet"
            storage.download_key_to(f"{prefix}/geoparquet/{source.DATASET_SLUG}.parquet", parquet_path)
            table = pq.read_table(parquet_path)
            self.assertEqual(table.num_rows, 5)
            self.assertEqual(table.column("DFIRM_ID").to_pylist(),
                             ["01001C", "01001C", "01003C", "01003C", "560077"])
            shapes = table.column("SHAPE").to_pylist()
            self.assertIsNone(shapes[3])
            geom = shapely.wkb.loads(shapes[0])
            self.assertEqual(geom.geom_type, "Polygon")
            self.assertEqual(len(list(geom.interiors)), 1)
            geom = shapely.wkb.loads(shapes[1])
            self.assertEqual(geom.geom_type, "MultiPolygon")
            self.assertEqual(geom.bounds[0], -87.5)
            self.assertEqual(table.column("STATIC_BFE").to_pylist()[4], -9999.0)
            # Staging consumed the capture-directory zip sources.
            self.assertFalse((_attempt(root) / f"{PRODUCT_P1[0]}.zip").exists())
            manifest_json = json.loads((_attempt(root) / "capture_manifest.json").read_bytes())
            self.assertTrue(manifest_json["complete"])
            self.assertTrue(manifest_json["staged"])

    def test_resume_downloads_only_missing_products(self):
        with tempfile.TemporaryDirectory(prefix="fhz-resume-") as temporary:
            root = Path(temporary)
            router = _Router(_default_listing(), self.zips)
            # Interrupt the first session after two product downloads.
            router.static_failures = 0
            listing_bodies = [_default_listing(), _default_listing()]
            with patch.object(source.httpx, "Client", side_effect=_client_factory(router)):
                storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
                # capture only the first two products, then simulate a crash by
                # running a router whose third product download raises forever.
                first = _Router(_default_listing(), self.zips, static_failures=0)
                crash = _Router(_default_listing(), self.zips)
                crash.static_downloads = 2  # next product download raises
                crash.static_failures = 10**9
                patcher = patch.object(source.httpx, "Client", side_effect=_client_factory(first))
                patcher.start()
                try:
                    storage  # noqa: B018
                    import shutil  # noqa: F401
                finally:
                    patcher.stop()
                # run once with a router that fails the 3rd download persistently
                with patch.multiple(source, **_speed_pins(), ZIP_ATTEMPTS=2):
                    with self.assertRaises(ValueError):
                        with patch.object(source.httpx, "Client", side_effect=_client_factory(crash)):
                            source.capture_nfhl_flood_hazard_zones(
                                storage, version="vfixture", capture_directory=str(root / "captures"))
            attempt = _attempt(root)
            done = {path.name for path in attempt.glob("*.zip")}
            self.assertEqual(len(done), 2)
            router = _Router(_default_listing(), self.zips)
            contract = json.loads((attempt / "population_pins.json").read_bytes()) \
                if (attempt / "population_pins.json").exists() else None
            self.assertIsNone(contract)
            candidate, storage = _run_capture(root, router,
                                              pins=self._live_pins(root))
            self.assertFalse(candidate["reused"])
            # Only the two missing products were downloaded in the second run.
            # (The first session's failures left no staged candidate; the pins
            # come from a separate completing live run below.)
            self.assertEqual(candidate["feature_count"], 5)

    def _live_pins(self, root: Path) -> dict:
        attempt = _attempt(root)
        path = attempt / "population_pins.json"
        if path.exists():
            return _pins_from_contract(json.loads(path.read_bytes()))
        with self.assertRaises(source._PinsPendingError):
            _run_capture(root, _Router(_default_listing(), self.zips))
        return _pins_from_contract(json.loads(path.read_bytes()))


class NfhlFloodHazardZonesCaptureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._fixture_dir = tempfile.TemporaryDirectory(prefix="fhz-zips-")
        cls.zips = _build_fixture_zips(Path(cls._fixture_dir.name))

    @classmethod
    def tearDownClass(cls):
        cls._fixture_dir.cleanup()

    def test_live_mode_capture_writes_pins_and_stages_nothing(self):
        with tempfile.TemporaryDirectory(prefix="fhz-live-") as temporary:
            root = Path(temporary)
            storage = _storage(root)
            with self.assertRaisesRegex(source._PinsPendingError, "source_population_pins_not_established"):
                _run_capture(storage, root, _Router(_default_listing(), self.zips))
            self.assertEqual(set(), set(storage.list_prefix()))
            attempt = _attempt(root)
            contract = json.loads((attempt / "population_pins.json").read_bytes())
            self.assertEqual(contract["expected_product_count"], 4)
            self.assertEqual(contract["expected_count"], 5)
            self.assertEqual(contract["expected_null_geometry_count"], 1)
            self.assertEqual(contract["expected_nan_geometry_count"], 0)
            for key in ("expected_product_roster_sha256", "expected_id_sha256",
                        "expected_schema_sha256", "expected_record_sha256",
                        "expected_geometry_sha256", "expected_observed_bounds"):
                self.assertIn(key, contract)
            manifest = json.loads((attempt / "capture_manifest.json").read_bytes())
            self.assertTrue(manifest["complete"])
            self.assertFalse(manifest["staged"])
            self.assertEqual(manifest["reason_code"], "source_population_pins_not_established")

    def test_pinned_capture_stages_candidate_and_preserves_values(self):
        with tempfile.TemporaryDirectory(prefix="fhz-pinned-") as temporary:
            root = Path(temporary)
            with self.assertRaises(source._PinsPendingError):
                _run_capture(_storage(root), root, _Router(_default_listing(), self.zips))
            contract = json.loads((_attempt(root) / "population_pins.json").read_bytes())
            storage = _storage(root)
            candidate = _run_capture(storage, root, _Router(_default_listing(), self.zips),
                                     pins=_pins_from_contract(contract))
            self.assertNotIn("reused", candidate)
            self.assertEqual(candidate["feature_count"], 5)
            self.assertEqual(candidate["source_null_geometry_count"], 1)
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
            staged = set(storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/metadata/candidate_ready.json", staged)
            self.assertIn(f"{prefix}/geoparquet/{source.DATASET_SLUG}.parquet", staged)
            self.assertIn(f"{prefix}/raw/{PRODUCT_P1[0]}.zip", staged)
            self.assertIn(f"{prefix}/raw/{PRODUCT_P1[0]}.receipt.json", staged)
            self.assertIn(f"{prefix}/raw/listing_before.body", staged)
            self.assertIn(f"{prefix}/raw/listing_before.wire", staged)
            ready = json.loads(storage.read_key(f"{prefix}/metadata/candidate_ready.json"))
            self.assertFalse(ready["approved"])
            self.assertFalse(ready["release_accepted"])
            self.assertEqual(ready["source_record_sha256"], contract["expected_record_sha256"])
            manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
            inputs = {entry["input_id"]: entry for entry in manifest["inputs"]}
            self.assertEqual(len(inputs), 6)
            self.assertEqual(inputs[PRODUCT_P1[0]]["release_date"], "2023-12-25")
            self.assertEqual(inputs[PRODUCT_P1[0]]["http_status"], 200)
            self.assertNotIn(" ", inputs[PRODUCT_P1[0]]["retrieval_url"])
            parquet_path = root / "readback.parquet"
            storage.download_key_to(f"{prefix}/geoparquet/{source.DATASET_SLUG}.parquet", parquet_path)
            table = pq.read_table(parquet_path)
            self.assertEqual(table.num_rows, 5)
            self.assertEqual(table.column("DFIRM_ID").to_pylist(),
                             ["01001C", "01001C", "01003C", "01003C", "560077"])
            shapes = table.column("SHAPE").to_pylist()
            self.assertIsNone(shapes[3])
            geom = shapely.wkb.loads(shapes[0])
            self.assertEqual(geom.geom_type, "Polygon")
            self.assertEqual(len(list(geom.interiors)), 1)
            geom = shapely.wkb.loads(shapes[1])
            self.assertEqual(geom.geom_type, "MultiPolygon")
            self.assertEqual(geom.bounds[0], -87.5)
            self.assertEqual(table.column("STATIC_BFE").to_pylist()[4], -9999.0)
            # Staging consumed the capture-directory zip sources.
            self.assertFalse((_attempt(root) / f"{PRODUCT_P1[0]}.zip").exists())
            manifest_json = json.loads((_attempt(root) / "capture_manifest.json").read_bytes())
            self.assertTrue(manifest_json["complete"])
            self.assertTrue(manifest_json["staged"])

    def test_resume_downloads_only_missing_products(self):
        with tempfile.TemporaryDirectory(prefix="fhz-resume-") as temporary:
            root = Path(temporary)
            storage = _storage(root)
            # Session 1: downloads two products, then the producer stops serving.
            with self.assertRaisesRegex(ValueError, "incomplete_source_capture:"):
                _run_capture(storage, root, _Router(_default_listing(), self.zips, succeed_max=2))
            attempt = _attempt(root)
            self.assertEqual({path.name for path in attempt.glob("*.zip") if ".attempt-" not in path.name},
                              {PRODUCT_P1[0] + ".zip", PRODUCT_P2[0] + ".zip"})
            self.assertFalse((attempt / "population_pins.json").exists())
            # Session 2: only the two missing products are downloaded; the
            # completing live run writes the pins contract.
            router = _Router(_default_listing(), self.zips)
            with self.assertRaises(source._PinsPendingError):
                _run_capture(storage, root, router)
            self.assertEqual(router.static_downloads, 2)
            contract = json.loads((attempt / "population_pins.json").read_bytes())
            # Session 3 (pinned): no downloads at all; candidate stages.
            router = _Router(_default_listing(), self.zips)
            candidate = _run_capture(storage, root, router, pins=_pins_from_contract(contract))
            self.assertEqual(router.static_downloads, 0)
            self.assertNotIn("reused", candidate)
            self.assertEqual(candidate["feature_count"], 5)

    def test_transient_transport_failure_retries_with_attempt_evidence(self):
        with tempfile.TemporaryDirectory(prefix="fhz-retry-") as temporary:
            root = Path(temporary)
            storage = _storage(root)
            router = _Router(_default_listing(), self.zips, static_failures=1)
            with self.assertRaises(source._PinsPendingError):
                _run_capture(storage, root, router)
            attempt = _attempt(root)
            self.assertTrue((attempt / f"{PRODUCT_P1[0]}.attempt-01.receipt.json").exists())
            self.assertTrue((attempt / f"{PRODUCT_P1[0]}.attempt-01.zip").exists())
            evidence = json.loads((attempt / f"{PRODUCT_P1[0]}.attempt-01.receipt.json").read_bytes())
            self.assertFalse(evidence["complete_body"])
            self.assertEqual(evidence["error_code"], "incomplete_source_capture")
            # every product was still captured exactly once
            self.assertEqual(router.static_downloads, 5)

    def test_persistent_zip_failure_fails_closed_after_bounded_retries(self):
        with tempfile.TemporaryDirectory(prefix="fhz-fail-") as temporary:
            root = Path(temporary)
            storage = _storage(root)
            router = _Router(_default_listing(), self.zips, serve_404_for={PRODUCT_P2[5]})
            with self.assertRaisesRegex(ValueError, "incomplete_source_capture:01003C-NFHL"):
                _run_capture(storage, root, router)
            attempt = _attempt(root)
            self.assertFalse((attempt / "population_pins.json").exists())
            self.assertEqual(set(), set(storage.list_prefix()))

    def test_redirect_off_the_producer_host_fails_closed(self):
        with tempfile.TemporaryDirectory(prefix="fhz-redirect-") as temporary:
            root = Path(temporary)
            storage = _storage(root)
            router = _Router(_default_listing(), self.zips,
                             redirect_host="https://mirror.example.com/mirror/")
            with self.assertRaisesRegex(ValueError, "source_download_redirect_host_unrecognized"):
                _run_capture(storage, root, router)
            self.assertEqual(set(), set(storage.list_prefix()))

    def test_corrupt_zip_crc_fails_closed(self):
        zips = dict(self.zips)
        zips[PRODUCT_P2[5]] = zips[PRODUCT_P2[5]][:200] + b"corrupted" + zips[PRODUCT_P2[5]][209:]
        with tempfile.TemporaryDirectory(prefix="fhz-crc-") as temporary:
            root = Path(temporary)
            storage = _storage(root)
            router = _Router(_default_listing(), zips)
            with self.assertRaisesRegex(ValueError, "incomplete_source_capture:01003C-NFHL"):
                _run_capture(storage, root, router)
            attempt = _attempt(root)
            attempts = sorted(path.name for path in attempt.glob("01003C-NFHL.attempt-*.zip"))
            self.assertEqual(len(attempts), source.ZIP_ATTEMPTS)

    def test_zip_over_the_capture_cap_fails_closed_without_retries(self):
        with tempfile.TemporaryDirectory(prefix="fhz-cap-") as temporary:
            root = Path(temporary)
            storage = _storage(root)
            router = _Router(_default_listing(), self.zips)
            with patch.multiple(source, PRODUCT_ZIP_MAX_BYTES=64):
                with self.assertRaisesRegex(ValueError, "source_product_zip_exceeds_capture_cap"):
                    _run_capture(storage, root, router)
            self.assertEqual(router.static_downloads, 1)

    def test_product_roster_drift_fails_closed_before_downloads(self):
        with tempfile.TemporaryDirectory(prefix="fhz-drift-") as temporary:
            root = Path(temporary)
            with self.assertRaises(source._PinsPendingError):
                _run_capture(_storage(root), root, _Router(_default_listing(), self.zips))
            attempt = _attempt(root)
            contract = json.loads((attempt / "population_pins.json").read_bytes())
            drifted = _listing_html([_row(*product) for product in PRODUCTS[:-1]]
                                    + [_row(PRODUCT_P4[0], PRODUCT_P4[1], PRODUCT_P4[2],
                                            PRODUCT_P4[3], PRODUCT_P4[4], "47167C_20190829.zip")])
            storage = _storage(root)
            router = _Router(drifted, self.zips)
            with self.assertRaisesRegex(ValueError, "source_product_roster_drift"):
                _run_capture(storage, root, router, pins=_pins_from_contract(contract))
            self.assertEqual(router.static_downloads, 0)
            self.assertEqual(set(), set(storage.list_prefix()))

    def test_listing_changed_during_capture_fails_closed(self):
        with tempfile.TemporaryDirectory(prefix="fhz-bracket-") as temporary:
            root = Path(temporary)
            storage = _storage(root)
            changed = _listing_html([_row(*product) for product in PRODUCTS[:3]])
            router = _Router(_default_listing(), self.zips, listing_bodies=[_default_listing(), changed])
            with self.assertRaisesRegex(ValueError, "source_listing_changed_during_capture"):
                _run_capture(storage, root, router)
            self.assertEqual(set(), set(storage.list_prefix()))

    def test_feature_class_schema_drift_fails_closed(self):
        zips = dict(self.zips)
        frame = _geodataframe("01003C", [_exterior(-87.9, 30.5, 0.3, 0.2)], ["11916C3_2011"])
        extra = frame.copy()
        extra["EXTRA_FIELD"] = "x"
        with tempfile.TemporaryDirectory(prefix="fhz-schema-") as workdir:
            work = Path(workdir)
            zips[PRODUCT_P2[5]] = _zip_bytes(work, PRODUCT_P2[5], extra)
            root = Path(workdir)
            storage = _storage(root)
            router = _Router(_default_listing(), zips)
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                _run_capture(storage, root, router)

    def test_crs_drift_fails_closed(self):
        zips = dict(self.zips)
        with tempfile.TemporaryDirectory(prefix="fhz-crs-") as workdir:
            work = Path(workdir)
            frame = _geodataframe("01003C", [_exterior(-87.9, 30.5, 0.3, 0.2)], ["11916C3_2011"]).to_crs(epsg=4326)
            zips[PRODUCT_P2[5]] = _zip_bytes(work, PRODUCT_P2[5], frame)
            root = Path(workdir)
            storage = _storage(root)
            router = _Router(_default_listing(), zips)
            with self.assertRaisesRegex(ValueError, "source_crs_drift"):
                _run_capture(storage, root, router)

    def test_product_without_polygon_feature_class_contributes_zero_features(self):
        with tempfile.TemporaryDirectory(prefix="fhz-zero-") as temporary:
            root = Path(temporary)
            storage = _storage(root)
            with self.assertRaises(source._PinsPendingError):
                _run_capture(storage, root, _Router(_default_listing(), self.zips))
            attempt = _attempt(root)
            contract = json.loads((attempt / "population_pins.json").read_bytes())
            self.assertEqual(contract["expected_count"], 5)
            self.assertEqual(contract["expected_product_count"], 4)


class NfhlFloodHazardZonesGeometryTests(unittest.TestCase):
    def test_null_and_nan_geometry_are_retained_and_gated(self):
        counters = {"null_geometry": 0, "nan_geometry": 0}
        bounds = [None, None, None, None]
        self.assertIsNone(source._validate_feature_geometry(None, bounds, counters))
        self.assertEqual(counters["null_geometry"], 1)
        polygon = _exterior(-86.5, 32.5, 0.4, 0.3)
        coordinates = np.asarray(shapely.get_coordinates(polygon), dtype="float64").copy()
        coordinates[1, 0] = np.nan
        wkb = shapely.wkb.dumps(shapely.set_coordinates(polygon, coordinates))
        self.assertIsNone(source._validate_feature_geometry(wkb, bounds, counters))
        self.assertEqual(counters["nan_geometry"], 1)
        self.assertIsNone(bounds[0], "NaN ordinates must not seed the bounds box")
        clean = shapely.wkb.dumps(_exterior(-86.5, 32.5, 0.4, 0.3))
        self.assertEqual(source._validate_feature_geometry(clean, bounds, counters), clean)
        self.assertEqual(bounds, [-86.5, 32.5, -86.1, 32.8])

    def test_z_geometry_fails_closed(self):
        counters = {"null_geometry": 0, "nan_geometry": 0}
        wkb = shapely.wkb.dumps(shapely.geometry.Polygon(
            [(-86.0, 32.5, 1.0), (-85.9, 32.5, 1.0), (-85.9, 32.6, 1.0), (-86.0, 32.6, 1.0),
             (-86.0, 32.5, 1.0)]))
        with self.assertRaisesRegex(ValueError, "source_z_or_m_geometry_unsupported"):
            source._validate_feature_geometry(wkb, [None, None, None, None], counters)

    def test_out_of_range_coordinates_fail_closed(self):
        counters = {"null_geometry": 0, "nan_geometry": 0}
        wkb = shapely.wkb.dumps(shapely.geometry.Polygon(
            [(-190.0, 32.5), (-86.0, 32.5), (-86.0, 32.6), (-190.0, 32.6), (-190.0, 32.5)]))
        with self.assertRaisesRegex(ValueError, "source_coordinate_range_drift"):
            source._validate_feature_geometry(wkb, [None, None, None, None], counters)


class NfhlFloodHazardZonesProducerTextTests(unittest.TestCase):
    """FEMA's own DBFs carry a few values that are not valid UTF-8.

    Live capture 2026-11-13: STUDY_TYP is a 38-byte field and FEMA's writer
    truncated 'Shaded Zone X with depths less than 1…' mid ellipsis in 6
    products (~10.6k records), leaving a lone 0xE2 lead byte; 51177C carries
    one CP1252 0x92 byte. The candidate retains those producer bytes
    verbatim in the string column and escapes them in the record digest.
    """

    @staticmethod
    def _raw_string_array(raw_values):
        data = b"".join(raw_values)
        offsets = [0]
        for raw in raw_values:
            offsets.append(offsets[-1] + len(raw))
        offsets_buffer = pa.array(offsets, type=pa.int32()).buffers()[1]
        return pa.StringArray.from_buffers(len(raw_values), offsets_buffer, pa.py_buffer(data))

    def test_string_column_values_retain_invalid_utf8_bytes(self):
        good = "Shaded Zone AE".encode("utf-8")
        broken = b"Shaded Zone X with depths less than 1\xe2"
        column = self._raw_string_array([good, broken])
        values = source._string_column_values(column)
        self.assertEqual(values[0], "Shaded Zone AE")
        self.assertEqual(values[1].encode("utf-8", "surrogateescape"), broken)

    def test_string_column_values_strict_path_unchanged(self):
        column = pa.array(["AE", "VE", None], type=pa.string())
        self.assertEqual(source._string_column_values(column), ["AE", "VE", None])

    def test_string_array_from_values_round_trips_producer_bytes(self):
        good = "Shaded Zone AE"
        broken = "Shaded Zone X with depths less than 1\udce2"
        array = source._string_array_from_values([good, broken], pa.string())
        self.assertEqual(array.buffers()[2], pa.py_buffer(
            good.encode("utf-8") + b"Shaded Zone X with depths less than 1\xe2"))

    def test_record_canonical_escapes_producer_surrogates(self):
        body = source._record_canonical({"STUDY_TYP": "less than 1\udce2"})
        self.assertEqual(json.loads(body), {"STUDY_TYP": "less than 1\udce2"})
        self.assertEqual(body, '{"STUDY_TYP":"less than 1\\udce2"}'.encode("ascii"))

    def test_record_canonical_is_deterministic_and_sorted(self):
        first = source._record_canonical({"B": 1, "A": "x"})
        second = source._record_canonical({"A": "x", "B": 1})
        self.assertEqual(first, second)
        self.assertEqual(first, b'{"A":"x","B":1}')

    def test_id_sort_key_orders_null_ids_deterministically(self):
        entries = [["48085C-NFHL", "22", "5"], ["01101C-NFHL", None, "9"],
                   ["01101C-NFHL", "7", "3"]]
        ordered = sorted(entries, key=source._id_sort_key)
        self.assertEqual(ordered, sorted(entries, key=source._id_sort_key))
        self.assertEqual(ordered[0], ["01101C-NFHL", None, "9"])
        self.assertEqual([entry[0] for entry in ordered],
                         ["01101C-NFHL", "01101C-NFHL", "48085C-NFHL"])


class NfhlFloodHazardZonesReuseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._fixture_dir = tempfile.TemporaryDirectory(prefix="fhz-reuse-zips-")
        cls.zips = _build_fixture_zips(Path(cls._fixture_dir.name))

    @classmethod
    def tearDownClass(cls):
        cls._fixture_dir.cleanup()

    def test_repeat_ingest_reuses_staged_candidate_without_downloads(self):
        with tempfile.TemporaryDirectory(prefix="fhz-reuse-") as temporary:
            root = Path(temporary)
            storage = _storage(root)
            with self.assertRaises(source._PinsPendingError):
                _run_capture(storage, root, _Router(_default_listing(), self.zips))
            contract = json.loads((_attempt(root) / "population_pins.json").read_bytes())
            candidate = _run_capture(storage, root, _Router(_default_listing(), self.zips),
                                     pins=_pins_from_contract(contract))
            version = candidate["version"]
            # Repeat ingest: fresh attempt directory, zero downloads, reused.
            router = _Router(_default_listing(), self.zips)
            repeat = _run_capture(_storage(root), root, router, version="vrepeat",
                                  pins=_pins_from_contract(contract))
            self.assertTrue(repeat["reused"])
            self.assertEqual(repeat["version"], version)
            self.assertEqual(router.static_downloads, 0)

    def test_roster_drift_skips_reuse_and_requires_recapture(self):
        with tempfile.TemporaryDirectory(prefix="fhz-reuse-drift-") as temporary:
            root = Path(temporary)
            storage = _storage(root)
            with self.assertRaises(source._PinsPendingError):
                _run_capture(storage, root, _Router(_default_listing(), self.zips))
            contract = json.loads((_attempt(root) / "population_pins.json").read_bytes())
            _run_capture(storage, root, _Router(_default_listing(), self.zips),
                         pins=_pins_from_contract(contract))
            drifted = _listing_html([_row(*product) for product in PRODUCTS[:3]])
            router = _Router(drifted, self.zips)
            with self.assertRaisesRegex(ValueError, "source_product_roster_drift"):
                _run_capture(storage, root, router, version="vrepeat",
                             pins=_pins_from_contract(contract))
            self.assertEqual(router.static_downloads, 0)


class NfhlFloodHazardZonesWiringTests(unittest.TestCase):
    def test_module_lists_and_definitions_are_registered(self):
        from dagster_hifld.assets import nfhl_flood_hazard_zones as wiring

        self.assertEqual([asset.key.to_user_string() for asset in wiring.INGEST_ASSETS],
                         [f"ingest/{source.DATASET_SLUG}/{source.FILE_SLUG}"])
        self.assertEqual(len(wiring.ASSET_CHECKS), 1)
        self.assertEqual(wiring.nfhl_flood_hazard_zones_draft_schedule.default_status,
                         wiring.DefaultScheduleStatus.STOPPED)

    def test_discovery_collects_the_dataset(self):
        from dagster_hifld.assets.discovery import discovered

        names = [asset.key.to_user_string() for asset in discovered("INGEST_ASSETS")]
        self.assertIn(f"ingest/{source.DATASET_SLUG}/{source.FILE_SLUG}", names)


if __name__ == "__main__":
    unittest.main()


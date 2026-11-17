"""Contract tests for the shared FEMA NFHL bulk county/community capture."""
from __future__ import annotations

import dataclasses
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.nfhl import LAYERS, LAYERS_BY_SLUG
from dagster_hifld.sources.nfhl import bulk as source

STATIC_BASE = "https://hazards.fema.gov/nfhlv2/output/County/"
FIELDS = ["DFIRM_ID", "VERSION_ID", "FLD_AR_ID", "STUDY_TYP", "FLD_ZONE", "ZONE_SUBTY",
          "SFHA_TF", "STATIC_BFE", "V_DATUM", "DEPTH", "LEN_UNIT", "VELOCITY", "VEL_UNIT",
          "AR_REVERT", "AR_SUBTRV", "BFE_REVERT", "DEP_REVERT", "DUAL_ZONE", "SOURCE_CIT"]


def _attrs(dfirm_id, fld_ar_id, fld_zone="AE", bfe=-9999.0):
    return {"DFIRM_ID": dfirm_id, "VERSION_ID": "1.1.1.0", "FLD_AR_ID": fld_ar_id,
            "STUDY_TYP": "NT", "FLD_ZONE": fld_zone, "ZONE_SUBTY": None,
            "SFHA_TF": "T" if fld_zone in ("A", "AE", "AH", "AO", "VE") else "N",
            "STATIC_BFE": bfe, "V_DATUM": "NAVD88" if bfe != -9999.0 else None,
            "DEPTH": -9999.0, "LEN_UNIT": None, "VELOCITY": -9999.0, "VEL_UNIT": None,
            "AR_REVERT": None, "AR_SUBTRV": None, "BFE_REVERT": -9999.0,
            "DEP_REVERT": -9999.0, "DUAL_ZONE": None, "SOURCE_CIT": fld_ar_id + "_CIT"}


def _center(text: str) -> str:
    return f'<center class="txt12">{text}</center>'


def _row(item_id, county, state, java_date, size, file_name) -> str:
    link = (f"Download/ProductsDownLoadServlet?DFIRMID={item_id[:-5]}&state={state}"
            f"&county={county}&fileName={file_name}")
    return (
        '<tr>\n'
        f'<td scope="row">{_center(item_id)}</td>\n'
        f'<td>{_center(f"<span style=\"display:none;\">{county} SORT</span>\n{county}")}</td>\n'
        f'<td>{_center(state)}</td>\n'
        f'<td>{_center(f"<span style=\"display:none;\">{java_date}</span>\n{java_date}")}</td>\n'
        f'<td>{_center(size)}</td>\n'
        '<td><center>'
        f'<a href="{link}" tabindex="1"><img src="/femaportal/NFHL/img/ZIPicon.png" /></a>'
        '</center></td>\n</tr>')


P1 = ("01001C-NFHL", "AUTAUGA COUNTY", "ALABAMA", "Mon Dec 25 19:32:41 EST 2023", "94MB",
      "01001C_20231225.zip")
P2 = ("01003C-NFHL", "BALDWIN COUNTY", "ALABAMA", "Sun Sep 27 08:00:00 CDT 2020", "150MB",
      "01003C_20200927.zip")
P3 = ("560077-NFHL", "TOWN OF MOORCROFT", "WYOMING", "Fri Feb  2 10:15:30 MST 2007", "20KB",
      "560077_20070202.zip")
P4 = ("47167C-NFHL", "LYMAN COUNTY", "SOUTH DAKOTA", "Fri Oct 21 00:00:00 EDT 2011", "12MB",
      "47167C_20190828.zip")


def _listing_html(products) -> bytes:
    rows = "".join(_row(*product) for product in products)
    return (f'<html><head><title>FEMA Map Service Center - National Flood Hazard Layer '
            f'(NFHL) Database Search Results</title></head><body><table>{rows}</table></body>'
            '</html>').encode()


def _default_listing() -> bytes:
    return _listing_html([P1, P2, P3, P4])


def _polygon(x, y, dx, dy):
    import shapely.geometry as geom
    return geom.Polygon([(x, y), (x + dx, y), (x + dx, y + dy), (x, y + dy), (x, y)])


def _frame(dfirm_id, geometries, ids, fields=FIELDS):
    import geopandas as gpd
    rows = [_attrs(dfirm_id, ids[index]) for index in range(len(geometries))]
    frame = gpd.GeoDataFrame(rows, geometry=geometries, crs="EPSG:4269")
    return frame[list(fields) + ["geometry"]]


def _zip_bytes(directory: Path, file_name: str, frames: dict) -> bytes:
    work = directory / file_name.replace(".zip", "")
    work.mkdir(parents=True, exist_ok=True)
    for feature_class, frame in frames.items():
        frame.to_file(work / f"{feature_class}.shp", driver="ESRI Shapefile")
    zip_path = directory / file_name
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for member in sorted(work.iterdir()):
            archive.write(member, arcname=member.name)
    return zip_path.read_bytes()


def _invalid_utf8_dbf(zip_bytes: bytes, marker: bytes, replacement: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        members = {name: archive.read(name) for name in archive.namelist()}
    dbf_name = next(name for name in members if name.endswith(".dbf"))
    if marker not in members[dbf_name]:
        raise AssertionError("marker missing from fixture DBF")
    members[dbf_name] = members[dbf_name].replace(marker, replacement)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def _fixture_zips(root: Path) -> dict[str, bytes]:
    root.mkdir(parents=True, exist_ok=True)
    zips = {
        P1[5]: _zip_bytes(root, P1[5], {"S_FLD_HAZ_AR": _frame(
            "01001C", [_polygon(-86.5, 32.5, 0.4, 0.3), _polygon(-87.5, 32.5, 0.2, 0.2)],
            ["11916A3_2001", "11916A3_2002"])}),
        P2[5]: _zip_bytes(root, P2[5], {"S_FLD_HAZ_AR": _frame(
            "01003C", [_polygon(-87.9, 30.5, 0.3, 0.2), None], ["11916C3_2011", "11916C3_2012"])}),
        P3[5]: _zip_bytes(root, P3[5], {"S_FLD_HAZ_AR": _frame(
            "560077", [_polygon(-104.95, 44.6, 0.05, 0.04)], ["12007B3_1990"])}),
    }
    line_fields = ["DFIRM_ID", "VERSION_ID", "FLD_LN_ID", "LN_TYP", "SOURCE_CIT"]
    import geopandas as gpd
    import shapely.geometry as geom
    line = gpd.GeoDataFrame(
        [{"DFIRM_ID": "47167C", "VERSION_ID": "1.1.1.0", "FLD_LN_ID": "LINE_1",
          "LN_TYP": "FLOODWAY", "SOURCE_CIT": "CIT"}],
        geometry=[geom.LineString([(-100.5, 44.5), (-100.4, 44.6)])], crs="EPSG:4269")
    zips[P4[5]] = _zip_bytes(root, P4[5], {"S_FLD_HAZ_LN": line[line_fields + ["geometry"]]})
    return zips


class _Router:
    def __init__(self, listing_body: bytes, zips: dict[str, bytes], *, listing_bodies=None,
                 redirect_host: str | None = None, rest_count: int | None = 5):
        self.listing_body = listing_body
        self.listing_bodies = list(listing_bodies or [])
        self.zips = zips
        self.redirect_host = redirect_host
        self.rest_count = rest_count
        self.downloads: list[str] = []
        self.listings = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == source.LISTING_URL:
            self.listings += 1
            body = self.listing_bodies.pop(0) if self.listing_bodies else self.listing_body
            return httpx.Response(200, content=body)
        if url.startswith(source.SERVLET_URL):
            file_name = dict(request.url.params)["fileName"]
            host = self.redirect_host or STATIC_BASE
            return httpx.Response(302, headers={"Location": host + file_name})
        if url.startswith(STATIC_BASE):
            file_name = url.rsplit("/", 1)[-1]
            self.downloads.append(file_name)
            return httpx.Response(200, content=self.zips[file_name])
        if url.startswith("https://hazards.fema.gov/arcgis/rest/services/public/NFHL/"):
            return httpx.Response(200, json={"count": self.rest_count})
        return httpx.Response(404)


def _fake_capture_get(router):
    """Emulate capture_get for the listing brackets (MockTransport cannot stream)."""
    import hashlib
    from datetime import datetime, timezone

    def capture_get(client, url, capture_root, name, **_kwargs):
        body = router.listing_bodies.pop(0) if router.listing_bodies else router.listing_body
        digest = hashlib.sha256(body).hexdigest()
        receipt = {"method": "GET", "url": url, "final_url": url,
                   "observed_at": datetime.now(timezone.utc).isoformat(), "status": 200,
                   "complete_body": True, "wire_bytes": len(body), "wire_sha256": digest,
                   "bytes": len(body), "sha256": digest, "content_encoding": "identity",
                   "content_length": len(body), "error_code": None, "error_type": None}
        (Path(capture_root) / f"{name}.body").write_bytes(body)
        (Path(capture_root) / f"{name}.wire").write_bytes(body)
        (Path(capture_root) / f"{name}.receipt.json").write_bytes(
            source._canonical(receipt) + b"\n")
        return body, receipt

    return capture_get


class ListingTests(unittest.TestCase):
    def test_parses_every_product_in_item_id_order(self):
        products = source.parse_listing(_default_listing())
        self.assertEqual([product.item_id for product in products],
                         ["01001C-NFHL", "01003C-NFHL", "47167C-NFHL", "560077-NFHL"])
        self.assertEqual(products[2].file_date, "20190828")
        self.assertEqual(source.update_date_iso(products[2].as_dict()), "2019-08-28")
        self.assertEqual(source.product_roster_sha256(products),
                         source.product_roster_sha256(tuple(products)))

    def test_structural_drift_fails_closed(self):
        body = _default_listing().replace(b"01001C-NFHL", b"01001X-NFHL")
        with self.assertRaisesRegex(ValueError, "source_listing_structure_drift"):
            source.parse_listing(body)

    def test_roster_change_is_detected(self):
        before = source.parse_listing(_default_listing())
        after = source.parse_listing(_listing_html([P1, P2, P3]))
        self.assertNotEqual(source.product_roster_sha256(before),
                            source.product_roster_sha256(after))


class DownloadTests(unittest.TestCase):
    def test_product_zip_is_resumable_and_hash_pinned(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            zips = _fixture_zips(root / "fixtures")
            router = _Router(_default_listing(), zips)
            settings = source.BulkSettings(transport=httpx.MockTransport(router))
            with patch.object(source, "capture_get", side_effect=_fake_capture_get(router)), \
                    settings.client() as client:
                products, stats = source.refresh_products(
                    client, root / "capture", settings=settings)
            self.assertEqual(stats["downloaded"], 4)
            self.assertEqual(stats["reused"], 0)
            self.assertTrue(all(source.verify_product_zip(root / "capture", product)
                                for product in products))
            router.downloads.clear()
            settings = source.BulkSettings(transport=httpx.MockTransport(router))
            with patch.object(source, "capture_get", side_effect=_fake_capture_get(router)), \
                    settings.client() as client:
                _products, stats = source.refresh_products(
                    client, root / "capture", settings=settings)
            self.assertEqual(stats["reused"], 4)
            self.assertEqual(stats["downloaded"], 0)
            self.assertEqual(router.downloads, [])

    def test_refresh_downloads_only_changed_product(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            zips = _fixture_zips(root / "fixtures")
            router = _Router(_default_listing(), zips)
            settings = source.BulkSettings(transport=httpx.MockTransport(router))
            with patch.object(source, "capture_get", side_effect=_fake_capture_get(router)), \
                    settings.client() as client:
                source.refresh_products(client, root / "capture", settings=settings)
            changed = P1[:5] + ("01001C_20240101.zip",)
            zips = dict(zips)
            zips["01001C_20240101.zip"] = zips[P1[5]]
            router = _Router(_listing_html([changed, P2, P3, P4]), zips)
            router.downloads.clear()
            settings = source.BulkSettings(transport=httpx.MockTransport(router))
            with patch.object(source, "capture_get", side_effect=_fake_capture_get(router)), \
                    settings.client() as client:
                _products, stats = source.refresh_products(
                    client, root / "capture", settings=settings)
            self.assertEqual(stats["downloaded"], 1)
            self.assertEqual(stats["reused"], 3)
            self.assertEqual(router.downloads, ["01001C_20240101.zip"])

    def test_redirect_off_the_producer_host_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            zips = _fixture_zips(root / "fixtures")
            router = _Router(_default_listing(), zips, redirect_host="https://example.com/")
            client = httpx.Client(transport=httpx.MockTransport(router),
                                  follow_redirects=False, timeout=30)
            with self.assertRaisesRegex(ValueError, "source_download_redirect_host_unrecognized"):
                source.capture_product_zip(client, source.parse_listing(_default_listing())[0],
                                           root, attempts=1, base_delay=0)


class StreamTests(unittest.TestCase):
    def _capture(self, root: Path, listing: bytes | None = None):
        zips = _fixture_zips(root / "fixtures")
        router = _Router(listing or _default_listing(), zips)
        settings = source.BulkSettings(transport=httpx.MockTransport(router))
        with patch.object(source, "capture_get", side_effect=_fake_capture_get(router)), \
                settings.client() as client:
            products, _stats = source.refresh_products(
                client, root / "capture", settings=settings)
        return products, root / "capture"

    def test_streams_polygons_with_null_geometry_and_bounds(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            products, capture = self._capture(root)
            layer = LAYERS_BY_SLUG["nfhl-flood-hazard-zones"]
            result = source.stream_feature_class(
                products, capture, layer, root / "candidate.parquet")
            self.assertEqual(result.feature_count, 5)
            self.assertEqual(result.counters.null_geometry, 1)
            self.assertEqual(result.counters.products_with_feature_class, 3)
            self.assertEqual(result.counters.products_without_feature_class, 1)
            self.assertEqual(result.schema_signature[0], ["DFIRM_ID", "string"])
            self.assertLess(result.bounds[0], result.bounds[2])
            table = pq.read_table(root / "candidate.parquet")
            self.assertEqual(table.num_rows, 5)
            self.assertIn("SHAPE", table.column_names)

    def test_streams_lines(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            products, capture = self._capture(root)
            layer = dataclasses.replace(
                LAYERS_BY_SLUG["nfhl-flood-hazard-boundaries"], source="bulk",
                feature_class="S_FLD_HAZ_LN",
                id_fields=("DFIRM_ID", "VERSION_ID", "FLD_LN_ID"))
            result = source.stream_feature_class(
                products, capture, layer, root / "candidate.parquet")
            self.assertEqual(result.feature_count, 1)
            self.assertEqual(result.counters.products_without_feature_class, 3)

    def test_non_utf8_dbf_text_fails_closed_with_product_and_field(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            products, capture = self._capture(root)
            layer = LAYERS_BY_SLUG["nfhl-flood-hazard-zones"]
            frame = _frame("01001C", [_polygon(-86.5, 32.5, 0.4, 0.3)], ["11916A3_2001"])
            frame["SOURCE_CIT"] = "CITMARKER"
            zip_path = capture / f"{products[0].item_id}.zip"
            zip_path.write_bytes(_invalid_utf8_dbf(
                _zip_bytes(root, "bad.zip", {"S_FLD_HAZ_AR": frame}),
                b"CITMARKER", b"CITM\x92RKER"))
            receipt = {"complete_body": True, "status": 200,
                       "bytes": zip_path.stat().st_size,
                       "sha256": source._file_sha256(zip_path),
                       "item_id": products[0].item_id,
                       "product_file_name": products[0].file_name,
                       "product_update_date": source.update_date_iso(products[0].as_dict())}
            (capture / f"{products[0].item_id}.receipt.json").write_bytes(
                source._canonical(receipt) + b"\n")
            with self.assertRaisesRegex(
                    ValueError,
                    rf"source_dbf_text_unencodable:{products[0].item_id}:SOURCE_CIT"):
                source.stream_feature_class((products[0],), capture, layer,
                                            root / "bad.parquet")

    def test_schema_drift_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            products, capture = self._capture(root)
            layer = LAYERS_BY_SLUG["nfhl-flood-hazard-zones"]
            source.stream_feature_class(products[:1], capture, layer, root / "one.parquet")
            extra = dataclasses.replace(products[0], item_id="01005C-NFHL",
                                        dfirm_id="01005C", file_name="01005C_20200101.zip")
            frame = _frame("01005C", [_polygon(-85.5, 31.5, 0.1, 0.1)], ["X"])
            frame["EXTRA"] = "value"
            zip_path = capture / f"{extra.item_id}.zip"
            zip_path.write_bytes(_zip_bytes(root, "01005C.zip", {"S_FLD_HAZ_AR": frame}))
            receipt = {"complete_body": True, "status": 200,
                       "bytes": zip_path.stat().st_size,
                       "sha256": source._file_sha256(zip_path),
                       "item_id": extra.item_id, "product_file_name": extra.file_name,
                       "product_update_date": source.update_date_iso(extra.as_dict())}
            (capture / f"{extra.item_id}.receipt.json").write_bytes(
                source._canonical(receipt) + b"\n")
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source.stream_feature_class((products[0], extra), capture, layer,
                                            root / "drift.parquet")


class CaptureTests(unittest.TestCase):
    def test_bulk_layer_stages_unapproved_candidate_and_reuses_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            zips = _fixture_zips(root / "fixtures")
            router = _Router(_default_listing(), zips, rest_count=5)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"))
            layer = LAYERS_BY_SLUG["nfhl-flood-hazard-zones"]
            settings = source.BulkSettings(transport=httpx.MockTransport(router))
            with patch.object(source, "capture_get", side_effect=_fake_capture_get(router)), \
                    patch.object(source.shutil, "disk_usage",
                                 lambda path: type("U", (), {"free": 10**12, "total": 10**12})()):
                result = source.capture_nfhl_bulk_layer(
                    storage, layer, capture_directory=str(root / "captures"), settings=settings)
                self.assertEqual(result["feature_count"], 5)
                self.assertEqual(result["source_count"], 5)
                self.assertEqual(result["rest_count_cross_check"], 5)
                self.assertEqual(result["rest_count_delta"], 0)
                self.assertFalse(result["release_accepted"])
                self.assertFalse(result["reused"])
                ready = storage.read_key(result["candidate_ready"])
                self.assertEqual(json.loads(ready)["approved"], False)
                self.assertTrue(storage.object_snapshot(
                    f"{result['candidate_ready'].rsplit('/metadata/', 1)[0]}/metadata/source_manifest.json"))
                router.downloads.clear()
                reused = source.capture_nfhl_bulk_layer(
                    storage, layer, capture_directory=str(root / "captures"), settings=settings)
                self.assertTrue(reused["reused"])
                self.assertEqual(router.downloads, [])

    def test_bulk_layer_requires_a_feature_class(self):
        with self.assertRaisesRegex(ValueError, "nfhl_layer_not_bulk_backed"):
            source.capture_nfhl_bulk_layer(
                StagingStorageResource(use_local=True, local_dir="/tmp"), 
                LAYERS_BY_SLUG["nfhl-water-lines"])


class GeometryValidationTests(unittest.TestCase):
    def _validate(self, geometry):
        import shapely.wkb
        counters = source.StreamCounters()
        bounds = [None, None, None, None]
        wkb = shapely.wkb.dumps(geometry)
        result = source._validate_feature_geometry(
            wkb, LAYERS_BY_SLUG["nfhl-gages"], counters, bounds)
        return result, counters, bounds

    def test_nan_coordinates_become_null_geometry(self):
        import shapely.geometry as geom
        result, counters, _bounds = self._validate(geom.Point(float("nan"), 1.0))
        self.assertIsNone(result)
        self.assertEqual(counters.nan_geometry, 1)

    def test_out_of_range_coordinates_fail_closed(self):
        import shapely.geometry as geom
        with self.assertRaisesRegex(ValueError, "source_coordinate_range_drift"):
            self._validate(geom.Point(200.0, 1.0))

    def test_z_geometry_fails_closed(self):
        import shapely.geometry as geom
        with self.assertRaisesRegex(ValueError, "source_z_or_m_geometry_unsupported"):
            self._validate(geom.Point(1.0, 2.0, 3.0))


class FailureEvidenceTests(unittest.TestCase):
    def test_transport_failure_preserves_attempt_evidence_and_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            product = source.parse_listing(_default_listing())[0]

            def handler(request):
                if str(request.url).startswith(source.SERVLET_URL):
                    return httpx.Response(
                        302, headers={"Location": STATIC_BASE + product.file_name})
                raise httpx.ConnectError("connection refused", request=request)

            client = httpx.Client(transport=httpx.MockTransport(handler),
                                  follow_redirects=False, timeout=30)
            with self.assertRaisesRegex(
                    ValueError, f"incomplete_source_capture:{product.item_id}"):
                source.capture_product_zip(client, product, root, attempts=2, base_delay=0)
            self.assertFalse((root / f"{product.item_id}.zip").exists())
            for attempt in (1, 2):
                receipt = json.loads(
                    (root / f"{product.item_id}.attempt-{attempt:02d}.receipt.json").read_bytes())
                self.assertFalse(receipt["complete_body"])
                self.assertEqual(receipt["error_code"], "incomplete_source_capture")
                self.assertEqual(receipt["bytes"], 0)
                self.assertTrue(
                    (root / f"{product.item_id}.attempt-{attempt:02d}.zip").exists())


class LayerTableTests(unittest.TestCase):
    def test_layer_28_is_bulk_backed_and_layer_8_is_rest(self):
        zones = LAYERS_BY_SLUG["nfhl-flood-hazard-zones"]
        self.assertEqual((zones.layer_id, zones.source, zones.feature_class),
                         (28, "bulk", "S_FLD_HAZ_AR"))
        self.assertEqual(zones.id_fields, ("DFIRM_ID", "VERSION_ID", "FLD_AR_ID"))
        self.assertIn(("nfhl/national-flood-hazard-layer-area-nfhl-1-east", 240862),
                      zones.inventory_aliases)
        datum = LAYERS_BY_SLUG["nfhl-datum-conversion-points"]
        self.assertEqual((datum.layer_id, datum.source), (8, "rest"))
        self.assertEqual(len({layer.layer_id for layer in LAYERS}), len(LAYERS))


if __name__ == "__main__":
    unittest.main()

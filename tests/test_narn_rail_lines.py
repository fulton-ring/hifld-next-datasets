"""Focused contract checks for the FRA NARN Rail Lines source adapter."""
from __future__ import annotations

import hashlib
import copy
import gzip
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs

import httpx
from dagster import AssetCheckSeverity, DagsterInstance, materialize
from dagster_hifld.assets import narn_rail_lines as assets
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.sources import arcgis_layer as shared
import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import narn_rail_lines as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_URL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_North_American_Rail_Network_Lines/FeatureServer"

PORTAL_ITEM_BODY = json.dumps({
    "id": "e143f436d4774402aa8cca1e663b1d24", "owner": "USDOT_BTS",
    "orgId": "xOi1kZaI0eWDREZv", "title": "North American Rail Network Lines",
    "type": "Feature Service", "access": "public", "url": SERVICE_URL,
    "description": ("The North American Rail Network (NARN) Rail Lines dataset was created in "
                    "2016 and was updated on October 19, 2026 from the Federal Railroad "
                    "Administration (FRA) and is part of the U.S. Department of Transportation "
                    "(USDOT)/Bureau of Transportation Statistics (BTS) National Transportation "
                    "Atlas Database (NTAD)."),
}, separators=(",", ":")).encode()

SERVICE_BODY = json.dumps({
    "serviceItemId": "e143f436d4774402aa8cca1e663b1d24",
    "layers": [{"id": 0, "name": "North American Rail Network Lines", "type": "Feature Layer"}],
}, separators=(",", ":")).encode()

EXTENT = {"xmin": -156.68977173810396, "ymin": 14.672258000071423,
          "xmax": -59.927231000261614, "ymax": 64.92614460638065,
          "spatialReference": {"wkid": 4326, "latestWkid": 4326}}


def _fields():
    return [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "nullable": False, "domain": None},
        {"name": "FRAARCID", "type": "esriFieldTypeInteger", "alias": "FRAARCID", "nullable": True, "domain": None},
        {"name": "FRFRANODE", "type": "esriFieldTypeInteger", "alias": "FRFRANODE", "nullable": True, "domain": None},
        {"name": "TOFRANODE", "type": "esriFieldTypeInteger", "alias": "TOFRANODE", "nullable": True, "domain": None},
        {"name": "STFIPS", "type": "esriFieldTypeString", "alias": "STFIPS", "length": 2, "nullable": True, "domain": None},
        {"name": "CNTYFIPS", "type": "esriFieldTypeString", "alias": "CNTYFIPS", "length": 3, "nullable": True, "domain": None},
        {"name": "STCNTYFIPS", "type": "esriFieldTypeString", "alias": "STCNTYFIPS", "length": 5, "nullable": True, "domain": None},
        {"name": "STATEAB", "type": "esriFieldTypeString", "alias": "STATEAB", "length": 2, "nullable": True, "domain": None},
        {"name": "COUNTRY", "type": "esriFieldTypeString", "alias": "COUNTRY", "length": 2, "nullable": True, "domain": None},
        {"name": "FRADISTRCT", "type": "esriFieldTypeSmallInteger", "alias": "FRADISTRCT", "nullable": True, "domain": None},
        {"name": "RROWNER1", "type": "esriFieldTypeString", "alias": "RROWNER1", "length": 4, "nullable": True, "domain": None},
        {"name": "RROWNER2", "type": "esriFieldTypeString", "alias": "RROWNER2", "length": 4, "nullable": True, "domain": None},
        {"name": "RROWNER3", "type": "esriFieldTypeString", "alias": "RROWNER3", "length": 4, "nullable": True, "domain": None},
        {"name": "TRKRGHTS1", "type": "esriFieldTypeString", "alias": "TRKRGHTS1", "length": 4, "nullable": True, "domain": None},
        {"name": "TRKRGHTS2", "type": "esriFieldTypeString", "alias": "TRKRGHTS2", "length": 4, "nullable": True, "domain": None},
        {"name": "TRKRGHTS3", "type": "esriFieldTypeString", "alias": "TRKRGHTS3", "length": 4, "nullable": True, "domain": None},
        {"name": "TRKRGHTS4", "type": "esriFieldTypeString", "alias": "TRKRGHTS4", "length": 4, "nullable": True, "domain": None},
        {"name": "TRKRGHTS5", "type": "esriFieldTypeString", "alias": "TRKRGHTS5", "length": 4, "nullable": True, "domain": None},
        {"name": "TRKRGHTS6", "type": "esriFieldTypeString", "alias": "TRKRGHTS6", "length": 4, "nullable": True, "domain": None},
        {"name": "TRKRGHTS7", "type": "esriFieldTypeString", "alias": "TRKRGHTS7", "length": 4, "nullable": True, "domain": None},
        {"name": "TRKRGHTS8", "type": "esriFieldTypeString", "alias": "TRKRGHTS8", "length": 4, "nullable": True, "domain": None},
        {"name": "TRKRGHTS9", "type": "esriFieldTypeString", "alias": "TRKRGHTS9", "length": 4, "nullable": True, "domain": None},
        {"name": "DIVISION", "type": "esriFieldTypeString", "alias": "DIVISION", "length": 100, "nullable": True, "domain": None},
        {"name": "SUBDIV", "type": "esriFieldTypeString", "alias": "SUBDIV", "length": 100, "nullable": True, "domain": None},
        {"name": "BRANCH", "type": "esriFieldTypeString", "alias": "BRANCH", "length": 100, "nullable": True, "domain": None},
        {"name": "YARDNAME", "type": "esriFieldTypeString", "alias": "YARDNAME", "length": 30, "nullable": True, "domain": None},
        {"name": "PASSNGR", "type": "esriFieldTypeString", "alias": "PASSNGR", "length": 1, "nullable": True, "domain": None},
        {"name": "STRACNET", "type": "esriFieldTypeString", "alias": "STRACNET", "length": 1, "nullable": True, "domain": None},
        {"name": "TRACKS", "type": "esriFieldTypeInteger", "alias": "TRACKS", "nullable": True, "domain": None},
        {"name": "NET", "type": "esriFieldTypeString", "alias": "NET", "length": 1, "nullable": True, "domain": None},
        {"name": "MILES", "type": "esriFieldTypeDouble", "alias": "MILES", "nullable": True, "domain": None},
        {"name": "KM", "type": "esriFieldTypeDouble", "alias": "KM", "nullable": True, "domain": None},
        {"name": "TIMEZONE", "type": "esriFieldTypeString", "alias": "TIMEZONE", "length": 1, "nullable": True, "domain": None},
        {"name": "Shape__Length", "type": "esriFieldTypeDouble", "alias": "Shape__Length", "nullable": True, "domain": None},
    ]


def _attributes(oid, fraarcid=None):
    rows = {
        10: {"FRAARCID": 300001, "FRFRANODE": 348741, "TOFRANODE": 348746, "STFIPS": "38",
             "CNTYFIPS": "015", "STCNTYFIPS": "38015", "STATEAB": "ND", "COUNTRY": "US",
             "FRADISTRCT": 4, "RROWNER1": "BNSF", "RROWNER2": None, "RROWNER3": None,
             "TRKRGHTS1": None, "TRKRGHTS2": None, "TRKRGHTS3": None, "TRKRGHTS4": None,
             "TRKRGHTS5": None, "TRKRGHTS6": None, "TRKRGHTS7": None, "TRKRGHTS8": None,
             "TRKRGHTS9": None, "DIVISION": "DAKOTA", "SUBDIV": "DEVILS LAKE", "BRANCH": None,
             "YARDNAME": None, "PASSNGR": None, "STRACNET": None, "TRACKS": 1, "NET": "M",
             "MILES": 12.345, "KM": 19.8616768, "TIMEZONE": "C", "Shape__Length": 0.111234},
        11: {"FRAARCID": 300002, "FRFRANODE": 348746, "TOFRANODE": 348748, "STFIPS": None,
             "CNTYFIPS": None, "STCNTYFIPS": None, "STATEAB": None, "COUNTRY": "CA",
             "FRADISTRCT": 99, "RROWNER1": None, "RROWNER2": None, "RROWNER3": None,
             "TRKRGHTS1": "CN", "TRKRGHTS2": None, "TRKRGHTS3": None, "TRKRGHTS4": None,
             "TRKRGHTS5": None, "TRKRGHTS6": None, "TRKRGHTS7": None, "TRKRGHTS8": None,
             "TRKRGHTS9": None, "DIVISION": None, "SUBDIV": None, "BRANCH": None,
             "YARDNAME": None, "PASSNGR": "C", "STRACNET": None, "TRACKS": None, "NET": "O",
             "MILES": None, "KM": None, "TIMEZONE": None, "Shape__Length": 0.05},
        12: {"FRAARCID": 300003, "FRFRANODE": 348748, "TOFRANODE": 348750, "STFIPS": None,
             "CNTYFIPS": None, "STCNTYFIPS": None, "STATEAB": None, "COUNTRY": "MX",
             "FRADISTRCT": 99, "RROWNER1": "FXE", "RROWNER2": None, "RROWNER3": None,
             "TRKRGHTS1": None, "TRKRGHTS2": None, "TRKRGHTS3": None, "TRKRGHTS4": None,
             "TRKRGHTS5": None, "TRKRGHTS6": None, "TRKRGHTS7": None, "TRKRGHTS8": None,
             "TRKRGHTS9": None, "DIVISION": None, "SUBDIV": None, "BRANCH": "SPUR",
             "YARDNAME": None, "PASSNGR": None, "STRACNET": "C", "TRACKS": 0, "NET": "I",
             "MILES": 0.25, "KM": 0.402336, "TIMEZONE": None, "Shape__Length": 0.002},
        13: {"FRAARCID": 300004, "FRFRANODE": 348750, "TOFRANODE": 348752, "STFIPS": None,
             "CNTYFIPS": None, "STCNTYFIPS": None, "STATEAB": None, "COUNTRY": "MX",
             "FRADISTRCT": None, "RROWNER1": None, "RROWNER2": None, "RROWNER3": None,
             "TRKRGHTS1": None, "TRKRGHTS2": None, "TRKRGHTS3": None, "TRKRGHTS4": None,
             "TRKRGHTS5": None, "TRKRGHTS6": None, "TRKRGHTS7": None, "TRKRGHTS8": None,
             "TRKRGHTS9": None, "DIVISION": None, "SUBDIV": None, "BRANCH": None,
             "YARDNAME": None, "PASSNGR": None, "STRACNET": None, "TRACKS": None, "NET": None,
             "MILES": None, "KM": None, "TIMEZONE": None, "Shape__Length": None},
        14: {"FRAARCID": 300005, "FRFRANODE": 348752, "TOFRANODE": 348754, "STFIPS": "15",
             "CNTYFIPS": "003", "STCNTYFIPS": "15003", "STATEAB": "HI", "COUNTRY": "US",
             "FRADISTRCT": 7, "RROWNER1": "Oahu", "RROWNER2": None, "RROWNER3": None,
             "TRKRGHTS1": None, "TRKRGHTS2": None, "TRKRGHTS3": None, "TRKRGHTS4": None,
             "TRKRGHTS5": None, "TRKRGHTS6": None, "TRKRGHTS7": None, "TRKRGHTS8": None,
             "TRKRGHTS9": None, "DIVISION": None, "SUBDIV": "HONOLULU", "BRANCH": None,
             "YARDNAME": None, "PASSNGR": None, "STRACNET": None, "TRACKS": 2, "NET": "M",
             "MILES": 3.5, "KM": 5.6327, "TIMEZONE": "H", "Shape__Length": 0.03},
        15: {"FRAARCID": 300006, "FRFRANODE": 348754, "TOFRANODE": 348756, "STFIPS": "02",
             "CNTYFIPS": "170", "STCNTYFIPS": "02170", "STATEAB": "AK", "COUNTRY": "US",
             "FRADISTRCT": 8, "RROWNER1": "ARR", "RROWNER2": None, "RROWNER3": None,
             "TRKRGHTS1": None, "TRKRGHTS2": None, "TRKRGHTS3": None, "TRKRGHTS4": None,
             "TRKRGHTS5": None, "TRKRGHTS6": None, "TRKRGHTS7": None, "TRKRGHTS8": None,
             "TRKRGHTS9": None, "DIVISION": None, "SUBDIV": None, "BRANCH": "AIA BRANCH",
             "YARDNAME": "ANCHORAGE INTL AIRPORT", "PASSNGR": None, "STRACNET": None,
             "TRACKS": 1, "NET": "O", "MILES": 0.21, "KM": 0.338, "TIMEZONE": "K",
             "Shape__Length": 0.006},
    }
    values = dict(rows[oid])
    if fraarcid is not None:
        values["FRAARCID"] = fraarcid
    return {"OBJECTID": oid, **values}


def _geometries():
    return {
        10: {"paths": [[[-100.745209133472, 46.8028492512936], [-100.745094918972, 46.8031918939714],
                        [-100.7449, 46.8035]]]},
        11: {"paths": [[[-123.1, 49.28], [-123.0, 49.3]]]},
        12: {"paths": [[[-106.4, 31.7], [-106.3, 31.8]], [[-106.35, 31.75], [-106.2, 31.9]]]},
        13: None,
        14: {"paths": [[[-159.0, 21.3], [-158.9, 21.4]]]},
        15: {"paths": [[[-149.99, 61.17], [-149.9, 61.2], [-149.8, 61.25]]]},
    }


def _fixture():
    fields = _fields()
    layer = {"id": 0, "name": "North American Rail Network Lines", "type": "Feature Layer",
             "geometryType": "esriGeometryPolyline", "objectIdField": "OBJECTID",
             "hasZ": False, "hasM": False, "maxRecordCount": 2000,
             "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
             "extent": EXTENT, "fields": fields}
    service = {"serviceItemId": "e143f436d4774402aa8cca1e663b1d24",
               "layers": [{"id": 0, "name": "North American Rail Network Lines", "type": "Feature Layer"}]}
    features = [{"attributes": _attributes(oid), "geometry": _geometries()[oid]}
                for oid in (10, 11, 12, 13, 14, 15)]
    ids = [10, 11, 12, 13, 14, 15]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f}
              for f in fields]
    attr_names = [f["name"] for f in fields]
    (geometry_records, null_count, invalid_count, bounds,
     vertex_total, multipart_count) = source._classify_geometries(features, attr_names)
    crs = {"wkid": 4326, "latestWkid": 4326}
    page0 = {"features": features[:2], "spatialReference": crs, "exceededTransferLimit": True}
    page1 = {"features": features[2:4], "spatialReference": crs, "exceededTransferLimit": True}
    page2 = {"features": features[4:], "spatialReference": crs}
    docs = {"portal_item": json.loads(PORTAL_ITEM_BODY), "service_before": service,
            "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 6}, "count_after": {"count": 6},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "page_000000_00002": page0, "page_000002_00002": page1, "page_000004_00002": page2}
    captures = {name: (json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode(), {"status": 200})
                for name, doc in docs.items()}
    geom = geometry_records
    counts = (null_count, invalid_count)
    extras = (vertex_total, multipart_count)
    return fields, features, captures, schema, geom, counts, bounds, extras


def _pins(features, schema, geom, counts, bounds, extras):
    null_count, invalid_count = counts
    return dict(PAGE_START_RECORDS=2,
                EXPECTED_NULL_GEOMETRY_COUNT=null_count,
                EXPECTED_INVALID_GEOMETRY_COUNT=invalid_count,
                EXPECTED_FRAARCID_NULL_COUNT=0, EXPECTED_FRAARCID_DUPLICATE_COUNT=0,
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(0, "North American Rail Network Lines", "Feature Layer")])


def _page_features(captures):
    rows = [row for name in sorted(captures) if name.startswith("page_")
            for row in json.loads(captures[name][0])["features"]]
    return rows


def _current_fixture(change=0):
    fields, rows, captures, schema, *_ = _fixture()
    rows[3]['geometry'] = {'paths': [[[-106.0, 28.6], [-105.9, 28.7]]]}
    if change > 0:
        added = copy.deepcopy(rows[2])
        added['attributes'].update(OBJECTID=16, FRAARCID=300007)
        added['geometry'] = {'paths': [[[-55.0, 66.0], [-54.9, 66.1]], [[-54.8, 66.2], [-54.7, 66.3]]]}
        rows.append(added)
    elif change < 0:
        rows.pop(0)
    documents = {name: json.loads(body) for name, (body, _) in captures.items() if not name.startswith('page_')}
    _, _, _, bounds, *_ = source._classify_geometries(rows, [field['name'] for field in fields])
    for name, document in documents.items():
        if name.startswith('ids_'):
            document['objectIds'] = [row['attributes']['OBJECTID'] for row in rows]
        elif name.startswith('count_'):
            document['count'] = len(rows)
        elif name.startswith('layer_'):
            document['extent'] = {**bounds, 'spatialReference': {'wkid': 4326}}
    return documents, rows, source._sha(_canonical(schema))


def _fixture_client(documents, rows, *, adaptive=False, torn=False, incomplete=False):
    requests = {}
    documents_by_path = {source.SERVICE_URL: documents['service_before'],
        source.LAYER_URL: documents['layer_before'],
        f'https://www.arcgis.com/sharing/rest/content/items/{source.ITEM_ID}': documents['portal_item']}

    def respond(request):
        query = parse_qs(request.url.query.decode())
        requests[str(request.url)] = requests.get(str(request.url), 0) + 1
        if 'resultOffset' in query:
            offset, width = int(query['resultOffset'][0]), int(query['resultRecordCount'][0])
            if adaptive and offset == 0 and width > 1:
                wire = gzip.compress(b'{"error":{"code":500}}', mtime=0)
                if requests[str(request.url)] % 2:
                    wire = wire[:-5]
            else:
                selected = rows[offset:offset + width]
                if incomplete and offset == 0:
                    selected = selected[:-1]
                page = {'features': selected, 'spatialReference': {'wkid': 4326},
                        'exceededTransferLimit': offset + width < len(rows)}
                wire = gzip.compress(json.dumps(page, separators=(',', ':')).encode(), mtime=0)
            return httpx.Response(200, headers={'Content-Encoding': 'gzip'}, stream=httpx.ByteStream(wire))
        if 'returnIdsOnly' in query:
            document = documents['ids_before']
        elif 'returnCountOnly' in query:
            document = {'count': len(rows) + 1} if torn and requests[str(request.url)] == 2 else documents['count_before']
        else:
            document = documents_by_path[str(request.url).split('?')[0]]
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(document, separators=(',', ':')).encode()))

    return httpx.Client(transport=httpx.MockTransport(respond))


class NarnRailLinesTests(unittest.TestCase):
    def test_real_adaptive_capture_replay_and_changed_population(self):
        from dagster_hifld.resources import StagingStorageResource
        documents, rows, schema_sha = _current_fixture()
        with tempfile.TemporaryDirectory() as tmp, DagsterInstance.ephemeral() as instance, \
                patch.multiple(source, EXPECTED_SCHEMA_SHA256=schema_sha, PAGE_START_RECORDS=4,
                               DOC_CAPTURE_BASE_DELAY_SECONDS=0, PAGE_PROBE_BASE_DELAY_SECONDS=0):
            root = Path(tmp)
            storage = StagingStorageResource(local_dir=str(root / 'stage'))
            client = _fixture_client(documents, rows, adaptive=True)
            with patch.object(source.httpx, 'Client', return_value=client):
                first = source.capture_narn_rail_lines(storage, version='fixture', capture_directory=str(root / 'live'))
            retained = Path(first['capture_directory'])
            prefix = first['candidate_ready'].removesuffix('/metadata/candidate_ready.json')
            original = {key: storage.read_key(key) for key in storage.list_prefix(prefix)}
            for width in (4, 2):
                parent = f'page_000000_{width:05d}'
                self.assertFalse(json.loads((retained / f'{parent}.attempt-01.receipt.json').read_bytes())['complete_body'])
                self.assertTrue(json.loads((retained / f'{parent}.receipt.json').read_bytes())['complete_body'])
            table = pq.read_table(Path(storage.local_dir) / prefix / f'geoparquet/{source.DATASET_SLUG}.parquet')
            self.assertEqual(table.column_names, [field['name'] for field in _fields()] + ['geometry'])
            for expected, actual in zip(rows, table.to_pylist(), strict=True):
                geometry = shapely.from_wkb(actual.pop('geometry'))
                self.assertEqual(actual, expected['attributes'])
                self.assertEqual(shapely.get_coordinates(geometry).tolist(),
                    [point for path in expected['geometry']['paths'] for point in path])
                self.assertEqual(geometry.geom_type, 'MultiLineString' if len(expected['geometry']['paths']) > 1 else 'LineString')
            with patch.object(httpx.Client, 'send', side_effect=AssertionError('unexpected replay HTTP')):
                replay = source.capture_narn_rail_lines(storage, version='replay', capture_directory=str(root / 'replay'),
                    offline_capture_directory=str(retained))
            self.assertTrue(replay['reused'])
            client = _fixture_client(documents, rows, adaptive=True)
            with patch.object(source.httpx, 'Client', return_value=client):
                refreshed = source.capture_narn_rail_lines(storage, version='refresh', capture_directory=str(root / 'refresh'))
            self.assertTrue(refreshed['reused'])
            self.assertEqual(refreshed['version'], first['version'])
            added_docs, added_rows, _ = _current_fixture(1)
            client = _fixture_client(added_docs, added_rows, adaptive=True)
            with patch.object(source.httpx, 'Client', return_value=client):
                result = materialize([*assets.INGEST_ASSETS, *assets.ASSET_CHECKS], instance=instance,
                    resources={'staging_storage': storage}, run_config={'ops': {assets.fra_narn_rail_network_lines.op.name: {
                        'config': {'capture_directory': str(root / 'added')}}}})
            self.assertTrue(result.success)
            added = result.output_for_node(assets.fra_narn_rail_network_lines.op.name)
            self.assertEqual(added['feature_count'], 7)
            checks = {check.check_name: check for check in result.get_asset_check_evaluations()}
            self.assertTrue(checks['fra_narn_rail_network_lines_complete'].passed)
            warning = checks['fra_narn_rail_network_lines_approved_baseline_drift']
            self.assertEqual(warning.severity, AssetCheckSeverity.WARN)
            self.assertEqual(warning.metadata['comparison_status'].value, 'skipped_no_approved_baseline')
            drift = approved_baseline_drift(added, {**first, 'release_accepted': True})
            self.assertFalse(drift.passed)
            self.assertEqual(drift.metadata['feature_count_delta'].value, 1)
            removed_docs, removed_rows, _ = _current_fixture(-1)
            client = _fixture_client(removed_docs, removed_rows, adaptive=True)
            with patch.object(source.httpx, 'Client', return_value=client):
                removed = source.capture_narn_rail_lines(storage, version='removed', capture_directory=str(root / 'removed'))
            self.assertEqual(removed['feature_count'], 5)
            self.assertNotEqual(removed['version'], first['version'])
            total_bytes = sum(path.stat().st_size for path in retained.iterdir())
            with patch.object(source, 'MAX_CAPTURE_TOTAL_BYTES', total_bytes - 1), \
                    patch.object(Path, 'read_bytes', side_effect=AssertionError('read before budget admission')), \
                    patch.object(source.shutil, 'copyfile', side_effect=AssertionError('copy before budget admission')):
                with self.assertRaisesRegex(ValueError, 'source_capture_exceeds_scratch_budget'):
                    source.capture_narn_rail_lines(storage, version='budget', capture_directory=str(root / 'budget'),
                        offline_capture_directory=str(retained))
            for mode in ('missing_parent', 'wrong_parent_request', 'missing_wire'):
                directory = root / mode
                shutil.copytree(retained, directory)
                if mode == 'missing_parent':
                    for suffix in ('.body', '.wire', '.receipt.json'):
                        (directory / ('page_000000_00004' + suffix)).unlink()
                elif mode == 'missing_wire':
                    (directory / 'page_000001_00001.wire').unlink()
                else:
                    path = directory / 'page_000000_00004.receipt.json'
                    receipt = json.loads(path.read_bytes())
                    receipt.update(url=source._page_query(4, 2), final_url=source._page_query(4, 2))
                    path.write_bytes(_canonical(receipt))
                path = directory / 'capture_manifest.json'
                manifest = json.loads(path.read_bytes())
                manifest['artifact_roster'] = source.capture_roster_digests(directory, exclude={'capture_manifest.json'})
                path.write_bytes(_canonical(manifest))
                with self.subTest(replay=mode), self.assertRaises((ValueError, OSError)):
                    source.capture_narn_rail_lines(storage, version='invalid', capture_directory=str(root / 'invalid'),
                        offline_capture_directory=str(directory))
            self.assertEqual({key: storage.read_key(key) for key in storage.list_prefix(prefix)}, original)
            path = Path(storage.local_dir) / prefix / 'metadata/source_manifest.json'
            original_manifest = path.read_bytes()
            path.write_bytes(b'{}')
            with self.assertRaisesRegex(ValueError, 'cached_candidate_object_integrity_failed'):
                source.capture_narn_rail_lines(storage, version='conflict', capture_directory=str(root / 'conflict'),
                    offline_capture_directory=str(retained))
            path.write_bytes(original_manifest)
            (Path(storage.local_dir) / f"_construction-state/{source.DATASET_SLUG}/{first['version']}.json").unlink()
            with self.assertRaisesRegex(ValueError, 'unsealed_candidate_prefix'):
                source.capture_narn_rail_lines(storage, version='partial', capture_directory=str(root / 'partial'),
                    offline_capture_directory=str(retained))

    def test_real_capture_rejects_torn_counts_and_missing_rows(self):
        from dagster_hifld.resources import StagingStorageResource
        documents, rows, schema_sha = _current_fixture()
        with tempfile.TemporaryDirectory() as tmp, patch.multiple(source, EXPECTED_SCHEMA_SHA256=schema_sha,
                PAGE_START_RECORDS=4, DOC_CAPTURE_BASE_DELAY_SECONDS=0, PAGE_PROBE_BASE_DELAY_SECONDS=0):
            storage = StagingStorageResource(local_dir=str(Path(tmp) / 'stage'))
            for mode in ('torn', 'incomplete'):
                client = _fixture_client(documents, rows, torn=mode == 'torn', incomplete=mode == 'incomplete')
                with self.subTest(mode=mode), patch.object(source.httpx, 'Client', return_value=client):
                    with self.assertRaises(ValueError):
                        source.capture_narn_rail_lines(storage, version=mode, capture_directory=str(Path(tmp) / mode))
            self.assertFalse(storage.list_prefix(source.DATASET_SLUG))
            self.assertTrue(storage.list_prefix('_quarantine/' + source.DATASET_SLUG))

    def test_streamed_list_sha_matches_canonical_digest(self):
        items = [[1, {"a": "b", "list": [1, 2, 3]}], None, "x", 4.5]
        self.assertEqual(source._list_sha(items), hashlib.sha256(_canonical(items)).hexdigest())

    def test_native_fields_ids_and_leading_zeros_preserved_in_parquet(self):
        fields, features, captures, schema, geom, counts, _, extras = _fixture()
        with patch.multiple(source, **_pins(features, schema, geom, counts, None, extras)):
            _, original_fields, extracted = source._validate_captures(captures)
            body = source._parquet_bytes(original_fields, extracted["features"])
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 6)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12, 13, 14, 15])
        self.assertEqual(table.column("FRAARCID").type.__str__(), "int32")
        self.assertEqual(table.column("FRAARCID").to_pylist(),
                         [300001, 300002, 300003, 300004, 300005, 300006])
        self.assertEqual(table.column("FRADISTRCT").type.__str__(), "int16")
        self.assertEqual(table.column("TRACKS").to_pylist(), [1, None, 0, None, 2, 1])
        # FIPS strings keep their leading zeros verbatim; nulls stay null.
        self.assertEqual(table.column("STFIPS").to_pylist(), ["38", None, None, None, "15", "02"])
        self.assertEqual(table.column("CNTYFIPS").to_pylist(), ["015", None, None, None, "003", "170"])
        self.assertEqual(table.column("STCNTYFIPS").to_pylist(),
                         ["38015", None, None, None, "15003", "02170"])
        self.assertEqual(table.column("MILES").to_pylist()[0], 12.345)
        self.assertEqual(table.column("KM").to_pylist()[5], 0.338)
        self.assertEqual(table.column("Shape__Length").to_pylist()[3], None)
        self.assertEqual(table.column("COUNTRY").to_pylist(), ["US", "CA", "MX", "MX", "US", "US"])
        geoms = [shapely.from_wkb(wkb) if wkb is not None else None
                 for wkb in table.column("geometry").to_pylist()]
        self.assertIsInstance(geoms[0], shapely.LineString)
        self.assertEqual(list(geoms[0].coords)[0], (-100.745209133472, 46.8028492512936))
        self.assertIsInstance(geoms[2], shapely.MultiLineString)
        self.assertEqual(geoms[2].geom_type, "MultiLineString")
        self.assertEqual(len(geoms[2].geoms), 2)
        self.assertIsNone(geoms[3])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["LineString", "MultiLineString"])

    def test_id_roster_response_order_is_not_population_drift(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        for name, ids in (("ids_seed", [12, 10, 11, 14, 13, 15]), ("ids_before", [11, 12, 10, 13, 14, 15]),
                          ("ids_after", [10, 11, 12, 13, 14, 15])):
            doc = json.loads(captures[name][0])
            doc["objectIds"] = ids
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), captures[name][1])
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            _, _, extracted = source._validate_captures(captures)
        self.assertEqual(extracted["ids"], [10, 11, 12, 13, 14, 15])
        self.assertEqual(extracted["audit"]["null_geometry_count"], 1)
        self.assertGreaterEqual(extracted["audit"]["multipart_feature_count"], 1)

    def test_portal_item_owner_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        portal = json.loads(captures["portal_item"][0])
        portal["owner"] = "NOT_USDOT_BTS"
        captures["portal_item"] = (json.dumps(portal, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_portal_item_identity_drift"):
                source._validate_captures(captures)

    def test_portal_item_url_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        portal = json.loads(captures["portal_item"][0])
        portal["url"] = "https://mirror.invalid/arcgis/rest/services/Other/FeatureServer"
        captures["portal_item"] = (json.dumps(portal, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_portal_item_identity_drift"):
                source._validate_captures(captures)

    def test_portal_item_delegation_text_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        portal = json.loads(captures["portal_item"][0])
        portal["description"] = "A repackaged copy of rail lines, no FRA lineage stated."
        captures["portal_item"] = (json.dumps(portal, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "official_FRA_NTAD_relationship_drift"):
                source._validate_captures(captures)

    def test_service_item_identity_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        service = json.loads(captures["service_before"][0])
        service["serviceItemId"] = "00000000000000000000000000000000"
        captures["service_before"] = (json.dumps(service, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "official_BTS_narn_rail_lines_service_identity_drift"):
                source._validate_captures(captures)

    def test_service_layer_roster_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        service = json.loads(captures["service_before"][0])
        service["layers"].append({"id": 1, "name": "Extra Layer", "type": "Feature Layer"})
        captures["service_before"] = (json.dumps(service, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "official_BTS_narn_rail_lines_service_identity_drift"):
                source._validate_captures(captures)

    def test_layer_geometry_type_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["geometryType"] = "esriGeometryPoint"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "exact_BTS_narn_rail_lines_layer_identity_drift"):
                source._validate_captures(captures)

    def test_pagination_capability_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["advancedQueryCapabilities"] = {"supportsPagination": False, "supportsOrderBy": True}
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_pagination_capability_changed"):
                source._validate_captures(captures)

    def test_record_limit_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["maxRecordCount"] = 1000
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_record_limit_drift"):
                source._validate_captures(captures)

    def test_layer_extent_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["extent"]["xmax"] = -50.0
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_layer_extent_drift"):
                source._validate_captures(captures)

    def test_crs_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["spatialReference"] = {"wkid": 3857, "latestWkid": 3857}
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_page_000000_crs_drift"):
                source._validate_captures(captures)

    def test_unexpected_field_is_quarantined_by_contract(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_captures(captures)

    def test_renamed_field_is_quarantined_by_contract(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"][4]["name"] = "STFIPS_V2"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_captures(captures)

    def test_field_type_change_is_quarantined_by_contract(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"][30]["type"] = "esriFieldTypeString"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_captures(captures)

    def test_missing_page_id_is_not_accepted(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"] = page["features"][:1]
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                source._validate_captures(captures)

    def test_final_page_cannot_claim_transfer_truncation(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        final = json.loads(captures["page_000004_00002"][0])
        final["exceededTransferLimit"] = True
        captures["page_000004_00002"] = (json.dumps(final, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
                source._validate_captures(captures)

    def test_record_value_change_is_accepted(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["attributes"]["COUNTRY"] = "XX"
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            _, _, extracted = source._validate_captures(captures)
        self.assertEqual(extracted["features"][0]["attributes"]["COUNTRY"], "XX")

    def test_geometry_change_is_accepted(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][1]["geometry"]["paths"][0][0] = [-123.05, 49.29]
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            _, _, extracted = source._validate_captures(captures)
        self.assertEqual(extracted["features"][1]["geometry"]["paths"][0][0], [-123.05, 49.29])

    def test_coordinate_range_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["geometry"]["paths"][0][0][0] = -200.0
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_coordinate_range_drift"):
                source._validate_captures(captures)

    def test_non_numeric_path_point_fails_closed(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][1]["geometry"]["paths"][0][0] = ["NaN", 49.28]
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
                source._validate_captures(captures)

    def test_empty_path_fails_closed_as_invalid_geometry(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][1]["geometry"] = {"paths": []}
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
                source._validate_captures(captures)

    def test_null_geometry_count_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        page = json.loads(captures["page_000002_00002"][0])
        page["features"][0]["geometry"] = None
        captures["page_000002_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
                source._validate_captures(captures)

    def test_multipart_geometry_edit_is_accepted(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][1]["geometry"]["paths"].append([[-120.0, 40.0], [-120.1, 40.1]])
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            _, _, extracted = source._validate_captures(captures)
        self.assertGreaterEqual(extracted["audit"]["multipart_feature_count"], 1)

    def test_fraarcid_null_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["attributes"]["FRAARCID"] = None
        captures["page_000000_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_feature_id_null_drift"):
                source._validate_captures(captures)

    def test_fraarcid_duplicate_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        page = json.loads(captures["page_000002_00002"][0])
        page["features"][0]["attributes"]["FRAARCID"] = 300001
        captures["page_000002_00002"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_feature_id_duplicate_drift"):
                source._validate_captures(captures)

    def test_count_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        count = json.loads(captures["count_after"][0])
        count["count"] = 5
        captures["count_after"] = (json.dumps(count, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
                source._validate_captures(captures)

    def test_upstream_added_feature_passes_and_warns_on_unapproved_drift(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        added = {"attributes": {**_attributes(10), "OBJECTID": 16, "FRAARCID": 300099},
                 "geometry": {"paths": [[[-100.0, 40.0], [-100.1, 40.1]]]}}
        ids = [10, 11, 12, 13, 14, 15, 16]
        for name in ("ids_seed", "ids_before", "ids_after"):
            doc = json.loads(captures[name][0]); doc["objectIds"] = ids
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), {"status": 200})
        for name in ("count_before", "count_after"):
            doc = json.loads(captures[name][0]); doc["count"] = 7
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), {"status": 200})
        page = {"features": [added], "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
        captures["page_000006_00001"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            _, _, extracted = source._validate_captures(captures)
        self.assertEqual(len(extracted["features"]), 7)
        from dagster_hifld.assets.narn_rail_lines import (
            fra_narn_rail_network_lines_approved_baseline_drift,
        )
        candidate = {"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
            "feature_count": 7, "source_id_sha256": source._sha(source._canonical(ids)),
            "release_accepted": False}
        drift = fra_narn_rail_network_lines_approved_baseline_drift(candidate)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")

    def test_id_roster_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        ids = json.loads(captures["ids_after"][0])
        ids["objectIds"] = [10, 11, 12, 13, 14, 99]
        captures["ids_after"] = (json.dumps(ids, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
                source._validate_captures(captures)

    def test_seed_roster_mismatch_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        seed = json.loads(captures["ids_seed"][0])
        seed["objectIds"] = [10, 11, 12, 13, 14, 99]
        captures["ids_seed"] = (json.dumps(seed, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
                source._validate_captures(captures)

    def test_page_tiling_gap_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        del captures["page_000004_00002"]
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_page_tiling_incomplete_or_overlapped"):
                source._validate_captures(captures)

    def test_malformed_page_name_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        captures["page_xxxxx_00002"] = captures.pop("page_000004_00002")
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
                source._validate_captures(captures)

    def test_layer_metadata_change_during_capture_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds, extras = _fixture()
        layer = json.loads(captures["layer_after"][0])
        layer["maxRecordCount"] = 1000
        captures["layer_after"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)):
            with self.assertRaisesRegex(ValueError, "source_layer_metadata_changed_during_capture"):
                source._validate_captures(captures)

    def test_transient_producer_error_doc_is_retried_with_evidence(self):
        ok_body = b'{"objectIds": []}'
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = error_body if len(calls) == 1 else ok_body
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "wire_bytes": len(body), "wire_sha256": digest,
                       "bytes": len(body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(shared, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep") as sleep_mock:
                body, _receipt = source._capture_with_retry(None, "https://producer.invalid/page",
                                                            root, "probe", max_bytes=1024,
                                                            attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                            base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
            self.assertEqual(body, ok_body)
            self.assertEqual(calls, ["probe", "probe"])
            self.assertEqual((root / "probe.body").read_bytes(), ok_body)
            self.assertEqual((root / "probe.attempt-01.body").read_bytes(), error_body)
            self.assertTrue(sleep_mock.called)

    def test_persistent_producer_error_doc_exhausts_retries(self):
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            digest = hashlib.sha256(error_body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(error_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(error_body)
            (capture_root / f"{name}.wire").write_bytes(error_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return error_body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(shared, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                body, _receipt = source._capture_with_retry(None, "https://producer.invalid/page",
                                                            root, "probe", max_bytes=1024,
                                                            attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                            base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
            self.assertEqual(body, error_body)
            self.assertEqual(len(calls), source.DOC_CAPTURE_ATTEMPTS)
            for attempt in range(1, source.DOC_CAPTURE_ATTEMPTS):
                self.assertEqual((root / f"probe.attempt-{attempt:02d}.body").read_bytes(), error_body)

    def test_incomplete_transport_capture_is_retried_bounded(self):
        ok_body = b'{"objectIds": []}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            if name == "probe2":
                raise ValueError("unrelated_source_capture_failure")
            if len(calls) == 1:
                (capture_root / f"{name}.body").write_bytes(b"")
                (capture_root / f"{name}.wire").write_bytes(b"")
                (capture_root / f"{name}.receipt.json").write_bytes(b'{"status": null}\n')
                raise ValueError(f"incomplete_source_capture:{name}")
            digest = hashlib.sha256(ok_body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(ok_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(ok_body)
            (capture_root / f"{name}.wire").write_bytes(ok_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return ok_body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(shared, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                body, _receipt = source._capture_with_retry(None, "https://producer.invalid/page",
                                                            root, "probe", max_bytes=1024,
                                                            attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                            base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
                with self.assertRaisesRegex(ValueError, "unrelated_source_capture_failure"):
                    source._capture_with_retry(None, "https://producer.invalid/other",
                                               root, "probe2", max_bytes=1024,
                                               attempts=source.DOC_CAPTURE_ATTEMPTS,
                                               base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
            self.assertEqual(body, ok_body)
            self.assertEqual(calls.count("probe"), 2)
            self.assertEqual(len(calls), 3)
            self.assertEqual((root / "probe.attempt-01.receipt.json").read_bytes(), b'{"status": null}\n')

    def test_adaptive_bisection_splits_producer_error_doc_failures(self):
        _, features, fixture_captures, _, _, _, _, _ = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = error_body if name == "page_000000_00004" else bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at":
                       "2026-11-13T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": digest, "bytes": len(body),
                       "sha256": digest, "content_encoding": None, "content_length": str(len(body)),
                       "error_code": None}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            with patch.multiple(source, PAGE_START_RECORDS=4,
                                PAGE_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=1), \
                 patch.object(shared, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                pages = source._capture_pages(None, root, 6)
            self.assertEqual(sorted(pages), ["page_000000_00002", "page_000002_00002", "page_000004_00002"])
            self.assertEqual((root / "page_000000_00004.body").read_bytes(), error_body)
            self.assertEqual(calls.count("page_000000_00004"), 1)

    def test_adaptive_bisection_splits_persistent_transport_failures(self):
        _, features, fixture_captures, _, _, _, _, _ = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            if name == "page_000000_00004":
                (capture_root / f"{name}.body").write_bytes(b"")
                (capture_root / f"{name}.wire").write_bytes(b"")
                (capture_root / f"{name}.receipt.json").write_bytes(b'{"status": null}\n')
                raise ValueError(f"incomplete_source_capture:{name}")
            body = bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at":
                       "2026-11-13T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": digest, "bytes": len(body),
                       "sha256": digest, "content_encoding": None, "content_length": str(len(body)),
                       "error_code": None}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            with patch.multiple(source, PAGE_START_RECORDS=4,
                                PAGE_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=1), \
                 patch.object(shared, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                pages = source._capture_pages(None, root, 6)
            self.assertEqual(sorted(pages), ["page_000000_00002", "page_000002_00002", "page_000004_00002"])
            self.assertEqual(calls.count("page_000000_00004"), 1)

    def test_unservable_single_feature_fails_explicitly(self):
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            digest = hashlib.sha256(error_body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(error_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(error_body)
            (capture_root / f"{name}.wire").write_bytes(error_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return error_body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            with patch.multiple(source, PAGE_START_RECORDS=4,
                                PAGE_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=2), \
                 patch.object(shared, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                with self.assertRaisesRegex(ValueError, "source_single_feature_unservable"):
                    source._capture_pages(None, root, 6)

    def test_capture_requires_explicit_local_staging(self):
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
                source.capture_narn_rail_lines(PublishedStorageResource(use_local=True,
                                                                        local_dir=str(root)),
                                               version="v20261113")
            bucketed = StagingStorageResource(use_local=False, bucket="some-bucket")
            with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
                source.capture_narn_rail_lines(bucketed, version="v20261113")
            prefixed = StagingStorageResource(use_local=True, local_dir=str(root), prefix="sub")
            with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
                source.capture_narn_rail_lines(prefixed, version="v20261113")

    def test_invalid_version_is_rejected(self):
        from dagster_hifld.resources import StagingStorageResource
        with tempfile.TemporaryDirectory() as temporary:
            storage = StagingStorageResource(use_local=True, local_dir=temporary, prefix="")
            with self.assertRaisesRegex(ValueError, "invalid_version"):
                source.capture_narn_rail_lines(storage, version="v/invalid")

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          "geoparquet/north-american-rail-network-lines.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/north-american-rail-network-lines.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError,
                                            "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, source.DATASET_SLUG,
                                        source.FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)

    def _seal_catalog_cache_flow(self):
        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        fields, features, fixture_captures, schema, geom, counts, bounds, extras = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            body = bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-13T00:00:00+00:00",
                       "status": 200, "complete_body": True, "wire_bytes": len(body), "wire_sha256": digest,
                       "bytes": len(body), "sha256": digest, "content_encoding": None,
                       "content_length": str(len(body)), "error_code": None}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        class EmptyClient:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        with tempfile.TemporaryDirectory(prefix="narn-rail-lines-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema, geom, counts, bounds, extras)), \
                 patch.object(shared, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_narn_rail_lines(storage, version="vfixture",
                                                           capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 6)
                self.assertEqual(candidate["source_null_geometry_count"], counts[0])
                self.assertEqual(candidate["source_invalid_geometry_count"], counts[1])
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                for name, body in bodies.items():
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.body"), body)
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.wire"), body)
                snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                         candidate["version"])
                manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                         candidate["version"]).metadata
                summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                   candidate["version"], source.DATASET_SLUG,
                                                   source_metadata=manifest)
                self.assertEqual(summary.quality_manifest["null_geometry_count"], 1)
                self.assertFalse(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG,
                                       file_slug=source.FILE_SLUG, version=candidate["version"],
                                       quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_narn_rail_lines(storage, version="vfixture",
                                                        capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        self._seal_catalog_cache_flow()

    def test_packaged_runtime_without_git_seals_then_reuses_candidate(self):
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="narn-rail-lines-packaged-") as temporary:
            packaged = Path(temporary) / "runtime"
            packaged.mkdir()
            shutil.copytree(repo_root / "src/dagster_hifld", packaged / "src/dagster_hifld",
                            ignore=shutil.ignore_patterns("__pycache__"))
            for name in ("pyproject.toml", "uv.lock"):
                shutil.copy2(repo_root / name, packaged / name)
            self.assertFalse((packaged / ".git").exists())
            packaged_source = packaged / "src/dagster_hifld/sources/narn_rail_lines.py"
            with patch.object(source, "__file__", str(packaged_source)):
                identity = source._code_identity()
                self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
                self.assertEqual(identity["working_tree_status"], "unknown")
                self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
                self._seal_catalog_cache_flow()


if __name__ == "__main__":
    unittest.main()

"""Focused contract and repeat-acquisition tests for the BOEM OCS lease-block adapter."""
from __future__ import annotations

import copy
import hashlib
import json
import shutil
import tempfile
import unittest
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetCheckSeverity, DagsterInstance, materialize
from dagster_hifld.assets import boem_ocs_lease_blocks as assets
from dagster_hifld.capture_completeness import approved_baseline_drift

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import boem_ocs_lease_blocks as blocks
from dagster_hifld.sources import capture_common


def _body(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


BLOCK_RING = [[-93.109375, 29.61602783203125], [-93.10858154296875, 29.575439453125],
              [-93.15496826171875, 29.5748291015625], [-93.1557, 29.6154],
              [-93.109375, 29.61602783203125]]


def _fixture(extra: bool = False):
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False, "length": 0},
        {"name": "PROT_NUMBER", "type": "esriFieldTypeString", "nullable": True, "length": 7},
        {"name": "BLOCK_NUMBER", "type": "esriFieldTypeString", "nullable": True, "length": 6},
        {"name": "BLK_FED_APRV_DT", "type": "esriFieldTypeString", "nullable": True, "length": 11},
        {"name": "Shape__Area", "type": "esriFieldTypeDouble", "nullable": True, "length": 0},
        {"name": "Shape__Length", "type": "esriFieldTypeDouble", "nullable": True, "length": 0},
    ]
    ids = [643213655, 643213656, 643213657, 644434609]
    attrs_rows = [
        {"PROT_NUMBER": "LA1", "BLOCK_NUMBER": "   71", "BLK_FED_APRV_DT": "01-JUL-2011",
         "Shape__Area": 0.00188226, "Shape__Length": 0.17392168171682998},
        {"PROT_NUMBER": "LA1", "BLOCK_NUMBER": "   72", "BLK_FED_APRV_DT": "01-JUL-2011",
         "Shape__Area": 0.00188832, "Shape__Length": 0.17421980968199502},
        {"PROT_NUMBER": "NN08-01", "BLOCK_NUMBER": " 7101", "BLK_FED_APRV_DT": "03-JUN-2003",
         "Shape__Area": 0.0021, "Shape__Length": None},
        {"PROT_NUMBER": "NN08-01", "BLOCK_NUMBER": " 7102", "BLK_FED_APRV_DT": None,
         "Shape__Area": 0, "Shape__Length": 0},
    ]
    geometries = [
        {"rings": [BLOCK_RING]},
        {"rings": [[[ -93.2, 29.6], [ -93.16, 29.6], [ -93.16, 29.57], [ -93.2, 29.57],
                    [ -93.2, 29.6]],
                   [[ -93.1, 29.6], [ -93.05, 29.6], [ -93.05, 29.57], [ -93.1, 29.57],
                    [ -93.1, 29.6]]]},
        {"rings": [[[ -160.5, 61.0], [ -160.4, 61.0], [ -160.4, 60.9], [ -160.5, 60.9],
                    [ -160.5, 61.0]]]},
        {"rings": []},
    ]
    if extra:
        ids.append(644434610)
        attrs_rows.append({"PROT_NUMBER": "NN08-01", "BLOCK_NUMBER": " 7103",
                           "BLK_FED_APRV_DT": None, "Shape__Area": 0, "Shape__Length": 0})
        geometries.append({"rings": [[[ -93.0, 29.6], [ -92.9, 29.6], [ -92.9, 29.5],
                                      [ -93.0, 29.5], [ -93.0, 29.6]]]})
    features = [{"attributes": {"OBJECTID": oid, **attrs}, "geometry": geom}
                for oid, attrs, geom in zip(ids, attrs_rows, geometries)]
    layer = {"id": blocks.LAYER_ID, "name": blocks.LAYER_NAME,
             "geometryType": "esriGeometryPolygon", "objectIdField": "OBJECTID",
             "hasZ": False, "hasM": False, "maxRecordCount": 10000,
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"serviceDescription": "Map Service contains primary data types created by "
                                     "both the Bureau of Ocean Energy Management (BOEM) and "
                                     "the Bureau of Safety and Environmental Enforcement "
                                     "(BSEE). Contact the Geospatial Services Division at "
                                     "mappingboundarybranch@boem.gov.",
               "spatialReference": {"wkid": 4269, "latestWkid": 4269},
               "layers": [{"id": blocks.LAYER_ID, "name": blocks.LAYER_NAME}]}
    id_doc = {"objectIds": ids}
    count_doc = {"count": len(ids)}
    page = {"spatialReference": {"wkid": 4269, "latestWkid": 4269}, "features": features}
    docs = {
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "ids_before": id_doc, "ids_after": id_doc,
        "count_before": count_doc, "count_after": count_doc,
        "page_000": page,
    }
    receipt = {"status": 200, "complete_body": True, "observed_at": "2026-01-01T00:00:00Z"}
    captures = {}
    page_urls = [blocks._query(where="1=1", outFields="*", returnGeometry="true", outSR="4269",
                               orderByFields="OBJECTID ASC", resultOffset=page * blocks.PAGE_SIZE,
                               resultRecordCount=blocks.PAGE_SIZE)
                 for page in range(0, (len(ids) + blocks.PAGE_SIZE - 1) // blocks.PAGE_SIZE)]
    for key, value in docs.items():
        body = _body(value)
        url = (blocks.SERVICE_URL + "?f=pjson" if key.startswith("service_") else
               blocks.LAYER_URL + "?f=pjson" if key.startswith("layer_") else
               blocks.LAYER_URL + "/query?where=1%3D1&returnCountOnly=true&f=json"
               if key.startswith("count_") else
               blocks.LAYER_URL + "/query?where=1%3D1&returnIdsOnly=true&f=json"
               if key.startswith("ids_") else
               page_urls[int(key.split("_")[1])])
        captures[key] = (body, {**receipt, "url": url, "final_url": url, "bytes": len(body),
                                "sha256": hashlib.sha256(body).hexdigest(),
                                "wire_bytes": len(body),
                                "wire_sha256": hashlib.sha256(body).hexdigest()})
    return captures


_UNPINNED = {"EXPECTED_SCHEMA_SHA256": ""}


@contextmanager
def fixture_pins():
    """Pin every module baseline to the fixture capture's own audit values."""
    with ExitStack() as stack:
        for name, value in _UNPINNED.items():
            stack.enter_context(patch.object(blocks, name, value))
        audit = blocks._validate(_fixture())[3]
        stack.close()
        pins = {"EXPECTED_SCHEMA_SHA256": audit["source_schema_sha256"]}
        for name, value in pins.items():
            stack.enter_context(patch.object(blocks, name, value))
        yield audit


def _live_fixture(*, extra=False, removed=False):
    captures = _fixture(extra=extra)
    docs = {name: json.loads(body) for name, (body, _) in captures.items()}
    features = docs.pop('page_000')['features']
    if removed:
        features.pop()
    ids = [row['attributes']['OBJECTID'] for row in features]
    for name in ('ids_before', 'ids_after'):
        docs[name] = {'objectIds': ids}
    for name in ('count_before', 'count_after'):
        docs[name] = {'count': len(ids)}
    for name in ('layer_before', 'layer_after'):
        docs[name]['maxRecordCount'] = blocks.PAGE_SIZE
    for index, start in enumerate(range(0, len(features), blocks.PAGE_SIZE)):
        docs[f'page_{index:03d}'] = {'spatialReference': {'wkid': 4269},
                                  'features': features[start:start + blocks.PAGE_SIZE]}
    return docs


def _write_capture(root, captures):
    urls = dict(blocks._capture_names(json.loads(captures['ids_before'][0])['objectIds']))
    for name, (body, receipt) in captures.items():
        (root / f'{name}.body').write_bytes(body)
        (root / f'{name}.wire').write_bytes(body)
        (root / f'{name}.receipt.json').write_bytes(_body({**receipt, 'method': 'GET',
            'url': urls[name], 'final_url': urls[name]}))
    (root / 'capture-summary.json').write_bytes(b'{"complete":true}')


def _client(docs, *, torn=False, missing_page_row=False):
    ids = docs['ids_before']['objectIds']
    urls = dict(blocks._capture_names(ids))
    payloads = {url: _body(docs[name]) for name, url in urls.items()}
    counts = {}
    def respond(request):
        url = str(request.url)
        counts[url] = counts.get(url, 0) + 1
        body = payloads[url]
        if torn and url == urls['count_after'] and counts[url] == 2:
            body = _body({'count': len(ids) + 1})
        if missing_page_row and url == urls['page_000']:
            page = copy.deepcopy(docs['page_000'])
            page['features'].pop()
            body = _body(page)
        return httpx.Response(200, stream=httpx.ByteStream(body))
    return httpx.Client(transport=httpx.MockTransport(respond))


class BoemOcsLeaseBlocksTests(unittest.TestCase):
    def test_offline_loader_refuses_corrupt_wire_instead_of_rewriting_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_capture(root, _fixture())
            (root / 'page_000.wire').write_bytes(b'corrupted')
            with self.assertRaisesRegex(ValueError, 'retained_capture_receipt_invalid'):
                blocks._load_offline(root)

    def test_live_materialization_replay_and_population_changes_with_real_provenance(self):
        with tempfile.TemporaryDirectory() as tmp, fixture_pins(), \
                patch.object(blocks, 'PAGE_SIZE', 2), DagsterInstance.ephemeral() as instance:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / 'stage'))
            Path(storage.local_dir).mkdir()
            client = _client(_live_fixture())
            with patch.object(blocks.httpx, 'Client', return_value=client):
                first = blocks.capture_boem_ocs_lease_blocks(storage, capture_directory=str(Path(tmp) / 'capture'))
            prefix = first['candidate_ready'].removesuffix('/metadata/candidate_ready.json')
            original = {key: storage.read_key(key) for key in storage.list_prefix(prefix)}
            with patch.object(httpx.Client, 'send', side_effect=AssertionError('unexpected network')):
                replay = blocks.capture_boem_ocs_lease_blocks(storage,
                    offline_capture_directory=first['capture_directory'], capture_directory=str(Path(tmp) / 'replay'))
            self.assertEqual(replay['version'], first['version'])
            self.assertTrue(replay['reused'])
            client = _client(_live_fixture())
            with patch.object(blocks.httpx, 'Client', return_value=client):
                refresh = blocks.capture_boem_ocs_lease_blocks(storage, capture_directory=str(Path(tmp) / 'refresh'))
            self.assertTrue(refresh['reused'])
            self.assertEqual({key: storage.read_key(key) for key in storage.list_prefix(prefix)}, original)
            client = _client(_live_fixture(extra=True))
            with patch.object(blocks.httpx, 'Client', return_value=client):
                run = materialize([*assets.INGEST_ASSETS, *assets.ASSET_CHECKS], instance=instance,
                    resources={'staging_storage': storage}, run_config={'ops': {assets.boem_ocs_lease_blocks.op.name: {
                        'config': {'capture_directory': str(Path(tmp) / 'added')}}}})
            self.assertTrue(run.success)
            added = run.output_for_node(assets.boem_ocs_lease_blocks.op.name)
            self.assertEqual(added['feature_count'], 5)
            evaluations = {check.check_name: check for check in run.get_asset_check_evaluations()}
            self.assertTrue(evaluations['boem_ocs_lease_blocks_complete'].passed)
            warning = evaluations['boem_ocs_lease_blocks_approved_baseline_drift']
            self.assertEqual(warning.severity, AssetCheckSeverity.WARN)
            self.assertEqual(warning.metadata['comparison_status'].value, 'skipped_no_approved_baseline')
            drift = approved_baseline_drift(added, {**first, 'release_accepted': True})
            self.assertFalse(drift.passed)
            self.assertEqual(drift.metadata['feature_count_delta'].value, 1)
            client = _client(_live_fixture(removed=True))
            with patch.object(blocks.httpx, 'Client', return_value=client):
                removed = blocks.capture_boem_ocs_lease_blocks(storage, capture_directory=str(Path(tmp) / 'removed'))
            self.assertEqual(removed['feature_count'], 3)
            self.assertNotEqual(removed['version'], first['version'])
            table = pq.read_table(pa.BufferReader(storage.read_key(prefix + f'/geoparquet/{blocks.DATASET_SLUG}.parquet')))
            self.assertEqual(table['BLOCK_NUMBER'].to_pylist()[0], '   71')
            self.assertIsNone(table['geometry'].to_pylist()[-1])
            for mode in ('wire', 'forged_gzip', 'missing', 'url', 'extra'):
                directory = Path(tmp) / ('bad-' + mode)
                shutil.copytree(first['capture_directory'], directory)
                if mode == 'wire':
                    (directory / 'page_001.wire').write_bytes(b'tampered')
                elif mode == 'forged_gzip':
                    wire = b'not-valid-gzip'
                    (directory / 'page_001.wire').write_bytes(wire)
                    path = directory / 'page_001.receipt.json'
                    receipt = json.loads(path.read_bytes())
                    receipt.update(content_encoding='gzip', wire_bytes=len(wire), wire_sha256=blocks._sha(wire))
                    path.write_bytes(_body(receipt))
                elif mode == 'missing':
                    (directory / 'page_001.body').unlink()
                elif mode == 'url':
                    path = directory / 'page_001.receipt.json'
                    receipt = json.loads(path.read_bytes())
                    receipt.update(url='https://gis.boem.gov/unrelated', final_url='https://gis.boem.gov/unrelated')
                    path.write_bytes(_body(receipt))
                else:
                    (directory / 'page_999.body').write_bytes(b'{}')
                with self.subTest(replay=mode), patch.object(httpx.Client, 'send', side_effect=AssertionError('unexpected network')):
                    with self.assertRaises((ValueError, OSError)):
                        blocks.capture_boem_ocs_lease_blocks(storage, offline_capture_directory=str(directory),
                            capture_directory=str(Path(tmp) / 'refused-replay'))
            original_manifest = storage.read_key(prefix + '/metadata/source_manifest.json')
            storage.write_key(prefix + '/metadata/source_manifest.json', b'{}')
            with self.assertRaisesRegex(ValueError, 'cached_candidate_object_integrity_failed'):
                blocks.capture_boem_ocs_lease_blocks(storage, offline_capture_directory=first['capture_directory'],
                    capture_directory=str(Path(tmp) / 'conflict'))
            storage.write_key(prefix + '/metadata/source_manifest.json', original_manifest)
            state = Path(storage.local_dir) / f"_construction-state/{blocks.DATASET_SLUG}/{first['version']}.json"
            state.unlink()
            with self.assertRaisesRegex(ValueError, 'unsealed_candidate_prefix'):
                blocks.capture_boem_ocs_lease_blocks(storage, offline_capture_directory=first['capture_directory'],
                    capture_directory=str(Path(tmp) / 'partial'))

    def test_real_acquisition_rejects_torn_witness_and_missing_page_row(self):
        with tempfile.TemporaryDirectory() as tmp, fixture_pins(), patch.object(blocks, 'PAGE_SIZE', 2):
            storage = StagingStorageResource(local_dir=str(Path(tmp) / 'stage'))
            Path(storage.local_dir).mkdir()
            for mode in ('torn', 'page'):
                client = _client(_live_fixture(), torn=mode == 'torn', missing_page_row=mode == 'page')
                with self.subTest(mode=mode), patch.object(blocks.httpx, 'Client', return_value=client):
                    with self.assertRaises(ValueError):
                        blocks.capture_boem_ocs_lease_blocks(storage, capture_directory=str(Path(tmp) / mode))
            self.assertFalse(storage.list_prefix(blocks.DATASET_SLUG))
            self.assertTrue(storage.list_prefix('_quarantine/' + blocks.DATASET_SLUG))

    def test_default_capture_root_keeps_the_historical_layout(self):
        self.assertEqual(str(blocks.DEFAULT_CAPTURE_ROOT),
                         str(Path(tempfile.gettempdir()) / "boem-ocs-lease-block-captures"))

    def test_native_fields_polygon_nulls_and_audit_counts(self):
        with fixture_pins():
            _, fields, features, audit = blocks._validate(_fixture())
        self.assertEqual([field["name"] for field in fields], blocks.FIELD_NAMES)
        self.assertEqual(len(features), 4)
        self.assertEqual(audit["unique_objectid_count"], 4)
        self.assertEqual(audit["unique_block_key_count"], 4)
        self.assertEqual(audit["duplicate_block_key_count"], 0)
        self.assertEqual(audit["null_counts_by_field"]["BLK_FED_APRV_DT"], 1)
        self.assertEqual(audit["null_counts_by_field"]["Shape__Length"], 1)
        self.assertEqual(audit["null_geometry_count"], 1)
        self.assertEqual(audit["ring_count_distribution"], {1: 2, 2: 1})
        self.assertEqual(audit["source_crs_epsg"], 4269)
        self.assertEqual(audit["geometry_bounds"][0], -160.5)
        self.assertEqual(audit["geometry_bounds"][3], 61.0)

    def test_spatial_reference_alias_is_valid_but_contradictions_are_rejected(self):
        with fixture_pins():
            captures = _fixture()
            _, _, _, audit = blocks._validate(captures)
            self.assertEqual(audit["source_crs_epsg"], 4269)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["layer_before"][0])
            doc["extent"]["spatialReference"] = {"wkid": 4326, "latestWkid": 4269}
            candidate["layer_before"] = (_body(doc), candidate["layer_before"][1])
            with self.assertRaisesRegex(ValueError, "source_layer_spatial_reference_drift"):
                blocks._validate(candidate)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["page_000"][0])
            doc["spatialReference"] = {"wkid": 4326, "latestWkid": 4269}
            candidate["page_000"] = (_body(doc), candidate["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_spatial_reference_drift"):
                blocks._validate(candidate)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["page_000"][0])
            doc["features"][0]["geometry"]["spatialReference"] = {"wkid": 3857}
            candidate["page_000"] = (_body(doc), candidate["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_geometry_spatial_reference_drift"):
                blocks._validate(candidate)

    def test_missing_id_truncation_and_schema_drift_are_rejected(self):
        with fixture_pins():
            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"].pop()
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"].pop("OBJECTID")
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["OBJECTID"] = 999
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["UNEXPECTED_COLUMN"] = "x"
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_feature_field_roster_drift"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["BLOCK_NUMBER"] = 71
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_string_field_type_drift"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["Shape__Area"] = "big"
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_double_field_type_drift"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"] = {"x": -93.1, "y": 29.6}
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_geometry_invalid"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"]["rings"][0][2][0] = -200.0
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_coordinates_invalid"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"]["rings"][0].pop(-1)
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_ring_unclosed"):
                blocks._validate(captures)

            # Nested (hole) rings fail closed: the producer publishes disjoint
            # outer rings only.
            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            inner_ring = [[-93.12, 29.60], [-93.12, 29.59], [-93.11, 29.59],
                          [-93.11, 29.60], [-93.12, 29.60]]
            doc["features"][0]["geometry"]["rings"] = [BLOCK_RING, inner_ring]
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_nested_rings_unexpected"):
                blocks._validate(captures)

            # Duplicate (PROT_NUMBER, BLOCK_NUMBER) keys are allowed (the
            # producer publishes split blocks as same-key rows) and audited.
            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["BLOCK_NUMBER"] = "   72"
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            _, _, _, audit = blocks._validate(captures)
            self.assertEqual(audit["duplicate_block_key_count"], 1)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["exceededTransferLimit"] = True
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_error_or_truncation"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][1]["attributes"]["OBJECTID"], doc["features"][0]["attributes"]["OBJECTID"] = (
                doc["features"][0]["attributes"]["OBJECTID"], doc["features"][1]["attributes"]["OBJECTID"])
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_order_drift"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["layer_before"][0])
            doc["fields"][1]["type"] = "esriFieldTypeInteger"
            captures["layer_before"] = (_body(doc), captures["layer_before"][1])
            with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
                blocks._validate(captures)

    def test_upstream_added_feature_passes_and_warns_on_unapproved_drift(self):
        from dagster_hifld.assets.boem_ocs_lease_blocks import (
            boem_ocs_lease_blocks_approved_baseline_drift,
        )
        with fixture_pins():
            _, _, features, audit = blocks._validate(_fixture(extra=True))
        self.assertEqual(len(features), 5)
        self.assertTrue(audit["source_population_complete"])
        drift = boem_ocs_lease_blocks_approved_baseline_drift({
            "dataset_slug": blocks.DATASET_SLUG, "file_slug": blocks.FILE_SLUG,
            "feature_count": 5, "source_id_sha256": audit["source_id_sha256"],
            "release_accepted": False,
        })
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value,
                         "skipped_no_approved_baseline")

    def test_same_content_reuses_original_receipts_and_changed_content_versions(self):
        fixture = _fixture()
        stable_identity = {"code_commit": "a" * 40, "files": {"x.py": "b" * 64}}

        def parquet(features, _fields, _root):
            return b"candidate:" + hashlib.sha256(_body([f["attributes"] for f in features])).digest()

        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as offline:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with fixture_pins():
                def run(observed_at, mutate=False):
                    captures = copy.deepcopy(fixture)
                    for _, receipt in captures.values():
                        receipt["observed_at"] = observed_at
                    if mutate:
                        doc = json.loads(captures["page_000"][0])
                        doc["features"][0]["attributes"]["BLK_FED_APRV_DT"] = "02-JUL-2011"
                        body = _body(doc)
                        receipt = captures["page_000"][1]
                        receipt.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest(),
                                       wire_bytes=len(body),
                                       wire_sha256=hashlib.sha256(body).hexdigest())
                        captures["page_000"] = (body, receipt)
                    extra = []
                    _write_capture(Path(offline), captures)
                    with patch.object(blocks, "_load_offline", return_value=captures), \
                            patch.object(blocks, "_geoparquet", side_effect=parquet), \
                            patch.object(blocks, "processing_identity",
                                         return_value=stable_identity), \
                            patch.object(blocks, "validate_construction_code_identity",
                                         return_value=None), \
                            patch.object(capture_common, "validate_construction_code_identity",
                                         return_value=None):
                        with ExitStack() as stack:
                            for extra_patch in extra:
                                stack.enter_context(extra_patch)
                            return blocks.capture_boem_ocs_lease_blocks(
                                storage, offline_capture_directory=offline)

                first = run("2026-01-01T00:00:00Z")
                first_manifest = json.loads(storage.read_key(
                    first["candidate_ready"].replace("metadata/candidate_ready.json",
                                                     "metadata/source_manifest.json")))
                second = run("2026-01-02T00:00:00Z")
                second_manifest = json.loads(storage.read_key(
                    second["candidate_ready"].replace("metadata/candidate_ready.json",
                                                      "metadata/source_manifest.json")))
                self.assertEqual(first["version"], second["version"])
                self.assertTrue(second["reused"])
                self.assertEqual(first_manifest["inputs"], second_manifest["inputs"])
                changed = run("2026-01-03T00:00:00Z", mutate=True)
                self.assertNotEqual(first["version"], changed["version"])

    def test_manifest_records_first_party_host_and_pending_acceptance(self):
        fixture = _fixture()
        stable_identity = {"code_commit": "a" * 40, "files": {"x.py": "b" * 64}}
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as offline:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            _write_capture(Path(offline), fixture)
            with fixture_pins():
                with patch.object(blocks, "_load_offline", return_value=copy.deepcopy(fixture)), \
                        patch.object(blocks, "_geoparquet", return_value=b"candidate"), \
                        patch.object(blocks, "processing_identity", return_value=stable_identity), \
                        patch.object(blocks, "validate_construction_code_identity",
                                     return_value=None), \
                        patch.object(capture_common, "validate_construction_code_identity",
                                     return_value=None):
                    result = blocks.capture_boem_ocs_lease_blocks(
                        storage, offline_capture_directory=offline)
            base = result["candidate_ready"].replace("metadata/candidate_ready.json", "metadata/")
            manifest = json.loads(storage.read_key(base + "source_manifest.json"))
            self.assertFalse(manifest["source_authority_accepted"])
            self.assertTrue(manifest["source_host_first_party"])
            self.assertTrue(all(host == "gis.boem.gov" for host in
                                [inp["host"] for inp in manifest["inputs"]]))
            self.assertIn("no ArcGIS Online hosting account involved",
                          manifest["source_authority_note"])
            self.assertIn("geoseas.geoplatform.gov", manifest["lineage"])
            self.assertIn("maintained producer REST publication", manifest["lineage"])
            self.assertIsNone(manifest["source_release_date"])
            self.assertIn("no dated source release",
                          manifest["inputs"][0]["release_date_unknown_reason"])
            audit = json.loads(storage.read_key(base + "source_audit.json"))
            self.assertFalse(audit["source_authority_accepted"])
            ready = json.loads(storage.read_key(base + "candidate_ready.json"))
            self.assertFalse(ready["source_authority_accepted"])
            self.assertFalse(result["source_authority_accepted"])

    def test_geoparquet_roundtrip_preserves_native_values_and_geometry(self):
        with fixture_pins():
            fixture = _fixture()
            _, fields, features, _ = blocks._validate(copy.deepcopy(fixture))
            with tempfile.TemporaryDirectory() as tmp:
                body = blocks._geoparquet(features, fields, Path(tmp))
        import io
        import pyarrow.parquet as pq
        from shapely import from_wkb
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.column_names, [*blocks.FIELD_NAMES, "geometry"])
        self.assertEqual(table.num_rows, 4)
        self.assertEqual(table.column("OBJECTID").to_pylist(),
                         [643213655, 643213656, 643213657, 644434609])
        # Space-padded block numbers survive the round trip verbatim.
        self.assertEqual(table.column("BLOCK_NUMBER").to_pylist(),
                         ["   71", "   72", " 7101", " 7102"])
        self.assertEqual(table.column("BLK_FED_APRV_DT").to_pylist(),
                         ["01-JUL-2011", "01-JUL-2011", "03-JUN-2003", None])
        self.assertEqual(table.column("Shape__Length").to_pylist()[2], None)
        geometries = table.column("geometry").to_pylist()
        self.assertIsNone(geometries[3])
        polygon = from_wkb(geometries[0])
        self.assertEqual(polygon.geom_type, "Polygon")
        first_ring = list(polygon.exterior.coords)
        self.assertEqual([round(c, 7) for c in first_ring[0]],
                         [round(c, 7) for c in first_ring[-1]])
        self.assertEqual(round(first_ring[0][0], 9), -93.109375)
        multipart = from_wkb(geometries[1])
        self.assertEqual(multipart.geom_type, "MultiPolygon")
        self.assertEqual(len(multipart.geoms), 2)
        self.assertEqual(round(list(multipart.geoms[1].exterior.coords)[0][0], 9), -93.1)
        geo_metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                         ["Polygon", "MultiPolygon"])
        self.assertEqual(geo_metadata["columns"]["geometry"]["crs"].get("name", ""), "NAD83")

    def test_transient_null_roster_answers_are_refetched_with_witnesses(self):
        import httpx

        class _Stream(httpx.SyncByteStream):
            def __init__(self, payload):
                self.body = payload

            def __iter__(self):
                yield self.body

            def close(self):
                return None

        null_answer = _body({"objectIdFieldName": "OBJECTID", "objectIds": None})
        real_answer = _body({"objectIdFieldName": "OBJECTID",
                             "objectIds": [643213655, 643213656]})
        url = blocks._query(where="1=1", returnIdsOnly="true")
        responses = iter([null_answer, real_answer])

        def respond(request):
            return httpx.Response(200, stream=_Stream(next(responses)),
                                  headers={"content-type": "application/json"})

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            client = httpx.Client(transport=httpx.MockTransport(respond))
            with patch.object(blocks, "_BRACKET_RETRY_BASE_DELAY_SECONDS", 0.0):
                body, receipt = blocks._acquire_document(client, root, "ids_after", url)
            self.assertEqual(json.loads(body)["objectIds"], [643213655, 643213656])
            self.assertTrue(receipt["complete_body"])
            witnesses = list(root.glob("ids_after.error-*.body"))
            self.assertEqual(len(witnesses), 1)
            self.assertEqual(witnesses[0].read_bytes(), null_answer)

            responses = iter([null_answer] * 4)
            client = httpx.Client(transport=httpx.MockTransport(respond))
            with patch.object(blocks, "_BRACKET_RETRY_BASE_DELAY_SECONDS", 0.0):
                body, _receipt = blocks._acquire_document(client, root, "ids_after", url)
            self.assertEqual(json.loads(body)["objectIds"], None)
            self.assertEqual(len(list(root.glob("ids_after.error-*.body"))), 5)
            with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
                blocks._validate({"ids_before": (_body({"objectIds": [643213655]}), {}),
                                  "ids_after": (body, {}),
                                  "count_before": (_body({"count": 1}), {}),
                                  "count_after": (_body({"count": 1}), {}),
                                  "service_before": (_body({"layers": []}), {}),
                                  "service_after": (_body({"layers": []}), {}),
                                  "layer_before": (_body({"layers": []}), {}),
                                  "layer_after": (_body({"layers": []}), {})})

    def test_entrypoint_quarantines_acquisition_validation_failures(self):
        fixture = _fixture()
        doc = json.loads(fixture["page_000"][0])
        doc["features"].pop()
        fixture["page_000"] = (_body(doc), fixture["page_000"][1])
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as offline:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            _write_capture(Path(offline), fixture)
            with fixture_pins(), patch.object(blocks, "_load_offline", return_value=fixture):
                with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
                    blocks.capture_boem_ocs_lease_blocks(storage, offline_capture_directory=offline)
            quarantined = sorted(str(path.relative_to(tmp)) for path in Path(tmp).rglob("*")
                                 if path.is_file())
            self.assertTrue(quarantined)
            self.assertTrue(all(name.startswith(f"_quarantine/{blocks.DATASET_SLUG}/")
                                for name in quarantined), quarantined)
            token_root = Path(tmp) / "_quarantine" / blocks.DATASET_SLUG
            tokens = [path for path in token_root.iterdir() if path.is_dir()]
            self.assertEqual(len(tokens), 1)
            failure = json.loads((tokens[0] / "metadata" / "failure.json").read_bytes())
            self.assertEqual(failure["reason_code"], "source_feature_population_incomplete")
            self.assertEqual(sorted(failure["captured_names"]), sorted(fixture))
            for name in fixture:
                self.assertTrue((tokens[0] / "raw" / f"{name}.body").is_file(), name)
                self.assertTrue((tokens[0] / "raw" / f"{name}.wire").is_file(), name)
                self.assertTrue((tokens[0] / "raw" / f"{name}.receipt.json").is_file(), name)


if __name__ == "__main__":
    unittest.main()

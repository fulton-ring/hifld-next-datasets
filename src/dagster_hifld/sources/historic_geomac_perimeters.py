"""Faithful NIFC GeoMAC 2000-2018 historic perimeters draft from the official gdb.

The combined layer US_HIST_FIRE_PERIMTRS_2000_2018_DD83 of the NIFC-published
File Geodatabase archive is captured from the ArcGIS Online item download,
identity-gated on the NIFC owner/organization, pinned on the measured archive
bytes/sha256 (the source publishes no hash), and preserved in native NAD83
(EPSG:4269) without reprojection or geometry repair. The 881 known invalid
producer geometries are retained unrepaired and keep the release blocked. The
same-org hosted Feature Service corroborates count and a sampled
outSR=4269 spot-check with a documented reprojection tolerance; server
reprojection is never labeled bitwise-native. Publication, contract, rights
and baseline acceptance remain unapproved.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import tempfile
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.sources.epa_frs_common import (
    STAGED_RELEASE_DERIVATIVE_NAMES,
    candidate_prefix,
    content_derived_version,
    processing_identity as packaged_processing_identity,
    validate_staged_release_derivatives,
)

SLUG = 'historic-perimeters-combined-2000-2018-geomac'
GDB_ITEM_ID = '5b3ff19978be49208d41a9d9a461ecfb'
SERVICE_ITEM_ID = 'ef25d7e8c9f3499ba9e3d8e09606e488'
OWNER = 'NIFC_Admin'
ORG = 'T4QMspbfLg3qTGWY'
GDB_ITEM_URL = f'https://www.arcgis.com/sharing/rest/content/items/{GDB_ITEM_ID}?f=json'
SERVICE_ITEM_URL = f'https://www.arcgis.com/sharing/rest/content/items/{SERVICE_ITEM_ID}?f=json'
SERVICE_URL = ('https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/'
               'Historic_Geomac_Perimeters_Combined_2000_2018/FeatureServer')
LAYER_URL = SERVICE_URL + '/0'
DOWNLOAD_URL = f'https://www.arcgis.com/sharing/rest/content/items/{GDB_ITEM_ID}/data'
URLS = {
    'gdb_item': GDB_ITEM_URL,
    'service_item': SERVICE_ITEM_URL,
    'layer': LAYER_URL + '?f=json',
    'count': LAYER_URL + '/query?where=1%3D1&returnCountOnly=true&f=json',
    'archive': DOWNLOAD_URL,
    'spotcheck': LAYER_URL + '/query?objectIds=1%2C4000%2C12000%2C20000%2C23776'
                                '&outFields=*&returnGeometry=true&outSR=4269&f=json',
}
ARCHIVE_NAME = 'Historic_Geomac_Perimeters_All_Years_2000_2018.gdb.zip'
# Measured identity pins (the NIFC item publishes no source hash): byte size
# confirmed by the item JSON, download content-length and repeated captures.
ARCHIVE_BYTES = 625_319_314
ARCHIVE_SHA256 = 'c5b65a2d3f1f6a55c7ec867c669282e628aa64c9c3a54a5a8b7b1db1dffdf6d1'
ZIP_MEMBERS = 123
ZIP_UNCOMPRESSED_BYTES = 752_087_361
GDB_TOP_DIR = 'Historic_Geomac_Perimeters_All_Years_2000_2018.gdb'
COMBINED_LAYER = 'US_HIST_FIRE_PERIMTRS_2000_2018_DD83'
COMBINED_FEATURES = 23_776
PER_YEAR_LAYERS = {
    'US_HIST_FIRE_PERIM_2000_DD83': 4055, 'US_HIST_FIRE_PERIM_2001_DD83': 1343,
    'US_HIST_FIRE_PERIM_2002_DD83': 4699, 'US_HIST_FIRE_PERIM_2003_DD83': 2293,
    'US_HIST_FIRE_PERIM_2004_DD83': 1653, 'US_HIST_FIRE_PERIM_2005_DD83': 2896,
    'US_HIST_FIRE_PERIM_2006_DD83': 2721, 'US_HIST_FIRE_PERIM_2007_DD83': 3545,
    'US_HIST_FIRE_PERIM_2008_DD83': 6054, 'US_HIST_FIRE_PERIM_2009_DD83': 1863,
    'US_HIST_FIRE_PERIM_2010_DD83': 1791, 'US_HIST_FIRE_PERIM_2011_DD83': 4467,
    'US_HIST_FIRE_PERIM_2012_DD83': 5033, 'US_HIST_FIRE_PERIM_2013_DD83': 4041,
    'US_HIST_FIRE_PERIM_2014_DD83': 3666, 'US_HIST_FIRE_PERIM_2015_DD83': 7609,
    'US_HIST_FIRE_PERIM_2016_DD83': 4410, 'US_HIST_FIRE_PERIM_2017_DD83': 7418,
    'US_HIST_FIRE_PERIM_2018_DD83': 6046,
}
PER_YEAR_TOTAL = 75_603
# 22 native attribute fields in producer order with pinned physical types.
EXPECTED_FIELDS = [
    ('agency', 'string'), ('comments', 'string'), ('mapmethod', 'string'),
    ('datecurrent', 'timestamp'), ('uniquefireidentifier', 'string'), ('fireyear', 'int32'),
    ('incidentname', 'string'), ('pooownerunit', 'string'), ('perimeterdatetime', 'timestamp'),
    ('gisacres', 'double'), ('complexname', 'string'), ('firecode', 'string'),
    ('complexparentirwinid', 'string'), ('pooresponsibleunit', 'string'), ('state', 'string'),
    ('inciwebid', 'string'), ('localincidentidentifier', 'string'), ('irwinid', 'string'),
    ('incomplex', 'string'), ('complexfirecode', 'string'),
    ('shape_Length', 'double'), ('shape_Area', 'double'),
]
DATE_FIELDS = ('datecurrent', 'perimeterdatetime')
EXPECTED_NULLS = {
    'OBJECTID': 0, 'agency': 0, 'comments': 10431, 'mapmethod': 1618, 'datecurrent': 2108,
    'uniquefireidentifier': 0, 'fireyear': 0, 'incidentname': 149, 'pooownerunit': 19904,
    'perimeterdatetime': 1, 'gisacres': 0, 'complexname': 21501, 'firecode': 9507,
    'complexparentirwinid': 23214, 'pooresponsibleunit': 1383, 'state': 0, 'inciwebid': 21519,
    'localincidentidentifier': 3870, 'irwinid': 17399, 'incomplex': 0, 'complexfirecode': 22003,
    'shape_Length': 0, 'shape_Area': 0,
}
EXPECTED_BOUNDS = [-178.8414555557593, 3.3864261395352173, -65.33316018923853, 70.15916168068225]
EXPECTED_INVALID_GEOMETRY_COUNT = 881
EXPECTED_EMPTY_GEOMETRY_COUNT = 0
EXPECTED_VERTICES = 13_400_285
EXPECTED_UNIQUE_FIRE_IDS = 19_545
EXPECTED_UNIQUE_FIRE_ID_DUPLICATES = 4_231
EXPECTED_FIREYEAR_RANGE = (2000, 2018)
EXPECTED_DATECURRENT_RANGE = ('2002-04-26T00:00:00Z', '3013-03-29T00:00:00Z')
EXPECTED_PERIMETERDATETIME_RANGE = ('1712-08-01T00:00:00Z', '2018-12-11T00:00:00Z')
EXPECTED_AGENCY_CRLF_COUNT = 1
SPOT_OBJECTIDS = (1, 4000, 12000, 20000, 23776)
# Server outSR=4269 reprojection noise was measured at ~3e-12 deg; the gate is
# an order of magnitude above the observation and far below real geometry drift.
SPOT_MAX_DEVIATION_DEGREES = 1e-9
MAX_JSON_CAPTURE_BYTES = 2_000_000
# The hosted Feature Service spells the computed shape fields with a double
# underscore and adds OBJECTID; the two representations are corroborated, not merged.
SERVICE_FIELDS = [
    {'name': 'OBJECTID', 'type': 'esriFieldTypeOID'},
    {'name': 'agency', 'type': 'esriFieldTypeString', 'length': 15},
    {'name': 'comments', 'type': 'esriFieldTypeString', 'length': 254},
    {'name': 'mapmethod', 'type': 'esriFieldTypeString', 'length': 25},
    {'name': 'datecurrent', 'type': 'esriFieldTypeDate', 'length': 8},
    {'name': 'uniquefireidentifier', 'type': 'esriFieldTypeString', 'length': 22},
    {'name': 'fireyear', 'type': 'esriFieldTypeInteger'},
    {'name': 'incidentname', 'type': 'esriFieldTypeString', 'length': 50},
    {'name': 'pooownerunit', 'type': 'esriFieldTypeString', 'length': 6},
    {'name': 'perimeterdatetime', 'type': 'esriFieldTypeDate', 'length': 8},
    {'name': 'gisacres', 'type': 'esriFieldTypeDouble'},
    {'name': 'complexname', 'type': 'esriFieldTypeString', 'length': 50},
    {'name': 'firecode', 'type': 'esriFieldTypeString', 'length': 4},
    {'name': 'complexparentirwinid', 'type': 'esriFieldTypeString', 'length': 250},
    {'name': 'pooresponsibleunit', 'type': 'esriFieldTypeString', 'length': 6},
    {'name': 'state', 'type': 'esriFieldTypeString', 'length': 2},
    {'name': 'inciwebid', 'type': 'esriFieldTypeString', 'length': 10},
    {'name': 'localincidentidentifier', 'type': 'esriFieldTypeString', 'length': 10},
    {'name': 'irwinid', 'type': 'esriFieldTypeGUID', 'length': 38},
    {'name': 'incomplex', 'type': 'esriFieldTypeString', 'length': 1},
    {'name': 'complexfirecode', 'type': 'esriFieldTypeString', 'length': 4},
    {'name': 'shape__Area', 'type': 'esriFieldTypeDouble'},
    {'name': 'shape__Length', 'type': 'esriFieldTypeDouble'},
]
PRODUCER = 'National Interagency Fire Center (NIFC) publishing U.S. Geological Survey GeoMAC historic wildfire perimeters'
PUBLISHER = 'National Interagency Fire Center ArcGIS Online (owner NIFC_Admin, organization T4QMspbfLg3qTGWY)'
DISK_FLOOR_BYTES = 4_000_000_000


class GeoMACQuarantineError(ValueError):
    """Raised instead of publishing when any pinned source contract drifts."""


def encode(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def strict_json(body: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate json key')
            result[key] = value
        return result
    try:
        return json.loads(body, object_pairs_hook=pairs, parse_constant=lambda token: (_ for _ in ()).throw(ValueError('non-finite json')))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError('invalid json capture') from exc


def require_local(storage: StagingStorageResource) -> None:
    if isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise GeoMACQuarantineError('draft_requires_explicit_isolated_local_staging')


def immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    before = storage.object_snapshot(key)
    if before is not None:
        if storage.read_key(key) != body or storage.object_snapshot(key) != before:
            raise GeoMACQuarantineError(f'immutable_staging_conflict:{key}')
    else:
        storage.write_key_if_unchanged(key, body, None)


def quarantine(storage: StagingStorageResource, body: bytes, receipt: dict, reason: str) -> str:
    prefix = f'_quarantine/{SLUG}/{sha(body)}/{uuid4().hex}'
    immutable(storage, f'{prefix}/response.body', body[:1_048_576])
    immutable(storage, f'{prefix}/receipt.json', encode(receipt))
    immutable(storage, f'{prefix}/report.json', encode({'approved': False, 'status': 'quarantined', 'reason': reason}))
    return prefix


def _redact_final_url(final_url: str) -> tuple[str, str | None]:
    """Drop the signed CDN query while pinning its hash for the receipt."""
    split = urlsplit(final_url)
    if split.query:
        redacted = split._replace(query='').geturl()
        return redacted, sha(split.query.encode('utf-8'))
    return final_url, None


def validate_receipt(body: bytes, receipt: dict, url: str) -> None:
    if (receipt.get('requested_url') != url or type(receipt.get('status')) is not int
            or receipt['status'] != 200 or type(receipt.get('byte_length')) is not int
            or receipt['byte_length'] != len(body) or receipt.get('sha256') != sha(body)):
        raise GeoMACQuarantineError('capture_receipt_url_status_hash_length_mismatch')
    timestamp = receipt.get('observed_at')
    if not isinstance(timestamp, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)', timestamp):
        raise GeoMACQuarantineError('capture_receipt_requires_strict_aware_iso_timestamp')
    if datetime.fromisoformat(timestamp.replace('Z', '+00:00')).tzinfo is None:
        raise GeoMACQuarantineError('capture_receipt_requires_timezone')
    final_url = receipt.get('final_url')
    if not isinstance(final_url, str) or urlsplit(final_url).username is not None:
        raise GeoMACQuarantineError('capture_receipt_final_url_invalid')
    if url == DOWNLOAD_URL:
        # The archive download redirects (302) to a short-lived signed CDN URL;
        # the pinned requested URL is retained, the redirect target is redacted.
        if urlsplit(final_url).hostname != 'www.arcgis.com' or urlsplit(final_url).query:
            raise GeoMACQuarantineError('archive_redirect_target_unpinned_or_signed_query_retained')
        if len(body) != ARCHIVE_BYTES or sha(body) != ARCHIVE_SHA256:
            raise GeoMACQuarantineError('pinned_archive_content_drift')
    else:
        if final_url != url:
            raise GeoMACQuarantineError('capture_redirect_not_pinned')
        if len(body) > MAX_JSON_CAPTURE_BYTES:
            raise GeoMACQuarantineError('capture_size_limit')
        strict_json(body)


def _live_capture(storage: StagingStorageResource, url: str) -> tuple[bytes, dict]:
    receipt = {'requested_url': url, 'observed_at': datetime.now(timezone.utc).isoformat(),
               'attempt': 1}
    response = None
    body = b''
    try:
        with httpx.Client(follow_redirects=True, timeout=120,
                          headers={'Accept-Encoding': 'identity',
                                   'User-Agent': 'hifld-next-datasets/geomac-historic-perimeters-draft'}) as client:
            with client.stream('GET', url) as response:
                receipt.update(status=response.status_code,
                               content_length_header=response.headers.get('content-length'))
                final_url = str(response.url)
                if url == DOWNLOAD_URL:
                    # Only the archive download redirects; its short-lived
                    # signed CDN query is dropped while pinning its hash.
                    final_url, query_sha = _redact_final_url(final_url)
                    receipt.update(final_url=final_url)
                    if query_sha is not None:
                        receipt['final_url_query_sha256'] = query_sha
                else:
                    # Pinned control URLs carry the requested query verbatim.
                    receipt.update(final_url=final_url)
                chunks = []
                for block in response.iter_bytes(chunk_size=1 << 20):
                    chunks.append(block)
                    total = sum(len(c) for c in chunks)
                    if total > (ARCHIVE_BYTES if url == DOWNLOAD_URL else MAX_JSON_CAPTURE_BYTES):
                        raise GeoMACQuarantineError('response_size_limit')
                body = b''.join(chunks)
                del chunks
                receipt.update(byte_length=len(body), sha256=sha(body),
                               wire_byte_length=response.num_bytes_downloaded)
                validate_receipt(body, receipt, url)
                if (response.headers.get('content-length') is not None
                        and int(response.headers['content-length']) != response.num_bytes_downloaded):
                    raise GeoMACQuarantineError('incomplete_content_length')
    except (httpx.HTTPError, OSError, ValueError, TypeError) as exc:
        receipt.update(byte_length=len(body), sha256=sha(body), partial=True)
        location = quarantine(storage, body, receipt, f'{type(exc).__name__}:{exc}')
        raise GeoMACQuarantineError(f'bounded_fetch_failed:{exc}; retained:{location}') from exc
    return body, receipt


def acquire(storage: StagingStorageResource, capture_dir: Path | None) -> dict:
    """Live-capture the roster, or replay a retained capture directory."""
    require_local(storage)
    captures = {}
    for name, url in URLS.items():
        if capture_dir is not None:
            body = b''
            receipt_body = b''
            receipt = {'capture_path': str(capture_dir), 'requested_url': url}
            try:
                with (capture_dir / (name + '.body')).open('rb') as stream:
                    body = stream.read(ARCHIVE_BYTES + 1 if url == DOWNLOAD_URL else MAX_JSON_CAPTURE_BYTES + 1)
                receipt_body = (capture_dir / (name + '.receipt.json')).read_bytes()
                immutable(storage, f'_raw-captures/{SLUG}/{sha(body)}/receipts/{sha(receipt_body)}.body', receipt_body)
                receipt = strict_json(receipt_body)
                validate_receipt(body, receipt, url)
            except (OSError, ValueError, TypeError) as exc:
                location = quarantine(storage, body, receipt, str(exc))
                immutable(storage, f'{location}/original-receipt.body', receipt_body)
                raise GeoMACQuarantineError(f'{exc}; retained:{location}') from exc
        else:
            body, receipt = _live_capture(storage, url)
        captures[name] = (body, receipt)
        immutable(storage, f'_raw-captures/{SLUG}/{sha(body)}/{name}.body', body)
        immutable(storage, f'_raw-captures/{SLUG}/{sha(body)}/{sha(encode(receipt))}.receipt.json', encode(receipt))
    return captures


def processing_identity() -> dict:
    root = Path(__file__).parents[3]
    identity = packaged_processing_identity(root)
    validate_construction_code_identity(root, identity['code_commit'], identity['files'], identity=identity)
    organization = os.environ.get('OGR_ORGANIZE_POLYGONS', 'GDAL_DEFAULT')
    if organization.strip().upper() != 'GDAL_DEFAULT':
        # Polygon shell/hole organization must not be skipped or altered: the
        # native ring structure is preserved exactly as GDAL's default orders it.
        raise GeoMACQuarantineError('gdal_polygon_organization_must_stay_default')
    return {**identity, 'gdal': pyogrio.__gdal_version_string__,
            'pyogrio': pyogrio.__version__,
            'gdal_polygon_organization': organization,
            'verifier_sha256': verifier_implementation_sha256()}


def _parse_source_ms(value: str) -> datetime:
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def _control_documents(captures: dict) -> dict:
    docs = {name: strict_json(captures[name][0]) for name in ('gdb_item', 'service_item', 'layer', 'count')}
    gdb_item, service_item, layer, count = (docs['gdb_item'], docs['service_item'], docs['layer'], docs['count'])
    if (gdb_item.get('id') != GDB_ITEM_ID or gdb_item.get('owner') != OWNER
            or gdb_item.get('orgId') != ORG or gdb_item.get('name') != ARCHIVE_NAME
            or gdb_item.get('type') != 'File Geodatabase' or gdb_item.get('size') != ARCHIVE_BYTES):
        raise GeoMACQuarantineError('official_nifc_gdb_item_identity_changed')
    if (service_item.get('id') != SERVICE_ITEM_ID or service_item.get('owner') != OWNER
            or service_item.get('orgId') != ORG or service_item.get('url') != SERVICE_URL
            or service_item.get('type') != 'Feature Service'):
        raise GeoMACQuarantineError('official_nifc_service_item_identity_changed')
    if (layer.get('name') != COMBINED_LAYER or layer.get('geometryType') != 'esriGeometryPolygon'
            or layer.get('serviceItemId') != SERVICE_ITEM_ID):
        raise GeoMACQuarantineError('corroborating_layer_identity_changed')
    spatial = layer.get('extent', {}).get('spatialReference', {})
    if spatial.get('wkid') != 102100 or spatial.get('latestWkid') != 3857:
        raise GeoMACQuarantineError('corroborating_layer_extent_crs_changed')
    declared = [{k: v for k, v in f.items() if k in ('name', 'type', 'length')} for f in layer.get('fields', [])]
    if declared != SERVICE_FIELDS:
        raise GeoMACQuarantineError('corroborating_service_field_roster_changed')
    if count != {'count': COMBINED_FEATURES}:
        raise GeoMACQuarantineError('corroborating_count_drift')
    return docs


def inspect_source(captures: dict, workdir: Path) -> tuple[dict, dict]:
    """Census the pinned combined layer from the retained archive bytes."""
    _control_documents(captures)
    archive = captures['archive'][0]
    (workdir / ARCHIVE_NAME).write_bytes(archive)
    with zipfile.ZipFile(workdir / ARCHIVE_NAME) as archive_zip:
        members = archive_zip.infolist()
        names = [item.filename for item in members]
        if len(names) != ZIP_MEMBERS:
            raise GeoMACQuarantineError('zip_member_census_drift')
        if {name.split('/')[0] for name in names} != {GDB_TOP_DIR}:
            raise GeoMACQuarantineError('zip_top_directory_changed')
        if any(PurePosixPath(name).is_absolute() or '..' in PurePosixPath(name).parts for name in names):
            raise GeoMACQuarantineError('zip_member_path_unsafe')
        if sum(item.file_size for item in members) != ZIP_UNCOMPRESSED_BYTES:
            raise GeoMACQuarantineError('zip_uncompressed_census_drift')
        archive_zip.extractall(workdir)
    gdb = workdir / GDB_TOP_DIR
    layers = {name: kind for name, kind in pyogrio.list_layers(gdb)}
    if (len(layers) != 1 + len(PER_YEAR_LAYERS) or COMBINED_LAYER not in layers
            or set(layers) != {COMBINED_LAYER, *PER_YEAR_LAYERS}
            or set(layers.values()) != {'MultiPolygon'}):
        raise GeoMACQuarantineError('gdb_layer_roster_changed')
    per_year_census = {}
    for name in PER_YEAR_LAYERS:
        observed = pyogrio.read_info(gdb, layer=name)['features']
        per_year_census[name] = observed
        if observed != PER_YEAR_LAYERS[name]:
            raise GeoMACQuarantineError('per_year_layer_count_drift:' + name)
    if sum(per_year_census.values()) != PER_YEAR_TOTAL:
        raise GeoMACQuarantineError('per_year_total_drift')
    info = pyogrio.read_info(gdb, layer=COMBINED_LAYER)
    if (info.get('features') != COMBINED_FEATURES or info.get('crs') != 'EPSG:4269'
            or info.get('geometry_type') != 'MultiPolygon' or info.get('fid_column') != 'OBJECTID'
            or info.get('geometry_name') != 'shape' or info.get('driver') != 'OpenFileGDB'
            or list(info['fields']) != [name for name, _ in EXPECTED_FIELDS]
            or [str(dtype) for dtype in info['dtypes']] != [
                'datetime64[ms]' if name in DATE_FIELDS else
                'int32' if kind == 'int32' else 'float64' if kind == 'double' else 'object'
                for name, kind in EXPECTED_FIELDS]
            or list(info['total_bounds']) != EXPECTED_BOUNDS):
        raise GeoMACQuarantineError('combined_layer_identity_or_schema_changed')
    meta, table = pyogrio.read_arrow(gdb, layer=COMBINED_LAYER, return_fids=True, datetime_as_string=True)
    if (list(table.schema.names) != ['OBJECTID', *[name for name, _ in EXPECTED_FIELDS], 'shape']
            or [str(field.type) for field in table.schema] != ['int64', *[
                'string' if kind == 'string' else 'int32' if kind == 'int32'
                else 'double' if kind == 'double' else 'string' for name, kind in EXPECTED_FIELDS], 'binary']
            or meta.get('fid_column') != 'OBJECTID' or meta.get('geometry_name') != 'shape'):
        raise GeoMACQuarantineError('native_arrow_schema_changed')
    data = table.to_pydict()
    oids = data['OBJECTID']
    if oids != list(range(1, COMBINED_FEATURES + 1)):
        raise GeoMACQuarantineError('objectid_not_contiguous_unique_population')
    nulls = {name: sum(1 for value in data[name] if value is None)
             for name in ['OBJECTID', *[n for n, _ in EXPECTED_FIELDS]]}
    if nulls != EXPECTED_NULLS:
        raise GeoMACQuarantineError('null_census_drift')
    geometries = shapely.from_wkb(data['shape'])
    invalid = int((~shapely.is_valid(geometries)).sum())
    empty = int(shapely.is_empty(geometries).sum())
    vertices = int(shapely.get_num_coordinates(geometries).sum())
    bounds = [float(value) for value in shapely.total_bounds(geometries)]
    if (invalid != EXPECTED_INVALID_GEOMETRY_COUNT or empty != EXPECTED_EMPTY_GEOMETRY_COUNT
            or vertices != EXPECTED_VERTICES or bounds != EXPECTED_BOUNDS):
        raise GeoMACQuarantineError('native_geometry_census_drift')
    unique_fire_ids = set(data['uniquefireidentifier'])
    if (len(unique_fire_ids) != EXPECTED_UNIQUE_FIRE_IDS
            or len(data['uniquefireidentifier']) - len(unique_fire_ids) != EXPECTED_UNIQUE_FIRE_ID_DUPLICATES):
        raise GeoMACQuarantineError('unique_fire_identifier_census_drift')
    if (min(data['fireyear']), max(data['fireyear'])) != EXPECTED_FIREYEAR_RANGE:
        raise GeoMACQuarantineError('fireyear_range_drift')
    datecurrent = [value for value in data['datecurrent'] if value is not None]
    perimeterdatetime = [value for value in data['perimeterdatetime'] if value is not None]
    if (min(datecurrent), max(datecurrent)) != EXPECTED_DATECURRENT_RANGE:
        raise GeoMACQuarantineError('datecurrent_range_drift')
    if (min(perimeterdatetime), max(perimeterdatetime)) != EXPECTED_PERIMETERDATETIME_RANGE:
        raise GeoMACQuarantineError('perimeterdatetime_range_drift')
    if sum(1 for value in data['agency'] if value is not None and '\r' in value) != EXPECTED_AGENCY_CRLF_COUNT:
        raise GeoMACQuarantineError('dirty_producer_value_census_drift')
    # Corroborate the retained native rows against the same-org hosted service.
    spot = strict_json(captures['spotcheck'][0])
    spot_features = spot.get('features')
    if (spot.get('objectIdFieldName') != 'OBJECTID' or spot.get('spatialReference', {}).get('wkid') != 4269
            or not isinstance(spot_features, list) or [f['attributes']['OBJECTID'] for f in spot_features] != list(SPOT_OBJECTIDS)):
        raise GeoMACQuarantineError('spotcheck_population_or_outsr_changed')
    deviations = {}
    for feature in spot_features:
        attributes = feature.get('attributes', {})
        geometry = feature.get('geometry', {})
        rings = geometry.get('rings') if isinstance(geometry, dict) else None
        if not isinstance(rings, list) or not rings:
            raise GeoMACQuarantineError('spotcheck_geometry_missing')
        oid = attributes['OBJECTID']
        if (attributes.get('uniquefireidentifier') != data['uniquefireidentifier'][oid - 1]
                or attributes.get('fireyear') != data['fireyear'][oid - 1]
                or attributes.get('gisacres') != data['gisacres'][oid - 1]):
            raise GeoMACQuarantineError('spotcheck_attribute_drift:' + str(oid))
        service_coordinates = sorted((x, y) for ring in rings for x, y in ring)
        native_coordinates = sorted(map(tuple, shapely.get_coordinates(geometries[oid - 1])))
        if len(service_coordinates) != len(native_coordinates):
            raise GeoMACQuarantineError('spotcheck_vertex_count_drift:' + str(oid))
        deviation = max(max(abs(a[0] - b[0]), abs(a[1] - b[1]))
                        for a, b in zip(service_coordinates, native_coordinates))
        if deviation > SPOT_MAX_DEVIATION_DEGREES:
            raise GeoMACQuarantineError('spotcheck_reprojection_deviation_exceeded:' + str(oid))
        deviations[str(oid)] = deviation
    record_digest, geometry_digest = hashlib.sha256(), hashlib.sha256()
    attribute_names = [name for name, _ in EXPECTED_FIELDS]
    for index in range(COMBINED_FEATURES):
        row = {name: data[name][index] for name in attribute_names}
        record_digest.update(len(encode(row)).to_bytes(8, 'big'))
        record_digest.update(encode(row))
        geometry_digest.update(len(encode([oids[index], data['shape'][index].hex()])).to_bytes(8, 'big'))
        geometry_digest.update(encode([oids[index], data['shape'][index].hex()]))
    audit = {
        'feature_count': COMBINED_FEATURES,
        'source_id_field': 'OBJECTID',
        'source_ids_sha256': sha(encode(oids)),
        'source_record_sha256': record_digest.hexdigest(),
        'source_geometry_sha256': geometry_digest.hexdigest(),
        'source_schema_sha256': sha(encode(EXPECTED_FIELDS)),
        'source_unique_id_count': COMBINED_FEATURES,
        'source_null_census': nulls,
        'source_null_geometry_count': 0,
        'source_invalid_geometry_count': invalid,
        'source_empty_geometry_count': empty,
        'source_vertices': vertices,
        'source_bounds': bounds,
        'source_unique_fire_identifier_count': len(unique_fire_ids),
        'source_unique_fire_identifier_duplicates': EXPECTED_UNIQUE_FIRE_ID_DUPLICATES,
        'source_fireyear_range': list(EXPECTED_FIREYEAR_RANGE),
        'source_datecurrent_range': list(EXPECTED_DATECURRENT_RANGE),
        'source_perimeterdatetime_range': list(EXPECTED_PERIMETERDATETIME_RANGE),
        'source_agency_crlf_rows': EXPECTED_AGENCY_CRLF_COUNT,
        'zip_members': ZIP_MEMBERS,
        'zip_uncompressed_bytes': ZIP_UNCOMPRESSED_BYTES,
        'gdb_top_dir': GDB_TOP_DIR,
        'per_year_layer_census': per_year_census,
        'per_year_layer_total': PER_YEAR_TOTAL,
        'archive_bytes': ARCHIVE_BYTES,
        'archive_sha256': ARCHIVE_SHA256,
        'archive_sha256_is_measured_pin': True,
        'archive_redirect_host': 'www.arcgis.com',
        'source_crs': 'EPSG:4269',
        'geometry_type': 'MultiPolygon',
        'geometry_input': 'official NIFC File Geodatabase combined layer read by GDAL OpenFileGDB; native EPSG:4269 WKB retained without reprojection, repair or ring reorganization',
        'service_corroboration': {
            'service_count': COMBINED_FEATURES,
            'service_extent_wkid': 102100,
            'spot_objectids': list(SPOT_OBJECTIDS),
            'spot_max_observed_deviation_degrees': max(deviations.values()),
            'spot_tolerance_degrees': SPOT_MAX_DEVIATION_DEGREES,
            'server_reprojection_is_not_bitwise_native': True,
            'service_shape_field_naming': 'hosted service declares shape__Length/shape__Area; gdb stores shape_Length/shape_Area; representations corroborated and never merged',
        },
        'invalid_geometries_preserved_unrepaired': True,
        'release_blocked_reason': '881 self-intersecting producer geometries preserved unrepaired; publication approval pending',
        'service_modified_is_not_release_date': True,
        'baseline_comparison': 'unresolved; 2000-2018 HIFLD baseline archive bytes not accessible in current audit',
    }
    return data, audit


def candidate_table(data: dict) -> pa.Table:
    fields = [pa.field('OBJECTID', pa.int64(), nullable=False)]
    columns: list = [pa.array(data['OBJECTID'], type=pa.int64())]
    arrow_types = {'string': pa.string(), 'int32': pa.int32(), 'double': pa.float64()}
    for name, kind in EXPECTED_FIELDS:
        if kind == 'timestamp':
            column = pa.array([_parse_source_ms(value) if value is not None else None
                               for value in data[name]], type=pa.timestamp('ms', tz='UTC'))
        else:
            column = pa.array(data[name], type=arrow_types[kind])
        fields.append(pa.field(name, column.type, nullable=bool(EXPECTED_NULLS[name])))
        columns.append(column)
    fields.append(pa.field('shape', pa.binary(), nullable=False))
    columns.append(pa.array(data['shape'], type=pa.binary()))
    table = pa.Table.from_arrays(columns, schema=pa.schema(fields))
    geo = {'version': '1.1.0', 'primary_column': 'shape',
           'columns': {'shape': {'encoding': 'WKB', 'geometry_types': ['MultiPolygon'],
                                 'crs': pyproj.CRS(4269).to_json_dict(), 'bbox': EXPECTED_BOUNDS}}}
    return table.replace_schema_metadata({b'geo': encode(geo)})


def verify_candidate_readback(parquet: bytes, data: dict) -> None:
    table = pq.read_table(pa.BufferReader(pa.py_buffer(parquet)))
    if table.num_rows != COMBINED_FEATURES:
        raise GeoMACQuarantineError('candidate_row_count_drift')
    if table.schema.names != ['OBJECTID', *[name for name, _ in EXPECTED_FIELDS], 'shape']:
        raise GeoMACQuarantineError('candidate_field_roster_drift')
    if table['OBJECTID'].to_pylist() != data['OBJECTID']:
        raise GeoMACQuarantineError('candidate_objectid_drift')
    for name, kind in EXPECTED_FIELDS:
        actual = table[name].to_pylist()
        if kind == 'timestamp':
            expected = [None if value is None else _parse_source_ms(value) for value in data[name]]
        else:
            expected = data[name]
        if actual != expected:
            raise GeoMACQuarantineError('candidate_attribute_drift:' + name)
    if table['shape'].to_pylist() != data['shape']:
        raise GeoMACQuarantineError('candidate_native_geometry_drift')
    geo = json.loads(table.schema.metadata[b'geo'])
    if (geo['primary_column'] != 'shape' or geo['columns']['shape']['geometry_types'] != ['MultiPolygon']
            or geo['columns']['shape']['crs'] != pyproj.CRS(4269).to_json_dict()
            or geo['columns']['shape']['bbox'] != EXPECTED_BOUNDS):
        raise GeoMACQuarantineError('candidate_geoparquet_metadata_drift')


def _prebuilt_policy(objects: dict) -> dict:
    return {'schema_version': 1, 'formats': {'geoparquet': {
        'reason': 'Native candidate retains all 22 gdb fields, native OBJECTID and NAD83 MultiPolygon boundaries; contract, baseline, rights and release approvals remain pending.',
        'objects': [{'path': relative, **objects[relative]} for relative in sorted(objects)]}}}


def provenance(captures: dict, identity: dict, audit: dict, objects: dict) -> dict:
    if set(captures) != set(URLS):
        raise GeoMACQuarantineError('capture_roster_drift')
    for name, url in URLS.items():
        validate_receipt(*captures[name], url)
    inputs = []
    for name, (body, receipt) in captures.items():
        role = 'producer_publication' if name == 'archive' else 'official_distributor'
        record = {
            'input_id': name + ':' + sha(body), 'producer': PRODUCER,
            'host': urlsplit(receipt['requested_url']).hostname, 'role': role,
            'canonical_url': GDB_ITEM_URL if name == 'gdb_item' else SERVICE_URL,
            'retrieval_url': receipt['requested_url'], 'observed_final_url': receipt['final_url'],
            'retrieved_at': receipt['observed_at'], 'http_status': receipt['status'],
            'sha256': sha(body), 'bytes': len(body),
            'license': 'unknown',
            'license_scope': 'NIFC gdb item licenseInfo carries the USGS no-warranty distribution statement, retained verbatim in the raw gdb_item capture; rights acceptance pending.',
            'attribution': 'U.S. Geological Survey GeoMAC historic perimeters; National Interagency Fire Center ArcGIS Online distribution.',
            'lineage': 'NIFC ArcGIS Online item 5b3ff19978be49208d41a9d9a461ecfb publishes the producer File Geodatabase archive; same-org item ef25d7e8c9f3499ba9e3d8e09606e488 hosts the corroborating Feature Service.',
            'release_date': None,
            'release_date_unknown_reason': 'The NIFC items expose only portal created/modified times (gdb item modified 2022-08-03; hosted service modified 2024-07-17); no producer release date is documented.',
        }
        if name == 'archive' and receipt.get('final_url_query_sha256'):
            record['redacted_final_url_query_sha256'] = receipt['final_url_query_sha256']
        if role == 'official_distributor':
            scope = ('Published File Geodatabase item identity' if name == 'gdb_item' else
                    'Same-organization hosted Feature Service corroboration of the gdb combined layer')
            record['delegation_evidence'] = {'url': GDB_ITEM_URL if name == 'gdb_item' else SERVICE_ITEM_URL,
                                             'scope': scope}
        inputs.append(record)
    manifest = {
        'schema_version': 1, 'processor': 'HIFLD Next', 'approved': False,
        'code_commit': identity['code_commit'], 'code_identity_scheme': identity['code_identity_scheme'],
        'title': 'Historic Perimeters Combined 2000-2018 GeoMAC (US_HIST_FIRE_PERIMTRS_2000_2018_DD83)',
        'publisher': PUBLISHER, 'agency': 'U.S. Geological Survey (GeoMAC); National Interagency Fire Center',
        'source_url': GDB_ITEM_URL, 'license': 'unknown',
        'source_modified': '2022-08-03T00:00:00Z',
        'date_issued': None, 'date_modified': None,
        'inputs': inputs,
        'transformations': [{
            'operation': 'Extract the combined layer of the official File Geodatabase archive into GeoParquet WKB',
            'parameters': {
                'selected_layer': COMBINED_LAYER,
                'source_ids': 'OBJECTID contiguous unique 1..23776; complete pinned population',
                'source_fields': 'all 22 gdb attribute fields preserved in producer order with declared physical types and values, plus native OBJECTID; no renames or generated IDs',
                'geometry': audit['geometry_input'],
                'crs': 'native EPSG:4269 preserved unreprojected',
                'gdal_polygon_organization': identity['gdal_polygon_organization'],
                'known_invalid_geometries': '881 invalid producer geometries preserved unrepaired; release blocked',
                'dirty_producer_values': 'datecurrent year 3013, perimeterdatetime year 1712, one agency value with CR/LF preserved',
                'candidate_status': 'unapproved; baseline equivalence unresolved',
            }}],
        'lineage': 'The NIFC ArcGIS Online organization T4QMspbfLg3qTGWY (owner NIFC_Admin) publishes the GeoMAC Historic Perimeters 2000-2018 File Geodatabase as item 5b3ff19978be49208d41a9d9a461ecfb, created 2019-11-14, modified 2022-08-03. This draft selects only the combined layer US_HIST_FIRE_PERIMTRS_2000_2018_DD83 (23,776 MultiPolygon features, EPSG:4269); the 19 per-year layers (75,603 features, older GeoMAC schema with latest/active flags) are census-receipted but not selected, and the hosted Feature Service (item ef25d7e8c9f3499ba9e3d8e09606e488) corroborates the combined layer without being merged into it. The archive sha256 pin is measured, not source-published. Producer release date, historical HIFLD baseline equivalence and reuse terms remain unresolved.',
        'unsupported_output_formats': {
            'shapefile_zip': 'No accepted schema/identifier mapping or packaging decision for 22 gdb fields and MultiPolygon geometry.',
            'geopackage': 'No accepted schema/identifier mapping or packaging decision.',
            'pmtiles': 'Reprojection/quantization changes native geometry; no visualization contract approved.',
        },
        'prebuilt_output_policy': _prebuilt_policy(objects),
    }
    validate_output_policy(manifest)
    validate_release_provenance(manifest)
    return manifest


def local_candidate_path(storage: StagingStorageResource, prefix: str) -> Path:
    require_local(storage)
    root = Path(storage.local_dir).resolve()
    key = storage._ensure_prefixed(prefix)
    if Path(key).is_absolute() or any(part in {'.', '..'} for part in Path(key).parts):
        raise GeoMACQuarantineError('unsafe_local_candidate_prefix')
    target = root / key
    current = root
    for part in target.relative_to(root).parts:
        current = current / part
        if current.is_symlink():
            raise GeoMACQuarantineError('linked_candidate_path')
    return target


@contextmanager
def candidate_lock(storage: StagingStorageResource, prefix: str):
    target = local_candidate_path(storage, prefix)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(target.parent / ('.' + target.name + '.lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'r+b') as held:
        if os.fstat(held.fileno()).st_nlink != 1:
            raise GeoMACQuarantineError('linked_candidate_lock')
        fcntl.flock(held, fcntl.LOCK_EX)
        yield target


def quarantine_cached_candidate_held(storage: StagingStorageResource, target: Path, prefix: str, reason: str) -> str:
    destination = Path(storage.local_dir) / '_quarantine' / SLUG / ('invalid-cache-' + uuid4().hex)
    destination.mkdir(parents=True)
    os.rename(target, destination / 'candidate')
    (destination / 'report.json').write_bytes(encode({'approved': False, 'status': 'quarantined_invalid_draft_cache',
                                                     'original_prefix': prefix, 'reason': reason}))
    return str(destination)


def commit_complete_candidate(storage: StagingStorageResource, prefix: str, payloads: dict, ready: dict) -> None:
    """Install one fully built local version atomically; partial builds stay invisible."""
    target = local_candidate_path(storage, prefix)
    target.parent.mkdir(parents=True, exist_ok=True)
    lock = target.parent / ('.' + target.name + '.lock')
    descriptor = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'r+b') as held:
        if os.fstat(held.fileno()).st_nlink != 1:
            raise GeoMACQuarantineError('linked_candidate_lock')
        fcntl.flock(held, fcntl.LOCK_EX)
        if target.exists() or target.is_symlink():
            raise GeoMACQuarantineError('concurrent_or_incomplete_existing_candidate')
        with tempfile.TemporaryDirectory(prefix='.candidate-build-', dir=target.parent) as tmp:
            private = StagingStorageResource(local_dir=tmp, use_local=True)
            for name, value in payloads.items():
                immutable(private, name, value)
            immutable(private, 'metadata/candidate_ready.json', encode(ready))
            os.rename(tmp, target)


def reconcile_cached_candidate(storage: StagingStorageResource, prefix: str, ready: dict,
                               data: dict, audit: dict, captures: dict, identity: dict) -> None:
    """Re-derive the source census and compare against the sealed first build."""
    root = local_candidate_path(storage, prefix)
    manifest_body = (root / 'metadata/source_manifest.json').read_bytes()
    authored_snapshot = 'metadata/source/version_' + sha(manifest_body) + '_source_manifest.json'
    allowed_derivatives = {'metadata/quality_manifest.json', 'metadata/data_dictionary.json',
                           'metadata/geoparquet_layout.json', 'metadata/resolved_provenance.json',
                           'metadata/source/source_manifest.json', 'metadata/source/data_dictionary.json',
                           'metadata/source/quality_manifest.json', authored_snapshot} | STAGED_RELEASE_DERIVATIVE_NAMES
    for relative in ('metadata/source/source_manifest.json', authored_snapshot):
        snapshot = root / relative
        if snapshot.exists() and snapshot.read_bytes() != manifest_body:
            raise GeoMACQuarantineError('cached_authored_manifest_snapshot_differs')
    paths = list(root.rglob('*'))
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink != 1) for p in paths):
        raise GeoMACQuarantineError('linked_candidate_object')
    actual = {str(p.relative_to(root)) for p in paths if p.is_file()}
    expected = set(ready['objects']) | {'metadata/candidate_ready.json'}
    if not expected <= actual or actual - expected - allowed_derivatives:
        raise GeoMACQuarantineError('existing_candidate_unrostered_or_missing_object')
    try:
        validate_staged_release_derivatives(storage, prefix, {f'{prefix}/{name}' for name in actual})
    except (OSError, UnicodeError, ValueError, TypeError) as exc:
        raise GeoMACQuarantineError('invalid_staged_release_derivatives') from exc
    for name, expected_sha in ready['objects'].items():
        key = f'{prefix}/{name}'
        snapshot = storage.object_snapshot(key)
        if snapshot is None or sha(storage.read_key(key)) != expected_sha or storage.object_snapshot(key) != snapshot:
            raise GeoMACQuarantineError(f'existing_candidate_object_changed:{name}')
    cached = {name: ((root / f'raw/captures/{name}.body').read_bytes(),
                     strict_json((root / f'raw/captures/{name}.receipt.json').read_bytes()))
              for name in URLS}
    for name in URLS:
        if cached[name][0] != captures[name][0]:
            raise GeoMACQuarantineError('cached_capture_differs_from_fresh_source')
        validate_receipt(*cached[name], URLS[name])
    if ready.get('source_record_sha256') != audit['source_record_sha256'] \
            or ready.get('source_geometry_sha256') != audit['source_geometry_sha256'] \
            or ready.get('source_schema_sha256') != audit['source_schema_sha256'] \
            or ready.get('processing_fingerprint') != sha(encode(identity)):
        raise GeoMACQuarantineError('cached_candidate_identity_conflict')
    staged_parquet = (root / 'geoparquet' / f'{SLUG}.parquet').read_bytes()
    expected_manifest = provenance(cached, identity, audit,
                                   {f'geoparquet/{SLUG}.parquet': {'bytes': len(staged_parquet), 'sha256': sha(staged_parquet)}})
    expected_manifest['processing_fingerprint'] = sha(encode(identity))
    manifest = strict_json(manifest_body)
    validate_release_provenance(manifest)
    resolved = root / 'metadata/resolved_provenance.json'
    if resolved.exists():
        projection = provenance_projection(load_resolved_source_manifest(storage, SLUG, SLUG, ready['version']).metadata)
        if strict_json(resolved.read_bytes()) != projection:
            raise GeoMACQuarantineError('cached_resolved_provenance_differs')
    if set(manifest) != set(expected_manifest):
        raise GeoMACQuarantineError('cached_provenance_field_roster_drift')
    for name, value in expected_manifest.items():
        if name != 'code_commit' and manifest.get(name) != value:
            raise GeoMACQuarantineError('cached_provenance_differs_from_original_capture')
    for name in ('metadata/source_audit.json', 'metadata/processing_identity.json',
                 'metadata/source_capture_manifest.json'):
        if (root / name).read_bytes() != {'metadata/source_audit.json': encode(audit),
                                          'metadata/processing_identity.json': encode(identity),
                                          'metadata/source_capture_manifest.json': encode(
                                              {'complete': True, 'source_feature_count': COMBINED_FEATURES,
                                               'receipts': {n: cached[n][1] for n in sorted(cached)}})}[name]:
            raise GeoMACQuarantineError('cached_source_audit_or_identity_drift:' + name)
    verify_candidate_readback(staged_parquet, data)


def stage_historic_geomac_perimeters_candidate(storage: StagingStorageResource, *, capture_dir: Path | None = None) -> dict:
    require_local(storage)
    if shutil.disk_usage(Path(storage.local_dir)).free < DISK_FLOOR_BYTES:
        raise GeoMACQuarantineError('disk_floor_before_acquisition')
    captures = acquire(storage, capture_dir)
    body, receipt = captures['archive']
    try:
        identity = processing_identity()
        with tempfile.TemporaryDirectory(prefix='geomac-perimeters-') as tmp:
            data, audit = inspect_source(captures, Path(tmp))
            version = content_derived_version(audit['source_record_sha256'], audit['source_geometry_sha256'],
                                              audit['source_schema_sha256'], identity)
            prefix = candidate_prefix(SLUG, SLUG, version)
            relative = f'geoparquet/{SLUG}.parquet'
            with candidate_lock(storage, prefix) as target:
                if target.exists():
                    try:
                        ready_key = f'{prefix}/metadata/candidate_ready.json'
                        if not storage.object_exists(ready_key):
                            raise GeoMACQuarantineError('existing_candidate_missing_ready_marker')
                        ready = strict_json(storage.read_key(ready_key))
                        expected_names = ({relative, 'metadata/source_manifest.json', 'metadata/source_audit.json',
                                           'metadata/processing_identity.json', 'metadata/source_capture_manifest.json'}
                                          | {f'raw/captures/{name}{suffix}' for name in URLS
                                             for suffix in ('.body', '.receipt.json')})
                        if (set(ready.get('objects', {})) != expected_names or ready.get('version') != version
                                or ready.get('status') != 'working_draft_candidate_ready'
                                or ready.get('approved') is not False or ready.get('release_blocked') is not True):
                            raise GeoMACQuarantineError('existing_candidate_inventory_conflict')
                        reconcile_cached_candidate(storage, prefix, ready, data, audit, captures, identity)
                        return _result(version, audit, prefix, reused=True)
                    except (ValueError, TypeError, KeyError, OSError, RuntimeError) as exc:
                        quarantine_cached_candidate_held(storage, target, prefix, str(exc))
                        raise
            table = candidate_table(data)
            sink = pa.BufferOutputStream()
            pq.write_table(table, sink, compression='zstd')
            parquet = sink.getvalue().to_pybytes()
            verify_candidate_readback(parquet, data)
            manifest = provenance(captures, identity, audit,
                                  {relative: {'bytes': len(parquet), 'sha256': sha(parquet)}})
            manifest['processing_fingerprint'] = sha(encode(identity))
            payloads = {
                relative: parquet,
                'metadata/source_manifest.json': encode(manifest),
                'metadata/source_audit.json': encode(audit),
                'metadata/processing_identity.json': encode(identity),
                'metadata/source_capture_manifest.json': encode(
                    {'complete': True, 'source_feature_count': COMBINED_FEATURES,
                     'receipts': {n: r for n, (_b, r) in sorted(captures.items())}}),
            }
            payloads.update({f'raw/captures/{name}.body': captured[0] for name, captured in captures.items()})
            payloads.update({f'raw/captures/{name}.receipt.json': encode(captured[1])
                             for name, captured in captures.items()})
            ready = {'status': 'working_draft_candidate_ready', 'approved': False, 'version': version,
                     'release_blocked': True,
                     'release_blocked_reason': audit['release_blocked_reason'],
                     'source_record_sha256': audit['source_record_sha256'],
                     'source_geometry_sha256': audit['source_geometry_sha256'],
                     'source_schema_sha256': audit['source_schema_sha256'],
                     'source_ids_sha256': audit['source_ids_sha256'],
                     'processing_fingerprint': sha(encode(identity)),
                     'objects': {name: sha(value) for name, value in payloads.items()}}
            commit_complete_candidate(storage, prefix, payloads, ready)
            return _result(version, audit, prefix, reused=False)
    except (ValueError, TypeError, KeyError, OSError, RuntimeError, zipfile.BadZipFile) as exc:
        location = quarantine(storage, body, receipt, str(exc))
        raise GeoMACQuarantineError(f'{exc}; retained:{location}') from exc


def _result(version: str, audit: dict, prefix: str, *, reused: bool) -> dict:
    return {'dataset_slug': SLUG, 'file_slug': SLUG, 'version': version,
            'feature_count': audit['feature_count'],
            'source_unique_id_count': audit['source_unique_id_count'],
            'source_null_geometry_count': audit['source_null_geometry_count'],
            'source_invalid_geometry_count': audit['source_invalid_geometry_count'],
            'source_vertices': audit['source_vertices'],
            'release_blocked': True,
            'candidate_ready': prefix + '/metadata/candidate_ready.json',
            'status': 'working_draft_candidate_ready', 'reused': reused,
            'source_release_accepted': False, 'source_freshness_accepted': False,
            'schema_contract_accepted': False, 'id_contract_accepted': False,
            'rights_accepted': False, 'mapping_approved': False, 'release_accepted': False}

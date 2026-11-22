"""Two original state publications, never a national ORNL replacement.

PSC Wisconsin explicitly describes some boundaries as approximate. Its three
native layers stay separate, including municipal Z/M ordinates. No EIA join,
coordinate repair or projection is performed. Local draft construction only.
"""
from __future__ import annotations

from dagster_hifld.sources.capture_common import (
    write_candidate_outputs, write_candidate_capture_files,
)

import hashlib
from collections import Counter
import io
import json
import math
import shutil
import stat
import struct
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.sources.electric_retail_service_territories_common import (
    _arrow_type, _canonical, _field_signature, _safe_local_storage,
)
from dagster_hifld.sources.capture_common import capture_get, processing_identity, write_immutable
from dagster_hifld.sources.arcgis_capture_sequence import capture_request_steps, ExactIdCapturePlan
from dagster_hifld.sources.arcgis_capture import RetainedCaptureView

IOWA = 'iowa-electric-service-boundaries'
WISCONSIN = 'wisconsin-psc-electric-service-territories'
LAYER = 'https://services.arcgis.com/vPD5PVLI6sfkZ5E4/arcgis/rest/services/Electrical_Service_Boundaries/FeatureServer/14'
ZIP_URL = 'https://psc.wi.gov/SiteAssets/Maps/EL_PSCW_ServTerr.zip'
MAX_BYTES = 32_000_000
GATES = {name: False for name in (
    'source_accepted', 'source_freshness_accepted', 'schema_contract_accepted',
    'id_contract_accepted', 'crs_accepted', 'coverage_accepted', 'rights_accepted',
    'baseline_accepted', 'release_accepted', 'promotion_allowed')}
WI_LAYERS = {'cooperative': 'Cooperative', 'investor-owned': 'Investor-Owned', 'municipal': 'Municipal'}


def digest(body):
    return hashlib.sha256(body).hexdigest()


def capture(url, directory, name, *, replay=False):
    """Bounded retained original response; offline mode never falls through to HTTP."""
    path = directory / (name + '.body')
    receipt_path = directory / (name + '.receipt.json')
    wire_path = directory / (name + '.wire')
    if replay:
        for item in (path, wire_path, receipt_path):
            info = item.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError('unsafe_capture_file')
        if path.stat().st_size > MAX_BYTES or wire_path.stat().st_size > MAX_BYTES or receipt_path.stat().st_size > 65536:
            raise ValueError('oversized_capture')
        body, wire = path.read_bytes(), wire_path.read_bytes()
        receipt = json.loads(receipt_path.read_bytes())
        if (receipt.get('url') != url or receipt.get('final_url') != url or receipt.get('method') != 'GET'
                or receipt.get('status') != 200 or receipt.get('complete_body') is not True
                or receipt.get('bytes') != len(body) or receipt.get('sha256') != digest(body)
                or receipt.get('wire_bytes') != len(wire) or receipt.get('wire_sha256') != digest(wire)):
            raise ValueError('retained_capture_mismatch:' + name)
        return body, receipt
    if any(p.exists() for p in (path, wire_path, receipt_path)):
        raise ValueError('capture_already_exists:' + name)
    def reserve(progress):
        if shutil.disk_usage(directory).free < 300_000_000 + progress.reserve_bytes:
            raise ValueError('capture_disk_floor')
    directory.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=httpx.Timeout(90, connect=15), follow_redirects=False) as client:
        return capture_get(client, url, directory, name, max_bytes=MAX_BYTES, reserve=reserve)


def iowa_table(features, fields):
    columns = {name: [] for name, _kind, _length in fields}
    if any(isinstance(feature.get('geometry'), dict) and 'curveRings' in feature['geometry'] for feature in features):
        raise ValueError('native_curves_require_separate_encoding_no_linearization')
    geometries = [None] * len(features)
    if any(feature.get('geometry') is not None for feature in features):
        document = {'geometryType': 'esriGeometryPolygon', 'hasZ': True, 'hasM': False,
            'spatialReference': {'wkid': 102004}, 'fields': [dict(name=n, type=k) for n,k,l in fields],
            'features': features}
        _info, native = pyogrio.read_arrow(_canonical(document))
        geometries = native['wkb_geometry'].to_pylist()
        if len(geometries) != len(features):
            raise ValueError('native_geometry_population_loss')
    for feature in features:
        attrs = feature.get('attributes')
        if not isinstance(attrs, dict) or set(attrs) != set(columns):
            raise ValueError('source_attributes_drift')
        if type(attrs['OBJECTID']) is not int:
            raise ValueError('source_objectid_drift')
        for name, kind, _length in fields:
            value = attrs[name]
            if value is not None:
                typ = _arrow_type(kind)
                if ((pa.types.is_integer(typ) and type(value) is not int)
                        or (pa.types.is_string(typ) and type(value) is not str)
                        or (pa.types.is_floating(typ) and (type(value) not in (int, float) or not math.isfinite(value)))):
                    raise ValueError('source_scalar_type_drift:' + name)
            columns[name].append(value)
        geometry = feature.get('geometry')
        if geometry is not None and geometry.get('spatialReference', {'wkid': 102004}).get('wkid') != 102004:
            raise ValueError('source_geometry_crs_drift')
    arrays = {name: pa.array(columns[name], type=_arrow_type(kind)) for name, kind, _length in fields}
    arrays['geometry'] = pa.array(geometries, type=pa.binary())
    return pa.table(arrays)


def validate_contract(file_slug, info, table):
    expected = CONTRACTS[file_slug]
    signature = None if table is None else [(f.name, str(f.type)) for f in table.schema]
    if (info.get('crs') != expected['crs'] or signature != [tuple(f) for f in expected['fields']]
            or info.get('geometry_type') != expected['geometry_type']):
        raise ValueError('source_contract_drift:' + file_slug)


def acquire_iowa(directory, replay):
    captures = RetainedCaptureView(directory)
    def get(name, url):
        body, receipt = capture(url, directory, name, replay=replay)
        captures[name] = (body, receipt)
        value = json.loads(body)
        if not isinstance(value, dict) or 'error' in value:
            raise ValueError('invalid_arcgis_response:' + name)
        return value
    def query(**params):
        return LAYER + '/query?' + urlencode({'f': 'json', **params})
    def request_steps():
        layer = (yield ('layer_before', LAYER + '?f=pjson',))
        fields = _field_signature(layer.get('fields'))
        if (layer.get('id') != 14 or layer.get('geometryType') != 'esriGeometryPolygon'
                or layer.get('objectIdField') != 'OBJECTID' or layer.get('hasZ') is not True or layer.get('hasM') is not False
                or layer.get('extent', {}).get('spatialReference', {}).get('wkid') != 102004):
            raise ValueError('iowa_layer_identity_or_crs_drift')
        if fields != [tuple(f) for f in IOWA_FIELDS]:
            raise ValueError('iowa_native_field_contract_drift')
        count = (yield ('count_before', query(where='1=1', returnCountOnly='true'),))['count']
        ids = (yield ('ids_before', query(where='1=1', returnIdsOnly='true'),))['objectIds']
        if (type(count) is not int or not 0 < count <= 5000 or len(ids) != count
                or any(type(i) is not int for i in ids) or len(set(ids)) != count):
            raise ValueError('iowa_population_incomplete')
        ids = sorted(ids)
        def page_request(offset, wanted):
            return (f'page_{offset:05}', query(objectIds=','.join(map(str, wanted)), outFields='*', returnGeometry='true', returnZ='true', returnM='false', returnTrueCurves='true', orderByFields='OBJECTID ASC'))
        def validate_page(offset, wanted, page):
            if (page.get('exceededTransferLimit') is True or page.get('geometryType') != 'esriGeometryPolygon'
                    or page.get('spatialReference', {}).get('wkid') != 102004
                    or _field_signature(page.get('fields')) != fields
                    or [f.get('attributes', {}).get('OBJECTID') for f in page.get('features', [])] != wanted):
                raise ValueError('iowa_page_incomplete_or_drift')
            return page['features']
        def validate_closing(name, value, closed):
            if ((name == 'count_after' and value['count'] != count)
                    or (name == 'ids_after' and sorted(value['objectIds']) != ids)
                    or (name == 'layer_after' and value != layer)):
                raise ValueError('iowa_changed_during_capture')
        features, _pages, closed = (yield ExactIdCapturePlan(ids, 100,
            page_request=page_request, validate_page=validate_page, validate_closing=validate_closing,
            read_page=lambda offset: captures[f'page_{offset:05}'][0],
            closing_requests=(('count_after', query(where='1=1', returnCountOnly='true')),
                ('ids_after', query(where='1=1', returnIdsOnly='true')),
                ('layer_after', LAYER + '?f=pjson'))))
        if (closed['count_after']['count'] != count
                or sorted(closed['ids_after']['objectIds']) != ids
                or closed['layer_after'] != layer):
            raise ValueError('iowa_changed_during_capture')
        table = iowa_table(list(features), fields)
        validate_contract('electrical-service-boundaries', {'crs': 'ESRI:102004', 'geometry_type': 'Polygon'}, table)
        return {'electrical-service-boundaries': (table, 'ESRI:102004', layer['editingInfo'].get('dataLastEditDate'))}, captures
    return capture_request_steps(request_steps(), get)


def _ring_signature(points):
    def value(number):
        return ('null',) if math.isnan(number) else ('value', number)
    points = [tuple(value(number) for number in point) for point in points]
    if points and points[0] == points[-1]:
        points = points[:-1]
    if not points:
        raise ValueError('empty_native_ring')
    smallest = min(points)
    variants = [tuple(points[i:] + points[:i]) for i, point in enumerate(points) if point == smallest]
    reverse = list(reversed(points))
    variants += [tuple(reverse[i:] + reverse[:i]) for i, point in enumerate(reverse) if point == smallest]
    return min(variants)


def normalize_shp_no_data_m(wkb):
    """Map only SHP no-value measures to WKB NaN; retain actual zeros and XYZ."""
    if wkb is None or not shapely.from_wkb(wkb).has_m:
        return wkb
    output = bytearray(wkb)
    def visit(offset):
        order = '<' if output[offset] == 1 else '>'
        kind, count = struct.unpack_from(order + '2I', output, offset + 1)
        offset += 9
        if kind == 3006:
            for _ in range(count): offset = visit(offset)
        elif kind == 3003:
            for _ in range(count):
                size = struct.unpack_from(order + 'I', output, offset)[0]
                offset += 4
                for _ in range(size):
                    if struct.unpack_from(order + 'd', output, offset + 24)[0] < -1e38:
                        struct.pack_into(order + 'd', output, offset + 24, math.nan)
                    offset += 32
        else:
            raise ValueError('unsupported_measured_polygon_wkb')
        return offset
    if visit(0) != len(output): raise ValueError('measured_wkb_trailing_bytes')
    return bytes(output)


def shapefile_geometry_audit(body, wkbs):
    """Compare every original SHP XY/Z/M ring with GDAL WKB, never infer M from its label.

    SHP M values below -1e38 are semantic no-data; GDAL uses WKB NaN.
    Source record/array presence and every no-data/actual-zero value are counted,
    and original SHP bytes remain retained in the full producer ZIP.
    """
    position, count = 100, 0
    audit = {'source_shp_sha256': digest(body), 'm_array_present_records': 0,
        'm_array_absent_records': 0, 'm_no_data_values': 0, 'm_actual_zero_values': 0,
        'm_actual_values': 0, 'source_vertices': 0, 'full_raw_xyzm_parity': True}
    while position < len(body):
        if position + 8 > len(body):
            raise ValueError('truncated_shp_record_header')
        _number, words = struct.unpack_from('>2i', body, position)
        record = body[position + 8:position + 8 + words * 2]
        if len(record) != words * 2 or len(record) < 4 or count >= len(wkbs):
            raise ValueError('truncated_or_extra_shp_record')
        kind = struct.unpack_from('<i', record)[0]
        geometry = None if wkbs[count] is None else shapely.from_wkb(wkbs[count])
        if kind == 0:
            if geometry is not None: raise ValueError('source_null_geometry_changed')
        elif kind in (5, 15):
            parts, size = struct.unpack_from('<2i', record, 36)
            starts = list(struct.unpack_from('<' + str(parts) + 'i', record, 44)) + [size]
            xy_offset = 44 + 4 * parts
            xy = [struct.unpack_from('<2d', record, xy_offset + 16 * i) for i in range(size)]
            coordinates = xy
            if kind == 15:
                z_offset = xy_offset + 16 * size
                zs = struct.unpack_from('<' + str(size) + 'd', record, z_offset + 16)
                m_offset = z_offset + 16 + 8 * size
                if len(record) == m_offset + 16 + 8 * size:
                    measures = struct.unpack_from('<' + str(size) + 'd', record, m_offset + 16)
                    audit['m_array_present_records'] += 1
                    audit['m_no_data_values'] += sum(m < -1e38 for m in measures)
                    audit['m_actual_zero_values'] += sum(m == 0 for m in measures)
                    audit['m_actual_values'] += sum(m >= -1e38 for m in measures)
                    coordinates = [(*point, z, math.nan if m < -1e38 else m)
                        for point, z, m in zip(xy, zs, measures, strict=True)]
                elif len(record) == m_offset:
                    audit['m_array_absent_records'] += 1
                    coordinates = [(*point, z) for point, z in zip(xy, zs, strict=True)]
                else:
                    raise ValueError('malformed_optional_m_array')
            if geometry is None: raise ValueError('native_geometry_lost')
            raw = Counter(_ring_signature(coordinates[a:b]) for a, b in zip(starts[:-1], starts[1:], strict=True))
            polygons = list(geometry.geoms) if geometry.geom_type == 'MultiPolygon' else [geometry]
            actual = Counter(_ring_signature(list(ring.coords)) for polygon in polygons
                for ring in [polygon.exterior, *polygon.interiors])
            if raw != actual: raise ValueError('source_shp_xyzm_ordinate_loss:' + str(count))
            audit['source_vertices'] += size
        else:
            raise ValueError('unsupported_native_shape_type:' + str(kind))
        count += 1
        position += 8 + words * 2
    if position != len(body) or count != len(wkbs): raise ValueError('shp_record_population_changed')
    audit['source_record_count'] = count
    return audit


class _GeometryWkbType(pa.ExtensionType):
    """geoarrow.wkb emits the required GeoParquet2 native GEOMETRY annotation."""
    def __init__(self, crs):
        self.crs = crs
        super().__init__(pa.binary(), 'geoarrow.wkb')

    def __arrow_ext_serialize__(self):
        return _canonical({'crs': self.crs, 'edges': 'planar'})

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, metadata):
        return cls(json.loads(metadata)['crs'])


def acquire_wisconsin(directory, replay):
    body, receipt = capture(ZIP_URL, directory, 'wisconsin_zip', replay=replay)
    path = directory / 'wisconsin_zip.body'
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        names = archive.namelist()
        if (len(names) != len(set(names)) or len(names) > 40
                or sum(i.file_size for i in archive.infolist()) > 50_000_000
                or {Path(n).stem for n in names if n.endswith('.shp')} != set(WI_LAYERS.values())):
            raise ValueError('wisconsin_archive_roster_drift')
    tables = {}
    for slug, layer in WI_LAYERS.items():
        native = f'/vsizip/{{{path}}}/EL_PSCW_ServTerr/{layer}.shp'
        info = pyogrio.read_info(native)
        _meta, table = pyogrio.read_arrow(native, ADJUST_GEOM_TYPE='ALL_SHAPES')
        geometry_column = table.schema.get_field_index('wkb_geometry')
        normalized = [normalize_shp_no_data_m(wkb) for wkb in table['wkb_geometry'].to_pylist()]
        table = table.set_column(geometry_column, table.schema.field(geometry_column), pa.array(normalized, type=pa.binary()))
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            raw_shape = archive.read(f'EL_PSCW_ServTerr/{layer}.shp')
        audit = shapefile_geometry_audit(raw_shape, table['wkb_geometry'].to_pylist())
        table = table.replace_schema_metadata({**(table.schema.metadata or {}),
            b'hifld_native_geometry_audit': _canonical(audit)})
        validate_contract(slug, info, table)
        if table.num_rows != info['features'] or not 0 < table.num_rows <= 5000:
            raise ValueError('wisconsin_population_incomplete')
        tables[slug] = (table, info['crs'], info['layer_metadata']['DBF_DATE_LAST_UPDATE'])
    return tables, {'wisconsin_zip': (body, receipt)}


def geoparquet(table, crs):
    geometry = 'geometry' if 'geometry' in table.column_names else 'wkb_geometry'
    shapes = shapely.from_wkb(table[geometry].to_numpy())
    measured = any(g is not None and g.has_m for g in shapes)
    types = sorted({g.geom_type + (' ZM' if g.has_z and g.has_m else ' M' if g.has_m else ' Z' if g.has_z else '') for g in shapes if g is not None})
    metadata = dict(table.schema.metadata or {})
    metadata[b'geo'] = _canonical({'version': '2.0.0' if measured else '1.1.0', 'primary_column': geometry,
        'columns': {geometry: {'encoding': 'WKB', 'geometry_types': types,
            'crs': pyproj.CRS(crs).to_json_dict()}}})
    sink = pa.BufferOutputStream()
    output = table.replace_schema_metadata(metadata)
    if measured:
        typ = _GeometryWkbType(pyproj.CRS(crs).to_json_dict())
        array = pa.ExtensionArray.from_storage(typ, table[geometry].combine_chunks())
        output = output.set_column(output.schema.get_field_index(geometry), pa.field(geometry, typ), array)
    pq.write_table(output, sink, compression='zstd')
    body = sink.getvalue().to_pybytes()
    if not pq.read_table(pa.BufferReader(body)).equals(table, check_metadata=False):
        raise ValueError('native_scalar_or_geometry_roundtrip_loss')
    return body, {'null_geometry_count': sum(g is None for g in shapes),
        'invalid_geometry_count': sum(g is not None and (not g.is_valid or g.is_empty) for g in shapes)}


def _candidate_fingerprint(captures, identity, output_digest):
    """Content-derived candidate identity: original source bytes, processing
    identity and output bytes. Retrieval observations never enter it, so an
    unchanged refresh derives the same version instead of minting a new one."""
    return digest(_canonical({'captures': {name: digest(body) for name, (body, _r) in captures.items()},
        'processing': identity, 'output': output_digest}))


def _existing_sealed_candidate(storage, prefix, captures, identity, output_digest):
    """Return the first construction's sealed result, or None for a fresh prefix.

    The seal is ``metadata/native_capture.json``, written last. A prefix holding
    earlier writes without it was left incomplete by an interrupted run and is
    never extended or overwritten; remove it explicitly. A sealed candidate is
    reused only when its own provenance pins exactly this construction's source
    bytes, processing identity and output digest.
    """
    seal_key = f'{prefix}/metadata/native_capture.json'
    if not storage.object_exists(seal_key):
        if storage.list_prefix(prefix):
            raise ValueError('staged_candidate_partially_constructed:' + prefix)
        return None
    try:
        manifest = json.loads(storage.read_key(f'{prefix}/metadata/source_manifest.json'))
        sealed = json.loads(storage.read_key(seal_key))
        stored = {item['input_id']: (item['sha256'], item['bytes']) for item in manifest['inputs']}
        objects = manifest['prebuilt_output_policy']['formats']['geoparquet']['objects']
        consistent = (stored == {name: (digest(body), len(body)) for name, (body, _r) in captures.items()}
            and len(objects) == 1 and objects[0]['sha256'] == output_digest
            and manifest['processing_code'] == identity
            and sealed.get('version') == prefix.rsplit('/', 1)[-1]
            and sealed.get('staging_prefix') == prefix and sealed.get('output_sha256') == output_digest)
    except (AttributeError, IndexError, KeyError, TypeError) as exc:
        raise ValueError('staged_candidate_first_construction_conflict:' + prefix) from exc
    if not consistent:
        raise ValueError('staged_candidate_first_construction_conflict:' + prefix)
    return sealed


def stage_state_territories(storage, state, capture_directory=None):
    """Immutable local candidates with original bytes and all acceptance gates false.

    Versions derive only from source bytes, processing identity and output
    bytes; retrieval observations live in capture history, and a sealed
    candidate is never rewritten, so an unchanged refresh reuses its first
    construction instead of minting a conflicting new version.
    """
    _safe_local_storage(storage)
    dataset = {'iowa': IOWA, 'wisconsin': WISCONSIN}[state]
    directory = Path(capture_directory).absolute() if capture_directory else Path(tempfile.mkdtemp(prefix=f'hifld-960-{state}-'))
    if any(p.is_symlink() for p in (directory, *directory.parents)):
        raise ValueError('unsafe_capture_directory')
    tables, captures = (acquire_iowa if state == 'iowa' else acquire_wisconsin)(directory, bool(capture_directory))
    identity = processing_identity(Path(__file__).parents[3])
    # Observations belong to capture history, outside every content-addressed candidate.
    for name, (raw, receipt) in captures.items():
        write_immutable(storage, f'_raw/{dataset}/{digest(raw)}/{name}.{digest(_canonical(receipt))}.receipt.json', _canonical(receipt))
    results = []
    for slug, (table, crs, modified) in tables.items():
        body, quality = geoparquet(table, crs)
        output_digest = digest(body)
        fingerprint = _candidate_fingerprint(captures, identity, output_digest)
        version = 'v0.0.0-draft-' + fingerprint[:32]
        prefix = f'{dataset}/{slug}/{version}'
        output = f'geoparquet/{slug}.parquet'
        sealed = _existing_sealed_candidate(storage, prefix, captures, identity, output_digest)
        if sealed is not None:
            results.append(sealed)
            continue
        publisher = 'Iowa Utilities Commission' if state == 'iowa' else 'Public Service Commission of Wisconsin'
        native_audit = json.loads((table.schema.metadata or {}).get(b'hifld_native_geometry_audit', b'{}'))
        manifest = {'schema_version': 1, 'approved': False, **GATES,
            'title': dataset + ' / ' + slug, 'publisher': publisher,
            'source_url': LAYER if state == 'iowa' else ZIP_URL,
            'source_modified': str(modified), 'producer_release_date': None,
            'native_geometry_audit': native_audit,
            'geometry_conversion': 'GDAL ESRIJSON native WKB; no repair, projection or dimensional flattening' if state == 'iowa' else 'GDAL ALL_SHAPES native shapefile WKB; XYZM audited against original SHP; M no-data sentinel maps to NaN, actual zero retained',
            'source_date_meaning': 'ArcGIS dataLastEditDate milliseconds' if state == 'iowa' else 'DBF update date; individual DATA_DATE values retained; not a release date',
            'license': 'Unaccepted; original source metadata retained, no open-rights inference.',
            'source_coverage_scope': 'Source-defined state electric service areas; approximate boundaries possible; no national ORNL parity or EIA-ID equivalence.',
            'processor': 'HIFLD Next', 'code_commit': identity['code_commit'],
            'code_identity_scheme': identity['code_identity_scheme'], 'processing_code': identity,
            'inputs': [{'input_id': name, 'producer': publisher, 'role': 'producer_publication',
                'host': urlsplit(receipt['url']).hostname,
                'canonical_url': LAYER if state == 'iowa' else ZIP_URL,
                'retrieval_url': receipt['url'], 'observed_final_url': receipt['final_url'],
                'retrieved_at': receipt['observed_at'], 'http_status': receipt['status'],
                'bytes': len(raw), 'sha256': digest(raw),
                'wire_bytes': receipt['wire_bytes'], 'wire_sha256': receipt['wire_sha256'],
                'release_date': None,
                'release_date_unknown_reason': 'Publisher exposes edit/container and feature dates, not a dated common release edition.',
                'license': 'Unresolved; public access does not establish redistribution rights acceptance.',
                'attribution': publisher + '; original state-defined electric service polygons, accuracy and rights unaccepted.'}
                for name, (raw, receipt) in captures.items()],
            'transformations': [{'operation': 'native_state_electric_territory_to_geoparquet', 'parameters': {
                'input_format': 'ESRIJSON' if state == 'iowa' else 'original PSC shapefile ZIP',
                'attributes': 'All original field names, order and typed scalar values; no EIA joins or generated IDs.',
                'geometry': 'GDAL native WKB, ALL_SHAPES for shapefiles; original CRS/XYZM retained, SHP M no-data maps to WKB NaN with counted source semantics; no projection or repair.',
                'scope': 'One original state layer only, not national ORNL parity.'}}],
            'lineage': 'Original state publisher publication; source-defined populations remain separate, without national reconstruction.',
            'capture_evidence': {name: {'url': receipt['url'], 'raw_path': f'raw/{name}.body',
                'sha256': digest(raw), 'bytes': len(raw), 'wire_path': f'raw/{name}.wire',
                'wire_sha256': receipt['wire_sha256'], 'wire_bytes': receipt['wire_bytes']}
                for name, (raw, receipt) in captures.items()},
            'prebuilt_output_policy': {'schema_version': 1, 'formats': {'geoparquet': {
                'reason': 'Preserve native attributes, WKB and CRS without repair or projection; draft only.',
                'objects': [{'path': output, 'sha256': digest(body), 'bytes': len(body)}]}}},
            'unsupported_output_formats': {'geopackage': 'No approved derived schema.', 'pmtiles': 'No approved display transformation.', 'shapefile_zip': 'Native dimensional and scalar contract requires review before a derived shapefile.'}}
        validate_release_provenance(manifest)
        write_candidate_capture_files(
            storage, prefix, directory, {name: receipt for name, (_raw, receipt) in captures.items()},
            lambda _directory, name: captures[name][0], order="insertion", receipt_encoding="omit")
        write_candidate_outputs(storage, prefix, {
            output: body, "metadata/source_manifest.json": _canonical(manifest)}, snapshot_check="none")
        result = {'dataset_slug': dataset, 'file_slug': slug, 'version': version,
            'staging_prefix': prefix, 'feature_count': table.num_rows, 'native_crs': crs,
            'source_population_complete': True, 'native_geometry_audit': native_audit, 'output_sha256': output_digest, **quality, **GATES}
        write_immutable(storage, f'{prefix}/metadata/native_capture.json', _canonical(result))
        results.append(result)
    return results


# Exact source field order/types and native CRS, inspected 2026-11-16.
CONTRACTS = {'electrical-service-boundaries': {'crs': 'ESRI:102004', 'geometry_type': 'Polygon', 'fields': [('OBJECTID', 'int64'), ('Owner', 'string'), ('ESB_Type', 'string'), ('ESB_ID', 'string'), ('REC_Number', 'string'), ('WebsiteURL', 'string'), ('Emergency_Phone', 'string'), ('Alternate_Phone', 'string'), ('Print_Name1', 'string'), ('Print_Name2', 'string'), ('created_user', 'string'), ('created_date', 'int64'), ('last_edited_user', 'string'), ('last_edited_date', 'int64'), ('local_edit_date', 'int64'), ('Shape__Area', 'double'), ('Shape__Length', 'double'), ('geometry', 'binary')]}, 'cooperative': {'crs': 'EPSG:3071', 'geometry_type': 'Polygon', 'fields': [('CITY', 'string'), ('CODE', 'string'), ('UTIL_LAB', 'string'), ('PSC_ID', 'int64'), ('AREA', 'double'), ('PERIMETER', 'string'), ('DATA_DATE', 'date32[day]'), ('GIS_DATA', 'string'), ('ADDRESS_1', 'string'), ('ADDRESS_2', 'string'), ('STATE', 'string'), ('ZIP', 'string'), ('PHONE', 'string'), ('WEB', 'string'), ('UTIL_TYPE', 'string'), ('LABEL', 'string'), ('FOE_Part', 'int32'), ('Shape_Leng', 'double'), ('Shape_Area', 'double'), ('wkb_geometry', 'binary')]}, 'investor-owned': {'crs': 'EPSG:3071', 'geometry_type': 'Polygon', 'fields': [('CODE', 'string'), ('UTIL_LAB', 'string'), ('PSC_ID', 'int64'), ('ADDRESS_1', 'string'), ('ADDRESS_2', 'string'), ('CITY', 'string'), ('STATE', 'string'), ('ZIP', 'string'), ('PHONE', 'string'), ('WEB', 'string'), ('AREA', 'double'), ('PERIMETER', 'string'), ('DATA_DATE', 'date32[day]'), ('GIS_DATA', 'string'), ('UTIL_TYPE', 'string'), ('LABEL', 'string'), ('FOE_Part', 'int32'), ('Shape_Leng', 'double'), ('Shape_Area', 'double'), ('wkb_geometry', 'binary')]}, 'municipal': {'crs': 'EPSG:3071', 'geometry_type': 'Polygon Z', 'fields': [('CODE', 'string'), ('COLOR', 'int32'), ('UTIL_LAB', 'string'), ('Util_Type', 'string'), ('PSC_ID', 'int64'), ('AREA', 'double'), ('PERIMETER', 'string'), ('ADDRESS_1', 'string'), ('ADDRESS_2', 'string'), ('CITY', 'string'), ('STATE', 'string'), ('ZIP', 'string'), ('PHONE', 'string'), ('WEB', 'string'), ('DATA_DATE', 'date32[day]'), ('GIS_DATA', 'string'), ('LABEL', 'string'), ('Shape_Leng', 'double'), ('Shape_Area', 'double'), ('wkb_geometry', 'binary')]}}

IOWA_FIELDS = [('OBJECTID', 'esriFieldTypeOID', None), ('Owner', 'esriFieldTypeString', 255), ('ESB_Type', 'esriFieldTypeString', 50), ('ESB_ID', 'esriFieldTypeString', 50), ('REC_Number', 'esriFieldTypeString', 50), ('WebsiteURL', 'esriFieldTypeString', 255), ('Emergency_Phone', 'esriFieldTypeString', 50), ('Alternate_Phone', 'esriFieldTypeString', 50), ('Print_Name1', 'esriFieldTypeString', 255), ('Print_Name2', 'esriFieldTypeString', 255), ('created_user', 'esriFieldTypeString', 255), ('created_date', 'esriFieldTypeDate', 8), ('last_edited_user', 'esriFieldTypeString', 255), ('last_edited_date', 'esriFieldTypeDate', 8), ('local_edit_date', 'esriFieldTypeDate', 8), ('Shape__Area', 'esriFieldTypeDouble', None), ('Shape__Length', 'esriFieldTypeDouble', None)]

"""Deterministic synthetic captures for the EPA 2023 regions adapters."""
import copy
import json

from dagster_hifld.sources import epa_regions_2023 as s


def layer_metadata():
    return {
        'id': 13,
        'name': 'US EPA Regions (DOC & EPA 2023)',
        'geometryType': 'esriGeometryPolygon',
        'objectIdField': 'OBJECTID',
        'hasZ': False,
        'hasM': False,
        'fields': copy.deepcopy(s.FIELD_DEFINITIONS),
        'extent': {'spatialReference': {'wkid': 4326, 'latestWkid': 4326}},
        'description': 'Boundaries are updated annually by the EPA Office of Mission Support.',
    }


def feature(oid):
    number = f'{oid:02d}'
    return {
        'attributes': {
            'OBJECTID': oid,
            'EPA_REGION': number,
            'ALAND_MI': 100.0 + oid,
            'AWATER_MI': 10.0 + oid,
            'ALAND_KM': 259.0 + oid,
            'AWATER_KM': 25.9 + oid,
            'TOTALAREA_MI': 110.0 + oid,
            'TOTALAREA_KM': 284.9 + oid,
            'Shape__Area': 1.5 + oid,
            'Shape__Length': 4.0 + oid,
            'RegionString': number,
            'RegionTitle': f'EPA Region {number}',
        },
        'geometry': {'rings': [[[oid, 0.0], [oid, 1.0], [oid + 1.0, 1.0], [oid + 1.0, 0.0], [oid, 0.0]]]},
    }


def page(features=None):
    return {
        'objectIdFieldName': 'OBJECTID',
        'geometryType': 'esriGeometryPolygon',
        'spatialReference': {'wkid': 4326, 'latestWkid': 4326},
        'fields': copy.deepcopy(s.PAGE_FIELDS),
        'features': copy.deepcopy(features if features is not None else [feature(i) for i in range(1, 11)]),
    }


def documents():
    return {
        'source_service': {'serviceItemId': s.ITEM, 'layers': [{'id': 13, 'name': 'US EPA Regions (DOC & EPA 2023)'}]},
        'item': {'id': s.ITEM, 'owner': 'EPA_GEO', 'orgId': 'cJ9YHowT8TU7DUyn', 'url': s.SERVICE, 'access': 'public', 'created': 1680893319000, 'modified': 1782992205000},
        'owner': {'username': 'EPA_GEO'},
        'layer_before': layer_metadata(),
        'layer_after': layer_metadata(),
        'item_after': {'id': s.ITEM, 'owner': 'EPA_GEO', 'orgId': 'cJ9YHowT8TU7DUyn', 'url': s.SERVICE, 'access': 'public', 'created': 1680893319000, 'modified': 1782992205000},
        'count_before': {'count': 10},
        'count_after': {'count': 10},
        'ids_before': {'objectIdFieldName': 'OBJECTID', 'objectIds': list(range(10, 0, -1))},
        'ids_after': {'objectIdFieldName': 'OBJECTID', 'objectIds': list(range(10, 0, -1))},
        'page_00000': page(),
        'license_epa': '<html>EPA data license</html>',
    }


def captures():
    result = {}
    for name, value in documents().items():
        body = value.encode() if isinstance(value, str) else s.encode(value)
        result[name] = (body, receipt(name, body))
    return result


def receipt(name, body):
    return {
        'url': s.URLS[name], 'final_url': s.URLS[name], 'status': 200,
        'bytes': len(body), 'sha256': s.digest(body), 'complete_body': True,
        'observed_at': '2026-11-17T00:00:00+00:00',
    }


def write_capture_dir(root):
    root.mkdir(parents=True, exist_ok=True)
    for name, (body, value) in captures().items():
        (root / f'{name}.body').write_bytes(body)
        (root / f'{name}.receipt.json').write_bytes(json.dumps(value).encode())
    return root

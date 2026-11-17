"""Shared USACE National Inventory of Dams source identity and replay helpers.

The producing agency, service identity, pinned field roster and the bounded
replay/JSON primitives are one dataset's contract, shared by the ArcGIS capture
adapter and the native FileGDB capture adapter. Both adapters pass their own
byte budgets and keep their own pipeline logic.
"""
from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode

DATASET_SLUG = FILE_SLUG = "national-inventory-of-dams-nid-1"


PUBLISHER = "U.S. Army Corps of Engineers (USACE), National Inventory of Dams"


SERVICE_URL = "https://geospatial.sec.usace.army.mil/dls/rest/services/NID/National_Inventory_of_Dams_Public_Service/FeatureServer"


LAYER_ID = 0


LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"


LAYER_NAME = "National Inventory of Dams"


EXPECTED_FIELD_SIGNATURES = (
    ('OBJECTID', 'esriFieldTypeOID', None), ('NIDID', 'esriFieldTypeString', 8),
    ('OTHER_STRUCTURE_ID', 'esriFieldTypeString', 100), ('FEDERAL_ID', 'esriFieldTypeString', 100),
    ('DATA_UPDATED', 'esriFieldTypeDate', 8), ('NAME', 'esriFieldTypeString', 255),
    ('OTHER_NAMES', 'esriFieldTypeString', 1000), ('OWNER_TYPES', 'esriFieldTypeString', 1000),
    ('PRIMARY_OWNER_TYPE', 'esriFieldTypeString', 1000), ('STATE_FED_AGENCY', 'esriFieldTypeString', 255),
    ('NUMBER_ASSOCIATED_STRUCTURES', 'esriFieldTypeInteger', None), ('DESIGNER_NAMES', 'esriFieldTypeString', 1000),
    ('NON_FED_ON_FED', 'esriFieldTypeString', 20), ('STATE_REGULATED', 'esriFieldTypeString', 20),
    ('STATE_JURISDICTION', 'esriFieldTypeString', 20), ('STATE_REGULATORY_AGENCY', 'esriFieldTypeString', 500),
    ('STATE_PERMITTING', 'esriFieldTypeString', 20), ('STATE_INSPECTION', 'esriFieldTypeString', 20),
    ('STATE_ENFORCEMENT', 'esriFieldTypeString', 20), ('PRIMARY_SOURCE_AGENCY', 'esriFieldTypeString', 255),
    ('LATITUDE', 'esriFieldTypeDouble', None), ('LONGITUDE', 'esriFieldTypeDouble', None),
    ('STATE', 'esriFieldTypeString', 500), ('COUNTYSTATE', 'esriFieldTypeString', 500),
    ('CITY', 'esriFieldTypeString', 256), ('DISTANCE', 'esriFieldTypeDouble', None),
    ('RIVER_OR_STREAM', 'esriFieldTypeString', 255), ('CONGDIST', 'esriFieldTypeString', 500),
    ('CONG_REPRESNTATIVE', 'esriFieldTypeString', 100), ('FED_AGENCY_OWNERS', 'esriFieldTypeString', 500),
    ('FED_AGENCY_FUNDINGS', 'esriFieldTypeString', 500), ('FED_AGENCY_DESIGNERS', 'esriFieldTypeString', 500),
    ('FED_AGENCY_CONSTRUCTIONS', 'esriFieldTypeString', 500), ('FED_AGENCY_REGULATORIES', 'esriFieldTypeString', 500),
    ('FED_AGENCY_INSPECTIONS', 'esriFieldTypeString', 500), ('FED_AGENCY_OPERATIONS', 'esriFieldTypeString', 500),
    ('FED_AGENCY_OTHERS', 'esriFieldTypeString', 500), ('PRIMARY_PURPOSE', 'esriFieldTypeString', 256),
    ('PURPOSES', 'esriFieldTypeString', 1000), ('PRIMARY_DAM_TYPE', 'esriFieldTypeString', 100),
    ('DAM_TYPES', 'esriFieldTypeString', 500), ('CORE_TYPES', 'esriFieldTypeString', 500),
    ('FOUNDATIONS', 'esriFieldTypeString', 500), ('DAM_HEIGHT', 'esriFieldTypeDouble', None),
    ('HYDRAULIC_HEIGHT', 'esriFieldTypeDouble', None), ('STRUCTURAL_HEIGHT', 'esriFieldTypeDouble', None),
    ('NID_HEIGHT', 'esriFieldTypeInteger', None), ('DAM_LENGTH', 'esriFieldTypeDouble', None),
    ('DAM_VOLUME', 'esriFieldTypeInteger', None), ('YEAR_COMPLETED', 'esriFieldTypeInteger', None),
    ('NID_STORAGE', 'esriFieldTypeDouble', None), ('MAX_STORAGE', 'esriFieldTypeDouble', None),
    ('NORMAL_STORAGE', 'esriFieldTypeDouble', None), ('SURFACE_AREA', 'esriFieldTypeDouble', None),
    ('DRAINAGE_AREA', 'esriFieldTypeDouble', None), ('MAX_DISCHARGE', 'esriFieldTypeDouble', None),
    ('SPILLWAY_TYPE', 'esriFieldTypeString', 256), ('SPILLWAY_WIDTH', 'esriFieldTypeInteger', None),
    ('NUMBER_OF_LOCKS', 'esriFieldTypeInteger', None), ('LENGTH_OF_LOCKS', 'esriFieldTypeDouble', None),
    ('WIDTH_OF_LOCKS', 'esriFieldTypeDouble', None), ('YEARS_MODIFIED', 'esriFieldTypeString', 500),
    ('OUTLET_GATES', 'esriFieldTypeString', 500), ('ASSOCIATED_STRUCTURES', 'esriFieldTypeString', 1000),
    ('LAST_INSPECTION_DATE', 'esriFieldTypeDate', 8), ('INSPECTION_FREQUENCY', 'esriFieldTypeInteger', None),
    ('HAZARD_POTENTIAL', 'esriFieldTypeString', 1000), ('CONDITION_ASSESSMENT', 'esriFieldTypeString', 20),
    ('CONDITION_ASSESS_DATE', 'esriFieldTypeDate', 8), ('EAP_PREPARED', 'esriFieldTypeString', 255),
    ('EAP_LAST_REV_DATE', 'esriFieldTypeDate', 8), ('SE_ANNO_CAD_DATA', 'esriFieldTypeBlob', None),
    ('FORMER_NAMES', 'esriFieldTypeString', 500), ('IS_ASSOCIATED_STRUCTURE', 'esriFieldTypeString', 3),
    ('AIANNH', 'esriFieldTypeString', 100), ('FEDERALLY_REGULATED_DAM', 'esriFieldTypeString', 3),
    ('BUILT_UNDER_SEC_OF_AG_AUTH', 'esriFieldTypeString', 3), ('NRCS_WATERSHED_DAM_AUTH', 'esriFieldTypeString', 100),
    ('OPERATIONAL_STATUS', 'esriFieldTypeString', 100), ('OPERATIONAL_STATUS_DATE', 'esriFieldTypeDate', 8),
    ('WEBSITE_URL', 'esriFieldTypeString', 150),
)


EXPECTED_FIELD_NAMES = [item[0] for item in EXPECTED_FIELD_SIGNATURES]


def _strict_json(body: bytes | str) -> object:
    def object_without_duplicate_keys(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('usace_nid_duplicate_json_key')
            result[key] = value
        return result
    return json.loads(body, object_pairs_hook=object_without_duplicate_keys,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('invalid_json_constant')))


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns,
            value.st_ctime_ns, value.st_nlink)


def _bounded_capture_file(path: Path, limit: int, expected_identity=None) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > limit
                or (expected_identity is not None and _stat_identity(before) != expected_identity)):
            raise ValueError('usace_nid_replay_artifact_unsafe_or_oversized')
        body = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    if (len(body) != before.st_size or len(body) > limit
            or _stat_identity(before) != _stat_identity(after)):
        raise ValueError('usace_nid_replay_artifact_changed_or_incomplete')
    return body


def _validate_local_directory_path(path: Path, requested: str | None = None) -> None:
    if requested is not None and '..' in Path(requested).parts:
        raise ValueError('usace_nid_path_traversal_refused')
    if '..' in path.parts or any(parent.is_symlink() for parent in (path, *path.parents)):
        raise ValueError('usace_nid_symlink_path_refused')


def _query(params: Mapping[str, object]) -> str:
    return LAYER_URL + '/query?' + urlencode(params)


def _field_signature(field: Mapping[str, object]) -> tuple[object, object, object]:
    return field.get('name'), field.get('type'), field.get('length')


def _is_native_4269(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    codes = [value[key] for key in ('wkid', 'latestWkid') if key in value]
    return bool(codes) and all(type(code) is int and code == 4269 for code in codes)


def _candidate_result(version: str, audit: Mapping[str, object], prefix: str, reused: bool,
                      capture_directory: str | None = None) -> dict[str, object]:
    result = {'dataset_slug': DATASET_SLUG, 'file_slug': FILE_SLUG, 'version': version,
              'feature_count': audit['source_feature_count'], 'source_null_geometry_count': audit['null_geometry_count'],
              'source_invalid_geometry_count': audit['invalid_geometry_count'], 'candidate_ready': prefix + '/metadata/candidate_ready.json',
              'source_release_accepted': False, 'freshness_accepted': False, 'schema_contract_accepted': False,
              'id_contract_accepted': False, 'rights_accepted': False, 'mapping_approved': False,
              'release_accepted': False, 'reused': reused}
    if capture_directory is not None:
        result['capture_directory'] = capture_directory
    return result



"""Faithful DISDI MIRTA DoD site boundaries draft from the official DoD zip.

The Military Installations, Ranges and Training Areas (MIRTA) boundaries layer
is captured from the DISDI Program download (OASD(EI&E), acq.osd.mil), pinned
on the measured archive bytes/sha256 (the source publishes no hash), and
published as native EPSG:4326 MultiPolygon WKB. The HIFLD Open entry this
issue tracks is the FY 2022 Version 1 edition (2023-07-31); its exact DHS
distribution is dead and DHS' crosswalk requires GII access, so the current
producer release (FY24 BSR-based, release notes 2025-09-02) is the maintained
official source. 121 curved producer MultiSurface features are linearized with
GDAL's own arc code at a pinned step; the 704 linear features keep their exact
GDB WKB. The sibling FY24 point layer is census-receipted but never selected.
Publication, contract, rights and baseline acceptance remain unapproved.
"""
from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
import re
import shutil
import ssl
import stat
import struct
import tempfile
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit
from uuid import uuid4

import certifi
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

SLUG = 'mirta-dod-sites-boundaries'
PRODUCER = ('Defense Installation Spatial Data Infrastructure (DISDI) Program, '
            'Office of the Assistant Secretary of Defense for Energy, Installations, and Environment')
LANDING_URL = 'https://www.acq.osd.mil/eie/imr/rpid/disdi/index.html'
RELEASE_NOTES_URL = 'https://www.acq.osd.mil/eie/imr/rpid/disdi/Downloads/release_notes.pdf'
ARCHIVE_URL = 'https://www.acq.osd.mil/eie/imr/rpid/disdi/Downloads/installations_ranges.zip'
URLS = {'landing': LANDING_URL, 'release_notes': RELEASE_NOTES_URL, 'archive': ARCHIVE_URL}
ARCHIVE_NAME = 'installations_ranges.zip'
# Measured identity pins (the DISDI download publishes no source hash).
ARCHIVE_BYTES = 3_723_148
ARCHIVE_SHA256 = 'c8f15d5de307528ec4f7d2648181e5182c89ecd33d337a0e1a128817ce084e83'
ZIP_MEMBERS = 53
ZIP_UNCOMPRESSED_BYTES = 5_455_472
GDB_TOP_DIR = 'FY24_MIRTA_Final.gdb'
BOUNDARY_LAYER = 'FY24MirtaLocations_A'
POINT_LAYER = 'FY24MirtaLocations'
BOUNDARY_FEATURES = 825
POINT_FEATURES = 831
POINT_FIELDS = ['countryName', 'featureDescription', 'featureName', 'isCui', 'isFirrmaSite',
                'isJointBase', 'mediaId', 'mirtaLocationsIdpk', 'sdsId', 'siteName',
                'siteOperationalStatus', 'siteReportingComponent', 'stateNameCode']
POINT_BOUNDS = [-167.98785503299996, 13.428655045000028, 174.10714007400009, 71.32619497400003]
# 13 producer attribute fields in producer order with GDB declared widths and
# aliases (the release notes' recommended-alias table).
EXPECTED_FIELDS = ['countryName', 'featureDescription', 'featureName', 'isCui', 'isFirrmaSite',
                   'isJointBase', 'mediaId', 'mirtaLocationsIdpk', 'sdsId', 'siteName',
                   'siteOperationalStatus', 'siteReportingComponent', 'stateNameCode']
EXPECTED_FIELD_WIDTHS = {'countryName': 25, 'featureDescription': 2000, 'featureName': 80,
                         'isCui': 3, 'isFirrmaSite': 3, 'isJointBase': 3, 'mediaId': 40,
                         'mirtaLocationsIdpk': 40, 'sdsId': None, 'siteName': 100,
                         'siteOperationalStatus': 4, 'siteReportingComponent': 22,
                         'stateNameCode': 5}
EXPECTED_FIELD_ALIASES = {'countryName': 'Country', 'featureDescription': 'Feature Description',
                          'featureName': 'Feature Name',
                          'isCui': 'Controlled Unclassified Information Indicator',
                          'isFirrmaSite': 'Is FIRRMA Site', 'isJointBase': 'Is Joint Base',
                          'mediaId': 'Media Identifier', 'mirtaLocationsIdpk': 'Primary Key Identifier',
                          'sdsId': 'Globally Unique Identifier', 'siteName': 'Site Name',
                          'siteOperationalStatus': 'Site Operational Status',
                          'siteReportingComponent': 'Site Reporting Component Code',
                          'stateNameCode': 'State Name Code'}
EXPECTED_BOUNDS = [-168.01305372299998, 13.30910940700005, 174.15651899800002, 71.34432336800006]
EXPECTED_NULLS = {name: (202 if name == 'sdsId' else 0) for name in EXPECTED_FIELDS}
EXPECTED_CONSTANT_VALUES = {'countryName': 'usa', 'featureDescription': 'na', 'isCui': 'no',
                            'mediaId': 'na', 'mirtaLocationsIdpk': ' '}
EXPECTED_VALUE_CENSUSES = {
    'isFirrmaSite': {'no': 601, 'yes': 224},
    'isJointBase': {'no': 810, 'yes': 15},
    'siteOperationalStatus': {'act': 792, 'care': 14, 'clsd': 11, 'excs': 4, 'semi': 4},
    'siteReportingComponent': {'afr': 10, 'airNationalGuard': 70, 'armyNationalGuard': 192,
                               'other': 1, 'usa': 130, 'usaf': 137, 'usar': 43, 'usmc': 28,
                               'usmcr': 6, 'usn': 199, 'usnr': 5, 'whs': 4},
    'stateNameCode': {'ak': 29, 'al': 30, 'ar': 6, 'az': 20, 'ca': 79, 'co': 12, 'ct': 6,
                      'dc': 4, 'de': 4, 'fl': 49, 'ga': 21, 'gu': 5, 'hi': 33, 'ia': 7,
                      'id': 10, 'il': 14, 'in': 13, 'ks': 9, 'ky': 5, 'la': 15, 'ma': 14,
                      'md': 28, 'me': 11, 'mi': 8, 'mn': 10, 'mo': 16, 'ms': 15, 'mt': 9,
                      'nc': 14, 'nd': 9, 'ne': 9, 'nh': 4, 'nj': 9, 'nm': 10, 'nv': 12,
                      'ny': 22, 'oh': 15, 'ok': 14, 'or': 10, 'pa': 21, 'pr': 20, 'ri': 8,
                      'sc': 8, 'sd': 6, 'tn': 16, 'tx': 32, 'ut': 8, 'va': 47, 'vi': 1,
                      'vt': 6, 'wa': 18, 'wi': 9, 'wv': 12, 'wy': 3},
}
SDS_ID_PATTERN = re.compile(r'\{[0-9A-F-]{36}\}')
EXPECTED_SDS_ID_NULLS = 202
EXPECTED_SDS_ID_NON_NULL = 623
EXPECTED_SDS_ID_UNIQUE = 619
EXPECTED_SDS_ID_DUPLICATE_ROWS = 4
EXPECTED_SDS_ID_SHARED_GROUPS = 3
EXPECTED_SDS_ID_CONFLICTING_SITE_GROUPS = 1
EXPECTED_OBJECTID_MIN = 1
EXPECTED_OBJECTID_MAX = 992
EXPECTED_FEATURE_NAME_SITE_NAME_EQUAL = 825
# Original GDB WKB subtype census (ISO 2D type codes): 704 MultiPolygon
# features and 121 curved MultiSurface features (CurvePolygon rings holding
# CompoundCurve/CircularString members).
EXPECTED_SOURCE_WKB_SUBTYPES = {2: 121, 3: 704, 6: 704, 8: 120, 9: 120, 10: 121, 12: 121}
EXPECTED_SOURCE_VERTICES = 724_735
EXPECTED_CURVED_FEATURES = 121
EXPECTED_LINEAR_FEATURES = 704
# Pinned GDAL arc linearization for the 121 curved MultiSurface features.
ARC_STEP_DEGREES = '0.01'
EXPECTED_CANDIDATE_VERTICES = 3_808_488
EXPECTED_INVALID_GEOMETRY_COUNT = 28
EXPECTED_KEPT_INVALID_GEOMETRY_COUNT = 15
EXPECTED_LINEARIZED_INVALID_GEOMETRY_COUNT = 13
EXPECTED_EMPTY_GEOMETRY_COUNT = 0
EXPECTED_NULL_GEOMETRY_COUNT = 0
DISK_FLOOR_BYTES = 2_000_000_000
MAX_LANDING_BYTES = 2_000_000
MAX_AUX_BYTES = 2_000_000
# The acq.osd.mil server sends only its leaf certificate; the IdenTrust
# TrustID Server CA O1 intermediate (from the leaf's own AIA URI
# validation.identrust.com/certs/trustidcaO1.p7c) completes the standard
# chain rooted at IdenTrust Commercial Root CA 1 in the system/certifi store.
PINNED_INTERMEDIATE_SHA256 = ('6baab0c433d779fd6a4b6d56d6304d5e6ea5de689fe35a4303'
                              '8a4028f345df60')
TRUSTID_INTERMEDIATE_PEM = """-----BEGIN CERTIFICATE-----
MIIGvTCCBKWgAwIBAgIQQAFu+vSEq6LjsbaVYwMsczANBgkqhkiG9w0BAQsFADBK
MQswCQYDVQQGEwJVUzESMBAGA1UEChMJSWRlblRydXN0MScwJQYDVQQDEx5JZGVu
VHJ1c3QgQ29tbWVyY2lhbCBSb290IENBIDEwHhcNMTkxMjEyMTYzMjM5WhcNMjkx
MjEyMTYzMjM5WjBZMQswCQYDVQQGEwJVUzESMBAGA1UEChMJSWRlblRydXN0MRcw
FQYDVQQLEw5UcnVzdElEIFNlcnZlcjEdMBsGA1UEAxMUVHJ1c3RJRCBTZXJ2ZXIg
Q0EgTzEwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC7g55cFrCoIeDK
1sq2+V5Am2l1vbqjhalYhRH6x0BhmP6XLx0jhqwcVm7HfhQ6pJQEVxo8kB0Qp9Ry
mSOvvVNCtW2UnmPxuoNxGFlZvjNaYf/jhMzUMGGQZ0ZXb2hKe5NMYDHwuVvnSHnT
MvSYAzu9rPOruXOlfURxYIu+3o3huWivZyBP+Ze+xMqmbRmYgnAcXSoOVI3UhwKF
/r5+o+CFOBvSkgB1ov8vLhE0rLQkHCXdk/qCmxFbwhYO+SsBrGKUfXOK8ze7AXLD
n6e01EyvUscRZu8NlFParxMWW7STjwZ+kh+QFB2HLj8o2V8irmubZMBsVkKWyG+W
HoEuL7m/AgMBAAGjggKOMIICijASBgNVHRMBAf8ECDAGAQH/AgEAMA4GA1UdDwEB
/wQEAwIBhjCBiQYIKwYBBQUHAQEEfTB7MDAGCCsGAQUFBzABhiRodHRwOi8vY29t
bWVyY2lhbC5vY3NwLmlkZW50cnVzdC5jb20wRwYIKwYBBQUHMAKGO2h0dHA6Ly92
YWxpZGF0aW9uLmlkZW50cnVzdC5jb20vcm9vdHMvY29tbWVyY2lhbHJvb3RjYTEu
cDdjMB8GA1UdIwQYMBaAFO1EGcDT8AaL7qR7vkLnJlTIjjZ2MIIBKwYDVR0gBIIB
IjCCAR4wggEaBgRVHSAAMIIBEDBKBggrBgEFBQcCARY+aHR0cHM6Ly9zZWN1cmUu
aWRlbnRydXN0LmNvbS9jZXJ0aWZpY2F0ZXMvcG9saWN5L3RzL2luZGV4Lmh0bWww
gcEGCCsGAQUFBwICMIG0DIGxVGhpcyBUcnVzdElEIFNlcnZlciBDZXJ0aWZpY2F0
ZSBoYXMgYmVlbiBpc3N1ZWQgaW4gYWNjb3JkYW5jZSB3aXRoIElkZW5UcnVzdCdz
IFRydXN0SUQgQ2VydGlmaWNhdGUgUG9saWN5IGZvdW5kIGF0IGh0dHBzOi8vc2Vj
dXJlLmlkZW50cnVzdC5jb20vY2VydGlmaWNhdGVzL3BvbGljeS90cy9pbmRleC5o
dG1sMEoGA1UdHwRDMEEwP6A9oDuGOWh0dHA6Ly92YWxpZGF0aW9uLmlkZW50cnVz
dC5jb20vY3JsL2NvbW1lcmNpYWxyb290Y2ExLmNybDAdBgNVHQ4EFgQUThcZFloO
TNxO9HLs5DoBwRmU/TUwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMA0G
CSqGSIb3DQEBCwUAA4ICAQCbLpB+BNTA1WvOU48YNwB9OzfX5UIqLOtw+3FoFuvs
MY0b5V4u8+lDj9n8hQW67qtbcb1eN+ZTD9hqkG39BnfkjnJeAdagfDBl08Utfmhq
gZpH0SI/6ug18pVTPU4MBAWHgyILWXsuSydFrt/Sw6pq+G53ACWWs8zYMvmfZB9y
4ABsr1k0YtM2wwist+XlxRQqH+NOqc9sUPeTfPmITxyvotAXR6sHrjEj12sQIE65
/IfFFvsYFlfmHpy9bX5Y3hQ05sscd35ONefXYzNDxe0spVpwbKmRc20Nd/TtjtDO
yINrxW1e92karKJUf2cO4GowOElP6c5MRSXzmmIDnahoJ/CSXcf5c5lv/YhdJU7M
b8ZYTBJqeaZYQemukOLyn8bXMlZdx4lSdQotO7wwnmXNxDaLt3AsoynM93yti0pJ
NuhNdFwG/ml5gDVcK6Vfq/f2mOcNf3YCkl7vJuLvvR+Qc1/+cpSpmnGxMC8qPp1m
cg9cX8n5eTzA6ypwRli4S+V+fJsi00SYpQGY72UEFwN/MmSJTL8vQevba2IHC8eR
Qj37A6623FIQRfV4CnPupuZ8i/ErZMgUM423EeuAg68TEgP+w22IijHD9ZLXlm7O
LfwZqbN9zHTd8TIoBYNBEzCv94oduRyUa0//S1mfW1QH9sHDoA0CNud3OBtTdpwL
iQ==
-----END CERTIFICATE-----
"""
# The DISDI landing page is the producer authority statement; structural
# markers are pinned instead of full bytes so benign page edits do not
# masquerade as source drift.
LANDING_MARKERS = (
    'href="../disdi/Downloads/installations_ranges.zip"',
    'href="../disdi/Downloads/release_notes.pdf"',
    'Military Installations, Ranges and Training Areas',
    'geospatial.sec.usace.army.mil',
    'www.Data.gov',
)
# The DISDI landing page is the producer authority statement; structural
# markers are pinned instead of full bytes so benign page edits do not
# masquerade as source drift. The release-notes PDF is pinned byte-exact.
FY22_EDITION_NOTE = ('HIFLD Open tracked the FY 2022 Version 1 MIRTA edition (edition date '
                     '2023-07-31, DISDI custodian). Its exact DHS-hosted distribution '
                     '(services1 FY22_MIRTA_Boundaries_gdb FeatureServer) is dead and the DHS '
                     'crosswalk routes the layer to the credential-gated GII portal, so this '
                     'draft ingests the producer DISDI FY24 annual release instead.')
LICENSE = ('DoD DISDI distribution statement: "Although these data have been provided by the '
           'DoD Components, no warranty expressed or implied is made regarding the utility of '
           'the data on any other system, in derived products or data alterations, nor shall '
           'the act of distribution constitute and such warranty." No license acceptance is '
           'established.')


class MirtaQuarantineError(ValueError):
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
        return json.loads(body, object_pairs_hook=pairs,
                          parse_constant=lambda token: (_ for _ in ()).throw(ValueError('non-finite json')))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError('invalid json capture') from exc


def tls_context() -> ssl.SSLContext:
    """Standard verification plus the pinned IdenTrust intermediate.

    The intermediate's DER bytes are re-hashed on every call so any embedded
    material tampering fails loudly before any request is issued.
    """
    context = ssl.create_default_context(cafile=certifi.where())
    der = base64.b64decode(''.join(line for line in TRUSTID_INTERMEDIATE_PEM.splitlines()
                                   if '-----' not in line))
    if hashlib.sha256(der).hexdigest() != PINNED_INTERMEDIATE_SHA256:
        raise MirtaQuarantineError('pinned_tls_intermediate_digest_drift')
    context.load_verify_locations(cadata=ssl.DER_cert_to_PEM_cert(der))
    return context


def require_local(storage: StagingStorageResource) -> None:
    if isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise MirtaQuarantineError('draft_requires_explicit_isolated_local_staging')


def immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    before = storage.object_snapshot(key)
    if before is not None:
        if storage.read_key(key) != body or storage.object_snapshot(key) != before:
            raise MirtaQuarantineError(f'immutable_staging_conflict:{key}')
    else:
        storage.write_key_if_unchanged(key, body, None)


def quarantine(storage: StagingStorageResource, body: bytes, receipt: dict, reason: str) -> str:
    prefix = f'_quarantine/{SLUG}/{sha(body)}/{uuid4().hex}'
    immutable(storage, f'{prefix}/response.body', body[:1_048_576])
    immutable(storage, f'{prefix}/receipt.json', encode(receipt))
    immutable(storage, f'{prefix}/report.json', encode({'approved': False, 'status': 'quarantined', 'reason': reason}))
    return prefix


def validate_receipt(body: bytes, receipt: dict, url: str) -> None:
    if (receipt.get('requested_url') != url or type(receipt.get('status')) is not int
            or receipt['status'] != 200 or type(receipt.get('byte_length')) is not int
            or receipt['byte_length'] != len(body) or receipt.get('sha256') != sha(body)):
        raise MirtaQuarantineError('capture_receipt_url_status_hash_length_mismatch')
    timestamp = receipt.get('observed_at')
    if not isinstance(timestamp, str) or not re.fullmatch(
            r'\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)', timestamp):
        raise MirtaQuarantineError('capture_receipt_requires_strict_aware_iso_timestamp')
    if datetime.fromisoformat(timestamp.replace('Z', '+00:00')).tzinfo is None:
        raise MirtaQuarantineError('capture_receipt_requires_timezone')
    final_url = receipt.get('final_url')
    if not isinstance(final_url, str) or urlsplit(final_url).username is not None:
        raise MirtaQuarantineError('capture_receipt_final_url_invalid')
    if final_url != url:
        raise MirtaQuarantineError('capture_redirect_not_pinned')
    if url == ARCHIVE_URL:
        if len(body) != ARCHIVE_BYTES or sha(body) != ARCHIVE_SHA256:
            raise MirtaQuarantineError('pinned_archive_content_drift')
    elif url == RELEASE_NOTES_URL:
        if len(body) > MAX_AUX_BYTES:
            raise MirtaQuarantineError('release_notes_capture_size_limit')
    else:
        if len(body) > MAX_LANDING_BYTES:
            raise MirtaQuarantineError('landing_capture_size_limit')
        decoded = body.decode('utf-8', errors='replace')
        for marker in LANDING_MARKERS:
            if marker not in decoded:
                raise MirtaQuarantineError('landing_marker_drift:' + marker)


def _live_capture(storage: StagingStorageResource, url: str) -> tuple[bytes, dict]:
    receipt = {'requested_url': url, 'observed_at': datetime.now(timezone.utc).isoformat(), 'attempt': 1}
    body = b''
    try:
        with httpx.Client(follow_redirects=False, timeout=120, verify=tls_context(),
                          headers={'Accept-Encoding': 'identity',
                                   'User-Agent': 'hifld-next-datasets/mirta-dod-sites-boundaries-draft'}) as client:
            with client.stream('GET', url) as response:
                receipt.update(status=response.status_code,
                               content_length_header=response.headers.get('content-length'),
                               final_url=str(response.url))
                limit = ARCHIVE_BYTES if url == ARCHIVE_URL else MAX_AUX_BYTES if url == RELEASE_NOTES_URL else MAX_LANDING_BYTES
                chunks = []
                for block in response.iter_bytes(chunk_size=1 << 20):
                    chunks.append(block)
                    if sum(len(c) for c in chunks) > limit + 1:
                        raise MirtaQuarantineError('response_size_limit')
                body = b''.join(chunks)
                del chunks
                receipt.update(byte_length=len(body), sha256=sha(body),
                               wire_byte_length=response.num_bytes_downloaded)
                validate_receipt(body, receipt, url)
                if (response.headers.get('content-length') is not None
                        and int(response.headers['content-length']) != response.num_bytes_downloaded):
                    raise MirtaQuarantineError('incomplete_content_length')
    except (httpx.HTTPError, OSError, ValueError, TypeError) as exc:
        receipt.update(byte_length=len(body), sha256=sha(body), partial=True)
        location = quarantine(storage, body, receipt, f'{type(exc).__name__}:{exc}')
        raise MirtaQuarantineError(f'bounded_fetch_failed:{exc}; retained:{location}') from exc
    return body, receipt


def acquire(storage: StagingStorageResource, capture_dir: Path | None) -> dict:
    """Live-capture the producer roster, or replay a retained capture directory."""
    require_local(storage)
    captures = {}
    for name, url in URLS.items():
        if capture_dir is not None:
            body = b''
            receipt_body = b''
            receipt = {'capture_path': str(capture_dir), 'requested_url': url}
            try:
                limit = ARCHIVE_BYTES if url == ARCHIVE_URL else MAX_AUX_BYTES
                with (capture_dir / (name + '.body')).open('rb') as stream:
                    body = stream.read(limit + 1)
                receipt_body = (capture_dir / (name + '.receipt.json')).read_bytes()
                immutable(storage, f'_raw-captures/{SLUG}/{sha(body)}/receipts/{sha(receipt_body)}.body', receipt_body)
                receipt = strict_json(receipt_body)
                validate_receipt(body, receipt, url)
            except (OSError, ValueError, TypeError) as exc:
                location = quarantine(storage, body, receipt, str(exc))
                immutable(storage, f'{location}/original-receipt.body', receipt_body)
                raise MirtaQuarantineError(f'{exc}; retained:{location}') from exc
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
        raise MirtaQuarantineError('gdal_polygon_organization_must_stay_default')
    return {**identity, 'gdal': pyogrio.__gdal_version_string__, 'pyogrio': pyogrio.__version__,
            'gdal_polygon_organization': organization, 'gdal_arc_step_degrees': ARC_STEP_DEGREES,
            'verifier_sha256': verifier_implementation_sha256()}


def _control_documents(captures: dict) -> dict:
    validate_receipt(*captures['landing'], LANDING_URL)
    validate_receipt(*captures['release_notes'], RELEASE_NOTES_URL)
    return {'landing': captures['landing'][0], 'release_notes': captures['release_notes'][0]}


def walk_wkb(w: bytes, offset: int = 0) -> tuple[int, set[int], int]:
    """Count stored coordinate pairs and collect ISO 2D subtype codes.

    The top-level call requires the walk to consume exactly the whole buffer;
    recursive member walks return their end offset for the parent to check.
    """
    count, subtypes, end = _walk_wkb(w, offset)
    if offset == 0 and end != len(w):
        raise MirtaQuarantineError('wkb_trailing_bytes')
    return count, subtypes, end


def _walk_wkb(w: bytes, offset: int) -> tuple[int, set[int], int]:
    if w[offset] not in (0, 1):
        raise MirtaQuarantineError('wkb_byte_order_marker_invalid')
    endian = '<' if w[offset] == 1 else '>'
    code = struct.unpack(endian + 'I', w[offset + 1:offset + 5])[0]
    offset += 5
    base = code % 1000
    if base == 1:
        return 1, {1}, offset + 16
    if base in (2, 8):
        count = struct.unpack(endian + 'I', w[offset:offset + 4])[0]
        return count, {base}, offset + 4 + 16 * count
    if base in (3,):
        rings, offset = struct.unpack(endian + 'I', w[offset:offset + 4])[0], offset + 4
        total = 0
        for _ in range(rings):
            points, offset = struct.unpack(endian + 'I', w[offset:offset + 4])[0], offset + 4
            total += points
            offset += 16 * points
        return total, {3}, offset
    if base in (9, 10):
        members, offset = struct.unpack(endian + 'I', w[offset:offset + 4])[0], offset + 4
        total, subtypes = 0, {base}
        for _ in range(members):
            count, codes, offset = _walk_wkb(w, offset)
            total += count
            subtypes |= codes
        return total, subtypes, offset
    if base in (4, 5, 6, 7, 11, 12):
        members, offset = struct.unpack(endian + 'I', w[offset:offset + 4])[0], offset + 4
        total, subtypes = 0, {base}
        for _ in range(members):
            count, codes, offset = _walk_wkb(w, offset)
            total += count
            subtypes |= codes
        return total, subtypes, offset
    raise MirtaQuarantineError(f'unsupported_wkb_subtype:{code}')


def wrap_multi_polygon(w: bytes) -> bytes:
    """Deterministically wrap one 2D Polygon WKB into a one-member MultiPolygon."""
    order = w[0]
    endian = '<' if order == 1 else '>'
    if struct.unpack(endian + 'I', w[1:5])[0] != 3:
        raise MirtaQuarantineError('wrap_requires_polygon_wkb')
    return bytes([order]) + struct.pack(endian + 'I', 6) + struct.pack(endian + 'I', 1) + w


def _attribute_census(data: dict, audit: dict) -> None:
    oids = data['OBJECTID']
    if (len(oids) != BOUNDARY_FEATURES or len(set(oids)) != BOUNDARY_FEATURES
            or oids != sorted(oids) or min(oids) != EXPECTED_OBJECTID_MIN
            or max(oids) != EXPECTED_OBJECTID_MAX):
        raise MirtaQuarantineError('objectid_roster_drift')
    nulls = {name: sum(1 for value in data[name] if value is None or (isinstance(value, str) and value == ''))
             for name in EXPECTED_FIELD_WIDTHS}
    if nulls != EXPECTED_NULLS:
        raise MirtaQuarantineError('null_census_drift')
    for name, value in EXPECTED_CONSTANT_VALUES.items():
        census = {key for key in data[name]}
        if census != {value}:
            raise MirtaQuarantineError('constant_column_value_drift:' + name)
    for name, expected in EXPECTED_VALUE_CENSUSES.items():
        observed = {}
        for value in data[name]:
            observed[value] = observed.get(value, 0) + 1
        if observed != expected:
            raise MirtaQuarantineError('value_census_drift:' + name)
    sds = [(value or '').strip() for value in data['sdsId']]
    non_null = [value for value in sds if value]
    groups: dict[str, int] = {}
    for value in non_null:
        groups[value] = groups.get(value, 0) + 1
    if (len(non_null) != EXPECTED_SDS_ID_NON_NULL or len(groups) != EXPECTED_SDS_ID_UNIQUE
            or sum(count - 1 for count in groups.values()) != EXPECTED_SDS_ID_DUPLICATE_ROWS
            or sum(1 for count in groups.values() if count > 1) != EXPECTED_SDS_ID_SHARED_GROUPS
            or any(not SDS_ID_PATTERN.fullmatch(value) for value in non_null)):
        raise MirtaQuarantineError('sds_identifier_census_drift')
    conflicting = 0
    for value, count in groups.items():
        if count > 1 and len({data['siteName'][index] for index, other in enumerate(sds) if other == value}) > 1:
            conflicting += 1
    if conflicting != EXPECTED_SDS_ID_CONFLICTING_SITE_GROUPS:
        raise MirtaQuarantineError('sds_identifier_site_conflict_census_drift')
    if sum(1 for a, b in zip(data['featureName'], data['siteName']) if a == b) != EXPECTED_FEATURE_NAME_SITE_NAME_EQUAL:
        raise MirtaQuarantineError('feature_name_site_name_census_drift')
    audit['source_null_census'] = nulls
    audit['source_sds_id_census'] = {'non_null': len(non_null), 'unique': len(groups),
                                     'duplicate_rows': EXPECTED_SDS_ID_DUPLICATE_ROWS,
                                     'shared_groups': EXPECTED_SDS_ID_SHARED_GROUPS,
                                     'conflicting_site_name_groups': conflicting}
    audit['source_id_field'] = 'OBJECTID'
    audit['source_unique_id_count'] = len(set(oids))


def _source_geometry_census(data: dict, audit: dict) -> tuple[int, ...]:
    subtype_census: dict[int, int] = {}
    vertices: list[int] = []
    curved: list[int] = []
    total = 0
    for index, wkb in enumerate(data['SHAPE']):
        count, codes, end = walk_wkb(wkb, 0)
        if end != len(wkb):
            raise MirtaQuarantineError('wkb_trailing_bytes')
        for code in codes:
            subtype_census[code] = subtype_census.get(code, 0) + 1
        vertices.append(count)
        total += count
        if struct.unpack('<I', wkb[1:5])[0] == 12:
            curved.append(index)
    if subtype_census != EXPECTED_SOURCE_WKB_SUBTYPES:
        raise MirtaQuarantineError('source_wkb_subtype_census_drift')
    if total != EXPECTED_SOURCE_VERTICES or len(curved) != EXPECTED_CURVED_FEATURES:
        raise MirtaQuarantineError('source_geometry_census_drift')
    audit['source_wkb_subtype_census'] = dict(sorted(subtype_census.items()))
    audit['source_vertices'] = total
    audit['source_curved_multisurface_features'] = len(curved)
    audit['source_linear_multipolygon_features'] = EXPECTED_LINEAR_FEATURES
    return tuple(vertices)


def _linearize_curved(data: dict, workdir: Path) -> tuple[list[bytes], dict]:
    """Linearize the curved producer WKBs through GDAL at the pinned arc step.

    The 704 linear MultiPolygon features keep their exact source WKB bytes.
    Curved MultiSurface features pass through a temporary linear-only ESRI
    Shapefile vessel so GDAL's own arc code produces the chord approximation;
    single Polygon results are wrapped into one-member MultiPolygon WKB.
    """
    if os.environ.get('OGR_ARC_STEPSIZE') is not None:
        raise MirtaQuarantineError('ambient_gdal_arc_step_must_not_be_set')
    pyogrio.set_gdal_config_options({'OGR_ARC_STEPSIZE': ARC_STEP_DEGREES})
    try:
        vessel = workdir / 'curve-vessel'
        vessel.mkdir()
        source = pa.table({'OBJECTID': pa.array(data['OBJECTID'], type=pa.int64()),
                           'SHAPE': pa.array(data['SHAPE'], type=pa.binary())})
        pyogrio.write_arrow(source, str(vessel / 'vessel.shp'), layer='vessel', driver='ESRI Shapefile',
                            geometry_name='SHAPE', geometry_type='MultiPolygon',
                            layer_options={'FID': 'OBJECTID', 'GEOMETRY_NAME': 'SHAPE'})
        _, vessel_table = pyogrio.read_arrow(str(vessel / 'vessel.shp'), return_fids=True)
        name = next(n for n in vessel_table.schema.names if str(vessel_table.schema.field(n).type) == 'binary')
        vessel_rows = vessel_table.to_pydict()
        if (vessel_rows['OBJECTID'] != data['OBJECTID'] or len(vessel_rows[name]) != BOUNDARY_FEATURES
                or any(w is None for w in vessel_rows[name])):
            raise MirtaQuarantineError('curve_vessel_order_or_population_drift')
        candidate: list[bytes] = []
        kept = 0
        linearized = 0
        invalid_kept = 0
        invalid_linearized = 0
        for index in range(BOUNDARY_FEATURES):
            source_code = struct.unpack('<I', data['SHAPE'][index][1:5])[0]
            if source_code == 6:
                candidate.append(data['SHAPE'][index])
                kept += 1
                if not shapely.from_wkb(candidate[-1]).is_valid:
                    invalid_kept += 1
                continue
            vessel_code = struct.unpack(('<' if vessel_rows[name][index][0] == 1 else '>')
                                        + 'I', vessel_rows[name][index][1:5])[0]
            if vessel_code == 3:
                wrapped = wrap_multi_polygon(vessel_rows[name][index])
            elif vessel_code == 6:
                wrapped = vessel_rows[name][index]
            else:
                raise MirtaQuarantineError('curve_vessel_unexpected_linear_subtype:' + str(vessel_code))
            candidate.append(wrapped)
            linearized += 1
            if not shapely.from_wkb(wrapped).is_valid:
                invalid_linearized += 1
        if (kept != EXPECTED_LINEAR_FEATURES or linearized != EXPECTED_CURVED_FEATURES
                or invalid_kept != EXPECTED_KEPT_INVALID_GEOMETRY_COUNT
                or invalid_linearized != EXPECTED_LINEARIZED_INVALID_GEOMETRY_COUNT):
            raise MirtaQuarantineError('linearization_census_drift')
        geometry = shapely.from_wkb(candidate)
        if any(geometry.geom_type != 'MultiPolygon'):
            raise MirtaQuarantineError('candidate_geometry_type_drift')
        digest = hashlib.sha256()
        for index in range(BOUNDARY_FEATURES):
            value = [data['OBJECTID'][index], candidate[index].hex()]
            digest.update(len(encode(value)).to_bytes(8, 'big'))
            digest.update(encode(value))
        audit = {'linearized_features': linearized, 'kept_exact_features': kept,
                 'invalid_kept': invalid_kept, 'invalid_linearized': invalid_linearized,
                 'gdal_arc_step_degrees': ARC_STEP_DEGREES,
                 'linearization_vehicle': 'temporary linear-only ESRI Shapefile vessel read back through GDAL',
                 'candidate_geometry_sha256': digest.hexdigest()}
        return candidate, audit
    finally:
        pyogrio.set_gdal_config_options({'OGR_ARC_STEPSIZE': None})


def inspect_source(captures: dict, workdir: Path) -> tuple[dict, dict, list[bytes]]:
    """Census the pinned boundary layer from the retained archive bytes."""
    _control_documents(captures)
    archive = captures['archive'][0]
    (workdir / ARCHIVE_NAME).write_bytes(archive)
    with zipfile.ZipFile(workdir / ARCHIVE_NAME) as archive_zip:
        members = archive_zip.infolist()
        names = [item.filename for item in members]
        if len(names) != ZIP_MEMBERS:
            raise MirtaQuarantineError('zip_member_census_drift')
        if {name.split('/')[0] for name in names} != {GDB_TOP_DIR}:
            raise MirtaQuarantineError('zip_top_directory_changed')
        if any(PurePosixPath(name).is_absolute() or '..' in PurePosixPath(name).parts for name in names):
            raise MirtaQuarantineError('zip_member_path_unsafe')
        if sum(item.file_size for item in members) != ZIP_UNCOMPRESSED_BYTES:
            raise MirtaQuarantineError('zip_uncompressed_census_drift')
        archive_zip.extractall(workdir)
    gdb = workdir / GDB_TOP_DIR
    layers = {name: kind for name, kind in pyogrio.list_layers(gdb)}
    if (set(layers) != {BOUNDARY_LAYER, POINT_LAYER}
            or layers.get(BOUNDARY_LAYER) != 'MultiPolygon' or layers.get(POINT_LAYER) != 'Point'):
        raise MirtaQuarantineError('gdb_layer_roster_changed')
    point_info = pyogrio.read_info(gdb, layer=POINT_LAYER)
    if (point_info.get('features') != POINT_FEATURES or point_info.get('crs') != 'EPSG:4326'
            or point_info.get('geometry_type') != 'Point'
            or list(point_info['fields']) != POINT_FIELDS
            or list(point_info['total_bounds']) != POINT_BOUNDS):
        raise MirtaQuarantineError('sibling_point_layer_census_drift')
    info = pyogrio.read_info(gdb, layer=BOUNDARY_LAYER)
    if (info.get('features') != BOUNDARY_FEATURES or info.get('crs') != 'EPSG:4326'
            or info.get('geometry_type') != 'MultiPolygon' or info.get('fid_column') != 'OBJECTID'
            or info.get('geometry_name') != 'SHAPE' or info.get('driver') != 'OpenFileGDB'
            or list(info['fields']) != [*EXPECTED_FIELDS, 'SHAPE_Length', 'SHAPE_Area']
            or [str(dtype) for dtype in info['dtypes']] != ['object'] * len(EXPECTED_FIELDS) + ['float64'] * 2
            or list(info['total_bounds']) != EXPECTED_BOUNDS):
        raise MirtaQuarantineError('boundary_layer_identity_or_schema_changed')
    meta, table = pyogrio.read_arrow(gdb, layer=BOUNDARY_LAYER, return_fids=True)
    columns = table.schema.names
    if columns != ['OBJECTID', *EXPECTED_FIELDS, 'SHAPE_Length', 'SHAPE_Area', 'SHAPE']:
        raise MirtaQuarantineError('native_arrow_schema_changed')
    if [str(field.type) for field in table.schema] != (['int64'] + ['string'] * len(EXPECTED_FIELDS)
                                                       + ['double'] * 2 + ['binary']):
        raise MirtaQuarantineError('native_arrow_physical_types_changed')
    for index, name in enumerate(EXPECTED_FIELDS):
        arrow_meta = table.schema.field(index + 1).metadata or {}
        width = arrow_meta.get(b'GDAL:OGR:width')
        alias = (arrow_meta.get(b'GDAL:OGR:alternative_name') or b'').decode()
        expected_width = EXPECTED_FIELD_WIDTHS[name]
        if (width is None) != (expected_width is None) or (width is not None and int(width) != expected_width) or alias != EXPECTED_FIELD_ALIASES[name]:
            raise MirtaQuarantineError('field_width_or_alias_drift:' + name)
    data = table.to_pydict()
    data['schema_meta'] = {name: dict(table.schema.field(index + 1).metadata or {})
                           for index, name in enumerate(EXPECTED_FIELDS)}
    audit: dict = {
        'feature_count': BOUNDARY_FEATURES,
        'source_ids_sha256': sha(encode(data['OBJECTID'])),
        'source_record_sha256': _digest_records(table, data),
        'source_geometry_sha256': _digest_geometry(data, data['SHAPE']),
        'source_schema_sha256': sha(encode([(name, str(table.schema.field(name).type)) for name in columns])),
        'source_bounds': EXPECTED_BOUNDS,
        'source_crs': 'EPSG:4326',
        'geometry_type': 'MultiPolygon',
        'archive_bytes': ARCHIVE_BYTES,
        'archive_sha256': ARCHIVE_SHA256,
        'archive_sha256_is_measured_pin': True,
        'zip_members': ZIP_MEMBERS,
        'zip_uncompressed_bytes': ZIP_UNCOMPRESSED_BYTES,
        'gdb_top_dir': GDB_TOP_DIR,
        'sibling_point_layer': {'layer': POINT_LAYER, 'feature_count': POINT_FEATURES,
                                'fields': POINT_FIELDS, 'bounds': POINT_BOUNDS,
                                'selected': False,
                                'note': 'census-receipted only; the sibling mirta-dod-sites-points issue owns it'},
    }
    _attribute_census(data, audit)
    _source_geometry_census(data, audit)
    candidate, linear_audit = _linearize_curved(data, workdir)
    audit['linearization'] = linear_audit
    geometry = shapely.from_wkb(candidate)
    invalid = int((~shapely.is_valid(geometry)).sum())
    empty = int(shapely.is_empty(geometry).sum())
    vertices = int(shapely.get_num_coordinates(geometry).sum())
    bounds = [float(value) for value in shapely.total_bounds(geometry)]
    if (invalid != EXPECTED_INVALID_GEOMETRY_COUNT or empty != EXPECTED_EMPTY_GEOMETRY_COUNT
            or vertices != EXPECTED_CANDIDATE_VERTICES or bounds != EXPECTED_BOUNDS):
        raise MirtaQuarantineError('candidate_geometry_census_drift')
    audit['source_invalid_geometry_count'] = invalid
    audit['source_null_geometry_count'] = 0
    audit['source_empty_geometry_count'] = empty
    audit['candidate_bounds'] = bounds
    audit['geometry_input'] = ('official DISDI File Geodatabase boundary layer read by GDAL OpenFileGDB; '
                               '704 linear MultiPolygon WKBs retained bit-exact, 121 curved MultiSurface '
                               'features linearized with GDAL arc code at the pinned 0.01-degree step, '
                               'original curved WKBs retained in the raw archive')
    audit['invalid_geometries_preserved_unrepaired'] = True
    audit['release_blocked_reason'] = ('curved-geometry linearization parameters, FY24-vs-FY22 edition '
                                       'relationship, schema/ID contract, rights and release acceptance all '
                                       'require maintainer decisions; 28 invalid producer geometries preserved unrepaired')
    audit['baseline_comparison'] = ('unresolved; the FY 2022 Version 1 HIFLD edition captured by this issue is '
                                    'a prior annual release whose exact DHS distribution is dead (verified '
                                    '2026-11-12) and whose DHS crosswalk continuation requires GII access')
    return data, audit, candidate


def _digest_records(table: pa.Table, data: dict) -> str:
    digest = hashlib.sha256()
    names = [name for name in table.schema.names if name not in ('OBJECTID', 'SHAPE')]
    for index in range(BOUNDARY_FEATURES):
        row = {name: data[name][index] for name in names}
        digest.update(len(encode(row)).to_bytes(8, 'big'))
        digest.update(encode(row))
    return digest.hexdigest()


def _digest_geometry(data: dict, wkbs: list[bytes]) -> str:
    digest = hashlib.sha256()
    for index in range(BOUNDARY_FEATURES):
        value = [data['OBJECTID'][index], wkbs[index].hex()]
        digest.update(len(encode(value)).to_bytes(8, 'big'))
        digest.update(encode(value))
    return digest.hexdigest()


def candidate_table(data: dict, candidate_geometry: list[bytes]) -> pa.Table:
    fields = [pa.field('OBJECTID', pa.int64(), nullable=False)]
    columns: list = [pa.array(data['OBJECTID'], type=pa.int64())]
    for index, name in enumerate(EXPECTED_FIELDS):
        arrow_meta = {key: value for key, value in (data['schema_meta'][name] or {}).items()}
        fields.append(pa.field(name, pa.string(), nullable=bool(EXPECTED_NULLS[name]), metadata=arrow_meta))
        columns.append(pa.array(data[name], type=pa.string()))
    for name in ('SHAPE_Length', 'SHAPE_Area'):
        fields.append(pa.field(name, pa.float64(), nullable=False))
        columns.append(pa.array(data[name], type=pa.float64()))
    fields.append(pa.field('shape', pa.binary(), nullable=False))
    columns.append(pa.array(candidate_geometry, type=pa.binary()))
    table = pa.Table.from_arrays(columns, schema=pa.schema(fields))
    geo = {'version': '1.1.0', 'primary_column': 'shape',
           'columns': {'shape': {'encoding': 'WKB', 'geometry_types': ['MultiPolygon'],
                                 'crs': pyproj.CRS(4326).to_json_dict(), 'bbox': EXPECTED_BOUNDS}}}
    return table.replace_schema_metadata({b'geo': encode(geo)})


def verify_candidate_readback(parquet: bytes, data: dict, candidate_geometry: list[bytes]) -> None:
    table = pq.read_table(pa.BufferReader(pa.py_buffer(parquet)))
    if table.num_rows != BOUNDARY_FEATURES:
        raise MirtaQuarantineError('candidate_row_count_drift')
    if table.schema.names != ['OBJECTID', *EXPECTED_FIELDS, 'SHAPE_Length', 'SHAPE_Area', 'shape']:
        raise MirtaQuarantineError('candidate_field_roster_drift')
    if table['OBJECTID'].to_pylist() != data['OBJECTID']:
        raise MirtaQuarantineError('candidate_objectid_drift')
    for name in EXPECTED_FIELDS:
        if table[name].to_pylist() != data[name]:
            raise MirtaQuarantineError('candidate_attribute_drift:' + name)
    for name in ('SHAPE_Length', 'SHAPE_Area'):
        if table[name].to_pylist() != data[name]:
            raise MirtaQuarantineError('candidate_shape_measure_drift:' + name)
    if table['shape'].to_pylist() != candidate_geometry:
        raise MirtaQuarantineError('candidate_native_geometry_drift')
    geo = json.loads(table.schema.metadata[b'geo'])
    if (geo['primary_column'] != 'shape' or geo['columns']['shape']['geometry_types'] != ['MultiPolygon']
            or geo['columns']['shape']['crs'] != pyproj.CRS(4326).to_json_dict()
            or geo['columns']['shape']['bbox'] != EXPECTED_BOUNDS):
        raise MirtaQuarantineError('candidate_geoparquet_metadata_drift')


def _prebuilt_policy(objects: dict) -> dict:
    return {'schema_version': 1, 'formats': {'geoparquet': {
        'reason': 'Native candidate retains all 13 gdb attribute fields with declared widths/aliases, '
                  'source OBJECTID, SHAPE_Length/SHAPE_Area and native EPSG:4326 WKB boundaries; contract, '
                  'baseline, rights and release approvals remain pending.',
        'objects': [{'path': relative, **objects[relative]} for relative in sorted(objects)]}}}


def provenance(captures: dict, identity: dict, audit: dict, objects: dict) -> dict:
    if set(captures) != set(URLS):
        raise MirtaQuarantineError('capture_roster_drift')
    for name, url in URLS.items():
        validate_receipt(*captures[name], url)
    inputs = []
    for name, (body, receipt) in captures.items():
        record = {
            'input_id': name + ':' + sha(body), 'producer': PRODUCER,
            'host': urlsplit(receipt['requested_url']).hostname,
            'role': 'producer_publication',
            'canonical_url': receipt['requested_url'],
            'retrieval_url': receipt['requested_url'], 'observed_final_url': receipt['final_url'],
            'retrieved_at': receipt['observed_at'], 'http_status': receipt['status'],
            'sha256': sha(body), 'bytes': len(body),
            'license': LICENSE,
            'attribution': 'Military Installations, Ranges and Training Areas (MIRTA) DoD Sites; '
                           'compiled by the DISDI Program, OASD(EI&E), U.S. Department of Defense.',
            'lineage': ('acq.osd.mil is the producer DISDI Program (OASD(EI&E)) web publication; the landing '
                        'page pins the download and release-notes URLs and delegates the MIRTA map viewer to '
                        'the U.S. Army Corps of Engineers host geospatial.sec.usace.army.mil.'),
            'release_date': None,
            'release_date_unknown_reason': ('The FY24 package release notes are dated 2025-09-02 and the DISDI '
                                            'landing page titles the release "(2024)"; no machine-readable '
                                            'release date is published for the zip.'),
        }
        if name == 'archive':
            record['content_identity'] = {'bytes': ARCHIVE_BYTES, 'sha256': ARCHIVE_SHA256,
                                          'measured_pin_not_source_published': True}
        if name == 'landing':
            record['authority_markers'] = list(LANDING_MARKERS)
        if name == 'release_notes':
            record['edition_statement'] = 'FY 2024 Base Structure Report sites; FIRRMA 31 CFR Part 802 map sites'
        inputs.append(record)
    manifest = {
        'schema_version': 1, 'processor': 'HIFLD Next', 'approved': False,
        'code_commit': identity['code_commit'], 'code_identity_scheme': identity['code_identity_scheme'],
        'title': 'Military Installations, Ranges, and Training Areas (MIRTA) DoD Sites - Boundaries (FY24 boundaries layer)',
        'publisher': PRODUCER, 'agency': 'U.S. Department of Defense',
        'source_url': LANDING_URL, 'license': 'unknown',
        'date_issued': None, 'date_modified': None,
        'inputs': inputs,
        'transformations': [{
            'operation': 'Extract the DISDI FY24 boundary feature class into GeoParquet WKB',
            'parameters': {
                'selected_layer': BOUNDARY_LAYER,
                'source_ids': 'OBJECTID retained (825 unique ascending values, 1..992 with producer gaps); '
                              'sdsId retained with its 202 nulls and 4 duplicate rows across 3 shared GUID '
                              'groups (one group spans two different siteName values); no substitute IDs',
                'source_fields': 'all 13 gdb attribute fields with declared widths and release-notes aliases, '
                                 'plus SHAPE_Length/SHAPE_Area and native OBJECTID; no renames or drops',
                'geometry': audit['geometry_input'],
                'crs': 'native EPSG:4326 preserved unreprojected',
                'gdal_polygon_organization': identity['gdal_polygon_organization'],
                'gdal_arc_step_degrees': ARC_STEP_DEGREES,
                'curved_geometry_linearization': '121 curved MultiSurface features linearized through a '
                                                 'temporary linear-only Shapefile vessel; 704 linear features '
                                                 'keep bit-exact GDB WKB; curved originals retained in raw',
                'known_invalid_geometries': '28 invalid producer geometries preserved unrepaired; release blocked',
                'sibling_layer': 'FY24MirtaLocations points (831 features) census-receipted, never selected',
                'candidate_status': 'unapproved; FY24 changed-successor vs FY22 HIFLD baseline equivalence unresolved',
            }}],
        'lineage': ('The DISDI Program (OASD(EI&E)) publishes MIRTA annually on acq.osd.mil. HIFLD Open tracked '
                    'the FY 2022 Version 1 edition (edition date 2023-07-31; the FY22 FGDC metadata bounding '
                    'rectangle is identical to this FY24 boundary layer extent). The exact FY22 DHS-hosted '
                    'FeatureServer (services1 FY22_MIRTA_Boundaries_gdb) is dead and the DHS crosswalk routes '
                    'the layer to the credential-gated GII portal (DUA not required, GII access required), so '
                    'this draft ingests the current producer release (FY24 BSR-based; release notes dated '
                    '2025-09-02). Research corroboration, not a pipeline fetch: the USACE-hosted MIRTA viewer '
                    'service referenced by the DISDI landing page served boundary attributes equal to this FY24 '
                    'GDB layer (825 rows, multiset-equal after brace/case normalization) on 2026-11-12, and the '
                    'REPI repi.osd.mil FY19_MIRTA_Boundaries layer (745 features, 73 California) is an older '
                    'partial DoD copy that was not selected. Baseline acceptance, curved-linearization '
                    'parameters, schema/ID contract, rights and release acceptance remain maintainer decisions.'),
        'unsupported_output_formats': {
            'shapefile_zip': 'No accepted schema/identifier mapping or packaging decision; curved-geometry '
                             'linearization also requires an approved field-order/width mapping for DBF.',
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
        raise MirtaQuarantineError('unsafe_local_candidate_prefix')
    target = root / key
    current = root
    for part in target.relative_to(root).parts:
        current = current / part
        if current.is_symlink():
            raise MirtaQuarantineError('linked_candidate_path')
    return target


@contextmanager
def candidate_lock(storage: StagingStorageResource, prefix: str):
    target = local_candidate_path(storage, prefix)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(target.parent / ('.' + target.name + '.lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'r+b') as held:
        if os.fstat(held.fileno()).st_nlink != 1:
            raise MirtaQuarantineError('linked_candidate_lock')
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
            raise MirtaQuarantineError('linked_candidate_lock')
        fcntl.flock(held, fcntl.LOCK_EX)
        if target.exists() or target.is_symlink():
            raise MirtaQuarantineError('concurrent_or_incomplete_existing_candidate')
        with tempfile.TemporaryDirectory(prefix='.candidate-build-', dir=target.parent) as tmp:
            private = StagingStorageResource(local_dir=tmp, use_local=True)
            for name, value in payloads.items():
                immutable(private, name, value)
            immutable(private, 'metadata/candidate_ready.json', encode(ready))
            os.rename(tmp, target)


def reconcile_cached_candidate(storage: StagingStorageResource, prefix: str, ready: dict,
                               data: dict, audit: dict, candidate_geometry: list[bytes],
                               captures: dict, identity: dict) -> None:
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
            raise MirtaQuarantineError('cached_authored_manifest_snapshot_differs')
    paths = list(root.rglob('*'))
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink != 1) for p in paths):
        raise MirtaQuarantineError('linked_candidate_object')
    actual = {str(p.relative_to(root)) for p in paths if p.is_file()}
    expected = set(ready['objects']) | {'metadata/candidate_ready.json'}
    if not expected <= actual or actual - expected - allowed_derivatives:
        raise MirtaQuarantineError('existing_candidate_unrostered_or_missing_object')
    try:
        validate_staged_release_derivatives(storage, prefix, {f'{prefix}/{name}' for name in actual})
    except (OSError, UnicodeError, ValueError, TypeError) as exc:
        raise MirtaQuarantineError('invalid_staged_release_derivatives') from exc
    for name, expected_sha in ready['objects'].items():
        key = f'{prefix}/{name}'
        snapshot = storage.object_snapshot(key)
        if snapshot is None or sha(storage.read_key(key)) != expected_sha or storage.object_snapshot(key) != snapshot:
            raise MirtaQuarantineError(f'existing_candidate_object_changed:{name}')
    cached = {name: ((root / f'raw/captures/{name}.body').read_bytes(),
                     strict_json((root / f'raw/captures/{name}.receipt.json').read_bytes()))
              for name in URLS}
    for name in URLS:
        if cached[name][0] != captures[name][0]:
            raise MirtaQuarantineError('cached_capture_differs_from_fresh_source')
        validate_receipt(*cached[name], URLS[name])
    if ready.get('source_record_sha256') != audit['source_record_sha256'] \
            or ready.get('source_geometry_sha256') != audit['source_geometry_sha256'] \
            or ready.get('candidate_geometry_sha256') != audit['linearization']['candidate_geometry_sha256'] \
            or ready.get('source_schema_sha256') != audit['source_schema_sha256'] \
            or ready.get('processing_fingerprint') != sha(encode(identity)):
        raise MirtaQuarantineError('cached_candidate_identity_conflict')
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
            raise MirtaQuarantineError('cached_resolved_provenance_differs')
    if set(manifest) != set(expected_manifest):
        raise MirtaQuarantineError('cached_provenance_field_roster_drift')
    for name, value in expected_manifest.items():
        if name != 'code_commit' and manifest.get(name) != value:
            raise MirtaQuarantineError('cached_provenance_differs_from_original_capture')
    for name in ('metadata/source_audit.json', 'metadata/processing_identity.json',
                 'metadata/source_capture_manifest.json'):
        if (root / name).read_bytes() != {'metadata/source_audit.json': encode(audit),
                                          'metadata/processing_identity.json': encode(identity),
                                          'metadata/source_capture_manifest.json': encode(
                                              {'complete': True, 'source_feature_count': BOUNDARY_FEATURES,
                                               'receipts': {n: cached[n][1] for n in sorted(cached)}})}[name]:
            raise MirtaQuarantineError('cached_source_audit_or_identity_drift:' + name)
    verify_candidate_readback(staged_parquet, data, candidate_geometry)


def stage_mirta_boundaries(storage: StagingStorageResource, *, capture_dir: Path | None = None) -> dict:
    require_local(storage)
    if shutil.disk_usage(Path(storage.local_dir)).free < DISK_FLOOR_BYTES:
        raise MirtaQuarantineError('disk_floor_before_acquisition')
    captures = acquire(storage, capture_dir)
    body, receipt = captures['archive']
    try:
        identity = processing_identity()
        with tempfile.TemporaryDirectory(prefix='mirta-boundaries-') as tmp:
            data, audit, candidate_geometry = inspect_source(captures, Path(tmp))
            version = content_derived_version(audit['source_record_sha256'],
                                              audit['linearization']['candidate_geometry_sha256'],
                                              audit['source_schema_sha256'], identity)
            prefix = candidate_prefix(SLUG, SLUG, version)
            relative = f'geoparquet/{SLUG}.parquet'
            with candidate_lock(storage, prefix) as target:
                if target.exists():
                    try:
                        ready_key = f'{prefix}/metadata/candidate_ready.json'
                        if not storage.object_exists(ready_key):
                            raise MirtaQuarantineError('existing_candidate_missing_ready_marker')
                        ready = strict_json(storage.read_key(ready_key))
                        expected_names = ({relative, 'metadata/source_manifest.json', 'metadata/source_audit.json',
                                           'metadata/processing_identity.json', 'metadata/source_capture_manifest.json'}
                                          | {f'raw/captures/{name}{suffix}' for name in URLS
                                             for suffix in ('.body', '.receipt.json')})
                        if (set(ready.get('objects', {})) != expected_names or ready.get('version') != version
                                or ready.get('status') != 'working_draft_candidate_ready'
                                or ready.get('approved') is not False or ready.get('release_blocked') is not True):
                            raise MirtaQuarantineError('existing_candidate_inventory_conflict')
                        reconcile_cached_candidate(storage, prefix, ready, data, audit, candidate_geometry,
                                                   captures, identity)
                        return _result(version, audit, prefix, reused=True)
                    except (ValueError, TypeError, KeyError, OSError, RuntimeError) as exc:
                        quarantine_cached_candidate_held(storage, target, prefix, str(exc))
                        raise
            table = candidate_table(data, candidate_geometry)
            sink = pa.BufferOutputStream()
            pq.write_table(table, sink, compression='zstd')
            parquet = sink.getvalue().to_pybytes()
            verify_candidate_readback(parquet, data, candidate_geometry)
            manifest = provenance(captures, identity, audit,
                                  {relative: {'bytes': len(parquet), 'sha256': sha(parquet)}})
            manifest['processing_fingerprint'] = sha(encode(identity))
            payloads = {
                relative: parquet,
                'metadata/source_manifest.json': encode(manifest),
                'metadata/source_audit.json': encode(audit),
                'metadata/processing_identity.json': encode(identity),
                'metadata/source_capture_manifest.json': encode(
                    {'complete': True, 'source_feature_count': BOUNDARY_FEATURES,
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
                     'candidate_geometry_sha256': audit['linearization']['candidate_geometry_sha256'],
                     'source_schema_sha256': audit['source_schema_sha256'],
                     'source_ids_sha256': audit['source_ids_sha256'],
                     'processing_fingerprint': sha(encode(identity)),
                     'objects': {name: sha(value) for name, value in payloads.items()}}
            commit_complete_candidate(storage, prefix, payloads, ready)
            return _result(version, audit, prefix, reused=False)
    except (ValueError, TypeError, KeyError, OSError, RuntimeError, zipfile.BadZipFile) as exc:
        location = quarantine(storage, body, receipt, str(exc))
        raise MirtaQuarantineError(f'{exc}; retained:{location}') from exc


def _result(version: str, audit: dict, prefix: str, *, reused: bool) -> dict:
    return {'dataset_slug': SLUG, 'file_slug': SLUG, 'version': version,
            'feature_count': audit['feature_count'],
            'source_unique_id_count': audit['source_unique_id_count'],
            'source_null_geometry_count': audit['source_null_geometry_count'],
            'source_invalid_geometry_count': audit['source_invalid_geometry_count'],
            'source_vertices': audit['source_vertices'],
            'release_blocked': True,
            'candidate_ready': prefix + '/metadata/candidate_ready.json',
            'reused': reused}

"""Pinned draft capture of the DISDI public FY24 MIRTA DoD Sites points release.

Issue #157. The producer, the Defense Installation Spatial Data Infrastructure
(DISDI) Program within OASD(EI&E), publishes the Military Installations,
Ranges, and Training Areas (MIRTA) dataset publicly on its own acq.osd.mil
distribution as a File Geodatabase ZIP. The DHS crosswalk moves the former
HIFLD Open FY22 points layer to the credential-gated GII Portal, so this
producer-side public release is the pipeline input; DataLumos and other
mirrors are research context only, never inputs.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import shutil
import ssl
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit
from uuid import uuid4

import certifi
import fiona
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)

DATASET_SLUG = FILE_SLUG = "mirta-dod-sites-points"
LANDING_URL = "https://www.acq.osd.mil/eie/imr/rpid/disdi/index.html"
RELEASE_NOTES_URL = "https://www.acq.osd.mil/eie/imr/rpid/disdi/Downloads/release_notes.pdf"
SOURCE_URL = "https://www.acq.osd.mil/eie/imr/rpid/disdi/Downloads/installations_ranges.zip"
PUBLISHER = ("Defense Installation Spatial Data Infrastructure (DISDI) Program, Office of the Assistant "
             "Secretary of Defense for Energy, Installations, and Environment (OASD EI&E)")

# www.acq.osd.mil serves an incomplete TLS chain: the leaf certificate is issued
# by IdenTrust TrustID Server CA O1, but the server omits that intermediate
# (AIA: http://validation.identrust.com/certs/trustidcaO1.p7c). The root,
# IdenTrust Commercial Root CA 1, already ships in the standard certifi store,
# so trust is not weakened; only the published intermediate is added.
_IDENTRUST_TRUSTID_SERVER_O1_PEM = """-----BEGIN CERTIFICATE-----
MIIGvTCCBKWgAwIBAgIQQAFu+vSEq6LjsbaVYwMsczANBgkqhkiG9w0BAQsFADBKMQswCQYDVQQG
EwJVUzESMBAGA1UEChMJSWRlblRydXN0MScwJQYDVQQDEx5JZGVuVHJ1c3QgQ29tbWVyY2lhbCBS
b290IENBIDEwHhcNMTkxMjEyMTYzMjM5WhcNMjkxMjEyMTYzMjM5WjBZMQswCQYDVQQGEwJVUzES
MBAGA1UEChMJSWRlblRydXN0MRcwFQYDVQQLEw5UcnVzdElEIFNlcnZlcjEdMBsGA1UEAxMUVHJ1
c3RJRCBTZXJ2ZXIgQ0EgTzEwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC7g55cFrCo
IeDK1sq2+V5Am2l1vbqjhalYhRH6x0BhmP6XLx0jhqwcVm7HfhQ6pJQEVxo8kB0Qp9RymSOvvVNC
tW2UnmPxuoNxGFlZvjNaYf/jhMzUMGGQZ0ZXb2hKe5NMYDHwuVvnSHnTMvSYAzu9rPOruXOlfURx
YIu+3o3huWivZyBP+Ze+xMqmbRmYgnAcXSoOVI3UhwKF/r5+o+CFOBvSkgB1ov8vLhE0rLQkHCXd
k/qCmxFbwhYO+SsBrGKUfXOK8ze7AXLDn6e01EyvUscRZu8NlFParxMWW7STjwZ+kh+QFB2HLj8o
2V8irmubZMBsVkKWyG+WHoEuL7m/AgMBAAGjggKOMIICijASBgNVHRMBAf8ECDAGAQH/AgEAMA4G
A1UdDwEB/wQEAwIBhjCBiQYIKwYBBQUHAQEEfTB7MDAGCCsGAQUFBzABhiRodHRwOi8vY29tbWVy
Y2lhbC5vY3NwLmlkZW50cnVzdC5jb20wRwYIKwYBBQUHMAKGO2h0dHA6Ly92YWxpZGF0aW9uLmlk
ZW50cnVzdC5jb20vcm9vdHMvY29tbWVyY2lhbHJvb3RjYTEucDdjMB8GA1UdIwQYMBaAFO1EGcDT
8AaL7qR7vkLnJlTIjjZ2MIIBKwYDVR0gBIIBIjCCAR4wggEaBgRVHSAAMIIBEDBKBggrBgEFBQcC
ARY+aHR0cHM6Ly9zZWN1cmUuaWRlbnRydXN0LmNvbS9jZXJ0aWZpY2F0ZXMvcG9saWN5L3RzL2lu
ZGV4Lmh0bWwwgcEGCCsGAQUFBwICMIG0DIGxVGhpcyBUcnVzdElEIFNlcnZlciBDZXJ0aWZpY2F0
ZSBoYXMgYmVlbiBpc3N1ZWQgaW4gYWNjb3JkYW5jZSB3aXRoIElkZW5UcnVzdCdzIFRydXN0SUQg
Q2VydGlmaWNhdGUgUG9saWN5IGZvdW5kIGF0IGh0dHBzOi8vc2VjdXJlLmlkZW50cnVzdC5jb20v
Y2VydGlmaWNhdGVzL3BvbGljeS90cy9pbmRleC5odG1sMEoGA1UdHwRDMEEwP6A9oDuGOWh0dHA6
Ly92YWxpZGF0aW9uLmlkZW50cnVzdC5jb20vY3JsL2NvbW1lcmNpYWxyb290Y2ExLmNybDAdBgNV
HQ4EFgQUThcZFloOTNxO9HLs5DoBwRmU/TUwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMC
MA0GCSqGSIb3DQEBCwUAA4ICAQCbLpB+BNTA1WvOU48YNwB9OzfX5UIqLOtw+3FoFuvsMY0b5V4u
8+lDj9n8hQW67qtbcb1eN+ZTD9hqkG39BnfkjnJeAdagfDBl08UtfmhqgZpH0SI/6ug18pVTPU4M
BAWHgyILWXsuSydFrt/Sw6pq+G53ACWWs8zYMvmfZB9y4ABsr1k0YtM2wwist+XlxRQqH+NOqc9s
UPeTfPmITxyvotAXR6sHrjEj12sQIE65/IfFFvsYFlfmHpy9bX5Y3hQ05sscd35ONefXYzNDxe0s
pVpwbKmRc20Nd/TtjtDOyINrxW1e92karKJUf2cO4GowOElP6c5MRSXzmmIDnahoJ/CSXcf5c5lv
/YhdJU7Mb8ZYTBJqeaZYQemukOLyn8bXMlZdx4lSdQotO7wwnmXNxDaLt3AsoynM93yti0pJNuhN
dFwG/ml5gDVcK6Vfq/f2mOcNf3YCkl7vJuLvvR+Qc1/+cpSpmnGxMC8qPp1mcg9cX8n5eTzA6ypw
Rli4S+V+fJsi00SYpQGY72UEFwN/MmSJTL8vQevba2IHC8eRQj37A6623FIQRfV4CnPupuZ8i/Er
ZMgUM423EeuAg68TEgP+w22IijHD9ZLXlm7OLfwZqbN9zHTd8TIoBYNBEzCv94oduRyUa0//S1mf
W1QH9sHDoA0CNud3OBtTdpwLiQ==
-----END CERTIFICATE-----
"""
_IDENTRUST_TRUSTID_SERVER_O1_PEM_SHA256 = (
    "a46bf9e4a3d3bbfb3cb90a1757512045e014ceecd121f6a31a39cf04239c2fca")

SOURCE_RELEASE = "FY24"
SOURCE_RELEASE_DATE = "2025-09-02"
EXPECTED_ZIP_MEMBER_DATE = (2025, 9, 2)
EXPECTED_ZIP_BYTES = 3723148
EXPECTED_ZIP_SHA256 = "c8f15d5de307528ec4f7d2648181e5182c89ecd33d337a0e1a128817ce084e83"
EXPECTED_RELEASE_NOTES_BYTES = 110939
EXPECTED_RELEASE_NOTES_SHA256 = "132712b7a464158d75eed8f90bfdfe6c7880aa4310e01ab3605b97fdfbe3f638"
EXPECTED_GDB_DIR = "FY24_MIRTA_Final.gdb"
EXPECTED_GDB_MEMBERS = frozenset({
    "a00000001.TablesByName.atx", "a00000001.gdbindexes", "a00000001.gdbtable", "a00000001.gdbtablx",
    "a00000002.gdbtable", "a00000002.gdbtablx", "a00000003.gdbindexes", "a00000003.gdbtable",
    "a00000003.gdbtablx", "a00000004.CatItemsByPhysicalName.atx", "a00000004.CatItemsByType.atx",
    "a00000004.FDO_UUID.atx", "a00000004.freelist", "a00000004.gdbindexes", "a00000004.gdbtable",
    "a00000004.gdbtablx", "a00000004.spx", "a00000005.CatItemTypesByName.atx",
    "a00000005.CatItemTypesByParentTypeID.atx", "a00000005.CatItemTypesByUUID.atx",
    "a00000005.gdbindexes", "a00000005.gdbtable", "a00000005.gdbtablx",
    "a00000006.CatRelsByDestinationID.atx", "a00000006.CatRelsByOriginID.atx",
    "a00000006.CatRelsByType.atx", "a00000006.FDO_UUID.atx", "a00000006.gdbindexes",
    "a00000006.gdbtable", "a00000006.gdbtablx", "a00000007.CatRelTypesByBackwardLabel.atx",
    "a00000007.CatRelTypesByDestItemTypeID.atx", "a00000007.CatRelTypesByForwardLabel.atx",
    "a00000007.CatRelTypesByName.atx", "a00000007.CatRelTypesByOriginItemTypeID.atx",
    "a00000007.CatRelTypesByUUID.atx", "a00000007.gdbindexes", "a00000007.gdbtable",
    "a00000007.gdbtablx", "a00000009.I8featureName.atx", "a00000009.I8siteName.atx",
    "a00000009.gdbindexes", "a00000009.gdbtable", "a00000009.gdbtablx", "a00000009.spx",
    "a0000000a.I9featureName.atx", "a0000000a.gdbindexes", "a0000000a.gdbtable",
    "a0000000a.gdbtablx", "a0000000a.spx", "gdb", "timestamps",
})
EXPECTED_GDB_LAYER_ROSTER = ["FY24MirtaLocations", "FY24MirtaLocations_A"]
EXPECTED_LAYER = "FY24MirtaLocations"
EXPECTED_BOUNDARY_LAYER = "FY24MirtaLocations_A"
EXPECTED_COUNT = 831
FIELD_ORDER = ("countryName", "featureDescription", "featureName", "isCui", "isFirrmaSite",
               "isJointBase", "mediaId", "mirtaLocationsIdpk", "sdsId", "siteName",
               "siteOperationalStatus", "siteReportingComponent", "stateNameCode")
EXPECTED_FIELDS = {
    "countryName": "str:25", "featureDescription": "str:2000", "featureName": "str:80",
    "isCui": "str:3", "isFirrmaSite": "str:3", "isJointBase": "str:3", "mediaId": "str:40",
    "mirtaLocationsIdpk": "str:40", "sdsId": "str", "siteName": "str:100",
    "siteOperationalStatus": "str:4", "siteReportingComponent": "str:22", "stateNameCode": "str:5",
}
EXPECTED_BOUNDS = [-167.98785503299996, 13.428655045000028, 174.10714007400009, 71.32619497400003]
EXPECTED_CONSTANT_FIELDS = {"countryName": "usa", "featureDescription": "na", "isCui": "no",
                            "mediaId": "na"}
EXPECTED_IDPK_VALUE = " "
EXPECTED_FIRRMA_COUNTS = {"no": 608, "yes": 223}
EXPECTED_JOINT_BASE_COUNTS = {"no": 816, "yes": 15}
EXPECTED_STATUS_COUNTS = {"Excs": 1, "act": 792, "care": 18, "clsd": 13, "excs": 3, "semi": 4}
EXPECTED_COMPONENT_COUNTS = {
    "afr": 10, "airNationalGuard": 70, "armyNationalGuard": 190, "other": 1, "usa": 131,
    "usaf": 140, "usmc": 27, "usmcr": 6, "usn": 204, "usnr": 5, "usar": 43, "whs": 4,
}
EXPECTED_STATE_SET = {
    "ak", "al", "ar", "az", "ca", "co", "ct", "dc", "de", "fl", "ga", "gu", "hi", "ia", "id",
    "il", "in", "ks", "ky", "la", "ma", "md", "me", "mi", "mn", "mo", "ms", "mt", "nc", "nd",
    "ne", "nh", "nj", "nm", "nv", "ny", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd", "tn",
    "tx", "ut", "va", "vi", "vt", "wa", "wi", "wv", "wy",
}
EXPECTED_SDS_NULL_COUNT = 790
EXPECTED_SDS_GUID_COUNT = 41
RECORD_SHA = "734ff19e5fc28e447bedae993ce34b82c8b5436fb17afad5f7cd91ab632f1b30"
GEOMETRY_SHA = "b35cada34c097504eb723f5742fa12455b25a463e2957e11dfb6901a4d6df7d4"
ID_SHA = "919641a22c1c507a0bf9e644b75eacf95c0e790551f3789a230867bf2f502505"
SCHEMA_SHA = "d863fd1b94db6d1dcadff489dd5e3e9e42e5e3a9bd072ede57d5f5acabfba964"
WGS84 = pyproj.CRS.from_epsg(4326)
_EXPECTED_URLS = {"release_notes": RELEASE_NOTES_URL, "mirta_zip": SOURCE_URL}
MAX_CAPTURE_BYTES = 12_000_000
MAX_TOTAL_CAPTURE_BYTES = 16_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _tls_context() -> ssl.SSLContext:
    if _sha(_IDENTRUST_TRUSTID_SERVER_O1_PEM.encode("utf-8")) != _IDENTRUST_TRUSTID_SERVER_O1_PEM_SHA256:
        raise ValueError("embedded_intermediate_certificate_drift")
    context = ssl.create_default_context(cafile=certifi.where())
    context.load_verify_locations(cadata=_IDENTRUST_TRUSTID_SERVER_O1_PEM)
    return context


def _validate_guid(value: str) -> bool:
    parts = value.strip("{}").split("-")
    return (len(parts) == 5 and all(len(p) == n for p, n in zip(parts, (8, 4, 4, 4, 12)))
            and all(c in "0123456789abcdefABCDEF" for c in "".join(parts)))


def _decode(body: bytes, tmp: Path):
    if len(body) != EXPECTED_ZIP_BYTES or _sha(body) != EXPECTED_ZIP_SHA256:
        raise ValueError("pinned_disdi_release_drift")
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        infos = z.infolist()
        dir_entries = [i for i in infos if i.is_dir() or i.filename.endswith("/")]
        file_entries = [i for i in infos if not (i.is_dir() or i.filename.endswith("/"))]
        names = [i.filename for i in file_entries]
        if len(names) != len(set(names)):
            raise ValueError("source_zip_duplicate_members")
        if len(dir_entries) != 1 or dir_entries[0].filename != EXPECTED_GDB_DIR + "/":
            raise ValueError("source_zip_member_roster_drift")
        for i in file_entries:
            mode = (i.external_attr >> 16) & 0o170000
            parts = Path(i.filename).parts
            if not i.filename or i.filename.startswith(("/", "\\")) or "\\" in i.filename \
                    or ".." in parts or len(parts) != 2 or mode == 0o120000 \
                    or parts[0] != EXPECTED_GDB_DIR or not parts[1]:
                raise ValueError("source_zip_member_path_rejected")
        if set(names) != {f"{EXPECTED_GDB_DIR}/{m}" for m in EXPECTED_GDB_MEMBERS}:
            raise ValueError("source_zip_member_roster_drift")
        if any(i.date_time[:3] != EXPECTED_ZIP_MEMBER_DATE for i in file_entries):
            raise ValueError("source_zip_member_timestamp_drift")
        if z.testzip() is not None:
            raise ValueError("source_zip_crc_invalid")
        gdb = tmp / EXPECTED_GDB_DIR
        gdb.mkdir()
        for name in EXPECTED_GDB_MEMBERS:
            with z.open(f"{EXPECTED_GDB_DIR}/{name}") as src, (gdb / name).open("xb") as dst:
                shutil.copyfileobj(src, dst)
    gdb_path = str(tmp / EXPECTED_GDB_DIR)
    if fiona.listlayers(gdb_path) != EXPECTED_GDB_LAYER_ROSTER:
        raise ValueError("source_gdb_layer_roster_drift")
    attrs, geoms = [], []
    with fiona.open(gdb_path, layer=EXPECTED_LAYER) as src:
        if src.schema["geometry"] != "Point" or list(src.schema["properties"]) != list(FIELD_ORDER) \
                or dict(src.schema["properties"]) != EXPECTED_FIELDS:
            raise ValueError("source_schema_drift")
        if not pyproj.CRS.from_user_input(src.crs).equals(WGS84, ignore_axis_order=True):
            raise ValueError("source_crs_drift")
        if list(map(float, src.bounds)) != EXPECTED_BOUNDS:
            raise ValueError("source_bounds_drift")
        for feature in src:
            properties = dict(feature["properties"])
            geometry = feature["geometry"]
            if geometry is None or geometry["type"] != "Point":
                raise ValueError("source_geometry_drift")
            x, y = geometry["coordinates"]
            if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in (x, y)):
                raise ValueError("source_geometry_drift")
            row = []
            for name in FIELD_ORDER:
                value = properties[name]
                if value is not None and not isinstance(value, str):
                    raise ValueError("source_attribute_type_drift:" + name)
                row.append(value)
            attrs.append(row)
            geoms.append([float(x), float(y)])
    if len(attrs) != EXPECTED_COUNT:
        raise ValueError("source_feature_count_drift")
    names = list(FIELD_ORDER)
    index = {name: position for position, name in enumerate(names)}
    if any(row[index["featureName"]] != row[index["siteName"]] for row in attrs):
        raise ValueError("source_feature_site_name_divergence")
    for name, expected in EXPECTED_CONSTANT_FIELDS.items():
        if any(row[index[name]] != expected for row in attrs):
            raise ValueError("source_constant_field_drift:" + name)
    if any(row[index["mirtaLocationsIdpk"]] != EXPECTED_IDPK_VALUE for row in attrs):
        raise ValueError("source_id_degenerate_drift:mirtaLocationsIdpk")
    for name, counts in (("isFirrmaSite", EXPECTED_FIRRMA_COUNTS),
                         ("isJointBase", EXPECTED_JOINT_BASE_COUNTS),
                         ("siteOperationalStatus", EXPECTED_STATUS_COUNTS),
                         ("siteReportingComponent", EXPECTED_COMPONENT_COUNTS)):
        observed: dict[str, int] = {}
        for row in attrs:
            value = row[index[name]]
            if value is None:
                raise ValueError("source_null_value_drift:" + name)
            observed[value] = observed.get(value, 0) + 1
        if observed != counts:
            raise ValueError("source_value_population_drift:" + name)
    states = {row[index["stateNameCode"]] for row in attrs}
    if None in states or states != EXPECTED_STATE_SET:
        raise ValueError("source_state_roster_drift")
    sds = [row[index["sdsId"]] for row in attrs]
    guid_count = 0
    for value in sds:
        if value is None:
            continue
        if not _validate_guid(value):
            raise ValueError("source_sds_id_format_drift")
        guid_count += 1
    if guid_count != EXPECTED_SDS_GUID_COUNT or len(sds) - guid_count != EXPECTED_SDS_NULL_COUNT:
        raise ValueError("source_sds_population_drift")
    record_sha = _sha(_canonical(attrs))
    geometry_sha = _sha(_canonical(geoms))
    id_sha = _sha(_canonical([row[index["mirtaLocationsIdpk"]] for row in attrs]))
    schema_sha = _sha(_canonical([[name, EXPECTED_FIELDS[name]] for name in FIELD_ORDER]))
    if record_sha != RECORD_SHA or geometry_sha != GEOMETRY_SHA or id_sha != ID_SHA \
            or schema_sha != SCHEMA_SHA:
        raise ValueError("source_content_snapshot_drift")
    null_counts = {name: sum(1 for row in attrs if row[index[name]] is None) for name in FIELD_ORDER}
    audit = {
        "source_feature_count": len(attrs), "source_population_complete": True,
        "source_release": SOURCE_RELEASE, "source_release_date": SOURCE_RELEASE_DATE,
        "source_release_date_evidence": "DISDI release notes PDF dated September 2, 2025 (captured as "
                                        "release_notes input); every FileGDB ZIP member timestamp is "
                                        "2025-09-02, agreeing with the release notes.",
        "source_id_field": "mirtaLocationsIdpk", "source_id_unique": False,
        "source_id_degenerate": True,
        "source_id_degenerate_reason": "The producer's primary key mirtaLocationsIdpk is the single "
                                       "space character for all 831 features and sdsId GUIDs are "
                                       "populated for only 41 of 831 features; no usable native row "
                                       "identifier is published. Values are preserved verbatim and no "
                                       "substitute identifiers are generated.",
        "source_id_sha256": id_sha, "source_record_sha256": record_sha,
        "source_geometry_sha256": geometry_sha, "source_schema_sha256": schema_sha,
        "source_gdb_schema": EXPECTED_FIELDS, "source_crs": "EPSG:4326",
        "source_geometry_type": "Point", "source_bounds": list(EXPECTED_BOUNDS),
        "source_null_counts": null_counts, "source_geometry_null_count": 0,
        "source_geometry_empty_count": 0,
        "source_coverage_scope": "United States and its Territories (50 states plus DC, PR, GU, VI) "
                                 "per the FY 2024 Base Structure Report scope criteria in the DISDI "
                                 "release notes; no sites outside the United States and its "
                                 "territories are published in this public release.",
        "hifld_inventory_id": "mirta-dod-sites-points",
        "drift_guard": "Pinned official ZIP checksum and member roster with 2025-09-02 timestamps and "
                       "CRC integrity, exact GDB layer roster, field names/types/order, feature "
                       "count, degenerate-ID and attribute value populations, SDS GUID counts, "
                       "unique siteName/featureName equality, CRS, native XY and bounds.",
    }
    return attrs, geoms, audit


def _parquet(attrs: list[list], geoms: list[list]) -> bytes:
    arrays, fields = [], []
    for position, name in enumerate(FIELD_ORDER):
        arrays.append(pa.array([row[position] for row in attrs], type=pa.string()))
        fields.append(pa.field(name, pa.string(), nullable=True))
    arrays.append(pa.array([Point(x, y).wkb for x, y in geoms], type=pa.binary()))
    fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"],
                                    "crs": WGS84.to_json_dict(), "edges": "planar"}}}
    sink = io.BytesIO()
    pq.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata={b"geo": _canonical(geo)})),
                   sink, compression="zstd", use_dictionary=False)
    table = pq.read_table(pa.BufferReader(sink.getvalue()))
    if table.num_rows != len(attrs):
        raise ValueError("candidate_row_count_readback_drift")
    for position, name in enumerate(FIELD_ORDER):
        if table[name].type != pa.string():
            raise ValueError("candidate_attribute_type_readback_drift:" + name)
        if table[name].to_pylist() != [row[position] for row in attrs]:
            raise ValueError("candidate_attribute_readback_drift:" + name)
    for position, (x, y) in enumerate(geoms):
        if table["geometry"][position].as_py() != Point(x, y).wkb:
            raise ValueError("candidate_geometry_readback_drift")
    return sink.getvalue()


def _load_capture(name, root):
    if name not in _EXPECTED_URLS:
        raise ValueError("unexpected_source_capture_name")
    paths = [root / f"{name}.body", root / f"{name}.wire", root / f"{name}.receipt.json"]
    if any(p.is_symlink() for p in paths):
        raise ValueError("capture_symlink_rejected")
    body, wire, receipt = paths[0].read_bytes(), paths[1].read_bytes(), json.loads(paths[2].read_bytes())
    if receipt.get("method") != "GET" or receipt.get("url") != _EXPECTED_URLS[name] \
            or receipt.get("final_url") != _EXPECTED_URLS[name] or receipt.get("complete_body") is not True \
            or receipt.get("status") != 200 or receipt.get("bytes") != len(body) \
            or receipt.get("sha256") != _sha(body) or receipt.get("wire_bytes") != len(wire) \
            or receipt.get("wire_sha256") != _sha(wire):
        raise ValueError("capture_receipt_invalid")
    return body, wire, receipt


def _capture(root: Path, storage: StagingStorageResource, version: str):
    mp = root / "capture_manifest.json"
    if mp.exists():
        m = json.loads(mp.read_bytes())
        if m.get("complete") is not True or set(m.get("receipts", {})) != set(_EXPECTED_URLS):
            raise ValueError("capture_manifest_invalid")
        captures = {name: _load_capture(name, root) for name in _EXPECTED_URLS}
        if any(captures[name][2] != m["receipts"][name] for name in _EXPECTED_URLS):
            raise ValueError("capture_manifest_receipt_drift")
        return captures
    if any(root.glob("*.body")) or any(root.glob("*.wire")) or any(root.glob("*.receipt.json")):
        raise ValueError("unsealed_capture_attempt")
    root.mkdir(parents=True, exist_ok=True)
    caps = {}
    try:
        with httpx.Client(verify=_tls_context(), follow_redirects=False,
                          timeout=httpx.Timeout(120, connect=30),
                          headers={"User-Agent": "hifld-next-datasets/mirta-dod-sites-points-draft"}) as client:
            notes_body, notes_receipt = capture_get(client, RELEASE_NOTES_URL, root, "release_notes",
                                                    max_bytes=MAX_CAPTURE_BYTES)
            if len(notes_body) != EXPECTED_RELEASE_NOTES_BYTES \
                    or _sha(notes_body) != EXPECTED_RELEASE_NOTES_SHA256:
                raise ValueError("pinned_release_notes_drift")
            caps["release_notes"] = (notes_body, notes_receipt)
            body, receipt = capture_get(client, SOURCE_URL, root, "mirta_zip", max_bytes=MAX_CAPTURE_BYTES)
            if receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL \
                    or receipt.get("final_url") != SOURCE_URL:
                raise ValueError("source_request_identity_mismatch")
            caps["mirta_zip"] = (body, receipt)
            with tempfile.TemporaryDirectory(dir=root) as tmp:
                _decode(body, Path(tmp))
        mp.write_bytes(_canonical({"complete": True, "captured_at": datetime.now(timezone.utc).isoformat(),
                                   "receipts": {name: caps[name][1] for name in _EXPECTED_URLS}}) + b"\n")
        return {name: (caps[name][0], (root / f"{name}.wire").read_bytes(), caps[name][1])
                for name in _EXPECTED_URLS}
    except Exception as exc:
        rec = {}
        for p in root.glob("*.receipt.json"):
            name = p.name.removesuffix(".receipt.json")
            try:
                rec[name] = json.loads(p.read_bytes())
            except Exception:
                pass
        mp.write_bytes(_canonical({"complete": False, "captured_at": datetime.now(timezone.utc).isoformat(),
                                   "error_code": type(exc).__name__, "receipts": rec}) + b"\n")
        raise


def _candidate_result(version: str, audit: Mapping[str, object], prefix: str, *,
                      reused: bool, capture_directory: str | None = None) -> dict[str, object]:
    """Return the same quality-check contract for first build and cache reuse."""
    result = {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
              "feature_count": audit["source_feature_count"],
              "source_null_geometry_count": audit["source_geometry_null_count"],
              "source_invalid_geometry_count": audit["source_geometry_empty_count"],
              "source_id_sha256": audit["source_id_sha256"],
              "source_record_sha256": audit["source_record_sha256"],
              "source_geometry_sha256": audit["source_geometry_sha256"],
              "source_schema_sha256": audit["source_schema_sha256"],
              "source_release": audit["source_release"],
              "source_release_date": audit["source_release_date"],
              "source_id_degenerate": audit["source_id_degenerate"],
              "source_coverage_scope": audit["source_coverage_scope"],
              "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": reused,
              "source_release_accepted": False, "freshness_accepted": False,
              "schema_contract_accepted": False, "id_contract_accepted": False,
              "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
              "release_accepted": False}
    if capture_directory is not None:
        result["capture_directory"] = capture_directory
    return result


def capture_mirta_dod_sites_points(storage: StagingStorageResource, *, version: str,
                                   capture_directory: str | None = None) -> dict[str, object]:
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not isinstance(version, str) or not version or Path(version).name != version \
            or "/" in version or "\\" in version:
        raise ValueError("unsafe_capture_version")
    stage = Path(storage.local_dir).absolute()
    if ".." in stage.parts or any(p.is_symlink() for p in (stage, *stage.parents)):
        raise ValueError("unsafe_local_staging_root")
    stage.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(stage).free < 256_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    root = requested or Path("/tmp/hifld-native-reconstruction/157/captures") / (version + "-" + uuid4().hex)
    caps = _capture(root, storage, version)
    with tempfile.TemporaryDirectory() as tmp:
        attrs, geoms, audit = _decode(caps["mirta_zip"][0], Path(tmp))
        parquet = _parquet(attrs, geoms)
    identity = processing_identity(Path(__file__).parents[3])
    evidence_sha = _sha(_canonical({name: {"body_sha256": caps[name][2]["sha256"],
                                           "wire_sha256": caps[name][2]["wire_sha256"]}
                                    for name in _EXPECTED_URLS}))
    audit.update({"source_capture_evidence_sha256": evidence_sha,
                  "source_receipts": {name: caps[name][2] for name in _EXPECTED_URLS},
                  "source_release_accepted": False, "source_freshness_accepted": False,
                  "schema_contract_accepted": False, "id_contract_accepted": False,
                  "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
                  "release_accepted": False})
    cv = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                 audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, cv)
    op = f"geoparquet/{FILE_SLUG}.parquet"
    inputs = []
    for name in _EXPECTED_URLS:
        receipt = caps[name][2]
        inputs.append({
            "input_id": name, "producer": PUBLISHER, "host": urlsplit(_EXPECTED_URLS[name]).hostname,
            "role": "producer_publication", "canonical_url": _EXPECTED_URLS[name],
            "retrieval_url": _EXPECTED_URLS[name], "observed_final_url": receipt["final_url"],
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"], "wire_bytes": receipt["wire_bytes"],
            "wire_sha256": receipt["wire_sha256"], "release_date": SOURCE_RELEASE_DATE,
            "license": "unknown_pending_rights_review",
            "attribution": "U.S. Department of Defense, DISDI Program (OASD EI&E) public release of the "
                           "Military Installations, Ranges, and Training Areas dataset; release notes "
                           "dated September 2, 2025, contact DISDI Helpdesk. Formal downstream rights "
                           "review remains pending.",
        })
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Military Installations, Ranges, and Training Areas (MIRTA) DoD Sites - Points "
                 "(DISDI FY24 public release)",
        "description": "Authoritative point locations of commonly known DoD sites, installations, "
                       "ranges, and training areas in the United States and its territories, compiled "
                       "by the DISDI Program from the FY 2024 Base Structure Report and the FIRRMA "
                       "real-estate map. Unapproved draft candidate; the DHS GII continuation of the "
                       "former HIFLD Open FY22 layer is credential-gated and inventory equivalence, "
                       "rights and release acceptance remain unresolved.",
        "publisher": PUBLISHER, "agency": "U.S. Department of Defense",
        "source_url": SOURCE_URL, "license": None, "source_modified": SOURCE_RELEASE_DATE,
        "date_issued": SOURCE_RELEASE_DATE, "date_modified": SOURCE_RELEASE_DATE,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"], "inputs": inputs,
        "transformations": [{"operation": "Native FileGDB attributes and EPSG:4326 point layer to GeoParquet",
                             "parameters": {"source_fields": "all native GDB fields preserved in order, "
                                                            "declared string widths and values",
                                            "source_ids": "mirtaLocationsIdpk and sdsId retained "
                                                          "verbatim including nulls and blank values; "
                                                          "no synthetic identifiers generated",
                                            "geometry": "native WGS84 XY retained without reprojection",
                                            "source_release": SOURCE_RELEASE,
                                            "candidate_status": "unapproved; source, schema/ID contract, "
                                                                "coverage and rights acceptance pending"}}],
        "lineage": "DISDI Program (OASD EI&E) publishes the MIRTA dataset publicly on www.acq.osd.mil "
                   "(landing page /eie/imr/rpid/disdi/index.html; release notes PDF documents the "
                   "September 2, 2025 release, FY 2024 BSR basis, US/PR/GU scope and WGS84 datum). The "
                   "DHS HIFLD Open FY22 points layer (geoplatform item de61396f603346b8a18784eee07fd767, "
                   "services1.arcgis.com FY22_MIRTA_Points_gdb) was migrated to the credential-gated GII "
                   "Portal per the DHS crosswalk; that portal is not anonymously accessible and is not "
                   "used. DataLumos 239602 and other mirrors are identification context only, never "
                   "inputs. The USACE-hosted MIRTA map viewer (geospatial.sec.usace.army.mil experience "
                   "8e2a2247222546f084fcae617d3a335e, FeatureServer item fc0f38c5a19a46dbacd92f2fb823ef8c "
                   "mirroring the same FY24 schema) is the DISDI-delegated display channel, not this "
                   "pipeline's input.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "All source fields, native ID values and point coordinates are retained.",
            "objects": [{"path": op, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No published contract/mapping approved",
                                       "pmtiles": "No display transformation approved"},
        "promotion_allowed": False, "source_accepted": False, "baseline": None,
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    outputs = {op: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n",
               "metadata/candidate_ready.json": _canonical(
                   {"approved": False, "version": cv,
                    "objects": {op: {"bytes": len(parquet), "sha256": _sha(parquet)}},
                    "source_population_complete": True, "source_release_accepted": False,
                    "freshness_accepted": False, "schema_contract_accepted": False,
                    "id_contract_accepted": False, "coverage_accepted": False,
                    "rights_accepted": False, "release_accepted": False}) + b"\n"}
    for name in _EXPECTED_URLS:
        outputs[f"raw/{name}.body"] = caps[name][0]
        outputs[f"raw/{name}.wire"] = caps[name][1]
        outputs[f"raw/{name}.receipt.json"] = _canonical(caps[name][2]) + b"\n"
    state = f"_construction-state/{DATASET_SLUG}/{cv}.json"
    fingerprint = _sha(_canonical({"identity": identity, "archive_sha256": EXPECTED_ZIP_SHA256,
                                   "record_sha256": audit["source_record_sha256"],
                                   "schema_sha256": audit["source_schema_sha256"]}))
    if storage.object_snapshot(state) is not None:
        original = json.loads(load_first_construction(storage, state, cv, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("objects"), dict):
            raise ValueError("cached_candidate_state_invalid")
        verify_cached_candidate(storage, Path(__file__).parents[3], state, prefix, cv, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"],
                                original["objects"], original.get("capture_receipts", {}))
        return _candidate_result(cv, audit, prefix, reused=True, capture_directory=str(root))
    descriptors = {}
    for n, b in outputs.items():
        key = f"{prefix}/{n}"
        write_immutable(storage, key, b)
        snap = storage.object_snapshot(key)
        if snap is None or snap.size != len(b) or storage.sha256_key(key) != _sha(b):
            raise ValueError("candidate_object_readback_failed:" + n)
        descriptors[n] = {"bytes": len(b), "sha256": _sha(b)}
    seal_ready_candidate(storage, state, cv, fingerprint,
                         {"processing_identity": identity,
                          "source_record_sha256": audit["source_record_sha256"],
                          "source_schema_sha256": audit["source_schema_sha256"],
                          "capture_receipts": {name: caps[name][2] for name in _EXPECTED_URLS},
                          "objects": descriptors})
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
    )
    return _candidate_result(cv, audit, prefix, reused=False, capture_directory=str(root))

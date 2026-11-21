"""Draft capture of the DISDI public FY25 V2 MIRTA DoD Sites points release.

Issue #157. The producer, the Defense Installation Spatial Data Infrastructure
(DISDI) Program within OASD(EI&E), publishes the Military Installations,
Ranges, and Training Areas (MIRTA) dataset publicly on its own acq.osd.mil
distribution as a File Geodatabase ZIP. The DHS crosswalk moves the former
HIFLD Open FY22 points layer to the credential-gated GII Portal, so this
producer-side public release is the pipeline input; DataLumos and other
mirrors are research context only, never inputs. The native FileGDB feature
ID column OBJECTID is preserved verbatim as the leading integer column.

The 2026-11-16 release (release notes for the point locations and boundary
datasets, dated November 16, 2026) supersedes the FY24 release: the archive
is ``FY25_MIRTA_FinalV2.gdb``, the points layer is renamed
``FY24MirtaLocations`` -> ``FY25MIRTALocations_P``, the boundary layer
``FY24MirtaLocations_A`` keeps its name but changes Polygon ->
MultiPolygon, the archive gains a top-level ``release_notes.pdf`` copy, and
the points population grew from 831 to 837 features. The points field roster
(13 fields, names, declared widths and order), the EPSG:4326 CRS and the
native OBJECTID column are unchanged from FY24.

The DISDI landing page does not publish a feature count or ID roster for
this file, so completeness is established at capture time instead of by a
code pin: the FileGDB's own driver-reported feature count (the publisher's
index) must agree with the feature-by-feature row walk, the walked OBJECTID
roster must be unique and positive, and
``capture_completeness.validate_capture_witness`` binds the two count
answers to the captured IDs. The count, the ID roster and the observed
value populations are recorded in the candidate audit; a change the
publisher makes upstream (a site added or renamed) changes the witnesses
and is surfaced by the non-blocking approved-baseline drift check, not by a
hard count pin. Structural contracts (archive layout, GDB member roster,
layer roster, schema, CRS, ID uniqueness, per-feature types) remain exact
gates. Publication stays blocked pending the succession, freshness,
schema/ID contract, coverage, rights and release decisions; all runtime
approval flags are false and the draft refresh schedule is stopped.
"""
from __future__ import annotations

import io
import json
import math
import shutil
import ssl
import tempfile
import zipfile
from collections import Counter
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
import pyogrio
import pyproj
from shapely.geometry import Point

from dagster_hifld.capture_completeness import validate_capture_witness
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.capture_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

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
# Re-verified 2026-11-21 against the live leaf (issuer C=US, O=IdenTrust,
# OU=TrustID Server, CN=TrustID Server CA O1).
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

SOURCE_RELEASE = "FY25 V2"
SOURCE_RELEASE_DATE = "2026-11-16"
EXPECTED_GDB_DIR = "FY25_MIRTA_FinalV2.gdb"
# The only top-level loose file the archive may carry next to the GDB
# directory; the copy embedded in the archive must agree byte-for-byte with
# the separately downloaded release notes (checked at capture time).
EXPECTED_TOP_LEVEL_FILE = "release_notes.pdf"
# Structural GDB member roster of the FY25 V2 archive (observed 2026-11-21:
# 58 members including the transient lock files below). Esri leaves
# ``*.sr.lock`` workspace-session artifacts in exported geodatabases; their
# names encode the export workstation's session id and change between exports
# of an unchanged GDB, so they are tolerated and recorded as observations,
# never pinned.
EXPECTED_GDB_MEMBERS = frozenset({
    "a00000001.TablesByName.atx", "a00000001.freelist", "a00000001.gdbindexes",
    "a00000001.gdbtable", "a00000001.gdbtablx", "a00000002.gdbtable",
    "a00000002.gdbtablx", "a00000003.gdbindexes", "a00000003.gdbtable",
    "a00000003.gdbtablx", "a00000004.CatItemsByPhysicalName.atx",
    "a00000004.CatItemsByType.atx", "a00000004.FDO_UUID.atx", "a00000004.freelist",
    "a00000004.gdbindexes", "a00000004.gdbtable", "a00000004.gdbtablx",
    "a00000004.horizon", "a00000005.CatItemTypesByName.atx",
    "a00000005.CatItemTypesByParentTypeID.atx", "a00000005.CatItemTypesByUUID.atx",
    "a00000005.gdbindexes", "a00000005.gdbtable", "a00000005.gdbtablx",
    "a00000006.CatRelsByDestinationID.atx", "a00000006.CatRelsByOriginID.atx",
    "a00000006.CatRelsByType.atx", "a00000006.FDO_UUID.atx", "a00000006.gdbindexes",
    "a00000006.gdbtable", "a00000006.gdbtablx", "a00000007.CatRelTypesByBackwardLabel.atx",
    "a00000007.CatRelTypesByDestItemTypeID.atx", "a00000007.CatRelTypesByForwardLabel.atx",
    "a00000007.CatRelTypesByName.atx", "a00000007.CatRelTypesByOriginItemTypeID.atx",
    "a00000007.CatRelTypesByUUID.atx", "a00000007.gdbindexes", "a00000007.gdbtable",
    "a00000007.gdbtablx", "a00000009.I3featureName.atx", "a00000009.freelist",
    "a00000009.gdbindexes", "a00000009.gdbtable", "a00000009.gdbtablx",
    "a00000009.horizon", "a00000009.spx", "a0000000a.freelist", "a0000000a.gdbindexes",
    "a0000000a.gdbtable", "a0000000a.gdbtablx", "a0000000a.horizon", "a0000000a.spx",
    "gdb", "timestamps",
})
EXPECTED_GDB_LAYER_ROSTER = ["FY25MirtaLocations_A", "FY25MIRTALocations_P"]
EXPECTED_LAYER = "FY25MIRTALocations_P"
EXPECTED_BOUNDARY_LAYER = "FY25MirtaLocations_A"
FIELD_ORDER = ("countryName", "featureDescription", "featureName", "isCui", "isFirrmaSite",
               "isJointBase", "mediaId", "mirtaLocationsIdpk", "sdsId", "siteName",
               "siteOperationalStatus", "siteReportingComponent", "stateNameCode")
# Schema contract of the FY25 V2 points layer (verified 2026-11-21 unchanged
# from FY24: names, declared widths and order).
EXPECTED_FIELDS = {
    "countryName": "str:25", "featureDescription": "str:2000", "featureName": "str:80",
    "isCui": "str:3", "isFirrmaSite": "str:3", "isJointBase": "str:3", "mediaId": "str:40",
    "mirtaLocationsIdpk": "str:40", "sdsId": "str", "siteName": "str:100",
    "siteOperationalStatus": "str:4", "siteReportingComponent": "str:22", "stateNameCode": "str:5",
}
# The native FileGDB feature ID column. GDAL's OpenFileGDB driver maps the
# Esri OBJECTID field to the OGR feature ID (pyogrio.read_info fid_column is
# "OBJECTID" and the field is not part of the fiona property schema); Fiona
# exposes each feature's OBJECTID value as feature["id"]. The native IDs are
# distinct, positive, and noncontiguous in the FY25 V2 release (837 features
# numbered 1..838), so they are the usable native row identifier and are
# preserved verbatim, never renumbered.
EXPECTED_FID_COLUMN = "OBJECTID"
WGS84 = pyproj.CRS.from_epsg(4326)
_EXPECTED_URLS = {"release_notes": RELEASE_NOTES_URL, "mirta_zip": SOURCE_URL}
MAX_CAPTURE_BYTES = 12_000_000


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


def _unzip(body: bytes, tmp: Path) -> dict[str, object]:
    """Validate the archive layout and extract the GDB; return observed layout facts."""
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        infos = z.infolist()
        dir_entries = [i for i in infos if i.is_dir() or i.filename.endswith("/")]
        file_entries = [i for i in infos if not (i.is_dir() or i.filename.endswith("/"))]
        names = [i.filename for i in file_entries]
        if len(names) != len(set(names)):
            raise ValueError("source_zip_duplicate_members")
        if len(dir_entries) != 1 or dir_entries[0].filename != EXPECTED_GDB_DIR + "/":
            raise ValueError("source_zip_layout_drift")
        loose = [n for n in names if "/" not in n]
        if loose != [EXPECTED_TOP_LEVEL_FILE]:
            raise ValueError("source_zip_layout_drift")
        lock_files: list[str] = []
        stable: set[str] = set()
        for i in file_entries:
            mode = (i.external_attr >> 16) & 0o170000
            parts = Path(i.filename).parts
            if not i.filename or i.filename.startswith(("/", "\\")) or "\\" in i.filename \
                    or ".." in parts or mode == 0o120000:
                raise ValueError("source_zip_member_path_rejected")
            if len(parts) == 1:
                continue
            if len(parts) != 2 or parts[0] != EXPECTED_GDB_DIR or not parts[1]:
                raise ValueError("source_zip_member_path_rejected")
            if parts[1].endswith(".sr.lock"):
                lock_files.append(parts[1])
            else:
                stable.add(parts[1])
        if stable != EXPECTED_GDB_MEMBERS:
            raise ValueError("source_zip_member_roster_drift")
        if z.testzip() is not None:
            raise ValueError("source_zip_crc_invalid")
        gdb = tmp / EXPECTED_GDB_DIR
        gdb.mkdir()
        for name in EXPECTED_GDB_MEMBERS:
            with z.open(f"{EXPECTED_GDB_DIR}/{name}") as src, (gdb / name).open("xb") as dst:
                shutil.copyfileobj(src, dst)
    return {
        "source_zip_total_members": len(infos),
        "source_gdb_member_count": len(stable) + len(lock_files),
        "source_gdb_lock_files": sorted(lock_files),
        "source_zip_member_dates": sorted({i.date_time[:3] for i in file_entries}),
    }


def _decode(body: bytes, tmp: Path) -> tuple[list[list], list[list], list[int], dict[str, object]]:
    layout = _unzip(body, tmp)
    gdb_path = str(tmp / EXPECTED_GDB_DIR)
    if fiona.listlayers(gdb_path) != EXPECTED_GDB_LAYER_ROSTER:
        raise ValueError("source_gdb_layer_roster_drift")
    info = pyogrio.read_info(gdb_path, layer=EXPECTED_LAYER)
    if info.get("fid_column") != EXPECTED_FID_COLUMN:
        raise ValueError("source_fid_column_drift")
    driver_count = info.get("features")
    if type(driver_count) is not int or driver_count <= 0:
        raise ValueError("source_feature_count_disagreement")
    attrs, geoms, fids = [], [], []
    with fiona.open(gdb_path, layer=EXPECTED_LAYER) as src:
        if src.schema["geometry"] != "Point" or list(src.schema["properties"]) != list(FIELD_ORDER) \
                or dict(src.schema["properties"]) != EXPECTED_FIELDS:
            raise ValueError("source_schema_drift")
        if not pyproj.CRS.from_user_input(src.crs).equals(WGS84, ignore_axis_order=True):
            raise ValueError("source_crs_drift")
        bounds = list(map(float, src.bounds))
        for feature in src:
            fid_raw = feature["id"]
            if isinstance(fid_raw, bool) or not isinstance(fid_raw, (int, str)):
                raise ValueError("source_fid_type_drift")
            try:
                fid = int(fid_raw)
            except (TypeError, ValueError) as exc:
                raise ValueError("source_fid_type_drift") from exc
            if fid < 1:
                raise ValueError("source_fid_type_drift")
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
            sds = row[FIELD_ORDER.index("sdsId")]
            if sds is not None and not _validate_guid(sds):
                raise ValueError("source_sds_id_format_drift")
            attrs.append(row)
            fids.append(fid)
            geoms.append([float(x), float(y)])
    if len(attrs) != driver_count:
        raise ValueError("source_feature_count_disagreement")
    if len(set(fids)) != len(fids):
        raise ValueError("source_fid_roster_drift")
    # The driver index count and the row walk are the capture-time witnesses:
    # both must agree and the walked roster must be exactly the captured set.
    validate_capture_witness(driver_count, fids, len(attrs), fids, captured_ids=fids)
    return attrs, geoms, fids, _audit(attrs, geoms, fids, bounds, layout, driver_count)


def _audit(attrs: list[list], geoms: list[list], fids: list[int], bounds: list[float],
           layout: Mapping[str, object], driver_count: int) -> dict[str, object]:
    names = list(FIELD_ORDER)
    index = {name: position for position, name in enumerate(names)}
    idpk_values = {row[index["mirtaLocationsIdpk"]] for row in attrs}
    degenerate = len(idpk_values) == 1
    record_sha = _sha(_canonical(attrs))
    geometry_sha = _sha(_canonical(geoms))
    id_sha = _sha(_canonical([row[index["mirtaLocationsIdpk"]] for row in attrs]))
    fid_sha = _sha(_canonical(fids))
    schema_sha = _sha(_canonical([[EXPECTED_FID_COLUMN, "int64"],
                                  *[[name, EXPECTED_FIELDS[name]] for name in FIELD_ORDER]]))
    sds = [row[index["sdsId"]] for row in attrs]
    population = {
        "isFirrmaSite": dict(Counter(row[index["isFirrmaSite"]] for row in attrs)),
        "isJointBase": dict(Counter(row[index["isJointBase"]] for row in attrs)),
        "siteOperationalStatus": dict(Counter(row[index["siteOperationalStatus"]] for row in attrs)),
        "siteReportingComponent": dict(Counter(row[index["siteReportingComponent"]] for row in attrs)),
    }
    return {
        "source_feature_count": len(attrs), "source_reported_count": driver_count,
        "source_population_complete": True, "source_release": SOURCE_RELEASE,
        "source_release_date": SOURCE_RELEASE_DATE,
        "source_release_date_evidence": "DISDI release notes PDF dated November 16, 2026 (captured as "
                                        "the release_notes input and embedded byte-identically in the "
                                        "archive); the FY25 V2 geodatabase directory name and the "
                                        "release notes' stated FY2025 Real Property Inventory basis "
                                        "agree on the edition.",
        "source_id_field": "mirtaLocationsIdpk",
        "source_id_unique": len({row[index["mirtaLocationsIdpk"]] for row in attrs}) == len(attrs),
        "source_id_degenerate": degenerate,
        "source_id_degenerate_reason": "The property-level key mirtaLocationsIdpk is the single space "
                                       "character for all " + str(len(attrs)) + " features, so it is not "
                                       "a usable row identifier. The native FileGDB feature ID column "
                                       "OBJECTID is distinct across all " + str(len(attrs)) + " features "
                                       "(first " + str(fids[0]) + ", last " + str(fids[-1]) + ") and is "
                                       "preserved verbatim as the leading integer OBJECTID column, so a "
                                       "usable native row identifier is published; no substitute "
                                       "identifiers are generated.",
        "source_fid_column": EXPECTED_FID_COLUMN,
        "source_fid_unique": len(set(fids)) == len(fids),
        "source_fid_first": fids[0], "source_fid_last": fids[-1],
        "source_fid_min": min(fids), "source_fid_max": max(fids),
        "source_fid_sha256": fid_sha,
        "source_id_sha256": id_sha, "source_record_sha256": record_sha,
        "source_geometry_sha256": geometry_sha, "source_schema_sha256": schema_sha,
        "source_gdb_schema": EXPECTED_FIELDS, "source_crs": "EPSG:4326",
        "source_geometry_type": "Point", "source_bounds": bounds,
        "source_null_counts": {name: sum(1 for row in attrs if row[index[name]] is None)
                               for name in FIELD_ORDER},
        "source_geometry_null_count": 0, "source_geometry_empty_count": 0,
        "source_sds_null_count": sum(1 for value in sds if value is None),
        "source_sds_guid_count": sum(1 for value in sds if value is not None),
        "source_feature_site_name_divergence_count":
            sum(1 for row in attrs if row[index["featureName"]] != row[index["siteName"]]),
        "source_constant_field_values":
            {name: sorted({row[index[name]] for row in attrs})
             for name in ("countryName", "featureDescription", "isCui", "mediaId")},
        "source_value_populations": population,
        "source_state_set": sorted({row[index["stateNameCode"]] for row in attrs}),
        "source_coverage_scope": "United States, Puerto Rico and Guam per the FY25 release notes "
                                 "scope criteria (DoW sites in the United States, Puerto Rico, or "
                                 "Guam from the FY2025 Real Property Inventory or the FIRRMA real "
                                 "estate map); no sites outside that scope are published in this "
                                 "public release.",
        "source_zip_total_members": layout["source_zip_total_members"],
        "source_gdb_member_count": layout["source_gdb_member_count"],
        "source_gdb_lock_files": layout["source_gdb_lock_files"],
        "source_zip_member_dates": [list(d) for d in layout["source_zip_member_dates"]],
        "hifld_inventory_id": "mirta-dod-sites-points",
        "drift_guard": "Official archive layout (single GDB directory plus the top-level release "
                       "notes copy, which must match the separately downloaded release notes), "
                       "structural GDB member roster with CRC integrity, exact GDB layer roster, "
                       "native OBJECTID feature ID column, field names/types/order, CRS, ID "
                       "uniqueness and per-feature type/geometry contracts; the feature count, ID "
                       "roster and value populations are capture-time witnesses recorded in the "
                       "audit, not code pins, and upstream edits are surfaced by the "
                       "approved-baseline drift check.",
    }


def _parquet(attrs: list[list], geoms: list[list], fids: list[int]) -> bytes:
    arrays = [pa.array(fids, type=pa.int64())]
    fields = [pa.field(EXPECTED_FID_COLUMN, pa.int64(), nullable=False)]
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
    if table[EXPECTED_FID_COLUMN].type != pa.int64():
        raise ValueError("candidate_fid_type_readback_drift")
    if table[EXPECTED_FID_COLUMN].to_pylist() != fids:
        raise ValueError("candidate_fid_readback_drift")
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
            caps["release_notes"] = (notes_body, notes_receipt)
            body, receipt = capture_get(client, SOURCE_URL, root, "mirta_zip", max_bytes=MAX_CAPTURE_BYTES)
            if receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL \
                    or receipt.get("final_url") != SOURCE_URL:
                raise ValueError("source_request_identity_mismatch")
            caps["mirta_zip"] = (body, receipt)
            with tempfile.TemporaryDirectory(dir=root) as tmp:
                _decode(body, Path(tmp))
            with zipfile.ZipFile(io.BytesIO(body)) as z:
                if EXPECTED_TOP_LEVEL_FILE not in z.namelist():
                    raise ValueError("source_zip_layout_drift")
                embedded = z.read(EXPECTED_TOP_LEVEL_FILE)
            if _sha(embedded) != _sha(notes_body):
                raise ValueError("source_release_notes_copy_drift")
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
              "source_reported_count": audit["source_reported_count"],
              "source_population_complete": audit["source_population_complete"],
              "source_null_geometry_count": audit["source_geometry_null_count"],
              "source_invalid_geometry_count": audit["source_geometry_empty_count"],
              "source_id_sha256": audit["source_id_sha256"],
              "source_record_sha256": audit["source_record_sha256"],
              "source_geometry_sha256": audit["source_geometry_sha256"],
              "source_schema_sha256": audit["source_schema_sha256"],
              "source_release": audit["source_release"],
              "source_release_date": audit["source_release_date"],
              "source_fid_column": audit["source_fid_column"],
              "source_fid_unique": audit["source_fid_unique"],
              "source_fid_sha256": audit["source_fid_sha256"],
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
    root = requested or default_capture_dir("hifld-native-reconstruction", "157", "captures") / (version + "-" + uuid4().hex)
    caps = _capture(root, storage, version)
    with tempfile.TemporaryDirectory() as tmp:
        attrs, geoms, fids, audit = _decode(caps["mirta_zip"][0], Path(tmp))
        parquet = _parquet(attrs, geoms, fids)
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
                           "dated November 16, 2026, contact DISDI Helpdesk. Formal downstream rights "
                           "review remains pending.",
        })
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Military Installations, Ranges, and Training Areas (MIRTA) DoD Sites - Points "
                 "(DISDI FY25 V2 public release)",
        "description": "Authoritative point locations of commonly known DoD sites, installations, "
                       "ranges, and training areas in the United States, Puerto Rico and Guam, "
                       "compiled by the DISDI Program from the FY2025 Real Property Inventory and "
                       "the FIRRMA real-estate map. Unapproved draft candidate; the DHS GII "
                       "continuation of the former HIFLD Open FY22 layer is credential-gated and "
                       "inventory equivalence, rights and release acceptance remain unresolved.",
        "publisher": PUBLISHER, "agency": "U.S. Department of Defense",
        "source_url": SOURCE_URL, "license": None, "source_modified": SOURCE_RELEASE_DATE,
        "date_issued": SOURCE_RELEASE_DATE, "date_modified": SOURCE_RELEASE_DATE,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"], "inputs": inputs,
        "transformations": [{"operation": "Native FileGDB attributes and EPSG:4326 point layer to GeoParquet",
                             "parameters": {"source_fields": "all native GDB fields preserved in order, "
                                                             "declared string widths and values",
                                            "source_ids": "native OBJECTID feature IDs retained "
                                                          "verbatim as the leading integer OBJECTID "
                                                          "column; mirtaLocationsIdpk and sdsId "
                                                          "retained verbatim including nulls and "
                                                          "blank values; no synthetic identifiers "
                                                          "generated",
                                            "geometry": "native WGS84 XY retained without reprojection",
                                            "source_release": SOURCE_RELEASE,
                                            "candidate_status": "unapproved; source, schema/ID contract, "
                                                                "coverage and rights acceptance pending"}}],
        "lineage": "DISDI Program (OASD EI&E) publishes the MIRTA dataset publicly on www.acq.osd.mil "
                   "(landing page /eie/imr/rpid/disdi/index.html; release notes PDF dated November 16, "
                   "2026 documents the FY25 V2 release, FY2025 Real Property Inventory basis, "
                   "US/PR/GU scope and WGS84 datum; the archive embeds a byte-identical copy of the "
                   "release notes). The FY25 V2 archive supersedes the FY24 release (gdb directory "
                   "FY24_MIRTA_Final.gdb -> FY25_MIRTA_FinalV2.gdb; points layer FY24MirtaLocations -> "
                   "FY25MIRTALocations_P; boundary layer FY24MirtaLocations_A keeps its name and gains "
                   "SHAPE_Length/SHAPE_Area with Polygon -> MultiPolygon geometry). The DHS HIFLD Open "
                   "FY22 points layer (geoplatform item de61396f603346b8a18784eee07fd767, "
                   "services1.arcgis.com FY22_MIRTA_Points_gdb) was migrated to the credential-gated GII "
                   "Portal per the DHS crosswalk; that portal is not anonymously accessible and is not "
                   "used. DataLumos 239602 and other mirrors are identification context only, never "
                   "inputs.",
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
    fingerprint = _sha(_canonical({"identity": identity,
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

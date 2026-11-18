"""BSEE Gulf and BOEM Pacific platform master/structure/location tables.

The inventory entry describes ORNL's Oil and Natural Gas Platforms layer for
Gulf of Mexico federal waters and the U.S. Pacific coast. The original ORNL
product is only on DHS's restricted GII portal, so this draft rebuilds the
observed product from the producing agencies' current platform tables.

The candidate keeps every published master, structure and location field as
served, one row per structure location with a native Point. It is unapproved:
source release, schema/ID, coverage, datum and release acceptance remain open.
"""
from __future__ import annotations

import csv
import html
import io
import json
import re
import shutil
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit

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
from dagster_hifld.sources.capture_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = FILE_SLUG = "oil-and-natural-gas-platforms"
BSEE = "Bureau of Safety and Environmental Enforcement"
BOEM = "Bureau of Ocean Energy Management"
BSEE_PUBLISHER = "Bureau of Safety and Environmental Enforcement, Office of Technical Data Management, Data Administration Unit"
BOEM_PUBLISHER = "Bureau of Ocean Energy Management, Office of Strategic Resources, Geospatial Services Division"
GULF_LANDING = "https://www.data.bsee.gov/Main/Platform.aspx"
PACIFIC_LANDING = "https://www.data.boem.gov/Main/PacificPlatform.aspx"
GULF_URLS = {
    "gulf_masters": "https://www.data.bsee.gov/Platform/Files/platmastfixed.zip",
    "gulf_structures": "https://www.data.bsee.gov/Platform/Files/platstrufixed.zip",
    "gulf_locations": "https://www.data.bsee.gov/Platform/Files/platlocfixed.zip",
}
PACIFIC_URLS = {
    "pacific_masters": "https://www.data.boem.gov/Platform/Files/platmastpacdelimit.zip",
    "pacific_structures": "https://www.data.boem.gov/Platform/Files/platstrupacdelimit.zip",
    "pacific_locations": "https://www.data.boem.gov/Platform/Files/platlocpacdelimit.zip",
}
LAYOUT_URLS = {
    "gulf_masters_layout": "https://www.data.bsee.gov/Main/HtmlPage.aspx?page=platformMasters",
    "gulf_structures_layout": "https://www.data.bsee.gov/Main/HtmlPage.aspx?page=platformStructures",
    "gulf_locations_layout": "https://www.data.bsee.gov/Main/HtmlPage.aspx?page=platformLocations",
    "pacific_masters_layout": "https://www.data.boem.gov/Main/HtmlPage.aspx?page=pacplatformMasters",
    "pacific_structures_layout": "https://www.data.boem.gov/Main/HtmlPage.aspx?page=pacplatformStructures",
    "pacific_locations_layout": "https://www.data.boem.gov/Main/HtmlPage.aspx?page=pacplatformLocations",
}
CAPTURE_URLS = {
    "gulf_landing": GULF_LANDING,
    "pacific_landing": PACIFIC_LANDING,
    **LAYOUT_URLS,
    **GULF_URLS,
    **PACIFIC_URLS,
}
CAPTURE_LIMITS = {"gulf_landing": 2_000_000, "pacific_landing": 2_000_000,
                  **{name: 2_000_000 for name in LAYOUT_URLS},
                  **{name: 8_000_000 for name in (*GULF_URLS, *PACIFIC_URLS)}}
MAX_BODY_BYTES = 8_000_000
CRS_NOTE = ("Published Point WKB uses a proposed EPSG:4267 (NAD27) mapping. The Gulf GIS "
            "metadata states NAD27; the ASCII location tables do not publish a datum, and the "
            "Pacific datum is unconfirmed. Coordinates are retained as served, without repair "
            "or reprojection. CRS acceptance remains false.")
SCOPE = ("Current platform structures in Gulf of Mexico federal OCS waters and the U.S. "
         "Pacific OCS. Source scope is the observed BSEE Gulf and BOEM Pacific platform tables; "
         "state waters, Alaska and the Atlantic are not included. Not a historical ORNL/HIFLD "
         "release and no ORNL ID, schema, value or geometry parity is claimed.")
GATES = {name: False for name in (
    "source_release_accepted", "freshness_accepted", "schema_contract_accepted",
    "id_contract_accepted", "rights_accepted", "crs_accepted", "coverage_accepted",
    "mapping_approved", "release_accepted", "quality_check_passed")}

# (start, length, canonical field, Gulf alias, Pacific machine-name prefix)
GULF_MASTERS = (
    (1, 8, "COMPLEX_ID_NUM", "Complex Id Num", None),
    (9, 1, "ABANDON_FLAG", "Abandon Flag", None),
    (10, 1, "ALOC_MTR_FLAG", "Aloc Mtr Flag", None),
    (11, 1, "ATTENDED_8_HR_FLAG", "Attended 8 Hr Flag", None),
    (12, 1, "CONDN_PROD_FLAG", "Condn Prod Flag", None),
    (13, 4, "DISTANCE_TO_SHORE", "Distance To Shore", None),
    (17, 1, "DRILLING_FLAG", "Drilling Flag", None),
    (18, 1, "FIRED_VESSEL_FLAG", "Fired Vessel Fl", None),
    (19, 1, "GAS_PROD_FLAG", "Gas Prod Flag", None),
    (20, 1, "GAS_FLARING_FLAG", "Gas Flaring Flag", None),
    (21, 5, "MMS_COMPANY_NUM", "Mms Company Num", None),
    (26, 1, "MANNED_24_HR_FLAG", "Manned 24 Hr Fl", None),
    (27, 1, "MAJ_CMPLX_FLAG", "Maj Cmplx Flag", None),
    (28, 7, "LEASE_NUMBER", "Lease Number", None),
    (35, 11, "LAST_REVISION_DATE", "Last Rev Date", None),
    (46, 1, "LACT_MTR_FLAG", "Lact Mtr Flag", None),
    (47, 1, "INJECTION_CODE", "Injection Code", None),
    (48, 1, "HELIPORT_FLAG", "Heliport Flag", None),
    (49, 1, "WORKOVER_FLAG", "Workover Flag", None),
    (50, 1, "WATER_PROD_FLAG", "Water Prod Flag", None),
    (51, 5, "WATER_DEPTH", "Water Depth", None),
    (56, 1, "TANK_GAUGE_FLAG", "Tank Gauge Flag", None),
    (57, 1, "SULFUR_PROD_FLAG", "Sulfur Prod Flag", None),
    (58, 2, "SUBDISTRICT_CODE", "Subdistrict Code", None),
    (60, 1, "STORE_TANK_FLAG", "Store Tank Flag", None),
    (61, 2, "RIG_COUNT", "Rig Count", None),
    (63, 1, "QUARTER_TYPE", "Qtr Type", None),
    (64, 1, "PROD_EQMT_FLAG", "Prod Eqmt Flag", None),
    (65, 1, "PRODUCTION_FLAG", "Production Flag", None),
    (66, 1, "POWER_SOURCE_TYPE", "Power Source Type", None),
    (67, 1, "POWER_GEN_FLAG", "Power Gen Flag", None),
    (68, 1, "OIL_PROD_FLAG", "Oil Prod Flag", None),
    (69, 1, "GAS_SALE_MTR_FLAG", "Gas Sale Mtr Flag", None),
    (70, 8, "FIELD_NAME_CODE", "Field Name Code", None),
    (78, 3, "DISTRICT_CODE", "District Code", None),
    (81, 2, "CRANE_COUNT", "Crane Count", None),
    (83, 1, "COMPRESSOR_FLAG", "Compressor Flag", None),
    (84, 1, "COMGL_PROD_FLAG", "Comgl Prod Flag", None),
    (85, 3, "BED_COUNT", "Bed Count", None),
    (88, 2, "AREA_CODE", "Area Code", None),
    (90, 6, "BLOCK_NUMBER", "Block Number", None),
    (96, 1, "METER_PROVER_FLAG", "Meter Prover Fl", None),
)
GULF_STRUCTURES = (
    (1, 2, "AREA_CODE", "Area Code", None),
    (3, 6, "BLOCK_NUMBER", "Block Number", None),
    (9, 8, "COMPLEX_ID_NUM", "Complex Id Num", None),
    (17, 2, "DECK_COUNT", "Deck Count", None),
    (19, 1, "EAST_WEST_DEPARTURE_CODE", "East West Departure Code", None),
    (20, 11, "INSTALL_DATE", "Install Date", None),
    (31, 11, "LAST_REVISION_DATE", "Last Revision Date", None),
    (42, 1, "MAJOR_STRUCTURE_FLAG", "Major Structure Flag", None),
    (43, 1, "NORTH_SOUTH_DEPARTURE_CODE", "North South Departure Code", None),
    (44, 11, "REMOVAL_DATE", "Removal Date", None),
    (55, 3, "SLANT_SLOT_COUNT", "Slant Slot Count", None),
    (58, 3, "SLOT_COUNT", "Slot Count", None),
    (61, 3, "SLOT_DRILL_COUNT", "Slot Drill Count", None),
    (64, 3, "SATELLITE_COMPLETION_COUNT", "Satellite Completion Count", None),
    (67, 15, "STRUCTURE_NAME", "Structure Name", None),
    (82, 3, "STRUCTURE_NUMBER", "Structure Number", None),
    (85, 5, "STRUCTURE_TYPE_CODE", "Structure Type Code", None),
    (90, 6, "SURFACE_EAST_WEST_DISTANCE", "Surface East West Distance", None),
    (96, 6, "SURFACE_NORTH_SOUTH_DISTANCE", "Surface North South Distance", None),
    (102, 3, "UNDERWATER_COMPLETION_COUNT", "Underwater Completion Count", None),
    (105, 16, "AUTHORITY_TYPE", "Authority Type", None),
    (121, 8, "AUTHORITY_NUMBER", "Authority Number", None),
    (129, 20, "AUTHORITY_STATUS", "Authority Status", None),
)
GULF_LOCATIONS = (
    (1, 3, "DISTRICT_CODE", "District Code", None),
    (4, 8, "COMPLEX_ID_NUM", "Complex Id Number", None),
    (12, 3, "STRUCTURE_NUMBER", "Structure Number", None),
    (15, 2, "AREA_CODE", "Area Code", None),
    (17, 6, "BLOCK_NUMBER", "Block Number", None),
    (23, 15, "STRUCTURE_NAME", "Structure Name", None),
    (38, 6, "SURFACE_NORTH_SOUTH_DISTANCE", "Surface North South Distance", None),
    (44, 1, "NORTH_SOUTH_DEPARTURE_CODE", "North South Departure Code", None),
    (45, 6, "SURFACE_EAST_WEST_DISTANCE", "Surface East West Distance", None),
    (51, 1, "EAST_WEST_DEPARTURE_CODE", "East West Departure Code", None),
    (52, 17, "X_LOCATION", "X Location", None),
    (69, 17, "Y_LOCATION", "Y Location", None),
    (86, 14, "LONGITUDE", "Longitude", None),
    (100, 13, "LATITUDE", "Latitude", None),
)
PACIFIC_MASTERS = (
    (1, 9, "COMPLEX_ID_NUM", None, "COMPLEX_ID_NUM"),
    (10, 1, "ABANDON_FLAG", None, "ABANDON_FLAG"),
    (11, 1, "ALOC_MTR_FLAG", None, "ALOC_MTR_FLAG"),
    (12, 1, "ATTENDED_8_HR_FLAG", None, "ATTENDED_8_HR_FL"),
    (13, 1, "CONDN_PROD_FLAG", None, "CONDN_PROD_FLAG"),
    (14, 5, "DISTANCE_TO_SHORE", None, "DISTANCE_TO_SH"),
    (19, 1, "DRILLING_FLAG", None, "DRILLING_FLAG"),
    (20, 1, "FIRED_VESSEL_FLAG", None, "FIRED_VESSEL_FL"),
    (21, 1, "GAS_PROD_FLAG", None, "GAS_PROD_FLAG"),
    (22, 1, "GAS_FLARING_FLAG", None, "GAS_FLARING_FLAG"),
    (23, 5, "MMS_COMPANY_NUM", None, "MMS_COMPANY_NUM"),
    (28, 1, "MANNED_24_HR_FLAG", None, "MANNED_24_HR_FL"),
    (29, 1, "MAJ_CMPLX_FLAG", None, "MAJ_CMPLX_FLAG"),
    (30, 7, "LEASE_NUMBER", None, "LEASE_NUMBER"),
    (37, 8, "LAST_REVISION_DATE", None, "LAST_REV_DATE"),
    (45, 1, "LACT_MTR_FLAG", None, "LACT_MTR_FLAG"),
    (46, 1, "INJECTION_CODE", None, "INJECTION_CODE"),
    (47, 1, "HELIPORT_FLAG", None, "HELIPORT_FLAG"),
    (48, 1, "WORKOVER_FLAG", None, "WORKOVER_FLAG"),
    (49, 1, "WATER_PROD_FLAG", None, "WATER_PROD_FLAG"),
    (50, 6, "WATER_DEPTH", None, "WATER_DEPTH"),
    (56, 1, "TANK_GAUGE_FLAG", None, "TANK_GAUGE_FLAG"),
    (57, 1, "SULFUR_PROD_FLAG", None, "SUL_PROD_FLAG"),
    (58, 3, "SUBDISTRICT_CODE", None, "SUBDISTRICT_CODE"),
    (61, 1, "STORE_TANK_FLAG", None, "STORE_TANK_FLAG"),
    (62, 2, "RIG_COUNT", None, "RIG_COUNT"),
    (64, 1, "QUARTER_TYPE", None, "QTR_TYPE_CODE"),
    (65, 1, "PROD_EQMT_FLAG", None, "PROD_EQMT_FLAG"),
    (66, 1, "PRODUCTION_FLAG", None, "PRODUCTION_FLAG"),
    (67, 1, "POWER_SOURCE_TYPE", None, "POWER_SOURCE_TYP"),
    (68, 1, "POWER_GEN_FLAG", None, "POWER_GEN_FLAG"),
    (69, 1, "OIL_PROD_FLAG", None, "OIL_PROD_FLAG"),
    (70, 1, "GAS_SALE_MTR_FLAG", None, "GAS_SALE_MTR_FL"),
    (71, 8, "FIELD_NAME_CODE", None, "FIELD_NAME_CODE"),
    (79, 4, "DISTRICT_CODE", None, "DISTRICT_CODE"),
    (83, 2, "CRANE_COUNT", None, "CRANE_COUNT"),
    (85, 1, "COMPRESSOR_FLAG", None, "COMPRESSOR_FLAG"),
    (86, 1, "COMGL_PROD_FLAG", None, "COMGL_PROD_FLAG"),
    (87, 5, "BED_COUNT", None, "BED_COUNT"),
    (92, 2, "AREA_CODE", None, "AREA_CODE"),
    (94, 6, "BLOCK_NUMBER", None, "BLOCK_NUMBER"),
    (100, 1, "METER_PROVER_FLAG", None, "METER_PROVER_FL"),
)
PACIFIC_STRUCTURES = (
    (1, 2, "AREA_CODE", None, "AREA_CODE"),
    (3, 6, "BLOCK_NUMBER", None, "BLOCK_NUMBER"),
    (9, 2000, "COMMENTS", None, "COMMENTS"),
    (2009, 9, "COMPLEX_ID_NUM", None, "COMPLEX_ID_NUM"),
    (2018, 2, "DECK_COUNT", None, "DECK_COUNT"),
    (2020, 1, "EAST_WEST_DEPARTURE_CODE", None, "SURF_E_W_CODE"),
    (2021, 8, "INSTALL_DATE", None, "INSTALL_DATE"),
    (2029, 8, "LAST_REVISION_DATE", None, "LAST_REV_DATE"),
    (2037, 1, "MAJOR_STRUCTURE_FLAG", None, "MAJ_STRUC_FLAG"),
    (2038, 1, "NORTH_SOUTH_DEPARTURE_CODE", None, "SURF_N_S_CODE"),
    (2039, 8, "REMOVAL_DATE", None, "REMOVAL_DATE"),
    (2047, 3, "SLANT_SLOT_COUNT", None, "SLANT_SLOT_CNT"),
    (2050, 4, "SLOT_COUNT", None, "SLOT_COUNT"),
    (2054, 4, "SLOT_DRILL_COUNT", None, "SLOT_DRILL_COUNT"),
    (2058, 15, "STRUCTURE_NAME", None, "STRUCTURE_NAME"),
    (2073, 3, "STRUCTURE_NUMBER", None, "STRUCTURE_NUMBER"),
    (2076, 6, "SURFACE_EAST_WEST_DISTANCE", None, "SURF_E_W_DIST"),
    (2082, 6, "SURFACE_NORTH_SOUTH_DISTANCE", None, "SURF_N_S_DIST"),
    (2088, 3, "UNDERWATER_COMPLETION_COUNT", None, "UNDWTR_COMP_CNT"),
)
PACIFIC_LOCATIONS = (
    (1, 4, "DISTRICT_CODE", None, "DISTRICT_CODE"),
    (5, 9, "COMPLEX_ID_NUM", None, "COMPLEX_ID_NUM"),
    (14, 3, "STRUCTURE_NUMBER", None, "STRUCTURE_NUMBER"),
    (17, 2, "AREA_CODE", None, "AREA_CODE"),
    (19, 6, "BLOCK_NUMBER", None, "BLOCK_NUMBER"),
    (25, 15, "STRUCTURE_NAME", None, "STRUCTURE_NAME"),
    (40, 6, "SURFACE_NORTH_SOUTH_DISTANCE", None, "SURF_N_S_DIST"),
    (46, 1, "NORTH_SOUTH_DEPARTURE_CODE", None, "SURF_N_S_CODE"),
    (47, 6, "SURFACE_EAST_WEST_DISTANCE", None, "SURF_E_W_DIST"),
    (53, 1, "EAST_WEST_DEPARTURE_CODE", None, "SURF_E_W_CODE"),
    (54, 19, "X_LOCATION", None, "PTFRM_X_LOCATION"),
    (73, 19, "Y_LOCATION", None, "PTFRM_Y_LOCATION"),
    (92, 16, "LONGITUDE", None, "LON"),
    (108, 15, "LATITUDE", None, "LAT"),
)
LAYOUTS = {
    "gulf_masters": GULF_MASTERS,
    "gulf_structures": GULF_STRUCTURES,
    "gulf_locations": GULF_LOCATIONS,
    "pacific_masters": PACIFIC_MASTERS,
    "pacific_structures": PACIFIC_STRUCTURES,
    "pacific_locations": PACIFIC_LOCATIONS,
}
TABLE_MEMBERS = {
    "gulf_masters": "platmast.DAT",
    "gulf_structures": "platstru.DAT",
    "gulf_locations": "platloc.DAT",
    "pacific_masters": "platmastpacdelimit.txt",
    "pacific_structures": "platstrupacdelimit.txt",
    "pacific_locations": "platlocpacdelimit.txt",
}
MASTER_FIELDS = tuple(name for _, _, name, _, _ in GULF_MASTERS if name != "COMPLEX_ID_NUM")
STRUCTURE_FIELDS = (
    "AREA_CODE", "BLOCK_NUMBER", "DECK_COUNT", "EAST_WEST_DEPARTURE_CODE",
    "LAST_REVISION_DATE", "MAJOR_STRUCTURE_FLAG", "NORTH_SOUTH_DEPARTURE_CODE",
    "SLANT_SLOT_COUNT", "SLOT_COUNT", "SLOT_DRILL_COUNT",
    "SATELLITE_COMPLETION_COUNT", "STRUCTURE_TYPE_CODE",
    "SURFACE_EAST_WEST_DISTANCE", "SURFACE_NORTH_SOUTH_DISTANCE",
    "UNDERWATER_COMPLETION_COUNT", "AUTHORITY_TYPE", "AUTHORITY_NUMBER",
    "AUTHORITY_STATUS", "COMMENTS",
)
LOCATION_FIELDS = (
    "DISTRICT_CODE", "AREA_CODE", "BLOCK_NUMBER", "SURFACE_NORTH_SOUTH_DISTANCE",
    "NORTH_SOUTH_DEPARTURE_CODE", "SURFACE_EAST_WEST_DISTANCE",
    "EAST_WEST_DEPARTURE_CODE", "X_LOCATION", "Y_LOCATION", "LONGITUDE", "LATITUDE",
)
OUTPUT_FIELDS = (
    "REGION", "COMPLEX_ID", "STRUCTURE_NUMBER", "STRUCTURE_NAME", "INSTALL_DATE",
    "REMOVAL_DATE",
    *(f"MASTER_{name}" for name in MASTER_FIELDS),
    *(f"STRUCTURE_{name}" for name in STRUCTURE_FIELDS),
    *(f"LOCATION_{name}" for name in LOCATION_FIELDS),
)


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _table_rows(body: bytes, layout: tuple, *, fixed: bool) -> list[dict[str, str | None]]:
    if fixed:
        max_end = max(start - 1 + length for start, length, *_ in layout)
        text = body.decode("latin1")
        rows = []
        for line in text.splitlines():
            if not line.strip():
                continue
            if len(line) > max_end:
                raise ValueError("source_fixed_width_row_overlong")
            padded = line.ljust(max_end)
            rows.append({name: _clean(padded[start - 1:start - 1 + length])
                         for start, length, name, _, _ in layout})
    else:
        parsed = list(csv.reader(io.StringIO(body.decode("latin1"), newline="")))
        if any(len(row) != len(layout) for row in parsed):
            raise ValueError("source_delimited_field_count_drift")
        rows = [{name: _clean(row[index]) for index, (_, _, name, _, _) in enumerate(layout)}
                for row in parsed]
    if not rows:
        raise ValueError("source_table_empty")
    return rows


def _parse_layout(body: bytes, expected: tuple) -> list[dict[str, object]]:
    text = body.decode("utf-8", "replace")
    parsed = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", text, re.S | re.I):
        cells = [html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", cell))).strip()
                 for cell in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S | re.I)]
        if len(cells) >= 4 and cells[0].isdigit() and cells[1].isdigit():
            parsed.append((int(cells[0]), int(cells[1]), cells[2], cells[3]))
    if len(parsed) != len(expected):
        raise ValueError("source_layout_field_count_drift")
    result = []
    for (start, length, alias, definition), (exp_start, exp_length, name, exp_alias, machine) in zip(parsed, expected):
        if (start, length) != (exp_start, exp_length):
            raise ValueError("source_layout_position_drift:" + name)
        if exp_alias is not None and alias.lower() != exp_alias.lower():
            raise ValueError("source_layout_alias_drift:" + name)
        if machine is not None and not definition.upper().startswith(machine):
            raise ValueError("source_layout_machine_name_drift:" + name)
        result.append({"start": start, "length": length, "name": name})
    return result


def _landing_timestamps(body: bytes, labels: tuple[str, ...]) -> dict[str, str]:
    text = html.unescape(re.sub(r"<[^>]+>", " ", body.decode("utf-8", "replace")))
    text = re.sub(r"\s+", " ", text)
    found = {}
    for label in labels:
        match = re.search(rf"{re.escape(label)}\s+(\d{{1,2}}/\d{{1,2}}/\d{{4}}\s+"
                          rf"\d{{1,2}}:\d{{2}}:\d{{2}}\s+[AP]M)", text)
        if match is None:
            raise ValueError("source_page_update_time_missing:" + label)
        found[label] = match.group(1)
    return found


def _validate_landing(name: str, body: bytes) -> dict[str, str]:
    text = body.decode("utf-8", "replace")
    if name == "gulf_landing":
        if ("Gulf" not in text or not all(url.rsplit("/", 1)[-1] in text for url in GULF_URLS.values())):
            raise ValueError("source_landing_links_or_scope_drift")
        return _landing_timestamps(body, ("Platform Masters", "Platform Structures", "Platform Locations"))
    if ("Pacific" not in text or not all(url.rsplit("/", 1)[-1] in text for url in PACIFIC_URLS.values())):
        raise ValueError("source_landing_links_or_scope_drift")
    return _landing_timestamps(body, ("Platform Masters", "Platform Structures", "Platform Locations"))


def _zip_member(body: bytes, name: str) -> bytes:
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            members = archive.namelist()
            if members != [TABLE_MEMBERS[name]]:
                raise ValueError("source_archive_member_roster_drift:" + name)
            return archive.read(TABLE_MEMBERS[name])
    except zipfile.BadZipFile as exc:
        raise ValueError("source_archive_not_a_zip:" + name) from exc


def _date_range(rows: list[dict[str, str | None]], field: str) -> dict[str, str | None]:
    values = [row.get(field) for row in rows if row.get(field)]
    parsed = []
    for value in values:
        for fmt in ("%d-%b-%Y", "%Y%m%d"):
            try:
                parsed.append(datetime.strptime(value, fmt).date().isoformat())
                break
            except ValueError:
                continue
    return {"count": len(values), "min": min(parsed) if parsed else None,
            "max": max(parsed) if parsed else None}


def _join_region(region: str, masters: list[dict[str, str | None]],
                 structures: list[dict[str, str | None]],
                 locations: list[dict[str, str | None]]) -> tuple[list[dict[str, object]], dict[str, object]]:
    def key(row: dict[str, str | None]) -> tuple[str, str]:
        complex_id, structure = row.get("COMPLEX_ID_NUM"), row.get("STRUCTURE_NUMBER")
        if not complex_id or not structure:
            raise ValueError("source_native_identifier_missing:" + region)
        return complex_id, structure

    master_by_complex = {row["COMPLEX_ID_NUM"]: row for row in masters}
    if len(master_by_complex) != len(masters):
        raise ValueError("source_duplicate_master_complex_id:" + region)
    structure_by_key = {key(row): row for row in structures}
    if len(structure_by_key) != len(structures):
        raise ValueError("source_duplicate_structure_key:" + region)
    location_by_key = {key(row): row for row in locations}
    if len(location_by_key) != len(locations):
        raise ValueError("source_duplicate_location_key:" + region)
    structure_only = sorted(set(structure_by_key) - set(location_by_key))
    location_only = sorted(set(location_by_key) - set(structure_by_key))
    master_only = sorted(set(master_by_complex) - {complex_id for complex_id, _ in structure_by_key})
    missing_master = [item for item in location_by_key if item[0] not in master_by_complex]
    if missing_master:
        raise ValueError("source_location_master_orphan:" + region)

    rows = []
    for location_key, location in sorted(location_by_key.items()):
        complex_id, structure_number = location_key
        master = master_by_complex[complex_id]
        structure = structure_by_key.get(location_key)
        row: dict[str, object] = {
            "REGION": region,
            "COMPLEX_ID": complex_id,
            "STRUCTURE_NUMBER": structure_number,
            "STRUCTURE_NAME": _clean((structure or {}).get("STRUCTURE_NAME")) or _clean(location.get("STRUCTURE_NAME")),
            "INSTALL_DATE": _clean((structure or {}).get("INSTALL_DATE")),
            "REMOVAL_DATE": _clean((structure or {}).get("REMOVAL_DATE")),
        }
        for name in MASTER_FIELDS:
            row[f"MASTER_{name}"] = _clean(master.get(name))
        for name in STRUCTURE_FIELDS:
            row[f"STRUCTURE_{name}"] = _clean((structure or {}).get(name))
        for name in LOCATION_FIELDS:
            row[f"LOCATION_{name}"] = _clean(location.get(name))
        longitude, latitude = location.get("LONGITUDE"), location.get("LATITUDE")
        if longitude is None or latitude is None:
            row["geometry"] = None
        else:
            try:
                x, y = float(longitude), float(latitude)
            except ValueError as exc:
                raise ValueError("source_coordinate_not_numeric:" + region) from exc
            if (x != x or y != y or x in (float("inf"), float("-inf"))
                    or y in (float("inf"), float("-inf"))
                    or not -180 <= x <= 180 or not -90 <= y <= 90):
                raise ValueError("source_coordinate_out_of_range:" + region)
            row["geometry"] = Point(x, y).wkb
        rows.append(row)

    counts = {
        "masters": len(masters), "structures": len(structures), "locations": len(locations),
        "structure_only_pairs": len(structure_only),
        "location_only_pairs": len(location_only),
        "masters_without_structures": len(master_only),
        "null_coordinates": sum(1 for row in rows if row["geometry"] is None),
        "zero_coordinates": sum(1 for row in rows
                                if row["geometry"] is not None and row["LOCATION_LONGITUDE"] in ("0", "0.0")
                                and row["LOCATION_LATITUDE"] in ("0", "0.0")),
        "structure_revision_dates": _date_range(structures, "LAST_REVISION_DATE"),
        "master_revision_dates": _date_range(masters, "LAST_REVISION_DATE"),
        "structure_only_examples": [[a, b] for a, b in structure_only[:20]],
        "location_only_examples": [[a, b] for a, b in location_only[:20]],
    }
    return rows, counts


def _validate_capture(captures: Mapping[str, tuple[bytes, Mapping[str, object]]]) -> tuple[list[dict[str, object]], dict[str, object]]:
    if set(captures) != set(CAPTURE_URLS):
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    page_times = {
        "gulf": _validate_landing("gulf_landing", captures["gulf_landing"][0]),
        "pacific": _validate_landing("pacific_landing", captures["pacific_landing"][0]),
    }
    for name, layout in LAYOUTS.items():
        _parse_layout(captures[f"{name}_layout"][0], layout)
    tables = {}
    for name, layout in LAYOUTS.items():
        body = _zip_member(captures[name][0], name)
        tables[name] = _table_rows(body, layout, fixed=name.startswith("gulf_"))
    gulf_rows, gulf_counts = _join_region(
        "Gulf of America", tables["gulf_masters"], tables["gulf_structures"], tables["gulf_locations"])
    pacific_rows, pacific_counts = _join_region(
        "Pacific", tables["pacific_masters"], tables["pacific_structures"], tables["pacific_locations"])
    rows = gulf_rows + pacific_rows
    rows.sort(key=lambda row: (row["REGION"], row["COMPLEX_ID"], row["STRUCTURE_NUMBER"]))
    source_count = len(tables["gulf_locations"]) + len(tables["pacific_locations"])
    if len(rows) != source_count:
        raise ValueError("source_location_population_incomplete")
    id_roster = [[row["REGION"], row["COMPLEX_ID"], row["STRUCTURE_NUMBER"]] for row in rows]
    if len({tuple(item) for item in id_roster}) != len(id_roster):
        raise ValueError("source_composite_identifier_collision")
    geometry = [row["geometry"] for row in rows]
    fields = [{"name": name, "type": "string"} for name in OUTPUT_FIELDS] + [
        {"name": "geometry", "type": "binary", "encoding": "WKB", "geometry_type": "Point"}]
    audit = {
        "source_feature_count": len(rows),
        "source_count_witness": source_count,
        "source_population_complete": True,
        "source_id_roster_complete": True,
        "unique_source_id_count": len(id_roster),
        "source_fields": fields,
        "source_schema_sha256": _sha(_canonical(fields)),
        "source_id_sha256": _sha(_canonical(sorted(id_roster))),
        "source_record_sha256": _sha(_canonical([
            [row[name] for name in OUTPUT_FIELDS] + [None if row["geometry"] is None else bytes(row["geometry"]).hex()]
            for row in rows])),
        "source_geometry_sha256": _sha(b"".join(row["geometry"] or b"" for row in rows)),
        "source_counts": {"gulf": gulf_counts, "pacific": pacific_counts},
        "source_page_update_times": page_times,
        "null_geometry_count": sum(1 for item in geometry if item is None),
        "invalid_geometry_count": 0,
        "zero_coordinate_count": gulf_counts["zero_coordinates"] + pacific_counts["zero_coordinates"],
        "proposed_crs_epsg": 4267,
        "crs_note": CRS_NOTE,
        "coverage_note": SCOPE,
        "attribute_note": (
            "All published master, structure and location fields are retained with source names "
            "and lexical values. ORNL fields with no producer equivalent (NAICS, OWNER, POSREL, "
            "GlobalID, VAL_METHOD, VAL_DATE) are not fabricated."),
        **GATES,
    }
    return rows, audit


def _parquet_body(rows: list[dict[str, object]]) -> bytes:
    schema = pa.schema(
        [pa.field(name, pa.string(), nullable=True) for name in OUTPUT_FIELDS]
        + [pa.field("geometry", pa.binary(), nullable=True)])
    columns = {name: pa.array([row[name] for row in rows], type=pa.string()) for name in OUTPUT_FIELDS}
    columns["geometry"] = pa.array([row["geometry"] for row in rows], type=pa.binary())
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["Point"],
        "crs": pyproj.CRS.from_epsg(4267).to_json_dict(), "edges": "planar"}}}
    table = pa.Table.from_pydict(columns, schema=schema.with_metadata(
        {b"geo": _canonical(geo), b"hifld:proposed_crs": CRS_NOTE.encode()}))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _capture_all(root: Path) -> dict[str, tuple[bytes, dict[str, object]]]:
    captures = {}
    with httpx.Client(follow_redirects=False, timeout=90,
                      headers={"User-Agent": "hifld-next-datasets/oil-and-natural-gas-platforms-draft"}) as client:
        for name, url in CAPTURE_URLS.items():
            body, receipt = capture_get(client, url, root, name, max_bytes=CAPTURE_LIMITS[name])
            captures[name] = (body, receipt)
    return captures


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict[str, object]]]:
    captures = {}
    for name, url in CAPTURE_URLS.items():
        body_path, wire_path, receipt_path = (root / f"{name}.body", root / f"{name}.wire",
                                              root / f"{name}.receipt.json")
        if any(path.is_symlink() for path in (body_path, wire_path, receipt_path)):
            raise ValueError("source_capture_symlink_rejected:" + name)
        body, wire = body_path.read_bytes(), wire_path.read_bytes()
        receipt = json.loads(receipt_path.read_bytes())
        if (receipt.get("status") != 200 or receipt.get("complete_body") is not True
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt.get("method") != "GET" or receipt.get("url") != url
                or receipt.get("final_url") != url):
            raise ValueError("source_capture_receipt_or_bytes_invalid:" + name)
        captures[name] = (body, receipt)
    return captures


def capture_oil_and_natural_gas_platforms(
    storage: StagingStorageResource, *, version: str, capture_directory: str | None = None,
) -> dict[str, object]:
    """Capture BSEE/BOEM tables and seal one complete, unapproved candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    local = Path(storage.local_dir).absolute()
    local.mkdir(parents=True, exist_ok=True)
    if (".." in local.parts or any(p.is_symlink() for p in (local, *local.parents))
            or shutil.disk_usage(local).free < 256_000_000):
        raise ValueError("unsafe_or_low_space_staging_root")
    root = Path(capture_directory).absolute() if capture_directory else Path(tempfile.mkdtemp(prefix="oil-platform-capture-"))
    if ".." in root.parts or any(p.is_symlink() for p in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        captures = _read_replay(root) if capture_directory else _capture_all(root)
        rows, audit = _validate_capture(captures)
        raw_evidence = {name: {"body_sha256": _sha(body), "wire_sha256": receipt["wire_sha256"]}
                        for name, (body, receipt) in sorted(captures.items())}
        audit["raw_evidence"] = raw_evidence
        audit["source_evidence_sha256"] = _sha(_canonical(raw_evidence))
        identity = processing_identity(Path(__file__).parents[3])
        parquet = _parquet_body(rows)
        version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                          audit["source_schema_sha256"], identity)
        prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
        raw_objects = {f"raw/{name}.body": body for name, (body, _) in captures.items()}
        raw_objects.update({f"raw/{name}.wire": (root / f"{name}.wire").read_bytes() for name in captures})
        receipts = {name: receipt for name, (_, receipt) in sorted(captures.items())}
        parquet_name = f"geoparquet/{FILE_SLUG}.parquet"
        inputs = []
        for name, receipt in receipts.items():
            producer = BOEM_PUBLISHER if name.startswith("pacific") else BSEE_PUBLISHER
            inputs.append({
                "input_id": name, "producer": producer, "host": urlsplit(receipt["url"]).hostname,
                "role": "producer_publication", "canonical_url": CAPTURE_URLS[name],
                "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                "license": "https://www.usa.gov/publicdomain/label/1.0/",
                "attribution": producer + "; source use and rights review remain pending.",
                "release_date": None,
                "release_date_unknown_reason": (
                    "The producer publishes page update timestamps and row revision dates, not a "
                    "versioned release date. Observed page update times are recorded in the audit."),
            })
        manifest = {
            "schema_version": 1, "approved": False,
            "title": "Oil and Natural Gas Platforms (BSEE/BOEM draft)",
            "description": SCOPE + " " + CRS_NOTE,
            "publisher": f"{BSEE_PUBLISHER} / {BOEM_PUBLISHER}",
            "agency": f"{BSEE} / {BOEM}",
            "source_url": GULF_LANDING,
            "license": "https://www.usa.gov/publicdomain/label/1.0/",
            "source_modified": None, "date_issued": None, "date_modified": None,
            "processor": "HIFLD Next", "code_commit": identity["code_commit"],
            "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
            "transformations": [{
                "operation": "producer_fixed_and_delimited_tables_to_structure_point_geoparquet",
                "parameters": {
                    "source_fields": "all published master, structure and location fields retained with source names and lexical values; no field is renamed away",
                    "identity": "native complex ID and structure number retained; the pair is the structure key within its region",
                    "geometry": CRS_NOTE,
                    "grain": "one row per published location row; structure and master attributes joined by native complex/structure keys",
                    "membership": "structure-only rows without a location are excluded and counted; location-only rows are retained with null structure attributes",
                    "candidate_status": "unapproved; source release, schema/ID, coverage, datum, rights and release pending",
                }}],
            "lineage": (
                "BSEE Platform/Rig and BOEM Pacific Platform landing pages publish the master, "
                "structure and location tables used here. The BSEE GIS platform.zip (Gulf only, "
                "five attributes, NAD27) was measured but not used because it lacks the key "
                "operational attributes present in the master and structure tables."),
            "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
                "reason": "Native source attributes and coordinates retained without repair or generalization; approval gates remain pending.",
                "objects": [{"path": parquet_name, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
            "unsupported_output_formats": {
                "geopackage": "No approved source-to-public schema mapping.",
                "pmtiles": "No display generalization approved."},
        }
        validate_release_provenance(manifest)
        validate_output_policy(manifest)
        ready = {
            "approved": False, "version": version,
            "objects": {parquet_name: {"bytes": len(parquet), "sha256": _sha(parquet)}},
            "source_evidence_sha256": audit["source_evidence_sha256"],
            "source_count_witness": audit["source_count_witness"],
            "source_population_complete": True, "source_id_roster_complete": True,
            **GATES,
        }
        outputs = {
            parquet_name: parquet,
            "metadata/source_manifest.json": _canonical(manifest) + b"\n",
            "metadata/source_audit.json": _canonical(audit) + b"\n",
            "metadata/processing_identity.json": _canonical(identity) + b"\n",
            "metadata/candidate_ready.json": _canonical(ready) + b"\n",
        }
        outputs.update(raw_objects)
        outputs.update({f"raw/{name}.receipt.json": _canonical(receipt) + b"\n" for name, receipt in receipts.items()})
    except Exception as exc:
        receipts = {}
        for path in sorted(root.glob("*.receipt.json")):
            try:
                receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError):
                receipts[path.name] = {"receipt_readback_failed": True}
        roster = [{"name": path.name, "bytes": path.stat().st_size, "sha256": _sha(path.read_bytes())}
                  for path in sorted(root.iterdir()) if path.is_file()]
        (root / "capture_manifest.json").write_bytes(_canonical({
            "complete": False, "error_code": type(exc).__name__,
            "retained_artifacts": roster, "receipts": receipts}) + b"\n")
        if captures:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
        raise
    fingerprint = _sha(_canonical(identity))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    result = {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["source_feature_count"],
        "source_count_witness": audit["source_count_witness"],
        "source_population_complete": True, "source_id_roster_complete": True,
        "source_id_sha256": audit["source_id_sha256"],
        "null_geometry_count": audit["null_geometry_count"],
        "invalid_geometry_count": audit["invalid_geometry_count"],
        "zero_coordinate_count": audit["zero_coordinate_count"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json",
        "source_evidence_sha256": audit["source_evidence_sha256"],
        **GATES,
    }
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if (not isinstance(payload, dict)
                or payload.get("source_record_sha256") != audit["source_record_sha256"]
                or payload.get("source_schema_sha256") != audit["source_schema_sha256"]):
            raise ValueError("cached_first_candidate_identity_or_source_changed")
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                fingerprint, identity, audit["source_record_sha256"],
                                audit["source_schema_sha256"], payload["objects"],
                                payload["capture_receipts"])
        return {**result, "reused": True}
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
    for name, descriptor in descriptors.items():
        snapshot = storage.object_snapshot(f"{prefix}/{name}")
        if (snapshot is None or snapshot.size != descriptor["bytes"]
                or storage.sha256_key(f"{prefix}/{name}") != descriptor["sha256"]
                or storage.object_snapshot(f"{prefix}/{name}") != snapshot):
            raise ValueError("candidate_output_physical_readback_failed:" + name)
    payload = {
        "processing_identity": identity,
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": receipts, "objects": descriptors,
    }
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    (root / "capture_manifest.json").write_bytes(_canonical({
        "complete": True, "feature_count": audit["source_feature_count"],
        "source_evidence_sha256": audit["source_evidence_sha256"], "receipts": receipts}) + b"\n")
    return {**result, "capture_directory": str(root)}

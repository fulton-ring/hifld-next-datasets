"""Electric retail service territory family core.

Builds a county-level utility service-territory draft from the current EIA-861
utility-by-county release joined to current Census TIGER/Line county polygons.
The same module keeps the shared capture, schema and staging helpers the state
pilot adapters use.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import tempfile
import unicodedata
import zipfile
from collections import defaultdict
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from xml.etree import ElementTree as ET

import geopandas as gpd
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.wkb import dumps as to_wkb

from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import census_latest, census_tiger_common
from dagster_hifld.sources.capture_common import (
    capture_get,
    content_derived_version,
    processing_identity,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = FILE_SLUG = "electric-retail-service-territories"
PAGE_URL = "https://www.eia.gov/electricity/data/eia861/"
REUSE_URL = "https://www.eia.gov/about/copyrights_reuse.php"
TIGER_ROOT = "https://www2.census.gov/geo/tiger/"
TIGER_PRODUCT = "COUNTY"
MAX_PAGE_BYTES = 4_000_000
MAX_REUSE_BYTES = 2_000_000
MAX_EIA_ZIP_BYTES = 200_000_000
MAX_TIGER_ZIP_BYTES = 200_000_000
MAX_EIA_UNCOMPRESSED_BYTES = 100_000_000
MIN_COUNTY_COUNT = 3_000
MAX_COUNTY_COUNT = 4_000
APPROVALS = {key: False for key in (
    "source_accepted", "source_freshness_accepted", "schema_contract_accepted",
    "id_contract_accepted", "coverage_accepted", "rights_accepted",
    "mapping_approved", "release_accepted", "promotion_allowed",
)}

_XLSX_NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
_SERVICE_HEADERS = ("Data Year", "Utility Number", "Utility Name", "Short Form", "State", "County")
_FRAME_HEADERS = (
    "Data Year", "Utility Number", "Utility Name", "Short Form", "Ownership Code",
    "Ownership", "Monthly", "Advanced Metering", "Delivery Company", "Demand Response",
    "Distribution Systems", "Dynamic Pricing", "Energy Efficiency", "Mergers",
    "Net Metering", "Non Net Metering Distributed", "Operational Data", "Reliablity",
    "Sales to Ultimate Customers", "Service Territory", "Utility Data",
)
_STRING_TYPES = {"esriFieldTypeString", "esriFieldTypeGUID", "esriFieldTypeGlobalID"}

STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56", "PR": "72", "VI": "78", "GU": "66",
    "AS": "60", "MP": "69",
}

COUNTY_ALIASES = {
    ("CT", "Fairfield"): ("09190", "09120", "09140"),
    ("CT", "Hartford"): ("09110", "09160", "09140"),
    ("CT", "Litchfield"): ("09160", "09140", "09190"),
    ("CT", "Middlesex"): ("09130",),
    ("CT", "New Haven"): ("09170", "09140"),
    ("CT", "New London"): ("09180", "09130", "09150"),
    ("CT", "Tolland"): ("09110", "09150"),
    ("CT", "Windham"): ("09150", "09180"),
    ("AK", "Prince of Wales Ketchikan"): ("02198", "02130"),
    ("AK", "Skagway Hoonah Angoon"): ("02230", "02105"),
    ("AK", "Valdez Cordova"): ("02063", "02066"),
    ("AK", "Wrangell Petersburg"): ("02275", "02195"),
    ("VA", "Bedford City"): ("51019",),
}


class _Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.urls.extend(value for key, value in attrs if key == "href" and value)


def latest_release(html: str) -> tuple[int, str]:
    """Select the newest active EIA-861 annual ZIP on the producer page."""
    parser = _Links()
    parser.feed(html)
    candidates = []
    for href in parser.urls:
        url = urljoin(PAGE_URL, href)
        match = re.fullmatch(re.escape(PAGE_URL) + r"(?:archive/)?zip/f861(20\d{2})(er)?\.zip", url)
        if match:
            candidates.append((int(match[1]), match[2] is not None, url))
    if not candidates:
        raise ValueError("no_active_eia861_release")
    year = max(item[0] for item in candidates)
    urls = [item[2] for item in candidates if item[0] == year and not item[1]]
    if len(urls) != 1:
        raise ValueError("ambiguous_latest_eia861_release")
    return year, urls[0]


def _xlsx_sheets(body: bytes, wanted: set[str]) -> dict[str, list[dict[str, str]]]:
    """Read bounded cached cell values from an EIA workbook without a spreadsheet engine."""
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        members = archive.infolist()
        if (len(members) > 200 or len({member.filename for member in members}) != len(members)
                or sum(member.file_size for member in members) > 350_000_000):
            raise ValueError("xlsx_expansion_limit")
        strings: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            with archive.open("xl/sharedStrings.xml") as stream:
                for _, element in ET.iterparse(stream, events=("end",)):
                    if element.tag == "{" + _XLSX_NS["s"] + "}si":
                        strings.append("".join(element.itertext()))
                        element.clear()
        relationships = {
            item.attrib["Id"]: item.attrib["Target"]
            for item in ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        }
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        sheets = workbook.find("s:sheets", _XLSX_NS)
        result = {}
        for sheet in sheets:
            name = sheet.attrib["name"]
            if name not in wanted:
                continue
            target = relationships[sheet.attrib[_REL_NS]]
            path = target.lstrip("/") if target.startswith("/") else "xl/" + target
            if path not in archive.namelist():
                raise ValueError("xlsx_sheet_path_invalid")
            rows = []
            with archive.open(path) as stream:
                for _, element in ET.iterparse(stream, events=("end",)):
                    if element.tag != "{" + _XLSX_NS["s"] + "}row":
                        continue
                    cells = {}
                    for cell in element:
                        column = re.fullmatch(r"([A-Z]+)\d+", cell.attrib["r"])[1]
                        value = cell.find("s:v", _XLSX_NS)
                        text = value.text if value is not None else None
                        if cell.find("s:f", _XLSX_NS) is not None and text is None:
                            raise ValueError("xlsx_uncached_formula_cell")
                        kind = cell.attrib.get("t")
                        if kind == "s":
                            text = strings[int(text)] if text is not None else None
                        elif kind == "inlineStr":
                            text = "".join(cell.find("s:is", _XLSX_NS).itertext())
                        elif kind == "e":
                            raise ValueError("xlsx_error_cell")
                        if text is not None:
                            cells[column] = text
                    rows.append(cells)
                    element.clear()
            result[name] = rows
    missing = wanted - set(result)
    if missing:
        raise ValueError("xlsx_sheet_missing:" + ",".join(sorted(missing)))
    return result


def _header(row: dict[str, str], width: int) -> tuple[str, ...]:
    return tuple(row.get(chr(ord("A") + index), "") for index in range(width))


def read_service_territory_workbook(body: bytes, year: int) -> list[dict[str, str]]:
    """Read the two EIA-861 utility-by-county sheets with native utility numbers."""
    sheets = _xlsx_sheets(body, {"Counties_States", "Counties_Territories"})
    rows = []
    seen = set()
    for sheet_name in ("Counties_States", "Counties_Territories"):
        sheet_rows = sheets[sheet_name]
        if not sheet_rows or _header(sheet_rows[0], 6) != _SERVICE_HEADERS:
            raise ValueError("eia861_service_schema_drift:" + sheet_name)
        for cells in sheet_rows[1:]:
            if not any(cells.values()):
                continue
            data_year, utility, name, short_form, state, county = _header(cells, 6)
            if data_year != str(year):
                raise ValueError("eia861_service_data_year_drift")
            if not re.fullmatch(r"\d+", utility):
                raise ValueError("eia861_service_utility_number_invalid")
            if not name.strip() or not county.strip():
                raise ValueError("eia861_service_name_or_county_missing")
            if state not in STATE_FIPS:
                raise ValueError("eia861_service_state_invalid:" + state)
            key = (utility, state, county)
            if key in seen:
                raise ValueError("eia861_service_duplicate_row")
            seen.add(key)
            rows.append({
                "data_year": data_year, "utility_number": utility,
                "utility_name": name, "short_form": short_form,
                "state": state, "county": county, "source_sheet": sheet_name,
            })
    if not rows:
        raise ValueError("empty_eia861_service_population")
    return rows


def read_frame_workbook(body: bytes, year: int) -> dict[str, dict[str, str]]:
    """Read utility-level ownership and service-territory participation from the frame."""
    sheets = _xlsx_sheets(body, {"Frame"})
    rows = sheets["Frame"]
    if not rows or _header(rows[0], len(_FRAME_HEADERS)) != _FRAME_HEADERS:
        raise ValueError("eia861_frame_schema_drift")
    frame = {}
    for cells in rows[1:]:
        if not any(cells.values()):
            continue
        data_year = cells.get("A", "")
        utility = cells.get("B", "")
        name = cells.get("C", "")
        ownership = cells.get("F", "")
        service_territory = cells.get("T", "")
        if data_year != str(year):
            raise ValueError("eia861_frame_data_year_drift")
        if not re.fullmatch(r"\d+", utility):
            raise ValueError("eia861_frame_utility_number_invalid")
        if not name.strip() or not ownership.strip():
            raise ValueError("eia861_frame_name_or_ownership_missing")
        if utility in frame:
            raise ValueError("eia861_frame_duplicate_utility")
        frame[utility] = {
            "utility_name": name, "ownership_type": ownership,
            "service_territory": service_territory,
        }
    if not frame:
        raise ValueError("empty_eia861_frame_population")
    return frame


def _normalize_county(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", text.casefold().replace("&", "and"))


def _county_lookup(counties: gpd.GeoDataFrame) -> dict[tuple[str, str], set[str]]:
    lookup: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in counties.itertuples():
        lookup[(row.STATEFP, _normalize_county(row.NAME))].add(row.GEOID)
        lookup[(row.STATEFP, _normalize_county(row.NAMELSAD))].add(row.GEOID)
    return lookup


def join_territories(
    territory_rows: list[dict[str, str]],
    frame: dict[str, dict[str, str]],
    counties: gpd.GeoDataFrame,
) -> tuple[list[dict[str, object]], dict[str, int]]:
    """Join each EIA utility-county row to current TIGER county equivalents."""
    lookup = _county_lookup(counties)
    known = set(counties["GEOID"])
    records = []
    unmatched = []
    skipped = 0
    for row in territory_rows:
        utility = row["utility_number"]
        if utility not in frame:
            raise ValueError("eia861_service_utility_missing_from_frame:" + utility)
        if frame[utility]["service_territory"] != "X":
            raise ValueError("eia861_service_utility_not_flagged_in_frame:" + utility)
        if frame[utility]["utility_name"] != row["utility_name"]:
            raise ValueError("eia861_service_utility_name_drift:" + utility)
        county = row["county"]
        if county == "Not Applicable":
            skipped += 1
            continue
        state_fips = STATE_FIPS[row["state"]]
        geoids = lookup.get((state_fips, _normalize_county(county)))
        if not geoids:
            geoids = set(COUNTY_ALIASES.get((row["state"], county), ()))
            missing = geoids - known
            if missing:
                raise ValueError("county_alias_target_missing:" + ",".join(sorted(missing)))
        if not geoids:
            unmatched.append(f"{row['state']}:{county}")
            continue
        records.append({
            **row,
            "ownership_type": frame[utility]["ownership_type"],
            "geoids": tuple(sorted(geoids)),
        })
    if unmatched:
        sample = ", ".join(sorted(set(unmatched))[:20])
        raise ValueError(f"unmatched_eia861_county_names:{len(unmatched)}:{sample}")
    if not records:
        raise ValueError("empty_eia861_county_join")
    return records, {"skipped_not_applicable": skipped, "matched_county_rows": len(records)}


def aggregate_territories(
    records: list[dict[str, object]],
    counties: gpd.GeoDataFrame,
    year: int,
) -> list[dict[str, object]]:
    """Dissolve joined counties into one feature per utility-state pair."""
    by_geoid = counties.set_index("GEOID")
    groups: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for record in records:
        groups[(str(record["utility_number"]), str(record["state"]))].append(record)
    features = []
    for (utility, state), rows in sorted(groups.items(), key=lambda item: (int(item[0][0]), item[0][1])):
        geoids = list(dict.fromkeys(geoid for row in rows for geoid in row["geoids"]))
        geometry = shapely.union_all(by_geoid.loc[geoids].geometry.array)
        if geometry.is_empty:
            raise ValueError("empty_dissolved_geometry:" + utility + ":" + state)
        if not geometry.is_valid:
            raise ValueError("invalid_dissolved_geometry:" + utility + ":" + state)
        first = rows[0]
        features.append({
            "utility_number": utility,
            "utility_name": str(first["utility_name"]),
            "ownership_type": str(first["ownership_type"]),
            "state": state,
            "county_count": len(rows),
            "source_year": year,
            "geometry": geometry,
        })
    return features


def _read_counties(body: bytes, edition: str) -> gpd.GeoDataFrame:
    stem = f"tl_{edition}_us_county"
    members = {
        f"{stem}.cpg", f"{stem}.dbf", f"{stem}.prj", f"{stem}.shp",
        f"{stem}.shp.ea.iso.xml", f"{stem}.shp.iso.xml", f"{stem}.shx",
    }
    frame = census_tiger_common.read_tiger_shapefile(
        body, expected_members=members, shapefile_name=f"{stem}.shp",
        extract_tmp_prefix="eia861-county-",
        read_file=lambda path: gpd.read_file(path, engine="pyogrio"),
    )
    required = {"GEOID", "STATEFP", "NAME", "NAMELSAD", "geometry"}
    if not required <= set(frame.columns) or str(frame.crs) != "EPSG:4269":
        raise ValueError("tiger_county_schema_or_crs_drift")
    if not MIN_COUNTY_COUNT <= len(frame) <= MAX_COUNTY_COUNT:
        raise ValueError("tiger_county_population_out_of_range")
    geoids = frame["GEOID"].astype(str)
    if (geoids.duplicated().any() or not geoids.str.fullmatch(r"\d{5}").all()
            or frame["STATEFP"].isna().any() or frame["NAME"].isna().any()):
        raise ValueError("tiger_county_identity_drift")
    geometry = frame.geometry
    if geometry.isna().any() or geometry.is_empty.any() or (~geometry.is_valid).any():
        raise ValueError("tiger_county_geometry_quality_failure")
    return frame[["GEOID", "STATEFP", "NAME", "NAMELSAD", "geometry"]].copy()


_OUTPUT_FIELDS = (
    ("utility_number", pa.string()),
    ("utility_name", pa.string()),
    ("ownership_type", pa.string()),
    ("state", pa.string()),
    ("county_count", pa.int64()),
    ("source_year", pa.int64()),
    ("geometry", pa.binary()),
)


def _geoparquet(features: list[dict[str, object]]) -> bytes:
    fields = [pa.field(name, kind, nullable=False) for name, kind in _OUTPUT_FIELDS]
    geometries = [to_wkb(row["geometry"], byte_order=1, output_dimension=2) for row in features]
    arrays = [
        pa.array([row["utility_number"] for row in features], type=pa.string()),
        pa.array([row["utility_name"] for row in features], type=pa.string()),
        pa.array([row["ownership_type"] for row in features], type=pa.string()),
        pa.array([row["state"] for row in features], type=pa.string()),
        pa.array([row["county_count"] for row in features], type=pa.int64()),
        pa.array([row["source_year"] for row in features], type=pa.int64()),
        pa.array(geometries, type=pa.binary()),
    ]
    geometry_types = sorted({row["geometry"].geom_type for row in features})
    metadata = {
        "version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB", "geometry_types": geometry_types,
            "crs": pyproj.CRS.from_epsg(4269).to_json_dict(),
        }},
    }
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata={b"geo": _canonical(metadata)}))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", version="2.6")
    output = sink.getvalue().to_pybytes()
    readback = pq.read_table(pa.BufferReader(output))
    if readback.num_rows != len(features) or readback.column_names != [field.name for field in fields]:
        raise ValueError("eia861_output_schema_or_population_readback_drift")
    if readback["geometry"].to_pylist() != geometries:
        raise ValueError("eia861_output_geometry_readback_drift")
    return output


def _stable_receipt(receipt: dict[str, object]) -> dict[str, object]:
    return {key: receipt.get(key) for key in (
        "method", "url", "final_url", "status", "complete_body", "wire_bytes",
        "wire_sha256", "bytes", "sha256", "content_encoding", "content_length",
    )}


def _input(
    input_id: str, receipt: dict[str, object], *, purpose: str,
    license: str, release_date: str | None, unknown_reason: str,
) -> dict[str, object]:
    url = str(receipt["url"])
    return {
        "input_id": input_id,
        "producer": "U.S. Energy Information Administration" if "eia.gov" in url else "U.S. Census Bureau, Geography Division",
        "host": urlsplit(url).hostname,
        "canonical_url": url,
        "retrieval_url": url,
        "observed_final_url": receipt.get("final_url"),
        "retrieved_at": receipt.get("observed_at"),
        "http_status": receipt.get("status"),
        "bytes": receipt.get("bytes"),
        "sha256": receipt.get("sha256"),
        "wire_bytes": receipt.get("wire_bytes"),
        "wire_sha256": receipt.get("wire_sha256"),
        "role": "producer_publication",
        "purpose": purpose,
        "release_date": release_date,
        "release_date_unknown_reason": unknown_reason,
        "license": license,
        "attribution": "U.S. Energy Information Administration" if "eia.gov" in url else "U.S. Census Bureau, Geography Division",
    }


def capture_electric_retail_service_territories(
    storage: StagingStorageResource, *, capture_directory: str | None = None,
    client: httpx.Client | None = None,
) -> dict[str, object]:
    """Capture the current EIA-861 and TIGER county inputs and stage one draft candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource):
        raise ValueError("explicit_staging_resource_required")
    root = Path(capture_directory).absolute() if capture_directory else Path(
        tempfile.mkdtemp(prefix="hifld-electric-retail-")
    )
    if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True, exist_ok=True)
    owned_client = client is None
    client = client or httpx.Client(
        timeout=httpx.Timeout(300.0, connect=30.0), follow_redirects=False,
        headers={"User-Agent": "hifld-next-datasets/electric-retail-1057"},
    )
    try:
        page_body, page_receipt = capture_get(
            client, PAGE_URL, root, "eia861_page", max_bytes=MAX_PAGE_BYTES,
        )
        year, eia_url = latest_release(page_body.decode("utf-8"))
        eia_body, eia_receipt = capture_get(
            client, eia_url, root, "eia861_zip", max_bytes=MAX_EIA_ZIP_BYTES,
        )
        _, reuse_receipt = capture_get(
            client, REUSE_URL, root, "eia_reuse_policy", max_bytes=MAX_REUSE_BYTES,
        )
        tiger_release = census_latest.discover_county_boundaries(client, root)
        tiger_body, tiger_receipt = capture_get(
            client, tiger_release["source_url"], root, "tiger_county_zip",
            max_bytes=MAX_TIGER_ZIP_BYTES,
        )
        with zipfile.ZipFile(io.BytesIO(eia_body)) as archive:
            members = archive.infolist()
            if (len(members) > 200 or len({member.filename for member in members}) != len(members)
                    or sum(member.file_size for member in members) > MAX_EIA_UNCOMPRESSED_BYTES):
                raise ValueError("eia861_zip_expansion_limit")
            service_name = f"Service_Territory_{year}.xlsx"
            frame_name = f"Frame_{year}.xlsx"
            names = {member.filename for member in members}
            if service_name not in names or frame_name not in names:
                raise ValueError("eia861_workbook_members_missing")
            service_body = archive.read(service_name)
            frame_body = archive.read(frame_name)
        territory_rows = read_service_territory_workbook(service_body, year)
        frame = read_frame_workbook(frame_body, year)
        counties = _read_counties(tiger_body, tiger_release["edition"])
        records, join_audit = join_territories(territory_rows, frame, counties)
        features = aggregate_territories(records, counties, year)
        if not features:
            raise ValueError("empty_electric_retail_population")
        identity = processing_identity(Path(__file__).parents[3])
        output = _geoparquet(features)
        record_sha = _sha(_canonical([
            {key: value for key, value in row.items() if key != "geometry"} for row in features
        ]))
        geometry_hash = hashlib.sha256()
        for row in features:
            value = to_wkb(row["geometry"], byte_order=1, output_dimension=2)
            geometry_hash.update(len(value).to_bytes(8, "little"))
            geometry_hash.update(value)
        geometry_sha = geometry_hash.hexdigest()
        schema_sha = _sha(_canonical({
            "output_fields": [name for name, _kind in _OUTPUT_FIELDS],
            "eia861_data_year": year,
            "tiger_edition": tiger_release["edition"],
            "state_fips": STATE_FIPS,
            "county_aliases": {f"{state}:{name}": list(geoids) for (state, name), geoids in COUNTY_ALIASES.items()},
        }))
        version = content_derived_version(record_sha, geometry_sha, schema_sha, identity)
        prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
        parquet_path = f"geoparquet/{FILE_SLUG}.parquet"
        utility_count = len({row["utility_number"] for row in features})
        county_count = len({geoid for record in records for geoid in record["geoids"]})
        state_count = len({row["state"] for row in features})
        coverage = {
            "population_scope": "All EIA-861 utility-by-county service-territory rows for the selected annual release, excluding only the source 'Not Applicable' county sentinel.",
            "eia861_data_year": year,
            "tiger_edition": tiger_release["edition"],
            "territory_row_count": len(territory_rows),
            "matched_county_row_count": join_audit["matched_county_rows"],
            "skipped_not_applicable_count": join_audit["skipped_not_applicable"],
            "feature_count": len(features),
            "utility_count": utility_count,
            "state_count": state_count,
            "distinct_county_count": county_count,
            "boundary_precision": "County-level approximation. Utility territories are dissolved current TIGER county equivalents, not utility-owned legal boundaries.",
            "source_population_complete": True,
        }
        inputs = [
            _input("eia861_page", page_receipt, purpose="methodology",
                   license=REUSE_URL, release_date=None,
                   unknown_reason="Producer release page; retrieval time is not a publication date."),
            _input("eia861_zip", eia_receipt, purpose="data",
                   license=REUSE_URL, release_date=None,
                   unknown_reason=f"EIA-861 data year {year}; the producer page and ZIP headers do not establish an exact publication date."),
            _input("eia_reuse_policy", reuse_receipt, purpose="license",
                   license=REUSE_URL, release_date=None,
                   unknown_reason="Producer reuse policy page; revision date is not established by this capture."),
            _input("tiger_annual_listing", tiger_release["evidence_receipts"][0], purpose="methodology",
                   license="https://www.usa.gov/government-works", release_date=None,
                   unknown_reason="Census annual directory listing; retrieval time is not a publication date."),
            _input("tiger_product_listing", tiger_release["evidence_receipts"][1], purpose="methodology",
                   license="https://www.usa.gov/government-works", release_date=None,
                   unknown_reason="Census product directory listing; retrieval time is not a publication date."),
            _input("tiger_county_zip", tiger_receipt, purpose="data",
                   license="https://www.usa.gov/government-works", release_date=None,
                   unknown_reason=f"TIGER/Line {tiger_release['edition']} county archive; the directory listing does not establish an exact archive publication date."),
        ]
        manifest = {
            "schema_version": 1, "approved": False, "processor": "HIFLD Next",
            "title": "Electric Retail Service Territories (EIA-861 utility-by-county; draft)",
            "description": "Utility service territories approximated by dissolving current Census TIGER county equivalents for each EIA-861 utility-by-county row. County-level approximation only; no legal boundary or historical HIFLD equivalence is claimed.",
            "publisher": "U.S. Energy Information Administration",
            "agency": "U.S. Department of Energy",
            "source_url": eia_url,
            "license": REUSE_URL,
            "date_issued": None, "date_modified": None, "source_modified": None,
            "source_vintage": f"EIA-861 {year} / TIGER/Line {tiger_release['edition']}",
            "lineage": [PAGE_URL, eia_url, TIGER_ROOT, tiger_release["listing_url"], tiger_release["source_url"], REUSE_URL],
            "code_commit": identity["code_commit"],
            "code_identity_scheme": identity["code_identity_scheme"],
            "inputs": inputs,
            "transformations": [
                {"operation": "Select the newest active EIA-861 annual ZIP and current TIGER/Line county ZIP from producer listings", "parameters": {"eia861_data_year": year, "tiger_edition": tiger_release["edition"], "selection_rule": "largest published year with one exact national ZIP; no older-edition fallback"}},
                {"operation": "Join each EIA utility-county row to TIGER county equivalents by state FIPS and normalized name", "parameters": {"county_aliases": {f"{state}:{name}": list(geoids) for (state, name), geoids in COUNTY_ALIASES.items()}, "skipped_sentinel": "Not Applicable"}},
                {"operation": "Dissolve joined county geometries into one feature per utility-state pair", "parameters": {"key": ["utility_number", "state"], "geometry": "shapely.union_all in native EPSG:4269"}},
            ],
            "field_definitions": {
                "utility_number": {"source_fields": ["Utility Number"], "input_ids": ["eia861_zip"], "basis": "publisher", "description": "Native EIA utility number as a string, preserving its lexical value."},
                "utility_name": {"source_fields": ["Utility Name"], "input_ids": ["eia861_zip"], "basis": "publisher", "description": "Native EIA utility name."},
                "ownership_type": {"source_fields": ["Ownership"], "input_ids": ["eia861_zip"], "basis": "publisher", "description": "Native EIA frame ownership type."},
                "state": {"source_fields": ["State"], "input_ids": ["eia861_zip"], "basis": "publisher", "description": "Native EIA state postal code; utilities operating in multiple states have one feature per state."},
                "county_count": {"source_fields": ["County"], "input_ids": ["eia861_zip"], "basis": "derived", "description": "Number of EIA utility-county rows dissolved into this feature.", "units": "count"},
                "source_year": {"source_fields": ["Data Year"], "input_ids": ["eia861_zip"], "basis": "publisher", "description": "Native EIA-861 data year."},
                "geometry": {"source_fields": ["GEOID", "geometry"], "input_ids": ["tiger_county_zip"], "basis": "derived", "description": "Union of current TIGER county equivalents for the utility's EIA county rows; native EPSG:4269. County-level approximation, not a legal boundary."},
            },
            "coverage": coverage,
            "source_relationship": {
                "relationship": "unverified",
                "evidence_input_ids": ["eia861_zip", "tiger_county_zip"],
                "unknown_reason": "This source-native county approximation does not establish equivalence to the historical HIFLD/ORNL utility polygon population or its identifiers.",
            },
            "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
                "reason": "County-dissolved utility-state draft in native EPSG:4269; no legal boundary, historical equivalence or release acceptance is claimed.",
                "objects": [{"path": parquet_path, "bytes": len(output), "sha256": _sha(output)}],
            }}},
            "unsupported_output_formats": {
                "geopackage": "No approved export-field mapping or downstream schema contract.",
                "shapefile_zip": "No approved field mapping or encoding contract.",
                "pmtiles": "No approved display transformation or generalization.",
            },
            **APPROVALS,
        }
        validate_provenance(manifest)
        validate_output_policy(manifest)
        validate_release_provenance(manifest)
        audit = {
            "source": {
                "eia861_page": PAGE_URL, "eia861_zip": eia_url, "eia861_data_year": year,
                "tiger_edition": tiger_release["edition"], "tiger_county_zip": tiger_release["source_url"],
            },
            "acquisition_receipts": {
                name: {**_stable_receipt(receipt), "observed_at": receipt.get("observed_at")}
                for name, receipt in (
                    ("eia861_page", page_receipt), ("eia861_zip", eia_receipt),
                    ("eia_reuse_policy", reuse_receipt), ("tiger_county_zip", tiger_receipt),
                    ("tiger_annual_listing", tiger_release["evidence_receipts"][0]),
                    ("tiger_product_listing", tiger_release["evidence_receipts"][1]),
                )
            },
            "eia861": {
                "territory_row_count": len(territory_rows), "frame_utility_count": len(frame),
            },
            "join": {
                **join_audit, "distinct_county_count": county_count,
                "county_aliases": {f"{state}:{name}": list(geoids) for (state, name), geoids in COUNTY_ALIASES.items()},
            },
            "output": {
                "feature_count": len(features), "utility_count": utility_count,
                "state_count": state_count, "county_row_count": len(territory_rows),
            },
            "processing_identity": identity,
            **APPROVALS,
        }
        raw_inputs = {
            "eia861_page": page_receipt, "eia861_zip": eia_receipt,
            "eia_reuse_policy": reuse_receipt, "tiger_county_zip": tiger_receipt,
            "tiger_annual_listing": tiger_release["evidence_receipts"][0],
            "tiger_product_listing": tiger_release["evidence_receipts"][1],
        }
        source_files = {
            "tiger_annual_listing": "county-annual-listing",
            "tiger_product_listing": "county-product-listing",
        }
        for name, receipt in raw_inputs.items():
            stem = source_files.get(name, name)
            body = (root / f"{stem}.body").read_bytes()
            wire = (root / f"{stem}.wire").read_bytes()
            write_immutable(storage, f"{prefix}/raw/{name}.body", body)
            write_immutable(storage, f"{prefix}/raw/{name}.wire", wire)
            write_immutable(storage, f"{prefix}/raw/{name}.receipt.json", _canonical(receipt) + b"\n")
        ready = {"approved": False, "version": version, "objects": {parquet_path: {"bytes": len(output), "sha256": _sha(output)}}, "source_population_complete": True, **APPROVALS}
        ready_body = _canonical(ready) + b"\n"
        for name, body in (
            (parquet_path, output),
            ("metadata/source_manifest.json", _canonical(manifest) + b"\n"),
            ("metadata/source_audit.json", _canonical(audit) + b"\n"),
            ("metadata/processing_identity.json", _canonical(identity) + b"\n"),
            ("metadata/candidate_ready.json", ready_body),
        ):
            write_immutable(storage, f"{prefix}/{name}", body)
            if storage.read_key(f"{prefix}/{name}") != body:
                raise ValueError("candidate_readback_mismatch:" + name)
        return {
            "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": len(features), "utility_count": utility_count,
            "state_count": state_count, "county_row_count": len(territory_rows),
            "matched_county_row_count": join_audit["matched_county_rows"],
            "distinct_county_count": county_count,
            "skipped_not_applicable_count": join_audit["skipped_not_applicable"],
            "source_year": year, "tiger_edition": tiger_release["edition"],
            "source_population_complete": True,
            "null_geometry_count": 0, "invalid_geometry_count": 0,
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "prebuilt_geoparquet": f"{prefix}/{parquet_path}",
            "prebuilt_geoparquet_sha256": _sha(output),
            **APPROVALS,
        }
    finally:
        if owned_client:
            client.close()


def _arrow_type(kind: str):
    if kind in _STRING_TYPES:
        return pa.string()
    if kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger", "esriFieldTypeDate"}:
        return pa.int64()
    if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
        return pa.float64()
    raise ValueError("unsupported_esri_field_type:" + kind)


def _field_signature(fields: object) -> list[tuple[str, str, int | None]]:
    if not isinstance(fields, list) or not fields or any(not isinstance(field, dict) for field in fields):
        raise ValueError("source_schema_missing")
    result = []
    for field in fields:
        name, kind, length = field.get("name"), field.get("type"), field.get("length")
        if not isinstance(name, str) or not isinstance(kind, str) or (length is not None and type(length) is not int):
            raise ValueError("source_schema_invalid")
        result.append((name, kind, length))
    if len({field[0] for field in result}) != len(result):
        raise ValueError("source_schema_invalid")
    return result


def _safe_local_storage(storage: StagingStorageResource) -> Path:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    root = Path(storage.local_dir).absolute()
    if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
        raise ValueError("unsafe_local_staging_root")
    root.mkdir(parents=True, exist_ok=True)
    return root

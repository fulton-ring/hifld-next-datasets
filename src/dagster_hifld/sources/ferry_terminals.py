"""Official BTS NCFO ferry terminals, newest edition discovered from data.bts.gov.

The BTS National Census of Ferry Operators publishes one terminals table per
edition on data.bts.gov. This adapter discovers the newest ``YYYY NCFO Terminals
File`` dataset from the BTS Socrata catalog, captures the table metadata and
every row, and writes a native GeoParquet candidate. Source values and their
precision are preserved; only the JSON string encoding is typed. Release and
mapping approvals remain false.
"""
from __future__ import annotations

import json
import math
import re
import shutil
import tempfile
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import Point

from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "ferry-terminals"
FILE_SLUG = DATASET_SLUG
DOMAIN = "data.bts.gov"
CATALOG_URL = f"https://{DOMAIN}/api/catalog/v1"
VIEW_URL = f"https://{DOMAIN}/api/views"
RESOURCE_URL = f"https://{DOMAIN}/resource"
TITLE_PATTERN = re.compile(r"^(?P<year>(?:19|20)\d{2}) NCFO Terminals File$")
PRODUCER = "U.S. Department of Transportation, Bureau of Transportation Statistics"
ATTRIBUTION = "U.S. Department of Transportation / Bureau of Transportation Statistics / NCFO"
LICENSE = "U.S. Government Works (Socrata licenseId USGOV_WORKS)"
PAGE_SIZE = 1000
MAX_BODY_BYTES = 64_000_000
MAX_PAGES = 100

COLUMN_TYPES = {
    "terminal_id": "int",
    "terminal_name": "text",
    "term_city": "text",
    "term_state": "text",
    "term_country": "text",
    "latitude": "float",
    "longitude": "float",
    "in_operation": "int",
    "intercity_bus": "int",
    "intercity_rail": "int",
    "local_bus": "int",
    "local_rail": "int",
    "bike_share": "int",
    "parking": "int",
    "terminal_operation": "text",
    "terminal_ownership": "text",
    "number_of_operators": "int",
    "census_year": "int",
}
FLAG_COLUMNS = frozenset({
    "in_operation", "intercity_bus", "intercity_rail", "local_bus",
    "local_rail", "bike_share", "parking",
})
INTEGER = re.compile(r"-?(?:0|[1-9]\d*)")


def require_local(storage: StagingStorageResource) -> None:
    """Only an explicit, unprefixed local staging root may hold draft output."""
    if (not isinstance(storage, StagingStorageResource)
            or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir
            or storage.prefix):
        raise ValueError("draft_requires_explicit_isolated_local_staging")


def _safe_release_date(epoch: object) -> str:
    if type(epoch) is not int or epoch < 0:
        raise ValueError("source_release_timestamp_missing")
    return datetime.fromtimestamp(epoch, timezone.utc).date().isoformat()


def _catalog_url() -> str:
    return CATALOG_URL + "?" + urlencode({"q": "NCFO Terminals", "limit": 100, "only": "dataset"})


def _view_url(dataset_id: str) -> str:
    return f"{VIEW_URL}/{dataset_id}.json"


def _count_url(dataset_id: str) -> str:
    return f"{RESOURCE_URL}/{dataset_id}.json?" + urlencode({"$select": "count(*)"})


def _page_url(dataset_id: str, offset: int) -> str:
    return f"{RESOURCE_URL}/{dataset_id}.json?" + urlencode({
        "$limit": PAGE_SIZE, "$offset": offset, "$order": "terminal_id",
    })


def _json(body: bytes) -> object:
    try:
        return json.loads(body)
    except ValueError as exc:
        raise ValueError("source_response_is_not_json") from exc


def select_newest_edition(catalog: object) -> dict:
    """Pick the newest exact ``YYYY NCFO Terminals File`` dataset on data.bts.gov."""
    if not isinstance(catalog, dict) or not isinstance(catalog.get("results"), list):
        raise ValueError("source_catalog_results_missing")
    candidates: dict[int, dict] = {}
    for item in catalog["results"]:
        if not isinstance(item, dict):
            continue
        resource = item.get("resource")
        metadata = item.get("metadata")
        if not isinstance(resource, dict) or not isinstance(metadata, dict):
            continue
        if resource.get("type") != "dataset" or metadata.get("domain") != DOMAIN:
            continue
        match = TITLE_PATTERN.match(str(resource.get("name", "")))
        if match is None:
            continue
        year = int(match.group("year"))
        if year in candidates:
            raise ValueError("ambiguous_ncfo_terminals_edition:" + str(year))
        candidates[year] = {
            "dataset_id": resource.get("id"),
            "name": resource.get("name"),
            "year": year,
        }
    if not candidates:
        raise ValueError("no_official_ncfo_terminals_dataset")
    newest = max(candidates)
    edition = candidates[newest]
    if not isinstance(edition["dataset_id"], str) or not re.fullmatch(r"[a-z0-9]{4}-[a-z0-9]{4}", edition["dataset_id"]):
        raise ValueError("source_dataset_id_invalid")
    return edition


def validate_view(view: object, edition: dict) -> list[str]:
    """Require the official BTS dataset identity, edition year and column roster."""
    if not isinstance(view, dict):
        raise ValueError("source_view_metadata_missing")
    if view.get("id") != edition["dataset_id"] or view.get("name") != edition["name"]:
        raise ValueError("source_dataset_identity_changed")
    if view.get("provenance") != "official" or "Bureau of Transportation Statistics" not in str(view.get("attribution", "")):
        raise ValueError("source_is_not_official_bts_publication")
    if f" {edition['year']} NCFO" not in str(view.get("description", "")):
        raise ValueError("source_edition_description_mismatch")
    columns = view.get("columns")
    if not isinstance(columns, list) or not columns:
        raise ValueError("source_column_roster_missing")
    names = []
    for column in columns:
        if not isinstance(column, dict) or not isinstance(column.get("fieldName"), str):
            raise ValueError("source_column_declaration_invalid")
        names.append(column["fieldName"])
    if len(names) != len(set(names)) or set(names) != set(COLUMN_TYPES):
        raise ValueError("source_column_roster_drift")
    for column in columns:
        name = column["fieldName"]
        expected = "text" if COLUMN_TYPES[name] == "text" else "number"
        if column.get("dataTypeName") != expected:
            raise ValueError("source_column_physical_type_drift:" + name)
    return names


def _integer(value: object, name: str) -> int:
    if not isinstance(value, str) or INTEGER.fullmatch(value) is None:
        raise ValueError("source_integer_value_drift:" + name)
    parsed = int(value)
    if not -(2 ** 63) <= parsed < 2 ** 63:
        raise ValueError("source_integer_out_of_range:" + name)
    return parsed


def _float(value: object, name: str) -> float:
    if not isinstance(value, str):
        raise ValueError("source_number_value_drift:" + name)
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("source_number_value_drift:" + name) from exc
    if not parsed.is_finite():
        raise ValueError("source_number_value_drift:" + name)
    result = float(parsed)
    if not math.isfinite(result):
        raise ValueError("source_number_value_drift:" + name)
    return result


def normalize_rows(pages: list[object], columns: list[str], edition: dict,
                   expected_count: int) -> list[dict]:
    """Type the Socrata string values without dropping or reordering records."""
    rows: list[dict] = []
    for page in pages:
        if not isinstance(page, list):
            raise ValueError("source_rows_page_missing")
        for row in page:
            if not isinstance(row, dict) or set(row) != set(columns):
                raise ValueError("source_record_column_roster_drift")
            typed: dict = {}
            for name in columns:
                value = row[name]
                kind = COLUMN_TYPES[name]
                if kind == "text":
                    if not isinstance(value, str):
                        raise ValueError("source_text_value_drift:" + name)
                    typed[name] = value
                elif kind == "int":
                    typed[name] = _integer(value, name)
                else:
                    typed[name] = _float(value, name)
            if typed["census_year"] != edition["year"]:
                raise ValueError("source_edition_year_mismatch")
            for name in FLAG_COLUMNS:
                if typed[name] not in (0, 1):
                    raise ValueError("source_flag_value_drift:" + name)
            if typed["number_of_operators"] < 1:
                raise ValueError("source_operator_count_drift")
            if not -90 <= typed["latitude"] <= 90 or not -180 <= typed["longitude"] <= 180:
                raise ValueError("source_coordinate_out_of_range")
            rows.append(typed)
    if len(rows) != expected_count:
        raise ValueError("source_population_incomplete")
    ids = [row["terminal_id"] for row in rows]
    if len(set(ids)) != len(ids):
        raise ValueError("source_terminal_identifier_duplicate")
    if ids != sorted(ids):
        raise ValueError("source_terminal_identifier_order_changed")
    return rows


def _arrow_type(kind: str) -> pa.DataType:
    return {"int": pa.int64(), "float": pa.float64(), "text": pa.string()}[kind]


def typed_table(rows: list[dict], columns: list[str]) -> pa.Table:
    schema = pa.schema([pa.field(name, _arrow_type(COLUMN_TYPES[name]), nullable=True)
                        for name in columns] + [pa.field("geometry", pa.binary(), nullable=False)])
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB",
            "geometry_types": ["Point"],
            "crs": pyproj.CRS.from_epsg(4326).to_json_dict(),
            "edges": "planar",
        }},
    }
    table = pa.Table.from_pylist(
        [{**{name: row[name] for name in columns},
          "geometry": Point(row["longitude"], row["latitude"]).wkb} for row in rows],
        schema=schema.with_metadata({b"geo": _canonical(geo)}),
    )
    return table


def _parquet(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _record_sha(rows: list[dict], columns: list[str]) -> str:
    return _sha(_canonical([[row[name] for name in columns] for row in rows]))


def _geometry_sha(rows: list[dict]) -> str:
    return _sha(_canonical([[row["terminal_id"], row["longitude"], row["latitude"]] for row in rows]))


def _schema_sha(columns: list[str]) -> str:
    return _sha(_canonical([[name, COLUMN_TYPES[name]] for name in columns]))


def _input(name: str, receipt: dict, dataset_id: str, release_date: str) -> dict:
    return {
        "input_id": f"{name}:{receipt['sha256']}",
        "producer": PRODUCER,
        "host": urlsplit(receipt["url"]).hostname,
        "role": "producer_publication",
        "canonical_url": f"https://{DOMAIN}/d/{dataset_id}",
        "retrieval_url": receipt["url"],
        "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"],
        "bytes": receipt["bytes"],
        "sha256": receipt["sha256"],
        "release_date": release_date,
        "license": LICENSE,
        "attribution": ATTRIBUTION,
    }


def _manifest(edition: dict, view: dict, rows: list[dict], receipts: dict,
              parquet: bytes, identity: dict, issued: str, modified: str) -> dict:
    inputs = [_input(name, receipt, edition["dataset_id"], issued)
              for name, receipt in sorted(receipts.items())]
    manifest = {
        "schema_version": 1,
        "approved": False,
        "title": f"Ferry Terminals — {edition['year']} NCFO",
        "description": (f"BTS National Census of Ferry Operators {edition['year']} terminals file; "
                        f"{len(rows)} terminal records published on {DOMAIN}."),
        "publisher": PRODUCER,
        "agency": "Bureau of Transportation Statistics",
        "source_url": f"https://{DOMAIN}/d/{edition['dataset_id']}",
        "license": LICENSE,
        "source_modified": modified,
        "date_issued": issued,
        "temporal_start": str(edition["year"]),
        "temporal_end": str(edition["year"]),
        "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "inputs": inputs,
        "lineage": (
            f"The BTS Socrata catalog on {DOMAIN} lists one exact '{edition['name']}' dataset per "
            f"NCFO edition; the newest matching dataset was selected ({edition['year']}). The dataset "
            f"view declares provenance=official, attribution '{view.get('attribution')}', and the "
            f"publication timestamp used here. All {len(rows)} rows were read in terminal_id order and "
            "the count was rechecked after capture. BTS states the 2024 terminals file has one entry per "
            "terminal, combining operator answers where they disagree. The separately maintained "
            "NTAD Ferry Routes layer (issue #88) still serves NCFO 2020 / data year 2019."
        ),
        "transformations": [{
            "operation": "Socrata JSON table to native GeoParquet",
            "parameters": {
                "columns": "all source columns retained in declared order",
                "terminal_id": "source identifier retained verbatim as int64",
                "coordinates": "source latitude/longitude decimal strings typed as float64 and encoded "
                               "as EPSG:4326 Point(longitude, latitude); no repair or reproject",
                "encoding": "Socrata JSON renders every value as a string; numeric typing only",
                "release_status": "unapproved",
            },
        }],
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": ("One typed source column per published field plus a native WKB point; "
                       "release and mapping approval pending."),
            "objects": [{"path": f"geoparquet/{FILE_SLUG}.parquet",
                         "sha256": _sha(parquet), "bytes": len(parquet)}],
        }}},
        "unsupported_output_formats": {
            "shapefile_zip": "DBF field-name truncation would change the published column names.",
            "geopackage": "No geopackage mapping is generated for this source; acceptance pending.",
            "pmtiles": "Display-only format; no publication transformation approved.",
        },
    }
    validate_release_provenance(manifest)
    return manifest


def _audit(edition: dict, columns: list[str], rows: list[dict],
           record_sha: str, geometry_sha: str, schema_sha: str,
           issued: str, modified: str) -> dict:
    def counts(name: str) -> dict:
        result: dict[str, int] = {}
        for row in rows:
            result[str(row[name])] = result.get(str(row[name]), 0) + 1
        return dict(sorted(result.items()))
    return {
        "dataset_id": edition["dataset_id"],
        "dataset_name": edition["name"],
        "edition_year": edition["year"],
        "census_year_values": sorted({row["census_year"] for row in rows}),
        "feature_count": len(rows),
        "source_columns": columns,
        "source_release_date": issued,
        "source_modified": modified,
        "source_record_sha256": record_sha,
        "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": schema_sha,
        "crs_epsg": 4326,
        "geometry_type": "Point",
        "bounds": [min(row["longitude"] for row in rows), min(row["latitude"] for row in rows),
                   max(row["longitude"] for row in rows), max(row["latitude"] for row in rows)],
        "unique_terminal_ids": len({row["terminal_id"] for row in rows}),
        "null_coordinates": sum(row["latitude"] is None or row["longitude"] is None for row in rows),
        "term_state_counts": counts("term_state"),
        "term_country_counts": counts("term_country"),
        "in_operation_counts": counts("in_operation"),
        "source_release_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    }


def _capture_root(directory: Path | None) -> tuple[Path, bool]:
    if directory is not None:
        root = Path(directory).absolute()
        root.mkdir(parents=True, exist_ok=True)
        return root, False
    return Path(tempfile.mkdtemp(prefix="hifld-ferry-terminals-")), True


def _result(version: str, edition: dict, rows: list[dict], release_date: str) -> dict:
    return {
        "dataset_slug": DATASET_SLUG,
        "file_slug": FILE_SLUG,
        "version": version,
        "feature_count": len(rows),
        "edition_year": edition["year"],
        "dataset_id": edition["dataset_id"],
        "source_release_date": release_date,
        "candidate_ready": f"{candidate_prefix(DATASET_SLUG, FILE_SLUG, version)}/metadata/candidate_ready.json",
        "source_release_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    }


def _reuse_candidate(storage: StagingStorageResource, prefix: str, edition: dict, rows: list[dict],
                     record_sha: str, schema_sha: str, issued: str,
                     capture_names: list[str]) -> dict:
    """Return an intact same-content candidate instead of rewriting timestamped receipts."""
    ready_key = f"{prefix}/metadata/candidate_ready.json"
    if storage.object_snapshot(ready_key) is None:
        if storage.list_prefix(prefix):
            raise ValueError("incomplete_existing_candidate")
        raise KeyError(ready_key)
    ready = _json(storage.read_key(ready_key))
    if (not isinstance(ready, dict) or ready.get("approved") is not False
            or ready.get("version") != prefix.rsplit("/", 1)[-1]
            or ready.get("feature_count") != len(rows)
            or ready.get("edition_year") != edition["year"]
            or ready.get("source_record_sha256") != record_sha
            or ready.get("source_schema_sha256") != schema_sha):
        raise ValueError("cached_candidate_identity_conflict")
    objects = ready.get("objects")
    if not isinstance(objects, dict) or set(objects) != {f"geoparquet/{FILE_SLUG}.parquet"}:
        raise ValueError("cached_candidate_object_roster_conflict")
    expected = set(objects) | {
        "metadata/source_manifest.json", "metadata/source_audit.json",
        "metadata/processing_identity.json", "metadata/candidate_ready.json",
    } | {f"raw/{name}{suffix}" for name in capture_names
         for suffix in (".body", ".wire", ".receipt.json")}
    actual = {key[len(prefix) + 1:] for key in storage.list_prefix(prefix)}
    derived = {"metadata/quality_manifest.json", "metadata/data_dictionary.json",
               "metadata/resolved_provenance.json", "metadata/source/source_manifest.json",
               "metadata/source/data_dictionary.json", "metadata/source/quality_manifest.json"}
    if not expected <= actual or actual - expected - derived - {
            name for name in actual if name.startswith("metadata/source/version_")}:
        raise ValueError("cached_candidate_storage_roster_changed")
    for name, descriptor in objects.items():
        snapshot = storage.object_snapshot(f"{prefix}/{name}")
        if (snapshot is None or descriptor != {"sha256": storage.sha256_key(f"{prefix}/{name}"),
                                               "bytes": snapshot.size}):
            raise ValueError("cached_candidate_object_integrity_failed:" + name)
    return _result(prefix.rsplit("/", 1)[-1], edition, rows, issued)


def stage_ferry_terminals_candidate(storage: StagingStorageResource, *,
                                    capture_dir: Path | None = None,
                                    client: httpx.Client | None = None) -> dict:
    """Capture the newest NCFO terminals edition and write a native draft candidate."""
    require_local(storage)
    root, ephemeral = _capture_root(capture_dir)
    owned = client is None
    if client is None:
        client = httpx.Client(follow_redirects=False, timeout=60,
                              headers={"User-Agent": "hifld-next-datasets/ferry-terminals-draft"})
    captures: dict[str, tuple[bytes, dict]] = {}
    try:
        catalog_body, catalog_receipt = capture_get(client, _catalog_url(), root, "catalog",
                                                    max_bytes=MAX_BODY_BYTES)
        captures["catalog"] = (catalog_body, catalog_receipt)
        edition = select_newest_edition(_json(catalog_body))
        dataset_id = edition["dataset_id"]

        view_body, view_receipt = capture_get(client, _view_url(dataset_id), root, "view",
                                              max_bytes=MAX_BODY_BYTES)
        captures["view"] = (view_body, view_receipt)
        view = _json(view_body)
        columns = validate_view(view, edition)
        issued = _safe_release_date(view.get("publicationDate"))
        modified = _safe_release_date(view.get("rowsUpdatedAt"))

        count_before_body, count_before_receipt = capture_get(
            client, _count_url(dataset_id), root, "count_before", max_bytes=MAX_BODY_BYTES)
        captures["count_before"] = (count_before_body, count_before_receipt)
        count_before = _source_count(_json(count_before_body))

        pages: list[object] = []
        offset = 0
        while offset < count_before:
            name = f"rows_{offset // PAGE_SIZE:04d}"
            page_body, page_receipt = capture_get(client, _page_url(dataset_id, offset), root, name,
                                                  max_bytes=MAX_BODY_BYTES)
            captures[name] = (page_body, page_receipt)
            page = _json(page_body)
            if not isinstance(page, list) or not page:
                raise ValueError("source_rows_page_empty_before_count")
            pages.append(page)
            offset += PAGE_SIZE
            if offset // PAGE_SIZE > MAX_PAGES:
                raise ValueError("source_page_budget_exceeded")

        count_after_body, count_after_receipt = capture_get(
            client, _count_url(dataset_id), root, "count_after", max_bytes=MAX_BODY_BYTES)
        captures["count_after"] = (count_after_body, count_after_receipt)
        if _source_count(_json(count_after_body)) != count_before:
            raise ValueError("source_population_changed_during_capture")

        rows = normalize_rows(pages, columns, edition, count_before)
        record_sha = _record_sha(rows, columns)
        geometry_sha = _geometry_sha(rows)
        schema_sha = _schema_sha(columns)
        identity = processing_identity(Path(__file__).parents[3])
        version = content_derived_version(record_sha, geometry_sha, schema_sha, identity)
        prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)

        existing = storage.object_snapshot(f"{prefix}/metadata/candidate_ready.json")
        if existing is not None or storage.list_prefix(prefix):
            return _reuse_candidate(storage, prefix, edition, rows, record_sha,
                                    schema_sha, issued, sorted(captures))

        parquet = _parquet(typed_table(rows, columns))
        receipts = {name: dict(receipt) for name, (_, receipt) in sorted(captures.items())}
        manifest = _manifest(edition, view, rows, receipts, parquet, identity, issued, modified)
        audit = _audit(edition, columns, rows, record_sha, geometry_sha, schema_sha, issued, modified)
        ready = {
            "approved": False,
            "version": version,
            "dataset_id": dataset_id,
            "edition_year": edition["year"],
            "feature_count": len(rows),
            "source_record_sha256": record_sha,
            "source_schema_sha256": schema_sha,
            "objects": {f"geoparquet/{FILE_SLUG}.parquet": {"sha256": _sha(parquet), "bytes": len(parquet)}},
            "source_release_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        }
        outputs = {
            f"geoparquet/{FILE_SLUG}.parquet": parquet,
            "metadata/source_manifest.json": _canonical(manifest),
            "metadata/source_audit.json": _canonical(audit),
            "metadata/processing_identity.json": _canonical(identity),
            "metadata/candidate_ready.json": _canonical(ready),
        }
        for name, (body, receipt) in captures.items():
            outputs[f"raw/{name}.body"] = body
            outputs[f"raw/{name}.wire"] = (root / f"{name}.wire").read_bytes()
            outputs[f"raw/{name}.receipt.json"] = _canonical(dict(receipt))
        for name, body in outputs.items():
            write_immutable(storage, f"{prefix}/{name}", body)
            snapshot = storage.object_snapshot(f"{prefix}/{name}")
            if (snapshot is None or snapshot.size != len(body)
                    or storage.sha256_key(f"{prefix}/{name}") != _sha(body)
                    or storage.object_snapshot(f"{prefix}/{name}") != snapshot):
                raise ValueError("candidate_readback_failed:" + name)
    except Exception as exc:
        retained = dict(captures)
        for receipt_path in root.glob("*.receipt.json"):
            name = receipt_path.name.removesuffix(".receipt.json")
            if name in retained:
                continue
            body_path = root / f"{name}.body"
            retained[name] = (body_path.read_bytes() if body_path.exists() else b"", {})
        if retained:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG,
                                          "capture-" + uuid4().hex[:16], retained, type(exc).__name__)
        raise
    finally:
        if owned:
            client.close()
        if ephemeral:
            shutil.rmtree(root, ignore_errors=True)
    return _result(version, edition, rows, issued)


def _source_count(payload: object) -> int:
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        raise ValueError("source_count_response_invalid")
    value = payload[0].get("count")
    if not isinstance(value, str) or not value.isdigit():
        raise ValueError("source_count_response_invalid")
    return int(value)

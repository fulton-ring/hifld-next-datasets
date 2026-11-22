"""EPA FRS EIA-860 SIC codes derived from the national combined FRS archive.

The previous EIA-860 SIC candidate read a 2014 EPA ArcGIS compilation that EPA
no longer maintains. This module builds the same view from EPA's current
combined national CSV download: it filters ``NATIONAL_PROGRAM_FILE.CSV`` to
``PGM_SYS_ACRNM = 'EIA-860'`` and joins every ``NATIONAL_SIC_FILE.CSV`` row on
``REGISTRY_ID``.

The archive is never downloaded whole. It uses the bounded range-request ZIP
reader in :mod:`dagster_hifld.sources.epa_frs_naics_national` for the archive
index and streams only the two required members in bounded compressed ranges,
verifying the producer's declared sizes and CRC-32 before releasing any row.

The candidate is a derived nonspatial draft. Source, schema, baseline, rights
and release acceptance remain ungranted; every approval flag stays false.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import epa_frs_national_archive as archive
from dagster_hifld.sources.capture_common import (
    candidate_prefix,
    content_derived_version,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    write_immutable,
)

DATASET_SLUG = "epa-frs-eia-860-power-generation-facilities-sic-codes"
FILE_SLUG = DATASET_SLUG
PROGRAM_MEMBER_NAME = "NATIONAL_PROGRAM_FILE.CSV"
SIC_MEMBER_NAME = "NATIONAL_SIC_FILE.CSV"
PROGRAM_FILTER_ACRONYM = "EIA-860"
OUTPUT_COLUMNS = ("REGISTRY_ID", "SIC_CODE", "PRIMARY_INDICATOR")
PROGRAM_COLUMNS = (
    "PGM_SYS_ACRNM", "PGM_SYS_ID", "REGISTRY_ID", "PRIMARY_NAME", "LOCATION_ADDRESS",
    "SUPPLEMENTAL_LOCATION", "CITY_NAME", "COUNTY_NAME", "FIPS_CODE", "STATE_CODE",
    "STATE_NAME", "COUNTRY_NAME", "POSTAL_CODE", "FEDERAL_FACILITY_CODE",
    "FEDERAL_AGENCY_CODE", "TRIBAL_LAND_CODE", "TRIBAL_LAND_NAME",
    "LEGISLATIVE_DIST_NUM", "HUC_CODE_8", "HUC_CODE_12", "EPA_REGION_CODE",
    "SITE_TYPE_NAME", "DATA_QUALITY_CODE", "LAST_REPORTED_DATE", "STD_NAME",
    "STD_LOC_ADDRESS", "STD_HOUSE_NUMBER", "STD_STREET_NAME", "STD_BASE_NAME",
    "STD_PREFIX", "STD_SUFFIX", "STD_STYPE_BEFORE", "STD_STYPE_AFTER",
    "STD_POSTAL_CODE", "STD_CITY_NAME", "STD_COUNTY_NAME", "STD_STATE_CODE",
    "STD_COUNTY_FIPS", "STD_COUNTRY", "STD_FULL_ADDRESS", "ADDR_TYPE", "LINK_MTHD",
    "LOCATION_DESCRIPTION", "CREATE_DATE", "UPDATE_DATE", "USER_ID", "SENSITIVE_IND",
    "USER_COMMENT", "PUBLIC_IND", "REFRESH_DATE", "SMALL_BUS_IND", "ENV_JUSTICE_CODE",
    "PARENT_PGM_SYS_ID", "STAND_ALONE_FLAG", "SOURCE_OF_DATA",
)
SIC_COLUMNS = (
    "REGISTRY_ID", "PGM_SYS_ACRNM", "PGM_SYS_ID", "INTEREST_TYPE",
    "SIC_CODE", "PRIMARY_INDICATOR", "CODE_DESCRIPTION",
)

# Dated producer metadata independently read from the archive on 2026-11-17.
# These are fail-closed drift comparators, not source acceptance: a producer
# republish that changes them quarantines the capture for review.
PROGRAM_MEMBER_COMPRESSED_BYTES = 418_434_426
PROGRAM_MEMBER_UNCOMPRESSED_BYTES = 3_052_045_108
PROGRAM_MEMBER_CRC32 = "9ceaa3c4"
SIC_MEMBER_COMPRESSED_BYTES = 25_544_114
SIC_MEMBER_UNCOMPRESSED_BYTES = 257_490_964
SIC_MEMBER_CRC32 = "3082e4b0"
EXPECTED_PROGRAM_ROWS = 7_121_724
EXPECTED_EIA860_PROGRAM_ROWS = 16_469
EXPECTED_EIA860_FACILITIES = 15_906
EXPECTED_SIC_ROWS = 2_277_677
EXPECTED_MATCHED_SIC_ROWS = 19_117
EXPECTED_MATCHED_DISTINCT_TRIPLETS = 7_980
EXPECTED_MATCHED_DISTINCT_FACILITIES = 3_618
EXPECTED_OUTPUT_ROWS = EXPECTED_MATCHED_SIC_ROWS
EXPECTED_OUTPUT_SHA256 = "e045b8c84c1df898584622b716257a6bc1ac16485391f3a7844bae539cd99613"
EXPECTED_SCHEMA_SHA256 = "281939912e7d62d183615e546314a100536c0813d009e1648742724f51a8e103"

# EPA Envirofacts reported this EIA-860 program-facility count on 2026-11-17.
ENVIROFACTS_EIA860_FACILITY_COUNT = 16_469
ENVIROFACTS_OBSERVED_DATE = "2026-11-17"

PUBLISHER = archive.PUBLISHER
MEMBER_CHUNK_BYTES = archive.MEMBER_CHUNK_BYTES
STREAM_WINDOW = archive.STREAM_WINDOW
CAPTURE_DIRECTORY = "/tmp/hifld-native-reconstruction/1050/captures"


class EPAFRSEIA860SICError(ValueError):
    """The national FRS archive cannot produce the pinned EIA-860 SIC view."""


def _canonical(value: object) -> bytes:
    return archive._canonical(value)


def _sha(body: bytes) -> str:
    return archive._sha(body)


def _member_receipt(member_name: str, summary: dict[str, object]) -> dict[str, object]:
    receipts = summary["receipts"]
    return {
        "url": archive.NATIONAL_COMBINED_ZIP_URL,
        "final_url": archive.NATIONAL_COMBINED_ZIP_URL,
        "observed_at": min(receipt["observed_at"] for receipt in receipts),
        "status": 206,
        "bytes": summary["compressed_bytes"],
        "sha256": summary["compressed_sha256"],
        "wire_bytes": summary["compressed_bytes"],
        "wire_sha256": summary["compressed_sha256"],
    }


def _member_input(member_name: str, summary: dict[str, object]) -> dict[str, object]:
    receipt = _member_receipt(member_name, summary)
    return archive._input_provenance(member_name.removesuffix(".CSV").lower(), receipt)


def _stream_member_rows(client: httpx.Client, members: dict[str, dict[str, object]],
                        member_name: str, *, capture_root: Path, capture_name: str,
                        summary: dict[str, object], compressed_bytes: int,
                        uncompressed_bytes: int, crc32: str,
                        captures: dict[str, tuple[bytes, dict[str, object]]],
                        budget: list[int]):
    member = members.get(member_name)
    if member is None:
        raise EPAFRSEIA860SICError("zip_member_missing:" + member_name)
    if (member["method"] != archive.EXPECTED_MEMBER_METHOD or member["flags"] & 0x08
            or member["crc32"] != crc32 or member["compressed"] != compressed_bytes
            or member["uncompressed"] != uncompressed_bytes):
        raise EPAFRSEIA860SICError("zip_member_declaration_drift:" + member_name)
    probe = min(archive.LOCAL_HEADER_PROBE_BYTES,
                archive.EXPECTED_ZIP_TOTAL_BYTES - member["local_header_offset"])
    header_body, header_receipt = archive._capture_range_get(
        client, archive.NATIONAL_COMBINED_ZIP_URL, capture_root, capture_name,
        start=member["local_header_offset"],
        end=member["local_header_offset"] + probe - 1,
        retained_bytes=budget,
    )
    captures[capture_name] = (header_body, header_receipt)
    data_offset = archive._parse_member_local_header(header_body, members, member_name)
    return archive._stream_member_csv(
        client, member_name, data_offset=data_offset,
        compressed_bytes=compressed_bytes, uncompressed_bytes=uncompressed_bytes,
        crc32=crc32, summary=summary, chunk_bytes=MEMBER_CHUNK_BYTES,
        max_workers=STREAM_WINDOW,
    )


def _capture_archive_index(client: httpx.Client, capture_root: Path,
                           captures: dict[str, tuple[bytes, dict[str, object]]],
                           budget: list[int]) -> tuple[dict[str, dict[str, object]],
                                                     dict[str, object]]:
    def get(name: str, **kwargs) -> bytes:
        body, receipt = archive._capture_range_get(
            client, archive.NATIONAL_COMBINED_ZIP_URL, capture_root, name,
            retained_bytes=budget, **kwargs)
        captures[name] = (body, receipt)
        return body

    page_body, page_receipt = archive._capture_range_get(
        client, archive.AUTHORITY_PAGE_URL, capture_root, "authority_page",
        retained_bytes=budget)
    captures["authority_page"] = (page_body, page_receipt)
    if (archive.AUTHORITY_LINK_MARKER not in page_body
            or archive.AUTHORITY_TEXT_MARKER not in page_body):
        raise EPAFRSEIA860SICError("authority_page_link_or_text_drift")
    get("zip_probe", start=0, end=0)
    eocd_tail = min(archive.EOCD_TAIL_BYTES, archive.EXPECTED_ZIP_TOTAL_BYTES)
    tail_body = get("zip_eocd", start=archive.EXPECTED_ZIP_TOTAL_BYTES - eocd_tail,
                    end=archive.EXPECTED_ZIP_TOTAL_BYTES - 1)
    _, cd_size, cd_offset = archive._parse_eocd(tail_body, archive.EXPECTED_ZIP_TOTAL_BYTES)
    cd_body = get("zip_central_directory", start=cd_offset, end=cd_offset + cd_size - 1)
    members = archive._parse_central_directory_members(cd_body)
    if set(members) != set(archive.EXPECTED_ARCHIVE_MEMBER_NAMES):
        raise EPAFRSEIA860SICError("zip_archive_member_roster_drift")
    probe_receipt = captures["zip_probe"][1]
    if (probe_receipt.get("status") != 206
            or probe_receipt.get("zip_total_bytes") != archive.EXPECTED_ZIP_TOTAL_BYTES
            or probe_receipt.get("accept_ranges") != "bytes"):
        raise EPAFRSEIA860SICError("zip_range_support_or_total_drift")
    return members, captures["zip_probe"][1]


def _read_program_facilities(client: httpx.Client, members: dict[str, dict[str, object]],
                             capture_root: Path,
                             captures: dict[str, tuple[bytes, dict[str, object]]],
                             budget: list[int], summary: dict[str, object]):
    lines = _stream_member_rows(
        client, members, PROGRAM_MEMBER_NAME,
        capture_root=capture_root, capture_name="program_member_local_header",
        summary=summary,
        compressed_bytes=PROGRAM_MEMBER_COMPRESSED_BYTES,
        uncompressed_bytes=PROGRAM_MEMBER_UNCOMPRESSED_BYTES,
        crc32=PROGRAM_MEMBER_CRC32, captures=captures, budget=budget,
    )
    try:
        reader = csv.reader(lines)
        header = next(reader)
        if tuple(header) != PROGRAM_COLUMNS:
            raise EPAFRSEIA860SICError("program_csv_schema_changed")
        index = {name: position for position, name in enumerate(header)}
        program_rows = 0
        eia860_rows = 0
        facilities: set[str] = set()
        for row in reader:
            program_rows += 1
            if len(row) != len(header):
                raise EPAFRSEIA860SICError("program_csv_row_field_count_changed")
            if row[index["PGM_SYS_ACRNM"]] == PROGRAM_FILTER_ACRONYM:
                registry_id = row[index["REGISTRY_ID"]]
                if not registry_id.isdigit():
                    raise EPAFRSEIA860SICError("native_id_value_contract_drift:program")
                eia860_rows += 1
                facilities.add(registry_id)
    finally:
        lines.close()
    return program_rows, eia860_rows, facilities


def _read_sic_join(client: httpx.Client, members: dict[str, dict[str, object]],
                   capture_root: Path,
                   captures: dict[str, tuple[bytes, dict[str, object]]],
                   budget: list[int], summary: dict[str, object],
                   facilities: set[str]):
    lines = _stream_member_rows(
        client, members, SIC_MEMBER_NAME,
        capture_root=capture_root, capture_name="sic_member_local_header",
        summary=summary,
        compressed_bytes=SIC_MEMBER_COMPRESSED_BYTES,
        uncompressed_bytes=SIC_MEMBER_UNCOMPRESSED_BYTES,
        crc32=SIC_MEMBER_CRC32, captures=captures, budget=budget,
    )
    try:
        reader = csv.reader(lines)
        header = next(reader)
        if tuple(header) != SIC_COLUMNS:
            raise EPAFRSEIA860SICError("sic_csv_schema_changed")
        index = {name: position for position, name in enumerate(header)}
        sic_rows = 0
        rows: list[list[str]] = []
        matched_facilities: set[str] = set()
        matched_triplets: set[tuple[str, str, str]] = set()
        for row in reader:
            sic_rows += 1
            if len(row) != len(header):
                raise EPAFRSEIA860SICError("sic_csv_row_field_count_changed")
            registry_id = row[index["REGISTRY_ID"]]
            if not registry_id.isdigit():
                raise EPAFRSEIA860SICError("native_id_value_contract_drift:sic")
            if registry_id in facilities:
                triplet = (registry_id, row[index["SIC_CODE"]],
                           row[index["PRIMARY_INDICATOR"]])
                rows.append(list(triplet))
                matched_facilities.add(registry_id)
                matched_triplets.add(triplet)
    finally:
        lines.close()
    return sic_rows, rows, matched_facilities, matched_triplets


def _parquet(rows: list[list[str]]) -> bytes:
    schema = pa.schema(
        [pa.field(name, pa.string(), nullable=True) for name in OUTPUT_COLUMNS],
        metadata={
            b"hifld:geometry": b"absent",
            b"hifld:source_url": archive.NATIONAL_COMBINED_ZIP_URL.encode(),
            b"hifld:source_members": (
                f"{PROGRAM_MEMBER_NAME},{SIC_MEMBER_NAME}".encode()),
            b"hifld:derivation": (
                b"NATIONAL_PROGRAM_FILE.CSV PGM_SYS_ACRNM=EIA-860 joined to "
                b"NATIONAL_SIC_FILE.CSV on REGISTRY_ID"
            ),
        },
    )
    table = pa.Table.from_pydict(
        {name: [row[position] for row in rows] for position, name in enumerate(OUTPUT_COLUMNS)},
        schema=schema,
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", version="2.6")
    return sink.getvalue().to_pybytes()


def _cached_candidate(storage: StagingStorageResource, repo_root: Path, state_key: str,
                      prefix: str, version: str, fingerprint: str,
                      identity: dict[str, object], output_sha: str, schema_sha: str,
                      expected_output_names: set[str],
                      capture_receipts: dict[str, dict[str, object]]) -> dict[str, object]:
    bundle = json.loads(load_first_construction(
        storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if not isinstance(payload, dict):
        raise EPAFRSEIA860SICError("cached_first_candidate_payload_missing")
    archive._verify_cached_candidate(
        storage, repo_root, state_key, prefix, version, fingerprint, identity,
        output_sha, schema_sha, payload.get("objects", {}), payload.get("capture_receipts", {}),
    )
    if set(payload.get("objects", {})) != expected_output_names:
        raise EPAFRSEIA860SICError("cached_candidate_roster_changed")
    audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "row_count": audit["row_count"], "source_record_sha256": output_sha,
        "source_schema_sha256": schema_sha, "refresh_status": "STOPPED",
        "drift_reasons": audit.get("drift_reasons", []), "reused": True,
        "native_columns": list(OUTPUT_COLUMNS),
        "parquet_key": f"{prefix}/unknown/{FILE_SLUG}.parquet",
        "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
        "eia860_program_rows": audit.get("eia860_program_rows"),
        "eia860_facilities": audit.get("eia860_distinct_registry_id"),
        "envirofacts_eia860_facility_count": ENVIROFACTS_EIA860_FACILITY_COUNT,
    }


def capture_epa_frs_eia860_sic(staging_storage: StagingStorageResource, *, version: str,
                               capture_directory: str | None = None) -> dict[str, object]:
    """Capture the national archive and build the EIA-860 SIC candidate."""
    if (not isinstance(staging_storage, StagingStorageResource)
            or isinstance(staging_storage, PublishedStorageResource)):
        raise EPAFRSEIA860SICError("explicit_staging_resource_required")
    if (not staging_storage.use_local or not staging_storage.local_dir
            or staging_storage.bucket or staging_storage.prefix):
        raise EPAFRSEIA860SICError("explicit_isolated_local_staging_required")
    repo_root = Path(__file__).parents[3]
    identity = processing_identity(repo_root)
    fingerprint = _sha(_canonical(identity))
    base = Path(staging_storage.local_dir).absolute()
    base.mkdir(parents=True, exist_ok=True)
    capture_root = (Path(capture_directory).absolute() if capture_directory
                    else Path(CAPTURE_DIRECTORY).absolute() / version)
    capture_root.mkdir(parents=True, exist_ok=True)
    if any(capture_root.iterdir()):
        raise EPAFRSEIA860SICError("capture_directory_not_empty")
    archive._require_disk_space(base)
    archive._require_disk_space(capture_root)

    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    budget: list[int] = []
    program_summary: dict[str, object] = {}
    sic_summary: dict[str, object] = {}
    client = httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/epa-frs-eia860-sic"})
    try:
        members, zip_receipt = _capture_archive_index(client, capture_root, captures, budget)
        program_rows, eia860_rows, facilities = _read_program_facilities(
            client, members, capture_root, captures, budget, program_summary)
        if program_rows != EXPECTED_PROGRAM_ROWS:
            raise EPAFRSEIA860SICError("complete_source_row_count_drift:program")
        if eia860_rows != EXPECTED_EIA860_PROGRAM_ROWS:
            raise EPAFRSEIA860SICError("eia860_program_row_count_drift")
        if len(facilities) != EXPECTED_EIA860_FACILITIES:
            raise EPAFRSEIA860SICError("eia860_facility_count_drift")
        sic_rows, rows, matched_facilities, matched_triplets = _read_sic_join(
            client, members, capture_root, captures, budget, sic_summary, facilities)
        if sic_rows != EXPECTED_SIC_ROWS:
            raise EPAFRSEIA860SICError("complete_source_row_count_drift:sic")
        if len(rows) != EXPECTED_MATCHED_SIC_ROWS:
            raise EPAFRSEIA860SICError("eia860_sic_join_row_count_drift")
        if len(matched_triplets) != EXPECTED_MATCHED_DISTINCT_TRIPLETS:
            raise EPAFRSEIA860SICError("eia860_sic_join_triplet_count_drift")
        if len(matched_facilities) != EXPECTED_MATCHED_DISTINCT_FACILITIES:
            raise EPAFRSEIA860SICError("eia860_sic_join_facility_count_drift")

        output_sha = _sha(_canonical(sorted(rows)))
        schema_sha = _sha(_canonical(list(OUTPUT_COLUMNS)))
        if output_sha != EXPECTED_OUTPUT_SHA256:
            raise EPAFRSEIA860SICError("complete_source_snapshot_drift")
        if schema_sha != EXPECTED_SCHEMA_SHA256:
            raise EPAFRSEIA860SICError("source_schema_drift")

        candidate_version = content_derived_version(output_sha, "", schema_sha, identity)
        prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, candidate_version)
        state_key = f"_construction-state/{DATASET_SLUG}/{candidate_version}.json"
        expected_output_names = {
            f"unknown/{FILE_SLUG}.parquet", "metadata/source_manifest.json",
            "metadata/source_audit.json", "metadata/processing_identity.json",
            "metadata/capture_manifest.json", "metadata/candidate_ready.json",
            *(f"raw/{name}.{suffix}" for name in captures
              for suffix in ("body", "wire", "receipt.json")),
        }
        if staging_storage.object_snapshot(state_key) is not None:
            return _cached_candidate(
                staging_storage, repo_root, state_key, prefix, candidate_version,
                fingerprint, identity, output_sha, schema_sha, expected_output_names,
                {name: receipt for name, (_body, receipt) in captures.items()},
            )

        capture_manifest, capture_manifest_sha = archive._write_capture_manifest(
            capture_root, captures, sum(budget))
        parquet_bytes = _parquet(rows)
        drift_reasons = []
        if eia860_rows != ENVIROFACTS_EIA860_FACILITY_COUNT:
            drift_reasons.append("eia860_program_rows_differ_from_envirofacts")

        member_inputs = [
            _member_input(PROGRAM_MEMBER_NAME, program_summary),
            _member_input(SIC_MEMBER_NAME, sic_summary),
        ]
        manifest = {
            "schema_version": 1, "approved": False,
            "title": "EPA Facility Registry Service - EIA-860 Power Generation Facilities SIC Codes",
            "description": (
                "Derived national SIC view for EPA FRS EIA-860 program facilities. "
                "NATIONAL_PROGRAM_FILE.CSV is filtered to PGM_SYS_ACRNM='EIA-860' and "
                "joined to NATIONAL_SIC_FILE.CSV on REGISTRY_ID. Draft candidate; "
                "release blocked pending maintainer source, schema, baseline, rights "
                "and release review."
            ),
            "publisher": PUBLISHER, "agency": "Environmental Protection Agency",
            "source_url": archive.AUTHORITY_PAGE_URL,
            "license": (
                "Unresolved: EPA's combined CSV download page and archive provide no "
                "explicit reuse license; EPA disclaimers apply and rights review remains "
                "required."
            ),
            "date_issued": None, "date_modified": None, "source_modified": None,
            "source_vintage": None,
            "zip_last_modified_http_date": zip_receipt.get("last_modified"),
            "zip_etag": zip_receipt.get("etag"),
            "zip_last_modified_is_source_release_date": False,
            "processor": "HIFLD Next", "code_commit": identity["code_commit"],
            "native_geometry": "absent", "refresh_status": "STOPPED",
            "publication_approval": "not_established_by_source_capture",
            "capture_manifest_sha256": capture_manifest_sha,
            "inputs": [
                archive._input_provenance(name, receipt)
                for name, (_body, receipt) in ((name, captures[name]) for name in sorted(captures))
            ] + member_inputs,
            "transformations": [{
                "operation": "bounded_http_range_zip_member_stream_and_registry_join",
                "parameters": {
                    "archive_url": archive.NATIONAL_COMBINED_ZIP_URL,
                    "program_member": PROGRAM_MEMBER_NAME,
                    "sic_member": SIC_MEMBER_NAME,
                    "method": (
                        "The full archive is never downloaded. The authority page, "
                        "archive EOCD and central directory are read with bounded HTTP "
                        "range requests. Only the two required members are streamed in "
                        "bounded compressed ranges and inflated with producer CRC-32 and "
                        "size verification."
                    ),
                    "filter": (
                        "NATIONAL_PROGRAM_FILE.CSV rows with PGM_SYS_ACRNM='EIA-860' "
                        "define the facility REGISTRY_ID set."
                    ),
                    "join": (
                        "Every NATIONAL_SIC_FILE.CSV row whose REGISTRY_ID is in that "
                        "set is retained in source order. Duplicate identical "
                        "(REGISTRY_ID, SIC_CODE, PRIMARY_INDICATOR) triplets are preserved."
                    ),
                    "grain": "One row per native SIC assignment for an EIA-860 program facility.",
                    "columns": (
                        "REGISTRY_ID, SIC_CODE and PRIMARY_INDICATOR are preserved as "
                        "delivered strings, including leading zeros."
                    ),
                    "source_release_accepted": False, "freshness_accepted": False,
                    "schema_contract_accepted": False, "id_contract_accepted": False,
                    "rights_accepted": False, "mapping_approved": False,
                    "release_accepted": False,
                },
            }],
            "lineage": [
                "EPA FRS combined national CSV download page (www.epa.gov/frs) links the "
                "national combined ZIP hosted by EPA at ordsext.epa.gov",
                "NATIONAL_PROGRAM_FILE.CSV member: EPA FRS program-system facility records",
                "NATIONAL_SIC_FILE.CSV member: EPA FRS SIC codes per program-system interest",
            ],
        }
        validate_release_provenance(manifest)
        validate_output_policy(manifest)
        audit = {
            "schema_version": 1, "approved": False,
            "row_count": len(rows),
            "source_record_sha256": output_sha,
            "source_schema_sha256": schema_sha,
            "native_columns": list(OUTPUT_COLUMNS),
            "program_member": {
                "name": PROGRAM_MEMBER_NAME,
                "compressed_bytes": program_summary["compressed_bytes"],
                "uncompressed_bytes": program_summary["uncompressed_bytes"],
                "crc32": program_summary["crc32"],
                "sha256": program_summary["compressed_sha256"],
                "rows": program_rows,
                "eia860_rows": eia860_rows,
                "eia860_distinct_registry_id": len(facilities),
                "chunk_receipts": program_summary["receipts"],
            },
            "sic_member": {
                "name": SIC_MEMBER_NAME,
                "compressed_bytes": sic_summary["compressed_bytes"],
                "uncompressed_bytes": sic_summary["uncompressed_bytes"],
                "crc32": sic_summary["crc32"],
                "sha256": sic_summary["compressed_sha256"],
                "rows": sic_rows,
                "matched_rows": len(rows),
                "matched_distinct_triplets": len(matched_triplets),
                "matched_distinct_registry_id": len(matched_facilities),
                "chunk_receipts": sic_summary["receipts"],
            },
            "eia860_program_rows": eia860_rows,
            "eia860_distinct_registry_id": len(facilities),
            "envirofacts_eia860_facility_count": ENVIROFACTS_EIA860_FACILITY_COUNT,
            "envirofacts_observed_date": ENVIROFACTS_OBSERVED_DATE,
            "envirofacts_count_matches_program_rows": (
                eia860_rows == ENVIROFACTS_EIA860_FACILITY_COUNT),
            "capture_mode": "live_source_capture",
            "capture_manifest_sha256": capture_manifest_sha,
            "drift_reasons": drift_reasons,
            "release_accepted": False, "source_release_accepted": False,
            "freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "refresh_status": "STOPPED",
        }
        ready = {
            "approved": False, "version": candidate_version,
            "source_record_sha256": output_sha, "source_schema_sha256": schema_sha,
            "objects": {
                f"unknown/{FILE_SLUG}.parquet": {
                    "bytes": len(parquet_bytes), "sha256": _sha(parquet_bytes)},
            },
            "refresh_status": "STOPPED", "release_accepted": False,
            "source_release_accepted": False, "freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False,
        }
        outputs: dict[str, bytes] = {
            f"unknown/{FILE_SLUG}.parquet": parquet_bytes,
            "metadata/source_manifest.json": _canonical(manifest) + b"\n",
            "metadata/source_audit.json": _canonical(audit) + b"\n",
            "metadata/processing_identity.json": _canonical(identity) + b"\n",
            "metadata/capture_manifest.json": _canonical(capture_manifest) + b"\n",
            "metadata/candidate_ready.json": _canonical(ready) + b"\n",
        }
        for name, (body, receipt) in ((name, captures[name]) for name in sorted(captures)):
            outputs[f"raw/{name}.body"] = body
            outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes()
            outputs[f"raw/{name}.receipt.json"] = _canonical(dict(receipt)) + b"\n"
        if set(outputs) != expected_output_names:
            raise EPAFRSEIA860SICError("candidate_output_roster_changed")
        archive._require_disk_space(base, sum(map(len, outputs.values()))
                                     + archive.STAGING_OVERHEAD_BYTES)
        for name, body in outputs.items():
            key = f"{prefix}/{name}"
            write_immutable(staging_storage, key, body)
            snapshot = staging_storage.object_snapshot(key)
            if (snapshot is None or snapshot.size != len(body)
                    or staging_storage.sha256_key(key) != _sha(body)
                    or staging_storage.object_snapshot(key) != snapshot):
                raise EPAFRSEIA860SICError("candidate_output_physical_readback_failed:" + name)
        payload = {
            "processing_identity": identity,
            "source_record_sha256": output_sha,
            "source_schema_sha256": schema_sha,
            "capture_receipts": {name: receipt for name, (_body, receipt) in captures.items()},
            "objects": {name: {"bytes": len(body), "sha256": _sha(body)}
                        for name, body in outputs.items()},
        }
        seal_ready_candidate(staging_storage, state_key, candidate_version,
                             fingerprint, payload)
        validate_construction_code_identity(
            repo_root, identity["code_commit"], identity["files"], identity=identity)
        return {
            "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG,
            "version": candidate_version, "row_count": len(rows),
            "source_record_sha256": output_sha, "source_schema_sha256": schema_sha,
            "refresh_status": "STOPPED", "drift_reasons": drift_reasons,
            "native_columns": list(OUTPUT_COLUMNS),
            "parquet_key": f"{prefix}/unknown/{FILE_SLUG}.parquet",
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "reused": False, "capture_directory": str(capture_root),
            "capture_manifest_sha256": capture_manifest_sha,
            "capture_mode": "live_source_capture",
            "eia860_program_rows": eia860_rows,
            "eia860_facilities": len(facilities),
            "envirofacts_eia860_facility_count": ENVIROFACTS_EIA860_FACILITY_COUNT,
            "source_release_accepted": False, "freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False,
        }
    except Exception as exc:
        reason = (str(exc) if isinstance(exc, (EPAFRSEIA860SICError, archive.EPAFRSNationalArchiveError))
                  else type(exc).__name__)
        if captures:
            quarantine_incomplete_capture(
                staging_storage, capture_root, DATASET_SLUG, version, captures, reason)
        raise
    finally:
        client.close()

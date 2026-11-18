"""Bounded range-request capture of EPA's FRS national combined NAICS CSV.

EPA's combined national CSV download page links a single national ZIP. This
dataset adapter never downloads that archive. It uses the shared national
archive reader to verify the page, probe the archive with HTTP range requests,
and extract only the ``NATIONAL_NAICS_FILE.CSV`` member, validating the ZIP's
own declared sizes and CRC-32 before parsing.

The candidate is a faithful nonspatial draft: every native CSV column and value
is preserved as delivered. Source, schema, baseline, rights and release
acceptance remain ungranted; every approval flag stays false.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
import zlib
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.capture_common import (
    candidate_prefix,
    content_derived_version,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    write_immutable,
)
from dagster_hifld.sources import epa_frs_national_archive as archive
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

MEMBER_NAME = "NATIONAL_NAICS_FILE.CSV"
FACILITY_MEMBER_NAME = "NATIONAL_FACILITY_FILE.CSV"
DATASET_SLUG = "frs-naics"
FILE_SLUG = DATASET_SLUG

# Dated producer metadata, independently verified 2026-11-09 (see the issue
# #102 handoff). These pins are fail-closed drift comparators, not acceptance:
# any producer republish that changes them quarantines the capture for review.
EXPECTED_MEMBER_COMPRESSED_BYTES = 35_345_985
EXPECTED_MEMBER_UNCOMPRESSED_BYTES = 340_628_836
EXPECTED_MEMBER_CRC32 = "b7a7dfbd"
EXPECTED_SOURCE_ROWS = 2_811_752
EXPECTED_CSV_SHA256 = "4f11df8a96d5646087e70a35ee1cf12f9af5e1cd99cceb0c2fcfe88d8b727c47"
EXPECTED_COLUMNS = (
    "REGISTRY_ID", "PGM_SYS_ACRNM", "PGM_SYS_ID", "INTEREST_TYPE",
    "NAICS_CODE", "PRIMARY_INDICATOR", "CODE_DESCRIPTION",
)
# Historical membership comparator recorded in issue #102 evidence; the prior
# public HIFLD candidate row count was not independently verified here and is
# never treated as an accepted baseline.
PRIOR_CANDIDATE_ROWS = 2_658_755

CSV_PARSE_BATCH_ROWS = 250_000

REQUIRED_CAPTURE_NAMES = frozenset({
    "authority_page", "zip_probe", "zip_eocd", "zip_central_directory", "member_local_header",
})


class EPAFRSNationalNAICSError(ValueError):
    """The EPA combined national NAICS delivery is incomplete or has drifted."""












def _parse_central_directory(body: bytes) -> dict[str, dict[str, object]]:
    members = archive._parse_central_directory_members(body)
    if set(members) != set(archive.EXPECTED_ARCHIVE_MEMBER_NAMES):
        raise EPAFRSNationalNAICSError("zip_archive_member_roster_drift")
    member = members[MEMBER_NAME]
    if (member["method"] != archive.EXPECTED_MEMBER_METHOD or member["flags"] & 0x08
            or member["crc32"] != EXPECTED_MEMBER_CRC32
            or member["compressed"] != EXPECTED_MEMBER_COMPRESSED_BYTES
            or member["uncompressed"] != EXPECTED_MEMBER_UNCOMPRESSED_BYTES):
        raise EPAFRSNationalNAICSError("zip_naics_member_declaration_drift")
    if FACILITY_MEMBER_NAME not in members:
        raise EPAFRSNationalNAICSError("zip_facility_member_missing")
    return members




def _parse_local_header(body: bytes, members: dict[str, dict[str, object]]) -> int:
    return archive._parse_member_local_header(body, members, MEMBER_NAME)


def _decompress_member(chunks: list[bytes], csv_path: Path) -> tuple[str, str, int]:
    """Stream-inflate retained member chunks; verify the producer CRC-32."""
    if sum(len(chunk) for chunk in chunks) != EXPECTED_MEMBER_COMPRESSED_BYTES:
        raise EPAFRSNationalNAICSError("zip_member_compressed_size_drift")
    crc32 = 0
    sha = hashlib.sha256()
    total_out = 0
    decoder = zlib.decompressobj(-15)
    with csv_path.open("wb") as out:
        for chunk in chunks:
            pending = chunk
            while pending:
                data = decoder.decompress(pending, archive.DECOMPRESS_BLOCK_BYTES)
                pending = decoder.unconsumed_tail
                if not data:
                    continue
                crc32 = zlib.crc32(data, crc32)
                sha.update(data)
                total_out += len(data)
                if total_out > EXPECTED_MEMBER_UNCOMPRESSED_BYTES:
                    raise EPAFRSNationalNAICSError("zip_member_uncompressed_size_exceeds_declaration")
                out.write(data)
        tail_data = decoder.flush()
        crc32 = zlib.crc32(tail_data, crc32)
        sha.update(tail_data)
        total_out += len(tail_data)
        out.write(tail_data)
        if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            raise EPAFRSNationalNAICSError("zip_member_deflate_stream_incomplete")
    if (total_out != EXPECTED_MEMBER_UNCOMPRESSED_BYTES
            or f"{crc32 & 0xffffffff:08x}" != EXPECTED_MEMBER_CRC32):
        raise EPAFRSNationalNAICSError("zip_member_content_crc_or_size_drift")
    return sha.hexdigest(), f"{crc32 & 0xffffffff:08x}", total_out






def _parse_member_csv(csv_path: Path) -> tuple[pa.Table, dict[str, object]]:
    """Strict row-by-row CSV parse preserving every native column and value."""
    column_batches: list[list[pa.Array]] = [[] for _ in EXPECTED_COLUMNS]
    pending: list[list[str]] = [[] for _ in EXPECTED_COLUMNS]
    row_count = 0
    batch_rows = 0
    empty_counts = [0] * len(EXPECTED_COLUMNS)
    nonnumeric_registry_ids = 0
    nonnumeric_naics_codes = 0
    registry_ids: set[str] = set()
    naics_codes: set[str] = set()
    acronyms: set[str] = set()
    interest_types: set[str] = set()
    primary_indicators: set[str] = set()
    with csv_path.open("r", newline="", encoding="utf-8") as stream:
        reader = csv.reader(stream, strict=True)
        try:
            header = next(reader)
        except StopIteration:
            raise EPAFRSNationalNAICSError("csv_header_missing") from None
        if tuple(header) != EXPECTED_COLUMNS:
            raise EPAFRSNationalNAICSError("csv_schema_changed")
        for row in reader:
            row_count += 1
            if len(row) != len(EXPECTED_COLUMNS):
                raise EPAFRSNationalNAICSError("csv_row_field_count_changed")
            if not row[0].isdigit():
                nonnumeric_registry_ids += 1
            if not row[4].isdigit():
                nonnumeric_naics_codes += 1
            for index, value in enumerate(row):
                if value == "":
                    empty_counts[index] += 1
                pending[index].append(value)
            registry_ids.add(row[0])
            acronyms.add(row[1])
            interest_types.add(row[3])
            naics_codes.add(row[4])
            primary_indicators.add(row[5])
            batch_rows += 1
            if batch_rows >= CSV_PARSE_BATCH_ROWS:
                for index in range(len(EXPECTED_COLUMNS)):
                    column_batches[index].append(pa.array(pending[index], type=pa.string()))
                    pending[index] = []
                batch_rows = 0
    if pending[0]:
        for index in range(len(EXPECTED_COLUMNS)):
            column_batches[index].append(pa.array(pending[index], type=pa.string()))
    if row_count != EXPECTED_SOURCE_ROWS:
        raise EPAFRSNationalNAICSError("complete_source_row_count_drift")
    if nonnumeric_registry_ids or nonnumeric_naics_codes:
        raise EPAFRSNationalNAICSError("native_id_value_contract_drift")
    schema = pa.schema(
        [pa.field(name, pa.string(), nullable=True) for name in EXPECTED_COLUMNS],
        metadata={
            b"hifld:geometry": b"absent",
            b"hifld:source_url": archive.NATIONAL_COMBINED_ZIP_URL.encode(),
            b"hifld:source_member": MEMBER_NAME.encode(),
        },
    )
    table = pa.Table.from_arrays(
        [pa.chunked_array(column) for column in column_batches], schema=schema)
    stats = {
        "row_count": row_count,
        "empty_counts": dict(zip(EXPECTED_COLUMNS, empty_counts)),
        "nonnumeric_registry_id_rows": nonnumeric_registry_ids,
        "nonnumeric_naics_code_rows": nonnumeric_naics_codes,
        "distinct_registry_id": len(registry_ids),
        "distinct_pgm_sys_acrnm": len(acronyms),
        "distinct_interest_type": len(interest_types),
        "distinct_naics_code": len(naics_codes),
        "distinct_primary_indicator": sorted(primary_indicators),
    }
    return table, stats


def _replay_retained_member(captures: dict[str, tuple[bytes, dict[str, object]]],
                            csv_path: Path) -> tuple[str, str, int]:
    members = _parse_central_directory(captures["zip_central_directory"][0])
    data_offset = _parse_local_header(captures["member_local_header"][0], members)
    member = members[MEMBER_NAME]
    spans: dict[int, bytes] = {}
    for name, (body, receipt) in captures.items():
        if not name.startswith("member_chunk_"):
            continue
        match = re.fullmatch(rf"bytes (\d+)-(\d+)/{archive.EXPECTED_ZIP_TOTAL_BYTES}",
                             receipt.get("content_range") or "")
        if (not match or receipt.get("status") != 206
                or len(body) != int(match.group(2)) - int(match.group(1)) + 1):
            raise EPAFRSNationalNAICSError("retained_member_chunk_receipt_drift:" + name)
        spans[int(match.group(1))] = body
    chunks: list[bytes] = []
    cursor = data_offset
    end = data_offset + member["compressed"]
    while cursor < end:
        if cursor not in spans:
            raise EPAFRSNationalNAICSError("retained_member_chunk_roster_incomplete")
        chunks.append(spans[cursor])
        cursor += len(spans[cursor])
    if cursor != end or sum(len(chunk) for chunk in chunks) > archive.MAX_MEMBER_BYTES_IN_MEMORY:
        raise EPAFRSNationalNAICSError("retained_member_chunk_coverage_drift")
    return _decompress_member(chunks, csv_path)










def capture_epa_frs_national_naics(storage: StagingStorageResource, *, version: str,
                                   capture_directory: str | None = None,
                                   retained_capture_directory: str | None = None,
                                   retained_capture_manifest_sha256: str | None = None) -> dict[str, object]:
    """Bracket a bounded range-request capture of the national NAICS member."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource):
        raise EPAFRSNationalNAICSError("explicit_staging_resource_required")
    if storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise EPAFRSNationalNAICSError("explicit_isolated_local_staging_required")
    if retained_capture_directory and capture_directory:
        raise EPAFRSNationalNAICSError("live_and_retained_capture_directories_conflict")
    if retained_capture_directory and (not isinstance(retained_capture_manifest_sha256, str)
                                       or not re.fullmatch(r"[0-9a-f]{64}", retained_capture_manifest_sha256)):
        raise EPAFRSNationalNAICSError("retained_capture_manifest_digest_required")
    if not retained_capture_directory and retained_capture_manifest_sha256 is not None:
        raise EPAFRSNationalNAICSError("retained_capture_manifest_digest_without_directory")
    base = Path(storage.local_dir).absolute()
    base.mkdir(parents=True, exist_ok=True)
    capture_root = (Path(retained_capture_directory).absolute() if retained_capture_directory else
                    Path(capture_directory or default_capture_dir("hifld-native-reconstruction", "102", "captures")).absolute() / version)
    if retained_capture_directory:
        if not capture_root.is_dir() or capture_root.is_symlink():
            raise EPAFRSNationalNAICSError("retained_capture_directory_invalid")
    else:
        capture_root.mkdir(parents=True, exist_ok=True)
        if any(capture_root.iterdir()):
            raise EPAFRSNationalNAICSError("capture_directory_not_empty")
    archive._require_disk_space(base)
    archive._require_disk_space(capture_root)

    repo_root = Path(__file__).parents[3]
    identity = processing_identity(repo_root)
    fingerprint = _sha(_canonical(identity))
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    capture_budget: list[int] = []
    capture_mode = "live_source_capture"
    quarantined = False
    csv_path = capture_root / MEMBER_NAME
    try:
        if retained_capture_directory:
            capture_mode = "offline_retained_source_replay"
            offline_manifest, offline_receipts, offline_manifest_sha = archive._load_retained_capture(capture_root)
            if offline_manifest_sha != retained_capture_manifest_sha256:
                raise EPAFRSNationalNAICSError("retained_capture_manifest_digest_mismatch")
            captures = dict(offline_receipts)
            csv_sha, observed_crc, observed_bytes = _replay_retained_member(captures, csv_path)
        else:
            client = httpx.Client(follow_redirects=False, timeout=180,
                                  headers={"User-Agent": "hifld-next-datasets/epa-frs-naics"})
            try:
                def get(name: str, **kwargs) -> bytes:
                    body, receipt = archive._capture_range_get(client, archive.NATIONAL_COMBINED_ZIP_URL,
                                                       capture_root, name, retained_bytes=capture_budget,
                                                       **kwargs)
                    captures[name] = (body, receipt)
                    return body

                page_body, page_receipt = archive._capture_range_get(
                    client, archive.AUTHORITY_PAGE_URL, capture_root, "authority_page",
                    retained_bytes=capture_budget)
                captures["authority_page"] = (page_body, page_receipt)
                if archive.AUTHORITY_LINK_MARKER not in page_body or archive.AUTHORITY_TEXT_MARKER not in page_body:
                    raise EPAFRSNationalNAICSError("authority_page_link_or_text_drift")
                get("zip_probe", start=0, end=0)
                eocd_tail = min(archive.EOCD_TAIL_BYTES, archive.EXPECTED_ZIP_TOTAL_BYTES)
                tail_body = get("zip_eocd",
                                start=archive.EXPECTED_ZIP_TOTAL_BYTES - eocd_tail,
                                end=archive.EXPECTED_ZIP_TOTAL_BYTES - 1)
                _, cd_size, cd_offset = archive._parse_eocd(tail_body, archive.EXPECTED_ZIP_TOTAL_BYTES)
                cd_body = get("zip_central_directory",
                              start=cd_offset, end=cd_offset + cd_size - 1)
                members = _parse_central_directory(cd_body)
                member = members[MEMBER_NAME]
                header_probe = min(archive.LOCAL_HEADER_PROBE_BYTES,
                                   archive.EXPECTED_ZIP_TOTAL_BYTES - member["local_header_offset"])
                header_body = get("member_local_header",
                                  start=member["local_header_offset"],
                                  end=member["local_header_offset"] + header_probe - 1)
                data_offset = _parse_local_header(header_body, members)
                if data_offset + member["compressed"] > cd_offset:
                    raise EPAFRSNationalNAICSError("zip_member_range_overlaps_directory")
                chunks: list[bytes] = []
                chunk_start = data_offset
                chunk_index = 0
                while chunk_start < data_offset + member["compressed"]:
                    chunk_end = min(chunk_start + archive.MEMBER_CHUNK_BYTES - 1,
                                    data_offset + member["compressed"] - 1)
                    chunks.append(get(f"member_chunk_{chunk_index:03d}",
                                     start=chunk_start, end=chunk_end))
                    chunk_start = chunk_end + 1
                    chunk_index += 1
                if sum(len(chunk) for chunk in chunks) > archive.MAX_MEMBER_BYTES_IN_MEMORY:
                    raise EPAFRSNationalNAICSError("member_compressed_bytes_exceed_memory_budget")
                csv_sha, observed_crc, observed_bytes = _decompress_member(chunks, csv_path)
            finally:
                client.close()

        if not REQUIRED_CAPTURE_NAMES <= set(captures):
            raise EPAFRSNationalNAICSError("capture_roster_incomplete")
        if not any(name.startswith("member_chunk_") for name in captures):
            raise EPAFRSNationalNAICSError("capture_roster_missing_member_chunks")
        page_body = captures["authority_page"][0]
        if archive.AUTHORITY_LINK_MARKER not in page_body or archive.AUTHORITY_TEXT_MARKER not in page_body:
            raise EPAFRSNationalNAICSError("authority_page_link_or_text_drift")
        probe_receipt = captures["zip_probe"][1]
        if (probe_receipt.get("status") != 206
                or probe_receipt.get("zip_total_bytes") != archive.EXPECTED_ZIP_TOTAL_BYTES
                or probe_receipt.get("accept_ranges") != "bytes"):
            raise EPAFRSNationalNAICSError("zip_range_support_or_total_drift")

        table, csv_stats = _parse_member_csv(csv_path)
        csv_path.unlink(missing_ok=True)
        schema_sha = _sha(_canonical(list(EXPECTED_COLUMNS)))
        if csv_sha != EXPECTED_CSV_SHA256:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, captures,
                                          "complete_source_snapshot_drift")
            quarantined = True
            raise EPAFRSNationalNAICSError("complete_source_snapshot_drift")
        drift_reasons = []
        if csv_stats["row_count"] != PRIOR_CANDIDATE_ROWS:
            drift_reasons.append("source_rows_changed_from_prior_hifld_candidate")

        version = content_derived_version(csv_sha, "", schema_sha, identity)
        prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
        state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
        if retained_capture_directory:
            capture_manifest, capture_manifest_sha = offline_manifest, offline_manifest_sha
        else:
            capture_manifest, capture_manifest_sha = archive._write_capture_manifest(
                capture_root, captures, sum(capture_budget))
        expected_output_names = {f"raw/{name}.{suffix}"
                                 for name in captures for suffix in ("body", "wire", "receipt.json")}
        expected_output_names |= {
            "unknown/" + FILE_SLUG + ".parquet", "metadata/source_manifest.json",
            "metadata/source_audit.json", "metadata/processing_identity.json",
            "metadata/capture_manifest.json", "metadata/candidate_ready.json",
        }
        if storage.object_snapshot(state_key) is not None:
            bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
            payload = bundle.get("payload")
            if not isinstance(payload, dict):
                raise EPAFRSNationalNAICSError("cached_first_candidate_payload_missing")
            archive._verify_cached_candidate(storage, repo_root, state_key, prefix, version, fingerprint,
                                     identity, csv_sha, schema_sha, payload.get("objects", {}),
                                     payload.get("capture_receipts", {}))
            if set(payload["objects"]) != expected_output_names:
                raise EPAFRSNationalNAICSError("cached_candidate_roster_changed")
            return {
                "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "row_count": csv_stats["row_count"], "source_csv_sha256": csv_sha,
                "source_schema_sha256": schema_sha, "refresh_status": "STOPPED",
                "drift_reasons": drift_reasons, "reused": True,
                "native_columns": list(EXPECTED_COLUMNS),
                "parquet_key": f"{prefix}/unknown/{FILE_SLUG}.parquet",
                "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
                "capture_manifest_sha256": capture_manifest_sha,
                "capture_directory": str(capture_root), "capture_mode": capture_mode,
                "source_release_accepted": False, "freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
            }

        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd", version="2.6")
        parquet_bytes = sink.getvalue().to_pybytes()
        parquet_name = f"unknown/{FILE_SLUG}.parquet"
        outputs: dict[str, bytes] = {parquet_name: parquet_bytes}
        for name, (body, receipt) in sorted(captures.items()):
            for suffix, payload_bytes in (("body", body),
                                          ("wire", (capture_root / f"{name}.wire").read_bytes()),
                                          ("receipt.json", _canonical(dict(receipt)) + b"\n")):
                outputs[f"raw/{name}.{suffix}"] = payload_bytes

        zip_receipt = captures["zip_probe"][1]
        manifest = {
            "schema_version": 1, "approved": False,
            "title": "FRS NAICS",
            "description": (
                "EPA Facility Registry Service national NAICS industrial-classification codes, "
                "extracted from the NATIONAL_NAICS_FILE.CSV member of EPA's official combined "
                "national CSV archive. Draft candidate; release blocked pending maintainer "
                "source, schema, baseline, rights and release review."
            ),
            "publisher": archive.PUBLISHER, "agency": "Environmental Protection Agency",
            "source_url": archive.AUTHORITY_PAGE_URL,
            "license": (
                "Unresolved: EPA's combined CSV download page and archive provide no explicit "
                "reuse license; EPA disclaimers apply and rights review remains required."
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
            "inputs": [archive._input_provenance(name, receipt)
                       for name, (_, receipt) in sorted(captures.items())],
            "transformations": [{
                "operation": "bounded_http_range_zip_member_extraction_to_native_parquet",
                "parameters": {
                    "archive_url": archive.NATIONAL_COMBINED_ZIP_URL,
                    "member": MEMBER_NAME,
                    "method": "The full 1.37GB archive is never downloaded. The authority page is "
                              "verified, the archive EOCD and central directory are read with HTTP "
                              "range requests, and only the declared NAICS member's compressed "
                              "bytes are fetched in bounded chunks, inflated with producer "
                              "CRC-32/size verification before a strict CSV parse.",
                    "grain": "Every NATIONAL_NAICS_FILE.CSV data row is retained exactly as "
                             "delivered: one NAICS classification per FRS program-system "
                             "interest record. No filtering, deduplication or row-ID invention.",
                    "columns": "All 7 native columns preserved as delivered strings in source "
                               "order, including leading-zero-capable REGISTRY_ID and "
                               "NAICS_CODE. No substitution, mapping or type coercion.",
                    "facility_cross_reference": "REGISTRY_ID joins to NATIONAL_FACILITY_FILE.CSV, "
                                                "declared in the same archive and not extracted "
                                                "by this dataset.",
                    "source_release_accepted": False, "freshness_accepted": False,
                    "schema_contract_accepted": False, "id_contract_accepted": False,
                    "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
                },
            }],
            "lineage": [
                "EPA FRS combined national CSV download page (www.epa.gov/frs) links the "
                "national combined ZIP hosted by EPA at ordsext.epa.gov",
                "NATIONAL_NAICS_FILE.CSV member: EPA FRS NAICS codes per program-system interest",
                "NATIONAL_FACILITY_FILE.CSV member declared in the same archive provides the "
                "facility-level cross-reference for REGISTRY_ID",
            ],
        }
        validate_release_provenance(manifest)
        validate_output_policy(manifest)
        audit = {
            "schema_version": 1, "approved": False,
            "row_count": csv_stats["row_count"],
            "source_csv_sha256": csv_sha, "source_schema_sha256": schema_sha,
            "source_member_crc32": observed_crc,
            "source_member_uncompressed_bytes": observed_bytes,
            "source_member_compressed_bytes": EXPECTED_MEMBER_COMPRESSED_BYTES,
            "zip_total_bytes": archive.EXPECTED_ZIP_TOTAL_BYTES,
            "capture_mode": capture_mode,
            "capture_manifest_sha256": capture_manifest_sha,
            "csv_stats": csv_stats,
            "native_columns": list(EXPECTED_COLUMNS),
            "drift_reasons": drift_reasons,
            "prior_candidate_rows": PRIOR_CANDIDATE_ROWS,
            "prior_candidate_independently_verified": False,
            "source_count_contract_passed": csv_stats["row_count"] == EXPECTED_SOURCE_ROWS,
            "source_value_contract_passed": csv_sha == EXPECTED_CSV_SHA256,
            "source_schema_contract_passed": True,
            "release_accepted": False, "source_release_accepted": False,
            "freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "refresh_status": "STOPPED",
        }
        ready = {
            "approved": False, "version": version,
            "source_record_sha256": csv_sha, "source_schema_sha256": schema_sha,
            "objects": {name: {"bytes": len(body), "sha256": _sha(body)}
                        for name, body in outputs.items()
                        if name == parquet_name or name == "metadata/capture_manifest.json"},
            "refresh_status": "STOPPED", "release_accepted": False,
            "source_release_accepted": False, "freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False,
        }
        outputs["metadata/source_manifest.json"] = _canonical(manifest) + b"\n"
        outputs["metadata/source_audit.json"] = _canonical(audit) + b"\n"
        outputs["metadata/processing_identity.json"] = _canonical(identity) + b"\n"
        outputs["metadata/capture_manifest.json"] = _canonical(capture_manifest) + b"\n"
        outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
        archive._require_disk_space(base, sum(map(len, outputs.values())) + archive.STAGING_OVERHEAD_BYTES)
        for name, body in outputs.items():
            key = f"{prefix}/{name}"
            write_immutable(storage, key, body)
            snapshot = storage.object_snapshot(key)
            if (snapshot is None or snapshot.size != len(body) or storage.sha256_key(key) != _sha(body)
                    or storage.object_snapshot(key) != snapshot):
                raise EPAFRSNationalNAICSError("candidate_output_physical_readback_failed:" + name)
        descriptors = {name: {"bytes": len(body), "sha256": _sha(body)}
                       for name, body in outputs.items()}
        seal_ready_candidate(storage, state_key, version, fingerprint, {
            "processing_identity": identity,
            "source_record_sha256": csv_sha,
            "source_schema_sha256": schema_sha,
            "capture_receipts": {name: receipt for name, (_, receipt) in captures.items()},
            "objects": descriptors,
        })
        validate_construction_code_identity(repo_root, identity["code_commit"], identity["files"],
                                            identity=identity)
        return {
            "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "row_count": csv_stats["row_count"], "source_csv_sha256": csv_sha,
            "source_schema_sha256": schema_sha, "refresh_status": "STOPPED",
            "drift_reasons": drift_reasons, "native_columns": list(EXPECTED_COLUMNS),
            "parquet_key": f"{prefix}/{parquet_name}",
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "reused": False, "capture_directory": str(capture_root),
            "capture_manifest_sha256": capture_manifest_sha, "capture_mode": capture_mode,
            "source_release_accepted": False, "freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        }
    except Exception as exc:
        reason = str(exc) if isinstance(exc, EPAFRSNationalNAICSError) else type(exc).__name__
        if not quarantined:
            for receipt_path in sorted(capture_root.glob("*.receipt.json")) if capture_root.exists() else []:
                name = receipt_path.name.removesuffix(".receipt.json")
                if name in captures:
                    continue
                body_path = capture_root / f"{name}.body"
                try:
                    receipt = json.loads(receipt_path.read_bytes())
                    if isinstance(receipt, dict) and body_path.is_file():
                        captures.setdefault(name, (body_path.read_bytes(), receipt))
                except (OSError, ValueError):
                    continue
            if captures:
                quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                              captures, reason)
        raise
    finally:
        csv_path.unlink(missing_ok=True)

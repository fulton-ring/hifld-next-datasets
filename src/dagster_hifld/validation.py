"""Full-population, disk-backed comparisons of explicitly typed record streams.

The JSONL adapter consumes one GeoJSON Feature per line. It does not establish
upstream completeness or acceptance: those require separately reviewed evidence.
No normalization, geometry repair, CRS inference or numeric coercion is applied.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from pyproj import CRS
from pyproj.exceptions import CRSError
from shapely.errors import ShapelyError
from shapely.geometry import shape


@dataclass(frozen=True)
class ComparisonLimits:
    max_record_bytes: int = 32 * 1024 * 1024
    max_scratch_bytes: int = 4 * 1024 * 1024 * 1024
    sqlite_cache_kib: int = 8192


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def typed_value(value: Any) -> Any:
    """Lossless type tagging; integer keys never pass through SQLite numbers."""
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["boolean", value]
    if isinstance(value, int):
        return ["integer", str(value)]
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Non-finite decimal")
        return ["decimal", str(value)]
    if isinstance(value, str):
        return ["string", value]
    if isinstance(value, list):
        return ["array", [typed_value(item) for item in value]]
    if isinstance(value, dict):
        return ["object", [[key, typed_value(value[key])] for key in sorted(value)]]
    raise ValueError(f"Unsupported value type: {type(value).__name__}")


def _encode(value: Any) -> bytes:
    return json.dumps(typed_value(value), ensure_ascii=False, separators=(",", ":")).encode()


def _reject_constant(value: str) -> None:
    raise ValueError(f"Non-finite JSON constant: {value}")


def _no_duplicate_members(pairs: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON member: {key}")
        result[key] = value
    return result


def _record_errors(record: dict, contract: dict) -> list[str]:
    props = record.get("properties")
    if record.get("type") != "Feature" or not isinstance(props, dict):
        return ["invalid_feature"]
    errors = []
    # Foreign members (including Feature.id and bbox) require an explicit future
    # adapter contract; silently discarding them would lose source identifiers.
    if set(record) != {"type", "properties", "geometry"}:
        errors.append("unsupported_feature_members")
    columns = contract["columns"]
    if set(props) != set(columns):
        errors.append("schema_columns")
    for name, rule in columns.items():
        value = props.get(name)
        if value is None:
            if not rule["nullable"]:
                errors.append(f"required_null:{name}")
        elif typed_value(value)[0] != rule["type"]:
            errors.append(f"schema_type:{name}")
    geometry = record.get("geometry")
    if contract["geometry"]:
        if geometry is None:
            errors.append("null_geometry")
        else:
            try:
                _validate_geometry(geometry)
                geom = shape(geometry)
                if geom.is_empty or not geom.is_valid:
                    errors.append("empty_or_invalid_geometry")
                if CRS.from_user_input(contract["crs"]).equals(CRS.from_epsg(4326)):
                    xmin, ymin, xmax, ymax = geom.bounds
                    if xmin < -180 or xmax > 180 or ymin < -90 or ymax > 90:
                        errors.append("coordinates_outside_declared_crs_domain")
            except (ValueError, TypeError, KeyError, OverflowError, ShapelyError, CRSError):
                errors.append("invalid_geometry")
    elif geometry is not None:
        errors.append("unexpected_geometry")
    return errors


def _validate_geometry(geometry: dict) -> None:
    """Validate GeoJSON structure before Shapely can coerce or repair values."""
    if not isinstance(geometry, dict):
        raise ValueError("Geometry must be an object")
    kind = geometry.get("type")
    if kind == "GeometryCollection":
        if set(geometry) != {"type", "geometries"} or not isinstance(geometry["geometries"], list):
            raise ValueError("Unsupported geometry collection members")
        for member in geometry["geometries"]:
            _validate_geometry(member)
        return
    depths = {"Point": 0, "MultiPoint": 1, "LineString": 1, "MultiLineString": 2, "Polygon": 2, "MultiPolygon": 3}
    if kind not in depths or set(geometry) != {"type", "coordinates"}:
        raise ValueError("Unsupported geometry type or members")

    def coordinates(value: Any, depth: int) -> None:
        if not isinstance(value, list):
            raise ValueError("Coordinates must be arrays")
        if depth:
            if not value:
                raise ValueError("Empty coordinate array")
            for member in value:
                coordinates(member, depth - 1)
        elif len(value) not in {2, 3} or any(isinstance(n, bool) or not isinstance(n, (int, Decimal))
                                          or (isinstance(n, Decimal) and not n.is_finite()) for n in value):
            raise ValueError("Positions require finite numeric coordinates")
    coordinates(geometry["coordinates"], depths[kind])
    polygons = [geometry["coordinates"]] if kind == "Polygon" else geometry["coordinates"] if kind == "MultiPolygon" else []
    for polygon in polygons:
        for ring in polygon:
            if len(ring) < 4 or ring[0] != ring[-1]:
                raise ValueError("Polygon rings must be explicitly closed")


def compare_full_rows(
    baseline: Path,
    candidate: Path,
    contract: dict,
    output_dir: Path,
    *,
    baseline_sha256: str,
    candidate_sha256: str,
    baseline_reference: dict,
    limits: ComparisonLimits = ComparisonLimits(),
) -> dict:
    """Compare every record, writing complete typed differences to a JSONL sidecar.

    Byte pins are mandatory, but do not mean the baseline has been accepted.
    Differences need a decision; malformed data or comparison errors fail closed.
    Scratch SQLite holds full keys, values, geometries and coverage groups.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "schema_version": 1,
        "adapter": "typed-geojson-feature-jsonl-v1",
        "status": "blocked",
        "baseline": baseline_reference,
        "baseline_sha256": baseline_sha256,
        "candidate_sha256": candidate_sha256,
        "contract_sha256": hashlib.sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest(),
        "reason_codes": [],
        "populations": {},
        "differences": {},
        "acceptance": "not_established_by_comparison",
        "comparator_sha256": sha256_file(Path(__file__)),
    }
    # Never leave a previous passing report visible while a rerun is unfinished.
    (output_dir / "baseline_comparison.json").write_text(json.dumps({
        **report, "reason_codes": ["comparison_started_not_completed"],
    }, indent=2) + "\n")
    sidecar = output_dir / "baseline_differences.jsonl"
    with sidecar.open("w", encoding="utf-8") as diffs:
        def emit(kind: str, **details: Any) -> None:
            diffs.write(json.dumps({"kind": kind, **details}, ensure_ascii=False) + "\n")

        try:
            if not baseline_reference or not contract.get("id_fields"):
                raise ValueError("Missing baseline identity or stable key contract")
            baseline_input = baseline_reference.get("comparison_input", {})
            if (baseline_input.get("adapter") != report["adapter"]
                    or baseline_input.get("sha256") != baseline_sha256
                    or baseline_input.get("bytes") != baseline.stat().st_size):
                raise ValueError("Comparison input does not match independently pinned baseline reference")
            columns = contract["columns"]
            if any(name not in columns for name in [*contract["id_fields"], *contract["coverage_fields"]]):
                raise ValueError("Contract key/coverage field absent from schema")
            if contract["geometry"] and not contract.get("crs"):
                raise ValueError("Unknown CRS")
            if contract["geometry"]:
                CRS.from_user_input(contract["crs"])
            if baseline.resolve() == candidate.resolve():
                raise ValueError("Candidate cannot be its own baseline")
            identities = {}
            for path, expected in ((baseline, baseline_sha256), (candidate, candidate_sha256)):
                identities[path] = path.stat()
                if sha256_file(path) != expected:
                    raise ValueError("Input checksum mismatch")
            report["baseline_input_bytes"] = baseline.stat().st_size
            report["candidate_input_bytes"] = candidate.stat().st_size
            with tempfile.TemporaryDirectory(dir=output_dir) as scratch:
                db_path = Path(scratch) / "comparison.sqlite"
                with sqlite3.connect(db_path) as db:
                    db.execute(f"PRAGMA cache_size=-{limits.sqlite_cache_kib}")
                    db.execute("PRAGMA temp_store=FILE")
                    db.execute("PRAGMA journal_mode=OFF")
                    db.execute("CREATE TABLE rows (side TEXT, key BLOB, props BLOB, geom BLOB)")
                    db.execute("CREATE INDEX row_keys ON rows(side,key)")
                    db.execute("CREATE TABLE coverage(side TEXT, field TEXT, bucket BLOB)")
                    db.execute("CREATE INDEX coverage_keys ON coverage(field,bucket,side)")
                    page_size = db.execute("PRAGMA page_size").fetchone()[0]

                    def quota() -> None:
                        diffs.flush()
                        allocated = db.execute("PRAGMA page_count").fetchone()[0] * page_size
                        size = sidecar.stat().st_size + max(allocated, sum(p.stat().st_size for p in Path(scratch).iterdir()))
                        if size > limits.max_scratch_bytes:
                            raise ValueError("Scratch quota exceeded")

                    for side, path in (("baseline", baseline), ("candidate", candidate)):
                        counts = {"rows": 0, "null_keys": 0, "invalid_records": 0, "duplicate_keys": 0}
                        with path.open("rb") as stream:
                            while line := stream.readline(limits.max_record_bytes + 1):
                                if len(line) > limits.max_record_bytes:
                                    raise ValueError("Record byte limit exceeded")
                                record = json.loads(line, parse_float=Decimal, parse_constant=_reject_constant,
                                                    object_pairs_hook=_no_duplicate_members)
                                if not isinstance(record, dict):
                                    raise ValueError("Record must be an object")
                                counts["rows"] += 1
                                errors = _record_errors(record, contract)
                                if errors:
                                    counts["invalid_records"] += 1
                                    emit("invalid_record", side=side, row=counts["rows"], errors=errors)
                                props = record.get("properties")
                                if not isinstance(props, dict):
                                    raise ValueError("Missing properties")
                                values = [props.get(name) for name in contract["id_fields"]]
                                if any(value is None for value in values):
                                    counts["null_keys"] += 1
                                key = _encode(values)
                                db.execute("INSERT INTO rows VALUES (?,?,?,?)", (side, key, _encode(props), _encode(record.get("geometry"))))
                                for field in contract["coverage_fields"]:
                                    db.execute("INSERT INTO coverage VALUES (?,?,?)", (side, field, _encode(props.get(field))))
                                quota()
                        db.commit()
                        for key, count in db.execute("SELECT key,COUNT(*) FROM rows WHERE side=? GROUP BY key HAVING COUNT(*)>1", (side,)):
                            counts["duplicate_keys"] += count - 1
                            emit("duplicate_key", side=side, key=json.loads(key), count=count)
                        report["populations"][side] = counts
                        if not counts["rows"] and not contract.get("allow_empty", False):
                            raise ValueError("Empty population requires explicit contract")
                        quota()
                    changes = {"added": 0, "removed": 0, "attributes": 0, "geometry": 0, "coverage_buckets": 0}
                    for side, other, kind in (("baseline", "candidate", "removed"), ("candidate", "baseline", "added")):
                        for (key,) in db.execute("SELECT DISTINCT a.key FROM rows a WHERE a.side=? AND NOT EXISTS (SELECT 1 FROM rows b WHERE b.side=? AND b.key=a.key)", (side, other)):
                            changes[kind] += 1
                            emit(kind, key=json.loads(key))
                            quota()
                    # Duplicate keys are invalid and excluded from retained matching.
                    query = """SELECT a.key,a.props,b.props,a.geom,b.geom FROM rows a JOIN rows b ON a.key=b.key
                        WHERE a.side='baseline' AND b.side='candidate'
                        AND (SELECT COUNT(*) FROM rows x WHERE x.side='baseline' AND x.key=a.key)=1
                        AND (SELECT COUNT(*) FROM rows x WHERE x.side='candidate' AND x.key=a.key)=1"""
                    for key, left, right, left_geom, right_geom in db.execute(query):
                        if left != right:
                            changes["attributes"] += 1
                            emit("attributes", key=json.loads(key), baseline=json.loads(left), candidate=json.loads(right))
                        if left_geom != right_geom:
                            changes["geometry"] += 1
                            emit("geometry", key=json.loads(key), baseline=json.loads(left_geom), candidate=json.loads(right_geom))
                        quota()
                    query = """SELECT field,bucket,SUM(CASE WHEN side='baseline' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN side='candidate' THEN 1 ELSE 0 END) FROM coverage
                        GROUP BY field,bucket HAVING SUM(CASE WHEN side='baseline' THEN 1 ELSE -1 END) != 0"""
                    for field, bucket, left, right in db.execute(query):
                        changes["coverage_buckets"] += 1
                        emit("coverage", field=field, bucket=json.loads(bucket), baseline=left, candidate=right)
                        quota()
                    report["differences"] = changes
                    for path, expected in ((baseline, baseline_sha256), (candidate, candidate_sha256)):
                        if path.stat() != identities[path] or sha256_file(path) != expected:
                            raise ValueError("Input changed during comparison")
                    invalid = any(c["invalid_records"] or c["null_keys"] or c["duplicate_keys"] for c in report["populations"].values())
                    report["status"] = "fail" if invalid else "needs_decision" if any(changes.values()) else "pass"
                    if invalid:
                        report["reason_codes"].append("invalid_schema_key_or_geometry")
                    elif any(changes.values()):
                        report["reason_codes"].append("full_population_changes_require_acceptance")
                    quota()
        except (ValueError, OSError, sqlite3.Error, KeyError, TypeError, OverflowError, ShapelyError, CRSError, RecursionError) as exc:
            report["status"] = "blocked"
            report["reason_codes"].append(f"comparison_incomplete:{type(exc).__name__}:{exc}")
    report["sidecar"] = {"path": sidecar.name, "sha256": sha256_file(sidecar), "bytes": sidecar.stat().st_size}
    (output_dir / "baseline_comparison.json").write_text(json.dumps(report, indent=2) + "\n")
    (output_dir / "baseline_comparison.md").write_text(
        f"Comparison: **{report['status']}**. Acceptance is separate.\n\n"
        f"Population totals: `{json.dumps(report['populations'])}`\n\n"
        f"Full differences: `{json.dumps(report['differences'])}`\n\n"
        f"Reasons: `{json.dumps(report['reason_codes'])}`\n"
    )
    return report

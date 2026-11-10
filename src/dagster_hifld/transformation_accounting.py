"""Executable accounting for constructed typed-record outputs.

Pipelines may union many pinned files, paginate APIs, join tables, filter,
deduplicate, aggregate or project before publishing. This adapter re-runs a
declared, bounded plan over complete pinned input streams and requires the
recomputed terminal stream to equal the pinned candidate exactly: partition
rosters must be complete, join cardinality/unmatched keys are counted, every
dropped row carries an explicit ledger reason, and every output row keeps
row/source lineage. Re-running the plan is custody accounting only; it never
approves a source, baseline, schema or release. Content acceptance still
requires the independent full-row baseline comparison and release decision.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import tempfile
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from dagster_hifld.evidence_io import new_evidence_stream, validate_evidence_directory, write_json_atomic
from dagster_hifld.validation import _no_duplicate_members, sha256_file, typed_value

ADAPTER = "transformation-accounting-v1"
_OPS = {"union", "join", "filter", "deduplicate", "aggregate", "project"}
_JOIN_TYPES = {"inner", "left", "right", "full"}
_FUNCTIONS = {"count", "min", "max", "sum"}
_NAME = r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}"
_UNKEYED = b"null"


@dataclass(frozen=True)
class AccountingLimits:
    max_streams: int = 256
    max_steps: int = 256
    max_union_inputs: int = 64
    max_key_paths: int = 8
    max_path_segments: int = 8
    max_properties: int = 64
    max_aggregations: int = 16
    max_record_bytes: int = 32 * 1024 * 1024
    max_ledger_bytes: int = 64 * 1024 * 1024
    max_plan_bytes: int = 1024 * 1024
    max_scratch_bytes: int = 4 * 1024 * 1024 * 1024
    max_unmatched_key_samples: int = 1000
    sqlite_cache_kib: int = 8192


def _json_bytes(value: Any) -> bytes:
    return json.dumps(typed_value(value), sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()


def _digest(value: Any) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _reject_constant(value: str) -> None:
    raise ValueError(f"Non-finite JSON constant: {value}")


def _code_digest() -> str:
    return _digest({path.name: sha256_file(path) for path in
                    (Path(__file__), Path(__file__).with_name("validation.py"), Path(__file__).with_name("evidence_io.py"))})


def _stream_name(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(_NAME, value):
        raise ValueError("Invalid stream name")
    return value


def _relative_key(value: object) -> str:
    if not isinstance(value, str) or not value.endswith(".jsonl"):
        raise ValueError("Object keys must be relative JSONL paths")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts) or not all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", part) for part in parts):
        raise ValueError("Unsafe relative object key")
    return value


def _sha_pin(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("Invalid object checksum")
    return value


def _paths(value: object, *, name: str, limits: AccountingLimits) -> list[list[str]]:
    if not isinstance(value, list) or not 1 <= len(value) <= limits.max_key_paths:
        raise ValueError(f"Invalid {name} paths")
    result = []
    for path in value:
        if not isinstance(path, list) or not 1 <= len(path) <= limits.max_path_segments:
            raise ValueError(f"Invalid {name} path")
        if any(not isinstance(segment, str) or not segment for segment in path):
            raise ValueError(f"Invalid {name} path segment")
        result.append(list(path))
    return result


def _resolve(record: Any, path: list[str]) -> Any:
    """Walk explicit paths; digit segments index arrays, others name keys."""
    for segment in path:
        if isinstance(record, dict):
            if segment not in record:
                return None
            record = record[segment]
        elif isinstance(record, list) and segment.isdigit():
            index = int(segment)
            if index >= len(record):
                return None
            record = record[index]
        else:
            return None
    return record


def _decimal_parts(value: Decimal) -> tuple[int, int]:
    """Exact (mantissa, scale) parts of a finite decimal; no context is used."""
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError("Non-finite decimal in aggregation field")
    mantissa = 0
    for digit in digits:
        mantissa = mantissa * 10 + digit
    return (-mantissa if sign else mantissa), -exponent


def _sum_parts(total: tuple[int, int], part: tuple[int, int]) -> tuple[int, int]:
    """Exact integer mantissa/scale addition; aligns scales without rounding."""
    total_mantissa, total_scale = total
    part_mantissa, part_scale = part
    if part_scale > total_scale:
        total_mantissa *= 10 ** (part_scale - total_scale)
        total_scale = part_scale
    elif total_scale > part_scale:
        part_mantissa *= 10 ** (total_scale - part_scale)
    return total_mantissa + part_mantissa, total_scale


def _parts_to_decimal(total: tuple[int, int]) -> Decimal:
    """Rebuild the exact decimal from integer parts via the lossless constructor."""
    mantissa, scale = total
    digits = tuple(int(digit) for digit in str(abs(mantissa)))
    return Decimal((1 if mantissa < 0 else 0, digits or (0,), -scale))


def _untyped(value: Any) -> Any:
    """Decode the lossless typed encoding back to its original Python value."""
    if not isinstance(value, list) or not value:
        raise ValueError("Malformed typed record")
    kind = value[0]
    if kind == "null" and len(value) == 1:
        return None
    if kind in {"boolean", "integer", "decimal", "string"} and len(value) == 2:
        if kind == "boolean":
            return bool(value[1])
        if kind == "integer":
            return int(value[1])
        if kind == "decimal":
            return Decimal(value[1])
        return value[1]
    if kind == "array" and len(value) == 2 and isinstance(value[1], list):
        return [_untyped(item) for item in value[1]]
    if kind == "object" and len(value) == 2 and isinstance(value[1], list):
        result: dict[str, Any] = {}
        for key, member in value[1]:
            if not isinstance(key, str) or key in result:
                raise ValueError("Malformed typed record member")
            result[key] = _untyped(member)
        return result
    raise ValueError("Malformed typed record")


def _key_is_null(key: bytes) -> bool:
    return any(member == ["null"] for member in json.loads(key)) if key != _UNKEYED else False


def _validate_plan(plan: dict, stream_names: set[str], limits: AccountingLimits) -> None:
    if (not isinstance(plan, dict) or plan.get("schema_version") != 1 or not isinstance(plan.get("steps"), list)
            or not 1 <= len(plan["steps"]) <= limits.max_steps or set(plan) - {"schema_version", "steps", "output"}
            or not isinstance(plan.get("output"), str)):
        raise ValueError("Invalid accounting plan")
    available = set(stream_names)
    consumed: dict[str, int] = {}
    for step in plan["steps"]:
        if not isinstance(step, dict) or step.get("op") not in _OPS:
            raise ValueError("Unsupported accounting step")
        op = step["op"]
        output = _stream_name(step.get("output_stream"))
        if output in available:
            raise ValueError("Duplicate output stream")
        if op == "union":
            if set(step) - {"op", "input_streams", "output_stream"}:
                raise ValueError("Unknown union parameters")
            declared = step.get("input_streams")
            if (not isinstance(declared, list) or not 1 <= len(declared) <= limits.max_union_inputs
                    or len(declared) != len(set(declared))):
                raise ValueError("Invalid union input streams")
            inputs = [_stream_name(name) for name in declared]
        elif op == "join":
            if set(step) - {"op", "left_stream", "right_stream", "join_type", "left_keys", "right_keys", "output_stream"}:
                raise ValueError("Unknown join parameters")
            if step.get("join_type") not in _JOIN_TYPES:
                raise ValueError("Invalid join type")
            inputs = [_stream_name(step.get("left_stream")), _stream_name(step.get("right_stream"))]
            _paths(step.get("left_keys"), name="join key", limits=limits)
            _paths(step.get("right_keys"), name="join key", limits=limits)
        elif op == "filter":
            if set(step) - {"op", "input_stream", "output_stream", "ledger"}:
                raise ValueError("Unknown filter parameters")
            inputs = [_stream_name(step.get("input_stream"))]
            ledger = step.get("ledger")
            if (not isinstance(ledger, dict) or set(ledger) != {"path", "sha256", "bytes"}
                    or type(ledger["bytes"]) is not int or ledger["bytes"] < 0
                    or ledger["bytes"] > limits.max_ledger_bytes):
                raise ValueError("Filter step requires an explicitly pinned ledger")
            _relative_key(ledger["path"])
            _sha_pin(ledger["sha256"])
        elif op == "deduplicate":
            if set(step) - {"op", "input_stream", "output_stream", "key_paths", "rule"} or step.get("rule") not in {"first", "last"}:
                raise ValueError("Invalid deduplication parameters")
            inputs = [_stream_name(step.get("input_stream"))]
            _paths(step.get("key_paths"), name="deduplication key", limits=limits)
        elif op == "aggregate":
            if set(step) - {"op", "input_stream", "output_stream", "key_paths", "aggregations"}:
                raise ValueError("Unknown aggregation parameters")
            inputs = [_stream_name(step.get("input_stream"))]
            _paths(step.get("key_paths"), name="aggregate key", limits=limits)
            aggregations = step.get("aggregations")
            if (not isinstance(aggregations, list) or not 1 <= len(aggregations) <= limits.max_aggregations
                    or any(not isinstance(item, dict) for item in aggregations)):
                raise ValueError("Invalid aggregations")
            names = set()
            for item in aggregations:
                if set(item) - {"name", "function", "field_path"} or item.get("function") not in _FUNCTIONS:
                    raise ValueError("Invalid aggregation")
                name = item.get("name")
                if (not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_]{0,63}", name)
                        or name in names | {"key", "rows"}):
                    raise ValueError("Invalid aggregation name")
                names.add(name)
                if item["function"] == "count":
                    if "field_path" in item:
                        raise ValueError("Count aggregation takes no field path")
                elif not isinstance(item.get("field_path"), list) or not 1 <= len(item["field_path"]) <= limits.max_path_segments:
                    raise ValueError("Invalid aggregation field path")
        else:
            if set(step) - {"op", "input_stream", "output_stream", "properties", "geometry_path"}:
                raise ValueError("Unknown projection parameters")
            inputs = [_stream_name(step.get("input_stream"))]
            properties = step.get("properties")
            if (not isinstance(properties, list) or not 1 <= len(properties) <= limits.max_properties
                    or any(not isinstance(item, dict) or set(item) != {"name", "path"} for item in properties)):
                raise ValueError("Invalid projection properties")
            names = set()
            for item in properties:
                if (not isinstance(item["name"], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_]{0,63}", item["name"])
                        or item["name"] in names or not isinstance(item["path"], list)
                        or not 1 <= len(item["path"]) <= limits.max_path_segments
                        or any(not isinstance(segment, str) or not segment for segment in item["path"])):
                    raise ValueError("Invalid projection property")
                names.add(item["name"])
            if "geometry_path" in step and (not isinstance(step["geometry_path"], list) or not step["geometry_path"]
                                            or any(not isinstance(segment, str) or not segment for segment in step["geometry_path"])):
                raise ValueError("Invalid projection geometry path")
        if any(name not in available for name in inputs):
            raise ValueError("Step input stream not declared or produced earlier")
        for name in inputs:
            consumed[name] = consumed.get(name, 0) + 1
        available.add(output)
    terminal = _stream_name(plan["output"])
    if terminal != plan["steps"][-1].get("output_stream"):
        raise ValueError("Plan output must be the final step's output stream")
    if any(consumed.get(name, 0) != 1 for name in available if name != terminal):
        raise ValueError("Every non-terminal stream must be consumed exactly once")


def _iter_records(path: Path, *, limits: AccountingLimits):
    """Strictly parse one pinned JSONL stream, one canonical record at a time."""
    with path.open("rb") as stream:
        while line := stream.readline(limits.max_record_bytes + 1):
            if len(line) > limits.max_record_bytes:
                raise ValueError("Record byte limit exceeded")
            record = json.loads(line, parse_float=Decimal, parse_constant=_reject_constant,
                                object_pairs_hook=_no_duplicate_members)
            yield _json_bytes(record)


def account_construction(spec: dict, plan: dict, candidate: dict, output_dir: Path,
                         *, limits: AccountingLimits = AccountingLimits()) -> dict:
    """Re-run a declared construction plan; the terminal must equal the candidate.

    Every JSONL object under the roster prefix must be pinned as an input
    stream, filter ledger or the terminal candidate, so unioned shards or join
    tables cannot silently omit a partition. The recomputed terminal stream
    must match the candidate row-by-row; accounting blocks otherwise and never
    establishes acceptance.
    """
    if (not isinstance(spec, dict) or spec.get("schema_version") != 1 or not isinstance(spec.get("root"), str)
            or set(spec) - {"schema_version", "root", "roster_prefix", "streams"}
            or not isinstance(spec.get("streams"), list) or not 1 <= len(spec["streams"]) <= limits.max_streams):
        raise ValueError("Invalid accounting input specification")
    roster_prefix = spec.get("roster_prefix", ".")
    if not isinstance(roster_prefix, str) or not roster_prefix or any(
            part in {"", ".", ".."} for part in roster_prefix.split("/")):
        raise ValueError("Invalid roster prefix")
    root = Path(spec["root"]).resolve()
    streams: dict[str, dict] = {}
    pinned: dict[str, str] = {}
    for stream in spec["streams"]:
        if not isinstance(stream, dict) or set(stream) != {"name", "path", "sha256", "bytes", "rows"}:
            raise ValueError("Invalid stream pin")
        name = _stream_name(stream["name"])
        key = _relative_key(stream["path"])
        if name in streams or key in pinned:
            raise ValueError("Duplicate stream name or object key")
        if type(stream["bytes"]) is not int or stream["bytes"] < 0 or type(stream["rows"]) is not int or stream["rows"] < 0:
            raise ValueError("Invalid stream pin values")
        streams[name] = {"path": key, "sha256": _sha_pin(stream["sha256"]), "bytes": stream["bytes"], "rows": stream["rows"]}
        pinned[key] = streams[name]["sha256"]
    if not isinstance(candidate, dict) or set(candidate) != {"path", "sha256", "bytes", "rows"}:
        raise ValueError("Invalid candidate pin")
    candidate_key = _relative_key(candidate["path"])
    if (candidate_key in pinned or type(candidate["bytes"]) is not int or candidate["bytes"] < 0
            or type(candidate["rows"]) is not int or candidate["rows"] < 0):
        raise ValueError("Invalid candidate pin values")
    candidate_sha = _sha_pin(candidate["sha256"])
    pinned[candidate_key] = candidate_sha
    _validate_plan(plan, set(streams), limits)
    if len(_json_bytes(plan)) > limits.max_plan_bytes:
        raise ValueError("Plan exceeds byte budget")
    plan_sha256 = hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    ledgers: dict[str, dict] = {}
    for step in plan["steps"]:
        if step["op"] == "filter":
            ledger = step["ledger"]
            if ledger["path"] in pinned or ledger["path"] in ledgers:
                raise ValueError("Duplicate or shadowed filter ledger key")
            ledgers[ledger["path"]] = ledger
            pinned[ledger["path"]] = ledger["sha256"]

    roster_root = root if roster_prefix == "." else root / roster_prefix
    protected = tuple(root / key for key in pinned)
    validate_evidence_directory(output_dir, protected_files=protected)
    if output_dir.resolve() == root or output_dir.resolve().is_relative_to(root):
        raise ValueError("Evidence output must be outside pinned input root")

    output_dir.mkdir(parents=True, exist_ok=True)
    receipt_path = output_dir / "transformation_accounting.json"
    lineage_path = output_dir / "transformation_lineage.jsonl"
    receipt: dict[str, Any] = {
        "schema_version": 1, "adapter": ADAPTER, "status": "blocked",
        "reason_codes": ["accounting_not_complete"], "acceptance": "not_established_by_accounting",
        "code_sha256": _code_digest(), "limits": asdict(limits), "plan_sha256": plan_sha256,
        "parameters": {"roster_prefix": roster_prefix,
                       "streams": {name: {"path": pin["path"], "sha256": pin["sha256"], "bytes": pin["bytes"], "rows": pin["rows"]}
                                   for name, pin in sorted(streams.items())},
                       "candidate": {"path": candidate_key, "sha256": candidate_sha,
                                      "bytes": candidate["bytes"], "rows": candidate["rows"]}},
        "steps": [], "null_keys": {}, "terminal": {}, "lineage": {},
    }
    write_json_atomic(receipt_path, receipt)
    try:
        # The roster prefix holds the complete input partition tree: every
        # JSONL object under it must be pinned as an input stream or filter
        # ledger, so unioned pagination shards and join tables cannot silently
        # omit a partition. The candidate stays separately pinned by bytes and
        # checksum wherever the pipeline stages it.
        roster_keys = {key for key in pinned if roster_prefix == "." or key.startswith(roster_prefix + "/")}
        actual: set[str] = set()
        for path in roster_root.rglob("*.jsonl"):
            if path.is_symlink():
                raise ValueError("Roster contains a symlinked object")
            if path.is_file():
                actual.add(path.relative_to(root).as_posix())
                if len(actual) > len(roster_keys):
                    raise ValueError("Roster exceeds declared object budget")
        if actual != roster_keys:
            raise ValueError("Roster incomplete or contains undeclared objects")

        sizes = {pin["path"]: pin["bytes"] for pin in streams.values()}
        sizes[candidate_key] = candidate["bytes"]
        sizes.update({path: ledger["bytes"] for path, ledger in ledgers.items()})
        for key, expected in pinned.items():
            path = root / key
            if path.is_symlink() or not path.is_file():
                raise ValueError("Missing pinned object or symlink escape")
            if path.stat().st_size != sizes[key]:
                raise ValueError("Pinned object size mismatch")
            if sha256_file(path) != expected:
                raise ValueError("Pinned object checksum mismatch")

        with tempfile.TemporaryDirectory(dir=output_dir) as scratch:
            db_path = Path(scratch) / "accounting.sqlite"
            with new_evidence_stream(lineage_path, binary=True) as lineage, sqlite3.connect(db_path) as db:
                db.execute(f"PRAGMA cache_size=-{limits.sqlite_cache_kib}")
                db.execute("PRAGMA temp_store=FILE")
                db.execute("PRAGMA journal_mode=OFF")
                db.execute("CREATE TABLE stream_rows (stream TEXT, ordinal INTEGER, key BLOB, record BLOB)")
                db.execute("CREATE INDEX stream_ordinal ON stream_rows(stream,ordinal)")
                db.execute("CREATE INDEX stream_key ON stream_rows(stream,key)")
                page_size = db.execute("PRAGMA page_size").fetchone()[0]
                lineage_rows = 0

                def quota() -> None:
                    lineage.flush()
                    allocated = db.execute("PRAGMA page_count").fetchone()[0] * page_size
                    scratch_bytes = max(allocated, sum(p.stat().st_size for p in Path(scratch).iterdir()))
                    if lineage_path.stat().st_size + scratch_bytes > limits.max_scratch_bytes:
                        raise ValueError("Scratch quota exceeded")

                charged = 0

                def charge(delta: int) -> None:
                    """Charge scratch growth by bytes so bulk loops stay quota-bounded."""
                    nonlocal charged
                    charged += delta
                    if charged >= 4 * 1024 * 1024:
                        charged = 0
                        quota()

                def emit(out_stream: str, ordinal: int, key: bytes, contributors) -> None:
                    # Contributors stream one at a time so a population-sized
                    # group cannot materialize its lineage row in Python memory;
                    # the assembled bytes equal _json_bytes of the whole record.
                    nonlocal lineage_rows
                    lineage.write(b'["object",[["contributors",["array",[')
                    written = 0
                    for contributor in contributors:
                        lineage.write((b"," if written else b"") + _json_bytes(contributor))
                        written += 1
                        if written % 1024 == 0:
                            lineage.flush()
                            quota()
                    lineage.write(b']]],["key",' + _json_bytes(json.loads(key)) + b'],["output_stream",'
                                  + _json_bytes(out_stream) + b'],["row",'
                                  + _json_bytes(ordinal) + b']]]\n')
                    lineage_rows += 1
                    if lineage_rows % 1024 == 0:
                        quota()

                def set_keys(stream_name: str, key_paths: list[list[str]]) -> int:
                    nulls = 0
                    updated = 0
                    for ordinal, record in db.execute(
                            "SELECT ordinal,record FROM stream_rows WHERE stream=? ORDER BY ordinal", (stream_name,)):
                        key = _json_bytes([_resolve(_untyped(json.loads(record)), path_) for path_ in key_paths])
                        if _key_is_null(key):
                            nulls += 1
                        db.execute("UPDATE stream_rows SET key=? WHERE stream=? AND ordinal=?",
                                   (key, stream_name, ordinal))
                        updated += 1
                        if updated % 1024 == 0:
                            quota()
                    quota()
                    return nulls

                def stream_size(stream_name: str) -> int:
                    return db.execute("SELECT COUNT(*) FROM stream_rows WHERE stream=?", (stream_name,)).fetchone()[0]

                for name, pin in sorted(streams.items()):
                    rows = 0
                    for record in _iter_records(root / pin["path"], limits=limits):
                        rows += 1
                        db.execute("INSERT INTO stream_rows VALUES (?,?,?,?)", (name, rows, _UNKEYED, record))
                        charge(len(record) + 64)
                    if rows != pin["rows"]:
                        raise ValueError("Input stream row count differs from pin")
                    quota()

                for index, step in enumerate(plan["steps"]):
                    op, out = step["op"], step["output_stream"]
                    accounting: dict[str, Any] = {"step": index, "op": op}

                    if op == "union":
                        total = 0
                        for name in step["input_streams"]:
                            rows = stream_size(name)
                            accounting[f"input_rows:{name}"] = rows
                            total += rows
                        ordinal = 0
                        for name in step["input_streams"]:
                            for row_ordinal, _, record in db.execute(
                                    "SELECT ordinal,key,record FROM stream_rows WHERE stream=? ORDER BY ordinal", (name,)):
                                ordinal += 1
                                db.execute("INSERT INTO stream_rows VALUES (?,?,?,?)", (out, ordinal, _UNKEYED, record))
                                emit(out, ordinal, _UNKEYED, [[name, row_ordinal]])
                                charge(len(record) + 64)
                        accounting["output_rows"] = ordinal
                        if ordinal != total:
                            raise ValueError("Union row accounting mismatch")

                    elif op == "join":
                        left, right = step["left_stream"], step["right_stream"]
                        left_nulls = set_keys(left, step["left_keys"])
                        right_nulls = set_keys(right, step["right_keys"])
                        receipt["null_keys"][left] = left_nulls
                        receipt["null_keys"][right] = right_nulls
                        left_rows, right_rows = stream_size(left), stream_size(right)
                        ordinal = 0
                        for left_ordinal, key, left_record in db.execute(
                                "SELECT ordinal,key,record FROM stream_rows WHERE stream=? ORDER BY ordinal", (left,)):
                            matched = False
                            for right_ordinal, right_record in db.execute(
                                    "SELECT ordinal,record FROM stream_rows WHERE stream=? AND key=? ORDER BY ordinal",
                                    (right, key)):
                                matched = True
                                ordinal += 1
                                record = _json_bytes({"left": _untyped(json.loads(left_record)), "right": _untyped(json.loads(right_record))})
                                db.execute("INSERT INTO stream_rows VALUES (?,?,?,?)", (out, ordinal, key, record))
                                emit(out, ordinal, key, [[left, left_ordinal], [right, right_ordinal]])
                                charge(len(record) + 64)
                            if not matched and step["join_type"] in {"left", "full"}:
                                ordinal += 1
                                record = _json_bytes({"left": _untyped(json.loads(left_record)), "right": None})
                                db.execute("INSERT INTO stream_rows VALUES (?,?,?,?)", (out, ordinal, key, record))
                                emit(out, ordinal, key, [[left, left_ordinal]])
                                charge(len(record) + 64)
                        if step["join_type"] in {"right", "full"}:
                            for right_ordinal, key, right_record in db.execute(
                                    "SELECT ordinal,key,record FROM stream_rows r WHERE r.stream=? AND NOT EXISTS("
                                    "SELECT 1 FROM stream_rows l WHERE l.stream=? AND l.key=r.key) ORDER BY r.ordinal",
                                    (right, left)):
                                ordinal += 1
                                record = _json_bytes({"left": None, "right": _untyped(json.loads(right_record))})
                                db.execute("INSERT INTO stream_rows VALUES (?,?,?,?)", (out, ordinal, key, record))
                                emit(out, ordinal, key, [[right, right_ordinal]])
                                charge(len(record) + 64)
                        matched_left = db.execute(
                            "SELECT COUNT(*) FROM stream_rows l WHERE l.stream=? AND EXISTS("
                            "SELECT 1 FROM stream_rows r WHERE r.stream=? AND r.key=l.key)", (left, right)).fetchone()[0]
                        matched_right = db.execute(
                            "SELECT COUNT(*) FROM stream_rows r WHERE r.stream=? AND EXISTS("
                            "SELECT 1 FROM stream_rows l WHERE l.stream=? AND l.key=r.key)", (right, left)).fetchone()[0]
                        distinct_matched_keys = db.execute(
                            "SELECT COUNT(DISTINCT l.key) FROM stream_rows l WHERE l.stream=? AND EXISTS("
                            "SELECT 1 FROM stream_rows r WHERE r.stream=? AND r.key=l.key)", (left, right)).fetchone()[0]
                        buckets = {"one_to_one_keys": 0, "one_to_many_keys": 0, "many_to_one_keys": 0, "many_to_many_keys": 0}
                        for left_count, right_count in db.execute(
                                "SELECT COUNT(DISTINCT a.ordinal),COUNT(DISTINCT b.ordinal) FROM stream_rows a "
                                "JOIN stream_rows b ON a.key=b.key AND b.stream=? WHERE a.stream=? GROUP BY a.key",
                                (right, left)):
                            buckets["one_to_one_keys" if left_count == 1 and right_count == 1 else
                                    "one_to_many_keys" if left_count == 1 else
                                    "many_to_one_keys" if right_count == 1 else "many_to_many_keys"] += 1
                        samples = []
                        for key, count in db.execute(
                                "SELECT key,COUNT(*) FROM stream_rows WHERE stream=? AND ordinal NOT IN "
                                "(SELECT a.ordinal FROM stream_rows a JOIN stream_rows b ON a.key=b.key AND b.stream=? WHERE a.stream=?) "
                                "GROUP BY key ORDER BY COUNT(*) DESC, key LIMIT ?",
                                (left, right, left, limits.max_unmatched_key_samples)):
                            samples.append([json.loads(key), count])
                        accounting.update({"left_rows": left_rows, "right_rows": right_rows,
                                           "matched_left_rows": matched_left, "matched_right_rows": matched_right,
                                           "unmatched_left_rows": left_rows - matched_left,
                                           "unmatched_right_rows": right_rows - matched_right,
                                           "distinct_matched_keys": distinct_matched_keys, "fan_out": buckets,
                                           "null_left_keys": left_nulls, "null_right_keys": right_nulls,
                                           "unmatched_left_key_samples": samples, "output_rows": ordinal})

                    elif op == "filter":
                        name = step["input_stream"]
                        ledger_path = root / step["ledger"]["path"]
                        if ledger_path.stat().st_size != step["ledger"]["bytes"]:
                            raise ValueError("Filter ledger size differs from pin")
                        input_rows = stream_size(name)
                        db.execute("DROP TABLE IF EXISTS filter_drops")
                        db.execute("CREATE TABLE filter_drops (row INTEGER PRIMARY KEY, reason TEXT)")
                        with ledger_path.open("rb") as ledger_file:
                            while line := ledger_file.readline(limits.max_record_bytes + 1):
                                if len(line) > limits.max_record_bytes:
                                    raise ValueError("Ledger entry exceeds byte limit")
                                entry = json.loads(line, parse_constant=_reject_constant, object_pairs_hook=_no_duplicate_members)
                                if (not isinstance(entry, dict) or set(entry) != {"row", "reason"}
                                        or type(entry["row"]) is not int or not 1 <= entry["row"] <= input_rows
                                        or not isinstance(entry["reason"], str) or not 1 <= len(entry["reason"]) <= 128):
                                    raise ValueError("Invalid ledger entry")
                                try:
                                    db.execute("INSERT INTO filter_drops VALUES (?,?)", (entry["row"], entry["reason"]))
                                except sqlite3.IntegrityError as exc:
                                    raise ValueError("Duplicate ledger row") from exc
                        reasons = {reason: count for reason, count in db.execute(
                            "SELECT reason,COUNT(*) FROM filter_drops GROUP BY reason ORDER BY reason")}
                        dropped_rows = db.execute("SELECT COUNT(*) FROM filter_drops").fetchone()[0]
                        ordinal = 0
                        for row_ordinal, key, record, drop_reason in db.execute(
                                "SELECT s.ordinal,s.key,s.record,d.reason FROM stream_rows s "
                                "LEFT JOIN filter_drops d ON d.row=s.ordinal WHERE s.stream=? "
                                "ORDER BY s.ordinal", (name,)):
                            if drop_reason is not None:
                                continue
                            ordinal += 1
                            db.execute("INSERT INTO stream_rows VALUES (?,?,?,?)", (out, ordinal, key, record))
                            emit(out, ordinal, key, [[name, row_ordinal]])
                            charge(len(record) + 64)
                        db.execute("DROP TABLE filter_drops")
                        accounting.update({"input_rows": input_rows, "kept_rows": ordinal,
                                           "dropped_rows": dropped_rows, "drop_reasons": reasons,
                                           "output_rows": ordinal})

                    elif op == "deduplicate":
                        name = step["input_stream"]
                        nulls = set_keys(name, step["key_paths"])
                        receipt["null_keys"][name] = nulls
                        rule = step["rule"]
                        db.execute("DROP TABLE IF EXISTS dedup_keep")
                        db.execute("CREATE TABLE dedup_keep (ordinal INTEGER PRIMARY KEY)")
                        db.execute(f"INSERT INTO dedup_keep SELECT {'MIN(ordinal)' if rule == 'first' else 'MAX(ordinal)'} "
                                   "FROM stream_rows WHERE stream=? GROUP BY key", (name,))
                        distinct = db.execute("SELECT COUNT(*) FROM dedup_keep").fetchone()[0]
                        dropped_rows = stream_size(name) - distinct
                        keys_with_duplicates = db.execute(
                            "SELECT COUNT(*) FROM (SELECT key FROM stream_rows WHERE stream=? GROUP BY key HAVING COUNT(*)>1)",
                            (name,)).fetchone()[0]
                        ordinal = 0
                        for row_ordinal, key, record in db.execute(
                                "SELECT s.ordinal,s.key,s.record FROM stream_rows s "
                                "JOIN dedup_keep k ON k.ordinal=s.ordinal WHERE s.stream=? ORDER BY s.ordinal", (name,)):
                            ordinal += 1
                            db.execute("INSERT INTO stream_rows VALUES (?,?,?,?)", (out, ordinal, key, record))
                            emit(out, ordinal, key, [[name, row_ordinal]])
                            charge(len(record) + 64)
                        db.execute("DROP TABLE dedup_keep")
                        accounting.update({"rule": rule, "distinct_keys": distinct,
                                           "keys_with_duplicates": keys_with_duplicates, "dropped_rows": dropped_rows,
                                           "input_rows": distinct + dropped_rows, "output_rows": ordinal})

                    elif op == "aggregate":
                        name = step["input_stream"]
                        nulls = set_keys(name, step["key_paths"])
                        receipt["null_keys"][name] = nulls
                        input_rows = stream_size(name)
                        ordinal = 0
                        groups = 0
                        max_group = 0
                        min_group = 0
                        # Groups and their members stream through SQLite; Python
                        # holds only per-group aggregates, never the population.
                        for (key,) in db.execute(
                                "SELECT key FROM stream_rows WHERE stream=? GROUP BY key ORDER BY MIN(ordinal)", (name,)):
                            groups += 1
                            ordinal += 1
                            members = 0
                            first_record: Any = None
                            states: dict[str, dict[str, Any]] = {}
                            for item in step["aggregations"]:
                                if item["function"] != "count":
                                    states[item["name"]] = {"kinds": set(), "int_sum": 0,
                                                            "parts": (0, 0), "best": None}
                            for _, record in db.execute(
                                    "SELECT ordinal,record FROM stream_rows WHERE stream=? AND key=? ORDER BY ordinal",
                                    (name, key)):
                                members += 1
                                record_value = _untyped(json.loads(record))
                                if first_record is None:
                                    first_record = record_value
                                for item in step["aggregations"]:
                                    function = item["function"]
                                    if function == "count":
                                        continue
                                    value = _resolve(record_value, item["field_path"])
                                    if value is None:
                                        continue
                                    if isinstance(value, bool) or not isinstance(value, (int, Decimal, str)):
                                        raise ValueError("Aggregation field type unsupported")
                                    state = states[item["name"]]
                                    state["kinds"].add("int" if isinstance(value, int)
                                                       else "decimal" if isinstance(value, Decimal) else "string")
                                    kinds = state["kinds"]
                                    if len(kinds) > 2 or ("int" in kinds and "string" in kinds) \
                                            or ("decimal" in kinds and "string" in kinds):
                                        raise ValueError("Aggregation field type unsupported")
                                    if function == "sum":
                                        # Integer mantissa/scale accumulation is
                                        # exact and independent of the ambient
                                        # decimal context, so replay cannot round.
                                        if "string" in kinds:
                                            raise ValueError("Aggregation field type unsupported")
                                        if isinstance(value, int):
                                            state["int_sum"] += value
                                        else:
                                            state["parts"] = _sum_parts(state["parts"], _decimal_parts(value))
                                    elif state["best"] is None or (value < state["best"] if function == "min"
                                                                   else value > state["best"]):
                                        state["best"] = value
                                if members % 1024 == 0:
                                    quota()
                            output: dict[str, Any] = {"key": [_resolve(first_record, path_) for path_ in step["key_paths"]],
                                                      "rows": members}
                            for item in step["aggregations"]:
                                function = item["function"]
                                if function == "count":
                                    output[item["name"]] = members
                                    continue
                                state = states[item["name"]]
                                if not state["kinds"]:
                                    output[item["name"]] = None
                                elif function == "sum":
                                    if state["kinds"] == {"int"}:
                                        output[item["name"]] = state["int_sum"]
                                    else:
                                        output[item["name"]] = _parts_to_decimal(
                                            _sum_parts(state["parts"], (state["int_sum"], 0)))
                                else:
                                    output[item["name"]] = state["best"]
                            db.execute("INSERT INTO stream_rows VALUES (?,?,?,?)", (out, ordinal, key, _json_bytes(output)))
                            emit(out, ordinal, key, ([name, member] for (member,) in db.execute(
                                    "SELECT ordinal FROM stream_rows WHERE stream=? AND key=? ORDER BY ordinal",
                                    (name, key))))
                            max_group = max(max_group, members)
                            min_group = members if groups == 1 else min(min_group, members)
                            quota()
                        accounting.update({"input_rows": input_rows, "groups": groups, "max_group_rows": max_group,
                                           "min_group_rows": min_group, "output_rows": ordinal})

                    else:  # project
                        name = step["input_stream"]
                        ordinal = 0
                        null_geometries = 0
                        for row_ordinal, key, record in db.execute(
                                "SELECT ordinal,key,record FROM stream_rows WHERE stream=? ORDER BY ordinal", (name,)):
                            record_value = _untyped(json.loads(record))
                            properties = {item["name"]: _resolve(record_value, item["path"]) for item in step["properties"]}
                            geometry = _resolve(record_value, step["geometry_path"]) if "geometry_path" in step else None
                            if geometry is None:
                                null_geometries += 1
                            ordinal += 1
                            record_out = _json_bytes({"type": "Feature", "properties": properties, "geometry": geometry})
                            db.execute("INSERT INTO stream_rows VALUES (?,?,?,?)", (out, ordinal, key, record_out))
                            emit(out, ordinal, key, [[name, row_ordinal]])
                            quota()
                        accounting.update({"input_rows": ordinal, "output_rows": ordinal,
                                           "null_geometries": null_geometries})
                    receipt["steps"].append(accounting)

                terminal = _stream_name(plan["output"])
                recomputed_rows = stream_size(terminal)
                if recomputed_rows != candidate["rows"]:
                    raise ValueError("Terminal row count differs from candidate")
                canonical = hashlib.sha256()
                compared = 0
                with (root / candidate_key).open("rb") as candidate_stream:
                    for (actual_record,) in db.execute(
                            "SELECT record FROM stream_rows WHERE stream=? ORDER BY ordinal", (terminal,)):
                        line = candidate_stream.readline(limits.max_record_bytes + 1)
                        if not line:
                            raise ValueError("Terminal row count differs from candidate")
                        if len(line) > limits.max_record_bytes:
                            raise ValueError("Record byte limit exceeded")
                        expected = _json_bytes(json.loads(line, parse_float=Decimal, parse_constant=_reject_constant,
                                                          object_pairs_hook=_no_duplicate_members))
                        if actual_record != expected:
                            raise ValueError("Terminal record differs from candidate")
                        canonical.update(actual_record + b"\n")
                        compared += 1
                        if compared % 1024 == 0:
                            quota()
                    if candidate_stream.read(1):
                        raise ValueError("Terminal row count differs from candidate")
                if compared != recomputed_rows:
                    raise ValueError("Terminal row count differs from candidate")
                receipt["terminal"] = {"stream": terminal, "rows": compared,
                                      "recomputed_sha256": canonical.hexdigest(),
                                      "candidate_sha256": candidate_sha, "candidate_rows": compared,
                                      "equal": True}
                # Pin lineage from the flushed file: the final quota() drains
                # the buffered sidecar and enforces the scratch budget on the
                # complete evidence before any checksum, size or row pin is
                # taken, so a valid final union/filter/dedup cannot pin the
                # empty-file digest that the release replay would reject.
                quota()
                receipt["lineage"] = {"path": lineage_path.name, "sha256": sha256_file(lineage_path),
                                      "bytes": lineage_path.stat().st_size, "rows": lineage_rows}
        for key, expected in pinned.items():
            path = root / key
            if sha256_file(path) != expected:
                raise ValueError("Pinned object changed during accounting")
        if _code_digest() != receipt["code_sha256"]:
            raise ValueError("Accounting implementation changed during execution")
        receipt["status"] = "pass"
        receipt["reason_codes"] = []
    except (ValueError, OSError, sqlite3.Error, KeyError, TypeError, OverflowError, RecursionError, UnicodeError) as exc:
        receipt["status"] = "blocked"
        receipt["reason_codes"] = [f"accounting_incomplete:{type(exc).__name__}:{exc}"]
    receipt["receipt_id"] = _digest(receipt)
    write_json_atomic(receipt_path, receipt)
    return receipt

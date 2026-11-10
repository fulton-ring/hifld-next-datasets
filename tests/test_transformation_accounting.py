import copy
import decimal
import json
import os
import tempfile
import tracemalloc
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.assets.publish import _copy_version_files
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import ReleaseApprovalError, require_release_approval
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.transformation_accounting import (
    AccountingLimits, ADAPTER, ADAPTER as ACCOUNTING_ADAPTER,
    _code_digest as _accounting_code_digest, _digest as _accounting_digest,
    _json_bytes, _untyped, account_construction, sha256_file,
)
from dagster_hifld.validation import compare_full_rows
from test_full_verification import accepted_fixture, feature as accepted_feature


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def feature(key, state="PA", name=None, x=1):
    return {"type": "Feature", "properties": {"id": key, "state": state, "name": name},
            "geometry": {"type": "Point", "coordinates": [x, 2]}}


def pin(path):
    return {"sha256": sha256_file(path), "bytes": path.stat().st_size}


def fixture_root(root):
    """Two pagination shards, a lookup table and an explicit filter ledger."""
    streams = root / "streams"
    write_jsonl(streams / "events-a.jsonl", [
        feature("e1", "PA", "Alpha", 1), feature("e2", "OH", "Bravo", 2)])
    write_jsonl(streams / "events-b.jsonl", [
        feature("e3", "PA", "Charlie", 3), feature("e4", "TX", "Delta", 4)])
    write_jsonl(streams / "regions.jsonl", [
        {"state": "PA", "region": "East"}, {"state": "OH", "region": "Midwest"}])
    write_jsonl(streams / "dropped.jsonl", [
        {"row": 4, "reason": "unmatched_region"}])
    plan = {
        "schema_version": 1,
        "steps": [
            {"op": "union", "input_streams": ["events-a", "events-b"], "output_stream": "events"},
            {"op": "join", "left_stream": "events", "right_stream": "regions", "join_type": "left",
             "left_keys": [["properties", "state"]], "right_keys": [["state"]],
             "output_stream": "joined"},
            {"op": "filter", "input_stream": "joined", "output_stream": "kept",
             "ledger": {"path": "streams/dropped.jsonl", **pin(streams / "dropped.jsonl")}},
            {"op": "project", "input_stream": "kept", "output_stream": "candidate",
             "properties": [{"name": "id", "path": ["left", "properties", "id"]},
                            {"name": "state", "path": ["left", "properties", "state"]},
                            {"name": "region", "path": ["right", "region"]}],
             "geometry_path": ["left", "geometry"]},
        ],
        "output": "candidate",
    }
    candidate_rows = [
        {"type": "Feature", "properties": {"id": "e1", "state": "PA", "region": "East"},
         "geometry": {"type": "Point", "coordinates": [1, 2]}},
        {"type": "Feature", "properties": {"id": "e2", "state": "OH", "region": "Midwest"},
         "geometry": {"type": "Point", "coordinates": [2, 2]}},
        {"type": "Feature", "properties": {"id": "e3", "state": "PA", "region": "East"},
         "geometry": {"type": "Point", "coordinates": [3, 2]}},
    ]
    write_jsonl(streams / "candidate.jsonl", candidate_rows)
    spec = {"schema_version": 1, "root": str(root), "roster_prefix": "streams", "streams": [
        {"name": "events-a", "path": "streams/events-a.jsonl", **pin(streams / "events-a.jsonl"), "rows": 2},
        {"name": "events-b", "path": "streams/events-b.jsonl", **pin(streams / "events-b.jsonl"), "rows": 2},
        {"name": "regions", "path": "streams/regions.jsonl", **pin(streams / "regions.jsonl"), "rows": 2},
    ]}
    candidate = {"path": "streams/candidate.jsonl", **pin(streams / "candidate.jsonl"), "rows": 3}
    return spec, plan, candidate


def account(root, evidence):
    spec, plan, candidate = fixture_root(root)
    return account_construction(spec, plan, candidate, evidence), spec, plan, candidate


class TransformationAccountingTests(unittest.TestCase):
    def test_full_plan_replays_candidate_with_complete_accounting(self):
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            receipt, *_ = account(root, evidence)
            self.assertEqual(receipt["status"], "pass")
            self.assertEqual(receipt["adapter"], ADAPTER)
            self.assertEqual(receipt["acceptance"], "not_established_by_accounting")
            self.assertTrue(receipt["terminal"]["equal"])
            self.assertEqual(receipt["terminal"]["rows"], 3)
            union, join, filter_, project = receipt["steps"]
            self.assertEqual(union["output_rows"], 4)
            self.assertEqual(join["left_rows"], 4)
            self.assertEqual(join["right_rows"], 2)
            self.assertEqual(join["matched_left_rows"], 3)
            self.assertEqual(join["unmatched_left_rows"], 1)
            self.assertEqual(join["distinct_matched_keys"], 2)
            self.assertEqual(join["fan_out"]["one_to_one_keys"], 1)
            self.assertEqual(join["fan_out"]["many_to_one_keys"], 1)
            self.assertEqual(filter_["kept_rows"], 3)
            self.assertEqual(filter_["dropped_rows"], 1)
            self.assertEqual(filter_["drop_reasons"], {"unmatched_region": 1})
            self.assertEqual(project["output_rows"], 3)
            lineage = (evidence / "transformation_lineage.jsonl").read_text().splitlines()
            self.assertEqual(len(lineage), 14)
            rows = [_untyped(json.loads(line)) for line in lineage]
            self.assertEqual(rows[-1]["contributors"], [["kept", 3]])
            stored = json.loads((evidence / "transformation_accounting.json").read_text())
            self.assertEqual(stored["status"], "pass")
            self.assertEqual(stored["receipt_id"], receipt["receipt_id"])

    def test_receipt_is_deterministic_across_replays(self):
        receipts = []
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root = Path(d)
            for _ in range(3):
                receipts.append(account(root, Path(e))[0])
        self.assertEqual(receipts[0], receipts[1])
        self.assertEqual(receipts[1], receipts[2])

    def test_undeclared_roster_entry_blocks(self):
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            account(root, evidence)
            write_jsonl(root / "streams/hidden-shard.jsonl", [feature("x")])
            receipt = account(root, evidence)[0]
            self.assertEqual(receipt["status"], "blocked")
            self.assertTrue(any("Roster" in code or "undeclared" in code for code in receipt["reason_codes"]))
            stored = json.loads((evidence / "transformation_accounting.json").read_text())
            self.assertEqual(stored["status"], "blocked")
            self.assertEqual(stored["reason_codes"], receipt["reason_codes"])

    def test_input_pin_mismatch_and_row_drift_block(self):
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            spec, plan, candidate = fixture_root(root)
            spec["streams"][0]["sha256"] = "0" * 64
            receipt = account_construction(spec, plan, candidate, evidence)
            self.assertEqual(receipt["status"], "blocked")
            self.assertTrue(any("checksum" in code for code in receipt["reason_codes"]))
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            spec, plan, candidate = fixture_root(root)
            spec["streams"][0]["rows"] = 3
            receipt = account_construction(spec, plan, candidate, evidence)
            self.assertEqual(receipt["status"], "blocked")
            self.assertTrue(any("row count" in code for code in receipt["reason_codes"]))

    def test_candidate_mismatch_and_count_drift_block(self):
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            spec, plan, candidate = fixture_root(root)
            write_jsonl(root / "streams/candidate.jsonl", [
                {"type": "Feature", "properties": {"id": "e1", "state": "PA", "region": "East"},
                 "geometry": {"type": "Point", "coordinates": [1, 2]}},
                {"type": "Feature", "properties": {"id": "e2", "state": "OH", "region": "Midwest"},
                 "geometry": {"type": "Point", "coordinates": [2, 2]}},
                {"type": "Feature", "properties": {"id": "e3", "state": "PA", "region": "WRONG"},
                 "geometry": {"type": "Point", "coordinates": [3, 2]}},
            ])
            candidate.update(pin(root / "streams/candidate.jsonl"))
            receipt = account_construction(spec, plan, candidate, evidence)
            self.assertEqual(receipt["status"], "blocked")
            self.assertTrue(any("differs from candidate" in code for code in receipt["reason_codes"]))
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            spec, plan, candidate = fixture_root(root)
            candidate["rows"] = 2
            receipt = account_construction(spec, plan, candidate, evidence)
            self.assertEqual(receipt["status"], "blocked")
            self.assertTrue(any("row count" in code for code in receipt["reason_codes"]))

    def test_ledger_integrity_failures_block(self):
        def run(root, evidence, ledger_rows):
            spec, plan, candidate = fixture_root(root)
            write_jsonl(root / "streams/dropped.jsonl", ledger_rows)
            plan["steps"][2]["ledger"].update(pin(root / "streams/dropped.jsonl"))
            return account_construction(spec, plan, candidate, evidence)
        cases = [
            ([{"row": 2, "reason": "r"}], "differs from candidate"),
            ([{"row": 9, "reason": "r"}], "Invalid ledger entry"),
            ([{"row": 1, "reason": "r"}, {"row": 1, "reason": "r"}], "Duplicate ledger row"),
            ([{"row": 1, "reason": ""}], "Invalid ledger entry"),
            ([{"row": 1}], "Invalid ledger entry"),
        ]
        for ledger_rows, expected in cases:
            with self.subTest(ledger=ledger_rows), tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
                receipt = run(Path(d), Path(e), ledger_rows)
                self.assertEqual(receipt["status"], "blocked")
                self.assertTrue(any(expected in code for code in receipt["reason_codes"]), receipt["reason_codes"])

    def test_plan_validation_rejects_unsound_constructions(self):
        cases = [
            {"schema_version": 1, "steps": [], "output": "x"},
            {"schema_version": 1, "steps": [{"op": "aggregate", "input_stream": "events-a",
             "output_stream": "x", "key_paths": [["id"]]}], "output": "x"},
            {"schema_version": 1, "steps": [{"op": "join", "left_stream": "events-a", "right_stream": "regions",
             "join_type": "outer", "left_keys": [["id"]], "right_keys": [["state"]], "output_stream": "x"}], "output": "x"},
        ]
        for plan in cases:
            with self.subTest(plan=plan), tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
                root, evidence = Path(d), Path(e)
                spec, _, candidate = fixture_root(root)
                with self.assertRaises(ValueError):
                    account_construction(spec, plan, candidate, evidence)

    def test_input_mutation_during_accounting_blocks(self):
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            spec, plan, candidate = fixture_root(root)
            original = sha256_file
            calls = 0
            def mutate(path):
                nonlocal calls
                calls += 1
                result = original(Path(path))
                if calls == 9:
                    # Fires during the filter-ledger checksum check (after the
                    # receipt/code digests and input pins), once the input
                    # streams are loaded; the final pin recheck must detect
                    # the changed original bytes.
                    write_jsonl(root / "streams/events-a.jsonl", [feature("drifted")])
                return result
            with patch("dagster_hifld.transformation_accounting.sha256_file", side_effect=mutate):
                receipt = account_construction(spec, plan, candidate, evidence)
            self.assertEqual(receipt["status"], "blocked")
            self.assertTrue(any("changed during accounting" in code for code in receipt["reason_codes"]), receipt["reason_codes"])

    def test_evidence_output_inside_input_root_is_refused(self):
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            spec, plan, candidate = fixture_root(root)
            with self.assertRaises(ValueError):
                account_construction(spec, plan, candidate, root / "streams/evidence")

    def test_dedup_aggregate_and_join_accounting(self):
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            streams = root / "streams"
            write_jsonl(streams / "votes.jsonl", [
                {"ward": "w1", "party": "A", "tally": 3},
                {"ward": "w1", "party": "B", "tally": 5},
                {"ward": "w2", "party": "A", "tally": 2},
                {"ward": "w2", "party": "A", "tally": 7},
                {"ward": "w2", "party": "B", "tally": 1},
            ])
            write_jsonl(streams / "auditors.jsonl", [
                {"ward": "w1", "auditor": "kim"}, {"ward": "w3", "auditor": "lee"},
            ])
            write_jsonl(streams / "dropped.jsonl", [{"row": 2, "reason": "duplicate_ward"}])
            plan = {
                "schema_version": 1,
                "steps": [
                    {"op": "deduplicate", "input_stream": "votes", "key_paths": [["ward"], ["party"]],
                     "rule": "first", "output_stream": "unique_votes"},
                    {"op": "filter", "input_stream": "unique_votes", "output_stream": "filtered",
                     "ledger": {"path": "streams/dropped.jsonl", **pin(streams / "dropped.jsonl")}},
                    {"op": "aggregate", "input_stream": "filtered", "key_paths": [["ward"]],
                     "aggregations": [{"name": "total", "function": "sum", "field_path": ["tally"]},
                                      {"name": "counted", "function": "count"},
                                      {"name": "best", "function": "max", "field_path": ["party"]}],
                     "output_stream": "totals"},
                    {"op": "join", "left_stream": "totals", "right_stream": "auditors", "join_type": "full",
                     "left_keys": [["key", "0"]], "right_keys": [["ward"]], "output_stream": "audited"},
                ],
                "output": "audited",
            }
            expected = [
                {"left": {"key": ["w1"], "rows": 1, "total": 3, "counted": 1, "best": "A"},
                 "right": {"ward": "w1", "auditor": "kim"}},
                {"left": {"key": ["w2"], "rows": 2, "total": 3, "counted": 2, "best": "B"}, "right": None},
                {"left": None, "right": {"ward": "w3", "auditor": "lee"}},
            ]
            write_jsonl(streams / "audited.jsonl", expected)
            spec = {"schema_version": 1, "root": str(root), "roster_prefix": "streams", "streams": [
                {"name": "votes", "path": "streams/votes.jsonl", **pin(streams / "votes.jsonl"), "rows": 5},
                {"name": "auditors", "path": "streams/auditors.jsonl", **pin(streams / "auditors.jsonl"), "rows": 2},
            ]}
            candidate = {"path": "streams/audited.jsonl", **pin(streams / "audited.jsonl"), "rows": 3}
            receipt = account_construction(spec, plan, candidate, evidence)
            self.assertEqual(receipt["status"], "pass")
            dedup, filter_, aggregate, join = receipt["steps"]
            self.assertEqual(dedup["dropped_rows"], 1)
            self.assertEqual(dedup["keys_with_duplicates"], 1)
            self.assertEqual(aggregate["groups"], 2)
            self.assertEqual(aggregate["max_group_rows"], 2)
            self.assertEqual(join["unmatched_left_rows"], 1)
            self.assertEqual(join["unmatched_right_rows"], 1)
            self.assertEqual(join["fan_out"]["one_to_one_keys"], 1)

    def test_aggregation_type_and_decimal_accounting(self):
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            streams = root / "streams"
            write_jsonl(streams / "items.jsonl", [
                {"group": "g", "value": 2},
                {"group": "g", "value": 3.5},
                {"group": "h", "value": 10},
            ])
            write_jsonl(streams / "sums.jsonl", [
                {"key": ["g"], "rows": 2, "total": 5.5, "counted": 2, "smallest": 2},
                {"key": ["h"], "rows": 1, "total": 10, "counted": 1, "smallest": 10},
            ])
            plan = {"schema_version": 1, "steps": [
                {"op": "aggregate", "input_stream": "items", "key_paths": [["group"]],
                 "aggregations": [{"name": "total", "function": "sum", "field_path": ["value"]},
                                  {"name": "counted", "function": "count"},
                                  {"name": "smallest", "function": "min", "field_path": ["value"]}],
                 "output_stream": "sums"}], "output": "sums"}
            spec = {"schema_version": 1, "root": str(root), "roster_prefix": "streams", "streams": [
                {"name": "items", "path": "streams/items.jsonl", **pin(streams / "items.jsonl"), "rows": 3}]}
            candidate = {"path": "streams/sums.jsonl", **pin(streams / "sums.jsonl"), "rows": 2}
            receipt = account_construction(spec, plan, candidate, evidence)
            self.assertEqual(receipt["status"], "pass")
            # json.dumps of 5.5 stays 5.5; Decimal 3.5 + 2 sums to 5.5 exactly.
            with tempfile.TemporaryDirectory() as d:
                root = Path(d)
                streams = Path(d) / "streams"
                write_jsonl(streams / "items.jsonl", [{"group": "g", "value": "text"}])
                write_jsonl(streams / "sums.jsonl", [{"key": ["g"], "rows": 1, "total": None, "counted": 1, "smallest": None}])
                plan_bad = copy.deepcopy(plan)
                candidate = {"path": "streams/sums.jsonl", **pin(streams / "sums.jsonl"), "rows": 1}
                spec = {"schema_version": 1, "root": str(root), "roster_prefix": "streams", "streams": [
                    {"name": "items", "path": "streams/items.jsonl", **pin(streams / "items.jsonl"), "rows": 1}]}
                receipt = account_construction(spec, plan_bad, candidate, evidence)
                self.assertEqual(receipt["status"], "blocked")
                self.assertTrue(any("Aggregation field type unsupported" in code for code in receipt["reason_codes"]))

    def test_accounted_candidate_feeds_full_row_comparison(self):
        """A constructed candidate still faces the unchanged baseline gate."""
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            receipt, spec, plan, candidate = account(root, evidence)
            self.assertEqual(receipt["status"], "pass")
            baseline = root / "baseline.jsonl"
            write_jsonl(baseline, [json.loads(line) for line in
                                  (root / "streams/candidate.jsonl").read_text().splitlines()])
            reference = {"role": "unapproved_constructed_candidate_reference", "approved": False,
                         "comparison_input": {"adapter": "typed-geojson-feature-jsonl-v1",
                                             "sha256": sha256_file(baseline),
                                             "bytes": baseline.stat().st_size}}
            contract = {"id_fields": ["id"], "coverage_fields": ["state"], "geometry": True, "crs": "EPSG:4326",
                        "columns": {"id": {"type": "string", "nullable": False},
                                    "state": {"type": "string", "nullable": False},
                                    "region": {"type": "string", "nullable": True}}}
            report = compare_full_rows(baseline, root / "streams/candidate.jsonl", contract, root / "metadata",
                                       baseline_sha256=sha256_file(baseline),
                                       candidate_sha256=sha256_file(root / "streams/candidate.jsonl"),
                                       baseline_reference=reference)
            self.assertEqual(report["status"], "pass")

    def test_decimal_sums_are_exact_and_context_independent(self):
        """High-precision and cancelled sums replay exactly under any decimal context."""
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            streams = root / "streams"
            streams.mkdir(parents=True)
            (streams / "items.jsonl").write_text(
                '{"group": "big", "value": 123456789012345678901234567890.1}\n'
                '{"group": "big", "value": 0.2}\n'
                '{"group": "mix", "value": 1000000000000000000000000000000}\n'
                '{"group": "mix", "value": 0.5}\n'
                '{"group": "cancel", "value": 12345678901234567890123456789}\n'
                '{"group": "cancel", "value": -12345678901234567890123456788}\n'
                '{"group": "cancel", "value": 1.5}\n')
            (streams / "sums.jsonl").write_text(
                '{"key": ["big"], "rows": 2, "total": 123456789012345678901234567890.3, "counted": 2}\n'
                '{"key": ["mix"], "rows": 2, "total": 1000000000000000000000000000000.5, "counted": 2}\n'
                '{"key": ["cancel"], "rows": 3, "total": 2.5, "counted": 3}\n')
            plan = {"schema_version": 1, "steps": [
                {"op": "aggregate", "input_stream": "items", "key_paths": [["group"]],
                 "aggregations": [{"name": "total", "function": "sum", "field_path": ["value"]},
                                  {"name": "counted", "function": "count"}],
                 "output_stream": "sums"}], "output": "sums"}
            spec = {"schema_version": 1, "root": str(root), "roster_prefix": "streams", "streams": [
                {"name": "items", "path": "streams/items.jsonl",
                 **pin(streams / "items.jsonl"), "rows": 7}]}
            candidate = {"path": "streams/sums.jsonl", **pin(streams / "sums.jsonl"), "rows": 3}
            with decimal.localcontext(decimal.Context(prec=2)):
                receipt_tiny = account_construction(spec, plan, candidate, evidence)
            with decimal.localcontext(decimal.Context(prec=999)):
                receipt_huge = account_construction(spec, plan, candidate, evidence)
            receipt_default = account_construction(spec, plan, candidate, evidence)
            self.assertEqual(receipt_default["status"], "pass", receipt_default["reason_codes"])
            self.assertEqual(receipt_tiny, receipt_default)
            self.assertEqual(receipt_huge, receipt_default)
            for line in (evidence / "transformation_lineage.jsonl").read_text().splitlines():
                raw = line.encode()
                self.assertEqual(_json_bytes(_untyped(json.loads(raw))), raw)
            # A context-rounded sum (prec 28 default) must never pass replay.
            (streams / "sums.jsonl").write_text(
                '{"key": ["big"], "rows": 2, "total": 1.234567890123456789012345679E+29, "counted": 2}\n'
                '{"key": ["mix"], "rows": 2, "total": 1000000000000000000000000000000.5, "counted": 2}\n'
                '{"key": ["cancel"], "rows": 3, "total": 2.5, "counted": 3}\n')
            candidate.update(pin(streams / "sums.jsonl"))
            receipt = account_construction(spec, plan, candidate, evidence)
            self.assertEqual(receipt["status"], "blocked")
            self.assertTrue(any("differs from candidate" in code for code in receipt["reason_codes"]),
                            receipt["reason_codes"])

    def test_final_step_lineage_pins_match_flushed_sidecar(self):
        """Union/filter/dedup endings pin the flushed sidecar, not the buffer."""
        cases = [
            ("union", [feature("e1")],
             {"op": "union", "input_streams": ["in"], "output_stream": "out"},
             [feature("e1")], []),
            ("filter", [feature("e1", "OH"), feature("e2")],
             {"op": "filter", "input_stream": "in", "output_stream": "out"},
             [feature("e2")], [{"row": 1, "reason": "filtered_row"}]),
            ("dedup", [feature("e1", "OH"), feature("e9", "OH")],
             {"op": "deduplicate", "input_stream": "in", "key_paths": [["properties", "state"]],
              "rule": "first", "output_stream": "out"},
             [feature("e1", "OH")], []),
        ]
        for name, rows, step, candidate_rows, ledger_rows in cases:
            with self.subTest(step=name), tempfile.TemporaryDirectory() as d, \
                    tempfile.TemporaryDirectory() as e:
                root, evidence = Path(d), Path(e)
                streams = root / "streams"
                write_jsonl(streams / "input.jsonl", rows)
                step = dict(step)
                if ledger_rows:
                    write_jsonl(streams / "dropped.jsonl", ledger_rows)
                    step["ledger"] = {"path": "streams/dropped.jsonl", **pin(streams / "dropped.jsonl")}
                write_jsonl(streams / "candidate.jsonl", candidate_rows)
                plan = {"schema_version": 1, "steps": [step], "output": "out"}
                spec = {"schema_version": 1, "root": str(root), "roster_prefix": "streams", "streams": [
                    {"name": "in", "path": "streams/input.jsonl", **pin(streams / "input.jsonl"), "rows": len(rows)}]}
                candidate = {"path": "streams/candidate.jsonl", **pin(streams / "candidate.jsonl"),
                             "rows": len(candidate_rows)}
                receipt = account_construction(spec, plan, candidate, evidence)
                self.assertEqual(receipt["status"], "pass", receipt["reason_codes"])
                lineage = evidence / "transformation_lineage.jsonl"
                actual_bytes = lineage.stat().st_size
                actual_rows = len(lineage.read_text().splitlines())
                self.assertGreater(actual_bytes, 0)
                self.assertEqual(receipt["lineage"]["bytes"], actual_bytes)
                self.assertEqual(receipt["lineage"]["sha256"], sha256_file(lineage))
                self.assertEqual(receipt["lineage"]["rows"], actual_rows)
                self.assertEqual(actual_rows, receipt["steps"][-1]["output_rows"])

    def test_full_population_accounting_stays_memory_bounded(self):
        """Many small records never materialize the population in Python memory."""
        rows = 40000
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as e:
            root, evidence = Path(d), Path(e)
            streams = root / "streams"
            streams.mkdir(parents=True)
            shard_a, shard_b = streams / "votes-a.jsonl", streams / "votes-b.jsonl"
            with shard_a.open("w") as first, shard_b.open("w") as second:
                for i in range(rows):
                    line = json.dumps({"id": i, "ward": "w", "tally": (i % 5) + 1}) + "\n"
                    (first if i < rows // 2 else second).write(line)
            total = sum((i % 5) + 1 for i in range(rows))
            (streams / "totals.jsonl").write_text(
                json.dumps({"key": ["w"], "rows": rows, "total": total, "counted": rows}) + "\n")
            plan = {"schema_version": 1, "steps": [
                {"op": "union", "input_streams": ["votes-a", "votes-b"], "output_stream": "votes"},
                {"op": "deduplicate", "input_stream": "votes", "key_paths": [["id"]], "rule": "first",
                 "output_stream": "unique"},
                {"op": "aggregate", "input_stream": "unique", "key_paths": [["ward"]],
                 "aggregations": [{"name": "total", "function": "sum", "field_path": ["tally"]},
                                  {"name": "counted", "function": "count"}],
                 "output_stream": "totals"}], "output": "totals"}
            spec = {"schema_version": 1, "root": str(root), "roster_prefix": "streams", "streams": [
                {"name": "votes-a", "path": "streams/votes-a.jsonl", **pin(shard_a), "rows": rows // 2},
                {"name": "votes-b", "path": "streams/votes-b.jsonl", **pin(shard_b), "rows": rows - rows // 2}]}
            candidate = {"path": "streams/totals.jsonl", **pin(streams / "totals.jsonl"), "rows": 1}
            tracemalloc.start()
            try:
                receipt = account_construction(spec, plan, candidate, evidence)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            self.assertEqual(receipt["status"], "pass", receipt["reason_codes"])
            union, dedup, aggregate = receipt["steps"]
            self.assertEqual(union["output_rows"], rows)
            self.assertEqual(dedup["dropped_rows"], 0)
            self.assertEqual(aggregate["groups"], 1)
            self.assertEqual(aggregate["max_group_rows"], rows)
            self.assertEqual(aggregate["min_group_rows"], rows)
            self.assertLess(peak, 12 * 1024 * 1024,
                            "population-sized Python allocations leaked into accounting")
            lineage = (evidence / "transformation_lineage.jsonl").read_text().splitlines()
            self.assertEqual(len(lineage), 2 * rows + 1)
            last = lineage[-1].encode()
            self.assertEqual(_json_bytes(_untyped(json.loads(last))), last)
            self.assertEqual(len(_untyped(json.loads(last))["contributors"]), rows)


class AccountingModeGateTests(unittest.TestCase):
    """Constructed candidates need replayed executable accounting to release."""

    def _accounted_fixture(self, root):
        """Extend the accepted fixture with a constructed candidate custody chain."""
        source, target, registry = accepted_fixture(root)
        prefix = "synthetic/points/v1.0.0"
        version_dir = root / "staging" / prefix
        shard_a = (json.dumps(accepted_feature()) + "\n").encode()
        shard_b = (json.dumps(accepted_feature("002", "VA", "B")) + "\n").encode()
        regions = (json.dumps({"state": "PA", "region": "East"}) + "\n").encode()
        ledger = (json.dumps({"row": 2, "reason": "unmatched_region"}) + "\n").encode()
        for name, body in (("streams/events-a.jsonl", shard_a), ("streams/events-b.jsonl", shard_b),
                           ("streams/regions.jsonl", regions), ("streams/dropped.jsonl", ledger)):
            source.write("synthetic", "points", "v1.0.0", name, body)
        plan = {
            "schema_version": 1,
            "steps": [
                {"op": "union", "input_streams": ["events-a", "events-b"], "output_stream": "events"},
                {"op": "join", "left_stream": "events", "right_stream": "regions", "join_type": "left",
                 "left_keys": [["properties", "state"]], "right_keys": [["state"]], "output_stream": "joined"},
                {"op": "filter", "input_stream": "joined", "output_stream": "kept",
                 "ledger": {"path": "streams/dropped.jsonl", "sha256": sha256_file(version_dir / "streams/dropped.jsonl"),
                            "bytes": len(ledger)}},
                {"op": "project", "input_stream": "kept", "output_stream": "candidate",
                 "properties": [{"name": "id", "path": ["left", "properties", "id"]},
                                {"name": "state", "path": ["left", "properties", "state"]},
                                {"name": "name", "path": ["left", "properties", "name"]}],
                 "geometry_path": ["left", "geometry"]},
                {"op": "union", "input_streams": ["candidate"], "output_stream": "audited"},
            ],
            "output": "audited",
        }
        spec = {"schema_version": 1, "root": str(version_dir), "roster_prefix": "streams", "streams": [
            {"name": "events-a", "path": "streams/events-a.jsonl", "sha256": sha256_file(version_dir / "streams/events-a.jsonl"),
             "bytes": len(shard_a), "rows": 1},
            {"name": "events-b", "path": "streams/events-b.jsonl", "sha256": sha256_file(version_dir / "streams/events-b.jsonl"),
             "bytes": len(shard_b), "rows": 1},
            {"name": "regions", "path": "streams/regions.jsonl", "sha256": sha256_file(version_dir / "streams/regions.jsonl"),
             "bytes": len(regions), "rows": 1},
        ]}
        candidate = version_dir / "metadata/candidate.records.jsonl"
        candidate_pin = {"path": "metadata/candidate.records.jsonl", "sha256": sha256_file(candidate),
                         "bytes": candidate.stat().st_size, "rows": 1}
        receipt = account_construction(spec, plan, candidate_pin, root / "accounting-evidence")
        assert receipt["status"] == "pass"
        source.write("synthetic", "points", "v1.0.0", "metadata/construction_plan.json",
                     (json.dumps(plan, sort_keys=True, indent=2) + "\n").encode())
        source.write("synthetic", "points", "v1.0.0", "metadata/transformation_accounting.json",
                     (root / "accounting-evidence/transformation_accounting.json").read_bytes())
        source.write("synthetic", "points", "v1.0.0", "metadata/transformation_lineage.jsonl",
                     (root / "accounting-evidence/transformation_lineage.jsonl").read_bytes())

        descriptor = {"adapter": ACCOUNTING_ADAPTER,
                      "plan": {"path": "construction_plan.json",
                               "sha256": sha256_file(version_dir / "metadata/construction_plan.json")},
                      "receipt": {"path": "transformation_accounting.json",
                                  "sha256": sha256_file(version_dir / "metadata/transformation_accounting.json")},
                      "lineage": {"path": "transformation_lineage.jsonl",
                                  "sha256": sha256_file(version_dir / "metadata/transformation_lineage.jsonl")}}
        report = json.loads(source.read_key(f"{prefix}/metadata/baseline_comparison.json"))
        report["construction_accounting"] = descriptor
        source.write_key(f"{prefix}/metadata/baseline_comparison.json", json.dumps(report, sort_keys=True).encode())

        manifest_key = f"{prefix}/metadata/source_manifest.json"
        manifest = json.loads(source.read_key(manifest_key))
        manifest["transformations"] = [{"name": "union_paged_shards"}, {"name": "left_join_regions"},
                                       {"name": "filter_unmatched_regions"}, {"name": "project_published_schema"}]
        objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
        manifest["lineage"] = {
            "schema_version": 1, "adapter": ACCOUNTING_ADAPTER,
            "code_sha256": _accounting_code_digest(),
            "input_streams": {f"{prefix}/streams/{name}": objects[f"{prefix}/streams/{name}"]
                              for name in ("events-a.jsonl", "events-b.jsonl", "regions.jsonl")},
            "plan": {"key": f"{prefix}/metadata/construction_plan.json", "sha256": descriptor["plan"]["sha256"]},
            "receipt": {"key": f"{prefix}/metadata/transformation_accounting.json", "sha256": descriptor["receipt"]["sha256"]},
            "lineage": {"key": f"{prefix}/metadata/transformation_lineage.jsonl", "sha256": descriptor["lineage"]["sha256"]},
            "output": {"key": f"{prefix}/metadata/candidate.records.jsonl",
                       "sha256": objects[f"{prefix}/metadata/candidate.records.jsonl"],
                       "bytes": candidate.stat().st_size},
        }
        source.write_key(manifest_key, json.dumps(manifest).encode())
        snapshot_source_metadata(source, "synthetic", "points", "v1.0.0")
        projection = provenance_projection(load_resolved_source_manifest(source, "synthetic", "points", "v1.0.0").metadata)
        source.write_key(f"{prefix}/metadata/resolved_provenance.json", json.dumps(projection).encode())
        source.write_key(f"{prefix}/metadata/data_dictionary.json",
                         json.dumps({"agency": "Synthetic Agency", **projection}).encode())

        objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
        value = json.loads(registry.read_text())
        entry = value["releases"][0]
        entry.update(
            report_sha256=objects[f"{prefix}/metadata/baseline_comparison.json"],
            provenance_sha256=objects[manifest_key],
            staged_objects=objects, published_objects=objects,
        )
        entry["candidate_comparison_input"]["derivation"] = {
            "code_sha256": _accounting_code_digest(),
            "parameters": {"mode": "transformation_accounting_v1",
                           "plan_path": "construction_plan.json",
                           "receipt_path": "transformation_accounting.json"},
            "source_objects": {f"{prefix}/streams/{name}": objects[f"{prefix}/streams/{name}"]
                               for name in ("events-a.jsonl", "events-b.jsonl", "regions.jsonl")},
        }
        registry.write_text(json.dumps(value))
        return source, target, registry, prefix, objects

    def test_accounted_construction_replays_and_promotes(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry, prefix, objects = self._accounted_fixture(root)
            candidate_key = f"{prefix}/metadata/candidate.records.jsonl"
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                snapshots = require_release_approval(source, target, [(candidate_key, candidate_key)])
            self.assertIn(candidate_key, snapshots)
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
            published = set(target.list_prefix())
            self.assertTrue(any(key.endswith("transformation_accounting.json") for key in published))
            self.assertTrue(any(key.endswith("transformation_lineage.jsonl") for key in published))
            self.assertEqual(target.sha256_key(f"{prefix}/streams/events-a.jsonl"), objects[f"{prefix}/streams/events-a.jsonl"])

    def test_forged_self_consistent_receipt_is_caught_by_replay(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry, prefix, objects = self._accounted_fixture(root)
            source, target = source, target
            receipt_key = f"{prefix}/metadata/transformation_accounting.json"
            receipt = json.loads(source.read_key(receipt_key))
            receipt["steps"][0]["output_rows"] += 100
            receipt["receipt_id"] = _accounting_digest({k: v for k, v in receipt.items() if k != "receipt_id"})
            source.write_key(receipt_key, json.dumps(receipt, sort_keys=True).encode())
            objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
            report = json.loads(source.read_key(f"{prefix}/metadata/baseline_comparison.json"))
            report["construction_accounting"]["receipt"]["sha256"] = objects[receipt_key]
            source.write_key(f"{prefix}/metadata/baseline_comparison.json", json.dumps(report, sort_keys=True).encode())
            value = json.loads(registry.read_text())
            entry = value["releases"][0]
            entry["staged_objects"] = objects
            entry["published_objects"] = objects
            entry["report_sha256"] = objects[f"{prefix}/metadata/baseline_comparison.json"]
            entry["candidate_comparison_input"]["derivation"]["source_objects"] = {
                f"{prefix}/streams/{name}": objects[f"{prefix}/streams/{name}"]
                for name in ("events-a.jsonl", "events-b.jsonl", "regions.jsonl")}
            manifest_key = f"{prefix}/metadata/source_manifest.json"
            manifest = json.loads(source.read_key(manifest_key))
            manifest["lineage"]["receipt"]["sha256"] = objects[receipt_key]
            source.write_key(manifest_key, json.dumps(manifest).encode())
            snapshot_source_metadata(source, "synthetic", "points", "v1.0.0")
            projection = provenance_projection(load_resolved_source_manifest(source, "synthetic", "points", "v1.0.0").metadata)
            source.write_key(f"{prefix}/metadata/resolved_provenance.json", json.dumps(projection).encode())
            source.write_key(f"{prefix}/metadata/data_dictionary.json",
                             json.dumps({"agency": "Synthetic Agency", **projection}).encode())
            objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
            entry["staged_objects"] = objects
            entry["published_objects"] = objects
            entry["provenance_sha256"] = objects[manifest_key]
            entry["report_sha256"] = objects[f"{prefix}/metadata/baseline_comparison.json"]
            registry.write_text(json.dumps(value))
            candidate_key = f"{prefix}/metadata/candidate.records.jsonl"
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                with self.assertRaisesRegex(ReleaseApprovalError, "construction_accounting_replay_mismatch"):
                    require_release_approval(source, target, [(candidate_key, candidate_key)])
            self.assertEqual(target.list_prefix(), [])

    def test_accounted_mode_requires_declared_transformations_and_matched_candidate_pin(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry, prefix, objects = self._accounted_fixture(root)
            manifest_key = f"{prefix}/metadata/source_manifest.json"
            candidate_key = f"{prefix}/metadata/candidate.records.jsonl"
            with tempfile.TemporaryDirectory() as mutation:
                manifest = json.loads(source.read_key(manifest_key))
                manifest["transformations"] = []
                source.write_key(manifest_key, json.dumps(manifest).encode())
                snapshot_source_metadata(source, "synthetic", "points", "v1.0.0")
                projection = provenance_projection(load_resolved_source_manifest(source, "synthetic", "points", "v1.0.0").metadata)
                source.write_key(f"{prefix}/metadata/resolved_provenance.json", json.dumps(projection).encode())
                source.write_key(f"{prefix}/metadata/data_dictionary.json",
                                 json.dumps({"agency": "Synthetic Agency", **projection}).encode())
                objects_now = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
                value = json.loads(registry.read_text())
                value["releases"][0].update(staged_objects=objects_now, published_objects=objects_now,
                                            provenance_sha256=objects_now[manifest_key])
                registry.write_text(json.dumps(value))
                with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                    with self.assertRaisesRegex(ReleaseApprovalError, "declared_transformations"):
                        require_release_approval(source, target, [(candidate_key, candidate_key)])
            self.assertEqual(target.list_prefix(), [])
            value = json.loads(registry.read_text())
            value["releases"][0]["candidate_comparison_input"]["sha256"] = "0" * 64
            registry.write_text(json.dumps(value))
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                with self.assertRaises(ReleaseApprovalError):
                    require_release_approval(source, target, [(candidate_key, candidate_key)])
            self.assertEqual(target.list_prefix(), [])


if __name__ == "__main__":
    unittest.main()

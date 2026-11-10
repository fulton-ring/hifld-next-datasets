import copy
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.transformation_accounting import (
    AccountingLimits, ADAPTER, account_construction, sha256_file,
)
from dagster_hifld.validation import compare_full_rows


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
            rows = [json.loads(line) for line in lineage]
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
                     "left_keys": [["key"]], "right_keys": [["ward"]], "output_stream": "audited"},
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


if __name__ == "__main__":
    unittest.main()
